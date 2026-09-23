/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The store's log-hash key on disk (#4004): the secret <see cref="PgLogHashKey"/> keys <c>raw_line_hash</c> and
/// <c>statement_fingerprint</c> with. The SERVICE generates it and keeps it outside the store, so a reader of the store
/// has nothing to test a guess against, and loads it once at start for every collector that hashes log text.
///
/// <para><b>Where it lives</b> (<see cref="DirectoryFor"/>), by distribution, next to the secrets that distribution
/// already keeps:</para>
/// <list type="bullet">
/// <item><description>Managed Windows: beside <c>pg-credential.dpapi</c> and the role credentials, as a DPAPI
/// (LocalMachine) blob restricted to SYSTEM, Administrators and the service account.</description></item>
/// <item><description>Compose (the service in its container): in the <c>darling-credentials</c> volume beside the role
/// passwords (#3914), an owner-only file.</description></item>
/// <item><description>Bring-your-own: in <c>darling-keys</c> beside <c>darling.json</c>, a directory the service creates
/// owner-only so the directory check below can hold (the config directory itself is the operator's, and is left as
/// it is).</description></item>
/// </list>
/// <para>On Windows the file always holds a DPAPI blob (<see cref="WindowsFileName"/>); on Linux it holds the key,
/// base64, in a 0600 file (<see cref="UnixFileName"/>).</para>
///
/// <para><b>Trusted the way #3983 trusts the compose role passwords</b>: the directory is set owner-only again first
/// (<see cref="DarlingManagedRoles.PrepareComposeCredentialDirectory"/>), then the file must be a regular file no one
/// else can reach (<see cref="DarlingManagedRoles.UntrustedComposeCredentialReason"/>): no group or other bits on
/// Unix; on Windows, owned by a trusted principal and not readable by ordinary users.</para>
///
/// <para><b>Where it parts from #3983: an existing key is not replaced, with one exception.</b> A role password costs
/// only a re-assert to regenerate, so #3983 discards a file it cannot trust. A new key gives every stored log event a
/// new identity: the reads dedupe on <c>raw_line_hash</c> and the lock-wait facts group on <c>statement_fingerprint</c>.
/// So a key is generated when there is no file at the path at all, and a file that cannot be trusted, read or parsed
/// is REFUSED: the load returns no key with the reason, the service logs it, and the collectors that hash refuse to run
/// (<see cref="PgLogHashKey.UnavailableMessage"/>) until an operator fixes the file or deletes it. Deleting is how an
/// operator rotates, and it is a deliberate act.</para>
///
/// <para>The exception is #3983's own case (#4004 review): a directory other users could write to until the service
/// set it owner-only. Any file in it could have been planted, and the file check cannot tell (it cannot see a Unix
/// owner), so the directory check removes the key there along with every role password, at the moment it finds the
/// directory open, whichever caller that is (<see cref="DarlingManagedRoles.PrepareComposeCredentialDirectory"/>),
/// and this load then generates a new one. Refusing the key instead would hold for one start only: the next finds the
/// directory owner-only, because the check set it so, and would trust the planted file. The removal is logged as an
/// error, since a directory that keeps being opened (a Kubernetes fsGroup re-applied on every mount) rotates the key
/// on every start, and the replacement is noted on the collection-log row of the first <c>pg_log_events</c> run after
/// it (<see cref="RotationNote"/>, #4004 review, round 3).</para>
/// </summary>
public static class DarlingLogHashKeyFile
{
    /// <summary>The key file on Windows: a DPAPI blob, named with the managed credentials' extension because it is
    /// protected the same way.</summary>
    public const string WindowsFileName = "log-hash-key.dpapi";

    /// <summary>The key file on Linux: the key itself, base64, owner-only.</summary>
    public const string UnixFileName = "log-hash-key";

    /// <summary>The directory beside <c>darling.json</c> that holds a bring-your-own service's key.</summary>
    public const string BringYourOwnDirectoryName = "darling-keys";

    /// <summary>This platform's key file name.</summary>
    public static string FileName => OperatingSystem.IsWindows() ? WindowsFileName : UnixFileName;

    /// <summary>
    /// The directory the key lives in for this service's distribution: the managed store's credential directory, the
    /// compose container's credentials volume, or <see cref="BringYourOwnDirectoryName"/> beside <paramref name="configPath"/>.
    /// The first two are the branches the worker's role provisioning takes, by the same tests.
    /// </summary>
    public static string DirectoryFor(DarlingConfig config, string configPath)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (config.Postgres.Managed && OperatingSystem.IsWindows())
        {
            var credential = DarlingManagedPostgres.CredentialPathFor(DarlingManagedPostgres.ResolveDataDirectory(config.Postgres));
            return Path.GetDirectoryName(credential) ?? throw new InvalidOperationException($"{credential} has no directory.");
        }

        if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)
        {
            return DarlingManagedRoles.ComposeStoreCredentialDirectory;
        }

        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath)) ?? AppContext.BaseDirectory;
        return Path.Combine(configDirectory, BringYourOwnDirectoryName);
    }

    /// <summary>
    /// The worker's one call at start: the key for this distribution, with <see cref="DarlingLogHashKeyLoad.Key"/> null
    /// when it cannot be used, in which case the reason is logged as an error naming the file and what to do, and
    /// <see cref="DarlingLogHashKeyLoad.Replaced"/> set when the key replaced one the directory check discarded. Never
    /// throws; never logs key material.
    /// </summary>
    public static DarlingLogHashKeyLoad LoadForService(DarlingConfig config, string configPath, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);

        string directory;
        try
        {
            directory = DirectoryFor(config, configPath);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                "PostgreSQL log events will not be collected: the directory for the store's log-hash key could not be resolved ({Message}) (#4004).",
                ex.Message);
            return new DarlingLogHashKeyLoad(null, string.Empty, Generated: false, Refusal: $"the directory for the store's log-hash key could not be resolved ({ex.Message})");
        }

        var load = Load(directory, logger);
        if (load.Key is null)
        {
            logger.LogError("{Refusal}", load.Refusal);
        }

        return load;
    }

    /// <summary>
    /// The note the first <c>pg_log_events</c> run after a start that replaced a discarded key carries on its
    /// collection-log row (#4004 review, round 3), so the point where stored events changed identity can be found beside
    /// the rows it affects rather than only in the service log. Null when <paramref name="load"/> replaced nothing.
    /// </summary>
    public static string? RotationNote(DarlingLogHashKeyLoad load)
    {
        ArgumentNullException.ThrowIfNull(load);
        return load.Replaced is { } reason
            ? $"the store's log-hash key was replaced at service start ({load.Path} was discarded because {reason}): log events stored from this run on have new identities, so an entry still in the log tail is stored once more and statement fingerprints change (#4004)"
            : null;
    }

    /// <summary>
    /// Loads the key from <paramref name="directory"/>, generating it when no file exists at its path, which includes a
    /// key the directory check has just removed (the class's one exception). Never throws for anything the file system
    /// or the file can do; the result says what happened.
    /// </summary>
    public static DarlingLogHashKeyLoad Load(string directory, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(logger);

        var path = Path.Combine(directory, FileName);
        try
        {
            var trust = DarlingManagedRoles.PrepareComposeCredentialDirectory(directory, create: true, logger);
            var exists = AnythingAt(path);

            /* Taken whichever way this load goes, so it describes this process's discard only once (#4004 review,
               round 3): the check that removed the key may have been another caller's, earlier this start. */
            var discarded = ComposeCredentialDirectoryGuard.Current.TakeDiscardedKey(directory);

            /* A directory other users could write to until now has already lost every file the service reads from it,
               this key included, inside the check itself (#4004 review): whichever caller found it open (role
               provisioning, a host's earlier-credential read, or this load) removed them before returning. So a key
               found past the check is one written while only the service could reach the directory, and a directory
               that is not trusted is one nothing is read from or written to. */
            if (trust.Distrust is { } distrust)
            {
                return RefuseForDirectory(path, directory, distrust);
            }

            if (exists)
            {
                var untrusted = DarlingManagedRoles.UntrustedComposeCredentialReason(new FileInfo(path));
                if (untrusted is not null)
                {
                    return Refuse(path, untrusted);
                }

                return Read(path, logger);
            }

            var generated = Generate(path, logger);
            return generated.Key is not null && discarded is not null ? generated with { Replaced = discarded } : generated;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Refuse(path, $"it could not be checked ({ex.Message})");
        }
    }

    /// <summary>Whether ANY entry is at the path, a dangling symbolic link or a directory included: generating a key is
    /// only for a path that holds nothing at all.</summary>
    private static bool AnythingAt(string path)
    {
        var info = new FileInfo(path);
        return info.LinkTarget is not null || info.Exists || Directory.Exists(path);
    }

    private static DarlingLogHashKeyLoad Read(string path, ILogger logger)
    {
        string text;
        try
        {
            text = File.ReadAllText(path).Trim();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return Refuse(path, $"it could not be read ({ex.Message})");
        }

        string encoded;
        if (OperatingSystem.IsWindows())
        {
            try
            {
                encoded = DarlingSecrets.Unprotect(text);
            }
            catch (Exception ex) when (ex is CryptographicException or FormatException or ArgumentException)
            {
                return Refuse(path, $"it could not be DPAPI-decrypted on this machine ({ex.Message}); a key file is readable only on the machine that wrote it");
            }
        }
        else
        {
            encoded = text;
        }

        var material = new byte[PgLogHashKey.KeyLength];
        try
        {
            if (!Convert.TryFromBase64String(encoded.Trim(), material, out var written) || written != PgLogHashKey.KeyLength)
            {
                return Refuse(path, $"it does not hold a {PgLogHashKey.KeyLength}-byte key");
            }

            logger.LogInformation("Loaded the store's log-hash key from {Path} (#4004)", path);
            return new DarlingLogHashKeyLoad(new PgLogHashKey(material), path, Generated: false, Refusal: null);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(material);
        }
    }

    /// <summary>
    /// Writes a new key owner-only from the moment it exists, the way #3983 writes a compose role password: 0600 at
    /// creation on Unix, the hardened ACL at creation on Windows (checked, then hardened once more if ordinary users can
    /// still read it), flushed to disk (<see cref="DarlingManagedRoles.FlushToDisk"/>, so a power loss cannot leave a
    /// zero-length key that every later start refuses), then moved into place WITHOUT overwrite, so a file that
    /// appeared meanwhile is never replaced.
    /// </summary>
    private static DarlingLogHashKeyLoad Generate(string path, ILogger logger)
    {
        var temporary = path + ".tmp";
        if (ClearStaleTemporary(temporary) is { } stale)
        {
            return Refuse(path, stale);
        }

        var material = RandomNumberGenerator.GetBytes(PgLogHashKey.KeyLength);
        try
        {
            var encoded = Convert.ToBase64String(material);
            File.Delete(temporary);
            if (OperatingSystem.IsWindows())
            {
                using (var stream = DarlingFileSecurity.CreateHardenedFile(temporary, allowInteractiveRead: false))
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(DarlingSecrets.Protect(encoded));
                    DarlingManagedRoles.FlushToDisk(writer, stream);
                }

                if (DarlingFileSecurity.IsReadableByOrdinaryUsers(temporary))
                {
                    DarlingFileSecurity.HardenFile(temporary, allowInteractiveRead: false);
                    if (DarlingFileSecurity.IsReadableByOrdinaryUsers(temporary))
                    {
                        throw new InvalidOperationException(
                            "ordinary local users could read it even after it was created owner-only and hardened again"
                            + DarlingFileSecurity.DescribeOwnerAndExposure(temporary));
                    }
                }
            }
            else
            {
                using var stream = new FileStream(temporary, new FileStreamOptions
                {
                    Mode = FileMode.CreateNew,
                    Access = FileAccess.Write,
                    UnixCreateMode = DarlingManagedRoles.OwnerOnlyFile,
                });
                using var writer = new StreamWriter(stream);
                writer.Write(encoded);
                DarlingManagedRoles.FlushToDisk(writer, stream);
            }

            File.Move(temporary, path, overwrite: false);
            logger.LogInformation(
                "Generated the store's log-hash key {Path} (#4004). Log events stored from now on are identified by hashes keyed with it; keep it with the store's other credentials, because a new key gives every stored log event a new identity.",
                path);
            return new DarlingLogHashKeyLoad(new PgLogHashKey(material), path, Generated: true, Refusal: null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            TryDelete(temporary, logger);
            return Refuse(path, $"there was no key, and a new one could not be written ({ex.Message})");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(material);
        }
    }

    /// <summary>
    /// A directory where the new key's temporary file goes (#4004 review): File.Delete cannot remove it, so the write
    /// failed on every start with a reason that named only the key ("it did not exist"). An empty one is removed, which
    /// is safe (a non-recursive delete removes nothing inside, and a symbolic link is not a directory here); one with
    /// anything in it is someone's tree, left alone, and named as the reason. Null when the path is clear.
    /// </summary>
    private static string? ClearStaleTemporary(string temporary)
    {
        if (new FileInfo(temporary).LinkTarget is not null || !Directory.Exists(temporary))
        {
            return null;
        }

        try
        {
            Directory.Delete(temporary, recursive: false);
            return null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return $"{temporary}, the file a new key is written through before it is moved into place, is a directory the service could not remove ({ex.Message}); remove it and restart";
        }
    }

    private static void TryDelete(string path, ILogger logger)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            logger.LogDebug("Could not remove {File}: {Message}", path, ex.Message);
        }
    }

    private static DarlingLogHashKeyLoad Refuse(string path, string reason) => new(
        Key: null,
        Path: path,
        Generated: false,
        Refusal: $"PostgreSQL log events will not be collected: the store's log-hash key {path} cannot be used because {reason} (#4004). "
            + "The service does not replace this key on its own, because a new key gives every stored log event a new identity. "
            + "If only this service could have written or read it, restrict it to the service account (on Windows, --harden-files run elevated does that) and restart; otherwise, or to start a new key, delete it and restart, and the service generates a new one.");

    /// <summary>
    /// The refusal when the DIRECTORY is what cannot be trusted (#4004 review, round 3). The key file's own remedies
    /// (restrict it, or delete it) do not apply: nothing in the directory was read, and the reason says what to fix.
    /// Round 2 gave this case the file's text, whose "restrict it" read as "set the directory 0700" to the operator
    /// the round-3 attack walks through.
    /// </summary>
    private static DarlingLogHashKeyLoad RefuseForDirectory(string path, string directory, string distrust) => new(
        Key: null,
        Path: path,
        Generated: false,
        Refusal: $"PostgreSQL log events will not be collected: the store's log-hash key {path} cannot be used because its directory {directory} is not trusted ({distrust}) (#4004). "
            + "Nothing in that directory is read or written while it is not trusted; fix what the reason names and restart.");
}

/// <summary>What <see cref="DarlingLogHashKeyFile.Load"/> found (#4004): the key, or why there is none. Its
/// <see cref="Key"/> prints as its type name only, so this record's generated <c>ToString</c> carries no key material.</summary>
/// <param name="Key">The key, or null when it cannot be used.</param>
/// <param name="Path">The key file's path.</param>
/// <param name="Generated">True when this load wrote a new key because none existed.</param>
/// <param name="Refusal">Why there is no key, written for the operator; null when there is one.</param>
/// <param name="Replaced">Why the key this load generated replaced one: the reason the directory check discarded the
/// old key this start (<see cref="ComposeCredentialDirectoryGuard.TakeDiscardedKey"/>). Null when it replaced nothing,
/// including a key generated because none had ever existed.</param>
public sealed record DarlingLogHashKeyLoad(PgLogHashKey? Key, string Path, bool Generated, string? Refusal, string? Replaced = null);

/// <summary>
/// "The store's log-hash key was replaced at start", held for the process until the first <c>pg_log_events</c> run
/// takes it (#4004 review, round 3): that run's collection-log row carries it as the run's note, and no later run's
/// does, so the point where stored events changed identity sits beside the rows it affects. A run that fails before it
/// returns a result does not take it, so it lands on the first row with a run note to carry it, whichever server that
/// run was for. The worker arms it from the key's load (<see cref="DarlingLogHashKeyFile.RotationNote"/>) and applies
/// it to every run it records.
/// </summary>
internal sealed class LogHashKeyRotationNote
{
    private string? _note;

    /// <summary>Holds <paramref name="note"/> for the next <c>pg_log_events</c> run. Null holds nothing and clears
    /// nothing, so a later load that replaced no key cannot drop a note no run has taken yet.</summary>
    internal void Arm(string? note)
    {
        if (note is not null)
        {
            Volatile.Write(ref _note, note);
        }
    }

    /// <summary>
    /// <paramref name="result"/> with the held note merged into its host note when this is a <c>pg_log_events</c> run
    /// and no run has taken the note yet; otherwise <paramref name="result"/> unchanged. Taking it clears it
    /// atomically, so two servers' runs finishing together cannot both carry it. Host note, not the composed
    /// <see cref="CollectorRunResult.Note"/>, which would lose the definition's measurements.
    /// </summary>
    internal CollectorRunResult ApplyTo(string collectorName, CollectorRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (!string.Equals(collectorName, PgLogEventsCollector.Instance.Name, StringComparison.OrdinalIgnoreCase)
            || Interlocked.Exchange(ref _note, null) is not { } note)
        {
            return result;
        }

        return result with { HostNote = EnumeratedCollectorDriver.MergeNotes(result.HostNote, note) };
    }
}
