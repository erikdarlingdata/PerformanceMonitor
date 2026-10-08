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
/// Unix; on Windows, owned by a trusted principal and not readable by ordinary users, then held to an allowlist: no
/// access for anyone beyond SYSTEM, Administrators and the service account (#4028), judged and read through one
/// handle nobody can rename, replace or write while it is open (<see cref="DarlingFileSecurity.OpenForServiceOnlyRead"/>).</para>
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
    /// or the file can do; the result says what happened. The trust checks, the read and the write are the shared
    /// loader's (<see cref="DarlingServiceKeyFile"/>); this class keeps the key's own wording.
    /// </summary>
    public static DarlingLogHashKeyLoad Load(string directory, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(logger);

        var result = DarlingServiceKeyFile.Load(directory, Spec, NewKey, createDirectory: true, logger);
        switch (result.State)
        {
            case ServiceKeyFileState.Loaded:
                logger.LogInformation("Loaded the store's log-hash key from {Path} (#4004)", result.Path);
                return new DarlingLogHashKeyLoad(result.Key, result.Path, Generated: false, Refusal: null);

            case ServiceKeyFileState.Generated:
                logger.LogInformation(
                    "Generated the store's log-hash key {Path} (#4004). Log events stored from now on are identified by hashes keyed with it; keep it with the store's other credentials, because a new key gives every stored log event a new identity.",
                    result.Path);
                return new DarlingLogHashKeyLoad(result.Key, result.Path, Generated: true, Refusal: null, Replaced: result.Discarded);

            case ServiceKeyFileState.DirectoryRefused:
                return RefuseForDirectory(result.Path, result.Reason!);

            default:
                return Refuse(result.Path, result.Reason ?? "it could not be checked");
        }
    }

    /// <summary>The most a key file's bytes may run to. A key this service writes is about 350 on Windows (a DPAPI blob
    /// of the base64 key, in base64) and 44 elsewhere.</summary>
    private const int MaxKeyFileBytes = 1024;

    private static readonly ServiceKeyFileSpec<PgLogHashKey> Spec = new(FileName, MaxKeyFileBytes, ParseKey, TakesDiscardRecord: true);

    private static ServiceKeyParse<PgLogHashKey> ParseKey(string encoded)
    {
        var material = new byte[PgLogHashKey.KeyLength];
        try
        {
            return !Convert.TryFromBase64String(encoded.Trim(), material, out var written) || written != PgLogHashKey.KeyLength
                ? new ServiceKeyParse<PgLogHashKey>(null, $"it does not hold a {PgLogHashKey.KeyLength}-byte key")
                : new ServiceKeyParse<PgLogHashKey>(new PgLogHashKey(material), null);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(material);
        }
    }

    private static ServiceKeyGeneration<PgLogHashKey> NewKey()
    {
        var material = RandomNumberGenerator.GetBytes(PgLogHashKey.KeyLength);
        return new ServiceKeyGeneration<PgLogHashKey>(new PgLogHashKey(material), material);
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
    private static DarlingLogHashKeyLoad RefuseForDirectory(string path, string reason) => new(
        Key: null,
        Path: path,
        Generated: false,
        Refusal: $"PostgreSQL log events will not be collected: the store's log-hash key {path} cannot be used because {reason} (#4004). "
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
