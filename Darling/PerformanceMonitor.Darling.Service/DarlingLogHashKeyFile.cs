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
/// Unix; on Windows, owned by a trusted principal and not readable by ordinary users. The directory is judged by the
/// mode this START first found it with (<see cref="ComposeCredentialDirectoryStart"/>), because on compose role
/// provisioning looks first and sets it owner-only before the key loads.</para>
///
/// <para><b>Where it parts from #3983: an existing key is not replaced, with one exception.</b> A role password costs
/// only a re-assert to regenerate, so #3983 discards a file it cannot trust. A new key gives every stored log event a
/// new identity: the reads dedupe on <c>raw_line_hash</c> and the lock-wait facts group on <c>statement_fingerprint</c>.
/// So a key is generated when there is no file at the path at all, and a file that cannot be trusted, read or parsed
/// is REFUSED: the load returns no key with the reason, the service logs it, and the collectors that hash refuse to run
/// (<see cref="PgLogHashKey.UnavailableMessage"/>) until an operator fixes the file or deletes it. Deleting is how an
/// operator rotates, and it is a deliberate act.</para>
///
/// <para>The exception is #3983's own case (#4004 review): a directory other users could write to until this start.
/// Any file in it could have been planted, and the file check cannot tell (it cannot see a Unix owner), so the key
/// there is discarded and a new one generated, as every role password there is. Refusing it instead would hold for
/// one start only: the next finds the directory owner-only, because this start set it so, and would trust the
/// planted file.</para>
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
    /// The worker's one call at start: the key for this distribution, or null when it cannot be used, in which case the
    /// reason is logged as an error naming the file and what to do. Never throws; never logs key material.
    /// </summary>
    public static PgLogHashKey? LoadForService(DarlingConfig config, string configPath, ILogger logger)
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
            return null;
        }

        var load = Load(directory, logger);
        if (load.Key is null)
        {
            logger.LogError("{Refusal}", load.Refusal);
        }

        return load.Key;
    }

    /// <summary>
    /// Loads the key from <paramref name="directory"/>, generating it when no file exists at its path, or when the
    /// directory was open to other users until this start (the class's one exception). Never throws for anything the
    /// file system or the file can do; the result says what happened.
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

            if (trust.Distrust is { } distrust)
            {
                if (!trust.MayWrite)
                {
                    return Refuse(path, $"its directory {directory} is not trusted ({distrust})");
                }

                /* The directory let other users put files in it until this start (#3983's arm), and a planted 0600 file
                   passes the file check, which cannot see a Unix owner. The verdict is the start's (#4004 review): on
                   compose, role provisioning runs first and sets the directory 0700, and judged by what this call found,
                   the key load trusted a planted key. So a key found there is discarded and a new one generated, as
                   #3983 replaces every role password there. Refusing it would last one start: the next finds the
                   directory owner-only and trusts the same file. Only the service can reach the directory now. */
                if (exists && Discard(path, directory, distrust, logger) is { } failure)
                {
                    return Refuse(path, failure);
                }

                return Generate(path, logger);
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

            return Generate(path, logger);
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

    /// <summary>
    /// Removes the key a directory that was open to other users held (#4004 review), or says why it could not. A
    /// symbolic link is removed itself, never followed; a directory at the path is not removed, and refuses the load
    /// as it would anywhere else.
    /// </summary>
    private static string? Discard(string path, string directory, string distrust, ILogger logger)
    {
        if (new FileInfo(path).LinkTarget is null && Directory.Exists(path))
        {
            return "it is a directory";
        }

        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return $"its directory {directory} was open to other users until this start ({distrust}) and it could not be removed ({ex.Message})";
        }

        logger.LogWarning(
            "The store's log-hash key {Path} was in a directory other users could write to until this start ({Reason}), so it may not be the key this service generated. It is discarded and a new one generated (#4004); log events stored from now on are identified by hashes keyed with the new key.",
            path, distrust);
        return null;
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
        var material = RandomNumberGenerator.GetBytes(PgLogHashKey.KeyLength);
        var temporary = path + ".tmp";
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
            return Refuse(path, $"it did not exist and a new one could not be written ({ex.Message})");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(material);
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
}

/// <summary>What <see cref="DarlingLogHashKeyFile.Load"/> found (#4004): the key, or why there is none. Its
/// <see cref="Key"/> prints as its type name only, so this record's generated <c>ToString</c> carries no key material.</summary>
/// <param name="Key">The key, or null when it cannot be used.</param>
/// <param name="Path">The key file's path.</param>
/// <param name="Generated">True when this load wrote a new key because none existed.</param>
/// <param name="Refusal">Why there is no key, written for the operator; null when there is one.</param>
public sealed record DarlingLogHashKeyLoad(PgLogHashKey? Key, string Path, bool Generated, string? Refusal);
