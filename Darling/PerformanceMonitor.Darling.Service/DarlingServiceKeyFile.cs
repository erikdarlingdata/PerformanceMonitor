/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One key file the service keeps beside its other credentials: its name on this platform, the most its bytes
/// may run to on the Windows read, and how its text becomes the key.</summary>
/// <typeparam name="TKey">The key as the caller holds it.</typeparam>
/// <param name="FileName">The file's name in the credentials directory.</param>
/// <param name="MaxBytes">The most the file may hold: a larger file is refused before it is read, and the read itself is
/// bounded to it plus one byte.</param>
/// <param name="Parse">Turns the file's text (base64, after the DPAPI layer is removed on Windows) into the key.</param>
/// <param name="TakesDiscardRecord">Whether this key is the one the directory check discards when it finds the directory
/// open (<see cref="ComposeCredentialDirectoryGuard.TakeDiscardedKey"/>), so its load takes that record.</param>
/// <param name="CheckOwner">Whether, on Unix, the file must be owned by the user the service runs as and have one name only
/// (#5366): for a key that cannot be generated again.</param>
/// <param name="CheckDirectoryAccess">Whether, on Windows, an existing directory is refused for a first write when
/// accounts beyond SYSTEM, Administrators and the service account can write to it or change what is in it (#5366).</param>
/// <param name="UniqueTemporary">Whether the file is written through a temporary file with a name of its own each time
/// (<see cref="DarlingServiceKeyFile.UniqueTemporaryInfix"/>), so two writers never share one (#5366).</param>
internal sealed record ServiceKeyFileSpec<TKey>(
    string FileName, int MaxBytes, ServiceKeyParser<TKey> Parse, bool TakesDiscardRecord = false,
    bool CheckOwner = false, bool CheckDirectoryAccess = false, bool UniqueTemporary = false)
    where TKey : class;

/// <summary>Turns a key file's text into the key, or says why it cannot.</summary>
internal delegate ServiceKeyParse<TKey> ServiceKeyParser<TKey>(string encoded)
    where TKey : class;

/// <summary>What a <see cref="ServiceKeyParser{TKey}"/> found: the key, or the reason there is none.</summary>
internal readonly record struct ServiceKeyParse<TKey>(TKey? Key, string? Problem)
    where TKey : class;

/// <summary>A new key and the bytes written for it. <see cref="Material"/> is zeroed by the loader once it is
/// written.</summary>
internal sealed record ServiceKeyGeneration<TKey>(TKey Key, byte[] Material)
    where TKey : class;

internal enum ServiceKeyFileState
{
    /// <summary>The file was trusted, read and parsed.</summary>
    Loaded,

    /// <summary>No file existed, so a new one was written.</summary>
    Generated,

    /// <summary>No file exists and none was written.</summary>
    Absent,

    /// <summary>A file exists and cannot be used; <see cref="ServiceKeyFileResult{TKey}.Reason"/> says why.</summary>
    Refused,

    /// <summary>The directory cannot be trusted, so nothing in it was read or written.</summary>
    DirectoryRefused,
}

/// <summary>What <see cref="DarlingServiceKeyFile"/> found or did. A class-free record: its <see cref="Key"/> is the
/// caller's type, which prints as its own <c>ToString</c> says.</summary>
/// <param name="State">What happened.</param>
/// <param name="Key">The key; null unless <see cref="ServiceKeyFileState.Loaded"/> or
/// <see cref="ServiceKeyFileState.Generated"/>.</param>
/// <param name="Path">The key file's path.</param>
/// <param name="Reason">Why the file or directory cannot be used, written for the operator; null otherwise.</param>
/// <param name="Exists">Whether any entry was at <see cref="Path"/> when this call looked.</param>
/// <param name="Untrusted">True when the refusal is that the file or its directory failed the trust check (as opposed
/// to unreadable or unparseable).</param>
/// <param name="Discarded">The reason the directory check discarded the key at this path this start; see
/// <see cref="ComposeCredentialDirectoryGuard.TakeDiscardedKey"/>.</param>
internal sealed record ServiceKeyFileResult<TKey>(
    ServiceKeyFileState State, TKey? Key, string Path, string? Reason, bool Exists, bool Untrusted, string? Discarded)
    where TKey : class;

/// <summary>
/// The loader every key file the service keeps beside its other credentials goes through (#4004, #5366): the
/// log-hash key (<see cref="DarlingLogHashKeyFile"/>) and the password key (<see cref="DarlingPasswordKeyFile"/>).
/// Extracted from the log-hash key's own, which it behaves as it did.
///
/// <para>The directory is set owner-only again first (<see cref="DarlingManagedRoles.PrepareComposeCredentialDirectory"/>),
/// then the file must be a regular file no one else can reach
/// (<see cref="DarlingManagedRoles.UntrustedComposeCredentialReason"/>), and on Windows is held to an allowlist: no
/// access for anyone beyond SYSTEM, Administrators and the service account, judged and read through one handle nobody
/// can rename, replace or write while it is open (<see cref="DarlingFileSecurity.OpenForServiceOnlyRead"/>). A file
/// that fails any of that is refused, never replaced.</para>
///
/// <para>A key is generated, when the caller supplies a generator, only for a path that holds nothing at all. It is
/// written owner-only from the moment it exists (0600 at creation on Unix, the hardened ACL at creation on Windows),
/// flushed to disk, then moved into place without overwrite, so a file that appeared meanwhile is never replaced. A
/// reason names the path, never the key.</para>
/// </summary>
internal static class DarlingServiceKeyFile
{
    /// <summary>What follows the key file's name in a unique temporary file's name, before the random part (#5366).</summary>
    internal const string UniqueTemporaryInfix = ".tmp-";

    /// <summary>
    /// Looks at <paramref name="spec"/>'s file in <paramref name="directory"/>. With <paramref name="generate"/> null
    /// nothing is ever written (and a missing <paramref name="directory"/> is not created unless
    /// <paramref name="createDirectory"/>); with one, a path that holds nothing gets a new key. Never throws for
    /// anything the file system or the file can do; the result says what happened.
    /// </summary>
    internal static ServiceKeyFileResult<TKey> Load<TKey>(
        string directory,
        ServiceKeyFileSpec<TKey> spec,
        Func<ServiceKeyGeneration<TKey>>? generate,
        bool createDirectory,
        ILogger logger)
        where TKey : class
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentNullException.ThrowIfNull(logger);

        var path = Path.Combine(directory, spec.FileName);
        try
        {
            var trust = DarlingManagedRoles.PrepareComposeCredentialDirectory(directory, createDirectory, logger);
            var exists = AnythingAt(path);

            /* Taken whichever way this load goes, so it describes this process's discard only once (#4004,
               round 3): the check that removed the key may have been another caller's, earlier this start. */
            var discarded = spec.TakesDiscardRecord ? ComposeCredentialDirectoryGuard.Current.TakeDiscardedKey(directory) : null;

            if (trust.Distrust is { } distrust)
            {
                return RefuseDirectory<TKey>(path, directory, distrust, exists);
            }

            if (exists)
            {
                var untrusted = DarlingManagedRoles.UntrustedComposeCredentialReason(new FileInfo(path));
                if (untrusted is not null)
                {
                    return Refuse<TKey>(path, untrusted, untrusted: true, discarded);
                }

                /* #5366: Unix only in effect: Windows has no Unix mode calls, so there is no owner to read there. */
                if (spec.CheckOwner && UnexpectedOwnerReason(path) is { } owner)
                {
                    return Refuse<TKey>(path, owner, untrusted: true, discarded);
                }

                if (!OperatingSystem.IsWindows())
                {
                    return Read(path, spec, held: null, discarded);
                }

                /* #4028: the shared check above only refuses a file Users, Authenticated Users or Everyone can read.
                   A key is read by the service alone, so on Windows it is held to an allowlist: an account beyond
                   SYSTEM, Administrators and the service account that can read it has the key, and one that can write
                   it or re-permission it can make that so (#4044). Judged and read through ONE handle that
                   nobody can rename, replace or write while it is open, so the key read is the key judged, whoever
                   controls the directory. */
                using var held = DarlingFileSecurity.OpenForServiceOnlyRead(path, out var refusal);
                return held is null
                    ? Refuse<TKey>(path, refusal ?? "it could not be checked", untrusted: true, discarded)
                    : Read(path, spec, held, discarded);
            }

            if (generate is null)
            {
                return new ServiceKeyFileResult<TKey>(ServiceKeyFileState.Absent, null, path, null, Exists: false, Untrusted: false, discarded);
            }

            return Write(path, generate, discarded, spec.UniqueTemporary, spec.CheckOwner, logger);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Refuse<TKey>(path, $"it could not be checked ({ex.Message})", untrusted: false, discarded: null, exists: AnythingAtOrFalse(path));
        }
    }

    /// <summary>
    /// Writes a new key where nothing is, and refuses where anything is, the directory is not trusted, or the write
    /// fails. Never overwrites and never reads what is there. Never throws.
    /// </summary>
    internal static ServiceKeyFileResult<TKey> Establish<TKey>(
        string directory,
        ServiceKeyFileSpec<TKey> spec,
        Func<ServiceKeyGeneration<TKey>> generate,
        ILogger logger)
        where TKey : class
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentNullException.ThrowIfNull(generate);
        ArgumentNullException.ThrowIfNull(logger);

        var path = Path.Combine(directory, spec.FileName);
        try
        {
            var existed = Directory.Exists(directory);
            var trust = DarlingManagedRoles.PrepareComposeCredentialDirectory(directory, create: true, logger);
            var exists = AnythingAt(path);
            var discarded = spec.TakesDiscardRecord ? ComposeCredentialDirectoryGuard.Current.TakeDiscardedKey(directory) : null;
            if (trust.Distrust is { } distrust)
            {
                return RefuseDirectory<TKey>(path, directory, distrust, exists);
            }

            /* #5366: a directory that was there before this write is an operator's: an account that can write to it can
               swap the temporary file for another between its close and the move, or rename the key away. The service
               does not re-permission it; it says what to change. A directory the service just made was hardened as it
               was made. */
            if (spec.CheckDirectoryAccess && existed && OperatingSystem.IsWindows()
                && ComposeCredentialDirectoryGuard.Current.UnixModes is null
                && DarlingFileSecurity.WriteAccessBeyondTrusted(directory) is { } writers)
            {
                return RefuseDirectory<TKey>(
                    path, directory, $"accounts beyond SYSTEM, Administrators and the service account can write to it or change what is in it: {writers}; remove their access and restart", exists);
            }

            if (exists)
            {
                return Refuse<TKey>(path, "a file is already there, and a key is never written over one", untrusted: false, discarded);
            }

            return Write(path, generate, discarded, spec.UniqueTemporary, spec.CheckOwner, logger);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Refuse<TKey>(path, $"it could not be checked ({ex.Message})", untrusted: false, discarded: null, exists: AnythingAtOrFalse(path));
        }
    }

    /// <summary>The reason the Unix file at <paramref name="path"/> is not the service's own (#5366): another user owns
    /// it, or it has more than one name. Null when it is the service's, or when this platform cannot say.</summary>
    internal static string? UnexpectedOwnerReason(string path)
    {
        if (ComposeCredentialDirectoryGuard.Current.UnixModes is not { } modes || modes.OwnerOf(path) is not { } owner)
        {
            return null;
        }

        if (owner.UserId != modes.EffectiveUserId)
        {
            return "it is owned by another user than the one the service runs as";
        }

        return owner.Links == 1
            ? null
            : $"it has {owner.Links.ToString(CultureInfo.InvariantCulture)} names, not one";
    }

    private static bool AnythingAtOrFalse(string path)
    {
        try
        {
            return AnythingAt(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            return false;
        }
    }

    /// <summary>Whether ANY entry is at the path, a dangling symbolic link or a directory included: generating a key is
    /// only for a path that holds nothing at all.</summary>
    internal static bool AnythingAt(string path)
    {
        var info = new FileInfo(path);
        return info.LinkTarget is not null || info.Exists || Directory.Exists(path);
    }

    private static string TooLarge<TKey>(ServiceKeyFileSpec<TKey> spec)
        where TKey : class =>
        $"it is larger than {spec.MaxBytes.ToString(CultureInfo.InvariantCulture)} bytes, which no key this service writes is";

    /// <summary>Reads the key from <paramref name="held"/>, the stream <see cref="Load{TKey}"/> judged the file through,
    /// or from the path when there is none (Unix, where mode and owner are the shared check's).</summary>
    private static ServiceKeyFileResult<TKey> Read<TKey>(string path, ServiceKeyFileSpec<TKey> spec, FileStream? held, string? discarded)
        where TKey : class
    {
        string text;
        FileStream? opened = null;
        byte[]? buffer = null;
        try
        {
            /* One bounded read for both platforms (#5366): a file larger than MaxBytes is refused before any of it is
               read, and what is read is never more than MaxBytes plus one byte, whatever the file grows to meanwhile. */
            var stream = held ?? (opened = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1));
            if (stream.Length > spec.MaxBytes)
            {
                return Refuse<TKey>(path, TooLarge(spec), untrusted: false, discarded);
            }

            buffer = new byte[spec.MaxBytes + 1];
            var length = 0;
            int read;
            while (length < buffer.Length && (read = stream.Read(buffer, length, buffer.Length - length)) > 0)
            {
                length += read;
            }

            if (length > spec.MaxBytes)
            {
                return Refuse<TKey>(path, TooLarge(spec), untrusted: false, discarded);
            }

            using var reader = new StreamReader(new MemoryStream(buffer, 0, length), Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            text = reader.ReadToEnd().Trim();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return Refuse<TKey>(path, $"it could not be read ({ex.Message})", untrusted: false, discarded);
        }
        finally
        {
            if (buffer is not null)
            {
                CryptographicOperations.ZeroMemory(buffer);
            }

            opened?.Dispose();
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
                return Refuse<TKey>(path, $"it could not be DPAPI-decrypted on this machine ({ex.Message}); a key file is readable only on the machine that wrote it", untrusted: false, discarded);
            }
        }
        else
        {
            encoded = text;
        }

        var parsed = spec.Parse(encoded.Trim());
        return parsed.Key is null
            ? Refuse<TKey>(path, parsed.Problem ?? "it does not hold a key", untrusted: false, discarded)
            : new ServiceKeyFileResult<TKey>(ServiceKeyFileState.Loaded, parsed.Key, path, null, Exists: true, Untrusted: false, discarded);
    }

    /// <summary>
    /// Writes a new key owner-only from the moment it exists, the way #3983 writes a compose role password: 0600 at
    /// creation on Unix, the hardened ACL at creation on Windows (checked, then hardened once more if ordinary users can
    /// still read it), flushed to disk (<see cref="DarlingManagedRoles.FlushToDisk"/>, so a power loss cannot leave a
    /// zero-length key that every later start refuses), then moved into place WITHOUT overwrite, so a file that
    /// appeared meanwhile is never replaced.
    /// </summary>
    private static ServiceKeyFileResult<TKey> Write<TKey>(
        string path, Func<ServiceKeyGeneration<TKey>> generate, string? discarded, bool uniqueTemporary, bool checkOwner, ILogger logger)
        where TKey : class
    {
        /* A unique temporary (#5366) is created with CreateNew and never deleted first, so a second writer cannot remove
           the first's file and put its own in its place. The fixed name is the log-hash key's, as it was. */
        if (uniqueTemporary)
        {
            RemoveLeftTemporaries(path, logger);
        }

        var temporary = uniqueTemporary
            ? path + UniqueTemporaryInfix + Convert.ToHexString(RandomNumberGenerator.GetBytes(8))
            : path + ".tmp";
        if (!uniqueTemporary && ClearStaleTemporary(temporary) is { } stale)
        {
            return Refuse<TKey>(path, stale, untrusted: false, discarded);
        }

        ServiceKeyGeneration<TKey>? generation = null;
        try
        {
            generation = generate();
            var encoded = Convert.ToBase64String(generation.Material);
            if (!uniqueTemporary)
            {
                File.Delete(temporary);
            }

            if (OperatingSystem.IsWindows())
            {
                using (var stream = DarlingFileSecurity.CreateHardenedFile(temporary, allowInteractiveRead: false))
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(DarlingSecrets.Protect(encoded));
                    DarlingManagedRoles.FlushToDisk(writer, stream);
                }

                /* The same allowlist a key is loaded under (#4028), so a key is never written that its next start
                   would refuse. */
                if (DarlingFileSecurity.AccessBeyondTrusted(temporary) is not null)
                {
                    DarlingFileSecurity.HardenFile(temporary, allowInteractiveRead: false);
                    if (DarlingFileSecurity.AccessBeyondTrusted(temporary) is { } accounts)
                    {
                        throw new InvalidOperationException(
                            $"accounts beyond SYSTEM, Administrators and the service account had access to it even after it was created owner-only and hardened again: {accounts}"
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

            /* The file a start would load is checked the way that start checks it, before it is moved into place (#5366): a
               directory that hands every new file to another user would otherwise take a key, have passwords sealed to it,
               and refuse it on every later start. Nothing is published when it fails. */
            if (checkOwner && UnexpectedOwnerReason(temporary) is { } owner)
            {
                throw new InvalidOperationException($"the new file was not kept, because {owner}");
            }

            File.Move(temporary, path, overwrite: false);
            return new ServiceKeyFileResult<TKey>(ServiceKeyFileState.Generated, generation.Key, path, null, Exists: true, Untrusted: false, discarded);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            TryDelete(temporary, logger);
            return Refuse<TKey>(path, $"there was no key, and a new one could not be written ({ex.Message})", untrusted: false, discarded);
        }
        finally
        {
            if (generation is not null)
            {
                CryptographicOperations.ZeroMemory(generation.Material);
            }
        }
    }

    /// <summary>
    /// A directory where the new key's temporary file goes (#4004): File.Delete cannot remove it, so the write
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

    /// <summary>
    /// Removes the unique temporary files an earlier write left behind (#5366), when it stopped between creating one and
    /// moving it: each holds a whole key, and no other step looks for them. Regular files only: a directory, or a link,
    /// with such a name is not ours and is left. A writer running at the same moment loses its file, so its move fails and
    /// it publishes nothing. Best effort: the next write tries again.
    /// </summary>
    private static void RemoveLeftTemporaries(string path, ILogger logger)
    {
        try
        {
            var directory = Path.GetDirectoryName(path);
            if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory))
            {
                return;
            }

            foreach (var left in Directory.EnumerateFiles(directory, Path.GetFileName(path) + UniqueTemporaryInfix + "*"))
            {
                var info = new FileInfo(left);
                if (info.LinkTarget is null && !info.Attributes.HasFlag(FileAttributes.Directory))
                {
                    TryDelete(left, logger);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            logger.LogDebug("Could not look for leftover temporary files of {File}: {Message}", path, ex.Message);
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
            /* best effort: the next write of a unique-named key file removes leftovers, and a fixed-name one clears its own */
            logger.LogDebug("Could not remove {File}: {Message}", path, ex.Message);
        }
    }

    private static ServiceKeyFileResult<TKey> Refuse<TKey>(string path, string reason, bool untrusted, string? discarded, bool exists = true)
        where TKey : class =>
        new(ServiceKeyFileState.Refused, null, path, reason, exists, untrusted, discarded);

    private static ServiceKeyFileResult<TKey> RefuseDirectory<TKey>(string path, string directory, string distrust, bool exists)
        where TKey : class =>
        new(ServiceKeyFileState.DirectoryRefused, null, path, $"its directory {directory} is not trusted ({distrust})", exists, Untrusted: true, Discarded: null);
}
