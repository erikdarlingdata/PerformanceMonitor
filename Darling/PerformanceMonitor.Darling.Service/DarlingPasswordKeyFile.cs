/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service's password key on disk (#5366): an RSA-3072 private key, PKCS#8, kept in the same directory as the
/// log-hash key (<see cref="DarlingLogHashKeyFile.DirectoryFor"/>), so both always sit with the service's other
/// credentials. On Windows the file holds a DPAPI (LocalMachine) blob, restricted to SYSTEM, Administrators and the
/// service account (<see cref="WindowsFileName"/>); elsewhere it holds the key, base64, in a 0600 file
/// (<see cref="UnixFileName"/>).
///
/// <para>The trust checks, the read and the write are <see cref="DarlingServiceKeyFile"/>'s, the loader the log-hash key
/// shares. What differs is what a missing file means. <see cref="Load"/> NEVER generates: the key's callers decide
/// whether a missing file is a first start or a lost key, so a read can never replace a key. <see cref="Generate"/>
/// writes a key only where nothing is, and never overwrites. <see cref="Retire"/> renames the file and never deletes
/// it, so the bytes stay.</para>
///
/// <para>A directory that other users could write to until the service set it owner-only keeps the key file, renamed to
/// <see cref="QuarantineFileName"/> (never deleted, never overwritten), so the live name is empty and the record is on
/// disk: every later start, and every other process, finds it there and says so
/// (<see cref="PasswordKeyFileLoad.FoundAfterOpenDirectory"/>). The caller decides whether the key it holds still
/// matches what it published, then <see cref="Accept"/>s it (renamed back to the live name) or <see cref="Retire"/>s
/// it. Every other credential file in such a directory is removed.</para>
///
/// <para>A refusal names the file and its directory, never the key.</para>
///
/// <para>The key's text form (base64) passes through managed strings while it is read and written, as the log-hash key's
/// does, and the key itself stays in memory for as long as the service runs.</para>
/// </summary>
public static class DarlingPasswordKeyFile
{
    /// <summary>The key file on Linux: the PKCS#8 key, base64, owner-only.</summary>
    public const string UnixFileName = "password-key";

    /// <summary>The key file on Windows: a DPAPI blob, named with the managed credentials' extension.</summary>
    public const string WindowsFileName = "password-key.dpapi";

    /// <summary>The key's size, in bits.</summary>
    public const int KeyBits = 3072;

    /// <summary>This platform's key file name.</summary>
    public static string FileName => OperatingSystem.IsWindows() ? WindowsFileName : UnixFileName;

    /// <summary>What a key file found while its directory was open is renamed to: the key file's name, then this.</summary>
    public const string QuarantineSuffix = ".open-directory";

    /// <summary>This platform's name for a key file kept from an open directory.</summary>
    public static string QuarantineFileName => FileName + QuarantineSuffix;

    /// <summary>The most a key file's bytes may run to: a 3072-bit PKCS#8 key is about 1.8 KB, which is about 2.4 KB
    /// as base64 and about 3.5 KB as a base64 DPAPI blob of that.</summary>
    private const int MaxKeyFileBytes = 8192;

    /* The key cannot be generated again, so it is also held to what a file the service wrote is: owned by the service's
       user with one name (Unix), and in a directory only the service can change (Windows), written through a temporary
       file of its own. */
    private static readonly ServiceKeyFileSpec<byte[]> Spec = new(
        FileName, MaxKeyFileBytes, ParseKey, CheckOwner: true, CheckDirectoryAccess: true, UniqueTemporary: true);

    private static readonly ServiceKeyFileSpec<byte[]> QuarantineSpec = Spec with { FileName = QuarantineFileName };

    /// <summary>
    /// Looks for the key in <paramref name="directory"/> and reads it when it is trusted. NEVER generates and never
    /// writes: with no file the result is <see cref="PasswordKeyFileLoad.Present"/> false, and a directory that does not
    /// exist is not created. Never throws for anything the file system or the file can do; the result says what
    /// happened. The caller owns <see cref="PasswordKeyFileLoad.Pkcs8"/> and zeroes it
    /// (<see cref="CryptographicOperations.ZeroMemory"/>) when done.
    ///
    /// <para>With nothing at the key's name and a file at <see cref="QuarantineFileName"/> (kept from an open directory),
    /// that file is read under the same checks and the result has
    /// <see cref="PasswordKeyFileLoad.FoundAfterOpenDirectory"/> true and the quarantine path. Files at both names are
    /// refused as untrusted: which one is the key is for the operator to say.</para>
    /// </summary>
    public static PasswordKeyFileLoad Load(string directory, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(logger);

        var path = Path.Combine(directory, FileName);
        ServiceKeyFileResult<byte[]>? live = null;
        try
        {
            live = DarlingServiceKeyFile.Load(directory, Spec, generate: null, createDirectory: false, logger);
            if (live.State == ServiceKeyFileState.DirectoryRefused || !DarlingServiceKeyFile.AnythingAt(path + QuarantineSuffix))
            {
                return Describe(live, foundAfterOpenDirectory: false, logger);
            }

            if (live.Exists)
            {
                /* Not used past here: the key is zeroed before the refusal returns (#5366). */
                if (live.Key is not null)
                {
                    CryptographicOperations.ZeroMemory(live.Key);
                }

                return new PasswordKeyFileLoad(
                    Present: true, Untrusted: true,
                    $"Two password key files are in {directory}: {FileName} and {QuarantineFileName}. Keep the right one and remove the other, then restart.",
                    Pkcs8: null, FoundAfterOpenDirectory: true, path);
            }

            return Describe(
                DarlingServiceKeyFile.Load(directory, QuarantineSpec, generate: null, createDirectory: false, logger),
                foundAfterOpenDirectory: true, logger);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (live?.Key is not null)
            {
                CryptographicOperations.ZeroMemory(live.Key);
            }

            return new PasswordKeyFileLoad(
                Present: ExistsOrFalse(path), Untrusted: false, Refusal: $"The password key {FileName} in {directory} could not be checked ({ex.Message}).",
                Pkcs8: null, FoundAfterOpenDirectory: false, path);
        }
    }

    private static PasswordKeyFileLoad Describe(ServiceKeyFileResult<byte[]> result, bool foundAfterOpenDirectory, ILogger logger)
    {
        switch (result.State)
        {
            case ServiceKeyFileState.Loaded:
                logger.LogInformation("Loaded the password key from {Path} (#5366)", result.Path);
                return new PasswordKeyFileLoad(Present: true, Untrusted: false, Refusal: null, result.Key, foundAfterOpenDirectory, result.Path);

            case ServiceKeyFileState.Absent:
                return new PasswordKeyFileLoad(Present: false, Untrusted: false, Refusal: null, Pkcs8: null, FoundAfterOpenDirectory: false, result.Path);

            default:
                return new PasswordKeyFileLoad(
                    Present: result.Exists, result.Untrusted, Refusal(result), Pkcs8: null, foundAfterOpenDirectory, result.Path);
        }
    }

    private static bool ExistsOrFalse(string path)
    {
        try
        {
            return DarlingServiceKeyFile.AnythingAt(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// Writes a new key where nothing is. <paramref name="newPkcs8"/> supplies the PKCS#8 bytes; they are checked (an
    /// RSA private key of exactly <see cref="KeyBits"/> bits) before anything is written, and zeroed after they are
    /// written. Written owner-only from the moment the file exists (0600 on Unix; DPAPI LocalMachine and the allowlist
    /// ACL on Windows), flushed to disk, then moved into place without overwrite. A file or any other entry already at
    /// the path is refused and left exactly as it is. The result's <see cref="PasswordKeyFileLoad.Pkcs8"/> is the
    /// caller's own copy of the key, to zero when done. Never throws for anything the file system can do.
    /// </summary>
    public static PasswordKeyFileLoad Generate(string directory, Func<byte[]> newPkcs8, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(newPkcs8);
        ArgumentNullException.ThrowIfNull(logger);

        var path = Path.Combine(directory, FileName);
        byte[]? supplied = null;
        byte[]? clone = null;
        var generated = false;
        try
        {
            /* A check that cannot be made refuses (#5366): a kept key may be waiting, and a new one is never written
               beside it. */
            bool kept;
            try
            {
                kept = DarlingServiceKeyFile.AnythingAt(path + QuarantineSuffix);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException)
            {
                return new PasswordKeyFileLoad(Present: false, Untrusted: false, Refusal: $"a new password key was not written to {path}: whether {QuarantineFileName} holds a key kept from an earlier start could not be checked ({ex.Message})", Pkcs8: null, FoundAfterOpenDirectory: false, path);
            }

            if (kept)
            {
                return new PasswordKeyFileLoad(Present: true, Untrusted: false, Refusal: $"a new password key was not written to {path}: {QuarantineFileName} holds a key kept from an earlier start; accept it or retire it first", Pkcs8: null, FoundAfterOpenDirectory: true, path);
            }

            supplied = newPkcs8();
            if (CheckPkcs8(supplied) is { } invalid)
            {
                return new PasswordKeyFileLoad(Present: false, Untrusted: false, Refusal: $"a new password key was not written to {path}: {invalid}", Pkcs8: null, FoundAfterOpenDirectory: false, path);
            }

            var material = supplied;
            var result = DarlingServiceKeyFile.Establish(
                directory,
                Spec,
                () => new ServiceKeyGeneration<byte[]>(clone = (byte[])material.Clone(), material),
                logger);

            if (result.State == ServiceKeyFileState.Generated)
            {
                generated = true;
                logger.LogInformation("Generated the password key {Path} (#5366)", result.Path);
                return new PasswordKeyFileLoad(Present: true, Untrusted: false, Refusal: null, result.Key, FoundAfterOpenDirectory: false, result.Path);
            }

            return new PasswordKeyFileLoad(
                Present: result.Exists, result.Untrusted, Refusal(result), Pkcs8: null, FoundAfterOpenDirectory: false, result.Path);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new PasswordKeyFileLoad(Present: false, Untrusted: false, Refusal: $"a new password key was not written to {path}: it could not be generated ({ex.Message})", Pkcs8: null, FoundAfterOpenDirectory: false, path);
        }
        finally
        {
            if (supplied is not null)
            {
                CryptographicOperations.ZeroMemory(supplied);
            }

            /* The caller's copy is the clone; when no key was generated nobody receives it. */
            if (!generated && clone is not null)
            {
                CryptographicOperations.ZeroMemory(clone);
            }
        }
    }

    /// <summary>
    /// Renames the key to <c>password-key.&lt;suffix&gt;</c> (this platform's file name, then a dot and the suffix)
    /// and returns the path it now has. The key is the file at <see cref="QuarantineFileName"/> when there is one (kept
    /// from an open directory), otherwise the file at the live name; with files at both, the quarantined one is
    /// retired and the live one stays. NEVER deletes: the bytes stay, so a key that was retired by mistake can be
    /// put back by renaming it. A name already taken by an earlier retirement is never overwritten; a number is added
    /// to the suffix instead. Returns an empty string when there was no file to retire. Throws
    /// <see cref="IOException"/>, naming the directory and the file, when the rename fails, and
    /// <see cref="ArgumentException"/> for a suffix that is not letters, digits, <c>-</c> or <c>_</c>.
    /// </summary>
    public static string Retire(string directory, string suffix, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentException.ThrowIfNullOrEmpty(suffix);
        ArgumentNullException.ThrowIfNull(logger);
        if (!suffix.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
        {
            throw new ArgumentException("The suffix may hold only letters, digits, '-' and '_'.", nameof(suffix));
        }

        var path = Path.Combine(directory, FileName);
        lock (ComposeCredentialDirectoryGuard.Current.Gate)
        {
            var source = DarlingServiceKeyFile.AnythingAt(path + QuarantineSuffix) ? path + QuarantineSuffix : path;
            if (!DarlingServiceKeyFile.AnythingAt(source))
            {
                return string.Empty;
            }

            for (var attempt = 0; attempt < 1000; attempt++)
            {
                var retired = path + "." + suffix + (attempt == 0 ? string.Empty : "-" + attempt.ToString(System.Globalization.CultureInfo.InvariantCulture));
                if (DarlingServiceKeyFile.AnythingAt(retired))
                {
                    continue;
                }

                try
                {
                    MoveWithoutOverwrite(source, retired);
                    logger.LogWarning("The password key {Path} was retired to {Retired} (#5366). It is kept, not deleted.", source, retired);
                    return retired;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    if (DarlingServiceKeyFile.AnythingAt(retired))
                    {
                        continue; /* something took the name between the look and the move */
                    }

                    throw new IOException($"The password key {Path.GetFileName(source)} in {directory} could not be renamed to {Path.GetFileName(retired)} ({ex.Message}).", ex);
                }
            }
        }

        throw new IOException($"The password key {FileName} in {directory} could not be retired: every name for it with the suffix {suffix} is taken.");
    }

    /// <summary>
    /// Puts the key kept from an open directory back at the live name (#5366): renames <see cref="QuarantineFileName"/>
    /// to <see cref="FileName"/> with no overwrite, and returns the live path. The caller has judged the kept key
    /// (it matches what the service published) before it calls this. Throws <see cref="InvalidOperationException"/>,
    /// with a sentence for the operator, when the live name is already taken or no file is kept, and
    /// <see cref="IOException"/> when the rename itself fails. Nothing is deleted or overwritten.
    /// </summary>
    public static string Accept(string directory, ILogger logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(logger);

        var path = Path.Combine(directory, FileName);
        var kept = path + QuarantineSuffix;
        lock (ComposeCredentialDirectoryGuard.Current.Gate)
        {
            if (!DarlingServiceKeyFile.AnythingAt(kept))
            {
                throw new InvalidOperationException($"There is no kept password key to accept: {QuarantineFileName} is not in {directory}.");
            }

            if (DarlingServiceKeyFile.AnythingAt(path))
            {
                throw new InvalidOperationException($"The password key {FileName} in {directory} is already there, so {QuarantineFileName} was not moved onto it. Keep the right one and remove the other.");
            }

            try
            {
                MoveWithoutOverwrite(kept, path);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                throw new IOException($"The password key {QuarantineFileName} in {directory} could not be renamed to {FileName} ({ex.Message}).", ex);
            }
        }

        logger.LogWarning("The password key {Kept} was accepted and is now {Path} (#5366).", kept, path);
        return path;
    }

    /// <summary>A directory or a link at <paramref name="from"/> is moved as it is, never followed.</summary>
    private static void MoveWithoutOverwrite(string from, string to)
    {
        if (Directory.Exists(from) && new FileInfo(from).LinkTarget is null)
        {
            Directory.Move(from, to);
        }
        else
        {
            File.Move(from, to, overwrite: false);
        }
    }

    /// <summary>What a refusal says: the file and its directory, then the loader's reason. Never the key.</summary>
    private static string Refusal(ServiceKeyFileResult<byte[]> result) =>
        $"The password key {Path.GetFileName(result.Path)} in {Path.GetDirectoryName(result.Path)} cannot be used because {result.Reason ?? "it could not be checked"}.";

    private static ServiceKeyParse<byte[]> ParseKey(string encoded)
    {
        var buffer = new byte[encoded.Length * 3 / 4 + 3];
        try
        {
            if (!Convert.TryFromBase64String(encoded, buffer, out var written))
            {
                return new ServiceKeyParse<byte[]>(null, "it is not base64");
            }

            var pkcs8 = buffer.AsSpan(0, written).ToArray();
            if (CheckPkcs8(pkcs8) is { } problem)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
                return new ServiceKeyParse<byte[]>(null, problem);
            }

            return new ServiceKeyParse<byte[]>(pkcs8, null);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
        }
    }

    /// <summary>Null when <paramref name="pkcs8"/> is exactly one RSA private key of <see cref="KeyBits"/> bits; the
    /// reason otherwise.</summary>
    private static string? CheckPkcs8(byte[] pkcs8)
    {
        try
        {
            using var rsa = RSA.Create();
            rsa.ImportPkcs8PrivateKey(pkcs8, out var read);
            if (read != pkcs8.Length)
            {
                return "it holds more than one key";
            }

            return rsa.KeySize == KeyBits ? null : $"it is not a {KeyBits}-bit RSA key";
        }
        catch (Exception ex) when (ex is CryptographicException or ArgumentException)
        {
            return "it does not hold a PKCS#8 RSA private key";
        }
    }
}

/// <summary>What <see cref="DarlingPasswordKeyFile"/> found or did (#5366). Its generated <c>ToString</c> would print
/// <see cref="Pkcs8"/>'s type, not its bytes, but nothing should log this record whole.</summary>
/// <param name="Present">Whether anything is at the key file's path (or, for a key found after an open directory, at its
/// quarantine path). <c>Present</c> true with <c>Untrusted</c> false and <c>Pkcs8</c> null (the file cannot be read, parsed
/// or decrypted) is a refusal: the caller must treat it as one, never as a missing key to generate over or a key to
/// retire.</param>
/// <param name="Untrusted">True when the file, or the directory it is in, failed the trust check.</param>
/// <param name="Refusal">Why the key cannot be used, written for the operator, naming the file and its directory; null
/// when there is a key or no file.</param>
/// <param name="Pkcs8">The key (PKCS#8) when it was loaded or written; the caller's own copy, to zero when done.</param>
/// <param name="FoundAfterOpenDirectory">True when the file is the one kept from a directory that was open to other users'
/// writes until the service set it owner-only: it is at <see cref="DarlingPasswordKeyFile.QuarantineFileName"/> and stays
/// reported until it is accepted or retired, on every start. Every other credential file in it was removed.</param>
/// <param name="Path">The path the key was read from or looked for: the quarantine path when
/// <paramref name="FoundAfterOpenDirectory"/> is true and the file was read.</param>
public sealed record PasswordKeyFileLoad(
    bool Present, bool Untrusted, string? Refusal, byte[]? Pkcs8, bool FoundAfterOpenDirectory, string Path);
