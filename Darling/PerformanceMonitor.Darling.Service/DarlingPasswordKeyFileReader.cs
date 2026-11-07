/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace PerformanceMonitor.Darling.Service;

/// <summary>What the <c>passwordKey</c> file checks look at (#8006 review, request 1). Every field that cannot be told
/// on this platform is null, empty or false, and then decides nothing.</summary>
/// <param name="RegularFile">Whether the target, every link followed, is a regular file; null when that cannot be told
/// before it is opened (Windows tells it from the open handle).</param>
/// <param name="Length">Its size in bytes, when it is a file.</param>
/// <param name="Mode">Its Unix permission bits.</param>
/// <param name="Owner">Its Unix owner and link count.</param>
/// <param name="EffectiveUserId">The user id the service runs as, compared with <see cref="UnixFileOwner.UserId"/>.</param>
/// <param name="DirectoryModes">The Unix modes of the directory the path is written in and of the one the file really
/// is in.</param>
/// <param name="WritableByOrdinaryUsers">Windows: Users, Authenticated Users, Everyone or INTERACTIVE can write it.</param>
/// <param name="ReadableByOrdinaryUsers">Windows: one of those groups can read it.</param>
internal readonly record struct PasswordKeyFileFacts(
    bool? RegularFile, long? Length, UnixFileMode? Mode, UnixFileOwner? Owner, uint EffectiveUserId,
    IReadOnlyList<UnixFileMode> DirectoryModes, bool WritableByOrdinaryUsers, bool ReadableByOrdinaryUsers);

/// <summary>A check a <c>passwordKey</c> file failed that does not stop it loading: a log message template whose one
/// placeholder is <c>{Path}</c>, and the path. Never any of the file's contents.</summary>
internal sealed record PasswordKeyFileWarning(string Template, string Path)
{
    /// <summary>The template with the path in place, as <c>--validate-config</c> prints it.</summary>
    public string Sentence => Template.Replace("{Path}", Path, StringComparison.Ordinal);
}

/// <summary>What <see cref="DarlingPasswordKeyFileReader.Judge"/> decided: a refusal, or the warnings to load with.</summary>
internal readonly record struct PasswordKeyFileVerdict(string? Refusal, IReadOnlyList<PasswordKeyFileWarning> Warnings);

/// <summary>What <see cref="DarlingPasswordKeyFileReader.Read"/> found: the file's text, or a refusal; and the warnings
/// its checks raised.</summary>
internal readonly record struct PasswordKeyFileRead(string? Text, string? Refusal, IReadOnlyList<PasswordKeyFileWarning> Warnings);

/// <summary>
/// Reads a <c>passwordKey</c> <c>file:</c> key (#8006 review, request 1) with the checks the default key file gets
/// (<see cref="DarlingServiceKeyFile"/>), made to fit a secret the service did not write. What anyone could use to
/// swap the key, or what is no key file at all, is refused: a target that is not a regular file (checked before it is
/// opened, so a FIFO is never waited on), one larger than the key file's limit (refused before any of it is read, and
/// the read itself is never more than the limit plus one byte), and on Unix one its group or other users can write,
/// on Windows one ordinary users can write. What a mounted Kubernetes secret is by design (owned by root, 0644 by
/// default, reached through <c>..data</c>) only warns, naming the path and the check: another owner, a second name,
/// read access for others, and a directory others can write to that has no sticky bit.
/// </summary>
internal static class DarlingPasswordKeyFileReader
{
    /// <summary>The most a key file may hold: the default key file's limit.</summary>
    internal const int MaxBytes = DarlingPasswordKeyFile.MaxKeyFileBytes;

    internal const string OwnerTemplate =
        "passwordKey: file '{Path}' is owned by another user than the one the service runs as, so that user can replace " +
        "the key. It is loaded anyway, as a mounted Kubernetes secret (owned by root) is; to clear this warning, give the " +
        "file to the service's user.";

    internal const string LinksTemplate =
        "passwordKey: file '{Path}' has more than one name (a hard link), so the key can be changed through a path this " +
        "check does not see. It is loaded anyway; to clear this warning, remove the other names.";

    internal const string OthersReadTemplate =
        "passwordKey: file '{Path}' is readable by other users (its mode grants read to others), so any local account can " +
        "read the key. It is loaded anyway; to clear this warning, remove read for others (chmod o-r, or defaultMode 0440 " +
        "for a Kubernetes secret).";

    internal const string OpenDirectoryTemplate =
        "passwordKey: file '{Path}' is in a directory that other users can write to and that has no sticky bit, so they " +
        "can replace the key. It is loaded anyway; to clear this warning, remove their write access to the directory.";

    internal const string OrdinaryUsersReadTemplate =
        "passwordKey: file '{Path}' is readable by Users, Authenticated Users, Everyone or INTERACTIVE, so any local " +
        "account can read the key. It is loaded anyway; to clear this warning, restrict it to SYSTEM, Administrators and " +
        "the service account.";

    private const UnixFileMode GroupOrOtherWrite = UnixFileMode.GroupWrite | UnixFileMode.OtherWrite;

    /// <summary>
    /// Decides on <paramref name="facts"/> alone: the first refusal that applies, or every warning that does. A refusal
    /// starts <c>passwordKey: file '&lt;path&gt;'</c>; a warning is a template on the same path. Neither names any
    /// contents.
    /// </summary>
    internal static PasswordKeyFileVerdict Judge(string path, PasswordKeyFileFacts facts)
    {
        var target = $"{DarlingPasswordKeySource.SettingName}: file '{path}'";
        if (facts.RegularFile == false)
        {
            return Refuse(NotRegular(target));
        }

        if (facts.Length > MaxBytes)
        {
            return Refuse(TooLarge(target));
        }

        if (facts.Mode is { } mode && (mode & GroupOrOtherWrite) != 0)
        {
            return Refuse(
                $"{target} can be written by its group or by other users (mode {Octal(mode)}), so they could replace the " +
                "key. Remove those write bits (chmod go-w) and restart.");
        }

        if (facts.WritableByOrdinaryUsers)
        {
            return Refuse(
                $"{target} can be written by Users, Authenticated Users, Everyone or INTERACTIVE, so any local account " +
                "could replace the key. Remove their write access and restart.");
        }

        var warnings = new List<PasswordKeyFileWarning>();
        if (facts.Owner is { } owner)
        {
            if (owner.UserId != facts.EffectiveUserId)
            {
                warnings.Add(new PasswordKeyFileWarning(OwnerTemplate, path));
            }

            if (owner.Links > 1)
            {
                warnings.Add(new PasswordKeyFileWarning(LinksTemplate, path));
            }
        }

        if (facts.Mode is { } read && (read & UnixFileMode.OtherRead) != 0)
        {
            warnings.Add(new PasswordKeyFileWarning(OthersReadTemplate, path));
        }

        foreach (var directory in facts.DirectoryModes)
        {
            if ((directory & UnixFileMode.OtherWrite) != 0 && (directory & UnixFileMode.StickyBit) == 0)
            {
                warnings.Add(new PasswordKeyFileWarning(OpenDirectoryTemplate, path));
                break;
            }
        }

        if (facts.ReadableByOrdinaryUsers)
        {
            warnings.Add(new PasswordKeyFileWarning(OrdinaryUsersReadTemplate, path));
        }

        return new PasswordKeyFileVerdict(null, warnings);
    }

    /// <summary>
    /// Checks and reads the key file <paramref name="path"/> names, as written in the setting. Never throws for what
    /// the file system or the file can do: a file that cannot be opened or read is a refusal naming the path, as
    /// <see cref="DarlingSecretSource.Resolve"/> words it. The text is the caller's to parse; the bytes it was decoded
    /// from are zeroed.
    /// </summary>
    internal static PasswordKeyFileRead Read(string path)
    {
        byte[]? buffer = null;
        try
        {
            var verdict = Judge(path, Gather(path));
            if (verdict.Refusal is not null)
            {
                return new PasswordKeyFileRead(null, verdict.Refusal, []);
            }

            var target = $"{DarlingPasswordKeySource.SettingName}: file '{path}'";
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1);

            /* Windows tells a device or a pipe from a file only by the open handle; opening one does not wait. */
            if (OperatingSystem.IsWindows() && !IsDiskFile(stream.SafeFileHandle))
            {
                return new PasswordKeyFileRead(null, NotRegular(target), []);
            }

            if (stream.CanSeek && stream.Length > MaxBytes)
            {
                return new PasswordKeyFileRead(null, TooLarge(target), []);
            }

            buffer = new byte[MaxBytes + 1];
            var length = DarlingServiceKeyFile.ReadBounded(stream, buffer);
            if (length > MaxBytes)
            {
                return new PasswordKeyFileRead(null, TooLarge(target), []);
            }

            using var reader = new StreamReader(new MemoryStream(buffer, 0, length), Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            return new PasswordKeyFileRead(reader.ReadToEnd(), null, verdict.Warnings);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return new PasswordKeyFileRead(
                null, $"{DarlingPasswordKeySource.SettingName}: secret file '{path}' could not be read: {ex.Message}", []);
        }
        finally
        {
            if (buffer is not null)
            {
                CryptographicOperations.ZeroMemory(buffer);
            }
        }
    }

    /// <summary>
    /// The facts <see cref="Judge"/> decides on, for the file <paramref name="path"/> names, every link followed. On Unix
    /// (and under a test's stand-in) through <see cref="ComposeCredentialDirectoryGuard"/>'s mode calls, which the
    /// default key file's owner check uses too; on Windows from the file's ACL. A target that is not there gives facts
    /// that decide nothing, so the open reports it.
    /// </summary>
    private static PasswordKeyFileFacts Gather(string path)
    {
        var full = Path.GetFullPath(path);
        var real = DarlingOwnedSecrets.RealPath(full) ?? full;
        if (ComposeCredentialDirectoryGuard.Current.UnixModes is { } modes)
        {
            var regular = modes.IsRegularFile(real);
            if (regular == false || !File.Exists(real))
            {
                return new PasswordKeyFileFacts(regular ?? (Directory.Exists(real) ? false : null), null, null, null, 0, [], false, false);
            }

            var directories = new List<UnixFileMode>();
            foreach (var directory in new[] { Path.GetDirectoryName(full), Path.GetDirectoryName(real) })
            {
                if (!string.IsNullOrEmpty(directory))
                {
                    directories.Add(modes.Get(directory));
                }
            }

            return new PasswordKeyFileFacts(
                regular, new FileInfo(real).Length, modes.Get(real), modes.OwnerOf(real), modes.EffectiveUserId, directories, false, false);
        }

        if (Directory.Exists(real))
        {
            return new PasswordKeyFileFacts(false, null, null, null, 0, [], false, false);
        }

        if (!File.Exists(real))
        {
            return new PasswordKeyFileFacts(null, null, null, null, 0, [], false, false);
        }

        return new PasswordKeyFileFacts(
            null, new FileInfo(real).Length, null, null, 0, [],
            OperatingSystem.IsWindows() && DarlingFileSecurity.IsWritableByOrdinaryUsers(real),
            OperatingSystem.IsWindows() && DarlingFileSecurity.IsReadableByOrdinaryUsers(real));
    }

    private static string NotRegular(string target) =>
        $"{target} is not a regular file (it is a directory, a device, a FIFO or a socket), so it is not read. Point " +
        $"{DarlingPasswordKeySource.SettingName} at the key file itself.";

    private static string TooLarge(string target) =>
        $"{target} is larger than {MaxBytes.ToString(CultureInfo.InvariantCulture)} bytes, which no password key is, so it " +
        $"is not read. Point {DarlingPasswordKeySource.SettingName} at the key file itself.";

    private static string Octal(UnixFileMode mode) => Convert.ToString((int)mode, 8);

    private static PasswordKeyFileVerdict Refuse(string refusal) => new(refusal, []);

    private const uint FileTypeDisk = 1;

    private static bool IsDiskFile(SafeFileHandle handle) => GetFileType(handle) == FileTypeDisk;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint GetFileType(SafeFileHandle file);
}
