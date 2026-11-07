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
using System.Runtime.Versioning;
using System.Security.AccessControl;
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
/// <param name="UntrustedWindowsOwner">Windows: its owner is not SYSTEM, Administrators or the service account.</param>
/// <param name="UntrustedWindowsFolder">Windows: the folder the path is written in, or the one the file really is in,
/// that fails the default key file's folder check; null when neither does.</param>
/// <param name="UnresolvedWindowsPath">Windows: the file opens through links that cannot all be followed to it, so the
/// folder it really is in is not known.</param>
internal readonly record struct PasswordKeyFileFacts(
    bool? RegularFile, long? Length, UnixFileMode? Mode, UnixFileOwner? Owner, uint EffectiveUserId,
    IReadOnlyList<UnixFileMode> DirectoryModes, bool WritableByOrdinaryUsers, bool ReadableByOrdinaryUsers,
    bool UntrustedWindowsOwner = false, PasswordKeyFolderProblem? UntrustedWindowsFolder = null,
    bool UnresolvedWindowsPath = false);

/// <summary>A folder a Windows <c>passwordKey</c> file is in that fails the default key file's folder check
/// (<see cref="DarlingFileSecurity.WriteAccessBeyondTrusted(string)"/>): <paramref name="Writers"/> are the accounts
/// beyond SYSTEM, Administrators and the service account that can write to it, delete from it or change who may, as that
/// check names them (an owner among them marked "(the owner)"); or, when its ACL cannot be read,
/// <paramref name="Unreadable"/> says why. One of the two is set.</summary>
internal readonly record struct PasswordKeyFolderProblem(string Folder, string? Writers, string? Unreadable);

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
/// opened, both where its links lead by their text and where the kernel's own links lead, so a FIFO or a pipe behind
/// <c>/dev/stdin</c> is never waited on), one larger than the key file's limit (refused before any of it is read, and
/// the read itself is never more than the limit plus one byte), and on Unix one its group or other users can write,
/// on Windows one ordinary users can write, or one in a folder that accounts beyond SYSTEM, Administrators and the
/// service account can change: the default key file's folder rule, held to both the folder the path is written in and
/// the folder the file really is in (and a file whose real path cannot be followed is refused, as that folder is then
/// unknown). What a mounted Kubernetes secret can be by design (0644 by default, reached
/// through <c>..data</c>) only warns, naming the path and the check: on Unix an owner that is neither root nor the
/// service's user, a second name, read access for others, and a directory its group or others can write to that has no
/// sticky bit; on Windows read access for ordinary users, and an owner that is not SYSTEM, Administrators or the service
/// account. A root-owned file gives no owner warning: root can replace any file anyway, and every Kubernetes or Swarm
/// secret is root-owned.
/// </summary>
internal static class DarlingPasswordKeyFileReader
{
    /// <summary>The most a key file may hold: the default key file's limit.</summary>
    internal const int MaxBytes = DarlingPasswordKeyFile.MaxKeyFileBytes;

    internal const string OwnerTemplate =
        "passwordKey: file '{Path}' is owned by a user that is neither root nor the user the service runs as, so that " +
        "user can replace the key. It is loaded anyway; to clear this warning, give the file to root or to the service's " +
        "user.";

    internal const string WindowsOwnerTemplate =
        "passwordKey: file '{Path}' is owned by an account other than SYSTEM, Administrators or the service account, " +
        "and a file's owner can change who may write it, so that account can replace the key. It is loaded anyway; to " +
        "clear this warning, give the file to Administrators (icacls <file> /setowner *S-1-5-32-544).";

    internal const string LinksTemplate =
        "passwordKey: file '{Path}' has more than one name (a hard link), so the key can be changed through a path this " +
        "check does not see. It is loaded anyway; to clear this warning, remove the other names.";

    internal const string OthersReadTemplate =
        "passwordKey: file '{Path}' is readable by other users (its mode grants read to others), so any local account can " +
        "read the key. It is loaded anyway; to clear this warning, remove read for others (chmod o-r, or defaultMode 0440 " +
        "for a Kubernetes secret).";

    internal const string OpenDirectoryTemplate =
        "passwordKey: file '{Path}' is in a directory that its group or other users can write to and that has no sticky " +
        "bit, so they can replace the key. It is loaded anyway; to clear this warning, remove their write access to the " +
        "directory.";

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

        /* Windows follows more links than the real path does: a file reached through more of them would leave the
           folder it really is in unjudged, so it is refused rather than judged by the folder as written alone. */
        if (facts.UnresolvedWindowsPath)
        {
            return Refuse(
                $"{target} is not read, because the links on its path could not all be followed to the file (more than 40 " +
                "of them, or one whose target could not be read), so the folder it really is in cannot be checked. Point " +
                $"{DarlingPasswordKeySource.SettingName} at the key file itself and restart.");
        }

        /* #8006 review: the default key file's folder rule, and as strict: on Windows no secret mount makes a folder look
           open by design, as a Kubernetes one does on Linux. Before the file's own ACL, because locking the folder down
           also fixes a file that inherits from it. */
        if (facts.UntrustedWindowsFolder is { } folder)
        {
            return Refuse($"{target} is not read, because {FolderProblem(folder)} {FolderRemedy(folder)}");
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
            /* Root is trusted like the service's own user: it can replace any file whoever owns it, so a root-owned key
               (every Kubernetes or Swarm secret) adds no account that could change it, and the warning's advice could
               not be followed there. The default key file keeps its own stricter rule. */
            if (owner.UserId != 0 && owner.UserId != facts.EffectiveUserId)
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

        /* A group that can write the directory can swap the key as surely as others can. Only a warning, not a refusal:
           a disk emptyDir is 0777 with no sticky bit, so a key an init container copies there would be refused. */
        foreach (var directory in facts.DirectoryModes)
        {
            if ((directory & GroupOrOtherWrite) != 0 && (directory & UnixFileMode.StickyBit) == 0)
            {
                warnings.Add(new PasswordKeyFileWarning(OpenDirectoryTemplate, path));
                break;
            }
        }

        if (facts.ReadableByOrdinaryUsers)
        {
            warnings.Add(new PasswordKeyFileWarning(OrdinaryUsersReadTemplate, path));
        }

        if (facts.UntrustedWindowsOwner)
        {
            warnings.Add(new PasswordKeyFileWarning(WindowsOwnerTemplate, path));
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
    /// default key file's owner check uses too; on Windows from the file's ACL and owner, and from its folders' ACLs
    /// (<see cref="UntrustedFolder"/>). A target that is not there gives facts that decide nothing, so the open reports
    /// it.
    /// </summary>
    private static PasswordKeyFileFacts Gather(string path)
    {
        var full = Path.GetFullPath(path);
        var resolved = DarlingOwnedSecrets.RealPath(full);
        var real = resolved ?? full;
        if (ComposeCredentialDirectoryGuard.Current.UnixModes is { } modes)
        {
            var regular = modes.IsRegularFile(real);

            /* The real path follows links by their text. A /proc fd link (file:/dev/stdin, file:/proc/self/fd/0) reads as
               "pipe:[N]", so the real path names nothing and its type cannot be told, while the open goes through the
               kernel's own link to the pipe and waits for its writer. The path as written, every link followed by the
               kernel, is asked too, and either answer of "not a regular file" refuses. */
            if (modes.IsRegularFile(full) == false)
            {
                regular = false;
            }

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

        /* Windows follows up to 63 links on a path, and the real path stops after 40 or at a link it cannot read. A file
           that still opens by the path as written is then in a folder no check can name. With nothing there either, the
           open reports what is missing. */
        if (resolved is null || !File.Exists(real))
        {
            return File.Exists(full)
                ? new PasswordKeyFileFacts(null, null, null, null, 0, [], false, false, UnresolvedWindowsPath: true)
                : new PasswordKeyFileFacts(null, null, null, null, 0, [], false, false);
        }

        return new PasswordKeyFileFacts(
            null, new FileInfo(real).Length, null, null, 0, [],
            OperatingSystem.IsWindows() && DarlingFileSecurity.IsWritableByOrdinaryUsers(real),
            OperatingSystem.IsWindows() && DarlingFileSecurity.IsReadableByOrdinaryUsers(real),
            /* A Windows file's owner holds WRITE_DAC whatever the ACL says, so it can grant itself write; SYSTEM,
               Administrators and the service account stand where root and the service's user do on Unix. */
            OperatingSystem.IsWindows() && !DarlingFileSecurity.IsTrustedOwner(real),
            OperatingSystem.IsWindows() ? UntrustedFolder(full, real) : null);
    }

    /// <summary>
    /// Windows (#8006 review): the first of the folder <paramref name="full"/> is written in and the folder
    /// <paramref name="real"/> is in that fails <see cref="DarlingFileSecurity.WriteAccessBeyondTrusted(string)"/>, the
    /// check the default key file's folder gets: accounts beyond SYSTEM, Administrators and the service account can write
    /// to it, delete from it or change who may, or its ACL cannot be read. Null when neither fails. Whoever can change the
    /// folder the file is in can put another file in its place. The folder as written is judged by the ACL read at its own
    /// name, which for a junction is the junction's own: whoever can change that can point the path at another folder.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static PasswordKeyFolderProblem? UntrustedFolder(string full, string real)
    {
        string? judged = null;
        foreach (var folder in new[] { Path.GetDirectoryName(full), Path.GetDirectoryName(real) })
        {
            if (string.IsNullOrEmpty(folder) || string.Equals(folder, judged, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            judged = folder;

            /* That check's own two steps, the read and the judging, taken apart so that an ACL the service cannot read
               gets a sentence of its own instead of standing where the accounts would. */
            DirectorySecurity security;
            try
            {
                security = new DirectoryInfo(folder).GetAccessControl();
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException)
            {
                return new PasswordKeyFolderProblem(folder, null, ex.Message);
            }

            if (DarlingFileSecurity.WriteAccessBeyondTrusted(security) is { } writers)
            {
                return new PasswordKeyFolderProblem(folder, writers, null);
            }
        }

        return null;
    }

    private static string NotRegular(string target) =>
        $"{target} is not a regular file (it is a directory, a device, a FIFO or a socket), so it is not read. Point " +
        $"{DarlingPasswordKeySource.SettingName} at the key file itself.";

    private static string FolderProblem(PasswordKeyFolderProblem folder) =>
        folder.Writers is { } writers
            ? $"its folder {folder.Folder} is not trusted. Accounts beyond SYSTEM, Administrators and the service account " +
              $"can write to it or change what is in it: {writers}."
            : $"its folder {folder.Folder} cannot be checked: the service cannot read its permissions ({folder.Unreadable}), " +
              "so it cannot tell who could replace the key.";

    /* #7363's rule for the store folder holds here too: a drive root is never re-owned or re-permissioned, since that would
       change every file and folder on the drive. */
    private static string FolderRemedy(PasswordKeyFolderProblem folder) =>
        DarlingManagedPostgres.IsDriveRoot(folder.Folder)
            ? "A drive root is not locked down for a key, because that would change the permissions of every file and folder " +
              "on the drive: move the key into a folder of its own, lock that folder down, and restart."
            : folder.Writers is not null
                ? "Remove their access, give the folder to Administrators if one of them owns it (icacls " +
                  $"\"{folder.Folder}\" /setowner *S-1-5-32-544), and restart."
                : "Give the service's account read on the folder (RX, inherited by what is in it) and restart.";

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
