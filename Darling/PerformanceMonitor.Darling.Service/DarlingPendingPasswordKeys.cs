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
using System.Linq;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Where the MCP <c>new_password_key</c> tool writes a new password key (#9644): one folder the service owns,
/// <c>pending-password-keys</c>, inside the folder that holds the service's active password key file, with a file name
/// the service makes (<c>new-password-key-&lt;fingerprint&gt;.key</c>). The caller names no path. The folder is created
/// with its owner-only mode (Unix) or hardened ACL (Windows) at creation, never locked afterwards, and a folder that is
/// already there is used only when it is a real folder, owned by the service's account, that nobody else can read or change. One
/// that is not is refused and left as it is. A credentials folder on a network location is refused before anything is made.
/// On Windows the folders from the volume root down to the pending folder are held open without following a link from the
/// check until the key file is made, so the folder written into is the folder checked.
/// </summary>
internal static class DarlingPendingPasswordKeys
{
    internal const string DirectoryName = "pending-password-keys";
    internal const string FilePrefix = "new-password-key-";
    internal const string FileExtension = ".key";

    /// <summary>Pending key files at which the next one is refused, so keys that were never installed or deleted do not pile up.</summary>
    internal const int MaxPending = 5;

    private const UnixFileMode GroupOrOtherAccess =
        UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute
        | UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

    /// <summary>What <see cref="Create"/> did: <see cref="Written"/> with the file's <see cref="FilePath"/> and the key's
    /// <see cref="Fingerprint"/>, or not, and <see cref="Message"/> says why. Never the key.</summary>
    internal sealed record PendingKeyOutcome(bool Written, string Message, string? FilePath, string? Fingerprint, bool Failed = false);

    /// <summary>The pending folder for a service whose active password key file is in <paramref name="credentialsDirectory"/>: a
    /// folder inside it, so it is under the same owner and access as the credentials, and on the same volume.</summary>
    internal static string DirectoryFor(string credentialsDirectory) =>
        Path.Combine(Path.TrimEndingDirectorySeparator(Path.GetFullPath(credentialsDirectory)), DirectoryName);

    /// <summary>True when <paramref name="fullPath"/> is on a network location: a UNC path, or a drive that is mapped to a share.
    /// Windows only, because only there is a pair of backslashes a share.</summary>
    internal static bool IsNetworkLocation(string fullPath)
    {
        if (!OperatingSystem.IsWindows())
        {
            return false;
        }

        /* #9644: a device-style path (the question-mark or dot prefix) reaches a share through its UNC folder or through a device such as
           GLOBALROOT\Device\Mup, and the no-follow open of the folder fails on those with an exception no caller expects. Only the
           drive-letter form is a local path this check can follow, so every other one is refused here, before the folder is made. */
        if (fullPath.StartsWith(@"\\?\", StringComparison.Ordinal) || fullPath.StartsWith(@"\\.\", StringComparison.Ordinal))
        {
            var afterPrefix = fullPath[4..];
            if (afterPrefix.Length < 2 || !char.IsAsciiLetter(afterPrefix[0]) || afterPrefix[1] != ':'
                || (afterPrefix.Length > 2 && afterPrefix[2] != '\\'))
            {
                return true;
            }

            fullPath = afterPrefix;
        }
        else if (fullPath.StartsWith(@"\\", StringComparison.Ordinal))
        {
            return true;
        }

        try
        {
            return Path.GetPathRoot(fullPath) is { Length: > 0 } root && new DriveInfo(root).DriveType == DriveType.Network;
        }
        catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    /// <summary>
    /// Makes a new password key in the pending folder. <paramref name="credentialsDirectory"/> is where the service keeps its
    /// active key file, or null when it cannot be resolved. <paramref name="keyFromSetting"/> is true when darling.json sets
    /// <c>passwordKey</c>, so the active key is not a file the service keeps. Both of those, and a service that has no key file yet,
    /// are refused: there is then no folder the service owns to put a key in, and nothing falls back to another place.
    /// </summary>
    internal static PendingKeyOutcome Create(string? credentialsDirectory, bool keyFromSetting, ILogger? logger) =>
        Create(credentialsDirectory, keyFromSetting, logger, afterFolderChecked: null);

    /// <summary><see cref="Create(string?, bool, ILogger?)"/> that calls <paramref name="afterFolderChecked"/> with the pending folder
    /// once it has passed its checks and before the key file is made, so a test can try to swap the folder at the moment that matters.</summary>
    internal static PendingKeyOutcome Create(string? credentialsDirectory, bool keyFromSetting, ILogger? logger, Action<string>? afterFolderChecked)
    {
        if (keyFromSetting)
        {
            return Refused("The service's password key comes from the passwordKey setting, not from a key file the service keeps, so there is no folder of the service's own to put a new key in. Make one with the --new-password-key command on the service's machine and a path you choose.");
        }

        if (credentialsDirectory is null)
        {
            return Refused("The folder where the service keeps its password key could not be found, so there is no folder of the service's own to put a new key in.");
        }

        IDisposable? pin = null;
        try
        {
            var credentialsFull = Path.TrimEndingDirectorySeparator(Path.GetFullPath(credentialsDirectory));
            if (IsNetworkLocation(credentialsFull))
            {
                return Refused($"The folder where the service keeps its password key, {credentialsFull}, is on a network location, so no folder for new keys is made there. Nothing was written. Whoever can reach the share could read or change a key kept on it.");
            }

            if (!DarlingServiceKeyFile.AnythingAt(Path.Combine(credentialsDirectory, DarlingPasswordKeyFile.FileName)))
            {
                return Refused("The service keeps no password key file yet, so there is no folder of the service's own to put a new key in. Try again once the service has started with its store.");
            }

            var directory = DirectoryFor(credentialsDirectory);
            if (EnsureFolder(directory, out pin) is { } problem)
            {
                logger?.LogWarning("The pending password key folder was not used: {Reason}", problem);
                return Refused($"The folder for new password keys, {directory}, was not used: {problem}. Nothing was written. Remove it or fix it, then try again.");
            }

            var pending = Directory.EnumerateFileSystemEntries(directory, FilePrefix + "*" + FileExtension).Count();
            if (pending >= MaxPending)
            {
                return Refused($"{pending.ToString(CultureInfo.InvariantCulture)} new password key files are already waiting in {directory}. Install the one you want, or delete the ones you do not, then try again.");
            }

            afterFolderChecked?.Invoke(directory);
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);
            var write = DarlingCliCommands.WriteNewPasswordKey(
                fixedPath: null,
                keyId => Path.Combine(directory, FilePrefix + PasswordSeal.DisplayKeyId(keyId) + FileExtension),
                output, error);
            return write.ExitCode == 0
                ? new PendingKeyOutcome(true, output.ToString().Trim(), write.Path, write.Fingerprint)
                : new PendingKeyOutcome(false, error.ToString().Trim(), null, null, Failed: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException or InvalidOperationException)
        {
            logger?.LogWarning("A new password key was not written: {Message}", ex.Message);
            return new PendingKeyOutcome(false, "The folder for new password keys could not be checked or written. The service log has the reason.", null, null, Failed: true);
        }
        finally
        {
            pin?.Dispose();
        }
    }

    private static PendingKeyOutcome Refused(string message) => new(false, message, null, null);

    /// <summary>Creates the folder owner-only if it is not there, then checks it. Null when the folder is a real folder only the
    /// service's account can reach, otherwise why not. An existing folder is judged, never repaired.</summary>
    private static string? EnsureFolder(string directory, out IDisposable? pin)
    {
        pin = null;
        var info = new DirectoryInfo(directory);
        if (info.LinkTarget is not null)
        {
            return "it is a link";
        }

        if (!info.Exists)
        {
            if (File.Exists(directory))
            {
                return "a file is there, not a folder";
            }

            var parent = Path.GetDirectoryName(directory);
            if (parent is null || !Directory.Exists(parent))
            {
                return "its parent folder does not exist";
            }

            if (ComposeCredentialDirectoryGuard.Current.UnixModes is { } createModes)
            {
                createModes.CreateOwnerOnly(directory);
            }
            else if (OperatingSystem.IsWindows())
            {
                DarlingFileSecurity.CreateHardenedDirectory(directory);
            }
            else
            {
                return "this platform has no way to make it owner-only";
            }

            info.Refresh();
        }

        if (info.LinkTarget is not null || !info.Exists)
        {
            return "it is not a real folder";
        }

        if (ComposeCredentialDirectoryGuard.Current.UnixModes is { } modes)
        {
            if (modes.OwnerOf(directory) is { } owner && owner.UserId != modes.EffectiveUserId)
            {
                return $"it is owned by user {owner.UserId.ToString(CultureInfo.InvariantCulture)}, not by user {modes.EffectiveUserId.ToString(CultureInfo.InvariantCulture)}, which the service runs as";
            }

            var mode = modes.Get(directory);
            return (mode & GroupOrOtherAccess) != 0
                ? $"its mode is {Convert.ToString((int)mode, 8)}, so other users can reach it"
                : null;
        }

        if (!OperatingSystem.IsWindows())
        {
            return "this platform has no way to check it";
        }

        /* Held from here until the key file is made: the folders down to this one are opened without following a link, and
           none can be renamed, deleted or replaced while they are held, so what is checked below is what is written into. */
        var held = DarlingFileSecurity.PinFolders(directory, out var chainProblem);
        if (held is null)
        {
            return chainProblem;
        }

        pin = held;
        if (!DarlingFileSecurity.IsTrustedOwner(directory))
        {
            return "it is not owned by SYSTEM, Administrators or the service account";
        }

        return DarlingFileSecurity.DirectoryAccessBeyondTrusted(directory) is { } accounts
            ? $"accounts beyond SYSTEM, Administrators and the service account have access to it: {accounts}"
            : null;
    }
}
