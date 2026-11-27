/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.AccessControl;
using System.Security.Principal;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The pending password key folder (#9644): where it is made, the checks on a folder that is already there, and that the folder written
/// into is the folder checked. The Windows rows run only on Windows, where the folder's owner and ACL are what is judged.
/// </summary>
public sealed class DarlingPendingPasswordKeysTests
{
    private static (string Root, string Credentials, string Pending) Service()
    {
        var root = Path.Combine(Path.GetTempPath(), "pmpend9659b-" + Guid.NewGuid().ToString("N")[..12]);
        var credentials = Path.Combine(root, "credentials");
        Directory.CreateDirectory(credentials);
        File.WriteAllText(Path.Combine(credentials, DarlingPasswordKeyFile.FileName), "placeholder");
        return (root, credentials, Path.Combine(credentials, DarlingPendingPasswordKeys.DirectoryName));
    }

    private static SecurityIdentifier Users => new(WellKnownSidType.BuiltinUsersSid, null);

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static void Grant(string directory, FileSystemRights rights, InheritanceFlags inheritance)
    {
        var info = new DirectoryInfo(directory);
        var security = info.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(Users, rights, inheritance, PropagationFlags.None, AccessControlType.Allow));
        info.SetAccessControl(security);
    }

    [Fact]
    public void Create_RefusesACredentialsFolderOnANetworkShare_BeforeMakingAnything()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "a share is a Windows path form");

        var outcome = DarlingPendingPasswordKeys.Create(@"\\pm9659b-no-such-host.invalid\share\credentials", keyFromSetting: false, logger: null);

        Assert.False(outcome.Written);
        Assert.False(outcome.Failed);
        Assert.Contains("network location", outcome.Message, StringComparison.Ordinal);
        Assert.Null(outcome.FilePath);
    }

    /* #9644: the device-style forms of a share. The dot-prefixed UNC form was exempt from the check, and a GLOBALROOT path
       ended in an exception from the folder open. Both are refused as a network location before the folder is made. */
    [Theory]
    [InlineData(@"\\.\UNC\localhost\C$\pm9659d-no-such-folder\credentials")]
    [InlineData(@"\\?\UNC\localhost\C$\pm9659d-no-such-folder\credentials")]
    [InlineData(@"\\?\GLOBALROOT\Device\Mup\localhost\C$\pm9659d-no-such-folder\credentials")]
    public void Create_RefusesADeviceStylePathToAShare_BeforeMakingAnything(string credentials)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "a share is a Windows path form");

        var outcome = DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null);

        Assert.False(outcome.Written);
        Assert.False(outcome.Failed);
        Assert.Contains("network location", outcome.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void IsNetworkLocation_TakesTheDriveLetterDevicePathAsLocal_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "a share is a Windows path form");

        var local = Path.GetFullPath(Path.GetTempPath());

        Assert.False(DarlingPendingPasswordKeys.IsNetworkLocation(@"\\?\" + local));
        Assert.False(DarlingPendingPasswordKeys.IsNetworkLocation(@"\\.\" + local));
    }

    [Fact]
    public void Create_NamesAFolderItCannotOpenInItsRefusal_InsteadOfThrowing_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the folder open is Windows's");
        var (root, credentials, pending) = Service();
        Microsoft.Win32.SafeHandles.SafeFileHandle? held = null;
        try
        {
            Assert.True(DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null).Written);
            foreach (var file in Directory.GetFiles(pending))
            {
                File.Delete(file);
            }

            // Another process holds the folder with no sharing, so the open that pins it fails with a sharing violation. An access entry
            // that denies the account cannot stand in for this, because an administrator's folder open passes through it.
            held = CreateFileW(pending, 0x00000001 | 0x00000080, FileShare.None, IntPtr.Zero, FileMode.Open, 0x02000000, IntPtr.Zero);
            Assert.False(held.IsInvalid);

            var outcome = DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null);

            Assert.False(outcome.Written);
            Assert.False(outcome.Failed);
            Assert.Contains(pending, outcome.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(outcome.FilePath);
        }
        finally
        {
            held?.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern Microsoft.Win32.SafeHandles.SafeFileHandle CreateFileW(
        string lpFileName, uint dwDesiredAccess, FileShare dwShareMode, IntPtr lpSecurityAttributes,
        FileMode dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [Fact]
    public void Create_RefusesAnExistingFolderThatUsersMayOnlyRead_AndWritesNothing_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the folder ACL is Windows's");
        var (root, credentials, pending) = Service();
        try
        {
            // Start from the folder the tool makes, which only the trusted accounts can reach, and add one read-only entry.
            Assert.True(DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null).Written);
            foreach (var file in Directory.GetFiles(pending))
            {
                File.Delete(file);
            }

            Grant(pending, FileSystemRights.ReadAndExecute, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit);
            Assert.Null(DarlingFileSecurity.WriteAccessBeyondTrusted(pending));

            var outcome = DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null);

            Assert.False(outcome.Written);
            Assert.Contains("have access to it", outcome.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.GetFileSystemEntries(pending));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Create_RefusesAnExistingFolderOwnedByAnotherAccount_AndWritesNothing_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the folder owner is Windows's");
        var (root, credentials, pending) = Service();
        try
        {
            Assert.True(DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null).Written);
            foreach (var file in Directory.GetFiles(pending))
            {
                File.Delete(file);
            }

            var info = new DirectoryInfo(pending);
            try
            {
                var security = info.GetAccessControl();
                security.SetOwner(Users);
                info.SetAccessControl(security);
            }
            catch (Exception ex) when (ex is InvalidOperationException or UnauthorizedAccessException or IOException)
            {
                Assert.Skip("this account may not hand a folder to another owner: " + ex.Message);
            }

            Assert.Null(DarlingFileSecurity.DirectoryAccessBeyondTrusted(pending));
            var outcome = DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null);

            Assert.False(outcome.Written);
            Assert.Contains("not owned by", outcome.Message, StringComparison.Ordinal);
            Assert.Empty(Directory.GetFileSystemEntries(pending));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Create_MakesTheFolderAndTheKeyFileWithNoAccessBeyondTheTrustedThree_EvenWhereTheCredentialsFolderGrantsMore_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the folder ACL is Windows's");
        var (root, credentials, pending) = Service();
        try
        {
            // What a plain create would inherit.
            Grant(credentials, FileSystemRights.Modify, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit);

            var outcome = DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null);

            Assert.True(outcome.Written, outcome.Message);
            Assert.Null(DarlingFileSecurity.DirectoryAccessBeyondTrusted(pending));
            Assert.Null(DarlingFileSecurity.AccessBeyondTrusted(outcome.FilePath!));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Create_WritesIntoTheFolderItChecked_WhenTheFolderIsSwappedForAJunctionAfterTheCheck_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "a junction is a Windows link");
        var (root, credentials, pending) = Service();
        var elsewhere = Path.Combine(root, "elsewhere");
        Directory.CreateDirectory(elsewhere);
        var swapped = false;
        try
        {
            var outcome = DarlingPendingPasswordKeys.Create(credentials, keyFromSetting: false, logger: null, afterFolderChecked: folder =>
            {
                var moved = false;
                try
                {
                    Directory.Move(folder, folder + "-moved");
                    moved = true;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    Assert.NotNull(ex.Message);
                }

                if (!moved)
                {
                    return;
                }

                using var mklink = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = $"/c mklink /J \"{folder}\" \"{elsewhere}\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                })!;
                mklink.WaitForExit();
                swapped = mklink.ExitCode == 0;
            });

            Assert.False(swapped, "the checked folder could be moved aside and replaced after its check");
            Assert.True(outcome.Written, outcome.Message);
            Assert.StartsWith(pending + Path.DirectorySeparatorChar, outcome.FilePath, StringComparison.Ordinal);
            Assert.Empty(Directory.GetFileSystemEntries(elsewhere));
        }
        finally
        {
            if (new DirectoryInfo(pending).LinkTarget is not null)
            {
                Directory.Delete(pending);
            }

            Directory.Delete(root, recursive: true);
        }
    }
}
