/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Principal;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7363: a data directory directly under a drive root (<c>D:\pg</c>) made the drive root the store folder, and the start
/// locked it: the lock writes entries that everything below inherits, which on a drive root is the whole drive. The service
/// now never changes a drive root's ACL. Nothing here writes an ACL anywhere but a folder this test made under the temp
/// folder: the real drive root is only ever a string handed to the code under test, with the lock replaced by a recorder.
/// </summary>
public sealed class DarlingDriveRootStoreTests
{
    /// <summary>The root of the drive the temp folder is on, such as <c>C:\</c>. Only a string here, never locked.</summary>
    private static string DriveRoot => Path.GetPathRoot(Path.GetFullPath(Path.GetTempPath()))!;

    private static string NewTempFolder()
    {
        var folder = Path.Combine(Path.GetTempPath(), "darling-driveroot-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        return folder;
    }

    private static void Remove(string folder)
    {
        try { Directory.Delete(folder, recursive: true); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Temp cleanup is best effort. */
        }
    }

    [Theory]
    [InlineData(@"D:\", true)]
    [InlineData(@"d:\", true)]
    [InlineData(@"D:/", true)]
    [InlineData(@"D:\pg", false)]
    [InlineData(@"D:\pg\", false)]
    [InlineData(@"D:\Darling\pg", false)]
    [InlineData(@"\\server\share", true)]
    [InlineData(@"\\server\share\", true)]
    [InlineData(@"\\server\share\pg", false)]
    [InlineData(@"\\server\share\a\pg", false)]
    [InlineData(@"\\?\D:\", true)]
    [InlineData(@"\\?\D:\pg", false)]
    [InlineData(@"\\?\Volume{12345678-1234-1234-1234-123456789012}\", true)]
    [InlineData(@"\\?\Volume{12345678-1234-1234-1234-123456789012}\pg", false)]
    [InlineData(@"\\?\UNC\server\share\", true)]
    [InlineData(@"\\?\UNC\server\share\pg", false)]
    [InlineData(@"C:\mnt\vol", false)]
    public void IsDriveRoot_JudgesTheFolderByItsPathRoot_SoUncShareRootsAndVolumePathsCount(string folder, bool expected)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Drive roots and UNC paths are Windows-only.");
        Assert.Equal(expected, DarlingManagedPostgres.IsDriveRoot(folder));
    }

    /// <summary>An existing store under a drive root keeps running: the lock is never attempted on the root, the start
    /// is told it was not locked, and one error names the folder, the data directory and the way to move the store.</summary>
    [Fact]
    public void AnExistingStoreUnderADriveRoot_IsNotLocked_AndOneErrorSaysSo()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Drive roots are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var dataDirectory = Path.Combine(temp, "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18");
            var logger = new CapturingTestLogger();
            var locked = new List<(string Folder, bool ViaLink)>();

            var result = DarlingManagedPostgres.LockStoreFolder(
                DriveRoot, viaLink: false, dataDirectory, logger, (folder, viaLink) => { locked.Add((folder, viaLink)); return true; });

            Assert.False(result);
            Assert.Empty(locked);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
            Assert.Contains(DriveRoot, logger.Joined, StringComparison.Ordinal);
            Assert.Contains(dataDirectory, logger.Joined, StringComparison.Ordinal);
            Assert.Contains("drive root", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("did not lock it", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("checked and restricted on their own", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("postgres.dataDirectory", logger.Joined, StringComparison.Ordinal);
            Assert.Contains(Path.Combine(DriveRoot, "Darling"), logger.Joined, StringComparison.Ordinal);
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>A new store has nothing to keep running, and the refusal before initdb speaks for it, so the start does not
    /// log the existing-store error as well. The lock is still not attempted.</summary>
    [Fact]
    public void ANewStoreUnderADriveRoot_IsNotLocked_AndLogsNoErrorOfItsOwn()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Drive roots are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var logger = new CapturingTestLogger();
            var locked = 0;

            var result = DarlingManagedPostgres.LockStoreFolder(
                DriveRoot, viaLink: false, Path.Combine(temp, "pg"), logger, (_, _) => { locked++; return true; });

            Assert.False(result);
            Assert.Equal(0, locked);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>A folder that is not a drive root is locked exactly as before: once, with the link flag handed on, the
    /// recorder's answer returned, and no error logged, whether or not the store exists.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public void ANormalSubfolder_IsLockedOnce_AsBefore(bool viaLink, bool answer)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Drive roots are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var dataDirectory = Path.Combine(temp, "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18");
            var logger = new CapturingTestLogger();
            var locked = new List<(string Folder, bool ViaLink)>();

            var result = DarlingManagedPostgres.LockStoreFolder(
                temp, viaLink, dataDirectory, logger, (folder, link) => { locked.Add((folder, link)); return answer; });

            Assert.Equal(answer, result);
            Assert.Equal([(temp, viaLink)], locked);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>A new store under a drive root is refused before initdb. The text names the data directory and a
    /// subfolder to use, and does not offer takeown or icacls, which would change the whole drive.</summary>
    [Fact]
    public void ANewStoreUnderADriveRoot_IsRefusedBeforeInitdb_NamingTheDataDirectoryAndASubfolder()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Drive roots are Windows-only.");
        var dataDirectory = Path.Combine(DriveRoot, "pg");

        var ex = Assert.Throws<InvalidOperationException>(
            () => DarlingManagedPostgres.RefuseInitdbOnUnlockedStoreFolder(DriveRoot, lockedDown: false, dataDirectory));

        Assert.Contains("Refusing to initialize a new store", ex.Message, StringComparison.Ordinal);
        Assert.Contains(dataDirectory, ex.Message, StringComparison.Ordinal);
        Assert.Contains("is a drive root", ex.Message, StringComparison.Ordinal);
        Assert.Contains(Path.Combine(DriveRoot, "Darling", "pg"), ex.Message, StringComparison.Ordinal);
        Assert.Contains("postgres.dataDirectory", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("takeown", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("icacls", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The drive root is refused whether or not the lock is claimed to have worked: it is the folder, not the
    /// lock, that is wrong.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AStoreFolderThatIsADriveRoot_HasAProblem_WhateverTheLockSaid(bool lockedDown)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Drive roots are Windows-only.");
        var reason = DarlingManagedPostgres.StoreFolderProblem(DriveRoot, lockedDown);

        Assert.NotNull(reason);
        Assert.Contains("drive root", reason, StringComparison.Ordinal);
    }

    /// <summary>The checks on the named items run with the drive root as the store folder, and nothing else on the drive
    /// counts: the root's own entries (every other program's folders, owned by whoever made them) are not judged, an
    /// item of the store that is open to ordinary users is given to the tighten step with the drive root as its anchor,
    /// and one that cannot be tightened is refused by name.</summary>
    [Fact]
    public void UnderADriveRoot_TheNamedItemsAreStillChecked_AndTheRootsOwnEntriesAreNot()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            DarlingFileSecurity.HardenDirectory(temp, allowInteractiveTraverse: false);
            var dataDirectory = Path.Combine(temp, "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "postgresql.conf"), "x");
            var serverLog = Path.Combine(temp, "pg.log");
            File.WriteAllText(serverLog, "x");
            var logFolder = Path.Combine(temp, "logs");
            var tightened = new List<(string Path, string Anchor)>();

            string? Problem() => DarlingManagedPostgres.StoreContentsProblem(
                DriveRoot, dataDirectory, serverLog, logFolder, (path, _, anchor) => { tightened.Add((path, anchor)); return "refused by the test"; });

            Assert.Null(Problem());
            Assert.Empty(tightened);

            var users = new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null);
            var file = new FileInfo(serverLog);
            var security = file.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(users, FileSystemRights.Read, AccessControlType.Allow));
            file.SetAccessControl(security);

            var problem = Problem();

            Assert.NotNull(problem);
            Assert.Contains(serverLog, problem, StringComparison.Ordinal);
            Assert.Contains("refused by the test", problem, StringComparison.Ordinal);
            var call = Assert.Single(tightened);
            Assert.Equal(Path.GetFullPath(serverLog), Path.GetFullPath(call.Path));
            Assert.Equal(DriveRoot, call.Anchor, ignoreCase: true);
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>The way an existing store under a drive root gets its data directory restricted: the item is tightened
    /// through a handle with the drive root as the anchor. Only the item (a folder this test made) gets the new ACL, and
    /// the root is opened for attributes and nothing else.</summary>
    [Fact]
    public void AnItemUnderADriveRootAnchor_IsTightened_WithoutAnythingBeingWrittenToTheRoot()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var item = Path.Combine(temp, "pg");
            Directory.CreateDirectory(item);

            var why = DarlingFileSecurity.HardenWithoutFollowingLinks(item, isDirectory: true, allowInteractive: false, DriveRoot);

            Assert.Null(why);
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(item));
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>The <c>--harden-files</c> verb builds the same list the service's start would lock: under a drive root it
    /// has no store directory in it, so the elevated run does not rewrite the drive, and the credential files beside the
    /// data directory are still in it. A folder that is not a drive root is still listed. Nothing is written here: only
    /// the list is built.</summary>
    [Fact]
    public void HardenFiles_DoesNotListADriveRootAsTheStoreDirectory_ButKeepsTheCredentialFiles()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var config = Path.Combine(temp, "darling.json");
            File.WriteAllText(config, "{}");

            var underRoot = DarlingCliCommands.HardenTargets(config, Path.Combine(DriveRoot, "pg"), keyDirectory: null);
            var underFolder = DarlingCliCommands.HardenTargets(config, Path.Combine(temp, "store", "pg"), keyDirectory: null);

            Assert.DoesNotContain(underRoot, target => target.IsDirectory);
            Assert.Contains(underRoot, target => target.What == "the admin credential"
                && string.Equals(target.Path, Path.Combine(DriveRoot, "pg-admin-credential.dpapi"), StringComparison.OrdinalIgnoreCase));
            Assert.Contains(underRoot, target => target.What == "the store credential");

            var store = Assert.Single(underFolder, target => target.IsDirectory);
            Assert.Equal(Path.Combine(temp, "store"), store.Path, ignoreCase: true);
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>A store folder link that points at a UNC share root with a trailing separator is a drive root too:
    /// <see cref="Path.GetPathRoot(string)"/> drops that separator, so a comparison with the full path missed it.</summary>
    [Fact]
    public void ALinkTargetThatIsAUncShareRootWithATrailingSeparator_IsADriveRoot()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "UNC paths are Windows-only.");
        var full = Path.GetFullPath(@"\\server\share\");

        Assert.NotEqual(full, Path.GetPathRoot(full));
        Assert.True(DarlingManagedPostgres.IsDriveRoot(full));
    }
}
