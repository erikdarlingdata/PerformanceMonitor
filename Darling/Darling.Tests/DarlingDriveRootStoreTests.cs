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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
            Assert.Contains("checked and restricted at every start", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("new cluster of a major upgrade is restricted", logger.Joined, StringComparison.Ordinal);
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

    /* ================= #7363 round 2: what the service creates beside the data directory ================= */

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly SecurityIdentifier s_users = new(WellKnownSidType.BuiltinUsersSid, null);

    /// <summary>Makes <paramref name="folder"/> hand Users read access to everything created below it, which is what a
    /// drive root at its default does. The folder is one this test made under the temp folder.</summary>
    private static void OpenToUsers(string folder)
    {
        var info = new DirectoryInfo(folder);
        var security = info.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(
            s_users, FileSystemRights.ReadAndExecute, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.None, AccessControlType.Allow));
        info.SetAccessControl(security);
    }

    private static DarlingStoreUpgrade.UpgradeContext UpgradeContextFor(string dataDirectory) => new(
        OldBinDirectory: Path.Combine(dataDirectory, "old", "bin"),
        NewBinDirectory: Path.Combine(dataDirectory, "new", "bin"),
        RuntimeRoot: Path.Combine(dataDirectory, "runtime"),
        ZipHash: "hash",
        DataDirectory: dataDirectory,
        Port: 55432,
        UserName: "darling",
        Password: "not-a-real-password",
        OldMajor: 17,
        NewMajor: 18,
        BundledTimescaleVersion: "2.0.0",
        AppendManagedConf: _ => { },
        SslServerOptions: () => string.Empty);

    private static readonly DarlingStoreUpgrade.ClusterIdentity s_identity = new("UTF8", "C", "C", "c", null, false);

    /// <summary>Whether the item's ACL stops inheriting, and every account with an entry in it.</summary>
    private static (bool Protected, HashSet<SecurityIdentifier> Accounts) AclOf(string path, bool isDirectory)
    {
        FileSystemSecurity security = isDirectory ? new DirectoryInfo(path).GetAccessControl() : new FileInfo(path).GetAccessControl();
        var accounts = security.GetAccessRules(includeExplicit: true, includeInherited: true, typeof(SecurityIdentifier))
            .Cast<FileSystemAccessRule>()
            .Select(rule => (SecurityIdentifier)rule.IdentityReference)
            .ToHashSet();
        return (security.AreAccessRulesProtected, accounts);
    }

    /// <summary>The accounts the service's ACL may name: SYSTEM, Administrators and the account the service runs as.</summary>
    private static bool OnlyTheTrustedAccounts(IEnumerable<SecurityIdentifier> accounts)
    {
        var system = new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null);
        var administrators = new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null);
        var self = WindowsIdentity.GetCurrent().User!;
        return accounts.All(account => account.Equals(system) || account.Equals(administrators) || account.Equals(self));
    }

    /// <summary>A major upgrade's new cluster is built in a folder beside the data directory, and under a drive root the
    /// folder above it is never locked, so initdb's output inherits the drive's access. The step restricts the folder right
    /// after initdb and before pg_upgrade: its ACL stops inheriting and names only SYSTEM, Administrators and the service
    /// account, and the files initdb already made below it lose the access they inherited. The folder above is open to Users
    /// here, and nothing is written outside the temp folder.</summary>
    [Fact]
    public async Task TheMajorUpgradesNewCluster_IsRestrictedAfterInitdb_EvenWhenTheFolderAboveIsOpenToUsers()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            OpenToUsers(temp);
            var dataDirectory = Path.Combine(temp, "pg");
            var newCluster = dataDirectory + DarlingStoreUpgrade.UpgradeStagingDirectorySuffix + "18";
            var passwordFile = Path.Combine(temp, "pg-upgrade-pwfile.tmp");
            var child = Path.Combine(newCluster, "PG_VERSION");
            var openWhenInitdbFinished = false;

            Task<(int ExitCode, string Output)> FakeInitdb(string exe, string arguments, TimeSpan timeout, CancellationToken token)
            {
                Directory.CreateDirectory(Path.Combine(newCluster, "base"));
                File.WriteAllText(child, "18");
                openWhenInitdbFinished = DarlingFileSecurity.IsReadableByOrdinaryUsers(newCluster)
                    && DarlingFileSecurity.IsReadableByOrdinaryUsers(child);
                return Task.FromResult((0, string.Empty));
            }

            await new DarlingStoreUpgrade(NullLogger.Instance).InitializeNewClusterAsync(
                UpgradeContextFor(dataDirectory), newCluster, passwordFile, s_identity, FakeInitdb, tightenItem: null, Ct);

            Assert.True(openWhenInitdbFinished, "the premise: the new cluster inherits the access of the folder above it");
            var folder = AclOf(newCluster, isDirectory: true);
            Assert.True(folder.Protected);
            Assert.True(OnlyTheTrustedAccounts(folder.Accounts), "the new cluster's ACL names " + string.Join(", ", folder.Accounts));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(newCluster));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(child));
            Assert.True(OnlyTheTrustedAccounts(AclOf(child, isDirectory: false).Accounts));
            Assert.True(OnlyTheTrustedAccounts(AclOf(Path.Combine(newCluster, "base"), isDirectory: true).Accounts));
            Assert.True(AclOf(passwordFile, isDirectory: false).Protected);
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>The rule the start follows when it cannot restrict the data directory: refuse. The step throws, which the
    /// upgrade's pre-commit handling turns into a failed upgrade that leaves the store on its current major, before
    /// pg_upgrade has copied anything into the folder. The anchor handed to the step is the folder above the new cluster.</summary>
    [Fact]
    public async Task TheMajorUpgradesNewCluster_ThatCannotBeRestricted_StopsTheUpgradeBeforePgUpgrade()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var dataDirectory = Path.Combine(temp, "pg");
            var newCluster = dataDirectory + DarlingStoreUpgrade.UpgradeStagingDirectorySuffix + "18";
            var calls = new List<(string Path, bool IsDirectory, string Anchor)>();

            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => new DarlingStoreUpgrade(NullLogger.Instance)
                .InitializeNewClusterAsync(
                    UpgradeContextFor(dataDirectory),
                    newCluster,
                    Path.Combine(temp, "pg-upgrade-pwfile.tmp"),
                    s_identity,
                    (exe, arguments, timeout, token) =>
                    {
                        Directory.CreateDirectory(newCluster);
                        return Task.FromResult((0, string.Empty));
                    },
                    (path, isDirectory, anchor) => { calls.Add((path, isDirectory, anchor)); return "refused by the test"; },
                    Ct));

            Assert.Contains("refused by the test", failure.Message, StringComparison.Ordinal);
            Assert.Contains(newCluster, failure.Message, StringComparison.Ordinal);
            var call = Assert.Single(calls);
            Assert.Equal(newCluster, call.Path);
            Assert.True(call.IsDirectory);
            Assert.Equal(temp, call.Anchor, ignoreCase: true);
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>A failed initdb is reported as it was, and the step does not go on to restrict a folder that was not made.</summary>
    [Fact]
    public async Task AFailedInitdb_IsReportedAsItWas_AndNothingIsRestricted()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            var dataDirectory = Path.Combine(temp, "pg");
            var newCluster = dataDirectory + DarlingStoreUpgrade.UpgradeStagingDirectorySuffix + "18";
            var restricted = false;

            var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => new DarlingStoreUpgrade(NullLogger.Instance)
                .InitializeNewClusterAsync(
                    UpgradeContextFor(dataDirectory),
                    newCluster,
                    Path.Combine(temp, "pg-upgrade-pwfile.tmp"),
                    s_identity,
                    (exe, arguments, timeout, token) => Task.FromResult((1, "initdb: error: could not create directory")),
                    (path, isDirectory, anchor) => { restricted = true; return null; },
                    Ct));

            Assert.Contains("initdb of the new cluster failed", failure.Message, StringComparison.Ordinal);
            Assert.False(restricted);
        }
        finally
        {
            Remove(temp);
        }
    }

    /// <summary>The copy of the store credential that goes beside a half-made data directory moved aside is created with the
    /// protected ACL, not with the access of the folder it lands in. The folder here is open to Users, and a plain copy of the
    /// file is what the old code made.</summary>
    [Fact]
    public void TheCredentialCopiedBesideAMovedAsideInitdbFolder_IsCreatedWithAProtectedAcl()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");
        var temp = NewTempFolder();
        try
        {
            OpenToUsers(temp);
            var dataDirectory = Path.Combine(temp, "pgdata");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "base"), "x");
            File.WriteAllText(DarlingManagedPostgres.InitdbIncompleteMarkerPath(dataDirectory), dataDirectory);
            DarlingManagedPostgres.WriteHardenedText(DarlingManagedPostgres.CredentialPathFor(dataDirectory), "the-protected-secret");

            DarlingManagedPostgres.DiscardInterruptedInitdb(dataDirectory, NullLogger.Instance);

            var copy = Directory.GetFiles(temp, "*" + DarlingManagedPostgres.InterruptedInitdbCredentialSuffix).Single();
            Assert.Equal("the-protected-secret", File.ReadAllText(copy));
            var acl = AclOf(copy, isDirectory: false);
            Assert.True(acl.Protected);
            Assert.True(OnlyTheTrustedAccounts(acl.Accounts), "the copy's ACL names " + string.Join(", ", acl.Accounts));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(copy));
        }
        finally
        {
            Remove(temp);
        }
    }
}
