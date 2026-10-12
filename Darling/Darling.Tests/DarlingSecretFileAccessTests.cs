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
using System.Security.AccessControl;
using System.Security.Principal;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The secret files the service and the CLI write or read beyond the credential ACLs themselves: the store folder a
/// first initdb runs under, the files <c>--harden-files</c> covers, the files created with their ACL rather than
/// hardened after, and the two secret reads that warn when ordinary users can read the file.
///
/// <para>In the <c>darling-config-env</c> collection (it runs alone) because these tests set the operator-group
/// stand-in (<c>DarlingFileSecurity.UseOperatorGroupForTests</c>) and the file-access sink
/// (<c>DarlingSecretSource.ReportFileAccessTo</c>), statics the whole test process shares.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingSecretFileAccessTests
{
    private static readonly SecurityIdentifier s_builtinUsers = new(WellKnownSidType.BuiltinUsersSid, null);
    private static readonly SecurityIdentifier s_operatorStandIn = new(WellKnownSidType.BuiltinPerformanceLoggingUsersSid, null);

    [Fact]
    public void StoreFolder_LockedDownAndOwnedByThisProcess_MayTakeAFreshCluster()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var folder = Path.Combine(Path.GetTempPath(), "darling-store-ok-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        DarlingFileSecurity.UseOperatorGroupForTests(() => s_operatorStandIn);
        try
        {
            DarlingFileSecurity.HardenDirectory(folder, allowInteractiveTraverse: true);

            Assert.Null(DarlingManagedPostgres.StoreFolderProblem(folder, lockedDown: true));
        }
        finally
        {
            DarlingFileSecurity.UseOperatorGroupForTests(null);
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void StoreFolder_ReadableByUsers_RefusesAFreshCluster()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var folder = Path.Combine(Path.GetTempPath(), "darling-store-open-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            /* The shape a folder an ordinary user created under ProgramData has: Users can list it, and everything
               created inside inherits that. The harden "succeeded" here so the read entry alone is what refuses. */
            var security = new DirectoryInfo(folder).GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                s_builtinUsers, FileSystemRights.Read,
                InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
            new DirectoryInfo(folder).SetAccessControl(security);

            var reason = DarlingManagedPostgres.StoreFolderProblem(folder, lockedDown: true);
            Assert.NotNull(reason);
            Assert.Contains("readable", reason, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>When the ACL could not be applied, the owner is someone else (the service cannot re-ACL what it does
    /// not own), so the folder is refused before its contents are even looked at.</summary>
    [Fact]
    public void StoreFolder_WhoseLockdownFailed_RefusesAFreshCluster()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var folder = Path.Combine(Path.GetTempPath(), "darling-store-unlocked-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            var reason = DarlingManagedPostgres.StoreFolderProblem(folder, lockedDown: false);
            Assert.NotNull(reason);
            Assert.Contains("could not be restricted", reason, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>The gate sits on the fresh-cluster branch only: an existing store keeps starting.</summary>
    [Fact]
    public void StoreFolder_Gate_RunsOnlyBeforeAFirstInitdb()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");

        var branch = source.IndexOf("if (!existingCluster)", StringComparison.Ordinal);
        Assert.True(branch >= 0, "the fresh-cluster branch is gone");
        var gate = source.IndexOf("RefuseInitdbOnUnlockedStoreFolder(ParentOf(_dataDirectory), storeFolderLocked);", StringComparison.Ordinal);
        var initdb = source.IndexOf("await InitializeClusterAsync(binDirectory, cancellationToken);", StringComparison.Ordinal);
        Assert.True(gate > branch && initdb > gate, "the store-folder gate must sit inside the fresh-cluster branch, before initdb");
        Assert.Equal(1, CountOf(source, "RefuseInitdbOnUnlockedStoreFolder(ParentOf"));
    }

    [Fact]
    public void HardenFilesVerb_CoversEveryRoleCredential()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs");

        var at = source.IndexOf("public static int HardenFiles(", StringComparison.Ordinal);
        Assert.True(at >= 0, "HardenFiles is gone");
        var body = source[at..Math.Min(source.Length, at + 10000)];

        foreach (var file in new[] { "pg-credential.dpapi", "pg-admin-credential.dpapi", "pg-viewer-credential.dpapi", "pg-mcp-credential.dpapi" })
        {
            Assert.Contains($"Path.Combine(storeRoot, \"{file}\")", body, StringComparison.Ordinal);
        }

        /* The same posture the service applies every start: admin and viewer readable by the operator group, mcp not. */
        Assert.Contains("\"pg-admin-credential.dpapi\"), AllowInteractive: true", body, StringComparison.Ordinal);
        Assert.Contains("\"pg-viewer-credential.dpapi\"), AllowInteractive: true", body, StringComparison.Ordinal);
        Assert.Contains("\"pg-mcp-credential.dpapi\"), AllowInteractive: false", body, StringComparison.Ordinal);
    }

    /// <summary>Each secret file is created with its ACL: none is written through a plain create and hardened after.</summary>
    [Theory]
    [InlineData("DarlingManagedPostgres.cs", "private async Task InitializeClusterAsync(", "File.WriteAllText(_credentialPath", "WriteHardenedText(_credentialPath")]
    [InlineData("DarlingManagedPostgres.cs", "private async Task InitializeClusterAsync(", "File.WriteAllText(passwordFile", "WriteHardenedText(passwordFile")]
    [InlineData("DarlingManagedRoles.cs", "private static void PersistRoleCredential(", "File.WriteAllText(", "DarlingFileSecurity.CreateHardenedFile(temporary, credential.AllowInteractiveRead)")]
    [InlineData("DarlingStoreUpgrade.cs", "var preUpgradeCopy = Path.Combine(parent, PreUpgradeAutoConfFileName);", "File.Copy(sourcePath, preUpgradeCopy", "DarlingFileSecurity.CreateHardenedFile(preUpgradeCopy, allowInteractiveRead: false)")]
    [InlineData("DarlingCliCommands.cs", "private static void CopyToHardenedBackup(", "File.Copy(sourcePath, backupPath", "DarlingFileSecurity.CreateHardenedFile(backupPath, allowInteractiveRead: false)")]
    public void SecretFiles_AreCreatedHardened_NotWrittenThenHardened(string file, string anchor, string forbidden, string required)
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", file);
        var at = source.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{anchor} is gone from {file}");
        var window = source[at..Math.Min(source.Length, at + 3000)];

        Assert.DoesNotContain(forbidden, window, StringComparison.Ordinal);
        Assert.Contains(required, window, StringComparison.Ordinal);
    }

    [Fact]
    public void HardenedCreate_CarriesNoInheritedEntry_FromAFolderThatGrantsUsersRead()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var folder = Path.Combine(Path.GetTempPath(), "darling-open-folder-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            var security = new DirectoryInfo(folder).GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                s_builtinUsers, FileSystemRights.Read,
                InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
            new DirectoryInfo(folder).SetAccessControl(security);

            var path = Path.Combine(folder, "darling.json.bak-test");
            using (var stream = DarlingFileSecurity.CreateHardenedFile(path, allowInteractiveRead: false))
            {
                stream.Write("secret"u8);
            }

            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
            var rules = new FileInfo(path).GetAccessControl().GetAccessRules(true, true, typeof(SecurityIdentifier));
            foreach (FileSystemAccessRule rule in rules)
            {
                Assert.False(rule.IsInherited, $"inherited entry survived for {rule.IdentityReference.Value}");
            }
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public void FileSecret_ReadableByUsers_WarnsOnceThroughTheSink()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-file-secret-" + Guid.NewGuid().ToString("N") + ".txt");
        File.WriteAllText(path, "hunter2\n");
        var warnings = new List<string>();
        try
        {
            var security = new FileInfo(path).GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(s_builtinUsers, FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(path).SetAccessControl(security);

            DarlingSecretSource.ReportFileAccessTo(warnings.Add);
            Assert.Equal("hunter2", DarlingSecretSource.Resolve("file:" + path, "smtp.password"));
            Assert.Equal("hunter2", DarlingSecretSource.Resolve("file:" + path, "smtp.password"));

            var warning = Assert.Single(warnings);
            Assert.Contains("smtp.password", warning, StringComparison.Ordinal);
            Assert.Contains(path, warning, StringComparison.Ordinal);
            Assert.Contains("readable by Users", warning, StringComparison.Ordinal);

            /* Hardened, the same read is silent. */
            warnings.Clear();
            DarlingSecretSource.ReportFileAccessTo(warnings.Add);
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: false);
            Assert.Equal("hunter2", DarlingSecretSource.Resolve("file:" + path, "smtp.password"));
            Assert.Empty(warnings);
        }
        finally
        {
            DarlingSecretSource.ReportFileAccessTo(null);
            File.Delete(path);
        }
    }

    private static int CountOf(string source, string needle)
    {
        var count = 0;
        for (var at = source.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = source.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
