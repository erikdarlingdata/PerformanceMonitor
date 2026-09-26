/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ManagedConfMigrationSteps"/> (#4336 lane 5a): the backup, the two-step atomic write, and the
/// verified stamp. Pure file I/O in throwaway temp directories, no database.
/// </summary>
public sealed class ManagedConfMigrationStepsTests : IDisposable
{
    private readonly string _dataDir;

    public ManagedConfMigrationStepsTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "pm-4336-5a-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
    }

    public void Dispose()
    {
        ManagedConfMigrationSteps.FailBetweenSteps = null;
        try
        {
            Directory.Delete(_dataDir, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup; leftover temp dirs don't fail the run.
        }
    }

    [Fact]
    public void BackupOriginal_CreatesBackupOnce()
    {
        var confPath = Path.Combine(_dataDir, "postgresql.conf");
        File.WriteAllText(confPath, "port = 5432\n");

        var utcNow = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var backupPath = ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, utcNow);

        Assert.True(File.Exists(backupPath));
        Assert.Equal("port = 5432\n", File.ReadAllText(backupPath));
        Assert.Contains("postgresql.conf.pre-4215.20260102T030405Z.bak", backupPath, StringComparison.Ordinal);
    }

    [Fact]
    public void BackupOriginal_SecondCall_MakesNoNewFile_ReturnsFirstPath()
    {
        var confPath = Path.Combine(_dataDir, "postgresql.conf");
        File.WriteAllText(confPath, "port = 5432\n");

        var first = ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

        // Mutate the conf and try again a day later — the backup must not move or duplicate.
        File.WriteAllText(confPath, "port = 9999\n");
        var second = ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, new DateTime(2026, 1, 3, 3, 4, 5, DateTimeKind.Utc));

        Assert.Equal(first, second);
        var backups = Directory.GetFiles(_dataDir, "postgresql.conf.pre-4215.*.bak");
        Assert.Single(backups);
    }

    [Fact]
    public void BackupOriginal_ExistingBackup_NeverOverwritten_CheckedByContent()
    {
        var confPath = Path.Combine(_dataDir, "postgresql.conf");
        File.WriteAllText(confPath, "port = 5432\n");
        var first = ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

        File.WriteAllText(confPath, "port = 1111\nshared_buffers = 1GB\n");
        ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, new DateTime(2026, 1, 4, 0, 0, 0, DateTimeKind.Utc));

        Assert.Equal("port = 5432\n", File.ReadAllText(first));
    }

    [Fact]
    public void WriteTwoSteps_Success_BothFilesHoldNewText()
    {
        File.WriteAllText(Path.Combine(_dataDir, "postgresql.conf"), "old\n");

        ManagedConfMigrationSteps.WriteTwoSteps(_dataDir, "managed body\n", "new conf\n");

        Assert.Equal("managed body\n", File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName)));
        Assert.Equal("new conf\n", File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
    }

    [Fact]
    public void WriteTwoSteps_InjectedFailureBetweenSteps_PostgresqlConfUntouched_ManagedConfHoldsNewText()
    {
        var confPath = Path.Combine(_dataDir, "postgresql.conf");
        File.WriteAllText(confPath, "old\n");

        ManagedConfMigrationSteps.FailBetweenSteps = () => throw new InvalidOperationException("simulated crash between steps");

        Assert.Throws<InvalidOperationException>(() =>
            ManagedConfMigrationSteps.WriteTwoSteps(_dataDir, "managed body\n", "new conf\n"));

        Assert.Equal("old\n", File.ReadAllText(confPath));
        Assert.Equal("managed body\n", File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName)));

        // A re-run (crash cleared) converges on the same bytes in both files.
        ManagedConfMigrationSteps.FailBetweenSteps = null;
        ManagedConfMigrationSteps.WriteTwoSteps(_dataDir, "managed body\n", "new conf\n");

        Assert.Equal("new conf\n", File.ReadAllText(confPath));
        Assert.Equal("managed body\n", File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName)));
    }

    [Fact]
    public void WriteVerifiedStamp_ThenIsVerified_IsTrue()
    {
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), "managed body\n");
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, "managed body\n");

        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    [Fact]
    public void IsVerified_ManagedConfEditedByHand_IsFalse()
    {
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), "managed body\n");
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, "managed body\n");

        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), "hand-edited\n");

        Assert.False(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    [Fact]
    public void IsVerified_StampDeleted_IsFalse()
    {
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), "managed body\n");
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, "managed body\n");

        File.Delete(Path.Combine(_dataDir, ManagedConfMigrationSteps.StampFileName));

        Assert.False(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    [Fact]
    public void IsVerified_RuleVersionBumped_IsFalse()
    {
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), "managed body\n");

        // Simulate a stamp written under a future rule version by writing the stamp content directly.
        var hash = Convert.ToHexString(
            System.Security.Cryptography.SHA256.HashData(new System.Text.UTF8Encoding(false).GetBytes("managed body\n"))).ToLowerInvariant();
        File.WriteAllText(
            Path.Combine(_dataDir, ManagedConfMigrationSteps.StampFileName),
            $"sha256:{hash}\nrule:{ManagedConfMigrationSteps.RuleVersion + 1}\n");

        Assert.False(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    [Fact]
    public void IsVerified_UnparsableStamp_IsFalse()
    {
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), "managed body\n");
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfMigrationSteps.StampFileName), "not a stamp at all\n");

        Assert.False(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }
}
