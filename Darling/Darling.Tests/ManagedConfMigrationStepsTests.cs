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
/// <see cref="ManagedConfMigrationSteps"/> (#4336): the backup, the two-step atomic write, and the
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

    private static FileSettingRow SampleRow(string name, string setting)
        => new(SourceFile: "postgresql.conf", SourceLine: 1, Name: name, Setting: setting, Applied: true, Error: null);

    [Fact]
    public void WritePending_ThenTryReadPending_RoundTripsTheBeforeSnapshotAndPriorManagedText()
    {
        var before = new[] { SampleRow("work_mem", "16MB"), SampleRow("shared_buffers", "2048MB") };

        ManagedConfMigrationSteps.WritePending(_dataDir, before, "prior managed text\n");

        var found = ManagedConfMigrationSteps.TryReadPending(_dataDir, out var readBack, out var priorText);

        Assert.True(found);
        Assert.Equal("prior managed text\n", priorText);
        Assert.Equal(2, readBack.Count);
        Assert.Contains(readBack, r => r.Name == "work_mem" && r.Setting == "16MB" && r.Applied);
        Assert.Contains(readBack, r => r.Name == "shared_buffers" && r.Setting == "2048MB" && r.Applied);
    }

    [Fact]
    public void WritePending_NoPriorManagedText_RoundTripsAsNull()
    {
        ManagedConfMigrationSteps.WritePending(_dataDir, new[] { SampleRow("work_mem", "16MB") }, priorManagedText: null);

        ManagedConfMigrationSteps.TryReadPending(_dataDir, out _, out var priorText);

        Assert.Null(priorText);
    }

    [Fact]
    public void WritePending_ValueWithTabAndNewline_RoundTripsExactly()
    {
        const string tricky = "line one\tcol\nline two";
        var before = new[] { SampleRow("comment", tricky) };

        ManagedConfMigrationSteps.WritePending(_dataDir, before, priorManagedText: tricky);

        ManagedConfMigrationSteps.TryReadPending(_dataDir, out var readBack, out var priorText);

        Assert.Equal(tricky, priorText);
        Assert.Equal(tricky, Assert.Single(readBack).Setting);
    }

    [Fact]
    public void TryReadPending_NoPendingFile_ReturnsFalseAndEmpty()
    {
        var found = ManagedConfMigrationSteps.TryReadPending(_dataDir, out var readBack, out var priorText);

        Assert.False(found);
        Assert.Empty(readBack);
        Assert.Null(priorText);
    }

    [Fact]
    public void DeletePending_RemovesTheFile_AndIsANoOpWhenAlreadyGone()
    {
        ManagedConfMigrationSteps.WritePending(_dataDir, new[] { SampleRow("work_mem", "16MB") }, priorManagedText: null);

        ManagedConfMigrationSteps.DeletePending(_dataDir);
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));

        // A no-op the second time.
        ManagedConfMigrationSteps.DeletePending(_dataDir);
    }

    [Fact]
    public void RestoreOriginal_RestoresPostgresqlConfFromBackup_AndManagedConfFromPriorText()
    {
        var confPath = Path.Combine(_dataDir, "postgresql.conf");
        File.WriteAllText(confPath, "original\n");
        var backupPath = ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

        // Simulate the migration having written new content to both files.
        File.WriteAllText(confPath, "migrated\n");
        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        File.WriteAllText(managedPath, "new managed body\n");

        ManagedConfMigrationSteps.RestoreOriginal(_dataDir, backupPath, priorManagedText: "old managed body\n");

        Assert.Equal("original\n", File.ReadAllText(confPath));
        Assert.Equal("old managed body\n", File.ReadAllText(managedPath));
    }

    /// <summary>Pin (fixes commit 9ae7410c): a pending file with a WRONG FIELD COUNT (5 instead of 6) does
    /// not throw -- it returns false with both out parameters empty/null, the same as a missing file.</summary>
    [Fact]
    public void TryReadPending_WrongFieldCount_ReturnsFalse_DoesNotThrow()
    {
        var pendingPath = Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName);
        File.WriteAllText(pendingPath, "N\nVpostgresql.conf\tV1\tVwork_mem\tV16MB\t1\n"); // 5 fields, not 6

        var found = ManagedConfMigrationSteps.TryReadPending(_dataDir, out var readBack, out var priorText);

        Assert.False(found);
        Assert.Empty(readBack);
        Assert.Null(priorText);
    }

    /// <summary>Pin (fixes commit 9ae7410c): a pending file whose source-line field is not an integer does
    /// not throw -- it returns false with both out parameters empty/null.</summary>
    [Fact]
    public void TryReadPending_NonIntegerLineNumber_ReturnsFalse_DoesNotThrow()
    {
        var pendingPath = Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName);
        File.WriteAllText(pendingPath, "N\nVpostgresql.conf\tVnot-a-number\tVwork_mem\tV16MB\t1\tN\n");

        var found = ManagedConfMigrationSteps.TryReadPending(_dataDir, out var readBack, out var priorText);

        Assert.False(found);
        Assert.Empty(readBack);
        Assert.Null(priorText);
    }

    [Fact]
    public void RestoreOriginal_NoPriorManagedText_LeavesTheManagedFileAbsent()
    {
        var confPath = Path.Combine(_dataDir, "postgresql.conf");
        File.WriteAllText(confPath, "original\n");
        var backupPath = ManagedConfMigrationSteps.BackupOriginal(_dataDir, confPath, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

        File.WriteAllText(confPath, "migrated\n");
        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        File.WriteAllText(managedPath, "new managed body\n");

        ManagedConfMigrationSteps.RestoreOriginal(_dataDir, backupPath, priorManagedText: null);

        Assert.Equal("original\n", File.ReadAllText(confPath));
        Assert.False(File.Exists(managedPath));
    }
}
