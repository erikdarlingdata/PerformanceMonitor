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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ManagedConfMigrationRunner"/> (#4336 lane 5b): <c>RunStepA</c> and <c>ResumePending</c> over
/// fake snapshot delegates, no database.
/// </summary>
public sealed class ManagedConfMigrationRunnerTests : IDisposable
{
    private readonly string _dataDir;

    public ManagedConfMigrationRunnerTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "pm-4336-5b-runner-" + Guid.NewGuid().ToString("N"));
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

    private static ManagedConfFile.RenderInputs SampleInputs(int port = 55432) => new(
        FormulaVersion: ManagedConfFile.CurrentFormulaVersion,
        Platform: "Windows",
        RamBytes: 17_179_869_184L,
        RamAuthoritative: true,
        ProcessorCount: 8,
        HypertableCount: 42,
        PostgresMajor: 18,
        DataVolumeFreeBytes: 100_000_000_000L,
        DataVolumeTotalBytes: 500_000_000_000L,
        DataVolumeAuthoritative: true,
        Port: port,
        EffectivePreloadList: null);

    private static FileSettingRow Applied(string name, string setting, string file = "postgresql.conf", int line = 1)
        => new(SourceFile: file, SourceLine: line, Name: name, Setting: setting, Applied: true, Error: null);

    private static Dictionary<string, string> DerivedValues(ManagedConfFile.RenderInputs inputs)
    {
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(derivedBody))
        {
            values[name] = value;
        }

        return values;
    }

    private void WriteConf(string text) => File.WriteAllText(Path.Combine(_dataDir, "postgresql.conf"), text);

    private static readonly DateTime UtcNow = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    /// <summary>Pin: the happy path — a snapshot that reports the derived values as applied, both before and
    /// after, verifies. Backup exists, pending is gone, IsVerified is true.</summary>
    [Fact]
    public async Task RunStepA_HappyPath_Verifies()
    {
        WriteConf("# base conf\n");
        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(
                Array.ConvertAll(new List<string>(derived.Keys).ToArray(), name => Applied(name, derived[name])));

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
        Assert.Empty(outcome.MismatchedKeys);
        Assert.NotNull(outcome.BackupPath);
        Assert.True(File.Exists(outcome.BackupPath));
        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    /// <summary>Pin: a mismatch — the after-snapshot disagrees with the before on one key. The conf is
    /// byte-equal to the backup, no stamp, and Failed lists the key.</summary>
    [Fact]
    public async Task RunStepA_Mismatch_RestoresAndListsTheKey()
    {
        const string original = "# base conf\nwork_mem = '16MB'\n";
        WriteConf(original);
        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        var callCount = 0;
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
        {
            callCount++;
            var rows = new List<FileSettingRow>();
            foreach (var kvp in derived)
            {
                var value = callCount == 1 && kvp.Key == "work_mem" ? "16MB" : kvp.Value;
                if (callCount > 1 && kvp.Key == "work_mem")
                {
                    value = "9999MB"; // The after-snapshot disagrees.
                }

                rows.Add(Applied(kvp.Key, value));
            }

            return Task.FromResult<IReadOnlyList<FileSettingRow>>(rows);
        };

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Failed, outcome.Status);
        Assert.Contains("work_mem", outcome.MismatchedKeys);
        Assert.NotNull(outcome.BackupPath);
        Assert.Equal(File.ReadAllText(outcome.BackupPath!), File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.False(ManagedConfMigrationSteps.IsVerified(_dataDir));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    /// <summary>Pin: the before-snapshot throws — no files changed at all, and the status is Unknown.</summary>
    [Fact]
    public async Task RunStepA_BeforeSnapshotThrows_NoFileChanges_Unknown()
    {
        const string original = "# base conf\n";
        WriteConf(original);
        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot =
            _ => throw new InvalidOperationException("simulated connection failure");

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Unknown, outcome.Status);
        Assert.Null(outcome.BackupPath);
        Assert.Equal(original, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfFile.FileName)));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    /// <summary>Ruled pin: the after-snapshot throws. Both postgresql.conf and darling-managed.conf are
    /// restored BYTE-IDENTICAL to their pre-migration bytes (the managed file absent if it was absent), the
    /// stamp is absent, the pending file is deleted, and Status is Unknown.</summary>
    [Fact]
    public async Task RunStepA_AfterSnapshotThrows_RestoresByteIdentical_NoStamp_Unknown()
    {
        const string originalConf = "# base conf\nwork_mem = '16MB'\n";
        WriteConf(originalConf);
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfFile.FileName)));

        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        var callCount = 0;
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
        {
            callCount++;
            if (callCount == 1)
            {
                var rows = new List<FileSettingRow>();
                foreach (var kvp in derived)
                {
                    rows.Add(Applied(kvp.Key, kvp.Key == "work_mem" ? "16MB" : kvp.Value));
                }

                return Task.FromResult<IReadOnlyList<FileSettingRow>>(rows);
            }

            throw new InvalidOperationException("simulated connection failure on re-read");
        };

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Unknown, outcome.Status);
        Assert.Equal(originalConf, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfFile.FileName)));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.StampFileName)));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    /// <summary>Ruled pin variant: the same as above, but a prior managed file already existed before this
    /// run — the restore must bring that file back byte-identical too, not leave it absent.</summary>
    [Fact]
    public async Task RunStepA_AfterSnapshotThrows_WithPriorManagedFile_RestoresItByteIdentical()
    {
        const string originalConf = "# base conf\nwork_mem = '16MB'\n";
        const string priorManagedText = "# prior managed body\nwork_mem = '16MB'\n";
        WriteConf(originalConf);
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), priorManagedText);

        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        var callCount = 0;
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
        {
            callCount++;
            if (callCount == 1)
            {
                var rows = new List<FileSettingRow>();
                foreach (var kvp in derived)
                {
                    rows.Add(Applied(kvp.Key, kvp.Key == "work_mem" ? "16MB" : kvp.Value));
                }

                return Task.FromResult<IReadOnlyList<FileSettingRow>>(rows);
            }

            throw new InvalidOperationException("simulated connection failure on re-read");
        };

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Unknown, outcome.Status);
        Assert.Equal(originalConf, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.Equal(priorManagedText, File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName)));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.StampFileName)));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    /// <summary>Pin: <c>FailBetweenSteps</c> then a re-run — one backup, the same final bytes.</summary>
    [Fact]
    public async Task RunStepA_FailBetweenSteps_ThenRerun_OneBackup_SameFinalBytes()
    {
        WriteConf("# base conf\n");
        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(
                Array.ConvertAll(new List<string>(derived.Keys).ToArray(), name => Applied(name, derived[name])));

        ManagedConfMigrationSteps.FailBetweenSteps = () => throw new IOException("simulated crash between the two writes");

        var logger = new CapturingTestLogger();
        await Assert.ThrowsAsync<IOException>(() => ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None));

        var firstBackups = Directory.GetFiles(_dataDir, "postgresql.conf.pre-4215.*.bak");
        Assert.Single(firstBackups);

        ManagedConfMigrationSteps.FailBetweenSteps = null;

        // Re-run: the crash cleared. This time it completes and verifies.
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
        var finalBackups = Directory.GetFiles(_dataDir, "postgresql.conf.pre-4215.*.bak");
        Assert.Single(finalBackups);
        Assert.Equal(firstBackups[0], finalBackups[0]);
    }

    /// <summary>Pin: a crash after step 2 (the two-step write completed, nothing stamped, no restore yet),
    /// then <c>ResumePending</c> — Verified.</summary>
    [Fact]
    public async Task ResumePending_AfterCrashPostWriteTwoSteps_Verifies()
    {
        WriteConf("# base conf\n");
        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        var before = new List<FileSettingRow>();
        foreach (var kvp in derived)
        {
            before.Add(Applied(kvp.Key, kvp.Value));
        }

        ManagedConfMigrationSteps.WritePending(_dataDir, before, priorManagedText: null);
        var backupPath = ManagedConfMigrationSteps.BackupOriginal(
            _dataDir, Path.Combine(_dataDir, "postgresql.conf"), UtcNow);

        var newManagedConfText = ManagedConfFile.RenderWithValues(
            inputs, new Dictionary<string, string>(derived, StringComparer.OrdinalIgnoreCase));
        var rewrite = ManagedConfMigration.Rewrite("# base conf\n", derived, inputs.Port);
        ManagedConfMigrationSteps.WriteTwoSteps(_dataDir, newManagedConfText, rewrite.NewConfText);

        // Simulate the crash: the files are written, but no stamp and no restore happened yet.
        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(before);

        var outcome = await ManagedConfMigrationRunner.ResumePending(_dataDir, snapshot, backupPath, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }
}
