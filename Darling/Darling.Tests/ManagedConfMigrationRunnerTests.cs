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
/// <see cref="ManagedConfMigrationRunner"/> (#4336): <c>RunStepA</c> and <c>ResumePending</c> over
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
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Best-effort cleanup; leftover temp dirs don't fail the run (a file the OS still
            // maps surfaces as UnauthorizedAccessException on Windows).
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

    /// <summary>A failed attempt, an edit to postgresql.conf, and a second failed attempt: the second
    /// restore puts back the EDITED file, not the first attempt's snapshot. The second attempt's backup is
    /// a new file holding the edit, and the first attempt's stays as it was. It used to restore the first
    /// snapshot, and every edit made between attempts was lost.</summary>
    [Fact]
    public async Task RunStepA_FailsTwice_ConfEditedBetweenAttempts_RestoreKeepsTheEdit()
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
                /* Odd calls are an attempt's before-snapshot, even calls its after-snapshot; every
                   after-snapshot disagrees on work_mem, so every attempt fails and restores. */
                var value = kvp.Key == "work_mem" ? (callCount % 2 == 1 ? "16MB" : "9999MB") : kvp.Value;
                rows.Add(Applied(kvp.Key, value));
            }

            return Task.FromResult<IReadOnlyList<FileSettingRow>>(rows);
        };

        var logger = new CapturingTestLogger();
        var first = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);
        Assert.Equal(ManagedConfVerificationStatus.Failed, first.Status);
        Assert.Equal(original, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));

        const string edited = original + "# added between attempts\nwork_mem = '32MB'\n";
        WriteConf(edited);

        var second = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow.AddDays(1), logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Failed, second.Status);
        Assert.Equal(edited, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.NotEqual(first.BackupPath, second.BackupPath);
        Assert.Equal(original, File.ReadAllText(first.BackupPath!));
        Assert.Equal(edited, File.ReadAllText(second.BackupPath!));
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
        Assert.Contains("before-snapshot", outcome.Detail);
        Assert.Contains("simulated connection failure", outcome.Detail);
        Assert.Contains("simulated connection failure", logger.Joined);
    }

    /// <summary>Pin: the after-snapshot throws. Both postgresql.conf and darling-managed.conf are
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
        Assert.Contains("after-snapshot", outcome.Detail);
        Assert.Contains("simulated connection failure on re-read", outcome.Detail);
    }

    /// <summary>Pin variant: the same as above, but a prior managed file already existed before this
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

    /// <summary>Pin (fixes commit 9ae7410c): a hand-edited comment line that embeds a product marker
    /// SUBSTRING mid-line (not at line start, so it never classifies as the marker's own Ours line) is a
    /// HandEdit the rewrite keeps verbatim -- the marker text survives into the rewritten postgresql.conf.
    /// RunStepA catches that and reports Failed with a Detail before EITHER new file
    /// (darling-managed.conf, the rewritten postgresql.conf) is written, and deletes the pending file it had
    /// already persisted. NOTE: WritePending and BackupOriginal both run earlier in RunStepA's order (before
    /// this check), so a backup file DOES exist afterward -- carrying the ORIGINAL bytes, since nothing wrote
    /// over postgresql.conf itself. That backup is reported back on the outcome, not deleted; only the pending
    /// file is cleaned up here.</summary>
    [Fact]
    public async Task RunStepA_MarkerSurvivesRewrite_AbortsBeforeAnyWrite_Failed()
    {
        const string original =
            "# base conf\n" +
            "do not delete: # Managed by PerformanceMonitor Darling (v11 job execution logging) -- do not remove this block\n";
        WriteConf(original);
        var inputs = SampleInputs();
        var derived = DerivedValues(inputs);

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(
                Array.ConvertAll(new List<string>(derived.Keys).ToArray(), name => Applied(name, derived[name])));

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Failed, outcome.Status);
        Assert.False(string.IsNullOrEmpty(outcome.Detail));
        Assert.Equal(original, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfFile.FileName)));
        Assert.NotNull(outcome.BackupPath);
        Assert.Equal(original, File.ReadAllText(outcome.BackupPath!));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    /// <summary>Pin (fixes commit 9ae7410c): a corrupt pending file with a backup present -- restores the
    /// conf byte-equal to the backup, deletes the pending file, and reports Failed with a non-empty Detail
    /// (not Unknown, so the store-settings alert actually fires).</summary>
    [Fact]
    public async Task ResumePending_CorruptPendingFile_WithBackup_RestoresAndReportsFailed()
    {
        const string originalConf = "# base conf\nwork_mem = '16MB'\n";
        WriteConf(originalConf);
        var backupPath = ManagedConfMigrationSteps.BackupOriginal(
            _dataDir, Path.Combine(_dataDir, "postgresql.conf"), UtcNow);

        // Simulate the migration having already written new content, then a pending file too corrupt to parse.
        WriteConf("# migrated\nwork_mem = '9999MB'\n");
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName), "N\ngarbage\tfields\n");

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot =
            _ => throw new InvalidOperationException("must not be called: the pending file is unreadable before any snapshot");

        var outcome = await ManagedConfMigrationRunner.ResumePending(_dataDir, snapshot, backupPath, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Failed, outcome.Status);
        Assert.False(string.IsNullOrEmpty(outcome.Detail));
        Assert.Equal(originalConf, File.ReadAllText(Path.Combine(_dataDir, "postgresql.conf")));
        Assert.False(File.Exists(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName)));
    }

    private void WriteManaged(string text) => File.WriteAllText(Path.Combine(_dataDir, ManagedConfFile.FileName), text);

    /// <summary>Pin (#4336): every rendered key matches its <c>pg_file_settings</c> row — the stamp
    /// is written and <see cref="ManagedConfMigrationSteps.IsVerified"/> is true.</summary>
    [Fact]
    public void VerifyStepB_AllKeysMatch_StampsVerified()
    {
        var rendered = "work_mem = '16MB'\nmax_connections = '200'\n";
        WriteManaged(rendered);

        var rows = new List<FileSettingRow>
        {
            Applied("work_mem", "16MB", file: Path.Combine(_dataDir, ManagedConfFile.FileName)),
            Applied("max_connections", "200", file: Path.Combine(_dataDir, ManagedConfFile.FileName)),
        };

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText: null);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
        Assert.Equal(ManagedConfMigrationStep.B, outcome.Step);
        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    /// <summary>Pin (#4336): a mismatched key restores the previous file bytes exactly, and the OLD
    /// stamp (written against <c>previousText</c> beforehand) is still verified against them.</summary>
    [Fact]
    public void VerifyStepB_Mismatch_RestoresPreviousTextAndOldStampStillVerifies()
    {
        var previousText = "work_mem = '8MB'\n";
        WriteManaged(previousText);
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, previousText);

        var rendered = "work_mem = '16MB'\n";
        WriteManaged(rendered);

        var rows = new List<FileSettingRow>
        {
            Applied("work_mem", "8MB", file: Path.Combine(_dataDir, ManagedConfFile.FileName)),
        };

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText);

        Assert.Equal(ManagedConfVerificationStatus.Failed, outcome.Status);
        Assert.Equal(ManagedConfMigrationStep.B, outcome.Step);
        Assert.Contains("work_mem", outcome.MismatchedKeys);
        Assert.Equal(previousText, File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName)));
        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    /// <summary>Pin (#4336): a key whose row has <c>Applied: false</c> because an operator line
    /// overrides it lower down still passes, as long as its setting and value agree and it carries no error.</summary>
    [Fact]
    public void VerifyStepB_OverriddenKey_StillPasses()
    {
        var rendered = "work_mem = '16MB'\n";
        WriteManaged(rendered);

        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "work_mem", Setting: "16MB", Applied: false, Error: null),
            Applied("work_mem", "32MB", file: "postgresql.conf", line: 99),
        };

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText: null);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
    }

    /// <summary>Pin (fixes commit 9ae7410c): a row whose sourcefile is <c>old-darling-managed.conf</c> --
    /// which ENDS WITH the managed file's name but is not it -- is not attributed to the managed file, so it
    /// cannot verify the rendered key. A row named exactly <c>DARLING-MANAGED.CONF</c> (case-insensitive) IS
    /// attributed and verifies.</summary>
    [Fact]
    public void VerifyStepB_StrayFileEndingWithTheName_IsNotAttributed_ExactCasedNameIs()
    {
        var rendered = "work_mem = '16MB'\n";
        WriteManaged(rendered);

        var strayRows = new List<FileSettingRow>
        {
            Applied("work_mem", "16MB", file: "C:/x/old-darling-managed.conf"),
        };
        var strayOutcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, strayRows, rendered, previousText: null);

        // The rendered key was never matched by an attributed row, so it counts as a mismatch, not a match.
        Assert.Equal(ManagedConfVerificationStatus.Failed, strayOutcome.Status);
        Assert.Contains("work_mem", strayOutcome.MismatchedKeys);

        WriteManaged(rendered); // VerifyStepB's Failed branch restored/deleted the managed file; rewrite it for the second call.
        var exactRows = new List<FileSettingRow>
        {
            Applied("work_mem", "16MB", file: "C:/x/DARLING-MANAGED.CONF"),
        };
        var exactOutcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, exactRows, rendered, previousText: null);

        Assert.Equal(ManagedConfVerificationStatus.Verified, exactOutcome.Status);
    }

    /// <summary>Pin (#4336): a fresh error row from <c>darling-managed.conf</c> on a rendered key fails
    /// verification even when a differently-sourced row for the same name is applied.</summary>
    [Fact]
    public void VerifyStepB_NewErrorFromManagedFile_Fails()
    {
        var rendered = "work_mem = '16MB'\n";
        WriteManaged(rendered);

        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "work_mem", Setting: "16MB", Applied: false, Error: "invalid value"),
        };

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText: null);

        Assert.Equal(ManagedConfVerificationStatus.Failed, outcome.Status);
        Assert.Contains("work_mem", outcome.MismatchedKeys);
    }

    /// <summary>Pin: <see cref="ManagedConfMigrationRunner.FindUnstampedManagedFileErrors"/> is the
    /// <c>MigratedUnstamped</c> re-verification's own scan (<c>DarlingManagedPostgres.MigrateManagedConfAsync</c>),
    /// which must skip the same <see cref="DarlingStoreHostProfile.CommandLineOnlyKeys"/> as
    /// <see cref="ManagedConfMigrationRunner.VerifyStepB"/> does, for the same reason: an exposed store's
    /// <c>listen_addresses</c> is always overridden by the command line, so darling-managed.conf's rendered
    /// (always loopback-only) line reports <c>error = "setting could not be applied"</c> on every start even
    /// though nothing needs healing. <c>192.0.2.10</c> (RFC 5737) stands in for the store's own address.
    /// </summary>
    [Fact]
    public void FindUnstampedManagedFileErrors_ListenAddressesOverriddenByCommandLine_IsNotAnError()
    {
        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "listen_addresses", Setting: "127.0.0.1", Applied: false, Error: "setting could not be applied"),
            Applied("work_mem", "16MB", file: managedPath, line: 2),
        };

        var (hasError, mismatchedKeys) = ManagedConfMigrationRunner.FindUnstampedManagedFileErrors(rows);

        Assert.False(hasError);
        Assert.Empty(mismatchedKeys);
    }

    /// <summary>Pin: the same skip for <c>port</c> — also command-line-owned
    /// (<see cref="DarlingStoreHostProfile.CommandLineOnlyKeys"/>).</summary>
    [Fact]
    public void FindUnstampedManagedFileErrors_PortOverriddenByCommandLine_IsNotAnError()
    {
        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "port", Setting: "5555", Applied: false, Error: "setting could not be applied"),
            Applied("work_mem", "16MB", file: managedPath, line: 2),
        };

        var (hasError, mismatchedKeys) = ManagedConfMigrationRunner.FindUnstampedManagedFileErrors(rows);

        Assert.False(hasError);
        Assert.Empty(mismatchedKeys);
    }

    /// <summary>Pin: a REAL error row on another key (not command-line-owned) still reports as an error,
    /// with that key named — the skip is narrow, not a blanket "ignore darling-managed.conf errors".</summary>
    [Fact]
    public void FindUnstampedManagedFileErrors_RealErrorOnOtherKey_StillReportsError()
    {
        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "listen_addresses", Setting: "127.0.0.1", Applied: false, Error: "setting could not be applied"),
            new(SourceFile: managedPath, SourceLine: 2, Name: "work_mem", Setting: "16MB", Applied: false, Error: "invalid value"),
        };

        var (hasError, mismatchedKeys) = ManagedConfMigrationRunner.FindUnstampedManagedFileErrors(rows);

        Assert.True(hasError);
        Assert.DoesNotContain("listen_addresses", mismatchedKeys);
        Assert.Contains("work_mem", mismatchedKeys);
    }

    /// <summary>Pin: a store with a configured network endpoint always starts PostgreSQL with
    /// <c>listen_addresses</c> forced onto the command line (<see cref="DarlingManagedPostgres.BuildServerRuntimeOptions"/>).
    /// The rendered <c>darling-managed.conf</c> line stays loopback-only, so PostgreSQL reports THAT file
    /// row with <c>error = "setting could not be applied"</c> once its command-line value differs —
    /// confirmed against a live PostgreSQL 18 instance. <see cref="ManagedConfMigrationRunner.VerifyStepB"/>
    /// must not treat that as a mismatch: <c>listen_addresses</c> is skipped, but a REAL mismatch on another
    /// key in the same batch still fails.</summary>
    [Fact]
    public void VerifyStepB_ListenAddressesOverriddenByCommandLine_IsNotAMismatch_OtherKeyStillFails()
    {
        var rendered = "listen_addresses = '127.0.0.1'\nwork_mem = '16MB'\n";
        WriteManaged(rendered);

        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "listen_addresses", Setting: "127.0.0.1", Applied: false, Error: "setting could not be applied"),
            new(SourceFile: managedPath, SourceLine: 2, Name: "work_mem", Setting: "8MB", Applied: true, Error: null),
        };

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText: null);

        Assert.Equal(ManagedConfVerificationStatus.Failed, outcome.Status);
        Assert.DoesNotContain("listen_addresses", outcome.MismatchedKeys);
        Assert.Contains("work_mem", outcome.MismatchedKeys);
    }

    /// <summary>Pin: when EVERY other key matches and the only rendered key that doesn't is
    /// <c>listen_addresses</c> — overridden by the command line, same as above — Step B now completes
    /// (<c>Verified</c>), writes the verified stamp against the rendered text, and leaves the managed file
    /// in place (no restore of <paramref name="previousText"/>). Before this fix, this row alone drove Step
    /// B to <c>Failed</c> on every start of any store with a configured network endpoint. The row's exact
    /// shape (<c>setting</c> reads the file's rendered value, not the command line's; <c>error</c> reads
    /// <c>"setting could not be applied"</c>; <c>applied</c> is <c>false</c>) was captured from a real
    /// <c>pg_file_settings</c> row on a PostgreSQL 18 container started with a command-line
    /// <c>listen_addresses</c> set to loopback plus the container's own address (<c>docker run
    /// timescale/timescaledb:2.30.1-pg18 -c listen_addresses=127.0.0.1,&lt;container address&gt;</c>, with an
    /// included conf file rendering the product's usual loopback-only <c>listen_addresses = '127.0.0.1'</c>
    /// line), then querying <see cref="ManagedConfFileSettings.SnapshotSql"/> directly against it. The address
    /// below (<c>192.0.2.10</c>) is the RFC 5737 documentation range, standing in for the container's own
    /// address — the captured row shape does not depend on which address is used.</summary>
    [Fact]
    public void VerifyStepB_OnlyListenAddressesOverriddenByCommandLine_Verifies()
    {
        var rendered = "listen_addresses = '127.0.0.1'\nwork_mem = '16MB'\nmax_connections = '200'\n";
        WriteManaged(rendered);

        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "listen_addresses", Setting: "127.0.0.1", Applied: false, Error: "setting could not be applied"),
            Applied("work_mem", "16MB", file: managedPath, line: 2),
            Applied("max_connections", "200", file: managedPath, line: 3),
        };

        var previousText = "work_mem = '8MB'\n";

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
        Assert.Equal(ManagedConfMigrationStep.B, outcome.Step);
        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
        Assert.Equal(rendered, File.ReadAllText(managedPath));
    }

    /// <summary>Pin: the same completion for <c>port</c> — also command-line-owned
    /// (<see cref="DarlingStoreHostProfile.CommandLineOnlyKeys"/>) — whose file row is overridden the same
    /// way when the command line pins a different port than the rendered file line.</summary>
    [Fact]
    public void VerifyStepB_OnlyPortOverriddenByCommandLine_Verifies()
    {
        var rendered = "port = '5555'\nwork_mem = '16MB'\n";
        WriteManaged(rendered);

        var managedPath = Path.Combine(_dataDir, ManagedConfFile.FileName);
        var rows = new List<FileSettingRow>
        {
            new(SourceFile: managedPath, SourceLine: 1, Name: "port", Setting: "5555", Applied: false, Error: "setting could not be applied"),
            Applied("work_mem", "16MB", file: managedPath, line: 2),
        };

        var outcome = ManagedConfMigrationRunner.VerifyStepB(_dataDir, rows, rendered, previousText: null);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);
        Assert.Equal(ManagedConfMigrationStep.B, outcome.Step);
        Assert.True(ManagedConfMigrationSteps.IsVerified(_dataDir));
    }

    /// <summary>Pin (#4336): the exact change-log line for two changed keys.</summary>
    [Fact]
    public void FormatStepBChangeLog_TwoKeys_ExactLine()
    {
        var inputs = SampleInputs(port: 5432);
        var changes = new List<(string Key, string? Old, string New)>
        {
            ("work_mem", "8MB", "16MB"),
            ("max_connections", null, "200"),
        };

        var log = ManagedConfMigrationRunner.FormatStepBChangeLog(changes, inputs);

        var expected =
            "work_mem: 8MB -> 16MB (RAM 16384 MB, authoritative True; platform Windows; PG 18; CPUs 8; hypertables 42)\n" +
            "max_connections: (unset) -> 200 (RAM 16384 MB, authoritative True; platform Windows; PG 18; CPUs 8; hypertables 42)";

        Assert.Equal(expected, log);
    }

    /// <summary>Pin (fixes commit 9ae7410c): a dropped product key — v12's <c>min_wal_size</c>, present in
    /// the before conf as a v12 block but not owned by THIS render (<c>DataVolumeAuthoritative: false</c>) —
    /// is carried into <c>darling-managed.conf</c> from the before snapshot, so the after-snapshot still
    /// reports it and the run Verifies rather than reporting a spurious mismatch on every start.</summary>
    [Fact]
    public async Task RunStepA_DroppedProductKey_CarriedIntoManagedConf_Verifies()
    {
        var v12Block = DarlingManagedPostgres.BuildWalSizingConfAppend(
            freeDiskBytesOnDataVolume: 64L * 1024 * 1024 * 1024,
            totalDiskBytesOnDataVolume: 256L * 1024 * 1024 * 1024,
            postgresMajor: 13);
        WriteConf("# base conf\n" + v12Block);

        // DataVolumeAuthoritative: false -- this render never derives min_wal_size/max_wal_size itself.
        var inputs = SampleInputs() with { DataVolumeAuthoritative = false };
        var derived = DerivedValues(inputs);
        Assert.DoesNotContain("min_wal_size", derived.Keys);

        var beforeRows = new List<FileSettingRow>();
        foreach (var kvp in derived)
        {
            beforeRows.Add(Applied(kvp.Key, kvp.Value));
        }

        // The before snapshot also reports min_wal_size as applied from the v12 block's own value (8192MB / 4 = 2048MB).
        beforeRows.Add(Applied("min_wal_size", "2048MB"));

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(beforeRows);

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);

        var managedText = File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName));
        Assert.Contains("min_wal_size = '2048MB'", managedText, StringComparison.Ordinal);
    }

    /// <summary>Pin (#4336): a carried <c>maintenance_work_mem</c> — present in the BEFORE
    /// snapshot at a pre-#3909 <c>2048MB</c>, which this render does not itself derive (v7's own value is
    /// smaller here) — goes through the same <c>NeedsLegacyMaintenanceWorkMemCap</c> predicate
    /// <see cref="ManagedConfFile.RenderBody"/>'s v14 block uses, so PostgreSQL 17 never receives an
    /// uncapped value it rejects on start.</summary>
    [Fact]
    public async Task RunStepA_CarriedMaintenanceWorkMemOver2047_Postgres17_Caps()
    {
        WriteConf("# base conf\nmaintenance_work_mem = '2048MB'\n");

        var inputs = SampleInputs() with { PostgresMajor = 17 };
        var derived = DerivedValues(inputs);

        var beforeRows = new List<FileSettingRow>();
        foreach (var kvp in derived)
        {
            var value = kvp.Key == DarlingManagedPostgres.MaintenanceWorkMemSetting ? "2048MB" : kvp.Value;
            beforeRows.Add(Applied(kvp.Key, value));
        }

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(beforeRows);

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);

        var managedText = File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName));
        Assert.Contains(
            $"maintenance_work_mem = '{DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB'", managedText, StringComparison.Ordinal);
        Assert.DoesNotContain("maintenance_work_mem = '2048MB'", managedText, StringComparison.Ordinal);
    }

    /// <summary>Pin: the same carried <c>2048MB</c> on PostgreSQL 18 is kept exactly — the cap is major-aware,
    /// same as <see cref="DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap"/> itself.</summary>
    [Fact]
    public async Task RunStepA_CarriedMaintenanceWorkMemOver2047_Postgres18_KeepsIt()
    {
        WriteConf("# base conf\nmaintenance_work_mem = '2048MB'\n");

        var inputs = SampleInputs() with { PostgresMajor = 18 };
        var derived = DerivedValues(inputs);

        var beforeRows = new List<FileSettingRow>();
        foreach (var kvp in derived)
        {
            var value = kvp.Key == DarlingManagedPostgres.MaintenanceWorkMemSetting ? "2048MB" : kvp.Value;
            beforeRows.Add(Applied(kvp.Key, value));
        }

        Func<CancellationToken, Task<IReadOnlyList<FileSettingRow>>> snapshot = _ =>
            Task.FromResult<IReadOnlyList<FileSettingRow>>(beforeRows);

        var logger = new CapturingTestLogger();
        var outcome = await ManagedConfMigrationRunner.RunStepA(
            _dataDir, snapshot, derived, inputs, inputs.Port, UtcNow, logger, CancellationToken.None);

        Assert.Equal(ManagedConfVerificationStatus.Verified, outcome.Status);

        var managedText = File.ReadAllText(Path.Combine(_dataDir, ManagedConfFile.FileName));
        Assert.Contains("maintenance_work_mem = '2048MB'", managedText, StringComparison.Ordinal);
    }
}
