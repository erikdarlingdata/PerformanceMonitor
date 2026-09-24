/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3927: a store upgrade that fails BEFORE its commit point, what it leaves behind, and what it is allowed to
/// say about that. Three defects lived in this failure path, each reproduced on copies of a real 17.10 +
/// TimescaleDB 2.28.1 store. A hard-link run never undid pg_upgrade's rename of the old cluster's
/// <c>global\pg_control</c>, so the store could not start on either runtime while the log said the data
/// directory had never been modified. <see cref="DarlingStoreUpgrade.RevertRuntime"/> moved the runtimes out
/// from under a postmaster the upgrade could not stop. And a revert whose second move failed left no runtime
/// at all. The outcome, and the Failed alert built from it, claimed a clean revert through all three.
///
/// <para>No PostgreSQL is needed. Runtimes and data directories are planted as marker files. A live
/// postmaster is Windows' ping running under the name postgres.exe, because a PID and an image name are all
/// the liveness check can see of a real one. And every failure these tests need is a file held open: an open
/// file anywhere under a directory stops that directory being renamed, and stops the file being moved. A
/// RUNNING executable does not, which is exactly why the live-server refusal has to come from looking.</para>
/// </summary>
public sealed class DarlingStoreUpgradeRevertTests
{
    private const string OldRuntime = "PostgreSQL 17 runtime";
    private const string NewRuntime = "PostgreSQL 18 runtime";
    private const string ControlFileContent = "the old cluster's control file";
    private const string PackageHash = "3927392739273927392739273927392739273927392739273927392739273927";
    private const int OldMajor = 17;

    private static readonly string s_ping = Path.Combine(Environment.SystemDirectory, "PING.EXE");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ==================== the pg_control restore, on its own ==================== */

    /// <summary>
    /// The restore: pg_upgrade's rename, undone. Planted exactly as pg_upgrade leaves it once hard-link mode
    /// begins linking, with <c>pg_control.old</c> present and <c>pg_control</c> gone.
    /// </summary>
    [Fact]
    public void RestoreLinkedControlFile_UndoesPgUpgradesRename()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-restore-");
        try
        {
            var global = Directory.CreateDirectory(Path.Combine(root.FullName, "global")).FullName;
            var renamed = Path.Combine(global, "pg_control.old");
            File.WriteAllText(renamed, ControlFileContent);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var result = DarlingStoreUpgrade.RestoreLinkedControlFile(root.FullName, log);

            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.ControlFileRestored, result);
            Assert.Equal(ControlFileContent, File.ReadAllText(Path.Combine(global, "pg_control")));
            Assert.False(File.Exists(renamed));

            /* ...and it says what it did, naming the file it renamed. */
            var said = Assert.Single(log.Entries, entry => entry.Message.StartsWith("Renamed ", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Warning, said.Level);
            Assert.Contains(renamed, said.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// It acts on exactly pg_upgrade's rename and on nothing else. A control file that is there is never
    /// overwritten, even with a <c>.old</c> beside it, and a directory holding neither gets nothing made up.
    /// </summary>
    [Fact]
    public void RestoreLinkedControlFile_NeverTouchesAControlFileThatIsThere()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-intact-");
        try
        {
            var global = Directory.CreateDirectory(Path.Combine(root.FullName, "global")).FullName;
            var log = new DarlingSelfAlertTests.CapturingLogger();

            Assert.Equal(
                DarlingStoreUpgrade.PreUpgradeDataDirectory.Untouched,
                DarlingStoreUpgrade.RestoreLinkedControlFile(root.FullName, log));
            Assert.Empty(Directory.GetFiles(global));

            File.WriteAllText(Path.Combine(global, "pg_control"), "live");
            File.WriteAllText(Path.Combine(global, "pg_control.old"), "stale");

            Assert.Equal(
                DarlingStoreUpgrade.PreUpgradeDataDirectory.Untouched,
                DarlingStoreUpgrade.RestoreLinkedControlFile(root.FullName, log));
            Assert.Equal("live", File.ReadAllText(Path.Combine(global, "pg_control")));
            Assert.Equal("stale", File.ReadAllText(Path.Combine(global, "pg_control.old")));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A rename it cannot make means the store cannot start on EITHER runtime, and that has to be said at
    /// CRITICAL, naming the exact rename to do by hand. Held open with no sharing, the file cannot be moved:
    /// the deterministic stand-in for a scanner or a shell holding it in the field.
    /// </summary>
    [Fact]
    public void RestoreLinkedControlFile_ARenameItCannotMake_SaysTheStoreCannotStart_AndNamesTheRename()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-stuck-");
        try
        {
            var global = Directory.CreateDirectory(Path.Combine(root.FullName, "global")).FullName;
            var controlFile = Path.Combine(global, "pg_control");
            var renamed = controlFile + ".old";
            File.WriteAllText(renamed, ControlFileContent);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            DarlingStoreUpgrade.PreUpgradeDataDirectory result;
            using (new FileStream(renamed, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                result = DarlingStoreUpgrade.RestoreLinkedControlFile(root.FullName, log);
            }

            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.NotRestored, result);
            Assert.True(File.Exists(renamed));
            Assert.False(File.Exists(controlFile));

            var critical = Assert.Single(log.Entries, entry => entry.Level == LogLevel.Critical);
            Assert.Contains(renamed, critical.Message, StringComparison.Ordinal);
            Assert.Contains("back to " + controlFile + " (", critical.Message, StringComparison.Ordinal);
            Assert.Contains("CANNOT start on either runtime", critical.Message, StringComparison.Ordinal);
            Assert.Contains("by hand", critical.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /* ==================== the pre-commit recovery, at its seam ==================== */

    /// <summary>
    /// THE #3927 case: hard-link mode, pg_upgrade's rename in place, a failure injected before the commit point.
    /// The control file comes back BEFORE the runtime does, and only after both is the store called back and
    /// the data called safe, with the reason that is true in hard-link mode.
    /// </summary>
    [Fact]
    public async Task PreCommitFailure_LinkMode_RestoresPgControlBeforeRevertingTheRuntime()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-link-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            File.Move(host.ControlFile, host.RenamedControlFile);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var outcome = await new DarlingStoreUpgrade(log).RecoverFromPreCommitFailureAsync(
                host.Context, DarlingStoreUpgrade.FileTransferMode.Link, oldStarted: false, host.StagingDirectory,
                "pg_upgrade", "pg_upgrade failed (exit 1).", "2.28.1");

            /* The old cluster can start again, on its own runtime, and the half-built one is gone. */
            Assert.Equal(ControlFileContent, File.ReadAllText(host.ControlFile));
            Assert.False(File.Exists(host.RenamedControlFile));
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.False(Directory.Exists(host.StagingDirectory));
            Assert.Equal(PackageHash, File.ReadAllText(host.BlockedMarker));

            /* The outcome says exactly that. */
            Assert.Equal(DarlingStoreUpgrade.StoreUpgradeStatus.Failed, outcome.Status);
            Assert.Equal("pg_upgrade", outcome.FailedStep);
            Assert.True(outcome.UsedLinkMode);
            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.ControlFileRestored, outcome.PreUpgradeData);
            Assert.True(outcome.RuntimeReverted);

            /* In that order: the data directory was put back before the runtime was. */
            var restored = IndexOf(log, "Renamed ");
            var reverted = IndexOf(log, "Reverted to the previous Postgres runtime");
            Assert.True(restored >= 0 && reverted > restored, Describe(log));

            /* And the reassurance comes last, from what happened, naming why it is true. */
            var last = log.Entries[^1];
            Assert.Equal(LogLevel.Warning, last.Level);
            Assert.Contains("NO data has been lost", last.Message, StringComparison.Ordinal);
            Assert.Contains("pg_control", last.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A restore that fails is the one shape where the store cannot start on either runtime. The runtime is
    /// still reverted, since it is the right one for when the file is back, but nothing may call the store
    /// back or the data safe, and the outcome carries the shape to the alert.
    /// </summary>
    [Fact]
    public async Task PreCommitFailure_LinkMode_ARestoreThatFails_NeverClaimsARunningStore()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-link-stuck-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            File.Move(host.ControlFile, host.RenamedControlFile);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            DarlingStoreUpgrade.StoreUpgradeOutcome outcome;
            using (new FileStream(host.RenamedControlFile, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                outcome = await new DarlingStoreUpgrade(log).RecoverFromPreCommitFailureAsync(
                    host.Context, DarlingStoreUpgrade.FileTransferMode.Link, oldStarted: false, host.StagingDirectory,
                    "pg_upgrade", "pg_upgrade failed (exit 1).", "2.28.1");
            }

            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.NotRestored, outcome.PreUpgradeData);
            Assert.True(outcome.RuntimeReverted);
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.False(File.Exists(host.ControlFile));

            Assert.DoesNotContain(log.Entries, entry => entry.Message.Contains("NO data has been lost", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Entries, entry => entry.Message.Contains("is back on PostgreSQL", StringComparison.Ordinal));

            var last = log.Entries[^1];
            Assert.Equal(LogLevel.Critical, last.Level);
            Assert.Contains("CANNOT start on either runtime", last.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Copy mode is the clean revert the old code assumed everywhere, and it still says so: nothing to put
    /// back, the runtime reverted, and the "never modified" reason, which copy mode makes true.
    /// </summary>
    [Fact]
    public async Task PreCommitFailure_CopyMode_IsTheCleanRevert()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-copy-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var outcome = await new DarlingStoreUpgrade(log).RecoverFromPreCommitFailureAsync(
                host.Context, DarlingStoreUpgrade.FileTransferMode.Copy, oldStarted: false, host.StagingDirectory,
                "pg_upgrade-check", "pg_upgrade --check failed (exit 1).", "2.28.1");

            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.Untouched, outcome.PreUpgradeData);
            Assert.True(outcome.RuntimeReverted);
            Assert.False(outcome.UsedLinkMode);
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.Equal(ControlFileContent, File.ReadAllText(host.ControlFile));

            var last = log.Entries[^1];
            Assert.Equal(LogLevel.Warning, last.Level);
            Assert.Contains("NO data has been lost: the pre-upgrade data directory was never modified", last.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// E12/E13's shape: the upgrade could not stop the old cluster it started, so a postmaster is still running
    /// on the data directory, from the rescued runtime. The revert is refused, the outcome says the runtime was
    /// NOT reverted, and nothing claims a store that is back.
    /// </summary>
    [Fact]
    public async Task PreCommitFailure_AServerItCouldNotStop_IsNeverReportedAsReverted()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-live-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            using var postmaster = FakePostmaster.StartIn(Path.Combine(host.PreviousPgsql, "bin"));
            WritePostmasterPid(host.DataDirectory, postmaster.Pid);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var outcome = await new DarlingStoreUpgrade(log).RecoverFromPreCommitFailureAsync(
                host.Context, DarlingStoreUpgrade.FileTransferMode.Copy, oldStarted: false, host.StagingDirectory,
                "read-cluster-identity", "template0 is missing from pg_database.", "2.28.1");

            Assert.False(outcome.RuntimeReverted);
            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.Untouched, outcome.PreUpgradeData);
            Assert.Equal(NewRuntime, host.RuntimeInPlace());
            Assert.False(postmaster.HasExited);

            Assert.DoesNotContain(log.Entries, entry => entry.Message.Contains("NO data has been lost", StringComparison.Ordinal));

            var last = log.Entries[^1];
            Assert.Equal(LogLevel.Critical, last.Level);
            Assert.Contains("NOT put back", last.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The directory swap's own undo can fail too, leaving nothing at the configured path and the store under
    /// its retained name. Left like that, the next start initializes an EMPTY store and the retention sweep
    /// deletes the real one two starts later. The recovery moves it back, and only then restores the control
    /// file inside it, before the runtime revert.
    /// </summary>
    [Fact]
    public async Task PreCommitFailure_ADataDirectoryTheSwapLeftAside_IsMovedBack()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-aside-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            File.Move(host.ControlFile, host.RenamedControlFile);
            Directory.Move(host.DataDirectory, host.RetainedDirectory);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var outcome = await new DarlingStoreUpgrade(log).RecoverFromPreCommitFailureAsync(
                host.Context, DarlingStoreUpgrade.FileTransferMode.Link, oldStarted: false, host.StagingDirectory,
                "swap-data-directories", "Access to the path is denied.", "2.28.1");

            Assert.False(Directory.Exists(host.RetainedDirectory));
            Assert.Equal("17\n", File.ReadAllText(Path.Combine(host.DataDirectory, "PG_VERSION")));
            Assert.Equal(ControlFileContent, File.ReadAllText(host.ControlFile));
            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.ControlFileRestored, outcome.PreUpgradeData);
            Assert.True(outcome.RuntimeReverted);

            var movedBack = IndexOf(log, "Moved the pre-upgrade data directory back");
            var restored = IndexOf(log, "Renamed ");
            var reverted = IndexOf(log, "Reverted to the previous Postgres runtime");
            Assert.True(movedBack >= 0 && restored > movedBack && reverted > restored, Describe(log));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// ...and when it cannot be moved back, nothing is deleted, the store is not called back, and the CRITICAL
    /// line names the move, the rename inside it, and why restarting first would be the expensive mistake.
    /// </summary>
    [Fact]
    public async Task PreCommitFailure_ADataDirectoryItCannotMoveBack_SaysTheStoreCannotStart()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-stranded-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            File.Move(host.ControlFile, host.RenamedControlFile);
            Directory.Move(host.DataDirectory, host.RetainedDirectory);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            DarlingStoreUpgrade.StoreUpgradeOutcome outcome;
            using (new FileStream(Path.Combine(host.RetainedDirectory, "PG_VERSION"), FileMode.Open, FileAccess.Read, FileShare.None))
            {
                outcome = await new DarlingStoreUpgrade(log).RecoverFromPreCommitFailureAsync(
                    host.Context, DarlingStoreUpgrade.FileTransferMode.Link, oldStarted: false, host.StagingDirectory,
                    "swap-data-directories", "Access to the path is denied.", "2.28.1");
            }

            /* The store's data is exactly where the swap left it. */
            Assert.True(File.Exists(Path.Combine(host.RetainedDirectory, "PG_VERSION")));
            Assert.False(Directory.Exists(host.DataDirectory));
            Assert.Equal(DarlingStoreUpgrade.PreUpgradeDataDirectory.NotRestored, outcome.PreUpgradeData);

            var stranded = Assert.Single(log.Entries, entry => entry.Message.StartsWith("The store's data is at ", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Critical, stranded.Level);
            Assert.Contains(host.RetainedDirectory, stranded.Message, StringComparison.Ordinal);
            Assert.Contains("pg_control.old", stranded.Message, StringComparison.Ordinal);
            Assert.Contains("EMPTY store", stranded.Message, StringComparison.Ordinal);

            Assert.DoesNotContain(log.Entries, entry => entry.Message.Contains("NO data has been lost", StringComparison.Ordinal));
            var last = log.Entries[^1];
            Assert.Equal(LogLevel.Critical, last.Level);
            Assert.Contains("CANNOT start on either runtime", last.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /* ==================== the real catch clauses ==================== */

    /// <summary>
    /// The pre-commit catch clause itself, not only the method it calls: the real
    /// <see cref="DarlingStoreUpgrade.UpgradeDataDirectoryAsync"/>, failing at its first tool because the old
    /// runtime has no pg_ctl.exe. The store is planted under its retained name, a state only the swap really
    /// produces; here it is what makes the put-back observable, since the transfer mode comes from measured
    /// disk space and a test box gets copy mode. A catch clause that no longer reached the recovery leaves the
    /// store where it is.
    /// </summary>
    [Fact]
    public async Task UpgradeDataDirectory_AFailureBeforeTheCommitPoint_RunsTheRecovery()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-catch-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            Directory.Move(host.DataDirectory, host.RetainedDirectory);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var outcome = await new DarlingStoreUpgrade(log).UpgradeDataDirectoryAsync(host.Context, Ct);

            Assert.Equal(DarlingStoreUpgrade.StoreUpgradeStatus.Failed, outcome.Status);
            Assert.Equal("start-old-cluster", outcome.FailedStep);
            Assert.True(File.Exists(Path.Combine(host.DataDirectory, "PG_VERSION")), Describe(log));
            Assert.False(Directory.Exists(host.RetainedDirectory));
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.True(outcome.RuntimeReverted);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The cancellation catch clause, the same way: a shutdown before the commit point puts the data directory
    /// back BEFORE the runtime revert, and records no block, because a shutdown is not a bad package. The old
    /// runtime's pg_ctl.exe is ping, so the upgrade reaches the step that observes the already-cancelled token.
    /// </summary>
    [Fact]
    public async Task UpgradeDataDirectory_AShutdownBeforeTheCommitPoint_PutsTheDataDirectoryBackFirst()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-cancel-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            Directory.Move(host.DataDirectory, host.RetainedDirectory);
            File.Copy(s_ping, Path.Combine(host.PreviousPgsql, "bin", "pg_ctl.exe"));

            using var shutdown = new CancellationTokenSource();
            shutdown.Cancel();

            var log = new DarlingSelfAlertTests.CapturingLogger();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => new DarlingStoreUpgrade(log).UpgradeDataDirectoryAsync(host.Context, shutdown.Token));

            Assert.True(File.Exists(Path.Combine(host.DataDirectory, "PG_VERSION")), Describe(log));
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.False(File.Exists(host.BlockedMarker));

            var movedBack = IndexOf(log, "Moved the pre-upgrade data directory back");
            var reverted = IndexOf(log, "Reverted to the previous Postgres runtime");
            Assert.True(movedBack >= 0 && reverted > movedBack, Describe(log));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The cancellation path's put-back must sit INSIDE the <c>!swapped</c> guard and ahead of the revert. The
    /// behavioral test above shows it runs and in which order; this pins the containment, the way the existing
    /// pin in <see cref="DarlingStoreUpgradeTests"/> pins the revert's: a put-back moved outside the guard would
    /// run after a committed swap and then report the store as back on its old major.
    /// </summary>
    [Fact]
    public void CancellationPath_PutsTheDataDirectoryBackInsideTheGuard_BeforeTheRevert()
    {
        var source = ReadUpgradeSource();

        var method = source.IndexOf("internal async Task<StoreUpgradeOutcome> UpgradeDataDirectoryAsync(", StringComparison.Ordinal);
        Assert.True(method >= 0, "UpgradeDataDirectoryAsync is gone, so this pin can no longer find what it guards");

        var cancel = source.IndexOf("catch (OperationCanceledException)", method, StringComparison.Ordinal);
        Assert.True(cancel > method, "the upgrade's cancellation catch clause is gone");

        var guard = source.IndexOf("if (!swapped)", cancel, StringComparison.Ordinal);
        var putBack = source.IndexOf("PutBackPreUpgradeDataDirectory(context, mode);", cancel, StringComparison.Ordinal);
        var revert = source.IndexOf("RevertRuntimeForCancel(context);", cancel, StringComparison.Ordinal);

        Assert.True(guard > cancel && putBack > guard && revert > putBack,
            "the cancellation path must put the data directory back inside the !swapped guard, before it reverts the runtime");
        Assert.DoesNotContain("}", source[guard..putBack], StringComparison.Ordinal);
    }

    /* ==================== RevertRuntime ==================== */

    /// <summary>
    /// #3927 part 2. Both runtime moves succeed under a live postmaster on Windows (E12), so the refusal has to
    /// come from looking. The fake runs from the rescued runtime's bin, where the real one runs from, and a
    /// running image does not stop that directory being moved, so without the check this revert really happens.
    /// </summary>
    [Fact]
    public void RevertRuntime_RefusesWhileAPostgresServerHoldsTheDataDirectory()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-refuse-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            using var postmaster = FakePostmaster.StartIn(Path.Combine(host.PreviousPgsql, "bin"));
            WritePostmasterPid(host.DataDirectory, postmaster.Pid);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var reverted = new DarlingStoreUpgrade(log).RevertRuntime(host.RuntimeRoot, PackageHash, host.DataDirectory, OldMajor);

            Assert.False(reverted);
            Assert.Equal(NewRuntime, host.RuntimeInPlace());
            Assert.True(File.Exists(Path.Combine(host.PreviousPgsql, "bin", "postgres.exe")),
                "the server's binaries must still be where it runs them from");
            Assert.False(postmaster.HasExited);

            /* Nothing recorded as though it had happened. */
            Assert.False(File.Exists(host.BlockedMarker));
            Assert.Equal(PackageHash, File.ReadAllText(host.StampFile));

            var refusal = Assert.Single(log.Entries, entry => entry.Message.StartsWith("REFUSING to revert", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Critical, refusal.Level);
            Assert.Contains("PID " + postmaster.Pid.ToString(CultureInfo.InvariantCulture), refusal.Message, StringComparison.Ordinal);
            Assert.Contains("stop -D", refusal.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// ...and it is a live POSTGRES process that refuses, not any live PID. Windows reuses PIDs, so the pid
    /// file a crashed postmaster left can name something unrelated, here the test runner itself. A PID nothing
    /// is using, and a file with no PID in it, name no server either.
    /// </summary>
    [Theory]
    [InlineData("another process")]
    [InlineData("no such process")]
    [InlineData("not a pid")]
    [InlineData("")]
    public void RevertRuntime_APidFileThatNamesNoLiveServer_DoesNotBlockIt(string recorded)
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-stale-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            var firstLine = recorded switch
            {
                "another process" => Environment.ProcessId.ToString(CultureInfo.InvariantCulture),
                "no such process" => (int.MaxValue - 3).ToString(CultureInfo.InvariantCulture),
                _ => recorded,
            };
            File.WriteAllText(Path.Combine(host.DataDirectory, "postmaster.pid"), firstLine + "\n" + host.DataDirectory + "\n");

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var reverted = new DarlingStoreUpgrade(log).RevertRuntime(host.RuntimeRoot, PackageHash, host.DataDirectory, OldMajor);

            Assert.True(reverted, Describe(log));
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>A standalone (single-user) backend records its PID negated, and holds the directory just the same.</summary>
    [Fact]
    public void FindLivePostmaster_ReadsThePidEitherWayPostgresRecordsIt()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-pid-");
        try
        {
            using var postmaster = FakePostmaster.StartIn(Path.Combine(root.FullName, "bin"));
            var dataDirectory = Directory.CreateDirectory(Path.Combine(root.FullName, "pg")).FullName;

            Assert.Null(DarlingStoreUpgrade.FindLivePostmaster(dataDirectory));

            WritePostmasterPid(dataDirectory, postmaster.Pid);
            Assert.Equal(postmaster.Pid, DarlingStoreUpgrade.FindLivePostmaster(dataDirectory));

            WritePostmasterPid(dataDirectory, -postmaster.Pid);
            Assert.Equal(postmaster.Pid, DarlingStoreUpgrade.FindLivePostmaster(dataDirectory));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3927 part 3. Each rename is all-or-nothing, but the pair is not: with the live runtime already moved
    /// aside, a second move that failed left NO runtime at all. A file held open under the rescued runtime
    /// makes exactly that happen, since nothing is open under the live one.
    /// </summary>
    [Fact]
    public void RevertRuntime_ASecondMoveThatFails_PutsTheFirstOneBack()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-halfway-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            var rescuedMarker = Path.Combine(host.PreviousPgsql, "bin", "runtime.txt");

            var log = new DarlingSelfAlertTests.CapturingLogger();
            bool reverted;
            using (new FileStream(rescuedMarker, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                reverted = new DarlingStoreUpgrade(log).RevertRuntime(host.RuntimeRoot, PackageHash, host.DataDirectory, OldMajor);
            }

            Assert.False(reverted);
            Assert.True(Directory.Exists(host.CurrentPgsql),
                "a revert that failed halfway must never leave the service with no runtime at all");
            Assert.Equal(NewRuntime, host.RuntimeInPlace());
            Assert.False(Directory.Exists(host.CurrentPgsql + ".failed"));

            /* The rescued copy is still where a hand revert needs it, and nothing was recorded as reverted. */
            Assert.Equal(OldRuntime, File.ReadAllText(rescuedMarker));
            Assert.False(File.Exists(host.BlockedMarker));
            Assert.Equal(PackageHash, File.ReadAllText(host.StampFile));

            var critical = Assert.Single(log.Entries, entry => entry.Level == LogLevel.Critical);
            Assert.Contains("was put back", critical.Message, StringComparison.Ordinal);
            Assert.Contains(host.PreviousPgsql, critical.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A marker that will not write after a revert that DID happen is bookkeeping, not a failed revert. It used
    /// to land in the revert's catch as CRITICAL "Could not revert", naming a hand restore from a rescued copy
    /// that was already gone. The stamp stays instead, because it still names the failed package and so still
    /// stops the next start from retrying it.
    /// </summary>
    [Fact]
    public void RevertRuntime_AMarkerItCannotWrite_IsStillAReportedRevert()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-marker-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);
            Directory.CreateDirectory(host.BlockedMarker);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var reverted = new DarlingStoreUpgrade(log).RevertRuntime(host.RuntimeRoot, PackageHash, host.DataDirectory, OldMajor);

            Assert.True(reverted, Describe(log));
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.Equal(PackageHash, File.ReadAllText(host.StampFile));
            Assert.DoesNotContain(log.Entries, entry => entry.Level == LogLevel.Critical);
            Assert.Contains(log.Entries, entry =>
                entry.Level == LogLevel.Warning && entry.Message.Contains("could not record the failed package", StringComparison.Ordinal));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>The ordinary revert, now with an answer: true, the block recorded, the stamp dropped.</summary>
    [Fact]
    public void RevertRuntime_Reverts_RecordsTheBlock_AndSaysSo()
    {
        var root = Directory.CreateTempSubdirectory("darling-3927-revert-");
        try
        {
            var host = PlantFailedUpgrade(root.FullName);

            var log = new DarlingSelfAlertTests.CapturingLogger();
            var reverted = new DarlingStoreUpgrade(log).RevertRuntime(host.RuntimeRoot, PackageHash, host.DataDirectory, OldMajor);

            Assert.True(reverted, Describe(log));
            Assert.Equal(OldRuntime, host.RuntimeInPlace());
            Assert.Equal(PackageHash, File.ReadAllText(host.BlockedMarker));
            Assert.False(File.Exists(host.StampFile));

            /* #4052: the cleanup now EMPTIES pg-runtime-prev rather than deleting and recreating it, so the
               folder itself survives (it is not the service's to remove under the narrowed install-root
               grant), but nothing is left inside it. */
            var previousRoot = DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot);
            Assert.True(Directory.Exists(previousRoot));
            Assert.Empty(Directory.EnumerateFileSystemEntries(previousRoot));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /* ==================== the outcome, through to the alert ==================== */

    /// <summary>
    /// THE SEAM: the worker's mapping is the hop between what the failure put back and what the alert may
    /// claim. An evaluator test passes in its own report and cannot see this hop, which is how the success
    /// arm once dropped its warning here. The enum travels as its name because a public test method cannot
    /// take an internal type.
    /// </summary>
    [Theory]
    [InlineData("Untouched", true)]
    [InlineData("ControlFileRestored", true)]
    [InlineData("NotRestored", true)]
    [InlineData("Untouched", false)]
    [InlineData("NotRestored", false)]
    public void BuildStoreUpgradeReport_TheFailedArmCarriesWhatTheFailurePutBack(string preUpgradeData, bool runtimeReverted)
    {
        var data = Enum.Parse<DarlingStoreUpgrade.PreUpgradeDataDirectory>(preUpgradeData);

        var report = DarlingWorker.BuildStoreUpgradeReport(new DarlingStoreUpgrade.StoreUpgradeOutcome(
            DarlingStoreUpgrade.StoreUpgradeStatus.Failed, 17, 18, "2.28.1", "2.28.1",
            "pg_upgrade", "pg_upgrade failed (exit 1).", UsedLinkMode: true, data, runtimeReverted));

        Assert.NotNull(report);
        Assert.False(report!.Succeeded);
        Assert.Equal(runtimeReverted, report.RuntimeReverted);
        Assert.Equal(data == DarlingStoreUpgrade.PreUpgradeDataDirectory.ControlFileRestored, report.ControlFileRestored);
        Assert.Equal(data == DarlingStoreUpgrade.PreUpgradeDataDirectory.NotRestored, report.DataDirectoryNotRestored);
    }

    /// <summary>
    /// A hard-link failure whose control file was put back IS a clean revert, and the alert may say so, but not
    /// on the old reason ("the pre-upgrade data directory is never modified until the upgrade succeeds"), which
    /// hard-link mode makes false. It says what was undone instead.
    /// </summary>
    [Fact]
    public async Task FailedAlert_ControlFileRestored_ReassuresWithTheTrueReason()
    {
        var fired = await FireFailedUpgradeAsync(controlFileRestored: true);

        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Contains("collecting normally", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("no data was lost", fired.DetailText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("pg_control.old", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("undone", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("never modified", fired.DetailText, StringComparison.Ordinal);
        Assert.EndsWith("reverted, still running", fired.ShortMessage, StringComparison.Ordinal);
    }

    /// <summary>
    /// A data directory that could not be put back means no store starts, so this alert cannot really be
    /// delivered. It must still never call the store running or reverted, or claim the data safe on the
    /// strength of a revert, whichever way the runtime went.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FailedAlert_DataDirectoryNotRestored_NeverClaimsARunningStore(bool runtimeReverted)
    {
        var fired = await FireFailedUpgradeAsync(runtimeReverted: runtimeReverted, dataDirectoryNotRestored: true);

        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.DoesNotContain("collecting normally", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("no data was lost", fired.DetailText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("reverted", fired.ShortMessage, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CANNOT start", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("CANNOT start on either runtime", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("pg_control.old", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(
            !runtimeReverted,
            fired.DetailText!.Contains("runtime has to be put back by hand as well", StringComparison.Ordinal));
    }

    /// <summary>
    /// A revert that was refused, or that failed, is not a revert, and the alert names what the operator has to
    /// do before the next restart, which is the one that fails.
    /// </summary>
    [Fact]
    public async Task FailedAlert_RuntimeNotReverted_NeverCallsItARevert_AndNamesTheManualStep()
    {
        var fired = await FireFailedUpgradeAsync(runtimeReverted: false);

        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.DoesNotContain("The store reverted", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("collecting normally", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("reverted, still running", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("NOT reverted", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("could NOT be put back", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("NEXT service start will fail", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains(@"pg-runtime-prev\pgsql", fired.DetailText, StringComparison.Ordinal);
    }

    /* ==================== fixtures ==================== */

    private static async Task<AlertOutcome> FireFailedUpgradeAsync(
        bool runtimeReverted = true, bool controlFileRestored = false, bool dataDirectoryNotRestored = false)
    {
        var harness = new DarlingSelfAlertTests.Harness();
        await harness.Build().EvaluateStoreUpgradeAsync(
            new DarlingSelfAlertEvaluator.StoreUpgradeReport(
                Succeeded: false, FromMajor: 17, ToMajor: 18,
                FromTimescale: "2.28.1", ToTimescale: "2.28.1",
                FailedStep: "pg_upgrade", FailureMessage: "pg_upgrade failed (exit 1).", WithoutRollbackCopy: false,
                RuntimeReverted: runtimeReverted, ControlFileRestored: controlFileRestored,
                DataDirectoryNotRestored: dataDirectoryNotRestored),
            Ct);

        return Assert.Single(harness.Deliverer.Outcomes);
    }

    /// <summary>
    /// A host one failed upgrade in, as the upgrade leaves it when its failure handling starts: the previous
    /// runtime rescued to pg-runtime-prev, the new one extracted and stamped, the store still on 17 with its
    /// control file in place, and the half-built 18 cluster beside it. Each runtime is a marker file, so which
    /// one is live can be read back.
    /// </summary>
    private static FailedUpgradeHost PlantFailedUpgrade(string root)
    {
        var runtimeRoot = Path.Combine(root, "deploy", "pg-runtime");
        var currentPgsql = Path.Combine(runtimeRoot, "pgsql");
        var previousPgsql = Path.Combine(DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot), "pgsql");
        foreach (var (pgsql, runtime) in new[] { (currentPgsql, NewRuntime), (previousPgsql, OldRuntime) })
        {
            Directory.CreateDirectory(Path.Combine(pgsql, "bin"));
            File.WriteAllText(Path.Combine(pgsql, "bin", "runtime.txt"), runtime);
        }

        /* The swap writes this once the new runtime is extracted; a completed revert is what drops it. */
        File.WriteAllText(Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName), PackageHash);

        var dataDirectory = Path.Combine(root, "store", "pg");
        Directory.CreateDirectory(Path.Combine(dataDirectory, "global"));
        File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "17\n");
        File.WriteAllText(Path.Combine(dataDirectory, "global", "pg_control"), ControlFileContent);

        var staging = dataDirectory + DarlingStoreUpgrade.UpgradeStagingDirectorySuffix + "18";
        Directory.CreateDirectory(staging);
        File.WriteAllText(Path.Combine(staging, "PG_VERSION"), "18\n");

        return new FailedUpgradeHost(runtimeRoot, currentPgsql, previousPgsql, dataDirectory, staging);
    }

    private sealed record FailedUpgradeHost(
        string RuntimeRoot, string CurrentPgsql, string PreviousPgsql, string DataDirectory, string StagingDirectory)
    {
        public string StampFile => Path.Combine(RuntimeRoot, DarlingStoreUpgrade.RuntimeStampFileName);

        public string BlockedMarker => Path.Combine(RuntimeRoot, DarlingStoreUpgrade.RuntimeBlockedFileName);

        public string ControlFile => Path.Combine(DataDirectory, "global", "pg_control");

        public string RenamedControlFile => ControlFile + ".old";

        public string RetainedDirectory => DarlingStoreUpgrade.RetainedDataDirectoryFor(DataDirectory, OldMajor);

        /* Nothing here connects to anything: the password, port and conf hook exist for the record's shape. */
        public DarlingStoreUpgrade.UpgradeContext Context => new(
            OldBinDirectory: Path.Combine(PreviousPgsql, "bin"),
            NewBinDirectory: Path.Combine(CurrentPgsql, "bin"),
            RuntimeRoot: RuntimeRoot,
            ZipHash: PackageHash,
            DataDirectory: DataDirectory,
            Port: 5432,
            UserName: "darling",
            Password: "unused",
            OldMajor: OldMajor,
            NewMajor: 18,
            BundledTimescaleVersion: "2.28.1",
            AppendManagedConf: static _ => { });

        /// <summary>Which runtime is live at pg-runtime\pgsql, read from its marker.</summary>
        public string RuntimeInPlace() => File.ReadAllText(Path.Combine(CurrentPgsql, "bin", "runtime.txt"));
    }

    /// <summary>
    /// A live process named postgres: Windows' ping, copied under that name, pinging loopback for a minute.
    /// Harmless, and a PID and an image name are all the liveness check can see of a real postmaster. Killed
    /// on dispose, whatever the test did.
    /// </summary>
    private sealed class FakePostmaster : IDisposable
    {
        private readonly Process _process;

        private FakePostmaster(Process process) => _process = process;

        public int Pid => _process.Id;

        public bool HasExited => _process.HasExited;

        public static FakePostmaster StartIn(string directory)
        {
            Directory.CreateDirectory(directory);
            var image = Path.Combine(directory, "postgres.exe");
            File.Copy(s_ping, image, overwrite: true);

            var process = Process.Start(new ProcessStartInfo(image, "-n 60 127.0.0.1")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            }) ?? throw new InvalidOperationException("Could not start " + image);

            /* Drained, so a full pipe can never stall it: it has to stay alive for the whole test. */
            process.OutputDataReceived += static (_, _) => { };
            process.ErrorDataReceived += static (_, _) => { };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            return new FakePostmaster(process);
        }

        public void Dispose()
        {
            try
            {
                if (!_process.HasExited)
                {
                    _process.Kill();
                    _process.WaitForExit(10_000);
                }
            }
            catch (InvalidOperationException)
            {
                /* It exited on its own between the check and the kill. */
            }

            _process.Dispose();
        }
    }

    /// <summary>A postmaster.pid in PostgreSQL's layout. Only the first line, the PID, is read.</summary>
    private static void WritePostmasterPid(string dataDirectory, int pid)
        => File.WriteAllText(
            Path.Combine(dataDirectory, "postmaster.pid"),
            pid.ToString(CultureInfo.InvariantCulture) + "\n" + dataDirectory + "\n1758585600\n5432\n\n127.0.0.1\n");

    private static int IndexOf(DarlingSelfAlertTests.CapturingLogger log, string messageStart)
        => log.Entries.FindIndex(entry => entry.Message.StartsWith(messageStart, StringComparison.Ordinal));

    private static string Describe(DarlingSelfAlertTests.CapturingLogger log)
        => "--- log ---\n" + string.Join("\n", log.Entries.Select(entry => $"[{entry.Level}] {entry.Message}"));

    private static string ReadUpgradeSource()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "DarlingStoreUpgrade.cs");
        Assert.True(File.Exists(path),
            "DarlingStoreUpgrade.cs was not copied beside the test binary; check the csproj None/Link item.");
        return File.ReadAllText(path);
    }

    private static void TryDeleteTree(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A leftover temp tree is not a test failure. */
        }
    }
}
