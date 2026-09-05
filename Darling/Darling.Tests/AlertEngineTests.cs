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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the shared <see cref="AlertEngine"/>'s gating semantics (Phase-5 slice D) against fakes.
/// Every expectation is derived from Lite's <c>MainWindow.AlertEngine.cs</c> (the transplant
/// source) — the Lite line is cited per pin — so an engine change that diverges from Lite's
/// behavior must consciously update both the pin and the forwarding plan.
/// </summary>
public sealed class AlertEngineTests
{
    private const string Key = "101";
    private const string Name = "SRV-A";

    /* ---------------- fakes ---------------- */

    private sealed class FakeSettings : IAlertEngineSettings
    {
        /* Lite's App.xaml.cs defaults for thresholds; per-alert enables default OFF here so each
           test switches on exactly the check it pins (a disabled check must not even fetch). */
        public bool AlertsEnabled { get; set; } = true;
        public bool CpuEnabled { get; set; }
        public bool BlockingEnabled { get; set; }
        public bool DeadlockEnabled { get; set; }
        public bool PoisonWaitEnabled { get; set; }
        public bool LongRunningQueryEnabled { get; set; }
        public bool TempDbSpaceEnabled { get; set; }
        public bool LowDiskEnabled { get; set; }
        public bool LongRunningJobEnabled { get; set; }
        public bool FailedJobEnabled { get; set; }
        public bool PvsEnabled { get; set; }
        public bool DatabaseStateEnabled { get; set; }
        public bool ForcePlanFailureEnabled { get; set; } = true;
        public int CpuThresholdPercent { get; set; } = 80;
        public int BlockingCountThreshold { get; set; } = 1;
        /* #1839: 0 = off, the shipped default — a test must opt in for the wait gate to run at all. */
        public int BlockingWaitSecondsThreshold { get; set; }
        public int DeadlockCountThreshold { get; set; } = 1;
        public int PoisonWaitThresholdMs { get; set; } = 500;
        public int LongRunningQueryThresholdMinutes { get; set; } = 30;
        public int LongRunningQueryMaxResults { get; set; } = 5;
        public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
        public bool LongRunningQueryExcludeWaitFor { get; set; } = true;
        public bool LongRunningQueryExcludeBackups { get; set; } = true;
        public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;
        public bool LongRunningQueryExcludeCdc { get; set; } = true;
        public int TempDbSpaceThresholdPercent { get; set; } = 80;
        public int LowDiskThresholdPercent { get; set; } = 10;
        public int LowDiskThresholdGb { get; set; } = 5;
        /* #2107: the previously-hardcoded knobs, at their shipped defaults. */
        public int DiskCriticalFreePercent { get; set; } = 3;
        public int DiskCriticalFreeGb { get; set; } = 2;
        public int SelfDiskFreeWarnPercent { get; set; } = 10;
        public int CollectionStaleMinutes { get; set; } = 30;
        public int CollectionFailureThreshold { get; set; } = 10;
        /* #1984: DarlingConfig defaults (40% / 1 GB); enable stays the class's opt-in OFF. */
        public int PvsThresholdPercent { get; set; } = 40;
        public int PvsFloorGb { get; set; } = 1;

        /* #2349: OFF in the fakes so existing expectations are untouched. */
        public bool FileGrowthEnabled { get; set; }
        public int FileGrowthRiseMb { get; set; } = 10240;
        public int FileGrowthVolumePercent { get; set; } = 60;
        public int FileGrowthLookbackMinutes { get; set; } = 60;
        public int LongRunningJobMultiplier { get; set; } = 3;
        public int FailedJobLookbackMinutes { get; set; } = 60;
        public int CooldownMinutes { get; set; } = 5;
        public List<string> ExcludedDatabasesList { get; } = new();
        public IReadOnlyList<string> ExcludedDatabases => ExcludedDatabasesList;
        public CpuAlertMode CpuAlertMode { get; set; } = CpuAlertMode.TotalServer;
    }

    private sealed class FakeReadAdapter : IAlertReadAdapter
    {
        public List<BlockedProcessAlertRow> Blocking { get; } = new();
        public List<DeadlockAlertRow> Deadlocks { get; } = new();
        public List<PoisonWaitDelta> PoisonWaits { get; } = new();
        public List<LongRunningQueryInfo> LongRunning { get; } = new();
        public List<VolumeFreeSpaceInfo> Volumes { get; } = new();
        public List<PvsPressureInfo> PvsDatabases { get; } = new();
        public TempDbSpaceInfo? TempDb { get; set; }
        public List<AnomalousJobInfo> AnomalousJobs { get; } = new();

        public int BlockingFetches { get; private set; }
        public int DeadlockFetches { get; private set; }
        public int BlockingWaitFetches { get; private set; }

        /* #1839: null = the store holds no blocking snapshot at all (the shipped state of a server that
           has never blocked); tests that exercise the gate assign a result. */
        public CurrentBlockingWaitResult? BlockingWait { get; set; }
        public (int ThresholdMinutes, int MaxResults, bool Diag, bool WaitFor, bool Backups, bool Misc, bool Cdc, IReadOnlyList<string> Excluded)? LastLrqArgs { get; private set; }

        public Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default)
        {
            BlockingFetches++;
            return Task.FromResult(new List<BlockedProcessAlertRow>(Blocking));
        }

        public Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(string serverKey, CancellationToken cancellationToken = default)
        {
            BlockingWaitFetches++;
            return Task.FromResult(BlockingWait);
        }

        public Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default)
        {
            DeadlockFetches++;
            return Task.FromResult(new List<DeadlockAlertRow>(Deadlocks));
        }

        public Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(string serverKey, double thresholdMs, CancellationToken cancellationToken = default) =>
            /* The seam contract: fetch-then-filter client-side, like Lite's loop. */
            Task.FromResult(PoisonWaits.FindAll(w => w.AvgMsPerWait >= thresholdMs));

        public Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
            string serverKey, int thresholdMinutes, int maxResults,
            bool excludeSpServerDiagnostics, bool excludeWaitFor, bool excludeBackups, bool excludeMiscWaits, bool excludeCdc,
            IReadOnlyList<string> excludedDatabases, CancellationToken cancellationToken = default)
        {
            LastLrqArgs = (thresholdMinutes, maxResults, excludeSpServerDiagnostics, excludeWaitFor, excludeBackups, excludeMiscWaits, excludeCdc, excludedDatabases);
            return Task.FromResult(new List<LongRunningQueryInfo>(LongRunning));
        }

        public Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<VolumeFreeSpaceInfo>(Volumes));

        /* #2349: empty on purpose. These tests exercise other alerts, and a fabricated file would
           make the file-growth gate fire inside an unrelated scenario. */
        public Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(
            string serverKey, int lookbackMinutes, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DatabaseFileGrowthInfo>());

        public Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(TempDb);

        public Task<List<PvsPressureInfo>> GetPvsPressureAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<PvsPressureInfo>(PvsDatabases));

        /* #1812: fakes report a FRESH snapshot by default so every pre-existing scenario keeps its
           meaning; the staleness tests flip SnapshotIsStale to model a dead collector. */
        public bool SnapshotIsStale { get; set; }

        public Task<AnomalousJobsResult> GetAnomalousJobsAsync(string serverKey, int multiplier, CancellationToken cancellationToken = default) =>
            Task.FromResult(SnapshotIsStale
                ? AnomalousJobsResult.Stale
                : new AnomalousJobsResult(SnapshotIsFresh: true, new List<AnomalousJobInfo>(AnomalousJobs)));

        /* Database-state deviations the engine should fire on — the store's baseline/ignore comparison
           is already applied, so tests set the deviating rows directly. */
        public List<DatabaseStateInfo> DatabaseStates { get; } = new();
        public int DatabaseStateFetches { get; private set; }

        public Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(string serverKey, CancellationToken cancellationToken = default)
        {
            DatabaseStateFetches++;
            return Task.FromResult(new List<DatabaseStateInfo>(DatabaseStates));
        }

        /* #2157: plantable rows + a fetch counter, mirroring the database-state seam above so the
           forced-plan alert's tests can assert both what fired and that the read happened. */
        public List<ForcePlanFailureInfo> ForcePlanFailures { get; } = new();

        public int ForcePlanFetches { get; private set; }

        public Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(string serverKey, CancellationToken cancellationToken = default)
        {
            ForcePlanFetches++;
            return Task.FromResult(new List<ForcePlanFailureInfo>(ForcePlanFailures));
        }
    }

    private sealed class FakeStateStore : IAlertStateStore
    {
        public Dictionary<(string Key, string Metric), int> EdgeWatermarks { get; } = new();
        public Dictionary<string, DateTime> FailedJobWatermarks { get; } = new();
        public List<(string Key, string Metric, int Watermark)> SavedEdge { get; } = new();
        public List<(string Key, DateTime Watermark)> SavedFailedJob { get; } = new();

        public Task<int?> LoadEdgeTriggerWatermarkAsync(string serverKey, string metricName) =>
            Task.FromResult(EdgeWatermarks.TryGetValue((serverKey, metricName), out var w) ? (int?)w : null);

        public Task SaveEdgeTriggerWatermarkAsync(string serverKey, string metricName, int watermark)
        {
            EdgeWatermarks[(serverKey, metricName)] = watermark;
            SavedEdge.Add((serverKey, metricName, watermark));
            return Task.CompletedTask;
        }

        public Task<DateTime?> LoadFailedJobWatermarkAsync(string serverKey) =>
            Task.FromResult(FailedJobWatermarks.TryGetValue(serverKey, out var w) ? (DateTime?)w : null);

        public Task SaveFailedJobWatermarkAsync(string serverKey, DateTime watermark)
        {
            FailedJobWatermarks[serverKey] = watermark;
            SavedFailedJob.Add((serverKey, watermark));
            return Task.CompletedTask;
        }

        /* #2216: real per-fingerprint occurrence state, so the engine tests can assert what the accumulator
           wrote AND seed a prior incident to accumulate against. */
        public Dictionary<(string Key, string Metric), Dictionary<string, IncidentOccurrenceState>> Occurrences { get; } = new();
        public List<(string Key, string Metric, int Count)> SavedOccurrences { get; } = new();

        public Task<IReadOnlyDictionary<string, IncidentOccurrenceState>> LoadIncidentOccurrencesAsync(string serverKey, string metricName) =>
            Task.FromResult<IReadOnlyDictionary<string, IncidentOccurrenceState>>(
                Occurrences.TryGetValue((serverKey, metricName), out var states)
                    ? states
                    : new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal));

        public Task SaveIncidentOccurrencesAsync(string serverKey, string metricName, IReadOnlyDictionary<string, IncidentOccurrenceState> states)
        {
            /* Replace-the-set, exactly like both real stores: whatever arrives IS the metric's state, so an
               empty map clears it. A fake that merged instead would hide the falling-edge bug class. */
            Occurrences[(serverKey, metricName)] = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal);
            foreach (var entry in states)
            {
                Occurrences[(serverKey, metricName)][entry.Key] = entry.Value;
            }
            SavedOccurrences.Add((serverKey, metricName, states.Count));
            return Task.CompletedTask;
        }

        /* #2166 */
        public List<(string Server, string Db, string State)> DatabaseStateAlerted { get; } = new();

        public Task SaveDatabaseStateAlertedAsync(string serverKey, string databaseName, string effectiveState)
        {
            DatabaseStateAlerted.Add((serverKey, databaseName, effectiveState));
            Memory[databaseName] = effectiveState;
            return Task.CompletedTask;
        }

        public List<(string Server, string Db)> DatabaseStateCleared { get; } = new();

        /// <summary>
        /// What the store would HOLD, not merely which calls arrived. The engine's edge trigger is a
        /// round trip — write on fire, read back through the adapter next cycle — and a stub that only
        /// counts calls cannot fail when one direction of that trip is missing. A test can feed this
        /// back in as LastAlertedState to exercise the real composition.
        /// </summary>
        public Dictionary<string, string> Memory { get; } = new(StringComparer.OrdinalIgnoreCase);

        public Task ClearDatabaseStateAlertedAsync(string serverKey, string databaseName)
        {
            DatabaseStateCleared.Add((serverKey, databaseName));
            Memory.Remove(databaseName);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }
    }

    /// <summary>One engine + fakes + a controllable clock per test.</summary>
    private sealed class Harness
    {
        public FakeSettings Settings { get; } = new();
        public FakeReadAdapter Adapter { get; } = new();
        public FakeStateStore StateStore { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public List<AlertResolution> Resolutions { get; } = new();
        public List<FailedJobInfo> FailedJobs { get; } = new();
        public int FailedJobFetches { get; private set; }
        public bool Muted { get; set; }
        public DateTime Now { get; set; } = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        /* #3013: the swallowed-read counter is an OPTIONAL harness input, defaulting to null, so every
           pin above builds an engine that counts nothing and no test can leak into another's totals or
           into the process-wide AlertReadFailureCounter.Shared. */
        public AlertReadFailureCounter? ReadFailures { get; set; }

        public AlertEngine Build(bool withFailedJobsFetcher = false) => new(
            Settings, Adapter, StateStore, Deliverer,
            isAlertMuted: _ => Muted,
            failedJobsFetcher: withFailedJobsFetcher
                ? (_, _, _) => { FailedJobFetches++; return Task.FromResult(new List<FailedJobInfo>(FailedJobs)); }
                : null,
            resolutionCallback: (r, _) => { Resolutions.Add(r); return Task.CompletedTask; },
            logger: null,
            utcNow: () => Now,
            readFailures: ReadFailures);

        public static AlertServerSnapshot Snapshot(
            double? sqlCpu = null, double? totalCpu = null,
            bool isOnline = true, bool isAzureSqlDb = false, bool suppressed = false) =>
            new(Key, Name, isOnline, sqlCpu, totalCpu, isAzureSqlDb, suppressed);
    }

    private static BlockedProcessAlertRow BlockingRow(
        int blockedSpid, string source = BlockedProcessAlertRow.XeReportSource, string database = "StackOverflow") => new()
    {
        EventTime = new DateTime(2026, 7, 1, 11, 55, 0),
        DatabaseName = database,
        BlockedSpid = blockedSpid,
        BlockingSpid = blockedSpid + 100,
        WaitTimeMs = 12000,
        LockMode = "X",
        BlockedSqlText = "UPDATE Users SET x = 1",
        BlockingSqlText = "BEGIN TRAN UPDATE Users",
        ContentiousObject = "StackOverflow.dbo.Users",
        Source = source
    };

    private static DeadlockAlertRow DeadlockRow(string database = "StackOverflow") => new()
    {
        VictimProcessId = "process1",
        VictimSqlText = "UPDATE Users SET Reputation = 1",
        DeadlockGraphXml =
            $@"<deadlock><victim-list><victimProcess id=""process1""/></victim-list><process-list><process id=""process1"" spid=""55"" currentdbname=""{database}""><inputbuf>UPDATE Users SET Reputation = 1</inputbuf></process><process id=""process2"" spid=""60"" currentdbname=""{database}""><inputbuf>UPDATE Badges SET Name = 'x'</inputbuf></process></process-list><resource-list><keylock objectname=""{database}.dbo.Users""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock></resource-list></deadlock>"
    };

    /* ---------------- master switch ---------------- */

    [Fact]
    public async Task AlertsDisabled_RunsNoChecksAtAll()
    {
        /* Lite AlertEngine.cs:38 — the master gate short-circuits the whole sweep. */
        var h = new Harness();
        h.Settings.AlertsEnabled = false;
        h.Settings.CpuEnabled = true;
        h.Settings.BlockingEnabled = true;

        await h.Build().EvaluateServerAsync(Harness.Snapshot(sqlCpu: 99, totalCpu: 99));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Equal(0, h.Adapter.BlockingFetches);
    }

    /* ---------------- CPU ---------------- */

    [Fact]
    public async Task Cpu_FiresAtThresholdInclusive_ThenCooldownSuppressesRepeat()
    {
        /* Lite AlertEngine.cs:65-67 (>= threshold) and :72 (cooldown gates the repeat). */
        var h = new Harness();
        h.Settings.CpuEnabled = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 80));
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("High CPU", fired.MetricName);
        Assert.Equal("80% (Total CPU)", fired.CurrentValue);    /* :82 current-value shape, :64 label */
        Assert.Equal("80%", fired.ThresholdValue);
        Assert.Null(fired.Context);                              /* :91-98 — CPU passes no context */
        /* #1830: the numerics are REQUIRED — without them the history stores text-parsed
           "80% (Total CPU)", failed on the parenthesized label, and stored 0 for every row. */
        Assert.Equal(80d, fired.NumericCurrentValue);
        Assert.Equal(80d, fired.NumericThresholdValue);
        Assert.False(fired.Muted);

        /* Same breach 1 minute later: inside the 5-minute cooldown — no repeat (:72). */
        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 85));
        Assert.Single(h.Deliverer.Outcomes);

        /* After the cooldown elapses the standing breach re-fires (CPU is level-triggered). */
        h.Now = h.Now.AddMinutes(5);
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 85));
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task Cpu_ModeSelection_HappensInsideTheEngine()
    {
        /* CpuPercentForAlert semantics (Lite LocalDataService.Overview.cs:143-144):
           Total → TotalCpuPercent ?? CpuPercent; SqlOnly → CpuPercent. */
        var h = new Harness();
        h.Settings.CpuEnabled = true;

        /* SqlProcess mode compares the SQL value even when total is higher. */
        h.Settings.CpuAlertMode = CpuAlertMode.SqlProcess;
        await h.Build().EvaluateServerAsync(Harness.Snapshot(sqlCpu: 50, totalCpu: 95));
        Assert.Empty(h.Deliverer.Outcomes);

        await h.Build().EvaluateServerAsync(Harness.Snapshot(sqlCpu: 85, totalCpu: 95));
        Assert.Equal("85% (SQL CPU)", Assert.Single(h.Deliverer.Outcomes).CurrentValue);

        /* TotalServer mode falls back to the SQL value when no total is available. */
        h.Deliverer.Outcomes.Clear();
        h.Settings.CpuAlertMode = CpuAlertMode.TotalServer;
        await h.Build().EvaluateServerAsync(Harness.Snapshot(sqlCpu: 90, totalCpu: null));
        Assert.Equal("90% (Total CPU)", Assert.Single(h.Deliverer.Outcomes).CurrentValue);

        /* No CPU sample at all → no alert (:66 HasValue gate). */
        h.Deliverer.Outcomes.Clear();
        await h.Build().EvaluateServerAsync(Harness.Snapshot(sqlCpu: null, totalCpu: null));
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Cpu_RecoveryEmitsResolution_WithLiteToastStrings_UnlessSuppressed()
    {
        /* Lite AlertEngine.cs:101-113 — active→inactive announces "CPU Resolved" gated on
           !suppressPopups && enabled (:107). */
        var h = new Harness();
        h.Settings.CpuEnabled = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 90));
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 20, totalCpu: 40));

        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("CPU Resolved", resolution.Title);                       /* :110 */
        Assert.Equal("SRV-A: Total CPU back to 40%", resolution.Message);     /* :111 */
        Assert.Equal("High CPU", resolution.MetricName);

        /* Suppressed recovery still flips the active state but says nothing (:107). */
        h.Resolutions.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 90, suppressed: true));
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 20, totalCpu: 40, suppressed: true));
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task Cpu_Suppressed_SetsActiveButDoesNotDeliverOrStampCooldown()
    {
        /* Lite AlertEngine.cs:71-72 — active is recorded, but the !suppressPopups gate sits
           BEFORE the cooldown stamp, so nothing is delivered and nothing is stamped. */
        var h = new Harness();
        h.Settings.CpuEnabled = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 90, suppressed: true));
        Assert.Empty(h.Deliverer.Outcomes);

        /* Un-suppressed one second later: fires immediately — no cooldown was stamped. */
        h.Now = h.Now.AddSeconds(1);
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 90));
        Assert.Single(h.Deliverer.Outcomes);
    }

    /* ---------------- mute ---------------- */

    [Fact]
    public async Task MutedAlert_IsDeliveredFlaggedMuted_AndStampsTheCooldown()
    {
        /* Lite AlertEngine.cs:74-98 — the mute check resolves BEFORE the stamp (:76), the email
           service is still called with muted: true (:97) so history records the muted fire. */
        var h = new Harness();
        h.Settings.CpuEnabled = true;
        h.Muted = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 90));
        Assert.True(Assert.Single(h.Deliverer.Outcomes).Muted);

        /* Unmuting inside the cooldown does not re-fire — the muted fire stamped it (:76). */
        h.Muted = false;
        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 90));
        Assert.Single(h.Deliverer.Outcomes);
    }

    /* ---------------- blocking ---------------- */

    [Fact]
    public async Task Blocking_EdgeTrigger_FiresOnNewEventsOnly_AndPersistsTheWatermark()
    {
        /* Lite AlertEngine.cs:135-153 + RollingCountAlertGate (#1091/#1145). */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        var engine = h.Build();

        h.Adapter.Blocking.Add(BlockingRow(55));
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Blocking Detected", fired.MetricName);
        Assert.Equal("1", fired.CurrentValue);
        Assert.NotNull(fired.Context);
        Assert.Contains((Key, AlertEngine.BlockingWatermarkMetric, 1), h.StateStore.SavedEdge); /* :147-149 */

        /* The SAME lingering report does not re-fire even after the cooldown elapses (#1091). */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A genuinely new report (count climbs past the watermark) fires again. */
        h.Adapter.Blocking.Add(BlockingRow(77));
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal("2", h.Deliverer.Outcomes[1].CurrentValue);

        /* Window empties: watermark resets to 0 (persisted) and "Blocking Cleared" is emitted
           (:185-193 + RollingCountAlertGate.cs:64-66). */
        h.Adapter.Blocking.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Contains((Key, AlertEngine.BlockingWatermarkMetric, 0), h.StateStore.SavedEdge);
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Blocking Cleared", resolution.Title);                   /* :190 */
        Assert.Equal("SRV-A: No active blocking", resolution.Message);        /* :191 */
    }

    [Fact]
    public async Task Blocking_CountPrefersXeRows_FallsBackToDmvOnlyWhenNoXe()
    {
        /* Lite's overview count (LocalDataService.Overview.cs:74-77): COALESCE(NULLIF(xe,0),dmv). */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;

        /* 1 XE + 1 uncovered DMV row in the merged feed → the count is the XE count (1). */
        h.Adapter.Blocking.Add(BlockingRow(55));
        h.Adapter.Blocking.Add(BlockingRow(77, source: BlockedProcessAlertRow.DmvSnapshotSource));
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("1", Assert.Single(h.Deliverer.Outcomes).CurrentValue);

        /* No XE rows at all → the DMV fallback count (2). */
        h.Deliverer.Outcomes.Clear();
        h.Adapter.Blocking.Clear();
        h.Adapter.Blocking.Add(BlockingRow(55, source: BlockedProcessAlertRow.DmvSnapshotSource));
        h.Adapter.Blocking.Add(BlockingRow(77, source: BlockedProcessAlertRow.DmvSnapshotSource));
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("2", Assert.Single(h.Deliverer.Outcomes).CurrentValue);
    }

    [Fact]
    public async Task Blocking_Suppressed_EvaluatesWithoutDelivering_AndWatermarkDoesNotAdvance()
    {
        /* RollingCountAlertGate.cs:76-84 via Lite AlertEngine.cs:141 — an event arriving while
           suppressed is NOT consumed; it fires on the next unsuppressed check. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        var engine = h.Build();

        h.Adapter.Blocking.Add(BlockingRow(55));
        await engine.EvaluateServerAsync(Harness.Snapshot(suppressed: true));
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.StateStore.SavedEdge);

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains((Key, AlertEngine.BlockingWatermarkMetric, 1), h.StateStore.SavedEdge);
    }

    [Fact]
    public async Task Blocking_Disabled_NeverFetches()
    {
        /* Lite AlertEngine.cs:140-142 — the gate collapses to inactive; no store read happens
           (Lite's count came from the summary; the engine skips its fetch entirely). */
        var h = new Harness();
        h.Adapter.Blocking.Add(BlockingRow(55));

        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(0, h.Adapter.BlockingFetches);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Blocking_ExcludedDatabases_ReduceTheEffectiveCount()
    {
        /* Lite AlertEngine.cs:118-127 — rows in excluded databases don't count (rows with no
           database always pass). */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.ExcludedDatabasesList.Add("stackoverflow");

        h.Adapter.Blocking.Add(BlockingRow(55, database: "StackOverflow"));
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);

        h.Adapter.Blocking.Add(BlockingRow(77, database: "OtherDb"));
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("1", Assert.Single(h.Deliverer.Outcomes).CurrentValue);
    }

    /* ---------------- blocking wait time (#1839) ---------------- */

    /// <summary>A fresh snapshot totalling <paramref name="totalWaitMs"/> across <paramref name="sessions"/> SPIDs.</summary>
    private static CurrentBlockingWaitResult WaitSnapshot(long totalWaitMs, int sessions = 3, bool fresh = true) =>
        new(new DateTime(2026, 7, 1, 11, 59, 0), totalWaitMs, sessions, fresh);

    [Fact]
    public async Task BlockingWait_OffByDefault_NeverReadsOrFires()
    {
        /* The shipped state: threshold 0 with blocking alerts ON. The gate must not even ask the store —
           an off feature that still costs a query per sweep per server is not off. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Adapter.BlockingWait = WaitSnapshot(600_000);

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(0, h.Adapter.BlockingWaitFetches);
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");
    }

    [Fact]
    public async Task BlockingWait_FiresAtThresholdInclusive_WithRealNumericsAndContent()
    {
        /* At/above, not strictly above — the same inclusive comparison every other threshold uses. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 600;
        h.Adapter.BlockingWait = WaitSnapshot(600_000, sessions: 3);
        h.Adapter.Blocking.Add(BlockingRow(55));

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");
        Assert.Equal("600s across 3 blocked session(s)", fired.CurrentValue);
        Assert.Equal("600s", fired.ThresholdValue);
        /* #1830: the numerics must carry the real values — the display text is prose no parser recovers. */
        Assert.Equal(600d, fired.NumericCurrentValue);
        Assert.Equal(600d, fired.NumericThresholdValue);
        /* The reporter asked for today's Blocking Detected content, built from this sweep's rows. */
        Assert.NotNull(fired.Context);
        Assert.False(string.IsNullOrWhiteSpace(fired.DetailText));
    }

    [Fact]
    public async Task BlockingWait_BelowThreshold_DoesNotFire()
    {
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 600;
        h.Adapter.BlockingWait = WaitSnapshot(599_999);

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(1, h.Adapter.BlockingWaitFetches);
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");
    }

    [Fact]
    public async Task BlockingWait_IsLevelTriggered_CooldownSuppressesThenRefiresWhileStillAbove()
    {
        /* The distinguishing behavior vs the count gate's edge trigger: blocking that STAYS above the
           threshold keeps announcing itself every cooldown instead of going quiet after one alert. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Settings.CooldownMinutes = 5;
        h.Adapter.BlockingWait = WaitSnapshot(120_000);
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");

        /* Inside the cooldown, still above: no second alert. */
        h.Now = h.Now.AddMinutes(4);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");

        /* Cooldown elapsed, still above: it re-fires — no edge required. */
        h.Now = h.Now.AddMinutes(2);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count(o => o.MetricName == "Blocking Wait Time"));
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task BlockingWait_ResolvesWhenItDropsBelow()
    {
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Adapter.BlockingWait = WaitSnapshot(120_000);
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");

        h.Adapter.BlockingWait = WaitSnapshot(1_000);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Blocking Wait Cleared", resolution.Title);
        Assert.Equal("Blocking Wait Time", resolution.MetricName);
        /* A resolution is not a history row — nothing new was delivered. */
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");
    }

    [Fact]
    public async Task BlockingWait_StaleSnapshot_NeitherFiresNorHoldsTheAlertActive()
    {
        /* #1812's rule: a stopped collector leaves a "latest" snapshot that reads as NOW. A level-
           triggered gate on frozen rows would re-fire every cooldown forever, so staleness is no
           evidence — and it RESOLVES rather than latching (see CurrentBlockingWaitResult). */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Adapter.BlockingWait = WaitSnapshot(120_000);
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");

        /* Same over-threshold numbers, now stale: no re-fire even once the cooldown has elapsed. */
        h.Adapter.BlockingWait = WaitSnapshot(120_000, fresh: false);
        h.Now = h.Now.AddMinutes(30);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");
        Assert.Equal("Blocking Wait Cleared", Assert.Single(h.Resolutions).Title);
    }

    [Fact]
    public async Task BlockingWait_NoSnapshotAtAll_DoesNotFire()
    {
        /* A server that has never blocked has no snapshot row; null must read as "not above". */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Adapter.BlockingWait = null;

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(1, h.Adapter.BlockingWaitFetches);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task BlockingWait_FollowsTheBlockingEnabledToggle()
    {
        /* Turning blocking alerts off silences BOTH gates — one toggle, as a user reading it expects. */
        var h = new Harness();
        h.Settings.BlockingEnabled = false;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Adapter.BlockingWait = WaitSnapshot(120_000);

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(0, h.Adapter.BlockingWaitFetches);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task BlockingWait_IsADistinctMetricFromTheCountGate()
    {
        /* Both gates can be over threshold in the same sweep and must produce two separate alerts, so
           muting or acknowledging one never silences the other. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingCountThreshold = 1;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Adapter.Blocking.Add(BlockingRow(55));
        h.Adapter.BlockingWait = WaitSnapshot(120_000);

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(
            new[] { "Blocking Detected", "Blocking Wait Time" },
            h.Deliverer.Outcomes.Select(o => o.MetricName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task BlockingWait_Muted_IsStillDeliveredFlagged()
    {
        /* Lite's flow: a muted alert is recorded, not sent — the deliverer decides, the engine flags. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 60;
        h.Adapter.BlockingWait = WaitSnapshot(120_000);
        h.Muted = true;

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.True(Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time").Muted);
    }

    /* ---------------- deadlocks ---------------- */

    [Fact]
    public async Task Deadlock_FiresWithFingerprintedContext_ThenWatermarkBlocksTheRefire()
    {
        /* Lite AlertEngine.cs:213-260 — the same #1091 gate, and the built context carries the
           #1140 involved-object fingerprint. */
        var h = new Harness();
        h.Settings.DeadlockEnabled = true;
        var engine = h.Build();

        h.Adapter.Deadlocks.Add(DeadlockRow());
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Deadlocks Detected", fired.MetricName);
        Assert.NotNull(fired.Context);
        Assert.NotNull(fired.Context!.Incidents);
        Assert.False(string.IsNullOrEmpty(fired.Context.Incidents![0].DedupKey));
        Assert.Contains((Key, AlertEngine.DeadlockWatermarkMetric, 1), h.StateStore.SavedEdge);

        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Deadlock_WatermarkSeededFromStore_PreventsThePostRestartRefire()
    {
        /* #1145 — Lite seeds its in-memory watermarks from the persisted store at startup
           (MainWindow.xaml.cs:1563-1579); the engine seeds per-key on first evaluation. */
        var h = new Harness();
        h.Settings.DeadlockEnabled = true;
        h.StateStore.EdgeWatermarks[(Key, AlertEngine.DeadlockWatermarkMetric)] = 1;

        h.Adapter.Deadlocks.Add(DeadlockRow());
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Deadlock_TotalOccurrences_AccumulateAcrossThrottledDeliveries()
    {
        /* #2216 end to end. Delivery one carries one deadlock; two more happen before the next eligible
           delivery. The window gauge reads 3 and the monotonic total reads 3 — the number a consumer that
           missed the middle of the incident needs, and the number the gauge alone cannot give it (a reading
           of 3 could equally mean "three new" or "one new, two aged out"). */
        var h = new Harness();
        h.Settings.DeadlockEnabled = true;
        var engine = h.Build();

        h.Adapter.Deadlocks.Add(DeadlockRow());
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var first = Assert.Single(h.Deliverer.Outcomes);
        var firstIncident = Assert.Single(first.Context!.Incidents!);
        Assert.Equal(1, firstIncident.OccurrenceCount);
        Assert.Equal(1L, firstIncident.TotalOccurrences);
        Assert.Equal(h.Now, firstIncident.IncidentStartedUtc);

        /* Same fingerprint (identical graphs), so this is the same incident continuing. */
        h.Adapter.Deadlocks.Add(DeadlockRow());
        h.Adapter.Deadlocks.Add(DeadlockRow());
        var openedAt = h.Now;
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        var second = h.Deliverer.Outcomes[1];
        var secondIncident = Assert.Single(second.Context!.Incidents!);
        Assert.Equal(3, secondIncident.OccurrenceCount);
        Assert.Equal(3L, secondIncident.TotalOccurrences);

        /* The start time did NOT move — that is how the consumer tells a continuation from a new incident
           that happens to read 3. */
        Assert.Equal(openedAt, secondIncident.IncidentStartedUtc);

        var persisted = h.StateStore.Occurrences[(Key, AlertEngine.DeadlockWatermarkMetric)];
        Assert.Equal(3L, persisted[secondIncident.DedupKey].TotalOccurrences);
    }

    [Fact]
    public async Task Deadlock_OccurrencesAreObservedOnSweepsThatDeliverNothing()
    {
        /* PR #2221's review: with the accumulation inside the Fire branch, no sweep between two deliveries
           observed anything, so an event the window retired during the cooldown cancelled an arrival and the
           arrival was never counted. The observation now runs on every sweep that fetched rows. */
        var h = new Harness();
        h.Settings.DeadlockEnabled = true;
        var engine = h.Build();

        h.Adapter.Deadlocks.Add(DeadlockRow());
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Two more deadlocks INSIDE the cooldown — no delivery, but the count must still be observed. */
        h.Adapter.Deadlocks.Add(DeadlockRow());
        h.Adapter.Deadlocks.Add(DeadlockRow());
        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Single(h.Deliverer.Outcomes);   /* the cooldown suppressed the delivery */

        var persisted = h.StateStore.Occurrences[(Key, AlertEngine.DeadlockWatermarkMetric)];
        Assert.Equal(3L, Assert.Single(persisted).Value.TotalOccurrences);
    }

    [Fact]
    public async Task Deadlock_OccurrenceStateSeededFromStore_ContinuesTheIncidentAcrossARestart()
    {
        /* The reason the counter is persisted at all: a total that reset on every service restart would be a
           second gauge wearing a total's name. A fresh engine (new in-memory state, as after a restart)
           seeded from the store must keep counting the incident it finds there. */
        var discovery = new Harness();
        discovery.Settings.DeadlockEnabled = true;
        discovery.Adapter.Deadlocks.Add(DeadlockRow());
        await discovery.Build().EvaluateServerAsync(Harness.Snapshot());
        var dedupKey = Assert.Single(Assert.Single(discovery.Deliverer.Outcomes).Context!.Incidents!).DedupKey;

        var restarted = new Harness();
        restarted.Settings.DeadlockEnabled = true;
        var openedAt = restarted.Now.AddMinutes(-20);
        restarted.StateStore.Occurrences[(Key, AlertEngine.DeadlockWatermarkMetric)] =
            new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal)
            {
                [dedupKey] = new(
                    TotalOccurrences: 9,
                    ObservedWindowCount: 2,
                    IncidentStartedUtc: openedAt,
                    LastObservedUtc: restarted.Now.AddMinutes(-5)),
            };

        restarted.Adapter.Deadlocks.Add(DeadlockRow());
        restarted.Adapter.Deadlocks.Add(DeadlockRow());
        restarted.Adapter.Deadlocks.Add(DeadlockRow());
        await restarted.Build().EvaluateServerAsync(Harness.Snapshot());

        var incident = Assert.Single(Assert.Single(restarted.Deliverer.Outcomes).Context!.Incidents!);

        /* 9 already counted, the window rose from 2 to 3, so one new occurrence: 10. */
        Assert.Equal(10L, incident.TotalOccurrences);
        Assert.Equal(openedAt, incident.IncidentStartedUtc);
    }

    [Fact]
    public async Task Deadlock_FallingEdge_ClearsTheOccurrenceState()
    {
        /* When the condition clears, the incident is over and its counters go with it — otherwise the next
           incident on the same fingerprint reads as a continuation of this one, reporting an undercount
           under a start time that points at an incident the user already saw resolve. */
        var h = new Harness();
        h.Settings.DeadlockEnabled = true;
        var engine = h.Build();

        h.Adapter.Deadlocks.Add(DeadlockRow());
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.NotEmpty(h.StateStore.Occurrences[(Key, AlertEngine.DeadlockWatermarkMetric)]);

        h.Adapter.Deadlocks.Clear();
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Contains(h.Resolutions, r => r.MetricName == "Deadlocks Detected");
        Assert.Empty(h.StateStore.Occurrences[(Key, AlertEngine.DeadlockWatermarkMetric)]);
    }

    [Fact]
    public async Task Blocking_TotalOccurrences_RideOnTheBlockingIncidentsToo()
    {
        /* Both count gates go through the same accumulator — the blocking half is not an afterthought, it is
           the other half of the reported feature. */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        var engine = h.Build();

        h.Adapter.Blocking.Add(BlockingRow(55));
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected");
        var incident = Assert.Single(fired.Context!.Incidents!);
        Assert.Equal(1L, incident.TotalOccurrences);
        Assert.Equal(h.Now, incident.IncidentStartedUtc);
        Assert.Contains((Key, AlertEngine.BlockingWatermarkMetric, 1), h.StateStore.SavedOccurrences);
    }

    [Fact]
    public async Task Deadlock_WhollyExcludedDatabaseGraphs_DontCount()
    {
        /* Lite AlertEngine.cs:198-205 + AlertContextBuilders.IsDeadlockExcluded — a deadlock
           whose processes ALL ran in excluded databases is dropped from the count. */
        var h = new Harness();
        h.Settings.DeadlockEnabled = true;
        h.Settings.ExcludedDatabasesList.Add("ExcludedDb");

        h.Adapter.Deadlocks.Add(DeadlockRow(database: "ExcludedDb"));
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);

        h.Adapter.Deadlocks.Add(DeadlockRow(database: "StackOverflow"));
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("1", Assert.Single(h.Deliverer.Outcomes).CurrentValue);
    }

    /* ---------------- poison waits ---------------- */

    [Fact]
    public async Task PoisonWait_FiresWithWorstWaitNumerics_AndResolvesWhenGone()
    {
        /* Lite AlertEngine.cs:274-333. */
        var h = new Harness();
        h.Settings.PoisonWaitEnabled = true;
        var engine = h.Build();

        h.Adapter.PoisonWaits.Add(new PoisonWaitDelta { WaitType = "THREADPOOL", DeltaMs = 100000, DeltaTasks = 50, AvgMsPerWait = 2000 });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Poison Wait", fired.MetricName);
        Assert.Equal("THREADPOOL (2000ms)", fired.CurrentValue);              /* :286 */
        Assert.Equal("500ms avg", fired.ThresholdValue);                      /* :314 */
        Assert.Equal(2000d, fired.NumericCurrentValue);                       /* :317 */
        Assert.Equal(500d, fired.NumericThresholdValue);                      /* :318 */

        h.Adapter.PoisonWaits.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Poison Waits Cleared", resolution.Title);               /* :329 */
        Assert.Equal("SRV-A: Poison wait avg below threshold", resolution.Message); /* :330 */
    }

    [Fact]
    public async Task PoisonWait_DoesNotRefire_OnTheSameCollectionTime_EvenAfterCooldownElapses()
    {
        /* The read adapter's own "newest row within 10 minutes" window can hand back the SAME
           wait_stats row across multiple sweeps when the collector's delivered cadence lags the
           alert cooldown — observed live as byte-identical "Poison Wait" alerts ~5-7 minutes
           apart on the same server. Cooldown elapsing is not proof a fresh observation exists;
           re-firing on an unrefreshed collection_time reports the same event twice. */
        var h = new Harness();
        h.Settings.PoisonWaitEnabled = true;
        var engine = h.Build();

        var firstCollection = new DateTime(2026, 8, 31, 6, 0, 0, DateTimeKind.Utc);
        h.Adapter.PoisonWaits.Add(new PoisonWaitDelta
        {
            WaitType = "RESOURCE_SEMAPHORE_QUERY_COMPILE",
            DeltaMs = 113997,
            DeltaTasks = 134,
            AvgMsPerWait = 850.7,
            CollectionTime = firstCollection
        });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Cooldown (5 min default) elapses, but the collector has not produced a new row yet —
           the adapter still hands back the identical collection_time. Must NOT re-fire. */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A genuinely new collection — even with the identical wait-type/value shape — is a
           fresh observation of the condition and must fire. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.PoisonWaits.Clear();
        h.Adapter.PoisonWaits.Add(new PoisonWaitDelta
        {
            WaitType = "RESOURCE_SEMAPHORE_QUERY_COMPILE",
            DeltaMs = 113997,
            DeltaTasks = 134,
            AvgMsPerWait = 850.7,
            CollectionTime = firstCollection.AddMinutes(7)
        });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    /* ---------------- long-running queries ---------------- */

    [Fact]
    public async Task LongRunningQuery_ForwardsEverySettingsKnobToTheAdapter_AndFires()
    {
        /* Lite AlertEngine.cs:346 — the read carries the threshold, cap, all five noise filters,
           and the excluded databases; :354 — minutes are integer division of seconds. */
        var h = new Harness();
        h.Settings.LongRunningQueryEnabled = true;
        h.Settings.ExcludedDatabasesList.Add("StageDb");

        h.Adapter.LongRunning.Add(new LongRunningQueryInfo
        {
            SessionId = 71,
            DatabaseName = "StackOverflow",
            QueryText = "SELECT COUNT(*) FROM Users",
            ElapsedSeconds = 2159, /* 35m 59s → "35m" via integer division */
            CpuTimeMs = 1000,
            QueryHash = "0x9AAF0129E4E9AD07"
        });
        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Long-Running Query", fired.MetricName);
        Assert.Equal("1 query(s), longest 35m", fired.CurrentValue);          /* :385 */
        Assert.Equal("30m", fired.ThresholdValue);
        Assert.Equal(35d, fired.NumericCurrentValue);                         /* :389 */

        var args = h.Adapter.LastLrqArgs!.Value;
        Assert.Equal(30, args.ThresholdMinutes);
        Assert.Equal(5, args.MaxResults);
        Assert.True(args.Diag && args.WaitFor && args.Backups && args.Misc && args.Cdc);
        Assert.Contains("StageDb", args.Excluded);
    }

    /* ---------------- tempdb ---------------- */

    [Fact]
    public async Task TempDb_FiresAtThreshold_AndResolutionCarriesTheCurrentPercent()
    {
        /* Lite AlertEngine.cs:420-465. */
        var h = new Harness();
        h.Settings.TempDbSpaceEnabled = true;
        var engine = h.Build();

        /* #2515: 1000 MB total and NO MaxSizeMb, which is deliberate and stays that way. The fixture predates
           the ceiling, so it describes a tempdb whose cap was never measured — and that is exactly the case
           where the denominator remains the allocation. It does NOT imply a 1000 MB cap; if it did, this pin
           would have to move, and the fact that it does not is the guarantee that no existing on-prem or RDS
           target with an unlimited (or uncollected) tempdb sees its number change. The capped case gets its
           own test below rather than being folded in here. */
        h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 800, UnallocatedMb = 200 }; /* 80% used */
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("tempdb Space", fired.MetricName);
        Assert.Equal("80% used (800 MB)", fired.CurrentValue);                /* :446 */
        Assert.Equal(80d, fired.NumericCurrentValue!.Value, precision: 3);    /* :450 */

        h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 200, UnallocatedMb = 800 }; /* 20% used */
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("tempdb Space Resolved", resolution.Title);              /* :463 */
        Assert.Equal("SRV-A: tempdb usage back to 20%", resolution.Message);  /* :461,:464 */
    }

    /// <summary>
    /// #2515, through the ENGINE rather than the arithmetic: the Azure shape from the issue must not fire, and
    /// the same allocation without a ceiling must. Same 59.75 MB of reserved tempdb in both, same 80% default —
    /// the only difference is whether the collector could see how far the files are allowed to grow.
    ///
    /// <para>This is the assertion the whole change exists for. <see cref="TempDbCeilingStoreTests"/> proves
    /// the arithmetic and the store round-trip; this proves the alert engine's decision follows it, which is
    /// what actually pages someone.</para>
    /// </summary>
    [Fact]
    public async Task TempDb_TheAzureCeiling_SuppressesTheAlert_ThatTheAllocationWouldFire()
    {
        var h = new Harness();
        h.Settings.TempDbSpaceEnabled = true;
        Assert.Equal(80, h.Settings.TempDbSpaceThresholdPercent);
        var engine = h.Build();

        /* GP_S_Gen5_2 with one ~57 MB #temp table: 62.44 MB allocated, 65,536 MB of headroom behind it. */
        h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 59.75, UnallocatedMb = 2.69, MaxSizeMb = 65_536 };
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);

        /* The identical snapshot with the ceiling unmeasured is the pre-#2515 reading, and it pages. */
        h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 59.75, UnallocatedMb = 2.69 };
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("tempdb Space", fired.MetricName);
        Assert.Equal("96% used (60 MB)", fired.CurrentValue);
    }

    /* ---------------- low disk ---------------- */

    [Fact]
    public async Task LowDisk_GradesCriticallyLowBreaches_AndAStandingBreachDoesNotRefire()
    {
        /* Lite AlertEngine.cs:487-536 — #1136 severity grading + the #754 worsening gate. */
        var h = new Harness();
        h.Settings.LowDiskEnabled = true;
        var engine = h.Build();

        /* 8% free / 8 GB on a 100 GB volume: breached (<10%) but NOT critically low
           (> 3% and > 2 GB) → no severity override. */
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 102400, FreeMb = 8192 });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var warning = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Volume Free Space", warning.MetricName);
        Assert.Null(warning.Severity);
        Assert.Equal("10% / 5 GB", warning.ThresholdValue);                   /* :529 FormatLowDiskThreshold */

        /* The SAME standing level does not re-fire after the cooldown (#754 gate, :495-497). */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Worsening to critically low (1% ≈ 1 GB free) re-fires, graded CRITICAL (:519-522). */
        h.Adapter.Volumes[0].FreeMb = 1024;
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[1].Severity);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[1].Context!.SeverityOverride);

        /* Recovery clears the worsening watermark and announces (:538-548)... */
        h.Adapter.Volumes.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("Volume Free Space Resolved", Assert.Single(h.Resolutions).Title);

        /* ...so a fresh breach at the ORIGINAL level alerts again (fresh = always notifies). */
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 102400, FreeMb = 8192 });
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
    }

    /* ---------------- persistent version store (#1984) ---------------- */

    [Fact]
    public async Task PvsPressure_FiresOnWorstDatabase_StandingBreachStaysQuiet_WorseningRefires()
    {
        var h = new Harness();
        h.Settings.PvsEnabled = true;
        var engine = h.Build();

        /* Two ADR databases over the 40% trigger; the worst (highest %) names the alert. */
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "ledger", PvsSizeMb = 2048, DatabaseDataSizeMb = 4096 });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Version Store (PVS)", fired.MetricName);
        Assert.Equal("shop PVS 60% of database (6.0 GB)", fired.CurrentValue);
        Assert.Equal("40% of database and ≥ 1 GB", fired.ThresholdValue);
        Assert.Equal(60d, fired.NumericCurrentValue!.Value, precision: 3);
        Assert.Equal(40d, fired.NumericThresholdValue);
        /* No severity tier: MS documents no "critical" PVS level, and inventing one is the folklore
           the collector deliberately avoided. */
        Assert.Null(fired.Severity);
        /* Both breaching databases ride in the context, worst first (the incident renderer appends
           its own dedup items after them, so the pin is on the headings, not the count). */
        Assert.StartsWith("shop", fired.Context!.Details[0].Heading, StringComparison.Ordinal);
        Assert.Contains(fired.Context.Details, d => d.Heading.StartsWith("ledger", StringComparison.Ordinal));

        /* The SAME standing level does not re-fire after the cooldown — a large PVS stays allocated
           even after its cause clears (measured on a live rig), so without the PvsAlertGate a
           recovered incident would re-notify every cooldown for hours. */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Rising past the 5-point worsening margin re-fires. */
        h.Adapter.PvsDatabases[0].PvsSizeMb = 7168; /* 70% */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* Recovery announces and clears the worsening watermark... */
        h.Adapter.PvsDatabases.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Version Store (PVS) Resolved", resolution.Title);
        Assert.Equal("SRV-A: All version stores back below threshold", resolution.Message);

        /* ...so a fresh breach at the ORIGINAL level alerts again (fresh = always notifies). */
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task PvsPressure_FloorKeepsSmallDatabasesQuiet_AndZeroFloorRemovesIt()
    {
        /* 70% of a tiny database is megabytes, and nobody should be paged for megabytes: the GB
           floor is an AND qualifier, unlike the low-disk pair's either-breach-fires OR. */
        var h = new Harness();
        h.Settings.PvsEnabled = true;
        var engine = h.Build();

        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "tiny", PvsSizeMb = 512, DatabaseDataSizeMb = 732 });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);
        /* Never-active means no resolution chatter either. */
        Assert.Empty(h.Resolutions);

        /* 0 removes the floor: percent alone decides. */
        h.Settings.PvsFloorGb = 0;
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("40% of database", fired.ThresholdValue);
    }

    [Fact]
    public async Task PvsPressure_DisabledOrZeroPercent_DoesNotEvaluate()
    {
        var h = new Harness();
        var engine = h.Build();
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });

        /* Disabled: the breaching row proves nothing was evaluated. */
        h.Settings.PvsEnabled = false;
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);

        /* Percent 0 disables outright — it is the alert's ONLY trigger, so there is no second
           dimension to fall back on (unlike low-disk). */
        h.Settings.PvsEnabled = true;
        h.Settings.PvsThresholdPercent = 0;
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* ---------------- anomalous jobs ---------------- */

    [Fact]
    public async Task AnomalousJob_CooldownIsKeyedPerJobRun()
    {
        /* Lite AlertEngine.cs:575-614 — the cooldown key is {server}:{jobId}:{startTime:O}, so a
           NEW run of the same job alerts without waiting out the old run's cooldown. */
        var h = new Harness();
        h.Settings.LongRunningJobEnabled = true;
        var engine = h.Build();

        var start = new DateTime(2026, 7, 1, 11, 0, 0);
        h.Adapter.AnomalousJobs.Add(new AnomalousJobInfo
        {
            JobName = "Nightly ETL", JobId = "job-1", StartTime = start,
            CurrentDurationSeconds = 3600, AvgDurationSeconds = 900, PercentOfAverage = 400
        });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Long-Running Job", fired.MetricName);
        Assert.Equal("1 job(s) exceeding 3x average", fired.CurrentValue);    /* :606 */
        Assert.Equal(400d, fired.NumericCurrentValue);                        /* :610 */
        Assert.Equal(300d, fired.NumericThresholdValue);                      /* :611 multiplier*100 */

        /* Same run, cooldown not yet elapsed → quiet (:581). */
        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A NEW run (different start time) of the same job fires immediately (:579 key). */
        h.Adapter.AnomalousJobs[0].StartTime = start.AddHours(2);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AnomalousJob_StaleSnapshotIsNoEvidence_NeitherFiresNorResolves()
    {
        /* #1812: a stale latest snapshot re-fired the same historical run every cooldown — the per-run
           cooldown key deliberately expires each pass, so the stale rows re-armed it forever. And an
           empty-on-stale read would have fabricated "jobs cleared" out of a collector that merely
           stopped reporting. Stale = NO evidence: skip both branches, leave the active state alone;
           fresh evidence resumes real evaluation. */
        var h = new Harness();
        h.Settings.LongRunningJobEnabled = true;
        var engine = h.Build();

        h.Adapter.AnomalousJobs.Add(new AnomalousJobInfo
        {
            JobName = "Nightly ETL", JobId = "job-1", StartTime = new DateTime(2026, 7, 1, 11, 0, 0),
            CurrentDurationSeconds = 3600, AvgDurationSeconds = 900, PercentOfAverage = 400
        });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);            /* the live fire — arms the active flag */

        /* The collector dies; the snapshot goes stale while its rows still "match". Two cooldowns
           elapse — the old behavior re-fired at each. */
        h.Adapter.SnapshotIsStale = true;
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);            /* no re-fire on stale evidence */
        Assert.Empty(h.Resolutions);                    /* and no fabricated "jobs cleared" */

        /* Fresh evidence returns with the jobs genuinely gone → the REAL resolution fires. */
        h.Adapter.SnapshotIsStale = false;
        h.Adapter.AnomalousJobs.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("Long-Running Jobs Cleared", Assert.Single(h.Resolutions).Title);
    }

    /* ---------------- failed jobs ---------------- */

    [Fact]
    public async Task FailedJobs_WatermarkDedups_AndPersistsTheServerLocalRunTime()
    {
        /* Lite AlertEngine.cs:663-709 — only a strictly newer failure re-fires; the persisted
           watermark is the newest failure's SERVER-LOCAL run time, saved on-change only (:682). */
        var h = new Harness();
        h.Settings.FailedJobEnabled = true;
        var engine = h.Build(withFailedJobsFetcher: true);

        var firstFailure = new DateTime(2026, 7, 1, 6, 55, 0); /* server-local, Kind-Unspecified */
        h.FailedJobs.Add(new FailedJobInfo { JobName = "Backup.Full", JobId = "j1", RunDateTime = firstFailure, StepId = 2, StepName = "Backup", Message = "disk full" });

        await engine.EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Failed Agent Job", fired.MetricName);
        Assert.Equal("1 job failure(s) in last 60m — Backup.Full", fired.CurrentValue); /* :701 */
        Assert.Equal((Key, firstFailure), Assert.Single(h.StateStore.SavedFailedJob));

        /* The same failure lingering in the lookback window never re-fires (:667). */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A strictly newer failure fires again and advances the watermark. */
        h.FailedJobs.Insert(0, new FailedJobInfo { JobName = "Index.Rebuild", JobId = "j2", RunDateTime = firstFailure.AddMinutes(30) });
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(firstFailure.AddMinutes(30), h.StateStore.FailedJobWatermarks[Key]);
    }

    [Fact]
    public async Task FailedJobs_GatedOnOnlineAndNotAzure_AndSuppressionHoldsTheWatermark()
    {
        /* Lite AlertEngine.cs:649-653 (online + non-Azure gates; the msdb probe deliberately
           did not transplant — Phase-5 review F11) and :669-682 (suppression sits before the
           watermark advance, so a suppressed failure is reported later, not swallowed). */
        var h = new Harness();
        h.Settings.FailedJobEnabled = true;
        var engine = h.Build(withFailedJobsFetcher: true);
        h.FailedJobs.Add(new FailedJobInfo { JobName = "Backup.Full", JobId = "j1", RunDateTime = new DateTime(2026, 7, 1, 6, 55, 0) });

        await engine.EvaluateServerAsync(Harness.Snapshot(isAzureSqlDb: true));
        Assert.Equal(0, h.FailedJobFetches);

        await engine.EvaluateServerAsync(Harness.Snapshot(isOnline: false));
        Assert.Equal(0, h.FailedJobFetches);

        await engine.EvaluateServerAsync(Harness.Snapshot(suppressed: true));
        Assert.Equal(1, h.FailedJobFetches);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.StateStore.SavedFailedJob);

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Single(h.StateStore.SavedFailedJob);
    }

    /* ---------------- #3013: swallowed condition reads reach a counter ---------------- */

    [Fact]
    public async Task EverySwallowedConditionRead_LandsOnTheCounter_UnderTheServerItBelongsTo()
    {
        /* #3013's whole defect is that these skips reached no surface. The log-and-skip posture itself is
           correct and is pinned by AdapterFailure_SkipsThatCheck_WithoutDisturbingItsState below; what this
           pin adds is that the skip is now COUNTED, per server, with the failing read named.

           Three checks enabled rather than all of them, because the exact total over an all-enabled sweep
           depends on gates this pin is not about (Azure-ness, the wait-seconds opt-in, whether a fetcher was
           supplied). Three is enough to prove the count is per-read and not per-pass. */
        var counter = new AlertReadFailureCounter(() => new DateTime(2026, 9, 5, 8, 0, 0, DateTimeKind.Utc));
        var h = new Harness { ReadFailures = counter };
        h.Settings.BlockingEnabled = true;
        h.Settings.DeadlockEnabled = true;
        h.Settings.DatabaseStateEnabled = true;
        h.Settings.ForcePlanFailureEnabled = true;

        var engine = new AlertEngine(
            h.Settings, new ThrowingAdapter(), h.StateStore, h.Deliverer, _ => false,
            utcNow: () => h.Now, readFailures: counter);

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var reading = counter.ReadFor(Key);

        /* Blocking, deadlocks, database state and forced plans all threw. The watermark seed reads the same
           throwing adapter and is counted too, which is deliberate: a failed seed means the edge triggers
           start from nothing for that server, which is exactly the kind of silent degradation #3013 is
           about. Bounded rather than exact so the pin does not have to be rewritten every time a check is
           added, with the LOWER bound the part that carries the claim. */
        Assert.True(
            reading.ServerReadFailures >= 4,
            $"expected at least the four enabled condition reads to be counted, got {reading.ServerReadFailures}");

        /* The denominator, and the reason this is not just a count: one pass. */
        Assert.Equal(1, reading.ServerAlertPasses);

        /* Nothing leaked to another server or to the fleet bucket: the instance total is this server's. */
        Assert.Equal(reading.ServerReadFailures, reading.InstanceReadFailures);
        Assert.Equal(0, counter.ReadFor("999").ServerReadFailures);

        /* The currency stamp and the named read — the two things a bare count cannot say. */
        Assert.Equal(new DateTime(2026, 9, 5, 8, 0, 0, DateTimeKind.Utc), reading.LastFailureAtUtc);
        Assert.False(string.IsNullOrWhiteSpace(reading.LastFailureRead));

        /* The finding names the read rather than restating the number. */
        var finding = AlertReadFailureCounter.FormatFinding(reading);
        Assert.NotNull(finding);
        Assert.Contains(reading.LastFailureRead!, finding, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHealthyPass_CountsItselfAndLeavesTheFailureCountAtZero()
    {
        /* The control, and the half that decides whether the counter can be read as reassurance: a pass over
           an adapter that answers normally must move the DENOMINATOR and nothing else. Without this, a
           counter wired to increment on every pass would look identical to a working one on the test above.

           The control also has to exercise the case worth worrying about — a pass that actually RAN its
           checks — so the same four checks are enabled here as in the failing pin, against the harness's
           own answering adapter rather than the throwing one. */
        var counter = new AlertReadFailureCounter();
        var h = new Harness { ReadFailures = counter };
        h.Settings.BlockingEnabled = true;
        h.Settings.DeadlockEnabled = true;
        h.Settings.DatabaseStateEnabled = true;
        h.Settings.ForcePlanFailureEnabled = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var reading = counter.ReadFor(Key);
        Assert.Equal(2, reading.ServerAlertPasses);
        Assert.Equal(0, reading.ServerReadFailures);
        Assert.Equal(0, reading.InstanceReadFailures);
        Assert.Null(reading.LastFailureAtUtc);
        Assert.Null(reading.LastFailureRead);

        /* A clean reading carries no sentence at all, rather than a sentence saying it is clean — the
           #3017 discipline: a finding that always renders trains a reader to skip it. */
        Assert.Null(AlertReadFailureCounter.FormatFinding(reading));

        /* And the checks really did run against the adapter, so the zero above is a zero from a pass that
           looked rather than one that was gated off. */
        Assert.True(h.Adapter.ForcePlanFetches > 0, "the control pass performed no reads, so its zero proves nothing");
    }

    [Fact]
    public async Task TheMasterSwitchOffPass_IsNotInTheDenominator()
    {
        /* A pass that never looked at the store must not dilute the denominator — otherwise a fleet with
           alerts switched off accumulates passes forever and three failures over "50,000 passes" reads as
           negligible when the real denominator is three. */
        var counter = new AlertReadFailureCounter();
        var h = new Harness { ReadFailures = counter };
        h.Settings.AlertsEnabled = false;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(0, counter.ReadFor(Key).ServerAlertPasses);
    }

    /* ---------------- engine hygiene ---------------- */

    [Fact]
    public async Task AdapterFailure_SkipsThatCheck_WithoutDisturbingItsState()
    {
        /* Class-remarks adaptation (2): a failed blocking fetch must not run the gate against a
           fabricated zero count (which would reset the watermark and later re-fire). */
        var h = new Harness();
        h.Settings.BlockingEnabled = true;
        var engine = h.Build();

        h.Adapter.Blocking.Add(BlockingRow(55));
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);
        h.StateStore.SavedEdge.Clear();

        var throwingAdapter = new ThrowingAdapter();
        var engine2 = new AlertEngine(h.Settings, throwingAdapter, h.StateStore, h.Deliverer, _ => false, utcNow: () => h.Now);
        await engine2.EvaluateServerAsync(Harness.Snapshot());

        /* No watermark churn, no resolution, no delivery from the failed sweep. */
        Assert.Empty(h.StateStore.SavedEdge);
        Assert.Single(h.Deliverer.Outcomes);
    }

    private sealed class ThrowingAdapter : IAlertReadAdapter
    {
        public Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(string serverKey, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(string serverKey, double thresholdMs, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(string serverKey, int thresholdMinutes, int maxResults, bool excludeSpServerDiagnostics, bool excludeWaitFor, bool excludeBackups, bool excludeMiscWaits, bool excludeCdc, IReadOnlyList<string> excludedDatabases, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(string serverKey, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");

        /* #2349: empty on purpose. These tests exercise other alerts, and a fabricated file would
           make the file-growth gate fire inside an unrelated scenario. */
        public Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(
            string serverKey, int lookbackMinutes, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DatabaseFileGrowthInfo>());
        public Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(string serverKey, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<List<PvsPressureInfo>> GetPvsPressureAsync(string serverKey, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<AnomalousJobsResult> GetAnomalousJobsAsync(string serverKey, int multiplier, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
        public Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(string serverKey, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");

        public Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(string serverKey, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("store down");
    }

    [Fact]
    public async Task StatePerServer_IsIndependent()
    {
        /* All engine state dictionaries key on serverKey — one server's cooldown or watermark
           never gates another's (Lite: per-key dicts, MainWindow.xaml.cs:56-104). */
        var h = new Harness();
        h.Settings.CpuEnabled = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(new AlertServerSnapshot("101", "SRV-A", true, 70, 90, false, false));
        await engine.EvaluateServerAsync(new AlertServerSnapshot("202", "SRV-B", true, 70, 90, false, false));

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(new[] { "101", "202" }, h.Deliverer.Outcomes.Select(o => o.ServerKey).ToArray());
    }

    /* ---------------- database state (baseline deviation) ---------------- */

    [Fact]
    public async Task ForcePlanFailure_Disabled_DoesNotFetch()
    {
        var h = new Harness();
        h.Settings.ForcePlanFailureEnabled = false;
        h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo { DatabaseName = "Sales", QueryId = 11, PlanId = 22, ForcingType = "MANUAL", FailureReason = "NO_INDEX", FailureDelta = 3, TotalFailures = 3 });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        /* The gate must skip the READ, not just the fire — a disabled alert should cost nothing. */
        Assert.Equal(0, h.Adapter.ForcePlanFetches);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ForcePlanFailure_FiresPerPlan_CarryingReasonForcingTypeAndDelta()
    {
        /* Two plans in the SAME database are two independent conditions — if the alert keyed per server or
           per database, the second would be swallowed by the first's cooldown and an operator would never
           learn about it. */
        var h = new Harness();
        h.Settings.ForcePlanFailureEnabled = true;
        h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo { DatabaseName = "Sales", QueryId = 11, PlanId = 22, ForcingType = "MANUAL", FailureReason = "NO_INDEX", FailureDelta = 4, TotalFailures = 9 });
        h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo { DatabaseName = "Sales", QueryId = 33, PlanId = 44, ForcingType = "AUTO", FailureReason = "NO_PLAN", FailureDelta = 1, TotalFailures = 1 });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal("Forced Plan Failing", o.MetricName));
        /* Warning for every rise — no Critical tier exists yet, on purpose (ForcePlanTokens). */
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Warning, o.Severity));

        var manual = h.Deliverer.Outcomes.Single(o => o.CurrentValue.Contains("plan 22"));
        Assert.Contains("NO INDEX", manual.DetailText, StringComparison.Ordinal);
        Assert.Contains("MANUAL", manual.DetailText, StringComparison.Ordinal);
        /* The delta, not the total, is what says 'happening now'. */
        Assert.Contains("4", manual.DetailText, StringComparison.Ordinal);

        var auto = h.Deliverer.Outcomes.Single(o => o.CurrentValue.Contains("plan 44"));
        Assert.Contains("AUTO", auto.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ForcePlanFailure_CooldownSuppressesSecondFire_ThenResolvesWhenTheCounterStops()
    {
        var h = new Harness();
        h.Settings.ForcePlanFailureEnabled = true;
        h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo { DatabaseName = "Sales", QueryId = 11, PlanId = 22, ForcingType = "MANUAL", FailureReason = "NO_INDEX", FailureDelta = 2, TotalFailures = 2 });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Still failing next sweep, inside the cooldown — one alert, not two. */
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* The adapter stops returning it: the counter stopped rising. That covers unforced, reproducible
           again, and query-no-longer-running alike — hence 'no longer failing' rather than 'fixed'. */
        h.Adapter.ForcePlanFailures.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == "Forced Plan Failing");
        /* The recovery text is read by a human in a toast, an email and a history row, so it must name
           the plan the way the firing message did — NOT the internal key. The first version of this
           test only asserted the message contained "22", which the leaked key 'forceplan:Sales:11:22'
           satisfied, so it passed while operators would have seen gibberish (review catch). */
        Assert.DoesNotContain(ForcePlanTokens.KeyPrefix, resolution.Message, StringComparison.Ordinal);
        Assert.Contains("Sales", resolution.Message, StringComparison.Ordinal);
        Assert.Contains("query 11", resolution.Message, StringComparison.Ordinal);
        Assert.Contains("plan 22", resolution.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ForcePlanFailure_ExcludedDatabase_IsNeverAlerted()
    {
        /* Parity with every other database-scoped family: the shared exclusion list wins, case-insensitively.
           A monitored-but-excluded database must not produce alerts an operator cannot mute per-database. */
        var h = new Harness();
        h.Settings.ForcePlanFailureEnabled = true;
        h.Settings.ExcludedDatabasesList.Add("sAlEs");
        h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo { DatabaseName = "Sales", QueryId = 11, PlanId = 22, ForcingType = "MANUAL", FailureReason = "NO_INDEX", FailureDelta = 5, TotalFailures = 5 });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task DatabaseState_ChosenState_AlreadyAnnounced_StaysQuiet()
    {
        /* #2166: the reporter's case — a database parked OFFLINE for a month generated hundreds of
           identical alerts. With the state already recorded as announced, a fresh evaluation must be
           SILENT even though the deviation is still present and no cooldown is in play. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "OFFLINE" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task DatabaseState_ChosenState_FirstObservation_FiresAndRecordsIt()
    {
        /* The transition still alerts — edge-triggered, not silenced — and the state is recorded so the
           NEXT evaluation is the quiet one. Recording is what makes the silence survive a restart. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains(h.StateStore.DatabaseStateAlerted, r => r.Db == "Archive" && r.State == "OFFLINE");
    }

    [Fact]
    public async Task DatabaseState_IntegrityState_StillRepeats_EvenWhenAlreadyAnnounced()
    {
        /* Nobody parks a database in SUSPECT, so continued repetition IS the signal there. An already-
           announced integrity state must keep firing on the cooldown — if this ever goes quiet, a real
           corruption stops nagging, which is the failure mode worth protecting against. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Payments", StateDesc = "SUSPECT", ExpectedState = "ONLINE", LastAlertedState = "SUSPECT" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Critical, fired.Severity);
    }

    [Fact]
    public async Task DatabaseState_ChosenState_ChangingToADifferentState_FiresAgain()
    {
        /* The composition property the reporter identified: going quiet for a parked state must NOT mean
           going blind. A database announced as OFFLINE that turns SUSPECT is a different state, so it
           alerts — and at Critical, not inheriting the quiet treatment of the state it left. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "SUSPECT", ExpectedState = "ONLINE", LastAlertedState = "OFFLINE" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Critical, fired.Severity);
    }

    [Fact]
    public async Task DatabaseState_TransitionToADifferentState_IsNotSuppressedByThePriorStatesCooldown()
    {
        /* The safety property, tested where it actually breaks. Both evaluations happen inside one cooldown
           window (they run back to back, so no wall-clock time passes), which is exactly the case the old
           per-database cooldown key swallowed: OFFLINE fires and stamps the database's only clock, then the
           flip to SUSPECT finds that clock still running and goes silent — permanently, now that a chosen
           state no longer re-fires every cooldown. SUSPECT is the state this alert must never lose. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        var engine = h.Build();

        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "" });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Same database, still deviating, but a DIFFERENT state — and the memory now says OFFLINE, which is
           what makes alreadyAnnounced false while the OFFLINE cooldown is still warm. */
        h.Deliverer.Outcomes.Clear();
        h.Adapter.DatabaseStates.Clear();
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "SUSPECT", ExpectedState = "ONLINE", LastAlertedState = "OFFLINE" });
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Critical, fired.Severity);
    }

    [Fact]
    public async Task DatabaseState_MutedFire_DoesNotRecordItAsAnnounced_SoUnmutingStillNotifies()
    {
        /* A mute must be reversible. The four edge-triggered states gate ALL future firing on the announced
           memory, so stamping it under a mute made the mute permanent: mute a parked database, remove the
           mute, and the alert never came back for as long as the state held. Muting suppresses delivery, not
           the engine's honesty about whether anyone was actually told. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Muted = true;
        var engine = h.Build();

        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "" });
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var muted = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(muted.Muted, "the fire itself must still be marked muted");
        Assert.DoesNotContain(h.StateStore.DatabaseStateAlerted, r => r.Db == "Archive");
        Assert.False(h.StateStore.Memory.ContainsKey("Archive"),
            "a muted fire must not record the state as announced — nobody was told");

        /* Mute removed, cooldown elapsed, same state still deviating. The adapter reports what the store
           holds, which is still nothing — so this must notify for real. */
        h.Muted = false;
        h.Now = h.Now.AddDays(1);
        h.Deliverer.Outcomes.Clear();
        h.Adapter.DatabaseStates.Clear();
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo
        {
            DatabaseName = "Archive",
            StateDesc = "OFFLINE",
            ExpectedState = "ONLINE",
            LastAlertedState = h.StateStore.Memory.TryGetValue("Archive", out var remembered) ? remembered : "",
        });
        await engine.EvaluateServerAsync(Harness.Snapshot());

        var announced = Assert.Single(h.Deliverer.Outcomes);
        Assert.False(announced.Muted, "unmuting must produce a real, deliverable alert");
        Assert.Contains(h.StateStore.DatabaseStateAlerted, r => r.Db == "Archive" && r.State == "OFFLINE");
    }

    [Fact]
    public async Task DatabaseState_RecoveryDoesNotClearACooldown_ForADatabaseWhoseNameContainsTheOldDelimiter()
    {
        /* SQL Server permits '|' in a database name, so while the cooldown key was a delimited STRING,
           recovering "Foo" prefix-matched and wiped "Foo|Bar"'s clock as well. Keying by tuple removes the
           bug class rather than documenting it.

           Observable via an integrity state: SUSPECT is not edge-suppressed (RepeatsAreNoise is false), so
           its cooldown is the ONLY thing keeping it quiet on the second evaluation. If the recovery sweep
           wrongly cleared it, "Foo|Bar" fires again here. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        var engine = h.Build();

        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Foo", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "" });
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Foo|Bar", StateDesc = "SUSPECT", ExpectedState = "ONLINE", LastAlertedState = "" });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);

        /* Foo returns to expected and drops out; Foo|Bar is untouched and still SUSPECT. */
        h.Deliverer.Outcomes.Clear();
        h.Adapter.DatabaseStates.Clear();
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Foo|Bar", StateDesc = "SUSPECT", ExpectedState = "ONLINE", LastAlertedState = "SUSPECT" });
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.DoesNotContain(h.StateStore.DatabaseStateCleared, r => r.Db == "Foo|Bar");
    }

    [Fact]
    public async Task DatabaseState_SameState_StillRateLimitsItself_WithinOneCooldown()
    {
        /* The other side of keying by state: it must not have turned the cooldown off. An integrity state
           repeats deliberately (RepeatsAreNoise is false for SUSPECT), so the only thing standing between it
           and an alert per evaluation is its own cooldown — which must still hold inside one window. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        var engine = h.Build();

        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Payments", StateDesc = "SUSPECT", ExpectedState = "ONLINE", LastAlertedState = "SUSPECT" });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        h.Deliverer.Outcomes.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task DatabaseState_RepeatEpisode_OfTheSameState_FiresAgainAfterRecovery()
    {
        /* The falling-edge property, driven as a full round trip through the store's MEMORY rather than
           through call counting — the decoupling that let the first cut of #2166 ship with a permanent
           memory. Park, recover, park again in the SAME state: the repeat soft-delete workflow this alert
           exists for. If recovery does not clear what firing recorded, evaluation 3 reads OFFLINE ==
           OFFLINE, judges itself already-announced, and the second parking is swallowed for good. */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        var engine = h.Build();

        /* Episode 1: parked. No memory yet, so it announces and records. */
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "" });
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("OFFLINE", h.StateStore.Memory["Archive"]);

        /* Recovery: back to expected, so it stops deviating and drops out of the adapter's results. */
        h.Adapter.DatabaseStates.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Contains(h.StateStore.DatabaseStateCleared, r => r.Db == "Archive");
        Assert.False(h.StateStore.Memory.ContainsKey("Archive"),
            "recovery must forget the announced state, or the edge can never trigger a second time");

        /* Episode 2: parked again, same state. The adapter reports whatever the store now holds — which is
           the whole point — so this fires only if the clear above actually happened. */
        h.Deliverer.Outcomes.Clear();
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo
        {
            DatabaseName = "Archive",
            StateDesc = "OFFLINE",
            ExpectedState = "ONLINE",
            LastAlertedState = h.StateStore.Memory.TryGetValue("Archive", out var remembered) ? remembered : "",
        });
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public void DatabaseState_AlertedStamp_IsAnUpdate_NeverAnInsert()
    {
        /* A row is only absent when the database was first observed in an integrity state, which the seed
           logic deliberately refuses to baseline. An INSERT here must supply expected_state (NOT NULL) and
           the only value on hand is the state being alerted ON — so inserting would baseline a SUSPECT
           database as "expected SUSPECT", stop it deviating, report it RECOVERED while still corrupt, and
           silence it permanently. Strictly worse than the repetition being fixed, so it is pinned. */
        var source = ReadStateStoreSource();
        var method = source[source.IndexOf("public async Task SaveDatabaseStateAlertedAsync", StringComparison.Ordinal)..];
        var body = method[..method.IndexOf("public async Task ClearDatabaseStateAlertedAsync", StringComparison.Ordinal)];

        Assert.Contains("UPDATE config.database_state_expected", body, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT INTO config.database_state_expected", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ON CONFLICT", body, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseState_TheNeverBaselinedList_IsWiderThanTheCriticalOne()
    {
        /* #2189. Two lists that look interchangeable and are not, which is exactly why they are worth
           pinning: one answers "bad enough to page about with no baseline to compare against", the other
           "must never be LEARNED as this database's normal". A transient state belongs only in the second.

           Collapsing them either way is a shipped bug. Widen the critical list and every restore in progress
           pages. Narrow the never-baselined list and a database observed mid-restore learns RESTORING as
           expected, then alerts forever for being ONLINE — the reported bug, 636 fires in 24 hours. */
        Assert.Contains(DatabaseStateTokens.Suspect, DatabaseStateTokens.CriticalSqlList, StringComparison.Ordinal);
        Assert.Contains(DatabaseStateTokens.RecoveryPending, DatabaseStateTokens.CriticalSqlList, StringComparison.Ordinal);
        Assert.Contains(DatabaseStateTokens.Emergency, DatabaseStateTokens.CriticalSqlList, StringComparison.Ordinal);

        /* A pending database in a transient state must stay SILENT, so these must not reach the critical arm. */
        Assert.DoesNotContain(DatabaseStateTokens.Restoring, DatabaseStateTokens.CriticalSqlList, StringComparison.Ordinal);
        Assert.DoesNotContain(DatabaseStateTokens.Recovering, DatabaseStateTokens.CriticalSqlList, StringComparison.Ordinal);

        Assert.StartsWith(DatabaseStateTokens.CriticalSqlList, DatabaseStateTokens.NeverBaselinedSqlList, StringComparison.Ordinal);
        Assert.Contains($"'{DatabaseStateTokens.Restoring}'", DatabaseStateTokens.NeverBaselinedSqlList, StringComparison.Ordinal);
        Assert.Contains($"'{DatabaseStateTokens.Recovering}'", DatabaseStateTokens.NeverBaselinedSqlList, StringComparison.Ordinal);

        /* STANDBY is synthetic and stable by construction — the whole reason it exists is to give a
           log-shipping secondary one steady token instead of the RESTORING flicker underneath it. Refusing to
           learn it would leave every standby secondary permanently pending for no benefit. */
        Assert.DoesNotContain(DatabaseStateTokens.Standby, DatabaseStateTokens.NeverBaselinedSqlList, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseState_BothDarlingSeedSites_ShareTheOneStateList()
    {
        /* The viewer's editor seeds and heals baselines with its own copy of this SQL because that project
           cannot reference the service's. Two hand-kept copies of "what must never be learned" is the drift
           that lets the editor write a baseline the alert refuses to — silently, and only for operators who
           happen to open the editor mid-restore. Both sites interpolate the shared constant instead, and
           BOTH statements use it: the seed to refuse those states, the heal to un-write them (#2189). A copy
           that widened only one of the two would be the subtler half of the same bug. */
        var viewer = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.DatabaseStates.cs"));
        var service = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs"));

        foreach (var source in new[] { viewer, service })
        {
            Assert.Contains("NOT IN ({DatabaseStateTokens.NeverBaselinedSqlList})", source, StringComparison.Ordinal);
            Assert.Contains("expected_state IN ({DatabaseStateTokens.NeverBaselinedSqlList})", source, StringComparison.Ordinal);
            Assert.DoesNotContain($"NOT IN ('{DatabaseStateTokens.Suspect}'", source, StringComparison.Ordinal);

            /* The heal must never be reachable for a state somebody DECLARED, in either copy. */
            Assert.Contains("is_user_override = false", source, StringComparison.Ordinal);
        }
    }

    private static string ReadStateStoreSource() =>
        ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "PgAlertStateStore.cs"));

    /// <summary>Reads a repo-relative source file, walking up from this test file to find the repo root.</summary>
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }

    [Fact]
    public async Task DatabaseState_Disabled_DoesNotFetch()
    {
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = false;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "X", StateDesc = "OFFLINE", ExpectedState = "ONLINE" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(0, h.Adapter.DatabaseStateFetches);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task DatabaseState_FiresPerDatabase_GradingSeverityByCurrentState()
    {
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Payments", StateDesc = "SUSPECT", ExpectedState = "ONLINE" });
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal("Database State", o.MetricName));

        var suspect = h.Deliverer.Outcomes.Single(o => o.CurrentValue.StartsWith("Payments"));
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Critical, suspect.Severity);
        var offline = h.Deliverer.Outcomes.Single(o => o.CurrentValue.StartsWith("Archive"));
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Warning, offline.Severity);
    }

    [Fact]
    public async Task DatabaseState_CooldownSuppressesSecondFire_ThenResolvesWhenBackToExpected()
    {
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Payments", StateDesc = "OFFLINE", ExpectedState = "ONLINE" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Same deviation next sweep, inside the cooldown window — no second fire. */
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Database returns to its expected state — deviation clears, resolution announced. */
        h.Adapter.DatabaseStates.Clear();
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains(h.Resolutions, r => r.MetricName == "Database State" && r.Message.Contains("Payments"));
    }

    [Fact]
    public async Task DatabaseState_PendingCriticalFirstObservation_FiresCriticalWithNoBaselineMessage()
    {
        /* A critical first observation has no baseline (empty expected) — the store returns it as pending;
           the engine must fire CRITICAL and word it as a first observation, not "expected UNKNOWN". */
        var h = new Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo { DatabaseName = "Payments", StateDesc = "SUSPECT", ExpectedState = "" });
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var o = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Database State", o.MetricName);
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Critical, o.Severity);
        Assert.Contains("no baseline", o.ShortMessage);
    }
}
