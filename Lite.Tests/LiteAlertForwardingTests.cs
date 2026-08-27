/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;
using EngineCpuAlertMode = PerformanceMonitor.Alerting.CpuAlertMode;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Phase-5 forwarding equivalence pins: Lite's alert loop now delegates to the shared
/// <see cref="AlertEngine"/> through Lite's REAL seam implementations (the live
/// <see cref="AppAlertEngineSettings"/> over the App.* statics; <see cref="LiteAlertDeliverer"/>
/// through its injected test seams; <see cref="LiteAlertStateStore"/> over a real DuckDB file),
/// and every scenario here is pinned to the PRE-FORWARDING loop's behavior with the old
/// <c>Lite/MainWindow.AlertEngine.cs</c> line cited per pin (the deleted source is at
/// <c>git show 5869ff9:Lite/MainWindow.AlertEngine.cs</c>). ZERO behavior change is the bar:
/// same toast strings/icons, same history sends, same watermark persistence, same suppression
/// and badge semantics.
/// <para>
/// #1965: this class is NOT the only one that touches the App.Alert* statics, which is what the
/// note here used to claim. <see cref="Lite.Tests.McpAlertSettingsKeyTests"/> sets and reads
/// App.AlertCpuMode, and <see cref="AlertSettingsCredentialLoadTests"/> drives App.LoadAlertSettings,
/// which rewrites the whole alert block (App.xaml.cs:611 for CpuMode alone). xUnit runs separate
/// classes in PARALLEL, so a write from here landed between that class's set and its assert and
/// failed it. All four classes therefore share the "app-alert-statics" collection — the same
/// serialization idiom the SMTP/webhook pair already used, widened to the statics it actually covers.
/// The constructor resets every alert static to its App.xaml.cs default before each test, and
/// Dispose puts them back after each one so ordering cannot leak out of the class either.
/// </para>
/// </summary>
[Collection("app-alert-statics")]
public class LiteAlertForwardingTests : IDisposable
{
    private const string Key = "101";
    private const string Name = "SRV-A";

    public LiteAlertForwardingTests() => ResetAlertStaticsToDefaults();

    /// <summary>Leave the statics as the rest of the assembly expects to find them (#1965).</summary>
    public void Dispose() => ResetAlertStaticsToDefaults();

    private static void ResetAlertStaticsToDefaults()
    {
        /* Reset the App.Alert* statics to their App.xaml.cs:94-160 defaults so every test starts
           from the shipped configuration (xunit news up the class per test method). */
        App.AlertsEnabled = true;
        App.AlertCpuEnabled = true;
        App.AlertCpuThreshold = 80;
        App.AlertCpuMode = CpuAlertMode.Total;
        App.AlertBlockingEnabled = true;
        App.AlertBlockingThreshold = 1;
        /* Default 0 = off (App.xaml.cs:134). Reset because EngineSettings_PassThroughEveryMember_Live
           sets it to 745 and nothing else put it back. */
        App.AlertBlockingWaitSecondsThreshold = 0;
        App.AlertDeadlockEnabled = true;
        App.AlertDeadlockThreshold = 1;
        App.AlertPoisonWaitEnabled = true;
        App.AlertPoisonWaitThresholdMs = 500;
        App.AlertLongRunningQueryEnabled = true;
        App.AlertLongRunningQueryThresholdMinutes = 30;
        App.AlertLongRunningQueryMaxResults = 5;
        App.AlertLongRunningQueryExcludeSpServerDiagnostics = true;
        App.AlertLongRunningQueryExcludeWaitFor = true;
        App.AlertLongRunningQueryExcludeBackups = true;
        App.AlertLongRunningQueryExcludeMiscWaits = true;
        App.AlertLongRunningQueryExcludeCdc = true;
        App.AlertExcludedDatabases = new List<string>();
        App.AlertTempDbSpaceEnabled = true;
        App.AlertTempDbSpaceThresholdPercent = 80;
        App.AlertLowDiskEnabled = true;
        App.AlertLowDiskThresholdPercent = 10;
        App.AlertLowDiskThresholdGb = 5;
        App.AlertPvsEnabled = true;
        App.AlertPvsThresholdPercent = 40;
        App.AlertPvsFloorGb = 1;
        App.AlertLongRunningJobEnabled = true;
        App.AlertLongRunningJobMultiplier = 3;
        App.AlertFailedJobEnabled = true;
        App.AlertFailedJobLookbackMinutes = 60;
        App.AlertCooldownMinutes = 5;
        App.AlertDeliveryMode = AlertNotificationMode.Summary;
        App.AlertPerEventMaxPerCycle = 10;
    }

    /* ---------------- fakes (adapter/store/deliverer — the engine sees Lite's REAL settings) ---------------- */

    private sealed class FakeReadAdapter : IAlertReadAdapter
    {
        public List<BlockedProcessAlertRow> Blocking { get; } = new();
        public List<DeadlockAlertRow> Deadlocks { get; } = new();
        public List<PoisonWaitDelta> PoisonWaits { get; } = new();
        public List<LongRunningQueryInfo> LongRunning { get; } = new();
        public List<VolumeFreeSpaceInfo> Volumes { get; } = new();
        public TempDbSpaceInfo? TempDb { get; set; }
        public List<AnomalousJobInfo> AnomalousJobs { get; } = new();

        public Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<BlockedProcessAlertRow>(Blocking));

        /* #1839: null = no blocking snapshot in the store; tests that exercise the gate assign one. */
        public CurrentBlockingWaitResult? BlockingWait { get; set; }

        public Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(BlockingWait);

        public Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(string serverKey, int hoursBack, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DeadlockAlertRow>(Deadlocks));

        public Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(string serverKey, double thresholdMs, CancellationToken cancellationToken = default) =>
            Task.FromResult(PoisonWaits.FindAll(w => w.AvgMsPerWait >= thresholdMs));

        public Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
            string serverKey, int thresholdMinutes, int maxResults,
            bool excludeSpServerDiagnostics, bool excludeWaitFor, bool excludeBackups, bool excludeMiscWaits, bool excludeCdc,
            IReadOnlyList<string> excludedDatabases, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<LongRunningQueryInfo>(LongRunning));

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
            Task.FromResult(new List<PvsPressureInfo>());

        /* #1812: fresh by default so pre-existing scenarios keep their meaning. */
        public bool SnapshotIsStale { get; set; }

        public Task<AnomalousJobsResult> GetAnomalousJobsAsync(string serverKey, int multiplier, CancellationToken cancellationToken = default) =>
            Task.FromResult(SnapshotIsStale
                ? AnomalousJobsResult.Stale
                : new AnomalousJobsResult(SnapshotIsFresh: true, new List<AnomalousJobInfo>(AnomalousJobs)));

        public List<DatabaseStateInfo> DatabaseStates { get; } = new();

        public Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<DatabaseStateInfo>(DatabaseStates));

        /* #2157: settable so a forwarding test can plant a risen-counter row. */
        public List<ForcePlanFailureInfo> ForcePlanFailures { get; } = new();

        public Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(string serverKey, CancellationToken cancellationToken = default) =>
            Task.FromResult(new List<ForcePlanFailureInfo>(ForcePlanFailures));
    }

    private sealed class InMemoryStateStore : IAlertStateStore
    {
        public Dictionary<(string Key, string Metric), int> EdgeWatermarks { get; } = new();
        public Dictionary<string, DateTime> FailedJobWatermarks { get; } = new();
        public List<(string Key, string Metric, int Watermark)> SavedEdge { get; } = new();

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
            return Task.CompletedTask;
        }

        /* #2216: replace-the-set, exactly like both real stores — whatever arrives IS the metric's state, so
           an empty map clears it. A fake that merged instead would hide the falling-edge bug class. */
        public Dictionary<(string Key, string Metric), Dictionary<string, IncidentOccurrenceState>> Occurrences { get; } = new();

        public Task<IReadOnlyDictionary<string, IncidentOccurrenceState>> LoadIncidentOccurrencesAsync(string serverKey, string metricName) =>
            Task.FromResult<IReadOnlyDictionary<string, IncidentOccurrenceState>>(
                Occurrences.TryGetValue((serverKey, metricName), out var states)
                    ? states
                    : new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal));

        public Task SaveIncidentOccurrencesAsync(string serverKey, string metricName, IReadOnlyDictionary<string, IncidentOccurrenceState> states)
        {
            var replacement = new Dictionary<string, IncidentOccurrenceState>(StringComparer.Ordinal);
            foreach (var entry in states)
            {
                replacement[entry.Key] = entry.Value;
            }
            Occurrences[(serverKey, metricName)] = replacement;
            return Task.CompletedTask;
        }

        /* #2166 */
        public List<(string Server, string Db, string State)> DatabaseStateAlerted { get; } = new();

        public Task SaveDatabaseStateAlertedAsync(string serverKey, string databaseName, string effectiveState)
        {
            DatabaseStateAlerted.Add((serverKey, databaseName, effectiveState));
            return Task.CompletedTask;
        }

        public List<(string Server, string Db)> DatabaseStateCleared { get; } = new();

        public Task ClearDatabaseStateAlertedAsync(string serverKey, string databaseName)
        {
            DatabaseStateCleared.Add((serverKey, databaseName));
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

    /// <summary>Engine over Lite's REAL live-settings adapter + fakes + a controllable clock.</summary>
    private sealed class Harness
    {
        public FakeReadAdapter Adapter { get; } = new();
        public InMemoryStateStore StateStore { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public List<AlertResolution> Resolutions { get; } = new();
        public List<FailedJobInfo> FailedJobs { get; } = new();
        public bool Muted { get; set; }
        public DateTime Now { get; set; } = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        public AlertEngine Build() => new(
            new AppAlertEngineSettings(),
            Adapter, StateStore, Deliverer,
            isAlertMuted: _ => Muted,
            failedJobsFetcher: (_, _, _) => Task.FromResult(new List<FailedJobInfo>(FailedJobs)),
            resolutionCallback: (r, _) => { Resolutions.Add(r); return Task.CompletedTask; },
            logger: null,
            utcNow: () => Now);

        public DateTime? FailedJobWatermark() =>
            StateStore.FailedJobWatermarks.TryGetValue(Key, out var w) ? w : (DateTime?)null;

        public static AlertServerSnapshot Snapshot(
            double? sqlCpu = null, double? totalCpu = null,
            bool isOnline = true, bool isAzureSqlDb = false, bool suppressed = false) =>
            new(Key, Name, isOnline, sqlCpu, totalCpu, isAzureSqlDb, suppressed);
    }

    /// <summary>Everything except the named check off, so a scenario pins exactly one alert.</summary>
    private static void DisableAllChecks()
    {
        App.AlertCpuEnabled = false;
        App.AlertBlockingEnabled = false;
        App.AlertDeadlockEnabled = false;
        App.AlertPoisonWaitEnabled = false;
        App.AlertLongRunningQueryEnabled = false;
        App.AlertTempDbSpaceEnabled = false;
        App.AlertLowDiskEnabled = false;
        App.AlertLongRunningJobEnabled = false;
        App.AlertFailedJobEnabled = false;
    }

    private static BlockedProcessAlertRow BlockingRow(int blockedSpid) => new()
    {
        EventTime = new DateTime(2026, 7, 1, 11, 55, 0),
        DatabaseName = "StackOverflow",
        BlockedSpid = blockedSpid,
        BlockingSpid = blockedSpid + 100,
        WaitTimeMs = 12000,
        LockMode = "X",
        BlockedSqlText = "UPDATE Users SET x = 1",
        BlockingSqlText = "BEGIN TRAN UPDATE Users",
        ContentiousObject = "StackOverflow.dbo.Users",
        Source = BlockedProcessAlertRow.XeReportSource
    };

    private static DeadlockAlertRow DeadlockRow() => new()
    {
        VictimProcessId = "process1",
        VictimSqlText = "UPDATE Users SET Reputation = 1",
        DeadlockGraphXml =
            @"<deadlock><victim-list><victimProcess id=""process1""/></victim-list><process-list><process id=""process1"" spid=""55"" currentdbname=""StackOverflow""><inputbuf>UPDATE Users SET Reputation = 1</inputbuf></process><process id=""process2"" spid=""60"" currentdbname=""StackOverflow""><inputbuf>UPDATE Badges SET Name = 'x'</inputbuf></process></process-list><resource-list><keylock objectname=""StackOverflow.dbo.Users""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock></resource-list></deadlock>"
    };

    /* =====================================================================================
       1. CPU fire → resolve strings (old MainWindow.AlertEngine.cs:64-116)
       ===================================================================================== */

    [Fact]
    public async Task Cpu_FireAndResolve_CarriesTheOldLoopsExactStrings()
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var h = new Harness();
        var engine = h.Build();

        /* Fire: Total mode uses TotalCpuPercent (:65 CpuPercentForAlert → Total). */
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 92));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("High CPU", fired.MetricName);
        /* :96 — email current value; :97 threshold. */
        Assert.Equal("92% (Total CPU)", fired.CurrentValue);
        Assert.Equal("80%", fired.ThresholdValue);
        /* :91 — the detail text block. */
        Assert.Equal("  Total CPU: 92%\n  Threshold: 80%", fired.DetailText);
        /* :84 — the toast body minus the "{server}: " prefix. */
        Assert.Equal("Total CPU at 92% (threshold: 80%)", fired.ShortMessage);
        /* :93-100 — CPU passes no context. #1830: the numerics are REQUIRED — without them the
           history stores text-parsed "92% (Total CPU)", failed on the parenthesized label, and
           stored 0 for every High CPU row. */
        Assert.Null(fired.Context);
        Assert.Equal(92d, fired.NumericCurrentValue);
        Assert.Equal(80d, fired.NumericThresholdValue);
        Assert.False(fired.Muted);

        /* Resolve: :110-113 — exact title + message strings, Success-severity tray-only toast. */
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 10, totalCpu: 12));

        var res = Assert.Single(h.Resolutions);
        Assert.Equal("CPU Resolved", res.Title);
        Assert.Equal("SRV-A: Total CPU back to 12%", res.Message);
        Assert.Single(h.Deliverer.Outcomes); /* no new fire, no history row for the resolve */
    }

    [Fact]
    public async Task Cpu_SqlOnlyMode_UsesSqlValueAndLabel()
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        App.AlertCpuMode = CpuAlertMode.SqlOnly; /* :66 — label follows the mode */
        var h = new Harness();

        /* SqlOnly compares CpuPercent (90), not Total (95) — :65 CpuPercentForAlert → SqlOnly. */
        await h.Build().EvaluateServerAsync(Harness.Snapshot(sqlCpu: 90, totalCpu: 95));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("90% (SQL CPU)", fired.CurrentValue);
        Assert.Equal("SQL CPU at 90% (threshold: 80%)", fired.ShortMessage);
    }

    /* =====================================================================================
       2. Blocking rolling-count fire + watermark persist (old :118-196 + #1091/#1145)
       ===================================================================================== */

    [Fact]
    public async Task Blocking_FiresOnce_PersistsWatermark_AndEdgeTriggerHoldsWithinTheHour()
    {
        DisableAllChecks();
        App.AlertBlockingEnabled = true;
        var h = new Harness();
        var engine = h.Build();
        h.Adapter.Blocking.Add(BlockingRow(51));
        h.Adapter.Blocking.Add(BlockingRow(52));

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Blocking Detected", fired.MetricName);
        Assert.Equal("2", fired.CurrentValue);                      /* :180 */
        Assert.Equal("1", fired.ThresholdValue);                    /* :181 */
        Assert.Equal("2 blocking session(s)", fired.ShortMessage);  /* :167 toast body */
        Assert.NotNull(fired.Context);                              /* :174 context built from the same rows */
        /* :149-151 — the #1145 watermark persisted on change, keyed by the shared metric row key. */
        Assert.Equal(2, h.StateStore.EdgeWatermarks[(Key, "Blocking Detected")]);

        /* Same 2 lingering reports after the cooldown → no re-fire (#1091, :140-145). */
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A third report climbs past the watermark → fires again (:143 gate), watermark moves. */
        h.Adapter.Blocking.Add(BlockingRow(53));
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(3, h.StateStore.EdgeWatermarks[(Key, "Blocking Detected")]);
    }

    [Fact]
    public async Task Blocking_RestartWithPersistedWatermark_DoesNotReAlert()
    {
        /* Pinned #1145 behavior: the old loop bulk-seeded _lastAlertedBlockingCount at startup
           (old MainWindow.xaml.cs:1563-1594); the engine seeds per key from the same store rows
           before the first sweep — a restart must not re-alert on already-alerted counts. */
        DisableAllChecks();
        App.AlertBlockingEnabled = true;
        var h = new Harness();
        h.Adapter.Blocking.Add(BlockingRow(51));
        h.Adapter.Blocking.Add(BlockingRow(52));
        h.StateStore.EdgeWatermarks[(Key, "Blocking Detected")] = 2; /* persisted pre-restart */

        await h.Build().EvaluateServerAsync(Harness.Snapshot());     /* fresh engine = restart */

        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* =====================================================================================
       3. Suppressed sweep: delivers nothing, blocking watermark does not advance
       (old :62 suppressPopups; RollingCountAlertGate suppressed semantics)
       ===================================================================================== */

    [Fact]
    public async Task SuppressedSweep_DeliversNothing_AndDoesNotAdvanceBlockingWatermark()
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        App.AlertBlockingEnabled = true;
        var h = new Harness();
        var engine = h.Build();
        h.Adapter.Blocking.Add(BlockingRow(51));

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 95, totalCpu: 99, suppressed: true));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
        /* :143 — the gate is told about suppression, so the watermark stays 0 and nothing was
           persisted; when the user un-acknowledges, the same lingering report still alerts. */
        Assert.Empty(h.StateStore.SavedEdge);

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 95, totalCpu: 99));
        Assert.Equal(2, h.Deliverer.Outcomes.Count); /* CPU + blocking both fire once unsuppressed */
    }

    /* =====================================================================================
       4. Muted alert → still delivered flagged-muted (history row still written)
       (old :76-77,:80,:99 — mute skips the toast, the send still runs with muted:true and
        EmailAlertService.TrySendAlertEmailAsync ALWAYS logs the history row)
       ===================================================================================== */

    [Fact]
    public async Task MutedAlert_IsDeliveredFlaggedMuted_SoTheHistoryRowIsStillWritten()
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var h = new Harness { Muted = true };
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 92));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted);
        /* The cooldown was stamped even when muted (:76-78) — no second delivery inside it. */
        h.Now = h.Now.AddMinutes(2);
        await engine.EvaluateServerAsync(Harness.Snapshot(sqlCpu: 70, totalCpu: 92));
        Assert.Single(h.Deliverer.Outcomes);
    }

    /* =====================================================================================
       5. Low-disk worsening + CRITICAL grading (old :477-557, #754 follow-up + #1136)
       ===================================================================================== */

    [Fact]
    public async Task LowDisk_FiresOnFreshBreach_OnlyRefiresOnWorsening_AndGradesCritical()
    {
        DisableAllChecks();
        App.AlertLowDiskEnabled = true;
        var h = new Harness();
        var engine = h.Build();
        /* 8% free (64/800 GB) — breaches the 10% threshold, not critically low (>3% and >2GB). */
        var volume = new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 819200, FreeMb = 65536 };
        h.Adapter.Volumes.Add(volume);

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Volume Free Space", fired.MetricName);
        /* :530 current value and :510 toast body share the same string. */
        Assert.Equal("D:\\ 8% free (64.0 GB)", fired.CurrentValue);
        Assert.Equal("D:\\ 8% free (64.0 GB)", fired.ShortMessage);
        Assert.Equal("10% / 5 GB", fired.ThresholdValue);           /* :531 FormatLowDiskThreshold */
        Assert.Null(fired.Severity);                                /* :521 — not critically low → WARNING default */

        /* Standing, unchanged breach after the cooldown → NO re-fire (#754 follow-up :497-499). */
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Worsening past the 1% margin AND critically low (2% ≤ 3%) → re-fires graded CRITICAL
           (#1136 :521-524). */
        volume.FreeMb = 16384; /* 16 GB = 2% ... also > 2GB so the percent dimension trips it */
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[1].Severity);

        /* Recovery → the exact resolve strings (:545-548) and the worsening watermark clears. */
        h.Adapter.Volumes.Clear();
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        var res = Assert.Single(h.Resolutions);
        Assert.Equal("Volume Free Space Resolved", res.Title);
        Assert.Equal("SRV-A: All volumes back above threshold", res.Message);
    }

    /* =====================================================================================
       6. Failed-job watermark dedup + restart survival (old :636-719 + PR #1173/#1145)
       ===================================================================================== */

    [Fact]
    public async Task FailedJob_Fires_PersistsWatermark_DedupsSameFailure_AndSurvivesRestart()
    {
        DisableAllChecks();
        App.AlertFailedJobEnabled = true;
        var h = new Harness();
        var engine = h.Build();
        var runTime = new DateTime(2026, 7, 1, 11, 45, 0); /* SERVER-LOCAL, never coerced to UTC */
        h.FailedJobs.Add(new FailedJobInfo { JobName = "NightlyETL", JobId = "job-1", RunDateTime = runTime, StepId = 2, StepName = "Load", Message = "boom" });

        await engine.EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Failed Agent Job", fired.MetricName);
        Assert.Equal("1 job failure(s) in last 60m — NightlyETL", fired.CurrentValue); /* :703 */
        Assert.Equal("1 job failure(s) — NightlyETL", fired.ShortMessage);             /* :690 toast body */
        /* :684 — the server-local watermark persisted the moment the alert fired. */
        Assert.Equal(runTime, h.FailedJobWatermark());

        /* The same failure lingers in the lookback window → no re-fire (:667-669). */
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Restart: a FRESH engine over the same store seeds the watermark and stays quiet
           (#1145 parity — the old bulk seed at MainWindow.xaml.cs:1584-1588). */
        var restarted = h.Build();
        h.Now = h.Now.AddMinutes(10);
        await restarted.EvaluateServerAsync(Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A strictly newer failure fires again (:667 hasNewFailure). */
        h.FailedJobs.Add(new FailedJobInfo { JobName = "NightlyETL", JobId = "job-1", RunDateTime = runTime.AddMinutes(30), StepId = 2, StepName = "Load", Message = "boom" });
        h.Now = h.Now.AddMinutes(10);
        await restarted.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task FailedJob_OfflineOrAzure_SkipsTheCheck()
    {
        /* :651-655 — only online, non-Azure-SQL-DB servers are queried (the msdb-access half of
           the old gate lives in MainWindow.FetchFailedJobsForAlertAsync, which degrades to an
           empty list). */
        DisableAllChecks();
        App.AlertFailedJobEnabled = true;
        var h = new Harness();
        h.FailedJobs.Add(new FailedJobInfo { JobName = "J", JobId = "1", RunDateTime = DateTime.Now });

        var offline = await h.Build().EvaluateServerAsync(Harness.Snapshot(isOnline: false));
        var azure = await h.Build().EvaluateServerAsync(Harness.Snapshot(isAzureSqlDb: true));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.False(offline.FailedJobConditionPresent);
        Assert.False(azure.FailedJobConditionPresent);
    }

    /* =====================================================================================
       7. The #754/#749 badge feed: standing conditions on the sweep result
       (old :54-57 locals + :721-734 sweep-end write — computed BEFORE suppression/watermark)
       ===================================================================================== */

    [Fact]
    public async Task SweepResult_CarriesStandingBadgeConditions_EvenWhenSuppressed()
    {
        DisableAllChecks();
        App.AlertLowDiskEnabled = true;
        App.AlertFailedJobEnabled = true;
        var h = new Harness();
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 819200, FreeMb = 65536 });
        h.FailedJobs.Add(new FailedJobInfo { JobName = "J", JobId = "1", RunDateTime = new DateTime(2026, 7, 1, 11, 45, 0) });

        /* Suppressed: nothing delivered, but the badge conditions are still observed —
           the old loop set curBadgeLowDisk (:487) / curBadgeFailedJob (:663) before the
           suppression gates. */
        var sweep = await h.Build().EvaluateServerAsync(Harness.Snapshot(suppressed: true));

        Assert.True(sweep.Evaluated);
        Assert.True(sweep.LowDiskConditionPresent);
        Assert.True(sweep.FailedJobConditionPresent);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task SweepResult_ClearsConditions_WhenChecksDisabledOrRecovered()
    {
        /* #1128 — a disabled feature clears a previously-lit badge at the sweep-end write. */
        DisableAllChecks();
        var h = new Harness();
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 819200, FreeMb = 65536 });
        h.FailedJobs.Add(new FailedJobInfo { JobName = "J", JobId = "1", RunDateTime = new DateTime(2026, 7, 1, 11, 45, 0) });

        var sweep = await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.True(sweep.Evaluated);
        Assert.False(sweep.LowDiskConditionPresent);
        Assert.False(sweep.FailedJobConditionPresent);
    }

    [Fact]
    public async Task MasterSwitchOff_ReturnsNotEvaluated_SoBadgesStayUntouched()
    {
        /* :40 — the old loop returned before ANY badge write when alerts were disabled. */
        App.AlertsEnabled = false;
        var h = new Harness();
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 819200, FreeMb = 65536 });

        var sweep = await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.False(sweep.Evaluated);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* =====================================================================================
       8. The remaining per-metric toast bodies (ShortMessage), verbatim from the old loop
       ===================================================================================== */

    [Fact]
    public async Task Deadlock_ToastBody_AndValues_MatchTheOldLoop()
    {
        DisableAllChecks();
        App.AlertDeadlockEnabled = true;
        var h = new Harness();
        h.Adapter.Deadlocks.Add(DeadlockRow());

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Deadlocks Detected", fired.MetricName);
        Assert.Equal("1", fired.CurrentValue);                                /* :257 */
        Assert.Equal("1 deadlock(s) in the last hour", fired.ShortMessage);   /* :244 toast body */
    }

    [Fact]
    public async Task PoisonWait_ToastBody_UsesTheWorstWait()
    {
        DisableAllChecks();
        App.AlertPoisonWaitEnabled = true;
        var h = new Harness();
        h.Adapter.PoisonWaits.Add(new PoisonWaitDelta { WaitType = "THREADPOOL", AvgMsPerWait = 750, DeltaMs = 15000, DeltaTasks = 20 });
        h.Adapter.PoisonWaits.Add(new PoisonWaitDelta { WaitType = "RESOURCE_SEMAPHORE", AvgMsPerWait = 600, DeltaMs = 6000, DeltaTasks = 10 });

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("THREADPOOL (750ms), RESOURCE_SEMAPHORE (600ms)", fired.CurrentValue); /* :288/:315 */
        Assert.Equal("500ms avg", fired.ThresholdValue);                                    /* :316 */
        Assert.Equal("THREADPOOL avg 750ms/wait", fired.ShortMessage);                      /* :302 toast body */
        Assert.Equal(750, fired.NumericCurrentValue);
    }

    [Fact]
    public async Task LongRunningQuery_ToastBody_CarriesSessionAndEightyCharPreview()
    {
        DisableAllChecks();
        App.AlertLongRunningQueryEnabled = true;
        var h = new Harness();
        var longText = "SELECT " + new string('x', 100); /* forces the :357 80-char preview cut */
        h.Adapter.LongRunning.Add(new LongRunningQueryInfo
        {
            SessionId = 55, DatabaseName = "SO", QueryText = longText, ElapsedSeconds = 2750 /* 45m 50s → 45 (integer division :356) */
        });

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("1 query(s), longest 45m", fired.CurrentValue);          /* :387 */
        Assert.Equal("30m", fired.ThresholdValue);                            /* :388 */
        var expectedPreview = PerformanceMonitor.Alerting.AlertContextBuilders.TruncateText(longText, 80);
        Assert.EndsWith("...", expectedPreview);
        Assert.Equal($"Session #55 running 45m — {expectedPreview}", fired.ShortMessage); /* :357-358, :374 */
    }

    [Fact]
    public async Task TempDb_ToastBody_MatchesTheOldLoop()
    {
        DisableAllChecks();
        App.AlertTempDbSpaceEnabled = true;
        var h = new Harness();
        h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 910, UnallocatedMb = 90 }; /* 91% used */

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("91% used (910 MB)", fired.CurrentValue);  /* :448 */
        Assert.Equal("tempdb 91% used", fired.ShortMessage);    /* :435 toast body */
    }

    /// <summary>
    /// #2515, the SKU-parity half: Lite and Darling share the engine and the type, so the ceiling has to
    /// change the decision identically on both. Darling's twin is
    /// <c>AlertEngineTests.TempDb_TheAzureCeiling_SuppressesTheAlert_ThatTheAllocationWouldFire</c>, with the
    /// same numbers — an operator must not get a page on one product and silence on the other for the same
    /// tempdb.
    /// </summary>
    [Fact]
    public async Task TempDb_TheAzureCeiling_SuppressesTheAlert_ThatTheAllocationWouldFire()
    {
        DisableAllChecks();
        App.AlertTempDbSpaceEnabled = true;
        Assert.Equal(80, App.AlertTempDbSpaceThresholdPercent);

        var h = new Harness();
        /* GP_S_Gen5_2 with one ~57 MB #temp table: 62.44 MB allocated, 65,536 MB of headroom behind it. */
        h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 59.75, UnallocatedMb = 2.69, MaxSizeMb = 65_536 };
        await h.Build().EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);

        /* The identical snapshot with the ceiling unmeasured is the pre-#2515 reading, and it pages. */
        var withoutCeiling = new Harness();
        withoutCeiling.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 59.75, UnallocatedMb = 2.69 };
        await withoutCeiling.Build().EvaluateServerAsync(Harness.Snapshot());
        var fired = Assert.Single(withoutCeiling.Deliverer.Outcomes);
        Assert.Equal("96% used (60 MB)", fired.CurrentValue);
        Assert.Equal("tempdb 96% used", fired.ShortMessage);
    }

    [Fact]
    public async Task LongRunningJob_ToastBody_CarriesJobNamePercentAndMinutes()
    {
        DisableAllChecks();
        App.AlertLongRunningJobEnabled = true;
        var h = new Harness();
        h.Adapter.AnomalousJobs.Add(new AnomalousJobInfo
        {
            JobName = "NightlyETL", JobId = "77", CurrentDurationSeconds = 5430, /* 90m (integer division :585) */
            AvgDurationSeconds = 1200, PercentOfAverage = 450, StartTime = new DateTime(2026, 7, 1, 10, 0, 0)
        });

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("1 job(s) exceeding 3x average", fired.CurrentValue);       /* :608 */
        Assert.Equal("3x historical avg", fired.ThresholdValue);                 /* :609 */
        Assert.Equal("NightlyETL at 450% of avg (90m)", fired.ShortMessage);     /* :595 toast body */
    }

    /* =====================================================================================
       9. LiteAlertDeliverer: the old delivery block, through its injected seams
       (old :80-100 toast-then-send order; :742-771 SendDetectedAlertAsync + override)
       ===================================================================================== */

    private sealed record ToastCall(string Title, string Message, BalloonIcon Icon, string ServerName, string MetricName);
    private sealed record SendCall(
        string MetricName, string ServerName, string CurrentValue, string ThresholdValue,
        int ServerId, AlertContext? Context, double? NumericCurrentValue, double? NumericThresholdValue,
        bool Muted, string? DetailText);

    private static (LiteAlertDeliverer Deliverer, List<ToastCall> Toasts, List<SendCall> Sends)
        BuildDeliverer(AlertNotificationMode? serverOverride = null)
    {
        var toasts = new List<ToastCall>();
        var sends = new List<SendCall>();
        var deliverer = new LiteAlertDeliverer(
            (title, message, icon, serverName, metricName) => toasts.Add(new ToastCall(title, message, icon, serverName, metricName)),
            (metricName, serverName, currentValue, thresholdValue, serverId, context, numCur, numThr, muted, detailText) =>
            {
                sends.Add(new SendCall(metricName, serverName, currentValue, thresholdValue, serverId, context, numCur, numThr, muted, detailText));
                return Task.CompletedTask;
            },
            _ => serverOverride);
        return (deliverer, toasts, sends);
    }

    private static AlertOutcome Outcome(
        string metric, string current = "cur", string threshold = "thr", AlertContext? context = null,
        double? numCur = null, double? numThr = null, bool muted = false, string? shortMessage = "body")
        => new(Key, Name, metric, current, threshold, context, "detail", numCur, numThr, muted, null, shortMessage);

    [Fact]
    public async Task Deliverer_Toast_TitleIsMetric_MessagePrefixedWithServer_IconTableMatches()
    {
        var (deliverer, toasts, _) = BuildDeliverer();

        /* Old loop's icon table: Error for deadlocks (:245) and poison waits (:303),
           Warning for CPU (:85), blocking (:168), LRQ (:375), tempdb (:436), volume (:511),
           long-running job (:596), failed job (:691). Title == metric name in every branch. */
        await deliverer.DeliverAsync(Outcome("High CPU", shortMessage: "Total CPU at 92% (threshold: 80%)"));
        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", shortMessage: "1 deadlock(s) in the last hour"));
        await deliverer.DeliverAsync(Outcome("Poison Wait", shortMessage: "THREADPOOL avg 750ms/wait"));
        await deliverer.DeliverAsync(Outcome("Volume Free Space", shortMessage: "D:\\ 8% free (64.0 GB)"));

        Assert.Equal(4, toasts.Count);
        Assert.Equal(new ToastCall("High CPU", "SRV-A: Total CPU at 92% (threshold: 80%)", BalloonIcon.Warning, Name, "High CPU"), toasts[0]);
        Assert.Equal(BalloonIcon.Error, toasts[1].Icon);
        Assert.Equal("SRV-A: 1 deadlock(s) in the last hour", toasts[1].Message); /* :244 full toast */
        Assert.Equal(BalloonIcon.Error, toasts[2].Icon);
        Assert.Equal(BalloonIcon.Warning, toasts[3].Icon);
    }

    [Fact]
    public async Task Deliverer_Muted_SkipsTheToast_ButStillSendsFlaggedMuted()
    {
        /* :80 (if (!isMuted) toast) + :93-99 (send always runs, muted: true → the history row is
           still written because TrySendAlertEmailAsync logs unconditionally). */
        var (deliverer, toasts, sends) = BuildDeliverer();

        await deliverer.DeliverAsync(Outcome("High CPU", muted: true));

        Assert.Empty(toasts);
        var send = Assert.Single(sends);
        Assert.True(send.Muted);
    }

    [Fact]
    public async Task Deliverer_NonBlockingMetrics_SendOnce_WithNumerics()
    {
        /* The old loop sent every non-blocking/deadlock metric directly (:312-322 poison,
           :384-394 LRQ, ...) with its numeric values — never through the per-event split, even
           when its context has incidents and the mode is PerEvent. */
        App.AlertDeliveryMode = AlertNotificationMode.PerEvent;
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident> { new("a", new[] { "dbo.Users" }), new("b", new[] { "dbo.Posts" }) }
        };
        var (deliverer, _, sends) = BuildDeliverer();

        await deliverer.DeliverAsync(Outcome("Long-Running Query", current: "1 query(s), longest 45m", threshold: "30m", context: context, numCur: 45, numThr: 30));

        var send = Assert.Single(sends);
        Assert.Equal(45, send.NumericCurrentValue);
        Assert.Equal(30, send.NumericThresholdValue);
        Assert.Same(context, send.Context);
    }

    [Fact]
    public async Task Deliverer_Blocking_SummaryMode_OneCombinedSend_ForwardsOutcomeNumerics()
    {
        /* :760-762 — the summary fallback carries the batched current value + detail text. #1830:
           the engine now supplies numerics on blocking/deadlock outcomes, and the deliverer must
           forward them — the old path dropped them to null on this route. */
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident> { new("a", new[] { "dbo.Users" }), new("b", new[] { "dbo.Posts" }) }
        };
        var (deliverer, _, sends) = BuildDeliverer();

        await deliverer.DeliverAsync(Outcome("Blocking Detected", current: "2", threshold: "1", context: context, numCur: 2, numThr: 1));

        var send = Assert.Single(sends);
        Assert.Equal("2", send.CurrentValue);
        Assert.Equal("detail", send.DetailText);
        Assert.Equal(2d, send.NumericCurrentValue);
        Assert.Equal(1d, send.NumericThresholdValue);
        Assert.Equal(101, send.ServerId);
    }

    [Fact]
    public async Task Deliverer_Blocking_PerEventMode_SplitsPerIncident_WithCapOverflow()
    {
        /* :748-757 — Per-event mode: one send per distinct incident, capped at
           AlertPerEventMaxPerCycle with a "+N more" trailer that keeps the remaining
           fingerprints; per-message detail text is rendered from the split context. */
        App.AlertDeliveryMode = AlertNotificationMode.PerEvent;
        App.AlertPerEventMaxPerCycle = 2;
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident>
            {
                new("a", new[] { "dbo.Users" }, OccurrenceCount: 3),
                new("b", new[] { "dbo.Posts" }, OccurrenceCount: 1),
                new("c", new[] { "dbo.Badges" }, OccurrenceCount: 2)
            }
        };
        var (deliverer, toasts, sends) = BuildDeliverer();

        await deliverer.DeliverAsync(Outcome("Blocking Detected", current: "6", threshold: "1", context: context, numCur: 6, numThr: 1));

        /* One toast (delivery-mode split shapes the SENDS only — the old loop toasted once). */
        Assert.Single(toasts);
        Assert.Equal(3, sends.Count); /* 2 per-incident + 1 overflow */
        Assert.Equal("3", sends[0].CurrentValue);   /* DescribeIncident = occurrence count */
        Assert.Equal("1", sends[1].CurrentValue);
        Assert.Equal("+1 more incident(s) this cycle", sends[2].CurrentValue);
        /* #1830: per-event sends carry per-incident numerics — occurrence counts, then the overflow
           COUNT for the "+N more" trailer whose text is unparseable (it stored 0 before). The
           threshold stays the outcome's on every send. */
        Assert.Equal(new double?[] { 3, 1, 1 }, sends.Select(s => s.NumericCurrentValue).ToArray());
        Assert.All(sends, s => Assert.Equal(1d, s.NumericThresholdValue));
        var overflowIncident = Assert.Single(sends[2].Context!.Incidents!);
        Assert.Equal("c", overflowIncident.DedupKey);
    }

    [Fact]
    public async Task Deliverer_PerServerOverride_WinsOverGlobalMode()
    {
        /* #1236 (:748, :768-771) — a per-server PerEvent override beats the global Summary. */
        App.AlertDeliveryMode = AlertNotificationMode.Summary;
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident> { new("a", new[] { "dbo.Users" }, OccurrenceCount: 1) }
        };
        var (deliverer, _, sends) = BuildDeliverer(serverOverride: AlertNotificationMode.PerEvent);

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", current: "1", threshold: "1", context: context));

        var send = Assert.Single(sends);
        Assert.Equal("1", send.CurrentValue); /* the per-incident send (occurrence count), not the summary */
        Assert.NotNull(send.Context);
        Assert.Single(send.Context!.Incidents!);
    }

    /* =====================================================================================
       10. AppAlertEngineSettings: live pass-through of every engine member
       ===================================================================================== */

    [Fact]
    public void EngineSettings_PassThroughEveryMember_Live()
    {
        var settings = new AppAlertEngineSettings();

        App.AlertsEnabled = false;
        Assert.False(settings.AlertsEnabled);
        App.AlertsEnabled = true;
        Assert.True(settings.AlertsEnabled);

        App.AlertCpuEnabled = false; Assert.False(settings.CpuEnabled);
        App.AlertBlockingEnabled = false; Assert.False(settings.BlockingEnabled);
        App.AlertDeadlockEnabled = false; Assert.False(settings.DeadlockEnabled);
        App.AlertPoisonWaitEnabled = false; Assert.False(settings.PoisonWaitEnabled);
        App.AlertLongRunningQueryEnabled = false; Assert.False(settings.LongRunningQueryEnabled);
        App.AlertTempDbSpaceEnabled = false; Assert.False(settings.TempDbSpaceEnabled);
        App.AlertLowDiskEnabled = false; Assert.False(settings.LowDiskEnabled);
        App.AlertPvsEnabled = false; Assert.False(settings.PvsEnabled);
        App.AlertLongRunningJobEnabled = false; Assert.False(settings.LongRunningJobEnabled);
        App.AlertFailedJobEnabled = false; Assert.False(settings.FailedJobEnabled);

        App.AlertCpuThreshold = 91; Assert.Equal(91, settings.CpuThresholdPercent);
        App.AlertBlockingThreshold = 7; Assert.Equal(7, settings.BlockingCountThreshold);
        App.AlertBlockingWaitSecondsThreshold = 745; Assert.Equal(745, settings.BlockingWaitSecondsThreshold);
        App.AlertDeadlockThreshold = 4; Assert.Equal(4, settings.DeadlockCountThreshold);
        App.AlertPoisonWaitThresholdMs = 999; Assert.Equal(999, settings.PoisonWaitThresholdMs);
        App.AlertLongRunningQueryThresholdMinutes = 15; Assert.Equal(15, settings.LongRunningQueryThresholdMinutes);
        App.AlertTempDbSpaceThresholdPercent = 66; Assert.Equal(66, settings.TempDbSpaceThresholdPercent);
        App.AlertLowDiskThresholdPercent = 20; Assert.Equal(20, settings.LowDiskThresholdPercent);
        App.AlertLowDiskThresholdGb = 9; Assert.Equal(9, settings.LowDiskThresholdGb);
        App.AlertPvsThresholdPercent = 55; Assert.Equal(55, settings.PvsThresholdPercent);
        App.AlertPvsFloorGb = 3; Assert.Equal(3, settings.PvsFloorGb);
        App.AlertLongRunningJobMultiplier = 5; Assert.Equal(5, settings.LongRunningJobMultiplier);
        App.AlertFailedJobLookbackMinutes = 120; Assert.Equal(120, settings.FailedJobLookbackMinutes);
        App.AlertCooldownMinutes = 30; Assert.Equal(30, settings.CooldownMinutes);

        /* The six LRQ read knobs — Lite exposes all six as real settings. */
        App.AlertLongRunningQueryMaxResults = 42; Assert.Equal(42, settings.LongRunningQueryMaxResults);
        App.AlertLongRunningQueryExcludeSpServerDiagnostics = false; Assert.False(settings.LongRunningQueryExcludeSpServerDiagnostics);
        App.AlertLongRunningQueryExcludeWaitFor = false; Assert.False(settings.LongRunningQueryExcludeWaitFor);
        App.AlertLongRunningQueryExcludeBackups = false; Assert.False(settings.LongRunningQueryExcludeBackups);
        App.AlertLongRunningQueryExcludeMiscWaits = false; Assert.False(settings.LongRunningQueryExcludeMiscWaits);
        App.AlertLongRunningQueryExcludeCdc = false; Assert.False(settings.LongRunningQueryExcludeCdc);

        App.AlertExcludedDatabases = new List<string> { "tempdb", "model" };
        Assert.Equal(new[] { "tempdb", "model" }, settings.ExcludedDatabases);

        /* The one mapped member: Lite's persisted enum → the engine's enum. */
        App.AlertCpuMode = CpuAlertMode.Total;
        Assert.Equal(EngineCpuAlertMode.TotalServer, settings.CpuAlertMode);
        App.AlertCpuMode = CpuAlertMode.SqlOnly;
        Assert.Equal(EngineCpuAlertMode.SqlProcess, settings.CpuAlertMode);
    }

    /* =====================================================================================
       11. LiteAlertStateStore: round-trip over the REAL DuckDB watermark table
       ===================================================================================== */

    [Fact]
    public async Task StateStore_RoundTripsWatermarks_ThroughTheRealDuckDbTable()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "LiteFwdTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(tempDir);
        try
        {
            var duckDb = new DuckDbInitializer(Path.Combine(tempDir, "test.duckdb"));
            await duckDb.InitializeAsync();
            IAlertStateStore store = new LiteAlertStateStore(new DuckDbAlertHistoryStore(duckDb));

            /* Nothing persisted yet → null, so the engine starts at watermark 0 like the old loop. */
            Assert.Null(await store.LoadEdgeTriggerWatermarkAsync("101", AlertEngine.BlockingWatermarkMetric));
            Assert.Null(await store.LoadFailedJobWatermarkAsync("101"));

            /* The engine's serverKey string maps to the same server_id int rows the pre-forwarding
               loop persisted — so existing watermarks seed the engine unchanged post-upgrade. */
            await store.SaveEdgeTriggerWatermarkAsync("101", AlertEngine.BlockingWatermarkMetric, 3);
            await store.SaveEdgeTriggerWatermarkAsync("101", AlertEngine.DeadlockWatermarkMetric, 2);
            await store.SaveEdgeTriggerWatermarkAsync("101", AlertEngine.BlockingWatermarkMetric, 5); /* upsert */
            var failedAt = new DateTime(2026, 7, 1, 11, 45, 0);
            await store.SaveFailedJobWatermarkAsync("101", failedAt);

            Assert.Equal(5, await store.LoadEdgeTriggerWatermarkAsync("101", AlertEngine.BlockingWatermarkMetric));
            Assert.Equal(2, await store.LoadEdgeTriggerWatermarkAsync("101", AlertEngine.DeadlockWatermarkMetric));
            Assert.Equal(failedAt, await store.LoadFailedJobWatermarkAsync("101"));
            /* Per-key isolation: another server's rows don't bleed in. */
            Assert.Null(await store.LoadEdgeTriggerWatermarkAsync("202", AlertEngine.BlockingWatermarkMetric));
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { /* best-effort */ }
        }
    }
}
