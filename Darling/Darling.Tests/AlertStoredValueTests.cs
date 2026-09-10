/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins WHAT NUMBER each self-alert actually stores (#1881) — the round trip from the producer's fire
/// site, through the <c>AlertOutcome</c>, to the double the history stores write into their NOT NULL
/// <c>current_value</c>/<c>threshold_value</c> columns.
///
/// <para><b>Why this suite exists at all.</b> Every one of these metrics was already covered by tests
/// that asserted its metric name, its severity, its edge behavior and its message text — and every one
/// of them was storing a semantically-wrong number the whole time, because nothing ever asserted the
/// number. "Store Runtime Upgrade" recorded the PostgreSQL major version (18) as the alert's current
/// value; "Collection Stopped" recorded a run count on one branch and elapsed minutes on the other;
/// "AG Sync Fell Behind" recorded whatever digit its prose reached first, which on an AG named
/// "Sales2024" was part of the name. All of it green. The gap was that the value under test lives on
/// the far side of a seam nobody crossed in a test: the producers were checked, the stores were checked,
/// and the number is decided BETWEEN them.</para>
///
/// <para>So each case here drives the REAL evaluator to a real <c>AlertOutcome</c> and then resolves it
/// exactly as a history store does, through the one shared
/// <see cref="AlertValueParser.ResolveStoredValue"/> both <c>PgAlertHistoryStore</c> and
/// <c>DuckDbAlertHistoryStore</c> now call. No live store is needed to pin the value, because the store
/// contributes nothing to the decision beyond that call — what a live store proves is that the column
/// accepts a double, which its own suite already covers.</para>
///
/// <para>Where a stored 0 is the answer, the test also asserts the READ side agrees
/// (<see cref="AlertMetricClassifier.IsStateOnly"/>), because a 0 that the grid renders as "0.00" is
/// not an improvement on 18 — the pair is the fix, and either half alone is a regression waiting to
/// happen.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class AlertStoredValueTests
{
    private const int ServerId = 515151;
    private const string Name = "STORED-VALUE-SRV";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ---------------- harness ---------------- */

    private sealed class Settings : IAlertEngineSettings
    {
        public bool AlertsEnabled { get; set; } = true;
        public bool CpuEnabled { get; set; }
        public bool BlockingEnabled { get; set; } = true;
        public bool DeadlockEnabled { get; set; } = true;
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
        public IReadOnlyList<string> ExcludedDatabases => Array.Empty<string>();
        public CpuAlertMode CpuAlertMode { get; set; } = CpuAlertMode.TotalServer;
    }

    private sealed class Recorder : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }
    }

    private sealed class NullHistory : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
    }

    private sealed class Rig
    {
        public Settings Config { get; } = new();
        public Recorder Deliverer { get; } = new();
        public NullHistory History { get; } = new();
        public DateTime Now { get; set; } = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        public int AgLagAlertSeconds { get; set; } = 300;

        public DarlingSelfAlertEvaluator Build() => new(
            Config, Deliverer, History, _ => false,
            logger: null, utcNow: () => Now,
            agLagAlertSeconds: () => AgLagAlertSeconds);
    }

    /// <summary>
    /// What a history store writes for the single outcome the rig recorded. This is the store's WHOLE
    /// contribution to the value — both <c>PgAlertHistoryStore</c> and <c>DuckDbAlertHistoryStore</c>
    /// resolve their two doubles through exactly this call and then bind them as parameters.
    /// </summary>
    private static (double Current, double Threshold) Stored(Rig rig)
    {
        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        return (AlertValueParser.ResolveStoredValue(outcome.NumericCurrentValue, outcome.CurrentValue),
                AlertValueParser.ResolveStoredValue(outcome.NumericThresholdValue, outcome.ThresholdValue));
    }

    /// <summary>
    /// Asserts the full state-only contract for the rig's single outcome, all the way to the pixels:
    /// the stored pair is the 0 sentinel, the metric is classified state-only, and BOTH history-grid
    /// columns therefore render the em dash rather than "0.00".
    ///
    /// <para>The last step is the one that makes this a real assertion. The stored 0 and the
    /// classification are two halves of one contract, and either alone passes happily while the row on
    /// screen still shows a number the alert never measured — which is exactly the state "Capture Down"
    /// and "Agent Not Running" shipped in. Asserting the rendered string closes it, and is possible
    /// because both apps' grids now go through the one shared
    /// <see cref="AlertMetricClassifier.FormatHistoryValue"/> instead of a copy each.</para>
    /// </summary>
    private static void AssertStateOnly(Rig rig, string expectedMetric)
    {
        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(expectedMetric, outcome.MetricName);

        var (current, threshold) = Stored(rig);
        Assert.Equal(0.0, current);
        Assert.Equal(0.0, threshold);
        Assert.True(
            AlertMetricClassifier.IsStateOnly(expectedMetric),
            $"'{expectedMetric}' stores the 0 sentinel, so it must be classified state-only or the grid " +
            "renders 0.00 for a value the alert never had.");

        Assert.Equal(AlertMetricClassifier.StateOnlyDisplay,
                     AlertMetricClassifier.FormatHistoryValue(expectedMetric, current));
        Assert.Equal(AlertMetricClassifier.StateOnlyDisplay,
                     AlertMetricClassifier.FormatHistoryValue(expectedMetric, threshold));
    }

    /* ---------------- Store Runtime Upgrade: the PostgreSQL major version ---------------- */

    private static DarlingSelfAlertEvaluator.StoreUpgradeReport UpgradeReport(
        bool succeeded = true, int fromMajor = 17, int toMajor = 18, string? failedStep = null,
        string? failureMessage = null) =>
        new(succeeded, fromMajor, toMajor, FromTimescale: null, ToTimescale: null,
            FailedStep: failedStep, FailureMessage: failureMessage, WithoutRollbackCopy: false);

    [Fact]
    public async Task StoreRuntimeUpgrade_Succeeded_StoresTheSentinel_NotThePostgresMajor()
    {
        var rig = new Rig();
        await rig.Build().EvaluateStoreUpgradeAsync(UpgradeReport(), Ct);

        /* The display text is unchanged and still names both versions — that is where a version belongs. */
        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal("PostgreSQL 18", outcome.CurrentValue);
        Assert.Equal("PostgreSQL 17", outcome.ThresholdValue);

        /* What is NOT allowed is for those to reach the numeric column. Before #1881 this row stored
           current_value = 18 and threshold_value = 17: a column of measurements in which two of the
           measurements were release numbers. */
        AssertStateOnly(rig, DarlingSelfAlertEvaluator.StoreUpgradeMetric);
    }

    [Fact]
    public async Task StoreRuntimeUpgrade_Failed_StoresTheSentinel_NotThePostgresMajor()
    {
        var rig = new Rig();
        await rig.Build().EvaluateStoreUpgradeAsync(
            UpgradeReport(succeeded: false, failedStep: "pg_upgrade", failureMessage: "boom"), Ct);

        AssertStateOnly(rig, DarlingSelfAlertEvaluator.StoreUpgradeMetric);
    }

    [Fact]
    public async Task StoreRuntimeUpgrade_DegradedSuccess_StoresTheSentinel()
    {
        /* The post-commit-bookkeeping-failed flavor renders a DIFFERENT current-value string
           ("PostgreSQL 18 (cleanup incomplete)"), so it is a separate parse and a separate pin. */
        var rig = new Rig();
        await rig.Build().EvaluateStoreUpgradeAsync(
            UpgradeReport(failureMessage: "could not write the retention marker"), Ct);

        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal("PostgreSQL 18 (cleanup incomplete)", outcome.CurrentValue);
        AssertStateOnly(rig, DarlingSelfAlertEvaluator.StoreUpgradeMetric);
    }

    /* ---------------- Collection Stopped: two branches, two units ---------------- */

    [Fact]
    public async Task CollectionStopped_ConsecutiveFailureBranch_StoresTheSentinel_NotTheRunCount()
    {
        var rig = new Rig();
        var evaluator = rig.Build();

        /* The exact reason string the consecutive-failure rule produces. */
        Assert.True(DarlingSelfAlertEvaluator.IsCollectionStopped(
            lastSuccessUtc: rig.Now.AddMinutes(-2),
            recentRunCount: DarlingSelfAlertEvaluator.ConsecutiveFailureThreshold,
            recentSuccessCount: 0, nowUtc: rig.Now, out var reason));
        Assert.StartsWith("The last 10 collector runs all failed", reason, StringComparison.Ordinal);

        await evaluator.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, reason, Ct);

        /* Stored 10 before #1881 — and 10 is not data even read charitably: the store read is LIMITed to
           ConsecutiveFailureThreshold and the rule requires the whole window to have failed, so this
           branch's count is ALWAYS exactly the threshold. It is the threshold restated as a measurement. */
        AssertStateOnly(rig, "Collection Stopped");
    }

    [Fact]
    public async Task CollectionStopped_StalenessBranch_StoresTheSentinel_NotTheMinutes()
    {
        var rig = new Rig();
        var evaluator = rig.Build();

        Assert.True(DarlingSelfAlertEvaluator.IsCollectionStopped(
            lastSuccessUtc: rig.Now.AddMinutes(-47), recentRunCount: 3, recentSuccessCount: 1,
            nowUtc: rig.Now, out var reason));
        Assert.StartsWith("No successful collection in 47 minutes", reason, StringComparison.Ordinal);

        await evaluator.ApplyCollectionStoppedAsync(ServerId, Name, stopped: true, reason, Ct);

        /* Stored 47 before #1881 — genuinely minutes, and genuinely a measurement, but the SAME metric
           name stores a run count on the branch above. One column, two units, indistinguishable to
           whoever reads the grid. That is the defect, and it is not fixed by keeping the better half. */
        AssertStateOnly(rig, "Collection Stopped");
    }

    /* ---------------- Compression Job Stuck: a duration on one branch, none on the other ---------------- */

    [Fact]
    public async Task CompressionJobStuck_HungRunBranch_StoresTheSentinel_NotTheMinutes()
    {
        var rig = new Rig();

        /* 8 hours into a run, against the 6-hour floor StuckRunningBound applies to a 30-minute schedule. */
        Assert.True(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: false, jobStatus: "Running",
            lastRunStartedAtUtc: rig.Now.AddMinutes(-480), scheduleInterval: TimeSpan.FromMinutes(30),
            nowUtc: rig.Now, out var reason));
        Assert.StartsWith("stuck in the Running state for 480 minutes", reason, StringComparison.Ordinal);

        await rig.Build().ApplyCompressionJobsStuckAsync(
            new[] { new StuckCompressionJob(9001L, "wait_stats", reason) }, _ => Task.FromResult(true), Ct);

        /* Stored 480 before #1881. The sibling branch below has no duration at all. */
        Assert.Equal(480.0, AlertValueParser.ParseOrDefault(reason));
        AssertStateOnly(rig, "Compression Job Stuck");
    }

    [Fact]
    public async Task CompressionJobStuck_NeverRunsAgainBranch_StoresTheSentinel()
    {
        var rig = new Rig();

        Assert.True(TimescaleSupport.IsCompressionJobStuck(
            nextStartIsNegativeInfinity: true, jobStatus: "Scheduled", lastRunStartedAtUtc: null,
            scheduleInterval: TimeSpan.FromMinutes(30), nowUtc: rig.Now, out var reason));

        await rig.Build().ApplyCompressionJobsStuckAsync(
            new[] { new StuckCompressionJob(9001L, "wait_stats", reason) }, _ => Task.FromResult(true), Ct);

        /* This branch always stored 0 — its reason carries no digit — but the metric was NOT classified
           state-only, so the grid rendered "0.00" for it. Both halves had to move. */
        AssertStateOnly(rig, "Compression Job Stuck");
    }

    /* ---------------- the digit-in-a-name family ---------------- */

    [Fact]
    public async Task AgSyncFellBehind_StoresTheSentinel_NotTheLagSecondsFromItsProse()
    {
        var rig = new Rig { AgLagAlertSeconds = 300 };

        await rig.Build().ApplyAgDatabaseHealthAsync(
            ServerId, Name,
            new[] { new AgDatabaseReading("AG1", "Sales", "NODE2", SecondaryLagSeconds: 900,
                                          RedoQueueSizeKb: 0, IsSuspended: false, SuspendReasonDesc: null) },
            Ct);

        /* #1846 already classified this metric state-only, correctly — its value is JudgeSync's prose,
           which breaches on lag seconds OR redo-queue kilobytes, so the number meant different units on
           different rows. But the write side never cooperated: the parser found a digit in the sentence
           and stored it anyway, so the read side's 0-gate never engaged. This is the two halves finally
           agreeing. */
        AssertStateOnly(rig, "AG Sync Fell Behind");
    }

    [Fact]
    public async Task AgSyncFellBehind_DigitBearingAgName_StoresTheSentinel_NotPartOfTheName()
    {
        /* The cross-cutting case #1881 names: an AG an operator called "Sales2024". The reason string
           leads with the AG name, so the parser's first digit came out of the NAME. */
        var rig = new Rig { AgLagAlertSeconds = 300 };

        await rig.Build().ApplyAgDatabaseHealthAsync(
            ServerId, Name,
            new[] { new AgDatabaseReading("Sales2024", "Sales", "SQL01", SecondaryLagSeconds: 900,
                                          RedoQueueSizeKb: 0, IsSuspended: false, SuspendReasonDesc: null) },
            Ct);

        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Contains("Sales2024", outcome.CurrentValue, StringComparison.Ordinal);

        /* Before the fix this row stored 2024. */
        Assert.Equal(2024.0, AlertValueParser.ParseOrDefault(outcome.CurrentValue));
        AssertStateOnly(rig, "AG Sync Fell Behind");
    }

    [Fact]
    public async Task ServerUnreachable_StoresTheSentinel_NotTheDriverErrorCode()
    {
        var rig = new Rig();
        var evaluator = rig.Build();

        await evaluator.ApplyConnectionOutcomeAsync(ServerId, Name, online: true, error: null, Ct);
        rig.Deliverer.Outcomes.Clear();

        /* A real connection failure message. Its numbers are a TCP port and a driver error code. */
        await evaluator.ApplyConnectionOutcomeAsync(
            ServerId, Name, online: false,
            error: "A network-related error occurred (provider: TCP Provider, error: 10060) on port 1433", Ct);

        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(10060.0, AlertValueParser.ParseOrDefault(outcome.CurrentValue));
        AssertStateOnly(rig, "Server Unreachable");
    }

    /* ---------------- the two metrics #1846 classified nothing for ---------------- */

    [Fact]
    public async Task CaptureDown_StoresTheSentinel_AndIsClassifiedStateOnly()
    {
        var rig = new Rig();
        await rig.Build().ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking", "Deadlock" }, Ct);

        /* "Blocking and Deadlock" against "session running" — a state on both sides, stored as 0 since
           the day it shipped, and rendered "0.00" the whole time because #1846's list stopped at the
           AG/connection family. */
        AssertStateOnly(rig, "Capture Down");
    }

    [Fact]
    public async Task AgentNotRunning_StoresTheSentinel_AndIsClassifiedStateOnly()
    {
        var rig = new Rig();
        await rig.Build().ApplyAgentNotRunningAsync(
            ServerId, Name, agentRunningFresh: false, agentEverSeenRunning: true, Ct);

        /* Literally "Stopped" against "Running". */
        AssertStateOnly(rig, "Agent Not Running");
    }

    /* ---------------- Store Disk Pressure: the one that keeps its number ---------------- */

    [Fact]
    public async Task StoreDiskPressure_StoresTheRealPercentFree_AndIsNeverStateOnly()
    {
        var rig = new Rig();

        /* 5.55% free. The reason text rounds to one decimal ("5.6% free"), so the old parse and the real
           measurement DIFFER here — which is what makes this case prove the producer now supplies the
           value rather than the store re-deriving it from display prose. */
        await rig.Build().ApplyDiskPressureAsync(
            freeBytes: 5_550, totalBytes: 100_000, storeSizeBytes: 40_000, Ct);

        var outcome = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal("Store Disk Pressure", outcome.MetricName);
        Assert.Contains("5.6% free", outcome.CurrentValue, StringComparison.Ordinal);
        Assert.Equal(5.6, AlertValueParser.ParseOrDefault(outcome.CurrentValue), precision: 6);

        var (current, threshold) = Stored(rig);
        Assert.Equal(5.55, current, precision: 6);
        Assert.Equal(DarlingSelfAlertEvaluator.DiskFreeWarnPercent, threshold);

        /* THE GUARD #1881 exists to protect. Percent-free is a genuine measurement and a genuine 0 means
           a FULL volume — the single reading an operator most needs to see as a number, and the one the
           em dash would swallow. If a future change adds this metric to the state-only list, this fails. */
        Assert.False(AlertMetricClassifier.IsStateOnly("Store Disk Pressure"));
        Assert.Equal("5.55", AlertMetricClassifier.FormatHistoryValue("Store Disk Pressure", current));
    }

    [Fact]
    public async Task StoreDiskPressure_FullVolume_StoresZero_AndStillRendersAsANumber()
    {
        /* The 0 that must NOT become a dash. It reaches the grid through the same stored-0 path every
           state-only metric uses, and only the classifier keeps them apart — so this pins the classifier
           answer at the exact input where getting it wrong is most expensive. */
        var rig = new Rig();
        await rig.Build().ApplyDiskPressureAsync(
            freeBytes: 0, totalBytes: 100_000, storeSizeBytes: 40_000, Ct);

        var (current, _) = Stored(rig);
        Assert.Equal(0.0, current);
        Assert.False(
            AlertMetricClassifier.IsStateOnly("Store Disk Pressure"),
            "A full store volume stores 0% free. Classifying this metric state-only would render that " +
            "as an em dash and hide a fleet-stopping condition behind 'no value'.");

        /* The rendering itself, which is the thing an operator sees at 3am. */
        Assert.Equal("0.00", AlertMetricClassifier.FormatHistoryValue("Store Disk Pressure", current));
        Assert.NotEqual(AlertMetricClassifier.StateOnlyDisplay,
                        AlertMetricClassifier.FormatHistoryValue("Store Disk Pressure", current));
    }

    /* ---------------- resolutions ---------------- */

    [Fact]
    public void ResolutionRows_StoreTheSentinel_AndAreClassifiedStateOnly()
    {
        /* BuildResolutionRecord hardcodes "resolved" / "" with both numerics null, so its stored pair is
           the parser's fallback rather than an explicit sentinel. That is left alone deliberately: the
           text is a compile-time constant with no digit in it, so the fallback is not luck there the way
           it was for every metric above, where the text is composed from server data. Pinned so the
           reasoning stays true if the constant ever changes. */
        var record = DarlingSelfAlertEvaluator.BuildResolutionRecord(
            new AlertResolution("1", Name, "Collection Stopped", "Collection Resumed", "back to normal"));

        Assert.Equal(0.0, AlertValueParser.ResolveStoredValue(record.NumericCurrentValue, record.CurrentValueText));
        Assert.Equal(0.0, AlertValueParser.ResolveStoredValue(record.NumericThresholdValue, record.ThresholdValueText));
        Assert.True(AlertMetricClassifier.IsStateOnly(record.MetricName));
    }

    /* ---------------- the invariant, stated once ---------------- */

    [Fact]
    public void EverySelfAlertMetricIsEitherStateOnlyOrStoreDiskPressure()
    {
        /* The line #1881 drew, asserted as a line rather than as eleven separate facts: among the metrics
           this evaluator fires, "Store Disk Pressure" is the ONLY one whose current value is a
           measurement. Its threshold text is a real bound ("10% free"); every sibling's is an English
           phrase ("collecting", "Running", "running on schedule", "session running", "CONNECTED",
           "SYNCHRONIZING", "caught up", "Online"). A new self-alert that is not a measurement and does
           not join the classifier fails here rather than silently storing whatever its prose leads with. */
        var selfAlertMetrics = new[]
        {
            "Collection Stopped", "Capture Down", "Agent Not Running", "Server Unreachable",
            "AG Failover", "AG Replica Disconnected", "AG Sync Fell Behind", "AG Database Suspended",
            "Compression Job Stuck", DarlingSelfAlertEvaluator.StoreUpgradeMetric,
        };

        var notClassified = selfAlertMetrics.Where(m => !AlertMetricClassifier.IsStateOnly(m)).ToList();
        Assert.True(
            notClassified.Count == 0,
            "These self-alert metrics store the 0 sentinel but are not classified state-only, so their " +
            "history rows render '0.00' for a value the alert never measured: " +
            string.Join(", ", notClassified));

        Assert.False(AlertMetricClassifier.IsStateOnly("Store Disk Pressure"));
    }
}
