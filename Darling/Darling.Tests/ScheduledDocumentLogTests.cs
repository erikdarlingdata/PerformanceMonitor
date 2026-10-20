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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6215: a fresh install wrote four Warnings an hour to the Windows event log - the collector-cost digest and the
/// fleet-sweep rollup each logged as "ALERT TRIGGERED [Warning]", and each one's delivery failure (a Teams endpoint
/// that never answered) was logged again as a Warning on every hourly retry. A report is not an alert, and a channel
/// that fails the same way every tick must not write a new Warning every tick, while a dead channel still must not
/// go quiet. Driven through the real evaluator with a scripted deliverer, a capturing logger and a clock the test
/// moves.
/// </summary>
public sealed class ScheduledDocumentLogTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTime Start = new(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);

    private const string TeamsTimeout = "Teams: timed out after 10 seconds";

    private static AlertDelivery Failed(string error) => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: error, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery Delivered() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private sealed class ScriptedDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public AlertDelivery? Answer { get; set; }

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.FromResult(Answer);
        }
    }

    private sealed class Rig
    {
        public Rig()
        {
            Evaluator = new DarlingSelfAlertEvaluator(
                new DarlingAlertSettings(new DarlingConfig()), Deliverer,
                new DarlingSelfAlertTests.FakeHistoryStore(), _ => false,
                logger: Log, utcNow: () => Now);
        }

        public DarlingSelfAlertEvaluator Evaluator { get; }

        public ScriptedDeliverer Deliverer { get; } = new();

        public DarlingSelfAlertTests.CapturingLogger Log { get; } = new();

        public DateTime Now { get; set; } = Start;

        public IEnumerable<string> At(LogLevel level) =>
            Log.Entries.Where(e => e.Level == level).Select(e => e.Message);

        public int WarningsOrWorse => Log.Entries.Count(e => e.Level >= LogLevel.Warning);

        public Task CostDigestAsync() => Evaluator.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
    }

    private static readonly DarlingCollectorCostReader.CostMover[] OneMover =
    {
        new(ServerId: 1, ServerName: "pm-server-1", CollectorName: "query_store", LatestRuns: 2,
            LatestWorstMs: 17_548, LatestMsPerRun: 17_548.0, BaselineMsPerRun: 6_477.0,
            BaselineP95MsPerRun: 17_935.0, BaselineWorstDayMsPerRun: 17_935.0, BaselineDays: 13,
            EligiblePairs: 1),
    };

    private static readonly DarlingCollectorCostReader.CollectorCostSummaryRow[] OneCensusRow =
    {
        new("query_stats", 43_891, 61_724_459, 88_561, TotalStorageMs: 0, TotalRows: 0, ServerCount: 43),
    };

    /// <summary>The report is logged as a report, at Information. The same fixture fired as the real paging
    /// alert beside it still logs "ALERT TRIGGERED [Warning]", so this is not a rewrite that silenced the
    /// line for everything.</summary>
    [Fact]
    public async Task ACostDigest_IsLoggedAsAReportAtInformation_AndARealAlertStillWarns()
    {
        var rig = new Rig();

        await rig.CostDigestAsync();

        Assert.Equal(0, rig.WarningsOrWorse);
        var line = Assert.Single(rig.At(LogLevel.Information));
        Assert.StartsWith("SCHEDULED REPORT Collector Cost Digest on ", line, StringComparison.Ordinal);
        Assert.DoesNotContain("ALERT TRIGGERED", line, StringComparison.Ordinal);
        Assert.Single(rig.Deliverer.Outcomes);

        var regression = new DarlingCollectorCostReader.CostRegression(
            1, "pm-server-1", "query_store", LatestMs: 20_000_000, BaselineMs: 5_000_000,
            LatestMetricTime: new DateTime(2026, 9, 15, 11, 0, 0, DateTimeKind.Unspecified),
            LatestRuns: 1000, LatestMsPerRun: 20_000.0, BaselineMsPerRun: 5_000.0, BaselineP95MsPerRun: 5_000.0);
        await rig.Evaluator.ApplyCostRegressionsAsync(new[] { regression }, Ct);

        Assert.Contains(rig.At(LogLevel.Warning),
            m => m.StartsWith("ALERT TRIGGERED [Warning] Collector Cost Regression", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AFleetSweepRollup_IsLoggedAsAReportAtInformation()
    {
        var rig = new Rig();
        var run = new FleetSweepRun(
            1, Start.AddHours(-2), Start.AddHours(-3), Start.AddHours(-2), null, true, 3, 3, true, "{\"alive\":true}",
            "{\"changes\":{\"band_transitions\":[{\"server\":\"pm-server-1\",\"from\":\"Healthy\",\"to\":\"Critical\"}]},"
            + "\"watch\":{\"opened\":[],\"closed\":[]},\"fleet\":{\"bands\":{\"Critical\":1}}}");

        await rig.Evaluator.ApplyFleetSweepRollupAsync(
            new[] { run }, Array.Empty<FleetSweepLedgerSpanEntry>(), new Dictionary<int, string> { [1] = "pm-server-1" },
            Start - DarlingSelfAlertEvaluator.FleetSweepRollupInterval, Start, Ct);

        Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(0, rig.WarningsOrWorse);
        var line = Assert.Single(rig.At(LogLevel.Information));
        Assert.StartsWith("SCHEDULED REPORT Fleet Sweep Rollup on ", line, StringComparison.Ordinal);
    }

    /// <summary>The measured defect: the same failure every hourly tick. One Warning on the first, then
    /// Information with the running count, then one Warning reminder a day after the last Warning - and the
    /// retry itself (a delivery attempt every tick) is unchanged.</summary>
    [Fact]
    public async Task ADigestFailingTheSameWayEveryTick_WarnsOnceThenCountsAtInformation_AndRemindsOnceADay()
    {
        var rig = new Rig();
        rig.Deliverer.Answer = Failed(TeamsTimeout);

        for (var tick = 0; tick < 30; tick++)
        {
            await rig.CostDigestAsync();
            rig.Now = rig.Now.AddHours(1);
        }

        Assert.Equal(30, rig.Deliverer.Outcomes.Count);

        var warnings = rig.At(LogLevel.Warning).ToList();
        Assert.Equal(2, warnings.Count);
        Assert.Contains(TeamsTimeout, warnings[0], StringComparison.Ordinal);
        Assert.Contains("collector-cost digest delivery failed", warnings[0], StringComparison.Ordinal);
        Assert.Contains("still failing", warnings[1], StringComparison.Ordinal);
        Assert.Contains("25 failed attempts", warnings[1], StringComparison.Ordinal);

        var counted = rig.At(LogLevel.Information).Where(m => m.Contains("failed again", StringComparison.Ordinal)).ToList();
        Assert.Equal(28, counted.Count);
        Assert.Contains("2 failed attempts", counted[0], StringComparison.Ordinal);
        Assert.Contains("24 failed attempts", counted[22], StringComparison.Ordinal);
    }

    /// <summary>A different failure is news: it warns at once, not after the day.</summary>
    [Fact]
    public async Task AChangedError_WarnsAgainAtOnce()
    {
        var rig = new Rig();
        rig.Deliverer.Answer = Failed(TeamsTimeout);
        await rig.CostDigestAsync();
        rig.Now = rig.Now.AddHours(1);
        await rig.CostDigestAsync();
        Assert.Single(rig.At(LogLevel.Warning));

        rig.Deliverer.Answer = Failed("Teams: 401 Unauthorized");
        rig.Now = rig.Now.AddHours(1);
        await rig.CostDigestAsync();

        var warnings = rig.At(LogLevel.Warning).ToList();
        Assert.Equal(2, warnings.Count);
        Assert.Contains("401 Unauthorized", warnings[1], StringComparison.Ordinal);
        Assert.Contains(rig.At(LogLevel.Information), m => m.Contains("failed again", StringComparison.Ordinal));
    }

    /// <summary>Recovery is one Information line, and the next failure starts over as a first failure.</summary>
    [Fact]
    public async Task ARecoveredDigest_LogsOneInformationLine_AndTheNextFailureWarnsAsAFirstFailure()
    {
        var rig = new Rig();
        rig.Deliverer.Answer = Failed(TeamsTimeout);
        for (var i = 0; i < 3; i++)
        {
            await rig.CostDigestAsync();
            rig.Now = rig.Now.AddHours(1);
        }

        rig.Deliverer.Answer = Delivered();
        await rig.CostDigestAsync();

        var recovered = Assert.Single(rig.At(LogLevel.Information), m => m.Contains("recovered", StringComparison.Ordinal));
        Assert.Contains("3 failed attempts", recovered, StringComparison.Ordinal);
        Assert.Single(rig.At(LogLevel.Warning));

        rig.Now = rig.Now.Add(DarlingSelfAlertEvaluator.CollectorCostDigestInterval).AddMinutes(1);
        rig.Deliverer.Answer = Failed(TeamsTimeout);
        await rig.CostDigestAsync();

        Assert.Equal(2, rig.At(LogLevel.Warning).Count());
    }

    /// <summary>Two documents failing on one channel keep separate runs: one's recovery says nothing about the
    /// other, and each warns once on its own first failure.</summary>
    [Fact]
    public async Task EachDocumentKeepsItsOwnRun()
    {
        var rig = new Rig();
        rig.Deliverer.Answer = Failed(TeamsTimeout);
        var run = new FleetSweepRun(
            1, Start.AddHours(-2), Start.AddHours(-3), Start.AddHours(-2), null, true, 3, 3, true, "{\"alive\":true}",
            "{\"changes\":{\"band_transitions\":[{\"server\":\"pm-server-1\",\"from\":\"Healthy\",\"to\":\"Critical\"}]},"
            + "\"watch\":{\"opened\":[],\"closed\":[]},\"fleet\":{\"bands\":{\"Critical\":1}}}");

        for (var tick = 0; tick < 3; tick++)
        {
            await rig.CostDigestAsync();
            await rig.Evaluator.ApplyFleetSweepRollupAsync(
                new[] { run }, Array.Empty<FleetSweepLedgerSpanEntry>(), new Dictionary<int, string> { [1] = "pm-server-1" },
                rig.Now - DarlingSelfAlertEvaluator.FleetSweepRollupInterval, rig.Now, Ct);
            rig.Now = rig.Now.AddHours(1);
        }

        var warnings = rig.At(LogLevel.Warning).ToList();
        Assert.Equal(2, warnings.Count);
        Assert.Contains(warnings, m => m.StartsWith("collector-cost digest delivery failed", StringComparison.Ordinal));
        Assert.Contains(warnings, m => m.StartsWith("fleet-sweep rollup delivery failed", StringComparison.Ordinal));
    }
}
