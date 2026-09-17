/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3443: <c>Collector Cost Regression</c> keeps the paging channel only for the shape that cannot be one
/// server's slow day, and everything else is reported by <c>Collector Cost Digest</c> once a day.
///
/// <para><b>The vacuity hazard this suite is built around.</b> A demotion is trivially satisfied by breaking
/// the alert: any pin showing "the unactionable case no longer pages" also passes when nothing pages at all,
/// and any pin showing "the digest lists everything" also passes when the digest lists garbage. So every
/// silence pin here has a firing twin, the partition is asserted as a count-preserving invariant rather than
/// as two independent membership checks, and the routing's discriminating fixture is a PERMUTATION — the same
/// number of regressed pairs carrying the same figures, differing only in how they are spread across
/// collectors and servers — so an exclusion is attributable to fan-out and to nothing else.</para>
///
/// <para><b>And the hazard specific to a digest.</b> A digest is a document somebody reads, so a pin
/// asserting its fields one at a time cannot see a sentence that contradicts its own numbers.
/// <see cref="TheRenderedDigestsFiguresReconcileWithEachOther"/> parses the SHIPPED rendering and asserts the
/// identities the printed figures must satisfy TOGETHER — the ratio against the two costs it is between, the
/// seconds-per-day against the per-run delta and the run count it multiplies, the header's "N of M" against
/// the number of lines actually rendered, and the ordering against the quantity the header claims it ranked
/// by.</para>
/// </summary>
public class CollectorCostDigestTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ---------------- fixtures ---------------- */

    /// <summary>
    /// The ONE construction site for a <see cref="DarlingCollectorCostReader.CostRegression"/> in this file.
    /// #3441 adds a member to that record's positional constructor, so keeping every fixture behind one
    /// factory makes that merge a one-line change here rather than one per test.
    /// </summary>
    private static DarlingCollectorCostReader.CostRegression Regression(
        int serverId, string collector, double latestMsPerRun = 200.0, double baselineMsPerRun = 50.0,
        long latestRuns = 1000) =>
        new(serverId, "pm-server-" + serverId.ToString(CultureInfo.InvariantCulture), collector,
            LatestMs: (long)(latestMsPerRun * latestRuns), BaselineMs: baselineMsPerRun * latestRuns,
            LatestMetricTime: new DateTime(2026, 7, 1, 11, 0, 0, DateTimeKind.Unspecified),
            LatestRuns: latestRuns, LatestMsPerRun: latestMsPerRun, BaselineMsPerRun: baselineMsPerRun,
            /* The one-line change the remark above reserved for #3441's merge: p95 pinned to the mean
               makes ThresholdMsPerRun resolve to the pre-#3441 bound exactly, so every fixture keeps
               asking the ROUTING question it was written to ask rather than gaining a dispersion one. */
            BaselineP95MsPerRun: baselineMsPerRun);

    private static DarlingCollectorCostReader.CollectorCostSummaryRow Census(
        string collector, int serverCount, long totalSqlMs = 1_000_000, long runCount = 1000,
        long maxSqlMs = 9_000) =>
        new(collector, runCount, totalSqlMs, maxSqlMs, TotalStorageMs: 0, TotalRows: 0, ServerCount: serverCount);

    private static DarlingCollectorCostReader.CostMover Mover(
        string collector, string server, double latestMsPerRun, double baselineMsPerRun, long latestRuns,
        double p95 = 0.0, double worstDay = 0.0, long worstRunMs = 0, int baselineDays = 13,
        long eligiblePairs = 1) =>
        new(ServerId: 1, ServerName: server, CollectorName: collector, LatestRuns: latestRuns,
            LatestWorstMs: worstRunMs, LatestMsPerRun: latestMsPerRun, BaselineMsPerRun: baselineMsPerRun,
            BaselineP95MsPerRun: p95, BaselineWorstDayMsPerRun: worstDay, BaselineDays: baselineDays,
            EligiblePairs: eligiblePairs);

    /* ---------------- the routing ---------------- */

    /// <summary>
    /// The partition invariant, and the reason the routing returns ONE value: every row the predicate
    /// selected is in exactly one of the two lists. A demotion that silently DROPPED rows would pass every
    /// "no longer pages" assertion in this file and would be the worst possible outcome — a finding that
    /// reaches neither channel.
    /// </summary>
    [Fact]
    public void EveryRegressionThePredicateSelected_IsInExactlyOneList()
    {
        var regressions = new[]
        {
            Regression(1, "query_store"), Regression(2, "query_store"), Regression(3, "query_store"),
            Regression(1, "latch_stats"), Regression(9, "database_size_stats"),
        };

        var routing = DarlingSelfAlertEvaluator.RouteCostRegressions(
            regressions, new[] { Census("query_store", 4), Census("latch_stats", 43), Census("database_size_stats", 43) });

        Assert.Equal(regressions.Length, routing.Paging.Count + routing.ReportOnly.Count);
        Assert.Equal(
            regressions.OrderBy(r => r.ServerId).ThenBy(r => r.CollectorName, StringComparer.Ordinal).ToList(),
            routing.Paging.Concat(routing.ReportOnly)
                .OrderBy(r => r.ServerId).ThenBy(r => r.CollectorName, StringComparer.Ordinal).ToList());
    }

    /// <summary>
    /// The demotion, and deliberately with a LARGE figure: 11,000 ms/run against a 50 ms baseline adding
    /// 10,950 s a day is not a small regression, and it still does not page, because size was never the
    /// question. #3316 already put a materiality floor on size and #3440 put a bound on dispersion; what is
    /// left is whether one server's cost movement is something to interrupt somebody for.
    /// </summary>
    [Fact]
    public void ASingleServersRegression_IsReportOnly_HoweverLargeItIs()
    {
        var routing = DarlingSelfAlertEvaluator.RouteCostRegressions(
            new[] { Regression(4, "query_store", latestMsPerRun: 11_000.0, baselineMsPerRun: 50.0) },
            new[] { Census("query_store", 43) });

        Assert.Empty(routing.Paging);
        var only = Assert.Single(routing.ReportOnly);
        Assert.Equal("query_store", only.CollectorName);
    }

    /// <summary>
    /// The firing twin of the pin above, and the one that stops this whole change being "disable the alert".
    /// The collector's code and the monitoring store are shared across the fleet, so a rise on most of the
    /// servers it runs on is a property of the collector — a build or a store change — and that still pages,
    /// per (server, collector), carrying every figure the predicate computed.
    /// </summary>
    [Fact]
    public void AFleetWideRegression_StillPages_OnEveryAffectedPair()
    {
        var regressions = Enumerable.Range(1, 30).Select(i => Regression(i, "query_store")).ToArray();

        var routing = DarlingSelfAlertEvaluator.RouteCostRegressions(
            regressions, new[] { Census("query_store", 43) });

        Assert.Equal(30, routing.Paging.Count);
        Assert.Empty(routing.ReportOnly);
    }

    /// <summary>
    /// The discriminating fixture: a COUNT-PRESERVING PERMUTATION. Both arrangements carry six regressed
    /// pairs, the same per-run costs, the same run counts and therefore the same added cost per day, over a
    /// fleet whose collectors all ran on the same number of servers. They differ only in the shape of the
    /// spread — one collector across six servers, or six collectors on one server each. The first pages
    /// entirely and the second not at all, so the exclusion is attributable to fan-out and to nothing about
    /// size, ratio, volume or count.
    /// </summary>
    [Fact]
    public void TheSameSixRegressions_PageWhenConcentratedAndDoNotWhenSpread()
    {
        var census = new[]
        {
            Census("c0", 9), Census("c1", 9), Census("c2", 9),
            Census("c3", 9), Census("c4", 9), Census("c5", 9),
        };

        var concentrated = Enumerable.Range(1, 6).Select(i => Regression(i, "c0")).ToArray();
        var spread = Enumerable.Range(0, 6)
            .Select(i => Regression(1, "c" + i.ToString(CultureInfo.InvariantCulture))).ToArray();

        /* Identical populations by every measure the old delivery consumed. */
        Assert.Equal(concentrated.Length, spread.Length);
        Assert.Equal(
            concentrated.Sum(r => r.AddedMsPerDay), spread.Sum(r => r.AddedMsPerDay), 6);

        var concentratedRouting = DarlingSelfAlertEvaluator.RouteCostRegressions(concentrated, census);
        var spreadRouting = DarlingSelfAlertEvaluator.RouteCostRegressions(spread, census);

        Assert.Equal(6, concentratedRouting.Paging.Count);
        Assert.Empty(concentratedRouting.ReportOnly);

        Assert.Empty(spreadRouting.Paging);
        Assert.Equal(6, spreadRouting.ReportOnly.Count);
    }

    /// <summary>
    /// A majority of one is one, which is exactly the shape being demoted — so the rule carries a floor of
    /// two servers as well as the majority comparison. Without the floor a collector that runs on a single
    /// server would page on that server alone and the demotion would have a hole precisely where the
    /// measured firings live.
    /// </summary>
    [Fact]
    public void ACollectorThatRanOnOneServer_DoesNotPageOnThatServerAlone()
    {
        var routing = DarlingSelfAlertEvaluator.RouteCostRegressions(
            new[] { Regression(1, "index_object_stats") },
            new[] { Census("index_object_stats", serverCount: 1) });

        Assert.Empty(routing.Paging);
        Assert.Single(routing.ReportOnly);
    }

    /// <summary>The boundary is strictly MORE than half, both sides of it asserted on the same denominator so
    /// the pin cannot pass by moving the fleet instead of the fan-out.</summary>
    [Theory]
    [InlineData(2, 4, false)]   /* exactly half */
    [InlineData(3, 4, true)]    /* more than half */
    [InlineData(21, 42, false)] /* exactly half, at fleet scale */
    [InlineData(22, 42, true)]
    public void MoreThanHalfPages_ExactlyHalfDoesNot(int regressedServers, int collectedServers, bool pages)
    {
        var routing = DarlingSelfAlertEvaluator.RouteCostRegressions(
            Enumerable.Range(1, regressedServers).Select(i => Regression(i, "query_stats")).ToArray(),
            new[] { Census("query_stats", collectedServers) });

        Assert.Equal(pages ? regressedServers : 0, routing.Paging.Count);
        Assert.Equal(pages ? 0 : regressedServers, routing.ReportOnly.Count);
    }

    /// <summary>
    /// A collector with no denominator is report-only, which is this routing failing toward SILENCE — the
    /// opposite of #3430's "fails toward posting", and safe only because the digest carries the finding in
    /// full. Asserted rather than commented, because the direction is the whole argument: if the digest were
    /// removed this default would have to invert.
    /// </summary>
    [Fact]
    public void ACollectorMissingFromTheCensus_IsReportOnly_NotPaged()
    {
        var routing = DarlingSelfAlertEvaluator.RouteCostRegressions(
            new[] { Regression(1, "brand_new_collector"), Regression(2, "brand_new_collector") },
            new[] { Census("query_store", 43) });

        Assert.Empty(routing.Paging);
        Assert.Equal(2, routing.ReportOnly.Count);
    }

    /// <summary>
    /// Two ways a duplicated row could make the majority rule easier to satisfy, both closed. A repeated
    /// (server, collector) must not inflate the fan-out NUMERATOR, and a repeated census row must not shrink
    /// the DENOMINATOR — so the numerator counts distinct servers and the denominator takes the highest
    /// server count offered for a collector name.
    /// </summary>
    [Fact]
    public void ADuplicatedRow_CannotInflateTheNumeratorOrShrinkTheDenominator()
    {
        /* Four rows, two servers: the numerator is 2, not 4, so 2 of 9 stays report-only. */
        var duplicatedRegressions = new[]
        {
            Regression(1, "wait_stats"), Regression(1, "wait_stats"),
            Regression(2, "wait_stats"), Regression(2, "wait_stats"),
        };
        Assert.Empty(DarlingSelfAlertEvaluator.RouteCostRegressions(
            duplicatedRegressions, new[] { Census("wait_stats", 9) }).Paging);

        /* A second census row claiming a smaller fleet must not win. */
        Assert.Empty(DarlingSelfAlertEvaluator.RouteCostRegressions(
            new[] { Regression(1, "wait_stats"), Regression(2, "wait_stats") },
            new[] { Census("wait_stats", 9), Census("wait_stats", 2) }).Paging);
    }

    /* ---------------- the digest lifecycle ---------------- */

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingHistoryStore : IAlertHistoryStore
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

    /// <summary>
    /// Built on the PRODUCT's own settings object over a default config rather than a fake, so the shared
    /// cooldown these tests sit beside is the shipped one — a hand-written fake would let the harness agree
    /// with a frozen literal and hide exactly the drift the #3060 pins exist to catch.
    /// </summary>
    private sealed class Harness
    {
        public DarlingConfig Config { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public RecordingHistoryStore History { get; } = new();
        public bool Muted { get; set; }
        public DateTime Now { get; set; } = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        public DarlingSelfAlertEvaluator Build() => new(
            new DarlingAlertSettings(Config), Deliverer, History, _ => Muted,
            logger: null, utcNow: () => Now);
    }

    /* Two runs rather than the measured firing's one (#3462): at one run the move is worth 11.1 s/day,
       under the 15 s/day floor the shipped read now gates on, and a lifecycle fixture should be a row the
       read could actually return. At two it is worth 22.1 s/day and everything else stays the measured
       #3440 figures. */
    private static readonly DarlingCollectorCostReader.CostMover[] OneMover =
    {
        Mover("query_store", "pm-server-1", latestMsPerRun: 17_548.0, baselineMsPerRun: 6_477.0,
            latestRuns: 2, p95: 17_935.0, worstDay: 17_935.0, worstRunMs: 17_548, eligiblePairs: 177),
    };

    private static readonly DarlingCollectorCostReader.CollectorCostSummaryRow[] OneCensusRow =
    {
        Census("query_stats", 43, totalSqlMs: 61_724_459, runCount: 43_891, maxSqlMs: 88_561),
    };

    /// <summary>
    /// The digest is a REPORT and says so in the only tier the rendering layer has for saying it: it fires
    /// with no severity override, which routes the severity map to this metric's own arm. The
    /// <c>Collector Cost Regression</c> page keeps its WARNING, so the two are distinguishable in every
    /// channel rather than only in the wording.
    /// </summary>
    [Fact]
    public async Task TheDigest_FiresUnderItsOwnMetric_WithNoSeverityOverride()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.CollectorCostDigestMetric, fired.MetricName);
        Assert.Equal("costdigest", fired.ServerKey);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Null(fired.Severity);
        Assert.Equal(1, fired.NumericCurrentValue);
        Assert.Equal(0, fired.NumericThresholdValue);
        Assert.Contains("no threshold", fired.ThresholdValue, StringComparison.Ordinal);
    }

    /// <summary>
    /// Once per interval, and the interval is the thing that makes it a periodic report rather than the
    /// same card arriving less often. Both directions asserted on one clock: inside the interval nothing is
    /// delivered, and past it the next digest goes out.
    /// </summary>
    [Fact]
    public async Task TheDigest_DoesNotRepeatInsideItsInterval_AndDoesAfterIt()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.Add(DarlingSelfAlertEvaluator.CollectorCostDigestInterval).AddMinutes(-1);
        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddMinutes(2);
        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    /// <summary>A digest with nothing in either section is a message saying a read returned no rows, which is
    /// the channel noise this issue is about. It sends nothing — and it does not consume the interval either,
    /// so the first real digest is not delayed by a day because an empty one was evaluated first.</summary>
    [Fact]
    public async Task AnEmptyDigest_SendsNothing_AndDoesNotConsumeTheInterval()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCollectorCostDigestAsync(
            Array.Empty<DarlingCollectorCostReader.CostMover>(),
            Array.Empty<DarlingCollectorCostReader.CollectorCostSummaryRow>(), Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    /// <summary>
    /// A digest has no resolution edge, unlike every sibling self-alert. Those are CONDITIONS, entered and
    /// left, so a recovery row closes the audit loop; this is a measurement of a period, with no state to
    /// leave. Asserted because the absence is a design choice — and because a "Digest Cleared" row arriving
    /// when a quiet day follows a busy one would be the most pointless message this product could send.
    /// </summary>
    [Fact]
    public async Task TheDigest_WritesNoResolutionRow_WhenTheNextPeriodIsEmpty()
    {
        var h = new Harness();
        var e = h.Build();

        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        h.Now = h.Now.Add(DarlingSelfAlertEvaluator.CollectorCostDigestInterval);
        await e.ApplyCollectorCostDigestAsync(
            Array.Empty<DarlingCollectorCostReader.CostMover>(),
            Array.Empty<DarlingCollectorCostReader.CollectorCostSummaryRow>(), Ct);

        Assert.Single(h.Deliverer.Outcomes);
        Assert.DoesNotContain(h.History.Records, r => r.MetricName.Contains("Digest", StringComparison.Ordinal));
    }

    /// <summary>Gated on the master alerts switch like every sibling, and the gate does not consume the
    /// interval — so turning alerting back on does not leave a day of silence behind it.</summary>
    [Fact]
    public async Task TheDigest_IsGatedOnTheMasterAlertsSwitch()
    {
        var h = new Harness();
        h.Config.Alerts.Enabled = false;
        var e = h.Build();

        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        h.Config.Alerts.Enabled = true;
        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    /// <summary>Muted through the SHARED seam, not a private decision. <c>Stale Mute Rules</c> is the one
    /// condition that judges its own mute, for a reason (it reports mutes, so a blanket rule silencing it
    /// would lose the report where it matters most) that does not apply to a cost report.</summary>
    [Fact]
    public async Task TheDigest_HonoursTheSharedMuteSeam()
    {
        var h = new Harness { Muted = true };
        var e = h.Build();

        await e.ApplyCollectorCostDigestAsync(OneMover, OneCensusRow, Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted);
    }

    /* ---------------- the rendered document ---------------- */

    private static async Task<string> RenderAsync(
        IReadOnlyList<DarlingCollectorCostReader.CostMover> movers,
        IReadOnlyList<DarlingCollectorCostReader.CollectorCostSummaryRow> census)
    {
        var h = new Harness();
        await h.Build().ApplyCollectorCostDigestAsync(movers, census, Ct);
        return Assert.Single(h.Deliverer.Outcomes).DetailText ?? string.Empty;
    }

    /// <summary>
    /// The identity pin. Flattened through the SHIPPED producer — the evaluator's own fire, so the string
    /// asserted here is the string an operator receives — and then re-parsed, because the failure this
    /// guards against is a document whose sentences disagree with each other rather than a field with a
    /// wrong value. Four identities, each of which must hold ACROSS figures rather than within one:
    ///
    /// <list type="number">
    /// <item>the header's "N of M" against the number of mover lines actually rendered, and N &lt;= M;</item>
    /// <item>each line's ratio against the two costs it sits between;</item>
    /// <item>each line's signed seconds per day against the per-run delta times the run count on the same
    ///   line — the quantity the header claims the ranking used;</item>
    /// <item>the ordering, against the absolute value of that same quantity.</item>
    /// </list>
    ///
    /// <para>The fixture mixes a riser and a faller, on deliberately awkward figures (a 1-run day and a
    /// 43,000-run day) so a renderer that formatted the delta instead of the product, or dropped the sign,
    /// cannot satisfy all four at once.</para>
    /// </summary>
    [Fact]
    public async Task TheRenderedDigestsFiguresReconcileWithEachOther()
    {
        /* Pre-ranked by |s/day|, the order MoverSql's ORDER BY delivers: the renderer trusts its reader
           and does not sort, so identity (4) below is an assertion about the fixture's realism, not about
           a sort the renderer performs. Realism also means every row clears #3462's 15 s/day floor — the
           shipped read no longer returns a row worth less — so the small-figures line is a cheap collector
           whose rise is material on VOLUME (the #3316 shape a per-run minimum would have wrongly excluded:
           3.0 -> 20.0 ms/run across 1,154 runs, the shared cadence's runs-per-day, is 19.6 s/day), and the
           1-run day became a 2-run day (11.1 s/day at one run sits under the floor). */
        var movers = new[]
        {
            Mover("procedure_stats", "pm-server-2", 456.2, 1_150.6, latestRuns: 44_002,
                p95: 1_175.9, worstDay: 1_175.9, worstRunMs: 81_854, eligiblePairs: 177),
            Mover("query_store", "pm-server-1", 17_548.0, 6_477.0, latestRuns: 2,
                p95: 17_935.0, worstDay: 17_935.0, worstRunMs: 17_548, eligiblePairs: 177),
            Mover("latch_stats", "pm-server-3", 20.0, 3.0, latestRuns: 1_154,
                p95: 3.2, worstDay: 3.4, worstRunMs: 46, eligiblePairs: 177),
        };

        var detail = await RenderAsync(movers, OneCensusRow);

        /* (1) the header against the lines. */
        var header = Regex.Match(detail, @"(\d+) of (\d+) \(server, collector\) pairs");
        Assert.True(header.Success, detail);
        var claimed = int.Parse(header.Groups[1].Value, CultureInfo.InvariantCulture);
        var eligible = int.Parse(header.Groups[2].Value, CultureInfo.InvariantCulture);

        var lines = Regex.Matches(
            detail,
            @"^- (?<collector>\S+) on (?<server>\S+): (?<latest>[\d,]+\.\d) ms/run vs a (?<baseline>[\d,]+\.\d) ms/run"
            + @" baseline \((?<ratio>[\d,]+\.\d\d)x\) over (?<days>[\d,]+) prior days"
            + @" whose own p95 was (?<p95>[\d,]+\.\d) ms/run and worst day (?<worstday>[\d,]+\.\d) ms/run;"
            + @" (?<runs>[\d,]+) runs, worst single run (?<worstrun>[\d,]+) ms"
            + @" -> (?<added>[-+]?[\d,]+\.\d) s/day$",
            RegexOptions.Multiline);

        Assert.Equal(movers.Length, lines.Count);
        Assert.Equal(lines.Count, claimed);
        Assert.True(claimed <= eligible, $"claimed {claimed} of {eligible}");

        double Number(Match m, string group) =>
            double.Parse(m.Groups[group].Value, NumberStyles.Any, CultureInfo.InvariantCulture);

        var addedByLine = new List<double>();
        foreach (Match line in lines)
        {
            var latest = Number(line, "latest");
            var baseline = Number(line, "baseline");
            var runs = Number(line, "runs");
            var ratio = Number(line, "ratio");
            var added = Number(line, "added");

            /* (2) the ratio is between the two costs printed beside it. */
            Assert.Equal(latest / baseline, ratio, 2);

            /* (3) the seconds per day are the per-run delta times the runs on THIS line. */
            Assert.Equal((latest - baseline) * runs / 1000.0, added, 1);

            /* A cheaper collector renders negative, which the paging condition cannot report at all. */
            Assert.Equal(latest < baseline, added < 0);

            addedByLine.Add(added);
        }

        /* (4) ranked by the absolute value of the quantity the header names. */
        Assert.Equal(
            addedByLine.OrderByDescending(Math.Abs).ToList(),
            addedByLine);

        /* At least one of each sign, or identity (3)'s sign half asserted nothing. */
        Assert.Contains(addedByLine, a => a > 0);
        Assert.Contains(addedByLine, a => a < 0);
    }

    /// <summary>
    /// The census section's own identity: the average printed beside a collector must be the total divided by
    /// the runs printed beside it. That average comes from <c>get_collector_cost</c>'s own summary row rather
    /// than from a figure this renderer computed, so this also pins that the digest and the MCP read agree.
    /// </summary>
    [Fact]
    public async Task TheCensusLinesAverageIsItsOwnTotalOverItsOwnRuns()
    {
        var census = new[]
        {
            Census("query_stats", 43, totalSqlMs: 61_724_459, runCount: 43_891, maxSqlMs: 88_561),
            Census("procedure_stats", 43, totalSqlMs: 98_100_000, runCount: 17_869, maxSqlMs: 137_455),
        };

        var detail = await RenderAsync(Array.Empty<DarlingCollectorCostReader.CostMover>(), census);

        var lines = Regex.Matches(
            detail,
            @"^= (?<collector>\S+): (?<total>[\d,]+) ms over (?<runs>[\d,]+) runs on (?<servers>[\d,]+) servers"
            + @" \((?<avg>[\d,]+) ms/run, worst single run (?<worst>[\d,]+) ms\)$",
            RegexOptions.Multiline);

        Assert.Equal(census.Length, lines.Count);
        foreach (Match line in lines)
        {
            var total = long.Parse(line.Groups["total"].Value, NumberStyles.Any, CultureInfo.InvariantCulture);
            var runs = long.Parse(line.Groups["runs"].Value, NumberStyles.Any, CultureInfo.InvariantCulture);
            var avg = long.Parse(line.Groups["avg"].Value, NumberStyles.Any, CultureInfo.InvariantCulture);
            Assert.Equal(total / runs, avg);
        }
    }

    /* ---------------- what must not be lost ---------------- */

    /// <summary>
    /// #2150's recorded figures, reconstructed. The Query Store collector's per-cycle SQL time went from a
    /// 4.8 s median to 37-100 minutes on the reporter's databases — the worst regression this metric has ever
    /// had to find. At the low end of that band (37 min = 2,220,000 ms per run against a 4,800 ms baseline)
    /// the digest must name it, and its printed ratio and added cost must be the reconstructed figures rather
    /// than something a renderer rounded into meaninglessness.
    ///
    /// <para><b>The honest caveat, stated here because a test is where it cannot be dropped:</b> #2150 itself
    /// was a LITE install, which has no <c>collect.collector_cost</c> twin (that series is the central
    /// store's own hourly self-metric), so neither this digest nor the alert it is demoted from could have
    /// seen that instance. What both surfaces see is the same per-run magnitude on a Darling store, which is
    /// where the same pathology was independently bisected (#2133/#2134).</para>
    /// </summary>
    [Fact]
    public async Task TheDigestSurfacesTheQueryStoreRegressionFrom2150()
    {
        var detail = await RenderAsync(
            new[]
            {
                Mover("query_store", "pm-server-1", latestMsPerRun: 2_220_000.0, baselineMsPerRun: 4_800.0,
                    latestRuns: 12, p95: 5_600.0, worstDay: 6_100.0, worstRunMs: 6_000_000, eligiblePairs: 177),
            },
            Array.Empty<DarlingCollectorCostReader.CollectorCostSummaryRow>());

        Assert.Contains("query_store on pm-server-1", detail, StringComparison.Ordinal);
        /* 2,220,000 / 4,800 = 462.5x, and the move is worth (2,220,000 - 4,800) x 12 / 1000 = 26,582.4 s/day. */
        Assert.Contains("(462.50x)", detail, StringComparison.Ordinal);
        Assert.Contains("+26582.4 s/day", detail, StringComparison.Ordinal);
        /* The dispersion pair is what makes it unambiguous rather than a slow day: its own worst prior day
           was 6.1 s per run, and today is three orders of magnitude past that. */
        Assert.Contains("worst day 6,100.0 ms/run", detail, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2862's recorded figures, reconstructed — and the reason the digest carries a heaviest-first census at
    /// all. <c>procedure_stats</c> was found at 98.1M ms/day over 17,869 runs (5,490 ms/run, worst run
    /// 137,455 ms) by READING A COST RANKING, not by any alert: it was expensive rather than newly expensive,
    /// so a ranking by MOVEMENT cannot see it. The movers list is deliberately EMPTY here, so this pin fails
    /// if the census section is ever dropped as decoration.
    /// </summary>
    [Fact]
    public async Task TheDigestSurfacesProcedureStatsFrom2862_ThroughTheCensus_WithNoMoversAtAll()
    {
        var detail = await RenderAsync(
            Array.Empty<DarlingCollectorCostReader.CostMover>(),
            new[] { Census("procedure_stats", 42, totalSqlMs: 98_100_000, runCount: 17_869, maxSqlMs: 137_455) });

        Assert.Contains("procedure_stats: 98,100,000 ms over 17,869 runs on 42 servers", detail, StringComparison.Ordinal);
        /* 98,100,000 / 17,869 = 5,489 ms/run by the summary row's integer division (#2862 quoted the
           rounded 5,490). The renderer prints the stored row's figure, not a recomputation. */
        Assert.Contains("(5,489 ms/run, worst single run 137,455 ms)", detail, StringComparison.Ordinal);
    }

    /// <summary>
    /// The half of the series the paging condition structurally cannot report: a collector that got CHEAPER.
    /// #2915's plan-render cadence gate took <c>procedure_stats</c> from 1,150.6 to 456.2 ms per run on one
    /// production fleet and nothing in this series said so, because the predicate fires only on rises and
    /// #2846 removed the bug that had made improvements read as regressions.
    /// </summary>
    [Fact]
    public async Task TheDigestReportsACollectorThatGotCheaper()
    {
        var detail = await RenderAsync(
            new[]
            {
                Mover("procedure_stats", "pm-server-2", latestMsPerRun: 456.2, baselineMsPerRun: 1_150.6,
                    latestRuns: 44_002, p95: 1_175.9, worstDay: 1_175.9, worstRunMs: 81_854, eligiblePairs: 177),
            },
            Array.Empty<DarlingCollectorCostReader.CollectorCostSummaryRow>());

        Assert.Contains("(0.40x)", detail, StringComparison.Ordinal);
        /* (456.2 - 1,150.6) x 44,002 / 1,000 = -30,555.0, in the +0.0;-0.0;0.0 format's plain digits. */
        Assert.Contains("-30555.0 s/day", detail, StringComparison.Ordinal);
        Assert.Contains("got CHEAPER", detail, StringComparison.Ordinal);
    }

    /* ---------------- the surfaces that read the metric name ---------------- */

    /// <summary>
    /// The metric name is a webhook automation key and two other assemblies carry their own literal of it —
    /// <c>AlertSeverity.ForMetric</c>'s INFO arm (pinned in <c>Lite.Tests</c>, whose project cannot reference
    /// this one) and <c>AlertMetricClassifier</c>'s count arm. Pinning the const's VALUE here is what ties
    /// those literals to this one: a rename that updated only the const would fail here, and one that
    /// updated only a literal would fail there.
    /// </summary>
    [Fact]
    public void TheDigestsMetricName_IsTheLiteralTheOtherAssembliesCarry()
    {
        Assert.Equal("Collector Cost Digest", DarlingSelfAlertEvaluator.CollectorCostDigestMetric);
    }

    /// <summary>The digest's current value is a COUNT of pairs, so it renders as a whole number rather than
    /// falling through to the two-decimal default — the <c>Stale Mute Rules</c> shape. It is not a
    /// resolution and not a warning-tier metric.</summary>
    [Fact]
    public void TheDigestsCount_RendersAsAWholeNumber()
    {
        Assert.Equal("20", AlertMetricClassifier.FormatHistoryValue(
            DarlingSelfAlertEvaluator.CollectorCostDigestMetric, 20));
        Assert.False(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.CollectorCostDigestMetric));
    }

    /* ---------------- the shipped query's own shape ---------------- */

    /// <summary>
    /// The digest read's shape after #3462: a window, the materiality floor, and a row cap — and NOTHING
    /// else, where the paging read also takes a ratio factor. #3448 shipped this read with two parameters
    /// and load-bearing prose that the ranking made a floor unnecessary; #3462 narrowed that deliberately,
    /// because the row cap is a presentation bound and on a quiet fleet the list filled its remainder with
    /// rows worth a few hundred milliseconds a day — the same noise the paging floor exists to stop, in a
    /// quieter channel. What this read still does NOT carry is the part that decides: no ratio factor and
    /// no dispersion bound, because it ranks and the reader judges — and the floor it gained is the SAME
    /// expression the ranking sorts on and <c>CostMover.AddedMsPerDay</c> derives, taken on abs() so a
    /// material improvement still gets its line, which is why gaining it does not change what kind of read
    /// this is.
    /// </summary>
    [Fact]
    public void TheDigestRead_TakesTheSharedMaterialityFloor_ButNoFactorAndNoDispersionBound()
    {
        var sql = DarlingCollectorCostReader.MoverSql;

        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);

        /* The floor gates the same expression the ranking sorts on, on the move's MAGNITUDE — asserted as
           the exact conjunct so a rewrite that floored only rises (dropping the got-cheaper half) or gated
           a different quantity than it prints goes red here. */
        Assert.Contains(
            "WHERE abs((e.latest_ms_per_run - e.baseline_ms_per_run) * e.latest_runs) >= $2",
            sql, StringComparison.Ordinal);

        /* The ranking is the absolute added cost per day — the same expression CostMover.AddedMsPerDay
           derives, so the printed figure and the row's position cannot disagree. */
        Assert.Contains(
            "ORDER BY abs((e.latest_ms_per_run - e.baseline_ms_per_run) * e.latest_runs) DESC",
            sql, StringComparison.Ordinal);

        /* The dispersion figures are CARRIED, and percentile_disc for #2460's reason - at most 13 prior days,
           where an interpolation between a bimodal series' two modes is a figure no day ever cost. */
        Assert.Contains("percentile_disc(0.95)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("percentile_cont", sql, StringComparison.Ordinal);
    }

    /// <summary>The added-cost figure is DERIVED from the members that explain it, so a line cannot print a
    /// delta and a total that do not multiply out — the property #3316 gave
    /// <c>CostRegression.AddedMsPerDay</c>, on the record that feeds a document rather than a card.</summary>
    [Theory]
    [InlineData(200.0, 50.0, 1000, 150_000.0)]
    [InlineData(50.0, 200.0, 1000, -150_000.0)]
    [InlineData(17_548.0, 6_477.0, 1, 11_071.0)]
    public void TheMoversAddedCost_IsItsOwnDeltaTimesItsOwnRuns(
        double latest, double baseline, long runs, double expected)
    {
        var mover = Mover("c", "s", latest, baseline, runs);
        Assert.Equal(expected, mover.AddedMsPerDay, 6);
        Assert.Equal(latest / baseline, mover.Ratio, 6);
    }

    /* ---------------- the delivery constraint (#3493) ---------------- */

    /// <summary>Every string Slack counts against its per-text-object ceiling, wherever it sits in the
    /// payload — section texts, field texts, the header title, context elements, button labels.</summary>
    private static IEnumerable<string> AllTextObjects(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.NameEquals("text") && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        yield return prop.Value.GetString()!;
                        continue;
                    }

                    foreach (var s in AllTextObjects(prop.Value))
                    {
                        yield return s;
                    }
                }

                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    foreach (var s in AllTextObjects(item))
                    {
                        yield return s;
                    }
                }

                break;
        }
    }

    /// <summary>
    /// The live failure, reconstructed end to end (#3493): a 20-mover digest — the shape both big-fleet
    /// stores produced on the first two digest nights, and Slack rejected with HTTP 400
    /// <c>invalid_attachments</c> while the same webhook delivered a rollup 80 milliseconds later — rendered
    /// through the SHIPPED producer and fed to the SHIPPED Slack builder, must now satisfy both of Slack's
    /// ceilings: every text object inside 3,000 characters and the message inside 50 blocks. And it must do
    /// so IN FULL: at this size the split alone is enough, so every one of the twenty movers is present and
    /// no stated-omission line appears — a fix that delivered a degraded digest where a whole one fits would
    /// be trading one silent loss for a quieter one.
    ///
    /// <para>The fixture is the digest's own render, not a lookalike string, so its line lengths track the
    /// real format: risers ranked by declining added cost (the read's ORDER BY, which the renderer trusts),
    /// every row clearing #3462's materiality floor, and the census at its full ten-row cap — a busy fleet's
    /// digest, because the busy fleets are exactly where the failure lived.</para>
    /// </summary>
    [Fact]
    public async Task ATwentyMoverDigest_DeliversInFull_InsideBothSlackCeilings()
    {
        var collectors = new[]
        {
            "query_store", "query_stats", "procedure_stats", "wait_stats",
            "latch_stats", "database_size_stats", "plan_correction", "cpu_utilization_stats",
        };

        /* Added cost declines 30,000 -> 3,400 s/day across the twenty lines, all above the 15 s/day
           floor; (i % 8, i % 12) keeps every (collector, server) pair distinct. */
        var movers = Enumerable.Range(0, 20).Select(i =>
        {
            var runs = 500L + (i * 37);
            var baseline = 250.0 + (i * 55.5);
            var latest = baseline + ((30_000_000.0 - (i * 1_400_000.0)) / runs);
            return Mover(
                collectors[i % collectors.Length],
                "pm-server-" + ((i % 12) + 1).ToString(CultureInfo.InvariantCulture),
                latestMsPerRun: latest, baselineMsPerRun: baseline, latestRuns: runs,
                p95: baseline * 1.3, worstDay: baseline * 1.8, worstRunMs: (long)(latest * 2.2),
                eligiblePairs: 177);
        }).ToArray();

        var census = new[]
        {
            Census("procedure_stats", 43, totalSqlMs: 98_100_000, runCount: 17_869, maxSqlMs: 137_455),
            Census("query_stats", 43, totalSqlMs: 61_724_459, runCount: 43_891, maxSqlMs: 88_561),
            Census("query_store", 4, totalSqlMs: 22_450_112, runCount: 1_204, maxSqlMs: 402_118),
            Census("wait_stats", 43, totalSqlMs: 9_812_334, runCount: 44_120, maxSqlMs: 4_209),
            Census("cpu_utilization_stats", 43, totalSqlMs: 7_401_889, runCount: 44_106, maxSqlMs: 3_118),
            Census("latch_stats", 43, totalSqlMs: 5_220_741, runCount: 44_098, maxSqlMs: 2_953),
            Census("database_size_stats", 43, totalSqlMs: 4_106_220, runCount: 21_997, maxSqlMs: 8_402),
            Census("plan_correction", 41, totalSqlMs: 3_551_078, runCount: 20_446, maxSqlMs: 12_336),
            Census("blocking_snapshot", 43, totalSqlMs: 2_900_432, runCount: 44_051, maxSqlMs: 1_890),
            Census("collection_health", 43, totalSqlMs: 1_744_216, runCount: 43_960, maxSqlMs: 977),
        };

        var detail = await RenderAsync(movers, census);

        /* The fixture guard: if the digest's render ever shrinks under one section's capacity, this test
           stops exercising the split and must say so rather than passing vacuously. */
        Assert.True(detail.Length > WebhookAlertService.SlackTextObjectLimit,
            $"the 20-mover fixture renders {detail.Length} chars — no longer past the ceiling, rebuild it");

        var payload = WebhookAlertService.BuildSlackPayload(
            DarlingSelfAlertEvaluator.CollectorCostDigestMetric,
            DarlingSelfAlertEvaluator.StoreServerLabel,
            "20", "no threshold (report)", DarlingAlertDeliverer.Branding, detailText: detail);

        /* Both ceilings, on the parsed document rather than the string. */
        using var doc = JsonDocument.Parse(payload);
        var blocks = doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks")
            .EnumerateArray().ToList();
        Assert.True(blocks.Count <= WebhookAlertService.SlackMessageBlockLimit,
            $"{blocks.Count} blocks");
        Assert.All(AllTextObjects(doc.RootElement),
            t => Assert.True(t.Length <= WebhookAlertService.SlackTextObjectLimit,
                $"a text object is {t.Length} chars"));

        /* The split actually engaged — one section was the defect. */
        var proseSections = blocks.Count(b =>
            b.GetProperty("type").GetString() == "section" && b.TryGetProperty("text", out _));
        Assert.True(proseSections >= 2, $"{proseSections} prose section(s) — the split did not engage");

        /* In full: every mover identity delivered (the splitter never cuts a line, so each appears intact
           inside one section), and nothing was omitted. */
        foreach (var mover in movers)
        {
            Assert.Contains($"{mover.CollectorName} on {mover.ServerName}:", payload, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("first omitted", payload, StringComparison.Ordinal);
    }
}
