/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The ungated half of <c>get_query_store_clutter</c> (#3797): the pure judgment in
/// <see cref="QueryStoreClutter"/> executed against planted rows, the tool's surface and description, the
/// four statements' text, and the registration sites a new Darling tool has to land on (the host, the web
/// catalog and dispatch, the instructions census row, the Lite inventory ratchet). The SQL's execution is
/// <see cref="QueryStoreClutterLivePostgresTests"/>, which plants the same shapes on a scratch store and
/// asserts the same raw numbers through the real tool.
///
/// <para>The judgment tests are written against the thresholds by NAME so a bar moved deliberately fails
/// here by one constant rather than by a dozen literals. Each boundary is exercised on both sides.</para>
/// </summary>
public sealed class QueryStoreClutterTests
{
    private const string ToolName = "get_query_store_clutter";
    private const string ToolSource = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreClutterTools.cs";

    private static MethodInfo Tool() => typeof(DarlingMcpQueryStoreClutterTools)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == ToolName);

    /* ─────────────────────────── row factories ─────────────────────────── */

    private static DarlingQueryStoreClutterReader.ReadCostRow ReadCost(
        string db, int runsObserved, int runsSlowest, double sharePct, int fanoutItems = 3, int slowestMs = 9000, int? othersMs = 3000) =>
        new(1, db, runsObserved, runsSlowest, slowestMs, slowestMs, 10000, sharePct, fanoutItems, new DateTime(2026, 9, 20, 12, 0, 0), othersMs);

    private static DarlingQueryStoreClutterReader.PlanChurnRow Churn(
        string db, int p95, int max = 0, int distinctPlans = 30, int seenOnce = 0, int collections = 3, int firstSeenLater = 0, double spanHours = 2) =>
        new(1, db, new DateTime(2026, 9, 20, 6, 0, 0), new DateTime(2026, 9, 20, 6, 0, 0).AddHours(spanHours), collections,
            10, distinctPlans, p95, Math.Max(max, p95), seenOnce, firstSeenLater);

    /// <param name="captureMode">The V137 (#3796) column. Defaults to <c>AUTO</c> so a fixture that says
    /// nothing about capture mode is a MEASURED one; a fixture about the pre-rung null passes null explicitly.</param>
    private static DarlingQueryStoreClutterReader.ConfigRow Config(
        string db, string actual = "READ_WRITE", int readonlyReason = 0, long current = 4096, long max = 8192, long maxPlans = 200,
        string? captureMode = "AUTO") =>
        new(1, db, actual, "READ_WRITE", readonlyReason, current, max, "AUTO", 21, maxPlans, 60, new DateTime(2026, 9, 20, 11, 30, 0), captureMode);

    /* ─────────────────────────── the judgment ─────────────────────────── */

    [Fact]
    public void AReplica_IsUnknownWithTheArchitecturalReason_AndNothingElse()
    {
        /* The replica bit beside a share and a churn that would otherwise be Critical: neither is judged,
           because both are the primary's. */
        var (verdict, reasons) = QueryStoreClutter.Judge(
            ReadCost("db", 10, 10, 95),
            Churn("db", QueryStoreClutter.PlansPerQueryP95Critical),
            Config("db", "READ_ONLY", readonlyReason: DarlingQueryStoreClutterReader.SecondaryReplicaReadonlyBit));

        Assert.Equal(HealthSeverity.Unknown, verdict);
        Assert.Equal(new[] { QueryStoreClutter.ReasonReplica }, reasons);
    }

    [Fact]
    public void TheReplicaBit_IsTestedAsABit_NotAnEquality()
    {
        /* 65544 = storage cap (65536) + replica (8): a replica whose primary also hit the cap is still a replica. */
        var config = Config("db", "READ_ONLY", readonlyReason: 65536 + DarlingQueryStoreClutterReader.SecondaryReplicaReadonlyBit);
        Assert.True(config.IsSecondaryReplica);
        Assert.False(Config("db", "READ_ONLY", readonlyReason: 65536).IsSecondaryReplica);
    }

    [Fact]
    public void NothingMeasured_IsUnknown_NeverHealthy()
    {
        var (verdict, reasons) = QueryStoreClutter.Judge(null, null, null);
        Assert.Equal(HealthSeverity.Unknown, verdict);
        Assert.Equal(new[] { QueryStoreClutter.ReasonNotMeasured }, reasons);
    }

    [Fact]
    public void QueryStoreOff_IsUnknownWithTheReason_AndIsNotABand()
    {
        var (verdict, reasons) = QueryStoreClutter.Judge(null, null, Config("db", "OFF"));
        Assert.Equal(HealthSeverity.Unknown, verdict);
        Assert.Equal(new[] { QueryStoreClutter.ReasonQueryStoreOff }, reasons);
    }

    [Fact]
    public void MeasuredAndQuiet_IsHealthy_WithNoReasons()
    {
        var (verdict, reasons) = QueryStoreClutter.Judge(
            ReadCost("db", 10, 1, 30),
            Churn("db", 1),
            Config("db"));

        Assert.Equal(HealthSeverity.Healthy, verdict);
        Assert.Empty(reasons);
    }

    [Theory]
    [InlineData(80, 90, 3, HealthSeverity.Critical)]   /* the motivating shape: usual slowest, 90% of the pass */
    [InlineData(80, 79.9, 3, HealthSeverity.Warning)]  /* just under the critical bar */
    [InlineData(80, 50, 3, HealthSeverity.Warning)]    /* exactly the warning bar */
    [InlineData(80, 49.9, 3, HealthSeverity.Healthy)]  /* just under the warning bar */
    [InlineData(49, 95, 3, HealthSeverity.Healthy)]    /* under the runs gate: one bad run is a run, not a database */
    [InlineData(100, 100, 1, HealthSeverity.Healthy)]  /* a fan-out of one database has a 100% share by construction */
    public void ReadCost_IsBandedByShare_BehindTheRunsGateAndTheFanoutFloor(double runsSlowestPct, double sharePct, int fanoutItems, HealthSeverity expected)
    {
        var (verdict, reasons) = QueryStoreClutter.Judge(ReadCost("db", 1000, (int)(runsSlowestPct * 10), sharePct, fanoutItems), null, null);
        Assert.Equal(expected, verdict);
        Assert.Equal(expected != HealthSeverity.Healthy, reasons.Contains(QueryStoreClutter.ReasonReadCostDominant));
    }

    [Theory]
    [InlineData(10, HealthSeverity.Critical)]
    [InlineData(9, HealthSeverity.Warning)]
    [InlineData(4, HealthSeverity.Warning)]
    [InlineData(3, HealthSeverity.Healthy)]
    public void PlanChurn_IsBandedByPlansPerQueryP95(int p95, HealthSeverity expected)
    {
        Assert.Equal(4, QueryStoreClutter.PlansPerQueryP95Warning);
        Assert.Equal(10, QueryStoreClutter.PlansPerQueryP95Critical);
        var (verdict, reasons) = QueryStoreClutter.Judge(null, Churn("db", p95), null);
        Assert.Equal(expected, verdict);
        Assert.Equal(expected != HealthSeverity.Healthy, reasons.Contains(QueryStoreClutter.ReasonPlanChurnHigh));
    }

    [Fact]
    public void OneShotPlans_NeedThePopulationFloor_AndTwoCollections()
    {
        /* Half of 40 plans seen once, three collections: Warning. */
        var (v1, r1) = QueryStoreClutter.Judge(null, Churn("db", 1, distinctPlans: 40, seenOnce: 20, collections: 3), null);
        Assert.Equal(HealthSeverity.Warning, v1);
        Assert.Contains(QueryStoreClutter.ReasonOneShotPlans, r1);

        /* The same fraction over 19 plans: not a workload shape. */
        var (v2, r2) = QueryStoreClutter.Judge(null, Churn("db", 1, distinctPlans: 19, seenOnce: 10, collections: 3), null);
        Assert.Equal(HealthSeverity.Healthy, v2);
        Assert.DoesNotContain(QueryStoreClutter.ReasonOneShotPlans, r2);

        /* One collection: every plan is seen once by construction, so the fraction is null and nothing fires. */
        var row = Churn("db", 1, distinctPlans: 40, seenOnce: 40, collections: 1);
        Assert.Null(QueryStoreClutter.NeverSeenTwiceFraction(row));
        var (v3, r3) = QueryStoreClutter.Judge(null, row, null);
        Assert.Equal(HealthSeverity.Healthy, v3);
        Assert.DoesNotContain(QueryStoreClutter.ReasonOneShotPlans, r3);
    }

    [Fact]
    public void PlansAtCap_ReadsTheConfiguredCap_AndOnlyWithAConfigRow()
    {
        var (v1, r1) = QueryStoreClutter.Judge(null, Churn("db", 1, max: 200), Config("db", maxPlans: 200));
        Assert.Equal(HealthSeverity.Warning, v1);
        Assert.Contains(QueryStoreClutter.ReasonPlansAtCap, r1);

        var (_, r2) = QueryStoreClutter.Judge(null, Churn("db", 1, max: 199), Config("db", maxPlans: 200));
        Assert.DoesNotContain(QueryStoreClutter.ReasonPlansAtCap, r2);

        /* No config row: the cap is unknown, so nothing is at it. */
        var (_, r3) = QueryStoreClutter.Judge(null, Churn("db", 1, max: 200), null);
        Assert.DoesNotContain(QueryStoreClutter.ReasonPlansAtCap, r3);
    }

    [Fact]
    public void Config_BandsStorageNearTheCap_AndAnOperatorReadOnlyAsCritical()
    {
        var (v1, r1) = QueryStoreClutter.Judge(null, null, Config("db", current: 7373, max: 8192)); /* 90.0% */
        Assert.Equal(HealthSeverity.Warning, v1);
        Assert.Equal(new[] { QueryStoreClutter.ReasonStorageNearCap }, r1);

        var (v2, _) = QueryStoreClutter.Judge(null, null, Config("db", current: 7372, max: 8192)); /* 89.99% */
        Assert.Equal(HealthSeverity.Healthy, v2);

        /* READ_ONLY for the storage cap (65536): the terminal state of clutter, and not the replica bit. */
        var (v3, r3) = QueryStoreClutter.Judge(null, null, Config("db", "READ_ONLY", readonlyReason: 65536, current: 8192, max: 8192));
        Assert.Equal(HealthSeverity.Critical, v3);
        Assert.Contains(QueryStoreClutter.ReasonQueryStoreReadOnly, r3);
        Assert.Contains(QueryStoreClutter.ReasonStorageNearCap, r3);
    }

    [Fact]
    public void TheVerdict_IsTheWorstArm_AndEveryArmKeepsItsReason()
    {
        var (verdict, reasons) = QueryStoreClutter.Judge(
            ReadCost("db", 10, 8, 60),                                /* Warning */
            Churn("db", QueryStoreClutter.PlansPerQueryP95Critical), /* Critical */
            Config("db", current: 8000, max: 8192));                   /* Warning */

        Assert.Equal(HealthSeverity.Critical, verdict);
        Assert.Equal(
            new[] { QueryStoreClutter.ReasonStorageNearCap, QueryStoreClutter.ReasonReadCostDominant, QueryStoreClutter.ReasonPlanChurnHigh },
            reasons);
    }

    /* ─────────────────────────── the derivations ─────────────────────────── */

    [Fact]
    public void DominanceRatio_IsThisDatabasesMedianOverTheOthersPooledMedian_AndNullWithNoOthers()
    {
        Assert.Equal(3.0, QueryStoreClutter.DominanceRatio(ReadCost("db", 10, 8, 90, slowestMs: 9000, othersMs: 3000)));
        Assert.Null(QueryStoreClutter.DominanceRatio(ReadCost("db", 10, 10, 100, slowestMs: 9000, othersMs: null)));
        Assert.Null(QueryStoreClutter.DominanceRatio(ReadCost("db", 10, 8, 90, slowestMs: 9000, othersMs: 0)));
    }

    [Fact]
    public void NewPlansPerDay_DividesArrivalsByTheObservedSpan_AndIsNullOnAPoint()
    {
        /* 3 arrivals over 2 hours = 36 a day; a span of zero cannot have a rate. */
        Assert.Equal(36.0, QueryStoreClutter.NewPlansPerDay(Churn("db", 1, firstSeenLater: 3, spanHours: 2))!.Value, 6);
        Assert.Null(QueryStoreClutter.NewPlansPerDay(Churn("db", 1, firstSeenLater: 3, spanHours: 0, collections: 1)));
    }

    [Fact]
    public void WaitMsPerHour_DividesRatedMillisecondsByMeasuredSeconds_AndIsNullUnrated()
    {
        Assert.Equal(36000.0, QueryStoreClutter.WaitMsPerHour(1800, 180));
        Assert.Null(QueryStoreClutter.WaitMsPerHour(1800, 0));
    }

    [Theory]
    [InlineData(new double[] { }, null)]
    [InlineData(new double[] { 7 }, 7.0)]
    [InlineData(new double[] { 30, 40 }, 30.0)]        /* percentile_disc(0.5): the smaller of two */
    [InlineData(new double[] { 90, 30, 40 }, 40.0)]
    [InlineData(new double[] { 1, 6, 3, 9 }, 3.0)]
    [InlineData(new double[] { 5, 1, 4, 2, 3 }, 3.0)]
    public void DiscreteMedian_IsPercentileDiscOfOneHalf(double[] values, double? expected) =>
        Assert.Equal(expected, QueryStoreClutter.DiscreteMedian(values));

    [Fact]
    public void TheExcludedQdsWaits_AreExactlyTheSharedIgnoreListsFour()
    {
        Assert.Equal(
            ["QDS_ASYNC_QUEUE", "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP", "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP", "QDS_SHUTDOWN_QUEUE"],
            QueryStoreClutter.ExcludedQdsWaitTypes);
        Assert.All(QueryStoreClutter.ExcludedQdsWaitTypes, w => Assert.Contains(w, IgnoredWaitDefaults.All));
        Assert.True(QueryStoreClutter.IsExcludedWaitType("QDS_ASYNC_QUEUE"));
        Assert.False(QueryStoreClutter.IsExcludedWaitType("QDS_LOADDB"));
        Assert.False(QueryStoreClutter.IsExcludedWaitType("QDS_BLOCKING_TASK"));
    }

    [Fact]
    public void AReplicaServer_HasTheBitOnEveryRecordingDatabase_AndAnEmptySetIsNotOne()
    {
        var replicaBit = DarlingQueryStoreClutterReader.SecondaryReplicaReadonlyBit;
        Assert.True(QueryStoreClutter.IsReplicaServer([Config("a", "READ_ONLY", replicaBit), Config("b", "OFF")]));
        Assert.False(QueryStoreClutter.IsReplicaServer([Config("a", "READ_ONLY", replicaBit), Config("b")]));
        Assert.False(QueryStoreClutter.IsReplicaServer([Config("a")]));
        Assert.False(QueryStoreClutter.IsReplicaServer([]));
    }

    /* ─────────────────────────── compose + fleet ─────────────────────────── */

    [Fact]
    public void Compose_UnionsEveryArmsDatabases_OrdersWorstFirst_AndMarksTheReplica()
    {
        var rows = QueryStoreClutter.Compose(
            [ReadCost("alpha", 10, 8, 90), ReadCost("gamma", 10, 1, 30)],
            [Churn("beta", 6), Churn("alpha", 1)],
            [Config("alpha"), Config("beta"), Config("gamma"), Config("delta", "OFF"), Config("rep", "READ_ONLY", DarlingQueryStoreClutterReader.SecondaryReplicaReadonlyBit)]);

        Assert.Equal(new[] { "alpha", "beta", "gamma", "delta", "rep" }, rows.Select(r => r.DatabaseName).ToArray());
        Assert.Equal(HealthSeverity.Critical, rows[0].Verdict);
        Assert.Equal(HealthSeverity.Warning, rows[1].Verdict);
        Assert.Equal(HealthSeverity.Healthy, rows[2].Verdict);
        Assert.Equal(HealthSeverity.Unknown, rows[3].Verdict);
        Assert.Equal(HealthSeverity.Unknown, rows[4].Verdict);
        Assert.True(rows[4].Excluded);
        Assert.Equal(QueryStoreClutter.ReasonReplica, rows[4].ExcludedReason);
        Assert.All(rows.Take(4), r => Assert.False(r.Excluded));
        Assert.Null(rows[1].ReadCost);
        Assert.Null(rows[2].PlanChurn);
    }

    [Fact]
    public void TheFleetMedian_ExcludesReplicaServers_AndCountsEachPopulation()
    {
        var replicaBit = DarlingQueryStoreClutterReader.SecondaryReplicaReadonlyBit;
        var readCost = new[]
        {
            ReadCost("alpha", 10, 8, 90), ReadCost("beta", 10, 1, 40), ReadCost("gamma", 10, 1, 30),
            /* a fan-out of one database on server 3: excluded from the share population by the floor */
            new DarlingQueryStoreClutterReader.ReadCostRow(3, "solo", 10, 10, 100, 100, 100, 100, 1, new DateTime(2026, 9, 20), null),
        };
        var churn = new[] { Churn("alpha", 1), Churn("beta", 6), new DarlingQueryStoreClutterReader.PlanChurnRow(3, "solo", new DateTime(2026, 9, 20), new DateTime(2026, 9, 20), 1, 1, 1, 12, 12, 0, 0) };
        var config = new[]
        {
            Config("alpha"), Config("beta"), Config("gamma"),
            new DarlingQueryStoreClutterReader.ConfigRow(2, "alpha", "READ_ONLY", "READ_WRITE", replicaBit, 0, 8192, "AUTO", 21, 200, 60, new DateTime(2026, 9, 20), "AUTO"),
            new DarlingQueryStoreClutterReader.ConfigRow(3, "solo", "READ_WRITE", "READ_WRITE", 0, 0, 8192, "AUTO", 21, 200, 60, new DateTime(2026, 9, 20), "AUTO"),
        };
        var waits = new[]
        {
            new DarlingQueryStoreClutterReader.QdsWaitRow(1, "QDS_LOADDB", 1900, 30, 1800, 180, 5, 1, 1, new DateTime(2026, 9, 20), new DateTime(2026, 9, 20)),
            new DarlingQueryStoreClutterReader.QdsWaitRow(1, "QDS_BLOCKING_TASK", 120, 1, 120, 60, 1, 0, 0, new DateTime(2026, 9, 20), new DateTime(2026, 9, 20)),
            new DarlingQueryStoreClutterReader.QdsWaitRow(1, "QDS_ASYNC_QUEUE", 59000, 1, 59000, 60, 1, 0, 0, new DateTime(2026, 9, 20), new DateTime(2026, 9, 20)),
            new DarlingQueryStoreClutterReader.QdsWaitRow(2, "QDS_LOADDB", 600, 10, 600, 60, 1, 0, 0, new DateTime(2026, 9, 20), new DateTime(2026, 9, 20)),
            new DarlingQueryStoreClutterReader.QdsWaitRow(3, "QDS_LOADDB", 3600, 10, 3600, 3600, 60, 0, 0, new DateTime(2026, 9, 20), new DateTime(2026, 9, 20)),
        };

        var median = QueryStoreClutter.ComputeFleetMedian([1, 2, 3], readCost, churn, config, waits);

        Assert.Equal(2, median.ServersInMedian);
        Assert.Equal(1, median.ReplicaServersExcluded);
        /* shares: alpha 90, beta 40, gamma 30 (server 3's solo fan-out is under the floor) → 40 */
        Assert.Equal(40.0, median.SlowestSharePct);
        Assert.Equal(3, median.DatabasesInReadCostMedian);
        /* churn: 1, 6, 12 → 6 */
        Assert.Equal(6.0, median.PlansPerQueryP95);
        Assert.Equal(3, median.DatabasesInChurnMedian);
        /* waits per server: 1 → 36000 + 7200 (the sleep wait is dropped), 3 → 3600; server 2 is the replica → [3600, 43200] → 3600 */
        Assert.Equal(3600.0, median.QdsWaitMsPerHour);
        Assert.Equal(2, median.ServersInWaitMedian);
    }

    [Fact]
    public void Recommendations_NameTheKnobs_AndTheCaptureModeTheRowActuallyCarries()
    {
        var row = QueryStoreClutter.Compose(
            [ReadCost("beta", 10, 8, 90)],
            [Churn("beta", 6, distinctPlans: 9)],
            [Config("beta")]).Single();

        var lines = QueryStoreClutter.Recommendations(row);
        Assert.Equal(2, lines.Count);
        Assert.Contains("per-database schedule override", lines[0], StringComparison.Ordinal);
        Assert.Contains("80% of the query_store collector's fan-out runs", lines[0], StringComparison.Ordinal);
        Assert.Contains("MAX_PLANS_PER_QUERY / STALE_QUERY_THRESHOLD_DAYS (currently 200 / 21)", lines[1], StringComparison.Ordinal);
        /* The fixture's mode is AUTO, so the sentence must say the workload is the remaining lever — and
           must NOT still be asking the reader to go and read a mode this row already carries. */
        Assert.Contains("it is already AUTO", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("not collected", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("#3796", lines[1], StringComparison.Ordinal);
    }

    /// <summary>
    /// The capture-mode clause is a FUNCTION of the mode on the row, and every mode the engine can be in
    /// gets its own answer. Pinned as a table rather than one example, because the failure this guards is a
    /// clause that reads correctly for ALL and says the same thing for AUTO — a right-sounding sentence
    /// under the wrong evidence.
    /// </summary>
    [Theory]
    [InlineData("ALL", "AUTO is the fix")]
    [InlineData("AUTO", "it is already AUTO")]
    [InlineData("CUSTOM", "operator-set thresholds")]
    [InlineData("NONE", "no NEW query is being captured")]
    [InlineData("SOMETHING_ELSE", "not one of the four modes")]
    public void CaptureModeClause_AnswersEachModeInItsOwnTerms(string mode, string expected)
    {
        var clause = QueryStoreClutter.CaptureModeClause(Config("beta", captureMode: mode));
        Assert.Contains(expected, clause, StringComparison.Ordinal);
    }

    [Fact]
    public void CaptureModeClause_CallsANullModeNeverAsked_AndNeverNone()
    {
        var preRung = QueryStoreClutter.CaptureModeClause(Config("beta", captureMode: null));
        Assert.Contains("never asked", preRung, StringComparison.Ordinal);
        Assert.Contains("#3796", preRung, StringComparison.Ordinal);
        Assert.DoesNotContain("NONE", preRung, StringComparison.Ordinal);

        /* No config row at all is a THIRD state — not a null mode and not a mode — and says so. */
        var noConfig = QueryStoreClutter.CaptureModeClause(null);
        Assert.Contains("no query_store_health capture", noConfig, StringComparison.Ordinal);
    }

    /// <summary>The mode steers the wording and never the band: the same arms under ALL and under AUTO
    /// must reach the same verdict and the same reason tokens.</summary>
    [Fact]
    public void CaptureMode_DoesNotMoveTheVerdict()
    {
        var all = QueryStoreClutter.Judge(ReadCost("beta", 10, 8, 90), Churn("beta", 6), Config("beta", captureMode: "ALL"));
        var auto = QueryStoreClutter.Judge(ReadCost("beta", 10, 8, 90), Churn("beta", 6), Config("beta", captureMode: "AUTO"));
        var unread = QueryStoreClutter.Judge(ReadCost("beta", 10, 8, 90), Churn("beta", 6), Config("beta", captureMode: null));

        Assert.Equal(all.Verdict, auto.Verdict);
        Assert.Equal(all.Verdict, unread.Verdict);
        Assert.Equal(all.Reasons, auto.Reasons);
        Assert.Equal(all.Reasons, unread.Reasons);
    }

    /* ─────────────────────────── the surface ─────────────────────────── */

    [Fact]
    public void ToolSurface_IsExactlyTheClutterRead_StaticAndStringReturning()
    {
        var tools = typeof(DarlingMcpQueryStoreClutterTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToArray();

        Assert.Equal(new[] { ToolName }, tools.Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name).ToArray());
        Assert.NotNull(typeof(DarlingMcpQueryStoreClutterTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(tools, m => Assert.True(m.IsStatic));
        Assert.All(tools, m => Assert.Equal(typeof(Task<string>), m.ReturnType));
    }

    [Fact]
    public void Parameters_AreTheWindowedPageDialect_AllOptional_WithTheFleetKnobOffByDefault()
    {
        var parameters = Tool().GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .ToArray();

        Assert.Equal(new[] { "server_name", "hours_back", "limit", "include_fleet_median", "as_of" }, parameters.Select(p => p.Name).ToArray());
        Assert.All(parameters, p => Assert.True(p.HasDefaultValue, $"{p.Name} must be optional"));
        Assert.Equal(24, parameters.Single(p => p.Name == "hours_back").DefaultValue);
        Assert.Equal(DarlingMcpQueryStoreClutterTools.DefaultLimit, parameters.Single(p => p.Name == "limit").DefaultValue);
        Assert.False((bool)parameters.Single(p => p.Name == "include_fleet_median").DefaultValue!);
        Assert.Equal(McpHelpers.AsOfDescription, parameters.Single(p => p.Name == "as_of").GetCustomAttribute<DescriptionAttribute>()!.Description);
    }

    [Fact]
    public void Description_TeachesTheArms_TheReplicaRule_TheExclusions_CaptureMode_AndTheWindowFloor()
    {
        var description = Tool().GetCustomAttribute<DescriptionAttribute>()!.Description;

        /* the three per-database arms and the one per-server block, by their payload names */
        foreach (var key in new[] { "read_cost", "plan_churn", "config", "qs_overhead", "slowest_share_pct", "dominance_ratio", "plans_per_query_p95", "new_plans_per_day", "never_seen_twice_fraction", "wait_ms_per_hour", "memory_clerk" })
        {
            Assert.Contains(key, description, StringComparison.Ordinal);
        }

        /* the replica rule with the reason token and the bit */
        Assert.Contains("excluded_reason: qs_read_only_replica", description, StringComparison.Ordinal);
        Assert.Contains("bit 8", description, StringComparison.Ordinal);
        Assert.Contains("never a defect", description, StringComparison.Ordinal);

        /* the excluded sleep waits, each by name */
        foreach (var w in QueryStoreClutter.ExcludedQdsWaitTypes)
        {
            Assert.Contains(w, description, StringComparison.Ordinal);
        }

        Assert.Contains("excluded_wait_types", description, StringComparison.Ordinal);

        /* capture mode: the column, its four values, its flag, and what a null there means */
        Assert.Contains("query_capture_mode", description, StringComparison.Ordinal);
        Assert.Contains("ALL / AUTO / CUSTOM / NONE", description, StringComparison.Ordinal);
        Assert.Contains("capture_mode_known", description, StringComparison.Ordinal);
        Assert.Contains("never NONE", description, StringComparison.Ordinal);
        Assert.Contains("#3796", description, StringComparison.Ordinal);

        /* the page dialect and the window floor's shared clause */
        Assert.Contains("truncated is the page cut", description, StringComparison.Ordinal);
        Assert.Contains("databases_returned", description, StringComparison.Ordinal);
        Assert.EndsWith(McpHelpers.WindowTruncatedDescription, description);

        /* it is a windowed read, not a latest snapshot, and it must not borrow that family's words */
        Assert.DoesNotContain("LATEST IS A TIME", description, StringComparison.Ordinal);
        Assert.DoesNotContain("discontinuities[]", description, StringComparison.Ordinal);

        /* the vocabulary clash it names rather than inherits */
        Assert.Contains("dominance_ratio is NOT get_collection_health's dominance", description, StringComparison.Ordinal);
    }

    [Fact]
    public void CaptureModeNote_NamesTheRung_AndWhatANullMeans()
    {
        Assert.Contains("#3796", DarlingMcpQueryStoreClutterTools.CaptureModeNote, StringComparison.Ordinal);
        Assert.Contains("V137", DarlingMcpQueryStoreClutterTools.CaptureModeNote, StringComparison.Ordinal);
        /* The whole job of this sentence: a null is an unasked question, and a reader who takes it for the
           engine's NONE has read a real mode out of an absence. */
        Assert.Contains("never asked, never NONE", DarlingMcpQueryStoreClutterTools.CaptureModeNote, StringComparison.Ordinal);
    }

    /* ─────────────────────────── the statements ─────────────────────────── */

    [Fact]
    public void EveryArm_TakesTheServerArray_AndBindsTheWindowAsParameters()
    {
        foreach (var sql in new[]
        {
            DarlingQueryStoreClutterReader.ReadCostSql,
            DarlingQueryStoreClutterReader.PlanChurnSql,
            DarlingQueryStoreClutterReader.ConfigSql,
            DarlingQueryStoreClutterReader.QdsWaitsSql,
        })
        {
            Assert.Contains("server_id = ANY($1::int[])", sql, StringComparison.Ordinal);
            Assert.Contains(">= $2", sql, StringComparison.Ordinal);
            Assert.Contains("<= $3", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("percentile_cont", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheReadCostArm_ReadsTheFanoutRollup_ForTheQueryStoreCollectorOnly_AndPoolsTheOthers()
    {
        var sql = DarlingQueryStoreClutterReader.ReadCostSql;
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("collector_name = $4", sql, StringComparison.Ordinal);
        Assert.Equal("query_store", DarlingQueryStoreClutterReader.CollectorName);
        Assert.Contains("fanout_item_count IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("duration_ms > 0", sql, StringComparison.Ordinal);
        /* #3502's share: the slowest item over the whole pass, as a discrete median */
        Assert.Contains("percentile_disc(0.5) WITHIN GROUP (ORDER BY 100.0 * slowest_item_ms / duration_ms)", sql, StringComparison.Ordinal);
        /* the others' pooled median: every run on which a DIFFERENT database was slowest, same server */
        Assert.Contains("AND r.slowest_item <> d.database_name", sql, StringComparison.Ordinal);
        Assert.Contains("ON  r.server_id = d.server_id", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlanChurnArm_ReadsTheRawTierByPlanId_OnTheCoveringIndexColumnsOnly()
    {
        var sql = DarlingQueryStoreClutterReader.PlanChurnSql;
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_query_store_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_plan_hash", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_plan_map", sql, StringComparison.Ordinal);
        /* the projection is exactly idx_query_store_stats_server_db_query_plan_time's columns */
        Assert.Contains("SELECT server_id, database_name, query_id, plan_id, collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("percentile_disc(0.95) WITHIN GROUP (ORDER BY plans)", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) FILTER (WHERE p.collections_seen = 1) AS plans_seen_once", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) FILTER (WHERE p.first_seen > d.first_collection)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheConfigArm_TakesTheNewestCapturePerDatabaseInsideTheWindow_AndProjectsItsStamp()
    {
        var sql = DarlingQueryStoreClutterReader.ConfigSql;
        Assert.Contains("SELECT DISTINCT ON (server_id, database_name)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_store_health", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, database_name, capture_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time\n", sql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        /* not a MAX(capture_time) anchor: the newest capture is per database, not per server */
        Assert.DoesNotContain("MAX(capture_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWaitArm_ReadsStoredDeltas_RatesOnlyMeasuredRows_AndTakesTheLiteralPrefix()
    {
        var sql = DarlingQueryStoreClutterReader.QdsWaitsSql;
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains(@"wait_type LIKE 'QDS\_%'", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_wait_time_ms) FILTER (WHERE sample_interval_seconds > 0)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(sample_interval_seconds) FILTER (WHERE sample_interval_seconds > 0)", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) FILTER (WHERE sample_interval_seconds = 0)", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) FILTER (WHERE sample_interval_seconds IS NULL)", sql, StringComparison.Ordinal);
        /* no differencing here — the collector's deltas are read, never the cumulative counters */
        Assert.DoesNotContain("LAG(", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_time_ms)", sql.Replace("delta_wait_time_ms", "", StringComparison.Ordinal), StringComparison.Ordinal);
        /* and no division by the interval in SQL at all: the rate is the tool's quotient over the measured span */
        Assert.DoesNotContain("/", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheClerkRead_ReturnsTheNewestCaptureOfAnything_BesideTheClerksOwn()
    {
        var sql = DarlingQueryStoreClutterReader.QueryStoreClerkSql;
        Assert.Contains("FROM v_memory_clerks", sql, StringComparison.Ordinal);
        Assert.Contains("clerk_type = $4", sql, StringComparison.Ordinal);
        Assert.Equal("MEMORYCLERK_QUERYDISKSTORE", DarlingQueryStoreClutterReader.QueryStoreMemoryClerk);
        Assert.Contains("MAX(collection_time) AS latest_capture", sql, StringComparison.Ordinal);
        /* the raw literal strips the common indent, so the CTE body sits four in */
        Assert.Contains("ORDER BY collection_time DESC\n    LIMIT 1", sql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    [Fact]
    public void TheFleetRoster_IsEnabledSqlServerTargets_WithAnUnstampedKindAdmitted()
    {
        var sql = DarlingQueryStoreClutterReader.EnabledSqlServerTargetsSql;
        Assert.Contains("WHERE is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("(engine_kind IS NULL OR engine_kind = $1)", sql, StringComparison.Ordinal);
        Assert.Equal("sqlserver", MonitoredEngineKind.SqlServer);
    }

    /* ─────────────────────────── the registration sites ─────────────────────────── */

    [Fact]
    public void TheTool_IsRegisteredOnTheHost_AndServedOnTheWebSurface()
    {
        var host = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
        Assert.Contains(".WithGeminiCompatibleTools<DarlingMcpQueryStoreClutterTools>()", host, StringComparison.Ordinal);

        Assert.True(DarlingWebEndpoints.BuildReadDispatch().ContainsKey(ToolName));
        var descriptor = DarlingWebEndpoints.CatalogDescriptors[ToolName];
        Assert.Equal(new[] { "server", "hours", "limit", "include_fleet_median", "as_of" }, descriptor.Params.Select(p => p.Name).ToArray());
        Assert.All(descriptor.Params, p => Assert.False(p.Required));
        Assert.Contains("Replicas excluded by architecture", descriptor.Description, StringComparison.Ordinal);
    }

    [Fact]
    public void TheInstructions_CarryTheRow_AndTheCensusSentenceNamesTheTool()
    {
        var instructions = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpInstructions.cs");
        var row = Regex.Match(instructions, @"^\s*\| `get_query_store_clutter` \| (.+?) \| (.+?) \|\s*$", RegexOptions.Multiline);
        Assert.True(row.Success, "DarlingMcpInstructions.cs has no Tool Reference row for get_query_store_clutter");
        Assert.Contains("qs_read_only_replica", row.Groups[1].Value, StringComparison.Ordinal);
        Assert.Contains("capture_mode_known", row.Groups[1].Value, StringComparison.Ordinal);
        Assert.Contains("never `NONE`", row.Groups[1].Value, StringComparison.Ordinal);
        Assert.Contains("`include_fleet_median` (default false)", row.Groups[2].Value, StringComparison.Ordinal);
        Assert.Contains("`get_query_store_clutter` composes the Query Store clutter view", instructions, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLiteInventoryRatchet_NamesTheToolAsAPort_NotAnArchitecturalBoundary()
    {
        var ratchet = ReadRepoFile("Lite.Tests", "CrossAppMcpToolInventoryPinTests.cs");
        var at = ratchet.IndexOf("\"get_query_store_clutter\",", StringComparison.Ordinal);
        Assert.True(at > 0, "get_query_store_clutter is not on KnownLiteMissingMcpTools");
        var remark = ratchet[Math.Max(0, at - 2000)..at];
        Assert.Contains("Lite has EVERY input", remark, StringComparison.Ordinal);
        Assert.Contains("fanout_item_count / slowest_item / slowest_item_ms", remark, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDarlingReadme_DescribesTheTool_WithTheReplicaRuleAndTheCaptureMode()
    {
        var readme = ReadRepoFile("Darling", "README.md");
        Assert.Contains("`get_query_store_clutter` (#3797)", readme, StringComparison.Ordinal);
        Assert.Contains("excluded by architecture", readme, StringComparison.Ordinal);
        Assert.Contains("`query_capture_mode` is the V137 (#3796) column read verbatim", readme, StringComparison.Ordinal);
    }

    /// <summary>The tool body never names the process clock: an anchored read's only "now" is its anchor
    /// (<c>AsOfWindowAnchorTests</c>' rule, restated here because a second window — the fleet's — is read
    /// in the same body and must be the same window).</summary>
    [Fact]
    public void TheToolBody_ReadsTheFleetOverTheSameAnchoredWindow()
    {
        var source = ReadRepoFile(ToolSource.Split('/'));
        var body = source[source.IndexOf("public static async Task<string> GetQueryStoreClutter", StringComparison.Ordinal)..];
        Assert.DoesNotContain("DateTime.UtcNow", body, StringComparison.Ordinal);
        Assert.Contains("GetReadCostAsync(postgres, fleetIds, requestedStart, now)", body, StringComparison.Ordinal);
        Assert.Contains("GetPlanChurnAsync(postgres, fleetIds, requestedStart, now)", body, StringComparison.Ordinal);
        Assert.Contains("GetQdsWaitsAsync(postgres, fleetIds, requestedStart, now)", body, StringComparison.Ordinal);
        /* the page is observed, never inferred */
        Assert.Contains("McpHelpers.BoundPage(composed, limit)", body, StringComparison.Ordinal);
        Assert.DoesNotContain(">= limit", body, StringComparison.Ordinal);
    }
}
