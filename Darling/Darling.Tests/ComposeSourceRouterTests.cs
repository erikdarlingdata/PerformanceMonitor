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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="ComposeSourceRouter"/>: source selection by the window's oldest-point AGE (not display grain,
/// not window span), the CAGG dimension-coverage gate (post-reshape), and the margin-below-retention invariant.
/// Pure decision logic — no DB — so it runs ungated.
/// </summary>
public sealed class ComposeSourceRouterTests
{
    private static readonly DateTime Now = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Unspecified);

    private static ComposeMeasure Measure(string key) =>
        MeasureCatalog.Measures.First(m => string.Equals(m.Key, key, StringComparison.Ordinal));

    private static PanelPlan Plan(
        string measureKey,
        PanelMode mode = PanelMode.TimeSeries,
        IReadOnlyList<ComposeDimension>? groupBy = null) =>
        new()
        {
            Measure = Measure(measureKey),
            Unit = "ms",
            Mode = mode,
            Filters = Array.Empty<ComposeFilter>(),
            GroupBy = groupBy ?? Array.Empty<ComposeDimension>(),
            Viz = "line",
        };

    [Fact]
    public void RecentWindow_RoutesRaw()
    {
        /* oldest point 2 days old — inside the 3-day raw route max → raw. */
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-2), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Raw, route.Tier);
        Assert.False(route.IsCagg);
        Assert.Null(route.CaggRelation);
    }

    [Fact]
    public void MidWindow_RoutesHourlyCagg()
    {
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-10), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
        Assert.Equal("query_stats_hourly", route.CaggRelation);
    }

    [Fact]
    public void OldWindow_RoutesDailyCagg()
    {
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-120), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
        Assert.Equal("query_stats_daily", route.CaggRelation);
    }

    [Fact]
    public void HistoricalWindow_RoutesByAge_NotWindowSpan()
    {
        /* A 5-day-SPAN window that is 120→115 days OLD must route by age (120d → daily), NOT by span (5d → hourly):
           the hourly chunks for that range were already dropped, so span-based routing would return empty.
           (30 days was past the hourly horizon when this was written; since #1937 old means past 90.) */
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-120), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
    }

    [Fact]
    public void RankedMode_RoutesByAge_WithNoDisplayGrain()
    {
        /* Ranked panels resolve no display grain at all — the v1-killer case. Age-based routing still works:
           a 120-day "top N" reaches the daily CAGG instead of truncating at raw's 4 days. */
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us", PanelMode.Ranked), Now, Now.AddDays(-120), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
    }

    [Fact]
    public void NoCaggTable_AlwaysRaw()
    {
        /* wait_stats has no CAGG → raw even for a 40-day window (routing is a no-op for the ~30 non-CAGG tables). */
        var route = ComposeSourceRouter.Resolve(Plan("wait_time_ms"), Now, Now.AddDays(-40), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Raw, route.Tier);
    }

    [Fact]
    public void ObjectNameDimension_NowRoutes_ViaModuleMap()
    {
        /* query_stats object_name is a #1568 module join, but now coverable on the CAGG via module_map (the CAGG
           carries sql_handle) → it routes; the compiler joins module_map for the attribution. */
        var objectName = MeasureCatalog.Dimension("query_stats", "object_name")!;
        var plan = Plan("query_worker_us", groupBy: new[] { objectName });
        Assert.True(plan.UsesModuleJoin);
        var route = ComposeSourceRouter.Resolve(plan, Now, Now.AddDays(-120), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
        Assert.Equal("query_stats_daily", route.CaggRelation);
    }

    [Fact]
    public void CoveredDimension_QueryHash_Routes()
    {
        var queryHash = MeasureCatalog.Dimension("query_stats", "query_hash")!;
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us", groupBy: new[] { queryHash }), Now, Now.AddDays(-10), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
    }

    [Fact]
    public void ServerDimension_IsUniversallyCovered()
    {
        var server = MeasureCatalog.ServerDimension("query_stats");
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us", groupBy: new[] { server }), Now, Now.AddDays(-10), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
    }

    [Fact]
    public void ProcedureStats_SchemaName_NowRoutes()
    {
        /* schema_name was added to the procedure_stats CAGG in the reshape (#1624) → it now routes. */
        var schemaName = MeasureCatalog.Dimension("procedure_stats", "schema_name")!;
        var route = ComposeSourceRouter.Resolve(Plan("proc_worker_us", groupBy: new[] { schemaName }), Now, Now.AddDays(-10), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
        Assert.Equal("procedure_stats_hourly", route.CaggRelation);
    }

    [Fact]
    public void QueryStore_RoutesByComposerDims()
    {
        /* query_store_stats routes by module_name/query_hash (the reshaped composer dims), and since #1849
           the primary pair is the CORRECTED one — same dims, same column names, deduped values. */
        var route = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-10), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
        Assert.Equal("query_store_stats_corrected_hourly", route.CaggRelation);
    }

    [Fact]
    public void QueryStore_OldWindow_RoutesToDailyCagg()
    {
        /* A 120-day window routes to the daily tier, not the 90d-capped (#1937) hourly — and to the CORRECTED daily
           since #1849. With no coverage evidence the corrected pair wins: the legacy fallback is comparative
           and fires only where legacy is MEASURED to reach further back. */
        var route = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-120), RollupAvailability.All, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
        Assert.Equal("query_store_stats_corrected_daily", route.CaggRelation);
    }

    [Fact]
    public void RouteThresholds_StayBelowRetentionHorizons()
    {
        /* The safety invariant: a route max must be strictly inside its retention horizon, so a drop lagging the
           boundary can never leave the chosen tier missing its oldest chunk. Raw kept 4d, hourly history CAGGs
           kept 90d since #1937 — the horizon is read from the constant below rather than from this sentence. */
        Assert.True(ComposeSourceRouter.RawRouteMaxAge < TimeSpan.FromDays(4));
        Assert.True(ComposeSourceRouter.HourlyRouteMaxAge < TimescaleSupport.HourlyRetentionSpan);
    }

    /* ── #1665: availability gates the age decision — route to what the store HAS ── */

    /// <summary>
    /// The 42P01 shape: a plain-PostgreSQL store has no rollups at all, so every window — hourly-age or
    /// daily-age — must stay on raw, which plain PG never drops and which therefore holds the complete answer.
    /// Before #1665 the router chose <c>query_stats_hourly</c>/<c>_daily</c> by age alone and the compiled
    /// panel failed at run time against the missing relation.
    /// </summary>
    [Theory]
    [InlineData(10)]
    [InlineData(40)]
    public void OldWindow_NoRollupsInStore_RoutesRaw(int ageDays)
    {
        var route = ComposeSourceRouter.Resolve(
            Plan("query_worker_us"), Now, Now.AddDays(-ageDays), RollupAvailability.None, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Raw, route.Tier);
    }

    /// <summary>
    /// The flags are per TABLE: a failure-isolated ensure sweep can build one table's pair and not
    /// another's, and only the panels reading the missing pair may degrade. query_stats' hourly view gone →
    /// its panel falls to raw; a procedure_stats panel on the same store still routes to ITS hourly view.
    /// </summary>
    [Fact]
    public void HourlyMissing_DegradesOnlyTheTableThatLostIt()
    {
        var partial = RollupAvailability.All with { QueryGrainHourly = false };

        var queryRoute = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-10), partial, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Raw, queryRoute.Tier);

        var procedureRoute = ComposeSourceRouter.Resolve(Plan("proc_elapsed_us"), Now, Now.AddDays(-10), partial, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, procedureRoute.Tier);
        Assert.Equal("procedure_stats_hourly", procedureRoute.CaggRelation);

        /* query_store_stats now has TWO rollup families (#1849), so losing ONE hourly view no longer strands
           the table on raw — the corrected pair still answers. Clearing only the legacy hourly must therefore
           still route, and route to the CORRECTED view. */
        var qsLegacyGone = RollupAvailability.All with { QueryStoreGrainHourly = false };
        var qsLegacyGoneRoute = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-10), qsLegacyGone, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, qsLegacyGoneRoute.Tier);
        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, qsLegacyGoneRoute.CaggRelation);

        /* Both families' hourly views gone IS the degrade-to-raw case. */
        var qsPartial = RollupAvailability.All with { QueryStoreGrainHourly = false, QueryStoreCorrectedHourly = false };
        var qsRoute = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-10), qsPartial, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Raw, qsRoute.Tier);
    }

    /* ─────────────── the corrected / legacy Query Store boundary (#1849) ─────────────── */

    /// <summary>
    /// A store whose service predates the corrected rollups has none of them, and every Query Store window
    /// must keep routing to the ORIGINAL pair rather than compiling SQL against relations that do not exist.
    /// This is the #1664/#1665 per-tier degrade doing the job that lets #1849 ship without a schema migration
    /// or a viewer version gate: existence IS the probe.
    /// </summary>
    [Fact]
    public void QueryStore_CorrectedRollupsAbsent_RoutesToTheLegacyPair()
    {
        var old = RollupAvailability.WithoutCorrectedQueryStore;

        var hourly = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-10), old, RollupCoverage.Unknown);
        Assert.Equal(TimescaleSupport.QueryStoreStatsHourlyView, hourly.CaggRelation);

        var daily = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-120), old, RollupCoverage.Unknown);
        Assert.Equal(TimescaleSupport.QueryStoreStatsDailyView, daily.CaggRelation);
    }

    /// <summary>
    /// WATCHED (mutation): drop the legacy fallback and this goes red. The corrected rollups start EMPTY and
    /// deepen from deploy, while the original pair already holds up to 21 days of hourly and unbounded daily
    /// history. A window inside the corrected coverage must read the corrected (deduped) numbers; a window
    /// OLDER than the corrected rollups have materialized must fall back to the legacy pair, which is the only
    /// relation holding that history at all — inflated, and documented as such, but present.
    ///
    /// <para>The fallback is COMPARATIVE, never absolute: legacy wins only where it measurably reaches
    /// further back. A store where the corrected pair covers everything asked for must never silently prefer
    /// the inflated numbers, which the first assertion pins.</para>
    /// </summary>
    [Fact]
    public void QueryStore_BeyondCorrectedCoverage_FallsBackToTheLegacyPair()
    {
        /* Corrected materialized back 5 days; legacy back 120. */
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStoreStatsCorrectedHourlyView] = Now.AddDays(-5),
                [TimescaleSupport.QueryStoreStatsCorrectedDailyView] = Now.AddDays(-5),
                [TimescaleSupport.QueryStoreStatsHourlyView] = Now.AddDays(-120),
                [TimescaleSupport.QueryStoreStatsDailyView] = Now.AddDays(-120),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_store_stats"] = Now.AddDays(-4) });

        /* Inside the corrected coverage → corrected, deduped. */
        var inside = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-4), RollupAvailability.All, coverage);
        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, inside.CaggRelation);

        /* Past it → the legacy pair, which is the only tier holding that history. */
        var beyond = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-100), RollupAvailability.All, coverage);
        Assert.Equal(TimescaleSupport.QueryStoreStatsDailyView, beyond.CaggRelation);
    }

    /// <summary>
    /// The mirror case, and the one that keeps the fallback from becoming a regression: once the corrected
    /// rollups have been backfilled at least as deep as the legacy pair, NOTHING routes to the inflated
    /// relations any more. Without the comparative test — if the fallback fired merely because a window
    /// started below the corrected floor — a fully-backfilled store would still be served inflated numbers
    /// for its oldest windows, which is #1849 unfixed.
    /// </summary>
    [Fact]
    public void QueryStore_CorrectedRollupsBackfilled_NeverRoutesToTheInflatedPair()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStoreStatsCorrectedHourlyView] = Now.AddDays(-30),
                [TimescaleSupport.QueryStoreStatsCorrectedDailyView] = Now.AddDays(-30),
                [TimescaleSupport.QueryStoreStatsHourlyView] = Now.AddDays(-30),
                [TimescaleSupport.QueryStoreStatsDailyView] = Now.AddDays(-30),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_store_stats"] = Now.AddDays(-4) });

        foreach (var age in new[] { 4, 10, 25, 29 })
        {
            var route = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-age), RollupAvailability.All, coverage);
            Assert.True(
                route.CaggRelation is TimescaleSupport.QueryStoreStatsCorrectedHourlyView
                    or TimescaleSupport.QueryStoreStatsCorrectedDailyView,
                $"a {age}-day window routed to {route.CaggRelation}, but the corrected rollups cover it — an " +
                "equally-deep legacy pair must never win.");
        }
    }

    /* ─────────────── the DAY-grain daily (#1869) ─────────────── */

    /// <summary>Corrected back 120 days, day-grain back <paramref name="dayGrainDays"/>, legacy back 240 —
    /// the three-deep Query Store daily ladder with each rung's floor stated explicitly.</summary>
    private static RollupCoverage QueryStoreLadder(int dayGrainDays) => new(
        new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsCorrectedHourlyView] = Now.AddDays(-120),
            [TimescaleSupport.QueryStoreStatsCorrectedDailyView] = Now.AddDays(-120),
            [TimescaleSupport.QueryStoreStatsDayGrainDailyView] = Now.AddDays(-dayGrainDays),
            [TimescaleSupport.QueryStoreStatsHourlyView] = Now.AddDays(-240),
            [TimescaleSupport.QueryStoreStatsDailyView] = Now.AddDays(-240),
        },
        new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_store_stats"] = Now.AddDays(-4) });

    /// <summary>
    /// WATCHED (mutation): delete the day-grain preference and this goes red. The corrected daily sums each
    /// interval once per collection HOUR, so an interval straddling an hour boundary lands in it about twice
    /// (measured 1.97x on the live proof); the day-grain daily counts it once. Where the newer view has
    /// materialized the window, it is simply the more correct answer and must win.
    /// </summary>
    [Fact]
    public void QueryStore_DayGrainDailyCoversTheWindow_WinsOverTheCorrectedDaily()
    {
        var route = ComposeSourceRouter.Resolve(
            Plan("qs_executions"), Now, Now.AddDays(-100), RollupAvailability.All, QueryStoreLadder(dayGrainDays: 120));

        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
        Assert.Equal(TimescaleSupport.QueryStoreStatsDayGrainDailyView, route.CaggRelation);
    }

    /// <summary>
    /// WATCHED (mutation): make the preference absolute — prefer the day-grain daily whenever it EXISTS — and
    /// this goes red. It is a NEW aggregate that starts empty and deepens from deploy, so beyond its floor it
    /// holds nothing, and serving a window off it would return empty rows where the corrected daily still has
    /// the (slightly over-counted) history. Coverage beats correctness by exactly one rung, every time.
    /// </summary>
    [Fact]
    public void QueryStore_BeyondDayGrainCoverage_FallsBackToTheCorrectedDaily()
    {
        /* Every age here is past HourlyRouteMaxAge (89 days since #1937): the preference lives at the DAILY tier,
           so a younger window would be answered by the hourly rung and prove nothing about this ladder. */
        var coverage = QueryStoreLadder(dayGrainDays: 100);

        /* Inside the day-grain coverage → the exactly-counted view. */
        var inside = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-95), RollupAvailability.All, coverage);
        Assert.Equal(TimescaleSupport.QueryStoreStatsDayGrainDailyView, inside.CaggRelation);

        /* Past it but inside the corrected daily → the corrected daily, NOT the legacy pair below it. */
        var beyond = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-110), RollupAvailability.All, coverage);
        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedDailyView, beyond.CaggRelation);

        /* Past BOTH → the superseded daily, the only relation holding it. The full three-rung ladder. */
        var ancient = ComposeSourceRouter.Resolve(Plan("qs_executions"), Now, Now.AddDays(-300), RollupAvailability.All, coverage);
        Assert.Equal(TimescaleSupport.QueryStoreStatsDailyView, ancient.CaggRelation);
    }

    /// <summary>
    /// WATCHED (mutation): drop the <c>ReachesFurtherBack</c> arm of the day-grain preference — return the
    /// corrected daily whenever the day-grain one does not COVER the window — and this goes red.
    ///
    /// <para>The third outcome of that preference, and the only one the #1885 review found untested: neither
    /// daily holds the window, but the day-grain one is measurably DEEPER, so it wins on the same
    /// comparative rule the corrected/legacy step below it uses. Serving from the shallower relation when a
    /// deeper one exists returns fewer rows AND the over-counted ones, which is the worst of both.</para>
    ///
    /// <para>The ladder is built inline rather than from <see cref="QueryStoreLadder"/> because the shape
    /// this branch needs is the inverse of that one's: the day-grain daily has to out-reach the corrected
    /// daily, and the legacy pair must NOT out-reach the day-grain daily — otherwise the legacy comparison
    /// in <c>Resolve</c> takes the window back off it and the assertion would pass for the wrong reason.</para>
    /// </summary>
    [Fact]
    public void QueryStore_NeitherDailyCoversTheWindow_DeeperDayGrainStillWins()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStoreStatsCorrectedHourlyView] = Now.AddDays(-120),
                [TimescaleSupport.QueryStoreStatsCorrectedDailyView] = Now.AddDays(-120),
                [TimescaleSupport.QueryStoreStatsDayGrainDailyView] = Now.AddDays(-160),
                [TimescaleSupport.QueryStoreStatsHourlyView] = Now.AddDays(-140),
                [TimescaleSupport.QueryStoreStatsDailyView] = Now.AddDays(-140),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_store_stats"] = Now.AddDays(-4) });

        /* -200 days: past every floor on the ladder, so nothing COVERS it and only depth can decide. */
        var route = ComposeSourceRouter.Resolve(
            Plan("qs_executions"), Now, Now.AddDays(-200), RollupAvailability.All, coverage);

        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
        Assert.Equal(TimescaleSupport.QueryStoreStatsDayGrainDailyView, route.CaggRelation);
    }

    /// <summary>
    /// The residual is IRREDUCIBLE at the hourly grain — an interval genuinely collected across two hours has
    /// to appear in both — so #1869 buys nothing there and must change nothing there. An hourly-age window
    /// routes exactly where #1849 left it, whatever the day-grain daily has materialized.
    /// </summary>
    [Fact]
    public void QueryStore_HourlyAgeWindow_IsUntouchedByTheDayGrainDaily()
    {
        var route = ComposeSourceRouter.Resolve(
            Plan("qs_executions"), Now, Now.AddDays(-10), RollupAvailability.All, QueryStoreLadder(dayGrainDays: 120));

        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, route.CaggRelation);
    }

    /// <summary>
    /// A store whose service predates #1869 has the corrected rollups but no day-grain pair, and every Query
    /// Store daily window must keep routing to the corrected daily rather than compiling SQL against a
    /// relation that does not exist. The same existence-is-the-probe degrade that let #1849 ship with no
    /// schema migration, which is why #1869 needs none either.
    /// </summary>
    [Fact]
    public void QueryStore_DayGrainDailyAbsent_KeepsRoutingToTheCorrectedDaily()
    {
        var route = ComposeSourceRouter.Resolve(
            Plan("qs_executions"), Now, Now.AddDays(-100),
            RollupAvailability.WithoutDayGrainQueryStore, QueryStoreLadder(dayGrainDays: 120));

        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedDailyView, route.CaggRelation);
    }

    /// <summary>
    /// No coverage measured at all (a failed probe) moves nothing: null is "no evidence" everywhere else in
    /// this router, and a rollup that might be empty must not displace one that was already answering. This is
    /// also what keeps every pre-#1869 routing pin in this file honest rather than silently re-targeted.
    /// </summary>
    [Fact]
    public void QueryStore_NoCoverageEvidence_LeavesTheCorrectedDailyInPlace()
    {
        var route = ComposeSourceRouter.Resolve(
            Plan("qs_executions"), Now, Now.AddDays(-120), RollupAvailability.All, RollupCoverage.Unknown);

        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedDailyView, route.CaggRelation);
    }

    /// <summary>Daily-age window, daily view missing but hourly present: fall to the hourly view (capped at
    /// its 90-day horizon, #1937) — the same ladder the built-in tabs use, better than raw's 4 days.</summary>
    [Fact]
    public void DailyAgeWindow_DailyMissing_FallsToHourly()
    {
        var partial = RollupAvailability.All with { QueryGrainDaily = false };
        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-120), partial, RollupCoverage.Unknown);
        Assert.Equal(ComposeSourceTier.Hourly, route.Tier);
        Assert.Equal("query_stats_hourly", route.CaggRelation);
    }

    /* ─────────────── the FROM clause and stitch boundary a route carries ─────────────── */

    private static RollupCoverage QueryStatsHourlyStore(DateTime successorFloor) => new(
        new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsHourlyView] = Now.AddDays(-80),
            [TimescaleSupport.QueryStatsIntervalHourlyView] = successorFloor,
            [TimescaleSupport.QueryStatsDailyView] = Now.AddDays(-110),
        },
        new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_stats"] = Now.AddDays(-4) },
        RollupAvailability.All);

    /// <summary>The route carries the instant its stitched clause splits at, as a value, so the data-start probe
    /// reads the successor over the range the panel does. A route that reads one relation carries none.</summary>
    [Fact]
    public void AStitchedHourlyRoute_CarriesTheBoundaryItSplitsAt_AndAnUnstitchedOneCarriesNone()
    {
        var windowStart = Now.AddDays(-10);

        var stitched = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, windowStart, RollupAvailability.All, QueryStatsHourlyStore(Now.AddDays(-1)));
        Assert.Equal(ComposeSourceTier.Hourly, stitched.Tier);
        Assert.Equal(Now.AddDays(-1), stitched.StitchBoundaryUtc);
        Assert.Contains("UNION ALL", stitched.CaggFromClause, StringComparison.Ordinal);

        var covered = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, windowStart, RollupAvailability.All, QueryStatsHourlyStore(Now.AddDays(-80)));
        Assert.Equal(ComposeSourceTier.Hourly, covered.Tier);
        Assert.Null(covered.StitchBoundaryUtc);
        Assert.DoesNotContain("UNION ALL", covered.CaggFromClause, StringComparison.Ordinal);
    }

    /// <summary>The daily boundary is the first whole day after the successor hourly's first bucket, so the successor
    /// daily can hold the day before it, which the stitched read leaves to the superseded daily.</summary>
    [Fact]
    public void AStitchedDailyRoute_CarriesTheBoundaryItSplitsAt()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStatsHourlyView] = Now.AddDays(-400),
                [TimescaleSupport.QueryStatsDailyView] = Now.AddDays(-400),
                [TimescaleSupport.QueryStatsIntervalHourlyView] = Now.Date.AddDays(-100).AddHours(5),
                [TimescaleSupport.QueryStatsIntervalDailyView] = Now.Date.AddDays(-110),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_stats"] = Now.AddDays(-4) },
            RollupAvailability.All);

        var route = ComposeSourceRouter.Resolve(Plan("query_worker_us"), Now, Now.AddDays(-150), RollupAvailability.All, coverage);

        Assert.Equal(ComposeSourceTier.Daily, route.Tier);
        Assert.Equal(Now.Date.AddDays(-99), route.StitchBoundaryUtc);
        Assert.Contains("UNION ALL", route.CaggFromClause, StringComparison.Ordinal);
    }

    /// <summary>The day-grain daily has no successor, so its clause names the one relation. The data-start probe reads
    /// the relations a panel read off the clause, so it must carry one like every other rollup route.</summary>
    [Fact]
    public void TheDayGrainDailyRoute_CarriesItsFromClause_SoTheDataStartProbeReadsIt()
    {
        var route = ComposeSourceRouter.Resolve(
            Plan("qs_executions"), Now, Now.AddDays(-100), RollupAvailability.All, QueryStoreLadder(dayGrainDays: 120));

        Assert.Equal(TimescaleSupport.QueryStoreStatsDayGrainDailyView, route.CaggRelation);
        Assert.Equal($"collect.{TimescaleSupport.QueryStoreStatsDayGrainDailyView} AS f", route.CaggFromClause);
        Assert.Null(route.StitchBoundaryUtc);

        var source = Assert.Single(ComposeStoreAvailability.DataStartSources("query_store_stats", route));
        Assert.Equal(TimescaleSupport.QueryStoreStatsDayGrainDailyView, source.Relation);
        Assert.Null(source.LowerBoundUtc);
    }

    [Fact]
    public void EveryRollupRoute_CarriesTheFromClauseItReads()
    {
        var coverages = new[]
        {
            RollupCoverage.Unknown,
            QueryStoreLadder(dayGrainDays: 120),
            QueryStatsHourlyStore(Now.AddDays(-1)),
            QueryStatsHourlyStore(Now.AddDays(-80)),
        };

        foreach (var plan in new[] { Plan("query_worker_us"), Plan("qs_executions") })
        {
            foreach (var coverage in coverages)
            {
                foreach (var age in new[] { 10, 40, 100, 150, 300 })
                {
                    var route = ComposeSourceRouter.Resolve(plan, Now, Now.AddDays(-age), RollupAvailability.All, coverage);

                    if (route.IsCagg)
                    {
                        Assert.False(string.IsNullOrEmpty(route.CaggFromClause), $"a {age}-day window routed to {route.CaggRelation} with no FROM clause");
                    }
                }
            }
        }
    }
}
