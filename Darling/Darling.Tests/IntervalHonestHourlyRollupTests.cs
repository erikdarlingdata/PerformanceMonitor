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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (Q12, item 16's rule-5 roster): the three INTERVAL-HONEST hourly rollups over the two SQL Server delta
/// families — <c>query_stats_interval_hourly</c>, <c>procedure_stats_interval_hourly</c>,
/// <c>query_stats_db_interval_hourly</c> — and everything that has to be true for them to be read.
///
/// <para><b>The lie they retire.</b> A restart writes a (0, 0) row — every delta 0 beside
/// <c>sample_interval_seconds = 0</c>, the collector's own "not knowable" verdict — and the legacy hourly
/// rollups aggregate it: <c>count(*) AS sample_count</c> counts it as a sample and <c>min(delta_*)</c> reads it as
/// a real minimum. The successors carry the verdict in their WHERE, so the row produces nothing, and carry the
/// measured interval as a per-bucket sum. The live test below plants the row and measures both readings.</para>
///
/// <para><b>What differs from #3698's baseline pair, and is pinned here because it is the load-bearing
/// decision.</b> The legacy trio does NOT leave the registry: each has an indefinite daily tier hierarchical
/// from it, so it goes on being created, refreshed and phased, and the successors are APPENDED. That is what
/// re-derived the phase grid (pinned with its derivation in TimescaleSupportTests), filled the daily
/// compression band (TimescaleAggregateCompressionTests), and made the reader-side choice a per-window rule
/// rather than a retirement: <see cref="RollupCoverage.HourlyRelationFor"/>, over
/// <see cref="TimescaleSupport.PrefersSuccessor"/> — the ONE supply rule the baseline provider already applied,
/// now shared rather than copied.</para>
/// </summary>
public sealed class IntervalHonestHourlyRollupTests
{
    private static readonly DateTime Now = new(2026, 9, 19, 12, 0, 0, DateTimeKind.Unspecified);

    private static DateTime DaysAgo(int days) => Now.AddDays(-days);

    /* ─────────────────────────── the registry ─────────────────────────── */

    /// <summary>
    /// Three pairs, each legacy REGISTERED (unlike #3698's pair), each successor registered and appended after
    /// the corrected Query Store pair, each dependent daily registered and hierarchical from the LEGACY. The
    /// structural facts the "stays registered" reasoning rests on, asserted against the shipped lists and the
    /// shipped CREATE text rather than restated.
    /// </summary>
    [Fact]
    public void ThreePairs_LegacyStaysRegistered_SuccessorAppended_DailyHangsOffTheLegacy()
    {
        var hourly = TimescaleSupport.HourlyAggregates.Select(a => a.View).ToArray();
        var daily = TimescaleSupport.DailyAggregates.Select(a => a.View).ToHashSet(StringComparer.Ordinal);

        Assert.Equal(3, TimescaleSupport.SupersededHourlyRollups.Length);
        Assert.Equal(9, hourly.Length);

        foreach (var (legacy, successor, dependentDaily) in TimescaleSupport.SupersededHourlyRollups)
        {
            Assert.Contains(legacy, hourly);
            Assert.Contains(successor, hourly);
            Assert.NotEqual(legacy, successor);
            Assert.True(Array.IndexOf(hourly, successor) > Array.IndexOf(hourly, TimescaleSupport.QueryStoreStatsCorrectedHourlyView),
                $"{successor} must be appended after the corrected Query Store pair, not inserted beside its legacy — an insertion re-deals the bounded band positions");
            Assert.Equal(successor, TimescaleSupport.SuccessorOf(legacy));
            Assert.Null(TimescaleSupport.SuccessorOf(successor));

            Assert.Contains(dependentDaily, daily);
            var dailyCreate = TimescaleSupport.DailyAggregates.Single(a => a.View == dependentDaily).CreateSql;
            Assert.Contains($"FROM collect.{legacy}", dailyCreate, StringComparison.Ordinal);
            Assert.DoesNotContain($"FROM collect.{successor}", dailyCreate, StringComparison.Ordinal);
        }

        /* The Query Store family and the successors themselves were never superseded. */
        Assert.Null(TimescaleSupport.SuccessorOf(TimescaleSupport.QueryStoreStatsHourlyView));
        Assert.Null(TimescaleSupport.SuccessorOf(TimescaleSupport.QueryStoreStatsIntervalHourlyView));
        Assert.Null(TimescaleSupport.SuccessorOf(TimescaleSupport.QueryStoreStatsCorrectedHourlyView));

        /* Neither list's names appear in the other's: a legacy here is registered, a legacy there is not. */
        var baselineLegacies = TimescaleSupport.SupersededBaselineRelations.Select(s => s.Legacy).ToHashSet(StringComparer.Ordinal);
        Assert.Empty(TimescaleSupport.SupersededHourlyRollups.Select(s => s.Legacy).Intersect(baselineLegacies));
        Assert.Empty(TimescaleSupport.SupersededHourlyRollups.Select(s => s.Legacy).Intersect(TimescaleSupport.RetiredBaselineRelations));
    }

    /// <summary>
    /// Every successor is a raw-sourced hourly rollup registered where the readers, the coverage probe, the
    /// backfill and the retention ladder look — <see cref="TimescaleSupport.RollupViews"/>,
    /// <see cref="TimescaleSupport.RollupProbeSql"/>, <see cref="RollupBackfill.Targets"/>,
    /// <see cref="TimescaleSupport.RetentionPolicies"/> — and, since #3653's LC froze the legacy trio off the raw
    /// purge, the two with a <see cref="TimescaleSupport.RawTierCoverage"/> row of their own
    /// (<c>query_stats</c>, <c>procedure_stats</c>) are there too — the #1661 precedent the registry's summary
    /// explains is now reversed BY DESIGN. <c>query_stats_db_hourly</c> shares <c>query_stats</c>' raw table
    /// rather than owning one, so its successor was never a candidate and stays out, same as before LC.
    /// </summary>
    [Fact]
    public void EverySuccessor_IsInTheCoverageProbe_TheBackfillPlan_TheRetentionLadder_AndTheRawGateWhereItHasOne()
    {
        foreach (var (legacy, successor, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            var legacyRow = TimescaleSupport.RollupViews.Single(r => r.View == legacy);
            var successorRow = TimescaleSupport.RollupViews.Single(r => r.View == successor);
            Assert.Equal(legacyRow.RawTable, successorRow.RawTable);
            Assert.Equal(legacyRow.Source, successorRow.Source);
            Assert.Equal("collection_time", successorRow.SourceTimeColumn);
            Assert.Equal(TimescaleSupport.HourlyBucket, successorRow.BucketWidth);
            Assert.Equal(legacyRow.RawTable, RollupCoverage.RawTableFor(successor));

            Assert.Contains($"to_regclass('collect.{successor}') IS NOT NULL", TimescaleSupport.RollupProbeSql, StringComparison.Ordinal);
            Assert.True(RollupAvailability.All.Has(successor));
            Assert.False(RollupAvailability.WithoutIntervalHourlies.Has(successor));
            Assert.True(RollupAvailability.WithoutIntervalHourlies.Has(legacy));

            /* Depth 0 in the backfill order — raw-sourced — and the probe SQL reads its own name. The legacy's
               OWN daily left the backfill plan with the rest of the frozen six (#3653 LC: nothing ever advances
               its watermark again); the successor's consumer there is now its own successor daily (#3653 LB),
               read through SuccessorDailyOf rather than restated. */
            var target = RollupBackfill.Targets.Single(t => t.View == successor);
            Assert.False(target.IsHierarchical);
            var successorDaily = TimescaleSupport.SuccessorDailyOf(successor)
                ?? throw new InvalidOperationException($"{successor} must be in {nameof(TimescaleSupport.SupersededDailyRollups)}.");
            Assert.True(Array.IndexOf(RollupBackfill.Targets, target) < Array.IndexOf(RollupBackfill.Targets, RollupBackfill.Targets.Single(t => t.View == successorDaily)),
                "a raw-sourced successor must be planned before its own successor daily");

            /* The hourly tier's horizon; coverage is the successor's OWN daily (#3653 LC), not the legacy daily
               SupersededHourlyRollups' third element still names for FrozenRollupAggregates and the coverage
               log to read — see RetentionPolicies' summary for why the legacy daily cannot gate this. */
            var policy = TimescaleSupport.RetentionPolicies.Single(p => p.Relation == successor);
            Assert.Equal(TimescaleSupport.HourlyRetentionInterval, policy.DropAfter);
            Assert.Equal("bucket", policy.TimeColumn);
            Assert.Equal(new[] { successorDaily }, policy.Coverage);
        }

        /* The raw purge moved onto the two successors with a raw table of their own naming them (#3653 LC).
           query_stats_db_hourly has no RawTierCoverage row of its own — it shares "query_stats" with
           query_stats_hourly, already that row's named consumer below — so it was never a raw-gate candidate. */
        var querySuccessor = TimescaleSupport.SuccessorOf(TimescaleSupport.QueryStatsHourlyView)
            ?? throw new InvalidOperationException($"{TimescaleSupport.QueryStatsHourlyView} must be in {nameof(TimescaleSupport.SupersededHourlyRollups)}.");
        var procedureSuccessor = TimescaleSupport.SuccessorOf(TimescaleSupport.ProcedureStatsHourlyView)
            ?? throw new InvalidOperationException($"{TimescaleSupport.ProcedureStatsHourlyView} must be in {nameof(TimescaleSupport.SupersededHourlyRollups)}.");
        var dbSuccessor = TimescaleSupport.SuccessorOf(TimescaleSupport.QueryStatsDbHourlyView)
            ?? throw new InvalidOperationException($"{TimescaleSupport.QueryStatsDbHourlyView} must be in {nameof(TimescaleSupport.SupersededHourlyRollups)}.");
        Assert.Equal(new[] { querySuccessor }, TimescaleSupport.RawTierCoverage.Single(t => t.Relation == "query_stats").Coverage);
        Assert.Equal(new[] { procedureSuccessor }, TimescaleSupport.RawTierCoverage.Single(t => t.Relation == "procedure_stats").Coverage);
        Assert.All(TimescaleSupport.RawTierCoverage, tier => Assert.DoesNotContain(dbSuccessor, tier.Coverage));

        Assert.False(RollupAvailability.WithoutIntervalHourlies.AllPresent);
        Assert.True(RollupAvailability.All.AllPresent);
        /* 16 through #3653 Q12, +3 for the A6 successor DAILIES this lane registers
           (query_stats_interval_daily, procedure_stats_interval_daily, query_stats_db_interval_daily). */
        Assert.Equal(19, TimescaleSupport.RollupViews.Length);
    }

    /// <summary>
    /// The successor is the legacy's shape plus the verdict: same FROM, same GROUP BY, every legacy select item
    /// under the same alias, the interval predicate in the WHERE, and <c>sum(sample_interval_seconds)</c>
    /// carried. The census (MeasurementContractCensusTests) pins the same thing through reflection over every
    /// CREATE constant; this is the direct statement over the three pairs, and it is what makes a reader's
    /// relation swap a swap of nothing else.
    /// </summary>
    [Fact]
    public void EachSuccessor_IsItsLegacysShape_PlusTheIntervalVerdict_AndTheCarriedIntervalSum()
    {
        foreach (var (legacy, successor, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            var legacyText = TimescaleSupport.HourlyAggregates.Single(a => a.View == legacy).CreateSql;
            var successorText = TimescaleSupport.HourlyAggregates.Single(a => a.View == successor).CreateSql;

            Assert.DoesNotContain("sample_interval_seconds IS DISTINCT FROM 0", legacyText, StringComparison.Ordinal);
            Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", successorText, StringComparison.Ordinal);
            Assert.Contains("sum(sample_interval_seconds) AS sample_interval_seconds_sum", successorText, StringComparison.Ordinal);
            Assert.DoesNotContain("sample_interval_seconds_sum", legacyText, StringComparison.Ordinal);

            /* Materialized-only on both, for the #1759 reason the legacy states: the coverage probes read
               min(bucket) as the materialized floor. */
            Assert.DoesNotContain("materialized_only", successorText, StringComparison.Ordinal);
            Assert.Contains("WITH NO DATA", successorText, StringComparison.Ordinal);
            Assert.StartsWith($"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.{successor}", successorText, StringComparison.Ordinal);

            Assert.Equal(
                Regex.Match(legacyText, @"\bFROM\s+collect\.(\w+)").Groups[1].Value,
                Regex.Match(successorText, @"\bFROM\s+collect\.(\w+)").Groups[1].Value);
            Assert.Equal(TimescaleSupport.RefreshGroupingTermsFor(legacyText), TimescaleSupport.RefreshGroupingTermsFor(successorText));

            foreach (Match item in Regex.Matches(legacyText, @"\b(\w+\([^)]*\)) AS (\w+)"))
            {
                Assert.Contains($"{item.Groups[1].Value} AS {item.Groups[2].Value}", successorText, StringComparison.Ordinal);
            }

            /* A legacy WHERE survives beside the verdict (the db-grain rollup's delta_worker_time IS NOT NULL). */
            var legacyWhere = Regex.Match(legacyText, @"\bWHERE\b(.*?)\bGROUP BY\b", RegexOptions.Singleline);
            if (legacyWhere.Success)
            {
                Assert.Contains(legacyWhere.Groups[1].Value.Trim(), successorText, StringComparison.Ordinal);
            }
        }

        /* The unbounded-cardinality classification follows the group key, so the query-grain successor is
           unbounded like its legacy and the other two are bounded like theirs. */
        Assert.True(TimescaleSupport.IsUnboundedCardinalityRefresh(TimescaleSupport.QueryStatsIntervalHourlyView));
        Assert.False(TimescaleSupport.IsUnboundedCardinalityRefresh(TimescaleSupport.ProcedureStatsIntervalHourlyView));
        Assert.False(TimescaleSupport.IsUnboundedCardinalityRefresh(TimescaleSupport.QueryStatsDbIntervalHourlyView));
    }

    /* ─────────────────────────── the supply rule ─────────────────────────── */

    /// <summary>
    /// The one rule, in the shared home, walked case by case — and the baseline provider's alias agrees with
    /// it at every case, which is what "not duplicated" means here.
    /// </summary>
    [Fact]
    public void PrefersSuccessor_IsSharedWithTheBaselineProvider_AndAnswersEveryCase()
    {
        var cases = new (bool LegacyExists, DateTime? LegacyOldest, DateTime? SuccessorOldest, DateTime WindowStart, bool Expected)[]
        {
            /* Legacy absent or empty: successor, whatever it holds. */
            (false, null, null, DaysAgo(30), true),
            (true, null, DaysAgo(2), DaysAgo(30), true),
            /* Empty successor never wins against a legacy with rows. */
            (true, DaysAgo(60), null, DaysAgo(30), false),
            /* Successor reaches the window's start: it covers everything the window asks. */
            (true, DaysAgo(60), DaysAgo(30), DaysAgo(30), true),
            (true, DaysAgo(60), DaysAgo(31), DaysAgo(30), true),
            /* Successor short of a window the legacy reaches: legacy. */
            (true, DaysAgo(60), DaysAgo(20), DaysAgo(30), false),
            /* Neither reaches the window but the successor reaches as far as the legacy does: successor —
               the legacy has nothing the successor lacks. */
            (true, DaysAgo(10), DaysAgo(10), DaysAgo(30), true),
            (true, DaysAgo(10), DaysAgo(9), DaysAgo(30), false),
            (true, DaysAgo(9), DaysAgo(10), DaysAgo(30), true),
        };

        foreach (var (legacyExists, legacyOldest, successorOldest, windowStart, expected) in cases)
        {
            Assert.Equal(expected, TimescaleSupport.PrefersSuccessor(legacyExists, legacyOldest, successorOldest, windowStart));
            Assert.Equal(expected, PgBaselineProvider.PrefersSuccessor(legacyExists, legacyOldest, successorOldest, windowStart));
        }
    }

    /// <summary>
    /// <see cref="RollupCoverage.HourlyRelationFor"/>: the rule applied to a store's floors, with the two
    /// guards the rule alone does not have — an absent successor is never named, and a view that was never
    /// superseded comes back as itself.
    /// </summary>
    [Fact]
    public void HourlyRelationFor_PicksTheSuccessorByCoverage_NeverNamesAnAbsentOne_AndLeavesTheQueryStoreFamilyAlone()
    {
        var legacy = TimescaleSupport.QueryStatsHourlyView;
        var successor = TimescaleSupport.QueryStatsIntervalHourlyView;

        /* No evidence at all: the legacy, which every store has. */
        Assert.Equal(legacy, RollupCoverage.Unknown.HourlyRelationFor(legacy, DaysAgo(10)));

        /* The two-argument constructor (every pre-#3653 caller) carries no availability, so it is the legacy too
           even with a deeper successor floor on file — naming a relation coverage alone cannot prove present
           is the 42P01 this guard exists for. */
        var noAvailability = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [legacy] = DaysAgo(5), [successor] = DaysAgo(40) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal));
        Assert.Equal(legacy, noAvailability.HourlyRelationFor(legacy, DaysAgo(10)));

        /* A store on a service that predates the successors: legacy, even when the legacy is EMPTY (where the
           bare rule would answer "successor"). */
        var older = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.WithoutIntervalHourlies);
        Assert.Equal(legacy, older.HourlyRelationFor(legacy, DaysAgo(10)));

        /* Upgraded store, day one: legacy deep, successor holding its first refresh. A 10-day window goes to
           the legacy; a window inside the successor's reach goes to the successor. */
        var dayOne = Coverage(legacyFloor: DaysAgo(80), successorFloor: DaysAgo(1));
        Assert.Equal(legacy, dayOne.HourlyRelationFor(legacy, DaysAgo(10)));
        Assert.Equal(successor, dayOne.HourlyRelationFor(legacy, DaysAgo(1)));
        Assert.Equal(successor, dayOne.HourlyRelationFor(legacy, Now.AddHours(-6)));

        /* Backfilled to raw's oldest, or ninety days on: the successor covers the tier. */
        var covered = Coverage(legacyFloor: DaysAgo(80), successorFloor: DaysAgo(80));
        Assert.Equal(successor, covered.HourlyRelationFor(legacy, DaysAgo(10)));
        Assert.Equal(successor, covered.HourlyRelationFor(legacy, DaysAgo(200)));

        /* Fresh store post-build: both empty, both present — the successor, which is the one that fills. */
        var fresh = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);
        Assert.Equal(successor, fresh.HourlyRelationFor(legacy, DaysAgo(10)));

        /* Every pair, not only the query-grain one. */
        foreach (var (pairLegacy, pairSuccessor, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            var eachCovered = new RollupCoverage(
                new Dictionary<string, DateTime>(StringComparer.Ordinal) { [pairLegacy] = DaysAgo(80), [pairSuccessor] = DaysAgo(80) },
                new Dictionary<string, DateTime>(StringComparer.Ordinal),
                RollupAvailability.All);
            Assert.Equal(pairSuccessor, eachCovered.HourlyRelationFor(pairLegacy, DaysAgo(10)));
            Assert.Equal(pairLegacy, RollupCoverage.Unknown.HourlyRelationFor(pairLegacy, DaysAgo(10)));
        }

        /* The Query Store family has no successor here and comes back as handed — the corrected pair is
           the compose router's own business (#1849). */
        Assert.Equal(TimescaleSupport.QueryStoreStatsHourlyView, covered.HourlyRelationFor(TimescaleSupport.QueryStoreStatsHourlyView, DaysAgo(10)));
        Assert.Equal(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, covered.HourlyRelationFor(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, DaysAgo(10)));

        Assert.Throws<ArgumentNullException>(() => covered.HourlyRelationFor(null!, DaysAgo(10)));
    }

    /// <summary>The hand-over milestone, pure: the successor's floor at or before the hourly tier's horizon.</summary>
    [Fact]
    public void SuccessorCoversHourlyTier_AtOrBeforeTheHorizon_NeverWhenEmpty()
    {
        var horizon = Now - TimescaleSupport.HourlyRetentionSpan;
        Assert.False(TimescaleSupport.SuccessorCoversHourlyTier(null, Now));
        Assert.False(TimescaleSupport.SuccessorCoversHourlyTier(horizon.AddSeconds(1), Now));
        Assert.True(TimescaleSupport.SuccessorCoversHourlyTier(horizon, Now));
        Assert.True(TimescaleSupport.SuccessorCoversHourlyTier(horizon.AddDays(-1), Now));
        Assert.Equal(TimeSpan.FromDays(90), TimescaleSupport.HourlyRetentionSpan);
    }

    /* ─────────────────────────── the readers ─────────────────────────── */

    /// <summary>
    /// Every hourly-tier reader resolves its relation through the rule, AFTER the tier is decided over the
    /// legacy pair: the compose router, the MCP trend route, the keyed query history, the daily summary and
    /// the FinOps workload readers. Each is exercised at both answers so the swap is shown to be a swap of the
    /// relation and nothing else.
    /// </summary>
    [Fact]
    public void EveryHourlyTierReader_TakesTheSuccessorWhereItCovers_AndTheLegacyWhereItDoesNot()
    {
        var legacy = TimescaleSupport.QueryStatsHourlyView;
        var successor = TimescaleSupport.QueryStatsIntervalHourlyView;
        var windowStart = DaysAgo(10);

        /* The two store shapes: day one (successor shallow) and covered. On both the TIER is Hourly, decided
           over the legacy's 80-day floor; only the relation differs. */
        var dayOne = Coverage(legacyFloor: DaysAgo(80), successorFloor: DaysAgo(1));
        var covered = Coverage(legacyFloor: DaysAgo(80), successorFloor: DaysAgo(80));

        /* Compose. */
        var plan = ComposePlan("query_worker_us");
        var dayOneRoute = ComposeSourceRouter.Resolve(plan, Now, windowStart, RollupAvailability.All, dayOne);
        var coveredRoute = ComposeSourceRouter.Resolve(plan, Now, windowStart, RollupAvailability.All, covered);
        Assert.Equal(ComposeSourceTier.Hourly, dayOneRoute.Tier);
        Assert.Equal(legacy, dayOneRoute.CaggRelation);
        Assert.Equal(ComposeSourceTier.Hourly, coveredRoute.Tier);
        Assert.Equal(successor, coveredRoute.CaggRelation);
        /* An older service's store: the successor is absent and the compose route never names it. The
           coverage is probed WITH that availability, as ComposeStoreAvailability probes both in one cycle — a
           coverage carrying a floor for a relation the availability says is absent is not a store shape. */
        var olderService = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [legacy] = DaysAgo(80), [TimescaleSupport.QueryStatsDailyView] = DaysAgo(110) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_stats"] = DaysAgo(4) },
            RollupAvailability.WithoutIntervalHourlies);
        Assert.Equal(legacy, ComposeSourceRouter.Resolve(plan, Now, windowStart, RollupAvailability.WithoutIntervalHourlies, olderService).CaggRelation);
        Assert.Equal(legacy, DarlingTrendReader.ResolveQueryDurationTrendRoute(windowStart, RollupAvailability.WithoutIntervalHourlies, olderService, Now).HourlyView);

        /* The MCP duration-trend route carries the resolved relation. */
        var dayOneTrend = DarlingTrendReader.ResolveQueryDurationTrendRoute(windowStart, RollupAvailability.All, dayOne, Now);
        var coveredTrend = DarlingTrendReader.ResolveQueryDurationTrendRoute(windowStart, RollupAvailability.All, covered, Now);
        Assert.Equal(RetentionTier.Hourly, dayOneTrend.Tier);
        Assert.Equal(legacy, dayOneTrend.HourlyView);
        Assert.Equal(legacy, dayOneTrend.Relation);
        Assert.Equal(RetentionTier.Hourly, coveredTrend.Tier);
        Assert.Equal(successor, coveredTrend.HourlyView);
        Assert.Equal(successor, coveredTrend.Relation);
        /* The tier's coverage on the route is the LEGACY pair's — the deeper one the tier was decided over. */
        Assert.Equal(DaysAgo(80), coveredTrend.Coverage.HourlyFloorUtc);

        var coveredProcedure = DarlingTrendReader.ResolveProcedureDurationTrendRoute(
            windowStart, RollupAvailability.All,
            new RollupCoverage(
                new Dictionary<string, DateTime>(StringComparer.Ordinal)
                {
                    [TimescaleSupport.ProcedureStatsHourlyView] = DaysAgo(80),
                    [TimescaleSupport.ProcedureStatsIntervalHourlyView] = DaysAgo(80),
                },
                new Dictionary<string, DateTime>(StringComparer.Ordinal),
                RollupAvailability.All),
            Now);
        Assert.Equal(TimescaleSupport.ProcedureStatsIntervalHourlyView, coveredProcedure.HourlyView);

        /* The hourly trend SQL for the successor is the builder over the successor's name and differs from the
           legacy text in that name alone. */
        var legacySql = DurationTrendRouting.BuildHourlyTrendSql(legacy, withDatabaseFilter: false);
        var successorSql = DurationTrendRouting.BuildHourlyTrendSql(successor, withDatabaseFilter: false);
        Assert.Equal(legacySql.Replace(legacy, successor, StringComparison.Ordinal), successorSql);

        /* #3897: the MCP reader's hourly constant is the BUCKETED builder over the legacy view, and that builder
           swaps the relation the same way — the successor's text differs in the name alone. */
        var bucketedLegacy = DurationTrendRouting.BuildBucketedHourlyTrendSql(legacy);
        Assert.Equal(DarlingTrendReader.QueryDurationTrendHourlySql, bucketedLegacy);
        Assert.Equal(bucketedLegacy.Replace(legacy, successor, StringComparison.Ordinal), DurationTrendRouting.BuildBucketedHourlyTrendSql(successor));

        /* The keyed query history: the constant is the builder over the legacy, and the builder swaps FROM. */
        Assert.Equal(DarlingTrendReader.QueryHistoryHourlySqlFor(legacy), DarlingTrendReader.QueryHistoryHourlySql);
        Assert.Equal(
            DarlingTrendReader.QueryHistoryHourlySql.Replace($"FROM {legacy}", $"FROM {successor}", StringComparison.Ordinal),
            DarlingTrendReader.QueryHistoryHourlySqlFor(successor));
        Assert.Contains($"FROM {successor}", DarlingTrendReader.QueryHistoryHourlySqlFor(successor), StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => DarlingTrendReader.QueryHistoryHourlySqlFor(" "));

        /* The daily summary: the one-argument form is the legacy; the two-argument form swaps the queries CTE's
           relation — and, since #3653 A6, carries the successor's own WHERE into the not-carried member's
           source probe (the hole scan's rule: a day holding only restart rows is not a hole for an aggregate
           that rejects them) — and nothing else; the daily tier ignores the argument. The expected text is
           built from the legacy's by exactly those two edits, so a third difference reds here. */
        Assert.Equal(DailySummarySql.RangeSqlFor(RetentionTier.Hourly), DailySummarySql.RangeSqlFor(RetentionTier.Hourly, legacy));
        var successorFilter = TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsIntervalHourlySql);
        Assert.Equal("sample_interval_seconds IS DISTINCT FROM 0", successorFilter);
        Assert.Equal(string.Empty, TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsHourlySql));
        const string SourceProbeTail = "AND s.collection_time < b.d + INTERVAL '1 day'";
        var legacyRouted = DailySummarySql.RangeSqlFor(RetentionTier.Hourly);
        Assert.Equal(1, legacyRouted.Split(SourceProbeTail).Length - 1);
        /* #3905: the probe now ends in its OFFSET 0 fence on the next line, so the filter is inserted after
           the tail itself — which the count above proves appears exactly once. */
        var expectedSuccessorRouted = legacyRouted
            .Replace($"FROM collect.{legacy}", $"FROM collect.{successor}", StringComparison.Ordinal)
            .Replace(SourceProbeTail, SourceProbeTail + "\n          AND " + successorFilter, StringComparison.Ordinal);
        Assert.Equal(expectedSuccessorRouted, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, successor));
        Assert.Contains(successor, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, successor), StringComparison.Ordinal);
        Assert.DoesNotContain(legacy, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, successor), StringComparison.Ordinal);
        Assert.Equal(DailySummarySql.RangeSqlFor(RetentionTier.Daily), DailySummarySql.RangeSqlFor(RetentionTier.Daily, successor));
        Assert.Equal(DailySummarySql.RangeSql, DailySummarySql.RangeSqlFor(RetentionTier.Raw, successor));
    }

    /// <summary>
    /// The source-order pin for the start path (the RetiredBaselineAggregateTests shape): the coverage log runs
    /// AFTER the ensure that creates the successors, inside the TimescaleDB block, and before compression —
    /// and it is an instrument, so it is the only #3653 call there: no drop, no policy removal.
    ///
    /// <para><b>Re-anchored by #3817, which is why the anchors below are segment walks rather than the two
    /// ensure calls they used to be.</b> The ensures either side of this log moved into one shared list
    /// (<c>s_storeObjectConvergence</c>) that the start path and the hourly store-maintenance tick both walk,
    /// so "the aggregate ensure" and "the aggregate-compression ensure" no longer appear as literal call sites
    /// in the worker at all. The PROPERTY this pin holds is unchanged and is in fact what #3817 had to
    /// preserve: the coverage log sits between the segment that ends with the aggregate ensure and the segment
    /// that begins with the dedup/compression pair, so its position relative to both is still asserted — now
    /// against the boundary that enforces it rather than against two calls that happened to straddle it. The
    /// list's own ordering (aggregates before compression) is pinned in <c>StoreObjectConvergenceTests</c>,
    /// which is where the one-order claim belongs.</para>
    /// </summary>
    [Fact]
    public void Worker_LogsTheSupersededCoverage_AfterTheEnsure_AndRemovesNothing()
    {
        var worker = ReadWorkerSource();

        /* The SEGMENT CALLS, not the stage names: the enum's own members and the list's per-step tags spell
           those same tokens a thousand lines earlier, so an anchor on the bare stage name measures the
           declaration's position and proves nothing about the start path. The argument list is what makes
           each anchor a call site. */
        var ensureAt = worker.IndexOf("StoreObjectConvergenceStage.Timescale, startupConvergence, stoppingToken);", StringComparison.Ordinal);
        var logAt = worker.IndexOf("TimescaleSupport.LogSupersededHourlyRollupCoverageAsync(", StringComparison.Ordinal);
        var compressionAt = worker.IndexOf("StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, startupConvergence, stoppingToken);", StringComparison.Ordinal);
        var plainModeAt = worker.IndexOf("continuing in plain-PostgreSQL mode", StringComparison.Ordinal);

        Assert.True(ensureAt > 0 && logAt > 0 && compressionAt > 0 && plainModeAt > 0);
        Assert.True(ensureAt < logAt, "the coverage log must run after the segment whose last step is the ensure that creates the successors");
        Assert.True(logAt < compressionAt, "the coverage log sits before the post-repair segment that carries the compression ensure");
        Assert.True(logAt < plainModeAt, "the coverage log is inside the TimescaleDB block — it reads timescaledb_information");

        /* Code references only — the comment beside the call names the registry, which is what a comment is
           for; what must not exist is a call that walks it or a statement that removes a policy. */
        Assert.DoesNotContain("TimescaleSupport.SupersededHourlyRollups", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("remove_continuous_aggregate_policy", worker, StringComparison.Ordinal);
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    private static RollupCoverage Coverage(DateTime legacyFloor, DateTime successorFloor) =>
        new(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [TimescaleSupport.QueryStatsHourlyView] = legacyFloor,
                [TimescaleSupport.QueryStatsIntervalHourlyView] = successorFloor,
                [TimescaleSupport.QueryStatsDailyView] = legacyFloor.AddDays(-30),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { ["query_stats"] = DaysAgo(4) },
            RollupAvailability.All);

    private static PanelPlan ComposePlan(string measureKey) =>
        new()
        {
            Measure = MeasureCatalog.Measures.First(m => string.Equals(m.Key, measureKey, StringComparison.Ordinal)),
            Unit = "us",
            Mode = PanelMode.TimeSeries,
            Filters = Array.Empty<ComposeFilter>(),
            GroupBy = Array.Empty<ComposeDimension>(),
            Viz = "line",
        };

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}

/// <summary>
/// #3653 (Q12) proven live on TimescaleDB, in a scratch database so the legacy trio can be created and refreshed
/// without touching a shared store's materializations: a restart's (0, 0) row planted beside rated rows and a
/// pre-column (NULL-interval) row, both aggregates refreshed over the same hour, the legacy counting four
/// samples with a floor of zero and the successor counting three with the real floor and the measured interval
/// summed; then the store's own coverage probe driving <see cref="RollupCoverage.HourlyRelationFor"/> and the
/// start-time coverage log.
/// </summary>
[Collection("live-postgres")]
public sealed class IntervalHonestHourlyRollupLiveTests
{
    private const int ServerId = -936536;
    private const string ServerName = "interval-honest-e2e";

    [Fact]
    public async Task RestartRow_CountedByTheLegacy_NotByTheSuccessor_AndTheProbeRoutesByCoverage_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live interval-honest rollup test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live interval-honest rollup test needs TimescaleDB: the difference it measures exists only between materializations.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        /* #3893: collection_log too, as the worker's "collection_log hypertable" step does before its aggregate ensure
           (pinned in StoreObjectConvergenceTests). The off-grid collection-health aggregate is sourced from it. On a
           store whose migrations ran before CREATE EXTENSION (CI's bundled PostgreSQL), it is still a plain table here,
           and the aggregate's CREATE fails with 0A000. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* One closed hour, well inside the raw horizon and aligned so refresh windows land on its bounds. */
        var hour = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-1).AddHours(10), DateTimeKind.Unspecified);
        const string db = "IntervalHonestDb";
        const string hash = "0xIHHASH";
        const string handle = "0xIHHANDLE";

        /* Four collections of one statement: rated (300 s, 1,000 us, 10 executions), the RESTART as the collector
           writes it (every delta 0 beside interval 0), rated again (300 s, 3,000 us, 30), and a pre-column row
           (NULL interval, 2,000 us, 20) that the three-state rule keeps. */
        var rows = new (int Minute, int? Interval, long Worker, long Executions)[]
        {
            (0, 300, 1000, 10),
            (5, 0, 0, 0),
            (10, 300, 3000, 30),
            (15, null, 2000, 20),
        };
        var collectionId = 1L;
        foreach (var (minute, interval, worker, executions) in rows)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10)", connection);
            insert.Parameters.AddWithValue(collectionId++);
            insert.Parameters.AddWithValue(hour.AddMinutes(minute));
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(ServerName);
            insert.Parameters.AddWithValue(db);
            insert.Parameters.AddWithValue(hash);
            insert.Parameters.AddWithValue(handle);
            insert.Parameters.AddWithValue(worker);
            insert.Parameters.AddWithValue(executions);
            insert.Parameters.AddWithValue(interval.HasValue ? interval.Value : DBNull.Value);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* The whole registry through the product's own sweep — legacy trio, successors, dailies, baselines —
           so the successors are created exactly as a store will create them, and the sweep's own count says
           every one built. */
        var ready = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.Equal(TimescaleSupport.HourlyAggregates.Length + TimescaleSupport.DailyAggregates.Length + TimescaleSupport.BaselineAggregates.Length + TimescaleSupport.OffGridAggregates.Length, ready);

        foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsDbHourlyView, TimescaleSupport.QueryStatsDbIntervalHourlyView })
        {
            await RollupBackfill.RunSliceAsync(connection, view, hour.Date, hour.Date.AddDays(1), SilentDisclosure(), ct);
        }

        /* THE MEASUREMENT. Legacy: four samples, floor 0. Successor: three, floor 1,000, 600 s of measured
           interval (the NULL row adds nothing to the sum but is a sample). Sums agree, because a 0 adds nothing
           to a sum — the contamination was never in the sums. */
        var legacy = await ReadRollupAsync(connection, TimescaleSupport.QueryStatsHourlyView, hour, ct);
        var successor = await ReadRollupAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, hour, ct);

        Assert.Equal(4, legacy.SampleCount);
        Assert.Equal(0, legacy.WorkerMin);
        Assert.Equal(6000, legacy.WorkerSum);
        Assert.Equal(60, legacy.ExecutionSum);

        Assert.Equal(3, successor.SampleCount);
        Assert.Equal(1000, successor.WorkerMin);
        Assert.Equal(6000, successor.WorkerSum);
        Assert.Equal(60, successor.ExecutionSum);
        Assert.Equal(600, successor.IntervalSum);

        /* The database-grain pair, the same way: sums equal, sample counts differ by the one restart row. */
        var legacyDb = await ReadDbRollupAsync(connection, TimescaleSupport.QueryStatsDbHourlyView, hour, ct);
        var successorDb = await ReadDbRollupAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, hour, ct);
        Assert.Equal(4, legacyDb.SampleCount);
        Assert.Equal(3, successorDb.SampleCount);
        Assert.Equal(legacyDb.WorkerSum, successorDb.WorkerSum);
        Assert.Equal(600, successorDb.IntervalSum);

        /* THE PROBE, off the store: both present, both materialized over the same hour, so the supply rule
           answers the successor for every window — the fresh-store shape. Then the successor's floor
           measured from the store's own probe, which is what every reader routes on. */
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.QueryGrainIntervalHourly && rollups.DbGrainIntervalHourly && rollups.ProcedureGrainIntervalHourly);
        Assert.True(rollups.AllPresent);

        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        Assert.Equal(hour, coverage.FloorOf(TimescaleSupport.QueryStatsHourlyView));
        Assert.Equal(hour, coverage.FloorOf(TimescaleSupport.QueryStatsIntervalHourlyView));
        Assert.Equal(TimescaleSupport.QueryStatsIntervalHourlyView, coverage.HourlyRelationFor(TimescaleSupport.QueryStatsHourlyView, hour.AddDays(-30)));
        Assert.Equal(TimescaleSupport.QueryStatsDbIntervalHourlyView, coverage.HourlyRelationFor(TimescaleSupport.QueryStatsDbHourlyView, hour));

        /* The upgraded-store shape, made on the same store: re-create the successor empty (WITH NO DATA) so
           the legacy is the deeper supply — the rule sends a window reaching into the hour to the legacy and a
           window past it to the successor. */
        /* #3653 A6 lane LB-6: dropping the hourly successor CASCADEs onto query_stats_interval_daily too —
           it is hierarchical FROM the hourly (CreateQueryStatsIntervalDailySql), so a CAGG's own dependency
           takes its child down with it. The product's ensure sweep always recreates every registered
           aggregate on the next start (EnsureContinuousAggregatesAsync runs the whole registry, not just
           the changed one), so the fixture must do the same here or the coverage probe below throws 42P01
           on a relation THIS test just destroyed and never rebuilt — a test-setup gap, not a product one. */
        await ExecuteAsync(connection, TimescaleSupport.DropRetiredBaselineRelationSql(TimescaleSupport.QueryStatsIntervalHourlyView), ct);
        await ExecuteAsync(connection, TimescaleSupport.CreateQueryStatsIntervalHourlySql, ct);
        await ExecuteAsync(connection, TimescaleSupport.CreateQueryStatsIntervalDailySql, ct);
        var upgraded = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        Assert.Null(upgraded.FloorOf(TimescaleSupport.QueryStatsIntervalHourlyView));
        Assert.Equal(TimescaleSupport.QueryStatsHourlyView, upgraded.HourlyRelationFor(TimescaleSupport.QueryStatsHourlyView, hour));
        Assert.Equal(TimescaleSupport.QueryStatsHourlyView, upgraded.HourlyRelationFor(TimescaleSupport.QueryStatsHourlyView, hour.AddDays(-30)));

        /* And the start-time instrument runs clean against the real catalog, saying so for all three pairs. */
        var log = new CapturingTestLogger();
        await TimescaleSupport.LogSupersededHourlyRollupCoverageAsync(connection, log, DateTime.UtcNow, ct);
        foreach (var (legacyView, successorView, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            Assert.Contains(legacyView, log.Joined, StringComparison.Ordinal);
            Assert.Contains(successorView, log.Joined, StringComparison.Ordinal);
        }
        Assert.Contains("nothing yet", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Could not read", log.Joined, StringComparison.Ordinal);
    }

    private sealed record RollupReading(long SampleCount, long WorkerMin, long WorkerSum, long ExecutionSum, long? IntervalSum);

    private static async Task<RollupReading> ReadRollupAsync(NpgsqlConnection connection, string view, DateTime bucket, CancellationToken ct)
    {
        var intervalColumn = string.Equals(view, TimescaleSupport.QueryStatsHourlyView, StringComparison.Ordinal) ? "NULL::bigint" : "sample_interval_seconds_sum";
        await using var read = new NpgsqlCommand(
            $"SELECT sample_count, worker_time_min, worker_time_sum, execution_count_sum, {intervalColumn} FROM collect.{view} WHERE server_id = $1 AND bucket = $2", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(bucket);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{view} holds no row for the planted hour");
        var reading = new RollupReading(
            reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4));
        Assert.False(await reader.ReadAsync(ct), $"{view} holds more than one row for the planted statement");
        return reading;
    }

    private static async Task<RollupReading> ReadDbRollupAsync(NpgsqlConnection connection, string view, DateTime bucket, CancellationToken ct)
    {
        var intervalColumn = string.Equals(view, TimescaleSupport.QueryStatsDbHourlyView, StringComparison.Ordinal) ? "NULL::bigint" : "sample_interval_seconds_sum";
        await using var read = new NpgsqlCommand(
            $"SELECT sample_count, 0::bigint, worker_time_sum, execution_count_sum, {intervalColumn} FROM collect.{view} WHERE server_id = $1 AND bucket = $2", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(bucket);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{view} holds no row for the planted hour");
        return new RollupReading(
            reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4));
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static RefreshDisclosure SilentDisclosure() =>
        new(message => Assert.Fail($"the refresh degraded unexpectedly on a 2.28.1 store: {message}"));
}
