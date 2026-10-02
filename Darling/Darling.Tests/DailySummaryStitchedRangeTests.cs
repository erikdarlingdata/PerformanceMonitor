/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A6, lane LA-4b: pure pins for <see cref="DailySummarySql.RangeSqlFor(RetentionTier, RollupCoverage, DateTime)"/>,
/// the three-argument, stitch-aware overload LA-3b2 added (commit e708093a4). No successor, or a successor that
/// does not straddle the window, must be byte-identical to today's single-probe text; a genuinely stitched pair
/// must UNION ALL the not-carried probe once per side of F, split exactly where
/// <see cref="RollupCoverage.StitchedRelationSql"/> would split the FROM clause (decision 3).
/// </summary>
public sealed class DailySummaryStitchedRangeTests
{
    private static readonly DateTime Now = new(2026, 9, 24, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime DaysAgo(double days) => Now.AddDays(-days);

    private const string Legacy = TimescaleSupport.QueryStatsHourlyView;
    private const string Successor = TimescaleSupport.QueryStatsIntervalHourlyView;

    [Fact]
    public void NoSuccessor_IsByteIdenticalToTheSingleProbeForm()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.WithoutIntervalHourlies);

        var windowStart = DaysAgo(10);
        var stitched = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, windowStart);
        var single = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage.HourlyRelationFor(Legacy, windowStart));

        Assert.Equal(single, stitched);

        /* The single-probe shape has exactly one "queries_ceiling" CTE (not the stitched pair's two, split by
           legacy/successor), and no per-side boundary literal. */
        Assert.Contains("queries_ceiling AS (", stitched, StringComparison.Ordinal);
        Assert.DoesNotContain("queries_ceiling_legacy", stitched, StringComparison.Ordinal);
        Assert.DoesNotContain("queries_ceiling_successor", stitched, StringComparison.Ordinal);
    }

    [Fact]
    public void SuccessorReachesBeforeWindowStart_RoutesLegacyOnly_ByteIdenticalToday()
    {
        /* The successor's floor already covers the whole window (StitchFloor answers null here too) — the
           legacy-only, single-probe form, but the RELATION resolved is the SUCCESSOR (HourlyRelationFor's own
           answer for this shape), matching what StitchedRelationSql would splice as successor-only. */
        var successorFloor = DaysAgo(80);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(90), [Successor] = successorFloor },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var windowStart = DaysAgo(10);
        var stitched = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, windowStart);
        var single = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage.HourlyRelationFor(Legacy, windowStart));

        Assert.Equal(single, stitched);
        Assert.Contains($"FROM collect.{Successor}", stitched, StringComparison.Ordinal);
        Assert.DoesNotContain(Legacy, stitched, StringComparison.Ordinal);
    }

    [Fact]
    public void StitchedPair_SplitsTheNotCarriedProbeOnceEachSide_AtTheSameBoundaryTheReadSplits()
    {
        var successorFloor = DaysAgo(5);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = successorFloor },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var windowStart = DaysAgo(10);
        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, windowStart);

        /* successorFloor lands mid-day (Now is 12:00:00), so the boundary literal actually spliced into the SQL
           is DAY-ALIGNED: the first whole day at or after F, never F's own hour (the fix for #4182's CI failure
           — see DailySummarySql.QueriesCteForStitchedCagg's remarks). */
        var boundaryDay = successorFloor.Date.AddDays(1);
        var literal = $"TIMESTAMP '{boundaryDay:yyyy-MM-dd HH:mm:ss.ffffff}'";

        /* The two rollup-half members, one per relation, each restricted to its own side of F. Matched by
           substring on the individually meaningful fragments rather than a whole multi-line block, so the
           assertion survives either line-ending convention. */
        Assert.Contains($"FROM collect.{Legacy}", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{Successor}", sql, StringComparison.Ordinal);
        Assert.Contains($"bucket >= $2 AND bucket < $3 AND bucket < {literal}", sql, StringComparison.Ordinal);
        Assert.Contains($"bucket >= $2 AND bucket < $3 AND bucket >= {literal}", sql, StringComparison.Ordinal);

        /* The not-carried ("NOT EXISTS") probe appears TWICE — once per relation name, decision 3 — each
           restricted to its side of F, and each keeps its own ceiling. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "NOT EXISTS").Count);
        Assert.Contains($"SELECT 1 FROM collect.{Legacy} AS r", sql, StringComparison.Ordinal);
        Assert.Contains($"SELECT 1 FROM collect.{Successor} AS r", sql, StringComparison.Ordinal);
        Assert.Contains("queries_ceiling_legacy", sql, StringComparison.Ordinal);
        Assert.Contains("queries_ceiling_successor", sql, StringComparison.Ordinal);
        Assert.Contains($"WHERE server_id = $1 AND bucket < {literal}", sql, StringComparison.Ordinal);
        Assert.Contains($"WHERE server_id = $1 AND bucket >= {literal}", sql, StringComparison.Ordinal);

        /* StitchedRelationSql's own FROM-clause split stays at F's actual hour (every row is examined on its
           own there, with no GROUP BY day, so a mid-day split costs it nothing); this probe instead needs the
           DAY F falls in, because its CTE groups by date_trunc('day', bucket) and a mid-day split would hand
           one calendar day two partial rows — the #4182 bug this fix corrects. So the two must agree only that
           this probe's boundary DAY is the first whole day at or after StitchedRelationSql's own F, not that
           the literals are identical. */
        var fromClause = coverage.StitchedRelationSql(Legacy, "f", windowStart, RollupCoverage.StitchTier.Hourly);
        Assert.Contains($"TIMESTAMP '{successorFloor:yyyy-MM-dd HH:mm:ss.ffffff}'", fromClause, StringComparison.Ordinal);
        Assert.Equal(boundaryDay, successorFloor.Date.AddDays(1));
    }

    /* ─────────────────────────── daily tier (#3653 A6, lane LA-8) ─────────────────────────── */

    private const string DailyLegacy = TimescaleSupport.QueryStatsDailyView;
    private const string DailySuccessor = "query_stats_interval_daily";

    [Fact]
    public void DailyTier_NoSuccessorInAvailability_IsByteIdenticalToTheTwoArgumentForm()
    {
        /* Pre-LB shape: the successor daily is not in RollupAvailability at all, even with hourly floors
           cached for the OTHER (hourly-tier) pair — the daily tier's own StitchFloor must gate on its own
           registry/availability, never leak the hourly tier's state. */
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [DailyLegacy] = DaysAgo(200),
                [Legacy] = DaysAgo(80),
                [Successor] = DaysAgo(5),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.WithoutIntervalDailies);

        var stitched = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, DaysAgo(10));
        var plain = DailySummarySql.RangeSqlFor(RetentionTier.Daily);

        Assert.Equal(plain, stitched);
        Assert.Contains("queries_ceiling AS (", stitched, StringComparison.Ordinal);
        Assert.DoesNotContain("queries_ceiling_legacy", stitched, StringComparison.Ordinal);
    }

    [Fact]
    public void DailyTier_SuccessorEmpty_IsByteIdenticalToTheTwoArgumentForm()
    {
        /* The successor daily is registered and available but has never refreshed (no floor) — legacy-only,
           same text as today. */
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [DailyLegacy] = DaysAgo(200) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var stitched = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, DaysAgo(10));
        var plain = DailySummarySql.RangeSqlFor(RetentionTier.Daily);

        Assert.Equal(plain, stitched);
    }

    [Fact]
    public void DailyTier_SuccessorReachesBeforeWindowStart_RoutesSuccessorOnly_NeverNamesTheLegacy()
    {
        var successorHourlyFloor = DaysAgo(90);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [DailyLegacy] = DaysAgo(200),
                [DailySuccessor] = DaysAgo(90),
                [TimescaleSupport.QueryStatsIntervalHourlyView] = successorHourlyFloor,
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, DaysAgo(10));

        Assert.Contains($"FROM collect.{DailySuccessor}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(DailyLegacy, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DailyTier_StitchedPair_SplitsTheNotCarriedProbeOnceEachSide_AtF_d()
    {
        /* Successor hourly's first bucket is mid-day, so F_d (ceiling-of-day) lands on the NEXT calendar day —
           the same day-alignment rule QueriesCteForStitchedCagg documents for the hourly pair. */
        var successorHourlyFloor = new DateTime(2026, 9, 19, 14, 0, 0, DateTimeKind.Utc);
        var successorDailyFloor = new DateTime(2026, 9, 19, 0, 0, 0, DateTimeKind.Utc);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [DailyLegacy] = DaysAgo(200),
                [DailySuccessor] = successorDailyFloor,
                [TimescaleSupport.QueryStatsIntervalHourlyView] = successorHourlyFloor,
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, DaysAgo(60));

        var boundaryDay = new DateTime(2026, 9, 20, 0, 0, 0, DateTimeKind.Unspecified);
        var literal = $"TIMESTAMP '{boundaryDay:yyyy-MM-dd HH:mm:ss.ffffff}'";

        Assert.Contains($"FROM collect.{DailyLegacy}", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{DailySuccessor}", sql, StringComparison.Ordinal);
        Assert.Contains($"bucket >= $2 AND bucket < $3 AND bucket < {literal}", sql, StringComparison.Ordinal);
        Assert.Contains($"bucket >= $2 AND bucket < $3 AND bucket >= {literal}", sql, StringComparison.Ordinal);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "NOT EXISTS").Count);
        Assert.Contains("queries_ceiling_legacy", sql, StringComparison.Ordinal);
        Assert.Contains("queries_ceiling_successor", sql, StringComparison.Ordinal);

        /* The not-carried probe's source for the successor SIDE is the successor HOURLY (MaterializationHoleTargets,
           RollupViews), not raw query_stats directly — the daily's hierarchical source. */
        Assert.Contains($"SELECT 1 FROM collect.{TimescaleSupport.QueryStatsIntervalHourlyView} AS s", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RawTier_IgnoresTheStitch_ReadsExactlyAsTheTwoArgumentFormDoes()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(5) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var stitched = DailySummarySql.RangeSqlFor(RetentionTier.Raw, coverage, DaysAgo(10));
        var plain = DailySummarySql.RangeSqlFor(RetentionTier.Raw);

        Assert.Equal(plain, stitched);
    }

    /// <summary>
    /// #3653 A6, lane LC-a4: the frozen legacy name this overload ITSELF ever names — <see cref="Legacy"/> on
    /// the Hourly tier, <see cref="DailyLegacy"/> on the Daily tier (both hardcoded in the two-argument form
    /// this overload falls back to; see its remarks) — must still build, both under
    /// <see cref="RollupCoverage.Unknown"/> (nothing measured, so the legacy alone is named) and under a
    /// stitched coverage whose successor floor falls INSIDE the window (the genuinely-stitched splice, which
    /// names the legacy AND the successor). Both threw <see cref="ArgumentException"/> on 1abc48ba:
    /// <c>QueriesCteForCagg</c>/<c>QueriesCteForStitchedCagg</c> read a frozen view's source off
    /// <c>MaterializationHoleTargets</c>, which the freeze (#3653 LC) had just emptied of every frozen view.
    /// </summary>
    [Fact]
    public void FrozenLegacy_StillBuilds_UnderUnknownCoverage_AndUnderAStitchThatCrossesTheWindow()
    {
        var windowStart = DaysAgo(10);

        var hourlyUnknown = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, RollupCoverage.Unknown, windowStart);
        Assert.Contains($"FROM collect.{Legacy}", hourlyUnknown, StringComparison.Ordinal);

        var dailyUnknown = DailySummarySql.RangeSqlFor(RetentionTier.Daily, RollupCoverage.Unknown, windowStart);
        Assert.Contains($"FROM collect.{DailyLegacy}", dailyUnknown, StringComparison.Ordinal);

        var hourlyStitch = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(5) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);
        var hourlyStitched = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, hourlyStitch, windowStart);
        Assert.Contains($"FROM collect.{Legacy}", hourlyStitched, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{Successor}", hourlyStitched, StringComparison.Ordinal);

        var dailyStitch = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal)
            {
                [DailyLegacy] = DaysAgo(200),
                [DailySuccessor] = DaysAgo(5),
                /* The daily stitch's own boundary is read off the successor HOURLY's floor (ceiling-of-day),
                   not the successor daily's floor directly — StitchedRelationSql's Daily branch. */
                [TimescaleSupport.QueryStatsIntervalHourlyView] = DaysAgo(90),
            },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);
        var dailyStitched = DailySummarySql.RangeSqlFor(RetentionTier.Daily, dailyStitch, windowStart);
        Assert.Contains($"FROM collect.{DailyLegacy}", dailyStitched, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{DailySuccessor}", dailyStitched, StringComparison.Ordinal);
    }
}
