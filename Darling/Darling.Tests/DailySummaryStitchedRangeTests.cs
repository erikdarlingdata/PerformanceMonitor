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

        var literal = $"TIMESTAMP '{successorFloor:yyyy-MM-dd HH:mm:ss.ffffff}'";

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

        /* And the split agrees, boundary for boundary, with what StitchedRelationSql would splice into the
           FROM clause for the same coverage and window — the two must never disagree (the class doc's own
           requirement on RangeSqlFor). */
        var fromClause = coverage.StitchedRelationSql(Legacy, "f", windowStart, RollupCoverage.StitchTier.Hourly);
        Assert.Contains(literal, fromClause, StringComparison.Ordinal);
    }

    [Fact]
    public void DailyTier_IgnoresTheStitch_ReadsExactlyAsTheTwoArgumentFormDoes()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(5) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var stitched = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, DaysAgo(10));
        var plain = DailySummarySql.RangeSqlFor(RetentionTier.Daily);

        Assert.Equal(plain, stitched);
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
}
