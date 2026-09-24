/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A6, lane LA-4a: <see cref="DarlingTrendReader.BuildBucketedHourlyTrendSql"/>'s and
/// <see cref="DarlingTrendReader.QueryHistoryHourlySqlFor"/>'s hourly builders now splice
/// <c>DurationTrendRoute.HourlyFromClauseOrDefault</c> (a FROM-clause item) rather than a bare relation name,
/// and <see cref="DarlingTrendReader.HasAnySampleOnRouteAsync"/> probes
/// <c>DurationTrendRoute.ProbeHourlyViewOrDefault</c> (the successor NAME when the stitch applies). Pure — no
/// live store — so every shape is pinned without a rig.
/// </summary>
public sealed class DarlingTrendReaderStitchTests
{
    private static readonly DateTime Now = new(2026, 9, 24, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime DaysAgo(double days) => Now.AddDays(-days);

    private const string Legacy = TimescaleSupport.QueryStatsHourlyView;
    private const string Successor = TimescaleSupport.QueryStatsIntervalHourlyView;

    /* ─────────────────────────── BuildBucketedHourlyTrendSql ─────────────────────────── */

    [Fact]
    public void BucketedHourlyTrendSql_NoSuccessor_IsByteIdenticalApartFromAlias()
    {
        // RollupCoverage.Unknown carries no floors and RollupAvailability.None, so StitchedRelationSql
        // answers legacy-only for any relation (StitchedRelationSqlTests.NoSuccessor... pins the same shape).
        var fromClause = RollupCoverage.Unknown.StitchedRelationSql(Legacy, "h", DaysAgo(10), RollupCoverage.StitchTier.Hourly);
        Assert.Equal($"collect.{Legacy} AS h", fromClause);

        var stitched = DurationTrendRouting.BuildBucketedHourlyTrendSql(fromClause);
        var oldWay = DurationTrendRouting.BuildBucketedHourlyTrendSql(Legacy).Replace(
            $"FROM {Legacy}", $"FROM collect.{Legacy} AS h", StringComparison.Ordinal);

        Assert.Equal(oldWay, stitched);
    }

    [Fact]
    public void BucketedHourlyTrendSql_SuccessorFloorInsideWindow_ContainsUnionAllAndBothNames()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var fromClause = coverage.StitchedRelationSql(Legacy, "h", DaysAgo(10), RollupCoverage.StitchTier.Hourly);
        Assert.Contains("UNION ALL", fromClause, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{Legacy}", fromClause, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{Successor}", fromClause, StringComparison.Ordinal);
        Assert.Contains("TIMESTAMP '" + DaysAgo(3).ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'", fromClause, StringComparison.Ordinal);

        var stitched = DurationTrendRouting.BuildBucketedHourlyTrendSql(fromClause);
        Assert.Contains("UNION ALL", stitched, StringComparison.Ordinal);
        Assert.Contains(Legacy, stitched, StringComparison.Ordinal);
        Assert.Contains(Successor, stitched, StringComparison.Ordinal);
    }

    /* ─────────────────────────── QueryHistoryHourlySqlFor ─────────────────────────── */

    [Fact]
    public void QueryHistoryHourlySqlFor_NoSuccessor_IsByteIdenticalApartFromAlias()
    {
        var fromClause = RollupCoverage.Unknown.StitchedRelationSql(Legacy, "h", DaysAgo(10), RollupCoverage.StitchTier.Hourly);
        Assert.Equal($"collect.{Legacy} AS h", fromClause);

        var stitched = DarlingTrendReader.QueryHistoryHourlySqlFor(fromClause);
        var oldWay = DarlingTrendReader.QueryHistoryHourlySqlFor(Legacy).Replace(
            $"FROM {Legacy}", $"FROM collect.{Legacy} AS h", StringComparison.Ordinal);

        Assert.Equal(oldWay, stitched);
    }

    [Fact]
    public void QueryHistoryHourlySqlFor_SuccessorFloorInsideWindow_ContainsUnionAllAndBothNames()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var fromClause = coverage.StitchedRelationSql(Legacy, "h", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        var stitched = DarlingTrendReader.QueryHistoryHourlySqlFor(fromClause);
        Assert.Contains("UNION ALL", stitched, StringComparison.Ordinal);
        Assert.Contains(Legacy, stitched, StringComparison.Ordinal);
        Assert.Contains(Successor, stitched, StringComparison.Ordinal);
        Assert.Contains("TIMESTAMP '" + DaysAgo(3).ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'", stitched, StringComparison.Ordinal);
    }

    /* ─────────────────────────── ResolveQueryDurationTrendRoute's probe name (decision 4) ─────────────────────────── */

    [Fact]
    public void ResolveRoute_NoWindowEnd_ProbeHourlyViewOrDefault_FallsBackToLegacy()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        // No caller ever routed a windowEndUtc before LA-4a; that caller's route must probe the legacy name,
        // exactly as it did before the stitch existed.
        var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(
            DaysAgo(10), RollupAvailability.All, coverage, nowUtc: Now);

        Assert.Equal(Legacy, route.ProbeHourlyViewOrDefault);
    }

    [Fact]
    public void ResolveRoute_SuccessorFloorAtOrBeforeWindowEnd_ProbeHourlyViewOrDefault_IsSuccessor()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(
            DaysAgo(10), RollupAvailability.All, coverage, nowUtc: Now, windowEndUtc: Now);

        Assert.Equal(Successor, route.ProbeHourlyViewOrDefault);
    }

    [Fact]
    public void ResolveRoute_WindowEndBeforeSuccessorFloor_ProbeHourlyViewOrDefault_IsLegacy()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        // A window whose end sits before the successor even started materializing must not probe a relation
        // that could not possibly hold this window's rows.
        var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(
            DaysAgo(20), RollupAvailability.All, coverage, nowUtc: Now, windowEndUtc: DaysAgo(5));

        Assert.Equal(Legacy, route.ProbeHourlyViewOrDefault);
    }

    [Fact]
    public void ResolveRoute_NoSuccessorAvailable_ProbeHourlyViewOrDefault_IsLegacy()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.WithoutIntervalHourlies);

        var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(
            DaysAgo(10), RollupAvailability.WithoutIntervalHourlies, coverage, nowUtc: Now, windowEndUtc: Now);

        Assert.Equal(Legacy, route.ProbeHourlyViewOrDefault);
    }
}
