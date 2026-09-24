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
/// #3653 A6 (design v2 §2): <see cref="RollupCoverage.StitchedRelationSql"/>, the SQL builder that stitches a
/// frozen legacy rollup to its interval-honest successor at the successor's first bucket, on both tiers.
/// Pure — no live store — so every shape is pinned without a rig.
/// </summary>
public sealed class StitchedRelationSqlTests
{
    private static readonly DateTime Now = new(2026, 9, 24, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime DaysAgo(double days) => Now.AddDays(-days);

    private const string Legacy = TimescaleSupport.QueryStatsHourlyView;
    private const string Successor = TimescaleSupport.QueryStatsIntervalHourlyView;

    /* ─────────────────────────── legacy-only ─────────────────────────── */

    [Fact]
    public void NoSuccessor_IsByteIdenticalToTodaysSingleRelationText()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [TimescaleSupport.QueryStoreStatsHourlyView] = DaysAgo(5) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(TimescaleSupport.QueryStoreStatsHourlyView, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        Assert.Equal($"collect.{TimescaleSupport.QueryStoreStatsHourlyView} AS f", sql);
    }

    [Fact]
    public void SuccessorAbsentFromAvailability_IsLegacyOnly()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.WithoutIntervalHourlies);

        var sql = coverage.StitchedRelationSql(Legacy, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        Assert.Equal($"collect.{Legacy} AS f", sql);
    }

    [Fact]
    public void LegacyEmpty_IsSuccessorOnly()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Successor] = DaysAgo(3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(Legacy, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        Assert.Equal($"collect.{Successor} AS f", sql);
    }

    [Fact]
    public void SuccessorEmpty_IsLegacyOnly()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(Legacy, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        Assert.Equal($"collect.{Legacy} AS f", sql);
    }

    /* ─────────────────────────── stitched ─────────────────────────── */

    [Fact]
    public void SuccessorReachesBeforeWindowStart_IsSuccessorOnly()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = DaysAgo(80) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(Legacy, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        Assert.Equal($"collect.{Successor} AS f", sql);
    }

    [Fact]
    public void SuccessorPartiallyReaches_IsStitchedAtItsOwnFloor()
    {
        var successorFloor = DaysAgo(5);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = successorFloor },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(Legacy, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);

        var literal = $"TIMESTAMP '{successorFloor:yyyy-MM-dd HH:mm:ss.ffffff}'";
        Assert.Contains($"FROM collect.{Legacy} WHERE bucket < {literal}", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{Successor} WHERE bucket >= {literal}", sql, StringComparison.Ordinal);
        Assert.EndsWith(") AS f", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sample_interval_seconds_sum", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StaleFloor_StillSplitsExactlyOnce_NoGapNoOverlap()
    {
        /* A boundary anywhere between the successor's true floor and the legacy's frozen ceiling is correct
           (design v2 §2's "cached probe that is slightly out of date is still safe"). Using a floor OLDER
           than what's "really" there still yields a well-formed, exactly-once split. */
        var staleFloor = DaysAgo(50);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [Legacy] = DaysAgo(80), [Successor] = staleFloor },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(Legacy, "f", DaysAgo(60), RollupCoverage.StitchTier.Hourly);

        var literal = $"TIMESTAMP '{staleFloor:yyyy-MM-dd HH:mm:ss.ffffff}'";
        Assert.Contains($"bucket < {literal}", sql, StringComparison.Ordinal);
        Assert.Contains($"bucket >= {literal}", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryPair_HasAColumnListRegistered()
    {
        foreach (var (legacy, successor, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            var coverage = new RollupCoverage(
                new Dictionary<string, DateTime>(StringComparer.Ordinal) { [legacy] = DaysAgo(80), [successor] = DaysAgo(5) },
                new Dictionary<string, DateTime>(StringComparer.Ordinal),
                RollupAvailability.All);

            var sql = coverage.StitchedRelationSql(legacy, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly);
            Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("sample_interval_seconds_sum", sql, StringComparison.Ordinal);
        }
    }

    /* ─────────────────────────── daily tier ─────────────────────────── */

    [Fact]
    public void DailyStitch_IsInertWhenSuccessorDailyIsAbsent()
    {
        /* No successor daily registered/available at all (today's shipped shape, before LB): legacy-only. */
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [TimescaleSupport.QueryStatsDailyView] = DaysAgo(200) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All);

        var sql = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDailyView, "f", DaysAgo(10), RollupCoverage.StitchTier.Daily);

        Assert.Equal($"collect.{TimescaleSupport.QueryStatsDailyView} AS f", sql);
    }

    [Fact]
    public void DailyStitch_FloorsAtCeilingOfDay_NotAtThePartialFirstBucket()
    {
        /* Successor hourly's first bucket is mid-day; F_d must be the START of the NEXT day, not that bucket. */
        var successorHourlyFloor = new DateTime(2026, 9, 19, 14, 0, 0, DateTimeKind.Utc);
        var successorDailyFloor = new DateTime(2026, 9, 19, 0, 0, 0, DateTimeKind.Utc); /* the daily rollup's own (partial) first bucket */

        var floors = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsDailyView] = DaysAgo(200),
            ["query_stats_interval_daily"] = successorDailyFloor,
            [TimescaleSupport.QueryStatsIntervalHourlyView] = successorHourlyFloor,
        };

        var availability = RollupAvailability.All with { QueryGrainIntervalHourly = true };
        var coverage = new RollupCoverage(floors, new Dictionary<string, DateTime>(StringComparer.Ordinal), availability);

        var sql = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDailyView, "f", DaysAgo(60), RollupCoverage.StitchTier.Daily);

        var expectedBoundary = new DateTime(2026, 9, 20, 0, 0, 0, DateTimeKind.Unspecified);
        var literal = $"TIMESTAMP '{expectedBoundary:yyyy-MM-dd HH:mm:ss.ffffff}'";
        Assert.Contains($"bucket < {literal}", sql, StringComparison.Ordinal);
        Assert.Contains($"bucket >= {literal}", sql, StringComparison.Ordinal);
        Assert.Contains("query_stats_interval_daily", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void NullLegacy_Throws()
    {
        var coverage = RollupCoverage.Unknown;
        Assert.Throws<ArgumentNullException>(() => coverage.StitchedRelationSql(null!, "f", DaysAgo(10), RollupCoverage.StitchTier.Hourly));
    }
}
