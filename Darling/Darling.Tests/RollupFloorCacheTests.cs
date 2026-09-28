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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4539: the pure half of the rollup floor cache \u2014 the planner that decides which rollups' <c>min(bucket)</c>
/// must be re-run this cycle, and the SQL overload that only names those rollups.
/// </summary>
public sealed class RollupFloorCacheTests
{
    private static readonly DateTime SomeFloor = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /* ─────────────────────────── the planner ─────────────────────────── */

    [Fact]
    public void RollupFloorsToMeasure_NoCacheEntry_IsMeasured()
    {
        var cached = new Dictionary<string, (string? ChunkName, DateTime Floor)>(StringComparer.Ordinal);
        var oldestNow = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = "_hyper_1_1_chunk",
        };

        var measure = TimescaleSupport.RollupFloorsToMeasure(cached, oldestNow, RollupAvailability.All);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_SameOldestChunk_IsReused()
    {
        var cached = new Dictionary<string, (string? ChunkName, DateTime Floor)>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = ("_hyper_1_1_chunk", SomeFloor),
        };
        var oldestNow = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = "_hyper_1_1_chunk",
        };

        var measure = TimescaleSupport.RollupFloorsToMeasure(cached, oldestNow, RollupAvailability.All);

        Assert.DoesNotContain(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_DifferentOldestChunk_IsMeasured()
    {
        var cached = new Dictionary<string, (string? ChunkName, DateTime Floor)>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = ("_hyper_1_1_chunk", SomeFloor),
        };
        var oldestNow = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = "_hyper_1_2_chunk",
        };

        var measure = TimescaleSupport.RollupFloorsToMeasure(cached, oldestNow, RollupAvailability.All);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_NoChunkAtAll_IsAlwaysMeasured()
    {
        var cached = new Dictionary<string, (string? ChunkName, DateTime Floor)>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = (null, SomeFloor),
        };
        var oldestNow = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = null,
        };

        var measure = TimescaleSupport.RollupFloorsToMeasure(cached, oldestNow, RollupAvailability.All);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_ViewNotPresent_IsNeverInTheResult()
    {
        var cached = new Dictionary<string, (string? ChunkName, DateTime Floor)>(StringComparer.Ordinal);
        var oldestNow = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = "_hyper_1_1_chunk",
        };

        var absent = RollupAvailability.All with { QueryStoreGrainHourly = false };
        var measure = TimescaleSupport.RollupFloorsToMeasure(cached, oldestNow, absent);

        Assert.DoesNotContain(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    /* ─────────────────────────── the two-argument SQL overload ─────────────────────────── */

    [Fact]
    public void RollupCoverageProbeSql_TwoArgument_NamesOnlyTheMeasuredViews()
    {
        var measure = new HashSet<string>(StringComparer.Ordinal) { TimescaleSupport.QueryStoreStatsHourlyView };

        var sql = TimescaleSupport.RollupCoverageProbeSql(RollupAvailability.All, measure);

        Assert.Contains($"(SELECT min(bucket) FROM collect.{TimescaleSupport.QueryStoreStatsHourlyView})", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"collect.{TimescaleSupport.QueryStatsHourlyView})", sql, StringComparison.Ordinal);

        /* Column count is unchanged \u2014 a non-measured present view still contributes a placeholder column,
           so the reader's ordinals never depend on which rollups happened to be measured this cycle. */
        var columns = sql["SELECT ".Length..].Split(", ");
        Assert.Equal(TimescaleSupport.RollupViews.Length + TimescaleSupport.RolledRawTables.Length, columns.Length);
    }

    [Fact]
    public void RollupCoverageProbeSql_NullMeasureSet_IsByteIdenticalToTheOneArgumentForm()
    {
        foreach (var shape in new[] { RollupAvailability.All, RollupAvailability.None, RollupAvailability.WithoutIntervalHourlies })
        {
            var oneArg = TimescaleSupport.RollupCoverageProbeSql(shape);
            var twoArgNullMeasure = TimescaleSupport.RollupCoverageProbeSql(shape, measure: null);

            Assert.Equal(oneArg, twoArgNullMeasure, StringComparer.Ordinal);
        }
    }

    [Fact]
    public void RollupCoverageProbeSql_OneArgumentForm_MatchesTodaysLiteralOutputForAllPresent()
    {
        /* A literal copy of today's (pre-#4539) output for RollupAvailability.All, so a change to this
           overload's SHAPE (not just its selectivity) cannot slip through unnoticed. */
        var expected = "SELECT " + string.Join(", ", TimescaleSupport.RollupViews
                .Select(r => $"(SELECT min(bucket) FROM collect.{r.View})")
                .Concat(TimescaleSupport.RolledRawTables.Select(t => $"(SELECT min(collection_time) FROM collect.{t})")));

        Assert.Equal(expected, TimescaleSupport.RollupCoverageProbeSql(RollupAvailability.All), StringComparer.Ordinal);
    }

    /* ─────────────────────────── the oldest-chunk catalog SQL ─────────────────────────── */

    [Fact]
    public void RollupOldestChunkSql_NamesOnlyPresentViews()
    {
        var partial = RollupAvailability.All with { QueryGrainDaily = false };
        var sql = TimescaleSupport.RollupOldestChunkSql(partial);

        Assert.Contains($"'{TimescaleSupport.QueryStatsHourlyView}'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"'{TimescaleSupport.QueryStatsDailyView}'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RollupOldestChunkSql_NoRollupsPresent_ReturnsNull()
    {
        Assert.Null(TimescaleSupport.RollupOldestChunkSql(RollupAvailability.None));
    }
}
