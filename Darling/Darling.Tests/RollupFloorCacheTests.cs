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
/// #4539/#4553: the pure half of the rollup floor cache — the planner that decides which rollups'
/// <c>min(bucket)</c> must be re-run this cycle, the floor-merge that must never reuse a stale floor for a
/// view measured NULL this cycle, and the SQL overload that only names the measured rollups.
/// </summary>
public sealed class RollupFloorCacheTests
{
    private static readonly DateTime SomeFloor = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Now = new(2026, 6, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private const string SomeHypertable = "_timescaledb_internal._materialized_hypertable_1";
    private const string AnotherHypertable = "_timescaledb_internal._materialized_hypertable_2";

    private static TimescaleSupport.RollupFloorCacheEntry Entry(
        string? chunkName, DateTime floor, DateTime? measuredAtUtc = null, string? materializationHypertable = SomeHypertable)
        => new(chunkName, materializationHypertable, floor, measuredAtUtc ?? Now);

    /* ─────────────────────────── the planner ─────────────────────────── */

    [Fact]
    public void RollupFloorsToMeasure_NoCacheEntry_IsMeasured()
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal);
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_1_chunk", SomeHypertable),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_SameOldestChunkAndHypertable_IsReused()
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = Entry("_hyper_1_1_chunk", SomeFloor),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_1_chunk", SomeHypertable),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.DoesNotContain(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_DifferentOldestChunk_IsMeasured()
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = Entry("_hyper_1_1_chunk", SomeFloor),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_2_chunk", SomeHypertable),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_SameChunkButDifferentHypertable_IsMeasured()
    {
        /* #4553: a drop/re-create can mint a new materialization hypertable whose first chunk happens to get
           the same generated chunk name a previous incarnation once had. The hypertable identity must catch
           what the chunk name alone would miss. */
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = Entry("_hyper_1_1_chunk", SomeFloor, materializationHypertable: SomeHypertable),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_1_chunk", AnotherHypertable),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_NoChunkAtAll_IsAlwaysMeasured()
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = Entry(null, SomeFloor),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new(null, null),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_ViewNotPresent_IsNeverInTheResult()
    {
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal);
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_1_chunk", SomeHypertable),
        };

        var absent = RollupAvailability.All with { QueryStoreGrainHourly = false };
        var measure = FloorsToMeasure(cached, oldestNow, absent, Now);

        Assert.DoesNotContain(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_CachedEntryOlderThanTheTtl_IsMeasured()
    {
        /* #4553: the TTL safety net — a raw-retention DELETE against the oldest materialization chunk can
           move the true floor later without the chunk's own identity ever changing, so identity matching
           alone is not always enough. An entry older than RollupFloorMaxReuse is measured regardless. */
        var measuredAt = Now - TimescaleSupport.RollupFloorMaxReuse;
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = Entry("_hyper_1_1_chunk", SomeFloor, measuredAt),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_1_chunk", SomeHypertable),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    [Fact]
    public void RollupFloorsToMeasure_CachedEntryWellUnderTheTtl_IsReused()
    {
        var measuredAt = Now - TimescaleSupport.RollupFloorMaxReuse + TimeSpan.FromMinutes(1);
        var cached = new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = Entry("_hyper_1_1_chunk", SomeFloor, measuredAt),
        };
        var oldestNow = new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStoreStatsHourlyView] = new("_hyper_1_1_chunk", SomeHypertable),
        };

        var measure = FloorsToMeasure(cached, oldestNow, RollupAvailability.All, Now);

        Assert.DoesNotContain(TimescaleSupport.QueryStoreStatsHourlyView, measure);
    }

    /* ─────────────────────────── the floor merge (the #4553 over-claim fix) ─────────────────────────── */

    /* ─────────────────────────── #4605: the ceiling ─────────────────────────── */

    [Fact]
    public void CeilingOf_UnknownView_IsNull()
    {
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All,
            new Dictionary<string, DateTime>(StringComparer.Ordinal));

        Assert.Null(coverage.CeilingOf(TimescaleSupport.QueryStoreStatsHourlyView));
    }

    /* ─────────────────────────── the two-argument SQL overload ─────────────────────────── */

    [Fact]
    public void RollupCoverageProbeSql_TwoArgument_NamesOnlyTheMeasuredViews()
    {
        var measure = new HashSet<string>(StringComparer.Ordinal) { TimescaleSupport.QueryStoreStatsHourlyView };

        var sql = TimescaleSupport.RollupCoverageProbeSql(RollupAvailability.All, measure);

        Assert.Contains($"(SELECT {TimescaleSupport.ColdFloorMarker} min(bucket) FROM collect.{TimescaleSupport.QueryStoreStatsHourlyView})", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"collect.{TimescaleSupport.QueryStatsHourlyView})", sql, StringComparison.Ordinal);

        /* Column count is unchanged — a non-measured present view still contributes a placeholder column,
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
                .Select(r => $"(SELECT /* coverage-floor:cold */ min(bucket) FROM collect.{r.View})")
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

    [Fact]
    public void RollupOldestChunkSql_NamesTheMaterializationHypertableColumn()
    {
        /* #4553: the cache key now also carries the materialization hypertable's own identity, not just the
           oldest chunk's name — this SQL is the source of that column. */
        var sql = TimescaleSupport.RollupOldestChunkSql(RollupAvailability.All);

        Assert.Contains("materialization_hypertable_schema", sql, StringComparison.Ordinal);
        Assert.Contains("materialization_hypertable_name", sql, StringComparison.Ordinal);
    }

    /* ─────────── #5329: a reused floor is still asked whether anything is earlier ─────────── */

    [Fact]
    public void RollupEarlierFloorProbeSql_IsABoundedReadPerView_AndNeverTheColdMinBucketStatement()
    {
        var sql = TimescaleSupport.RollupEarlierFloorProbeSql(new[] { TimescaleSupport.QueryStatsIoHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView });

        Assert.Contains($"FROM collect.{TimescaleSupport.QueryStatsIoHourlyView} AS m WHERE m.bucket < $1::timestamp", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{TimescaleSupport.ProcedureStatsIoHourlyView} AS m WHERE m.bucket < $2::timestamp", sql, StringComparison.Ordinal);
        /* The floor-cache live tests count the markers (ColdFloorMarker for the cold sort, EarlierFloorMarker for this bounded
           read) to prove the cold sort is not re-run; the bounded read carries the earlier marker and never the cold one. */
        Assert.DoesNotContain(TimescaleSupport.ColdFloorMarker, sql, StringComparison.Ordinal);
        Assert.Contains(TimescaleSupport.EarlierFloorMarker, sql, StringComparison.Ordinal);
    }
    /// <summary>Every view a cycle does not serve as it stands: <see cref="TimescaleSupport.PlanRollupFloorMeasurements"/>'s
    /// Inline and Background sets together (#5808 removed the production wrapper that did this).</summary>
    private static IReadOnlySet<string> FloorsToMeasure(
        IReadOnlyDictionary<string, TimescaleSupport.RollupFloorCacheEntry> cached,
        IReadOnlyDictionary<string, TimescaleSupport.RollupChunkIdentity> oldestNow,
        RollupAvailability availability,
        DateTime now)
    {
        var plan = TimescaleSupport.PlanRollupFloorMeasurements(cached, oldestNow, availability, now);
        var measure = new HashSet<string>(plan.Inline, StringComparer.Ordinal);
        measure.UnionWith(plan.Background);
        return measure;
    }
}
