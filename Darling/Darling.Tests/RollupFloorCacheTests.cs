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
    public void RollupFloorsToMeasure_AnEntryWhoseHypertableIsGoneFromTheCatalog_IsMeasured()
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

    /* ─────────────────────────── the one-view cold floor and the raw floors ─────────────────────────── */

    [Fact]
    public void RollupColdFloorSql_NamesOnlyItsOwnView_WithTheColdMarker()
    {
        var sql = TimescaleSupport.RollupColdFloorSql(TimescaleSupport.QueryStoreStatsHourlyView);

        Assert.Equal(
            $"SELECT (SELECT {TimescaleSupport.ColdFloorMarker} min(bucket) FROM collect.{TimescaleSupport.QueryStoreStatsHourlyView})",
            sql,
            StringComparer.Ordinal);
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews.Where(r => r.View != TimescaleSupport.QueryStoreStatsHourlyView))
        {
            Assert.DoesNotContain($"collect.{view})", sql, StringComparison.Ordinal);
        }
    }

    /* #5818: the per-probe check of cached raw floors. A floored table binds its floor as the next parameter and asks for
       a row exactly there and none earlier; a table cached as empty asks for no row. No min() sort, and its own marker,
       which does not contain the raw read's. */
    [Fact]
    public void RawFloorCheckSql_ChecksEachFloorAtItsOwnParameter_AndAnEmptyTableForNoRow()
    {
        var sql = TimescaleSupport.RawFloorCheckSql(new[] { "query_stats", "procedure_stats", "query_store_stats" }, new[] { true, false, true });
        Assert.Equal(
            "SELECT (EXISTS (SELECT 1 FROM collect.query_stats WHERE collection_time = $1) AND NOT EXISTS (SELECT 1 FROM collect.query_stats WHERE collection_time < $1)), "
            + "(NOT EXISTS (SELECT 1 FROM collect.procedure_stats)), "
            + "(EXISTS (SELECT 1 FROM collect.query_store_stats WHERE collection_time = $2) AND NOT EXISTS (SELECT 1 FROM collect.query_store_stats WHERE collection_time < $2)) "
            + TimescaleSupport.RawFloorCheckMarker,
            sql, StringComparer.Ordinal);
        Assert.DoesNotContain("min(", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(TimescaleSupport.RawFloorMarker, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RawFloorProbeSql_ReadsOneColumnPerTableItIsGiven_AndNoRollup()
    {
        var all = TimescaleSupport.RawFloorProbeSql(TimescaleSupport.RolledRawTables);
        var expected = "SELECT " + string.Join(", ", TimescaleSupport.RolledRawTables.Select(t => $"(SELECT min(collection_time) FROM collect.{t})"))
            + " " + TimescaleSupport.RawFloorMarker;
        Assert.Equal(expected, all, StringComparer.Ordinal);

        var one = TimescaleSupport.RawFloorProbeSql(new[] { "procedure_stats" });
        Assert.Equal($"SELECT (SELECT min(collection_time) FROM collect.procedure_stats) {TimescaleSupport.RawFloorMarker}", one, StringComparer.Ordinal);
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            Assert.DoesNotContain($"collect.{view}", all, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(6, 32)]
    [InlineData(7, 60)]
    [InlineData(40, 60)]
    public void RollupFloorRetryDelay_DoublesFromAMinute_AndNeverPassesTheReuseBound(int failures, int expectedMinutes)
    {
        Assert.Equal(TimeSpan.FromMinutes(expectedMinutes), TimescaleSupport.RollupFloorRetryDelay(failures));
        Assert.True(TimescaleSupport.RollupFloorRetryDelay(failures) <= TimescaleSupport.RollupFloorMaxReuse);
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

    /* ─────────── #5808: the cold reason and the planner make ONE identity test ─────────── */

    /// <summary>
    /// #5808: an entry measured while the view was not in the catalog, from ANOTHER database (a store recreated under
    /// the same name), is not serveable. The planner always said so; the cold reason used to call it serveable without the
    /// OID check, and EnqueueRollupFloorWork skips a job whose cold reason is null and whose entry is under the hour,
    /// so the view was served no floor and never queued.
    /// </summary>
    [Fact]
    public void RollupFloorColdReason_AnUnlistedViewsEntryFromAnotherDatabase_IsNotServeable_AndIsQueued()
    {
        var entry = new TimescaleSupport.RollupFloorCacheEntry(null, null, SomeFloor, Now, DatabaseOid: 1);
        var unlistedNow = new TimescaleSupport.RollupChunkIdentity(null, null, DatabaseOid: 2);

        Assert.NotNull(TimescaleSupport.RollupFloorColdReason(true, entry, unlistedNow, catalogMiss: true));
        Assert.False(TimescaleSupport.RollupFloorEntryMatches(entry, unlistedNow));

        var view = TimescaleSupport.QueryStatsHourlyView;
        var plan = TimescaleSupport.PlanRollupFloorMeasurements(
            new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal) { [view] = entry },
            new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal) { [view] = unlistedNow },
            RollupAvailability.All,
            Now);
        Assert.Contains(view, plan.Inline);

        /* The same database: serveable, by both. */
        var sameDatabase = unlistedNow with { DatabaseOid = 1 };
        Assert.Null(TimescaleSupport.RollupFloorColdReason(true, entry, sameDatabase, catalogMiss: true));
        Assert.True(TimescaleSupport.RollupFloorEntryMatches(entry, sameDatabase));
    }

    /// <summary>#5808: across every combination of chunk, hypertable and OID, the cold reason is null exactly when the
    /// planner would serve the entry (its helper matches), so no view is "serveable" to one and "changed" to the other.</summary>
    [Fact]
    public void RollupFloorColdReason_IsNullExactlyWhenThePlannerServesTheEntry()
    {
        string?[] chunks = { null, "_hyper_1_1_chunk", "_hyper_1_2_chunk" };
        string?[] hypertables = { null, SomeHypertable, AnotherHypertable };
        long[] oids = { 1, 2 };
        var view = TimescaleSupport.QueryStatsHourlyView;
        foreach (var entryChunk in chunks)
        foreach (var entryHypertable in hypertables)
        foreach (var entryOid in oids)
        foreach (var chunk in chunks)
        foreach (var hypertable in hypertables)
        foreach (var oid in oids)
        {
            var entry = new TimescaleSupport.RollupFloorCacheEntry(entryChunk, entryHypertable, SomeFloor, Now, DatabaseOid: entryOid);
            var identity = new TimescaleSupport.RollupChunkIdentity(chunk, hypertable, oid);
            var plan = TimescaleSupport.PlanRollupFloorMeasurements(
                new Dictionary<string, TimescaleSupport.RollupFloorCacheEntry>(StringComparer.Ordinal) { [view] = entry },
                new Dictionary<string, TimescaleSupport.RollupChunkIdentity>(StringComparer.Ordinal) { [view] = identity },
                RollupAvailability.All,
                Now);
            var catalogMiss = chunk is null && hypertable is null;
            Assert.Equal(!plan.Inline.Contains(view), TimescaleSupport.RollupFloorColdReason(true, entry, identity, catalogMiss) is null);
        }
    }

    /* ─────────── #5808: the catalog floor ─────────── */

    [Fact]
    public void IsBucketAligned_IsTrueOnlyOnABucketBoundary()
    {
        var hour = TimeSpan.FromHours(1);
        var day = TimeSpan.FromDays(1);

        Assert.True(TimescaleSupport.IsBucketAligned(new DateTime(2026, 1, 5, 7, 0, 0), hour));
        Assert.False(TimescaleSupport.IsBucketAligned(new DateTime(2026, 1, 5, 7, 30, 0), hour));
        Assert.False(TimescaleSupport.IsBucketAligned(new DateTime(2026, 1, 5, 7, 0, 0).AddTicks(1), hour));
        Assert.True(TimescaleSupport.IsBucketAligned(new DateTime(2026, 1, 5), day));
        Assert.False(TimescaleSupport.IsBucketAligned(new DateTime(2026, 1, 5, 1, 0, 0), day));
        Assert.False(TimescaleSupport.IsBucketAligned(new DateTime(2026, 1, 5), TimeSpan.Zero));
    }

    /// <summary>#5808: the catalog floor check is an EXISTS of a bucket EXACTLY at the bound (a row at the chunk's range
    /// start is the floor; a chunk with no row there answers false and is measured instead), over the one view it names,
    /// with the bound bound as $1, never interpolated, and with its own marker, never the cold sort's.</summary>
    [Fact]
    public void RollupCatalogFloorCheckSql_IsAnExistsAtExactlyTheBoundParameter_OverItsOwnView()
    {
        var view = TimescaleSupport.QueryStatsIoHourlyView;
        var sql = TimescaleSupport.RollupCatalogFloorCheckSql(view);

        Assert.Equal($"SELECT EXISTS (SELECT 1 FROM collect.{view} WHERE bucket = $1) {TimescaleSupport.CatalogFloorMarker}", sql);
        Assert.DoesNotContain("bucket <", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("min(", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(TimescaleSupport.ColdFloorMarker, sql, StringComparison.Ordinal);
        foreach (var (other, _, _, _, _) in TimescaleSupport.RollupViews.Where(r => r.View != view))
        {
            Assert.DoesNotContain($"collect.{other} ", sql, StringComparison.Ordinal);
        }
    }

    /* ─────────── #5808: the raw floors follow the raw tables' oldest chunk ─────────── */

    [Fact]
    public void RawFloorNeedsRead_ReadsANewOrMovedTable_AndOneWithNoChunk_ButNotAnUnchangedOne()
    {
        var table = TimescaleSupport.RolledRawTables[0];
        var entry = new TimescaleSupport.RawFloorCacheEntry(SomeFloor, Now, 1, "_hyper_5_10_chunk");
        Dictionary<string, string?> Chunks(string? chunk) => new(StringComparer.Ordinal) { [table] = chunk };

        Assert.True(TimescaleSupport.RawFloorNeedsRead(false, default, 1, Chunks("_hyper_5_10_chunk"), table));
        Assert.True(TimescaleSupport.RawFloorNeedsRead(true, entry, 2, Chunks("_hyper_5_10_chunk"), table));
        Assert.False(TimescaleSupport.RawFloorNeedsRead(true, entry, 1, Chunks("_hyper_5_10_chunk"), table));

        /* A drop_chunks retention pass: the oldest chunk is a different one. */
        Assert.True(TimescaleSupport.RawFloorNeedsRead(true, entry, 1, Chunks("_hyper_5_11_chunk"), table));

        /* No chunk at all (a plain table whose conversion failed, or an empty one): read on every probe. */
        Assert.True(TimescaleSupport.RawFloorNeedsRead(true, entry with { ChunkName = null }, 1, Chunks(null), table));

        /* A failed catalog read: only a missing or another database's entry is read. */
        Assert.False(TimescaleSupport.RawFloorNeedsRead(true, entry, 1, null, table));
    }

    [Fact]
    public void RollupOldestChunkSql_AlsoReadsEachRawTablesOldestChunk_FlaggedRaw()
    {
        var sql = TimescaleSupport.RollupOldestChunkSql(RollupAvailability.All)!;

        foreach (var table in TimescaleSupport.RolledRawTables)
        {
            Assert.Contains($"('{table}')", sql, StringComparison.Ordinal);
        }

        Assert.Contains("c.hypertable_name = raw.table_name", sql, StringComparison.Ordinal);
        Assert.Contains("false AS is_raw", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("min(collection_time)", sql, StringComparison.Ordinal);
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
