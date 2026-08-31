/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2736: the Query Store duration trend's rollup routing — the pure decision, the emitted SQL's bounds,
/// and the two apps' shared shape. The defect this guards: the raw read ranks the whole Query Store slab
/// per call under a fixed <c>-1 day / +30 days</c> collection_time slab, which exceeds the mcp role's
/// statement_timeout at ANY window width on a large store. The fix serves the materialized window portion
/// from <c>query_store_stats_corrected_hourly</c> and ranks raw ONLY over the unmaterialized tail, bounded
/// to the tail — so the pins here are about WHERE the cost went: the rollup is named, the slab is gone from
/// the routed SQL, and the raw fallback (still the whole read on stores without a rollup) is byte-for-byte
/// what it was.
/// </summary>
public sealed class QueryStoreTrendRoutingTests
{
    /* ── the pure routing decision ── */

    [Fact]
    public void Resolve_NoRollup_RoutesRawOnly()
    {
        var route = QueryStoreTrendRouting.Resolve(rollupExists: false, oldestBucketUtc: null, newestBucketUtc: null);
        Assert.False(route.UseRollup);
    }

    /// <summary>
    /// A rollup that EXISTS but has materialized nothing (created WITH NO DATA, no refresh yet) must route
    /// raw-only — #1759's lesson: existence and coverage are different questions, and routing on existence
    /// alone served empty results off a rollup while raw still held the rows.
    /// </summary>
    [Fact]
    public void Resolve_RollupExistsButEmpty_RoutesRawOnly()
    {
        var route = QueryStoreTrendRouting.Resolve(rollupExists: true, oldestBucketUtc: null, newestBucketUtc: null);
        Assert.False(route.UseRollup);
    }

    [Fact]
    public void Resolve_MaterializedRollup_RawStartsOneBucketAfterTheNewest()
    {
        var oldest = new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var newest = new DateTime(2026, 3, 4, 11, 0, 0, DateTimeKind.Unspecified);

        var route = QueryStoreTrendRouting.Resolve(rollupExists: true, oldest, newest);

        Assert.True(route.UseRollup);
        /* The raw arms start at the bucket AFTER the newest materialized one, so the two regions partition
           the window: rollup strictly below, raw at or above. Off by a bucket in either direction is a
           double count or a dropped hour. */
        Assert.Equal(newest.AddHours(1), route.RawStartUtc);
        Assert.Equal(oldest, route.RollupFloorUtc);
    }

    /* ── the probes ── */

    /// <summary>
    /// Availability first, by name resolution: <c>to_regclass</c> returns NULL rather than erroring on a
    /// plain-PostgreSQL store, and the bounds statement — which NAMES the view, a parse-time resolution —
    /// exists as a separate string precisely so it is only ever issued after the probe said yes.
    /// </summary>
    [Fact]
    public void Probes_AvailabilityByRegclass_BoundsNameTheViewSeparately()
    {
        Assert.Contains("to_regclass('query_store_stats_corrected_hourly')", QueryStoreTrendRouting.RollupProbeSql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM query_store_stats_corrected_hourly", QueryStoreTrendRouting.RollupProbeSql, StringComparison.Ordinal);

        Assert.Contains("min(bucket)", QueryStoreTrendRouting.RollupBoundsSql, StringComparison.Ordinal);
        Assert.Contains("max(bucket)", QueryStoreTrendRouting.RollupBoundsSql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_stats_corrected_hourly", QueryStoreTrendRouting.RollupBoundsSql, StringComparison.Ordinal);
    }

    /* ── the routed SQL's shape: rollup-served, tail-bounded, no slab ── */

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RollupTrendSql_ServesTheRollup_AndPartitionsAtTheBoundary(bool withDatabaseFilter)
    {
        var sql = QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter);

        /* The materialized region is a rollup scan over the corrected hourly's pre-deduped sums. */
        Assert.Contains("FROM query_store_stats_corrected_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(duration_us_weighted_sum) / 1000.0", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(execution_count_sum)", sql, StringComparison.Ordinal);

        /* The partition seam: rollup buckets strictly BELOW $4 (load-bearing against a refresh landing
           between the probe and the read), raw points at or above it. */
        Assert.Contains("bucket >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("bucket <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("bucket < $4", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc >= $4", sql, StringComparison.Ordinal);

        /* Both raw arms survive in the tail: the #1841 dedup at full interval identity, and the legacy
           IS NULL arm so the tail's rows still partition with no overlap and no gap. */
        Assert.Contains("PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE #2736 pin: the routed SQL's raw scan is bounded to the tail it serves. The old shape's fixed
    /// slab — <c>$2 - interval '1 day'</c> below and <c>$3 + interval '30 days'</c> above — is what made
    /// cost independent of the requested window; its absence here IS the fix, so it is pinned by absence
    /// and the replacement bounds are pinned by presence.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RollupTrendSql_BoundsTheRawScanToTheTail_TheSlabIsGone(bool withDatabaseFilter)
    {
        var sql = QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter);

        Assert.DoesNotContain("interval '30 days'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$2 - interval '1 day'", sql, StringComparison.Ordinal);

        /* GREATEST($2, $4): the raw region starts at the later of the window start and the rollup
           boundary, so a stalled refresh widens the scan only to the stall — never to the window's full
           depth plus a day. The +1 day ceiling is the engine's INTERVAL_LENGTH_MINUTES maximum of closing-
           fetch margin, affordable because the tail sits at the recent edge by construction. */
        Assert.Contains("collection_time >= GREATEST($2, $4) - interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3 + interval '1 day'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The raw-only read — the route for stores WITHOUT a materialized rollup — keeps its original slab on
    /// purpose: those are the stores where the generosity is affordable (plain PostgreSQL, or a store in
    /// its first hours), and the +30 days is real collector-behind margin there, not decoration. Tightening
    /// it would trade a timeout nobody hits on those stores for a silent undercount of catch-up eras.
    /// </summary>
    [Fact]
    public void RawFallbackSql_KeepsItsSlab_Deliberately()
    {
        Assert.Contains("collection_time >= $2 - interval '1 day'", DarlingTrendReader.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3 + interval '30 days'", DarlingTrendReader.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2 - interval '1 day'", ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3 + interval '30 days'", ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
    }

    /* ── the two apps share one shape ── */

    /// <summary>
    /// The MCP reader and the desktop viewer serve their routed SQL from the SAME builder — the #1841 twin
    /// discipline ("rewriting either arm would make the browser and the desktop viewer disagree about the
    /// same hour"), enforced by construction rather than by doc comment. The viewer's copy differs only by
    /// the #1319 database filter.
    /// </summary>
    [Fact]
    public void BothApps_ServeTheBuilderOutput_DifferingOnlyByTheDatabaseFilter()
    {
        Assert.Equal(QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: false), DarlingTrendReader.QueryStoreDurationTrendRollupSql);
        Assert.Equal(QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true), ViewerDataService.QueryStoreDurationTrendRollupSql);

        /* Strip the filter lines from the viewer's copy and the two must be byte-identical. */
        var viewerSql = ViewerDataService.QueryStoreDurationTrendRollupSql;
        var stripped = viewerSql
            .Replace("\n    AND   ($5::text[] IS NULL OR database_name = ANY($5))", "", StringComparison.Ordinal)
            .Replace("\n            AND   ($5::text[] IS NULL OR database_name = ANY($5))", "", StringComparison.Ordinal);
        Assert.Equal(DarlingTrendReader.QueryStoreDurationTrendRollupSql, stripped);

        /* Every arm of the viewer's copy carries the filter: the rollup scan, the identified-interval arm,
           and the legacy arm — three occurrences, or one arm silently ignores the user's selection. */
        var occurrences = viewerSql.Split("$5::text[] IS NULL").Length - 1;
        Assert.Equal(3, occurrences);
        Assert.DoesNotContain("$5", DarlingTrendReader.QueryStoreDurationTrendRollupSql, StringComparison.Ordinal);
    }
}
