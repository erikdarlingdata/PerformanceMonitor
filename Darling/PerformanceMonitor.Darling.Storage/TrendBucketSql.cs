/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL half of the trend bucketing contract (#3897) that the store's own readers need — the MCP reads in the
/// service and the PostgreSQL trend reads here build their <c>date_bin</c> from it. The sizing rules (the width
/// ladder, the point budget, the refusals) live in <c>PerformanceMonitor.Common.TrendBuckets</c>, which this
/// assembly cannot see; <c>TrendBucketSqlTests</c> pins <see cref="OriginSql"/> equal to <c>TrendBuckets.OriginSql</c>,
/// which Lite's DuckDB reads use, so both SKUs bin the same rows into the same buckets at any width.
/// </summary>
public static class TrendBucketSql
{
    /// <summary>
    /// The origin every bucketing read aligns to. A midnight, so every ladder width (each divides a day) lands on
    /// round clock times; and an explicit one, because PostgreSQL's <c>date_bin</c> requires it and DuckDB's
    /// <c>time_bucket</c> otherwise defaults to 2000-01-03, which bins a width that does not divide that offset
    /// differently.
    /// </summary>
    public const string OriginSql = "TIMESTAMP '2000-01-01 00:00:00'";

    /// <summary>
    /// The memory-grant series' shared per-collection read (#3548, #4349): total granted MB across all pools per
    /// grants collection over the window, SUM CAST to double precision. The MCP <c>get_memory_trend</c> tool
    /// (<c>DarlingTrendReader.MemoryGrantTrendSql</c>) reads this AS ITS OWN trend — unbucketed, one row per
    /// collection — then wraps it in its own <c>date_bin</c> (<c>MemoryGrantTrendBucketedSql</c>) to align onto
    /// the memory series' buckets. The viewer's Overview overlay (<c>ViewerDataService.MemoryGrantTrendSql</c>)
    /// wraps the SAME per-collection CTE in a second, independent bucketing outer select for its own chart. Both
    /// wrappers bucket this text; this text itself must never bucket, or MCP's caller would bucket twice. $1
    /// server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string MemoryGrantPerCollectionSql = """
        SELECT
            collection_time,
            CAST(SUM(granted_memory_mb) AS double precision) AS total_granted_mb
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY collection_time
        """;

    /// <summary>
    /// The Memory Grants chart data — Lite's <c>GetMemoryGrantChartDataAsync</c> ported to Postgres, then
    /// bucketed (#4349): per collection_time + pool_id, the summed sizing MB (available/granted/used) and
    /// activity counts (grantees/waiters/timeouts/forced) — the inner <c>per_collection</c> CTE is the
    /// UNCHANGED pre-bucket aggregation (multiple resource-semaphore rows per pool summed to one row per
    /// collection + pool). The outer bucket then AVERAGES every gauge (sizing MB, grantee/waiter counts —
    /// point-in-time readings, not accumulating counters) and SUMS the two true deltas
    /// (<c>timeout_error_count_delta</c> / <c>forced_grant_count_delta</c>) over a <c>rated</c> CTE that
    /// nulls them out (not filters the row) when <c>sample_interval_seconds</c> is 0 (#3540) — the same
    /// null-not-drop treatment <c>ViewerDataService.TempDbFileIoTrendSql</c> uses, so a bucket's sizing gauges are
    /// never lost just because its one collection's delta was unrated. <c>grantee_count</c> /
    /// <c>waiter_count</c> round their averaged bigint back to bigint (<c>ROUND</c> before the CAST — an
    /// AVG of integers is fractional). Shared by the viewer's Memory Grants charts and get_memory_grants' <c>chart: true</c> (#6299), so the
    /// two draw the same points from one text. $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket
    /// width minutes.
    /// </summary>
    public const string MemoryGrantChartDataSql = $$"""
        WITH per_collection AS (
            SELECT
                collection_time,
                pool_id,
                CAST(SUM(available_memory_mb) AS double precision) AS available_memory_mb,
                CAST(SUM(granted_memory_mb) AS double precision) AS granted_memory_mb,
                CAST(SUM(used_memory_mb) AS double precision) AS used_memory_mb,
                CAST(SUM(target_memory_mb) AS double precision) AS target_memory_mb,
                CAST(SUM(max_target_memory_mb) AS double precision) AS max_target_memory_mb,
                CAST(SUM(grantee_count) AS bigint) AS grantee_count,
                CAST(SUM(waiter_count) AS bigint) AS waiter_count,
                /* #3540/#4349/#4364: MAX over the collection's own resource-semaphore rows. interval_seconds
                   is the sanctioned NULLIF form the measurement-contract census requires of any alias named
                   interval_sec(onds) — 0 (a restart, the ONLY known-unrateable case) becomes NULL through it.
                   But NULLIF cannot by itself tell that known-zero NULL apart from a pre-V128 row's true NULL
                   (interval never recorded) once collapsed into one alias, so the rated CTE below tests the
                   RAW max_interval_seconds_raw instead — the same IS DISTINCT FROM 0 the pre-#4364 guard used —
                   to keep an unknown interval's delta (LockWaitTrendSql's NULL-falls-back idiom, not #3540's
                   drop rule) while still nulling a genuine restart's. */
                NULLIF(MAX(sample_interval_seconds), 0) AS interval_seconds,
                MAX(sample_interval_seconds) AS max_interval_seconds_raw,
                CAST(SUM(timeout_error_count_delta) AS bigint) AS timeout_error_count_delta,
                CAST(SUM(forced_grant_count_delta) AS bigint) AS forced_grant_count_delta
            FROM v_memory_grant_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time, pool_id
        ),
        rated AS (
            SELECT
                collection_time,
                pool_id,
                available_memory_mb,
                granted_memory_mb,
                used_memory_mb,
                target_memory_mb,
                max_target_memory_mb,
                grantee_count,
                waiter_count,
                CASE WHEN max_interval_seconds_raw IS DISTINCT FROM 0 THEN timeout_error_count_delta END AS rated_timeout_error_count_delta,
                CASE WHEN max_interval_seconds_raw IS DISTINCT FROM 0 THEN forced_grant_count_delta END AS rated_forced_grant_count_delta
            FROM per_collection
        )
        SELECT
            pool_id,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{OriginSql}}), $2) AS bucket_start,
            AVG(available_memory_mb) AS available_memory_mb,
            AVG(granted_memory_mb) AS granted_memory_mb,
            AVG(used_memory_mb) AS used_memory_mb,
            CAST(ROUND(AVG(grantee_count)) AS bigint) AS grantee_count,
            CAST(ROUND(AVG(waiter_count)) AS bigint) AS waiter_count,
            CAST(SUM(rated_timeout_error_count_delta) AS bigint) AS timeout_error_count_delta,
            CAST(SUM(rated_forced_grant_count_delta) AS bigint) AS forced_grant_count_delta,
            AVG(target_memory_mb) AS target_memory_mb,
            AVG(max_target_memory_mb) AS max_target_memory_mb,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY pool_id, 2
        ORDER BY pool_id, 2
        """;
}
