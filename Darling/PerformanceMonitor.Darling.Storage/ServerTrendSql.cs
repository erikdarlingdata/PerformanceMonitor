/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The bucketed per-server trend reads shared by the desktop viewer's charts and the <c>get_server_trend</c> MCP tool,
/// so both answer from one SQL text. Every read takes $1 server_id, $2 window start, $3 window end (naive UTC) and $4
/// the bucket width in minutes, and is windowed on both sides. Each bucket also carries <c>first_collection_time</c>
/// and <c>collection_count</c>, so a caller can stamp a bucket that merged nothing at its one collection's own time.
/// </summary>
public static class ServerTrendSql
{
    /// <summary>Total wait time across ALL wait types as one per-second series: the summed delta over the summed stored interval of each collection (a collection whose whole interval was unknowable is dropped, never read as 0), then summed per bucket as a time-weighted rate.</summary>
    public const string TotalWaits = $"""
        WITH per_collection AS
        (
            SELECT
                collection_time,
                SUM(delta_wait_time_ms) AS total_delta_ms,
                /* #3540: the collection's STORED interval — MAX over its rows, because a wait type first
                   seen in an otherwise steady pass carries 0 beside its siblings' real interval and adds 0
                   to the sum; MAX is 0 only when EVERY row was unknowable (a restart), and that 0 becomes
                   NULL through NULLIF so the point is dropped rather than rendered as 0.00. NULL (pre-V127
                   rows) falls back to the LAG this read always used. */
                CASE WHEN MAX(sample_interval_seconds) IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                     ELSE NULLIF(MAX(sample_interval_seconds), 0)
                END AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        ),
        rated AS
        (
            SELECT
                collection_time,
                CASE WHEN interval_seconds > 0 THEN total_delta_ms END AS rated_delta_ms,
                CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds
            FROM per_collection
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            CAST(SUM(rated_delta_ms) AS double precision) / SUM(rated_seconds) AS wait_time_ms_per_second,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY 1
        HAVING COUNT(rated_seconds) > 0
        ORDER BY 1
        """;

    /// <summary>The CPU scheduler pressure trend: the runnable / blocked / queued task counts, point-in-time gauges averaged per bucket.</summary>
    public const string CpuScheduler = $"""
        WITH raw AS
        (
            SELECT
                collection_time,
                collection_id,
                total_runnable_tasks_count,
                total_blocked_task_count,
                total_queued_request_count
            FROM v_cpu_scheduler_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(COALESCE(total_runnable_tasks_count, 0)) AS total_runnable_tasks_count,
            AVG(COALESCE(total_blocked_task_count, 0)) AS total_blocked_task_count,
            AVG(COALESCE(total_queued_request_count, 0)) AS total_queued_request_count,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM raw
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>The plan-cache size trend: single-use vs multi-use cache size (MB) per collection, summed across every (cacheobjtype, objtype) group, then averaged per bucket (a gauge).</summary>
    public const string PlanCache = $"""
        WITH per_collection AS
        (
            SELECT
                collection_time,
                CAST(SUM(single_use_size_mb) AS double precision) AS single_use_mb,
                CAST(SUM(multi_use_size_mb) AS double precision) AS multi_use_mb
            FROM v_plan_cache_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(single_use_mb) AS single_use_mb,
            AVG(multi_use_mb) AS multi_use_mb,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM per_collection
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>
    /// The memory-clerk trend for <paramref name="typeCount"/> clerk types in one query, grouped by clerk type and
    /// bucket. The <c>clerk_type IN (...)</c> list takes $4 onward and the bucket width the parameter after it.
    /// <c>memory_mb</c> is a gauge, averaged per bucket.
    /// </summary>
    public static string MemoryClerks(int typeCount)
    {
        var typeParams = string.Join(", ", Enumerable.Range(0, typeCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (typeCount + 4);
        return $$"""
            SELECT
                clerk_type,
                GREATEST(date_bin(CAST({{widthParam}} AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
                CAST(AVG(memory_mb) AS double precision) AS memory_mb,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count
            FROM v_memory_clerks
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   clerk_type IN ({{typeParams}})
            GROUP BY clerk_type, 2
            ORDER BY clerk_type, 2
            """;
    }

    /// <summary>
    /// The heaviest memory clerk types in the window, by total memory, for the clerk trend's default selection.
    /// $1 server_id, $2/$3 window (naive UTC), $4 how many to return.
    /// </summary>
    public const string TopMemoryClerks = """
        SELECT
            clerk_type
        FROM v_memory_clerks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY clerk_type
        ORDER BY SUM(memory_mb) DESC, clerk_type
        LIMIT CAST($4 AS integer)
        """;

    /// <summary>Whether the server has EVER recorded a wait-stats sample. $1 server_id.</summary>
    public const string HasAnyWaits = """
        SELECT 1
        FROM v_wait_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether the server has EVER recorded a CPU scheduler sample. $1 server_id.</summary>
    public const string HasAnyCpuScheduler = """
        SELECT 1
        FROM v_cpu_scheduler_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether the server has EVER recorded a memory-clerk sample. $1 server_id.</summary>
    public const string HasAnyMemoryClerks = """
        SELECT 1
        FROM v_memory_clerks
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether the server has EVER recorded a plan-cache sample. $1 server_id.</summary>
    public const string HasAnyPlanCache = """
        SELECT 1
        FROM v_plan_cache_stats
        WHERE server_id = $1
        LIMIT 1
        """;
}
