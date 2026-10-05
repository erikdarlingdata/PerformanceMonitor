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

    /// <summary>The server-wide session-summary trend (the Activity tab's chart): the eight status and count gauges averaged per bucket, plus the top application and host of each bucket's newest collection (they cannot be averaged). Runs on <c>v_session_summary_stats</c>. Also carries <c>latest_collection_time</c>.</summary>
    public const string SessionSummary = $"""
        WITH raw AS
        (
            SELECT
                collection_time,
                GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
                total_sessions,
                running_sessions,
                sleeping_sessions,
                background_sessions,
                dormant_sessions,
                idle_sessions_over_30min,
                sessions_waiting_for_memory,
                databases_with_connections,
                top_application_name,
                top_application_connections,
                top_host_name,
                top_host_connections
            FROM v_session_summary_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        agg AS
        (
            SELECT
                bucket_start,
                AVG(COALESCE(total_sessions, 0)) AS total_sessions,
                AVG(COALESCE(running_sessions, 0)) AS running_sessions,
                AVG(COALESCE(sleeping_sessions, 0)) AS sleeping_sessions,
                AVG(COALESCE(background_sessions, 0)) AS background_sessions,
                AVG(COALESCE(dormant_sessions, 0)) AS dormant_sessions,
                AVG(COALESCE(idle_sessions_over_30min, 0)) AS idle_sessions_over_30min,
                AVG(COALESCE(sessions_waiting_for_memory, 0)) AS sessions_waiting_for_memory,
                AVG(COALESCE(databases_with_connections, 0)) AS databases_with_connections,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count
            FROM raw
            GROUP BY bucket_start
        ),
        latest AS
        (
            SELECT DISTINCT ON (bucket_start)
                bucket_start,
                top_application_name,
                top_application_connections,
                top_host_name,
                top_host_connections,
                collection_time AS latest_collection_time
            FROM raw
            ORDER BY bucket_start, collection_time DESC
        )
        SELECT
            agg.bucket_start,
            agg.total_sessions,
            agg.running_sessions,
            agg.sleeping_sessions,
            agg.background_sessions,
            agg.dormant_sessions,
            agg.idle_sessions_over_30min,
            agg.sessions_waiting_for_memory,
            agg.databases_with_connections,
            latest.top_application_name,
            latest.top_application_connections,
            latest.top_host_name,
            latest.top_host_connections,
            agg.first_collection_time,
            agg.collection_count,
            latest.latest_collection_time
        FROM agg
        JOIN latest ON latest.bucket_start = agg.bucket_start
        ORDER BY agg.bucket_start
        """;

    /// <summary>
    /// The latch-class wait trend for <paramref name="nameCount"/> latch classes in one query, grouped by class and
    /// bucket. The <c>latch_class IN (...)</c> list takes $4 onward and the bucket width the parameter after it. Each
    /// bucket's <c>wait_time_ms_per_second</c> is the summed delta over the summed stored interval of the collections
    /// whose interval was knowable (the stored interval, or for rows that never stored one the per-class LAG), so a
    /// restart's fabricated zero is dropped and a bucket the window cuts short still holds a true rate.
    /// </summary>
    public static string LatchWaits(int nameCount) => RatedByName(
        "v_latch_stats", "latch_class", "delta_wait_time_ms", "wait_time_ms_per_second", nameCount);

    /// <summary>The spinlock twin of <see cref="LatchWaits"/>: <c>collisions_per_second</c> per spinlock name and bucket.</summary>
    public static string SpinlockCollisions(int nameCount) => RatedByName(
        "v_spinlock_stats", "spinlock_name", "delta_collisions", "collisions_per_second", nameCount);

    private static string RatedByName(string view, string nameColumn, string deltaColumn, string rateColumn, int nameCount)
    {
        var nameParams = string.Join(", ", Enumerable.Range(0, nameCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (nameCount + 4);
        return $$"""
            WITH raw AS
            (
                SELECT
                    {{nameColumn}},
                    collection_time,
                    {{deltaColumn}},
                    /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                       and the bucket drops the row rather than reading 0.00. NULL (a pre-V127 row) falls back to the LAG. */
                    CASE WHEN sample_interval_seconds IS NULL
                         THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY {{nameColumn}} ORDER BY collection_time))))
                         ELSE NULLIF(sample_interval_seconds, 0)
                    END AS interval_seconds
                FROM {{view}}
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
                AND   {{nameColumn}} IN ({{nameParams}})
            )
            SELECT
                {{nameColumn}},
                GREATEST(date_bin(CAST({{widthParam}} AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
                CAST(SUM(CASE WHEN interval_seconds > 0 AND {{deltaColumn}} IS NOT NULL THEN {{deltaColumn}} END) AS double precision) / SUM(CASE WHEN interval_seconds > 0 AND {{deltaColumn}} IS NOT NULL THEN interval_seconds END) AS {{rateColumn}},
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count
            FROM raw
            GROUP BY {{nameColumn}}, 2
            HAVING COUNT(CASE WHEN interval_seconds > 0 AND {{deltaColumn}} IS NOT NULL THEN 1 END) > 0
            ORDER BY {{nameColumn}}, 2
            """;
    }

    /// <summary>
    /// The collector run-duration trend for <paramref name="nameCount"/> collectors in one query, grouped by collector
    /// and bucket: the bucket's longest and average successful run and its run count. The <c>collector_name IN (...)</c>
    /// list takes $4 onward and the bucket width the parameter after it. A run counts when status is SUCCESS with a duration.
    /// </summary>
    public static string CollectorDurations(int nameCount)
    {
        var nameParams = string.Join(", ", Enumerable.Range(0, nameCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (nameCount + 4);
        return $$"""
            SELECT
                collector_name,
                GREATEST(date_bin(CAST({{widthParam}} AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
                CAST(MAX(duration_ms) AS double precision) AS max_duration_ms,
                CAST(AVG(duration_ms) AS double precision) AS avg_duration_ms,
                CAST(COUNT(*) AS double precision) AS run_count,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   status = 'SUCCESS'
            AND   duration_ms IS NOT NULL
            AND   collector_name IN ({{nameParams}})
            GROUP BY collector_name, 2
            ORDER BY collector_name, 2
            """;
    }

    /// <summary>The latch classes with the most wait time in the window, for the default selection. $1 server_id, $2/$3 window (naive UTC), $4 how many to return.</summary>
    public const string TopLatchClasses = """
        SELECT
            latch_class
        FROM v_latch_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY latch_class
        ORDER BY SUM(delta_wait_time_ms) DESC, latch_class
        LIMIT CAST($4 AS integer)
        """;

    /// <summary>The spinlocks with the most collisions in the window, for the default selection. $1 server_id, $2/$3 window (naive UTC), $4 how many to return.</summary>
    public const string TopSpinlocks = """
        SELECT
            spinlock_name
        FROM v_spinlock_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY spinlock_name
        ORDER BY SUM(delta_collisions) DESC, spinlock_name
        LIMIT CAST($4 AS integer)
        """;

    /// <summary>The collectors with the longest successful run in the window, for the default selection. $1 server_id, $2/$3 window (naive UTC), $4 how many to return.</summary>
    public const string TopCollectors = """
        SELECT
            collector_name
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   status = 'SUCCESS'
        AND   duration_ms IS NOT NULL
        GROUP BY collector_name
        ORDER BY MAX(duration_ms) DESC, collector_name
        LIMIT CAST($4 AS integer)
        """;

    /// <summary>Whether the server has EVER recorded a latch-stats sample. $1 server_id.</summary>
    public const string HasAnyLatch = """
        SELECT 1
        FROM v_latch_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether the server has EVER recorded a spinlock-stats sample. $1 server_id.</summary>
    public const string HasAnySpinlock = """
        SELECT 1
        FROM v_spinlock_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether the server has EVER recorded a session-summary sample. $1 server_id.</summary>
    public const string HasAnySessionSummary = """
        SELECT 1
        FROM v_session_summary_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether the server has EVER logged a collector run. $1 server_id.</summary>
    public const string HasAnyCollectionLog = """
        SELECT 1
        FROM v_collection_log
        WHERE server_id = $1
        LIMIT 1
        """;
}
