/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;

using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets recent waiting task snapshots for a server: the waiting tasks captured over the window, newest capture first then longest wait, capped at
    /// <paramref name="limit"/> when one is given.
    ///
    /// <para>#5633 item 18: one row per session and wait type per snapshot, with <c>ThreadCount</c>; a blocker equal to the session is
    /// <c>IntraQueryParallelism</c>, not a blocker. Darling's twin is <c>DarlingSessionReader.WaitingTasksSql</c>.</para>
    /// <para>#5633 review L-a: <c>WaitDurationMs</c>, <c>ResourceDescription</c> and <c>BlockingSessionId</c> are the LONGEST-waiting thread's
    /// (an ordered <c>first</c>), not three independent MAXes; when that thread waits on its own session the blocker is the longest-waiting thread
    /// blocked by another session. Review round 2 Low-2: every column takes its thread through the SAME ordering as Darling's ordered
    /// <c>array_agg</c> (wait DESC NULLS LAST, then the blocker ASC NULLS LAST, then the resource DESC NULLS LAST), the fallback blocker
    /// included, so equal waits and NULL waits give the row PostgreSQL gives; <c>arg_max</c> picked an arbitrary tie and ignored NULL waits. <c>BlockingSessionIds</c> lists every distinct other blocker, only when there is more than one.</para>
    /// <para>The cap is a PARAMETER (#3541 A3), null for the grid callers that read the whole window as they
    /// always did. <c>get_waiting_tasks</c> read this UNBOUNDED and then took <c>limit</c> rows in C#, so a busy
    /// window materialised every waiting-task row to return thirty, and the envelope stated no bound at all.
    /// The tool now passes <c>limit + 1</c> and reads the extra row as truncation.</para>
    /// </summary>
    public async Task<List<WaitingTaskRow>> GetWaitingTasksAsync(int serverId, int hoursBack = 1, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null, int? limit = null, bool includeMonitor = false)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* #1240 parity: exclude the user's ignored (benign) wait types at DISPLAY time too, so waiting
           tasks already in the DuckDB (collected before a type was ignored) don't surface here — the same
           BuildExclusionClause the wait-stats reads use. */
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);

        /* The window's upper edge is $3, so the optional database list starts at $4. Bounding both edges
           (rather than only the lower one) is what lets an as_of anchor mean anything here. The row cap, when
           one is given, binds LAST so the database list keeps its ordinals whether or not a cap is present. */
        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var limitClause = limit.HasValue ? $"\nLIMIT ${4 + dbValues.Count}" : string.Empty;
        command.CommandText = $@"
SELECT
    collection_time,
    session_id,
    rtrim(wait_type) AS wait_type,
    MAX(wait_duration_ms) AS wait_duration_ms,
    COALESCE(
        first(CASE WHEN blocking_session_id <> session_id THEN blocking_session_id END
              ORDER BY wait_duration_ms DESC NULLS LAST, blocking_session_id ASC NULLS LAST),
        first(blocking_session_id ORDER BY wait_duration_ms DESC NULLS LAST, blocking_session_id ASC NULLS LAST)
            FILTER (WHERE blocking_session_id <> session_id)) AS blocking_session_id,
    first(resource_description
          ORDER BY wait_duration_ms DESC NULLS LAST, blocking_session_id ASC NULLS LAST, resource_description DESC NULLS LAST) AS resource_description,
    database_name,
    COUNT(*) AS thread_count,
    COALESCE(BOOL_OR(blocking_session_id = session_id), FALSE) AS intra_query_parallelism,
    CASE WHEN COUNT(DISTINCT blocking_session_id) FILTER (WHERE blocking_session_id <> session_id) > 1
         THEN list_aggregate(list_sort(list_distinct(list(blocking_session_id) FILTER (WHERE blocking_session_id <> session_id))), 'string_agg', ',')
    END AS blocking_session_ids
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3{dbClause}
{exclude}{MonitorSnapshotClause("program_name", includeMonitor)}
GROUP BY collection_time, session_id, rtrim(wait_type), database_name
ORDER BY collection_time DESC, MAX(wait_duration_ms) DESC{limitClause}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        if (limit.HasValue)
            command.Parameters.Add(new DuckDBParameter { Value = limit.Value });

        var items = new List<WaitingTaskRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitingTaskRow
            {
                CollectionTime = reader.GetDateTime(0),
                SessionId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                WaitType = reader.IsDBNull(2) ? "" : reader.GetString(2),
                WaitDurationMs = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                BlockingSessionId = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4),
                ResourceDescription = reader.IsDBNull(5) ? "" : reader.GetString(5),
                DatabaseName = reader.IsDBNull(6) ? "" : reader.GetString(6),
                ThreadCount = reader.IsDBNull(7) ? 1 : (int)ToInt64(reader.GetValue(7)),
                IntraQueryParallelism = !reader.IsDBNull(8) && reader.GetBoolean(8),
                BlockingSessionIds = reader.IsDBNull(9)
                    ? null
                    : reader.GetString(9).Split(',', StringSplitOptions.RemoveEmptyEntries).Select(int.Parse).ToArray()
            });
        }

        return items;
    }

    /// <summary>
    /// How many series <c>get_current_waits_trend</c> will draw in the window (#7904): the wait types, and the databases with
    /// blocked sessions under <paramref name="databaseNames"/>. The bucket width is sized for their sum, because the point budget
    /// counts points across every series. Same filters as <see cref="GetCurrentWaitsWaitPointsAsync"/> and
    /// <see cref="GetCurrentWaitsBlockedPointsAsync"/>; Darling's twin is <c>DarlingDataReader.CountWaitingTaskSeriesAsync</c> plus <c>CountBlockedSessionSeriesAsync</c>.
    /// </summary>
    public async Task<int> CountCurrentWaitsSeriesAsync(int serverId, int hoursBack, DateTime? asOfUtc, IReadOnlyList<string>? databaseNames)
    {
        using var connection = await OpenConnectionAsync();
        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        var total = 0;

        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
SELECT COUNT(DISTINCT rtrim(wait_type))
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   wait_type IS NOT NULL
{exclude}";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });
            var waitTypes = await command.ExecuteScalarAsync();
            total += waitTypes is null or DBNull ? 0 : Convert.ToInt32(waitTypes);
        }

        using (var command = connection.CreateCommand())
        {
            var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
            command.CommandText = @"
SELECT COUNT(DISTINCT database_name)
FROM v_waiting_tasks
WHERE server_id = $1
AND   blocking_session_id > 0
AND   blocking_session_id <> session_id
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
AND   database_name IS NOT NULL";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });
            foreach (var db in dbValues)
                command.Parameters.Add(new DuckDBParameter { Value = db });
            var databases = await command.ExecuteScalarAsync();
            total += databases is null or DBNull ? 0 : Convert.ToInt32(databases);
        }

        return total;
    }

    /// <summary>
    /// <c>get_current_waits_trend</c>'s waiting-task series (#7877): per wait type, bucketed to <paramref name="bucketMinutes"/>. The inner read keeps the
    /// per-collection total (the wait type's summed duration in ONE collection); the outer read averages those over EVERY collection
    /// that ran in the bucket (#7974), a collection in which the wait type was absent counting 0, and keeps the largest as the peak, so a wide bucket never reads as a bigger wait
    /// than one collection and a wait type seen in 1 of 60 collections reads 1/60 of one seen in all 60. The run count M is the bucket's SUCCESS runs of the
    /// <c>waiting_tasks</c> collector in <c>v_collection_log</c> (a run that found nothing waiting writes no row but is logged), floored at the series' own row
    /// count so a log older than the data never divides by fewer collections than the series was seen in; <c>collection_count</c> is M. (The desktop chart's
    /// <see cref="GetWaitingTaskTrendAsync"/> sums per bucket on purpose and is not this read.) Darling's twin is <c>DarlingDataReader.GetWaitingTaskTrendAsync</c>.
    /// </summary>
    internal async Task<List<CurrentWaitsWaitPoint>> GetCurrentWaitsWaitPointsAsync(int serverId, int hoursBack, DateTime? asOfUtc, int bucketMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        rtrim(wait_type) AS wait_type,
        SUM(wait_duration_ms) AS total_wait_ms
    FROM v_waiting_tasks
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   wait_type IS NOT NULL
{exclude}
    GROUP BY
        collection_time,
        rtrim(wait_type)
),
bucketed AS
(
    SELECT
        GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
        wait_type,
        SUM(total_wait_ms) AS sum_wait_ms,
        MAX(total_wait_ms) AS peak_wait_ms,
        COUNT(*) AS present_count
    FROM per_collection
    GROUP BY
        1, wait_type
),
runs AS
(
    SELECT
        GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
        COUNT(*) AS run_count
    FROM v_collection_log
    WHERE server_id = $1
    AND   collector_name = 'waiting_tasks'
    AND   status = 'SUCCESS'
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY
        1
)
SELECT
    b.bucket_start,
    b.wait_type,
    ROUND(CAST(b.sum_wait_ms AS DOUBLE) / GREATEST(COALESCE(r.run_count, 0), b.present_count)) AS avg_wait_ms,
    b.peak_wait_ms,
    GREATEST(COALESCE(r.run_count, 0), b.present_count) AS collection_count
FROM bucketed AS b
LEFT JOIN runs AS r
    ON r.bucket_start = b.bucket_start
ORDER BY
    b.bucket_start, b.wait_type";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var items = new List<CurrentWaitsWaitPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CurrentWaitsWaitPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                Convert.ToInt64(reader.GetValue(4))));
        }

        return items;
    }

    /// <summary>
    /// <c>get_current_waits_trend</c>'s blocked-session series (#7877): per database, bucketed to <paramref name="bucketMinutes"/>. The inner read is
    /// <see cref="GetBlockedSessionTrendAsync"/>'s per-collection distinct blocked sessions (#7577); the outer read averages them over EVERY collection that ran
    /// in the bucket (a gauge is never summed, #4349; a collection with none blocked for the database counts 0, #7974, over the same run count as
    /// <see cref="GetCurrentWaitsWaitPointsAsync"/>) and keeps the most any one collection saw as the peak. Darling's twin is
    /// <c>DarlingDataReader.GetBlockedSessionTrendAsync</c>.
    /// </summary>
    internal async Task<List<CurrentWaitsBlockedPoint>> GetCurrentWaitsBlockedPointsAsync(
        int serverId, int hoursBack, DateTime? asOfUtc, IReadOnlyList<string>? databaseNames, int bucketMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var widthParam = 4 + dbValues.Count;

        command.CommandText = $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        database_name,
        COUNT(DISTINCT session_id) AS blocked_count
    FROM v_waiting_tasks
    WHERE server_id = $1
    AND   blocking_session_id > 0
    AND   blocking_session_id <> session_id
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + $@"
    AND   database_name IS NOT NULL
    GROUP BY
        collection_time,
        database_name
),
bucketed AS
(
    SELECT
        GREATEST(time_bucket(to_minutes(CAST(${widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
        database_name,
        SUM(blocked_count) AS sum_blocked_count,
        MAX(blocked_count) AS peak_blocked_count,
        COUNT(*) AS present_count
    FROM per_collection
    GROUP BY
        1, database_name
),
runs AS
(
    SELECT
        GREATEST(time_bucket(to_minutes(CAST(${widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
        COUNT(*) AS run_count
    FROM v_collection_log
    WHERE server_id = $1
    AND   collector_name = 'waiting_tasks'
    AND   status = 'SUCCESS'
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY
        1
)
SELECT
    b.bucket_start,
    b.database_name,
    CAST(b.sum_blocked_count AS DOUBLE) / GREATEST(COALESCE(r.run_count, 0), b.present_count) AS avg_blocked_count,
    b.peak_blocked_count,
    GREATEST(COALESCE(r.run_count, 0), b.present_count) AS collection_count
FROM bucketed AS b
LEFT JOIN runs AS r
    ON r.bucket_start = b.bucket_start
ORDER BY
    b.bucket_start, b.database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var items = new List<CurrentWaitsBlockedPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CurrentWaitsBlockedPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                Convert.ToInt64(reader.GetValue(4))));
        }

        return items;
    }

    /// <summary>
    /// Whether the waiting-task collector has EVER sampled this server, ignoring any window.
    /// <para>Separates an all-clear from missing data. The wrong answer here is the REASSURING one:
    /// "nothing was waiting" stops a caller looking, where "never collected" sends them to check the
    /// collector. Darling's twin is <c>DarlingDataReader.HasAnyWaitingTaskSampleAsync</c>.</para>
    /// <para>Reads <c>v_waiting_tasks</c>, the same source every other reader in this file uses. A probe
    /// on the base table could report that a server has been sampled for rows the trend itself cannot
    /// see, which would pick the wrong branch in exactly the case this exists to get right.</para>
    /// </summary>
    public async Task<bool> HasAnyWaitingTaskSampleAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_waiting_tasks
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets waiting task duration trend grouped by wait type for charting.
    /// <para>#4349: bucketed (matching #4234/#4340's shape). A waiting-task snapshot carries no delta or
    /// sample interval, so unlike the #3540 trend family every row is unconditionally "rated" — a bucket's
    /// total is the SUM of its rows' durations, and <c>collection_count</c> is a plain <c>COUNT(*)</c>.
    /// Buckets to <see cref="TrendBudget.Chart"/>'s point budget per wait type; when every bucket the call
    /// returns holds exactly one physical collection, every point is stamped at its own raw collection time
    /// instead of the <c>time_bucket</c> grid line.</para>
    /// </summary>
    public async Task<List<WaitingTaskTrendPoint>> GetWaitingTaskTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        /* #1240 parity: exclude the user's ignored (benign) wait types at DISPLAY time (mirrors the
           wait-stats reads) so the Current Waits duration chart matches the Wait Stats tab. Keyed on
           rtrim(wait_type), so a name stored with the DMV's trailing space is the same series. */
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    rtrim(wait_type) AS wait_type,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(wait_duration_ms) AS total_wait_ms,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   wait_type IS NOT NULL
{exclude}
GROUP BY
    rtrim(wait_type), 2
ORDER BY
    rtrim(wait_type), 2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, long TotalWaitMs)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (Convert.ToInt64(reader.GetValue(4)) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2))));
        }

        var items = new List<WaitingTaskTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new WaitingTaskTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                WaitType = row.WaitType,
                TotalWaitMs = row.TotalWaitMs
            });
        }
        return items;
    }

    /// <summary>
    /// Gets blocked session count trend grouped by database for charting.
    /// <para>#7577: counts DISTINCT sessions with a waiting task blocked by ANOTHER session (<c>blocking_session_id &lt;> session_id</c>);
    /// a parallel query's exchange threads wait on their own session and are not blocked.</para>
    /// <para>#4349: bucketed (matching #4234/#4340's shape). A blocked-session count is a PER-SNAPSHOT
    /// gauge, not a delta — a bucket's value is the AVERAGE of the per-collection counts it covers
    /// (rounded, matching the CPU tab's gauge-averaging idiom), never their SUM: summing would double (or
    /// N-tuple) the count purely because a wide bucket merged N snapshots, with no more blocking having
    /// happened. The inner per-collection subquery keeps the pre-bucket per-collection count exactly as the
    /// un-bucketed read computed it; <c>collection_count</c> is the number of distinct physical collections
    /// the bucket merged.</para>
    /// </summary>
    public async Task<List<BlockedSessionTrendPoint>> GetBlockedSessionTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var widthParam = 4 + dbValues.Count;

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        database_name,
        COUNT(DISTINCT session_id) AS blocked_count
    FROM v_waiting_tasks
    WHERE server_id = $1
    AND   blocking_session_id > 0
    AND   blocking_session_id <> session_id
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + $@"
    AND   database_name IS NOT NULL
    GROUP BY
        collection_time,
        database_name
)
SELECT
    database_name,
    GREATEST(time_bucket(to_minutes(CAST(${widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    ROUND(AVG(blocked_count)) AS blocked_count,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM per_collection
GROUP BY
    database_name, 2
ORDER BY
    database_name, 2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string DatabaseName, DateTime BucketStart, DateTime FirstCollectionTime, int BlockedCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (Convert.ToInt64(reader.GetValue(4)) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2))));
        }

        var items = new List<BlockedSessionTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new BlockedSessionTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                DatabaseName = row.DatabaseName,
                BlockedCount = row.BlockedCount
            });
        }
        return items;
    }
}

public class WaitingTaskRow
{
    public DateTime CollectionTime { get; set; }
    public int SessionId { get; set; }
    public string WaitType { get; set; } = "";
    public long WaitDurationMs { get; set; }
    public int? BlockingSessionId { get; set; }
    public string ResourceDescription { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    /// <summary>#5633 item 18: waiting tasks (threads) rolled into this row.</summary>
    public int ThreadCount { get; set; } = 1;
    /// <summary>#5633 item 18: some of those threads wait on their own session (a parallel query's exchange waits); not a blocker.</summary>
    public bool IntraQueryParallelism { get; set; }
    /// <summary>#5633 review L-a: every distinct blocker other than the session itself, set only when the threads have more than one;
    /// <see cref="BlockingSessionId"/>, <see cref="ResourceDescription"/> and <see cref="WaitDurationMs"/> are the longest-waiting thread's.</summary>
    public IReadOnlyList<int>? BlockingSessionIds { get; set; }

    public string WaitDurationFormatted => WaitDurationMs < 1000
        ? $"{WaitDurationMs} ms"
        : WaitDurationMs < 60000
            ? $"{WaitDurationMs / 1000.0:F1} s"
            : $"{WaitDurationMs / 60000.0:F1} min";
}

public class WaitingTaskTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string WaitType { get; set; } = "";
    public long TotalWaitMs { get; set; }
}

public class BlockedSessionTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string DatabaseName { get; set; } = "";
    public int BlockedCount { get; set; }
}
