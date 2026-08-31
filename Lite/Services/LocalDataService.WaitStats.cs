/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /* #1240: the wait-stats tab shares the collector's ignored-wait list and excludes those types at
       DISPLAY time, so benign waits already in the DuckDB (collected before the filter was active) don't
       surface in the tab/picker — copying the JSON only stops new collection, not existing rows. */
    private readonly Lazy<HashSet<string>> _ignoredWaitTypes = new(IgnoredWaitTypes.Load);

    /// <summary>
    /// Gets aggregated wait stats for a server over a time period, sorted by delta wait time.
    /// </summary>
    public async Task<List<WaitStatsRow>> GetWaitStatsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetWaitStatsAsync", "v_wait_stats top by delta");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    wait_type,
    SUM(delta_waiting_tasks) AS total_waiting_tasks,
    SUM(delta_wait_time_ms) AS total_wait_time_ms,
    SUM(delta_signal_wait_time_ms) AS total_signal_wait_time_ms,
    COUNT(*) AS sample_count
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
{exclude}
GROUP BY wait_type
ORDER BY SUM(delta_wait_time_ms) DESC
LIMIT 50";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<WaitStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitStatsRow
            {
                WaitType = reader.GetString(0),
                TotalWaitingTasks = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                TotalWaitTimeMs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                TotalSignalWaitTimeMs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                SampleCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4)
            });
        }

        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a wait sample, ignoring any window.
    /// <para>Lets an empty get_wait_types say WHICH kind of nothing it found. "No wait types in the last N
    /// hours" is true both of a quiet window and of a server nothing has been stored for, and those want
    /// opposite responses — widen the window, versus go find out why collection is not running. Reads
    /// <c>v_wait_stats</c>, the same source <see cref="GetDistinctWaitTypesAsync"/> reads. Deliberately
    /// does NOT apply the ignored-wait-type exclusion the read applies: the question here is whether the
    /// COLLECTOR has stored anything, not whether anything survives the display filter. Darling's twin is
    /// <c>DarlingDataReader.HasAnyWaitStatAsync</c>; the two must stay in step so a user moving between the
    /// SKUs is not told a different story about the same state.</para>
    /// </summary>
    public async Task<bool> HasAnyWaitStatAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_wait_stats
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets the distinct wait types that have been collected for a server.
    /// </summary>
    public async Task<List<string>> GetDistinctWaitTypesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    wait_type,
    SUM(delta_wait_time_ms) AS total_delta
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
{exclude}
GROUP BY wait_type
ORDER BY SUM(delta_wait_time_ms) DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(reader.GetString(0));
        }
        return items;
    }

    /// <summary>
    /// Gets wait stats trend data for charting.
    /// </summary>
    public async Task<List<WaitStatsTrendPoint>> GetWaitStatsTrendAsync(int serverId, string waitType, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        delta_wait_time_ms,
        delta_signal_wait_time_ms,
        delta_waiting_tasks,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   wait_type = $2
    AND   collection_time >= $3
    AND   collection_time <= $4
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS signal_wait_time_ms_per_second,
    CASE WHEN delta_waiting_tasks > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / delta_waiting_tasks ELSE 0 END AS avg_ms_per_wait
FROM raw
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = waitType });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<WaitStatsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitStatsTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                WaitTimeMsPerSecond = reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                SignalWaitTimeMsPerSecond = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                AvgMsPerWait = reader.IsDBNull(3) ? 0 : reader.GetDouble(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Batched sibling of <see cref="GetWaitStatsTrendAsync"/>: fetches the per-second trend for
    /// ALL selected wait types in ONE query (replacing an N+1 query-per-type loop), grouped by type.
    /// The LAG window is partitioned by wait_type so each type's per-second rate is computed independently.
    /// </summary>
    public async Task<Dictionary<string, List<WaitStatsTrendPoint>>> GetWaitStatsTrendsByTypesAsync(int serverId, List<string> waitTypes, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetWaitStatsTrendsByTypesAsync", "v_wait_stats trends batched by type");
        var result = new Dictionary<string, List<WaitStatsTrendPoint>>();
        if (waitTypes.Count == 0) return result;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var typeParams = string.Join(", ", waitTypes.Select((_, i) => "$" + (i + 4)));

        command.CommandText = $@"
WITH raw AS
(
    SELECT
        wait_type,
        collection_time,
        delta_wait_time_ms,
        delta_signal_wait_time_ms,
        delta_waiting_tasks,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time)))) AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   wait_type IN ({typeParams})
)
SELECT
    wait_type,
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS signal_wait_time_ms_per_second,
    CASE WHEN delta_waiting_tasks > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / delta_waiting_tasks ELSE 0 END AS avg_ms_per_wait
FROM raw
ORDER BY wait_type, collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var wt in waitTypes)
            command.Parameters.Add(new DuckDBParameter { Value = wt });

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var wt = reader.GetString(0);
            if (!result.TryGetValue(wt, out var list))
            {
                list = new List<WaitStatsTrendPoint>();
                result[wt] = list;
            }
            list.Add(new WaitStatsTrendPoint
            {
                CollectionTime = reader.GetDateTime(1),
                WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                AvgMsPerWait = reader.IsDBNull(4) ? 0 : reader.GetDouble(4)
            });
        }

        return result;
    }

    /// <summary>
    /// Gets total wait time trend across all wait types as a single aggregated time-series.
    /// Used by the correlated timeline lanes for a single-line wait stats overview.
    /// </summary>
    public async Task<List<WaitStatsTrendPoint>> GetTotalWaitTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetTotalWaitTrendAsync", "wait_stats total trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
WITH per_collection AS
(
    SELECT
        collection_time,
        SUM(delta_wait_time_ms) AS total_delta_ms,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    {exclude}
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(total_delta_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second
FROM per_collection
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<WaitStatsTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitStatsTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                WaitTimeMsPerSecond = reader.IsDBNull(1) ? 0 : reader.GetDouble(1)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest poison wait deltas for alert checking.
    /// Returns entries where delta_waiting_tasks > 0 with computed avg ms per wait.
    /// </summary>
    public async Task<List<PoisonWaitDelta>> GetLatestPoisonWaitAvgsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    wait_type,
    delta_wait_time_ms AS delta_ms,
    delta_waiting_tasks AS delta_tasks,
    CASE WHEN delta_waiting_tasks > 0
    THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / delta_waiting_tasks
    ELSE 0 END AS avg_ms_per_wait,
    collection_time
FROM v_wait_stats
WHERE server_id = $1
AND wait_type IN ('THREADPOOL', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE')
AND delta_waiting_tasks > 0
AND collection_time >= $2
ORDER BY collection_time DESC
LIMIT 3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddMinutes(-10) });

        var items = new List<PoisonWaitDelta>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PoisonWaitDelta
            {
                WaitType = reader.GetString(0),
                DeltaMs = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                DeltaTasks = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                AvgMsPerWait = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                CollectionTime = reader.GetDateTime(4)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query snapshots filtered by wait type, for the wait drill-down feature.
    /// Returns sessions that were experiencing the specified wait type during the time range.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetQuerySnapshotsByWaitTypeAsync(
        int serverId, string waitType, int hoursBack = 24,
        DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH blocked_counts AS (
    SELECT collection_time, blocking_session_id AS blocker_session_id, COUNT(*) AS blocked_session_count
    FROM v_query_snapshots
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   blocking_session_id > 0
    GROUP BY collection_time, blocking_session_id
)
SELECT
    q.session_id,
    q.database_name,
    q.elapsed_time_formatted,
    q.query_text,
    q.status,
    q.blocking_session_id,
    q.wait_type,
    q.wait_time_ms,
    q.wait_resource,
    q.cpu_time_ms,
    q.total_elapsed_time_ms,
    q.reads,
    q.writes,
    q.logical_reads,
    q.granted_query_memory_gb,
    q.transaction_isolation_level,
    q.dop,
    q.parallel_worker_count,
    q.query_plan,
    q.live_query_plan,
    q.collection_time,
    q.login_name,
    q.host_name,
    q.program_name,
    q.open_transaction_count,
    q.percent_complete,
    q.query_hash,
    COALESCE(bc.blocked_session_count, 0) AS blocked_session_count,
    q.requested_memory_mb,
    q.used_memory_mb,
    q.max_used_memory_mb,
    q.tempdb_current_mb,
    q.tempdb_allocations_mb,
    q.tran_log_used_mb,
    q.tran_start_time,
    q.request_id
FROM v_query_snapshots q
LEFT JOIN blocked_counts bc
  ON  bc.collection_time = q.collection_time
  AND bc.blocker_session_id = q.session_id
WHERE q.server_id = $1
AND   q.collection_time >= $2
AND   q.collection_time <= $3
AND   q.wait_type = $4
ORDER BY q.wait_time_ms DESC
LIMIT 500";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = waitType });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                QueryPlan = reader.IsDBNull(18) ? null : reader.GetString(18),
                LiveQueryPlan = reader.IsDBNull(19) ? null : reader.GetString(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20),
                LoginName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                HostName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ProgramName = reader.IsDBNull(23) ? "" : reader.GetString(23),
                OpenTransactionCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25)),
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                BlockedSessionCount = reader.IsDBNull(27) ? 0 : Convert.ToInt32(reader.GetValue(27)),
                RequestedMemoryMb = reader.IsDBNull(28) ? 0 : ToDouble(reader.GetValue(28)),
                UsedMemoryMb = reader.IsDBNull(29) ? 0 : ToDouble(reader.GetValue(29)),
                MaxUsedMemoryMb = reader.IsDBNull(30) ? 0 : ToDouble(reader.GetValue(30)),
                TempdbCurrentMb = reader.IsDBNull(31) ? 0 : ToDouble(reader.GetValue(31)),
                TempdbAllocationsMb = reader.IsDBNull(32) ? 0 : ToDouble(reader.GetValue(32)),
                TranLogUsedMb = reader.IsDBNull(33) ? 0 : ToDouble(reader.GetValue(33)),
                TranStartTime = reader.IsDBNull(34) ? (DateTime?)null : reader.GetDateTime(34),
                RequestId = reader.IsDBNull(35) ? 0 : reader.GetInt32(35)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets ALL query snapshots in a time range (for chain walking).
    /// Used when a chain wait type (LCK_M_*, LATCH_EX/UP) needs blocking chain traversal.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetAllQuerySnapshotsInRangeAsync(
        int serverId, int hoursBack = 24,
        DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH blocked_counts AS (
    SELECT collection_time, blocking_session_id AS blocker_session_id, COUNT(*) AS blocked_session_count
    FROM v_query_snapshots
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   blocking_session_id > 0
    GROUP BY collection_time, blocking_session_id
)
SELECT
    q.session_id,
    q.database_name,
    q.elapsed_time_formatted,
    q.query_text,
    q.status,
    q.blocking_session_id,
    q.wait_type,
    q.wait_time_ms,
    q.wait_resource,
    q.cpu_time_ms,
    q.total_elapsed_time_ms,
    q.reads,
    q.writes,
    q.logical_reads,
    q.granted_query_memory_gb,
    q.transaction_isolation_level,
    q.dop,
    q.parallel_worker_count,
    q.query_plan,
    q.live_query_plan,
    q.collection_time,
    q.login_name,
    q.host_name,
    q.program_name,
    q.open_transaction_count,
    q.percent_complete,
    q.query_hash,
    COALESCE(bc.blocked_session_count, 0) AS blocked_session_count,
    q.requested_memory_mb,
    q.used_memory_mb,
    q.max_used_memory_mb,
    q.tempdb_current_mb,
    q.tempdb_allocations_mb,
    q.tran_log_used_mb,
    q.tran_start_time,
    q.request_id
FROM v_query_snapshots q
LEFT JOIN blocked_counts bc
  ON  bc.collection_time = q.collection_time
  AND bc.blocker_session_id = q.session_id
WHERE q.server_id = $1
AND   q.collection_time >= $2
AND   q.collection_time <= $3
ORDER BY q.collection_time DESC
LIMIT 2000";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                QueryPlan = reader.IsDBNull(18) ? null : reader.GetString(18),
                LiveQueryPlan = reader.IsDBNull(19) ? null : reader.GetString(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20),
                LoginName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                HostName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ProgramName = reader.IsDBNull(23) ? "" : reader.GetString(23),
                OpenTransactionCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25)),
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                BlockedSessionCount = reader.IsDBNull(27) ? 0 : Convert.ToInt32(reader.GetValue(27)),
                RequestedMemoryMb = reader.IsDBNull(28) ? 0 : ToDouble(reader.GetValue(28)),
                UsedMemoryMb = reader.IsDBNull(29) ? 0 : ToDouble(reader.GetValue(29)),
                MaxUsedMemoryMb = reader.IsDBNull(30) ? 0 : ToDouble(reader.GetValue(30)),
                TempdbCurrentMb = reader.IsDBNull(31) ? 0 : ToDouble(reader.GetValue(31)),
                TempdbAllocationsMb = reader.IsDBNull(32) ? 0 : ToDouble(reader.GetValue(32)),
                TranLogUsedMb = reader.IsDBNull(33) ? 0 : ToDouble(reader.GetValue(33)),
                TranStartTime = reader.IsDBNull(34) ? (DateTime?)null : reader.GetDateTime(34),
                RequestId = reader.IsDBNull(35) ? 0 : reader.GetInt32(35)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets long-running queries from the latest collection snapshot.
    /// Returns sessions whose total elapsed time exceeds the given threshold.
    /// </summary>
    public async Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
        int serverId,
        int thresholdMinutes,
        int maxResults = 5,
        bool excludeSpServerDiagnostics = true,
        bool excludeWaitFor = true,
        bool excludeBackups = true,
        bool excludeMiscWaits = true,
        bool excludeCdc = true)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var thresholdMs = (long)thresholdMinutes * 60 * 1000;

        // sp_server_diagnostics usually sits in SP_SERVER_DIAGNOSTICS_SLEEP, but it also does Extended Events work,
        // so it can be captured in a different wait (e.g. PREEMPTIVE_XE_GETTARGETSTATE) where the wait-type match
        // alone misses it and the alert fires; the query-text match (case-insensitive, NULL-safe) catches it
        // regardless of the wait it happens to be in at capture time.
        string spServerDiagnosticsFilter = excludeSpServerDiagnostics
            ? "AND r.wait_type NOT LIKE N'%SP_SERVER_DIAGNOSTICS%' AND (r.query_text IS NULL OR r.query_text NOT ILIKE N'%sp_server_diagnostics%')" : "";
        string waitForFilter = excludeWaitFor
            ? "AND r.wait_type NOT IN (N'WAITFOR', N'BROKER_RECEIVE_WAITFOR')" : "";
        string backupsFilter = excludeBackups
            ? "AND r.wait_type NOT IN (N'BACKUPTHREAD', N'BACKUPIO')" : "";
        string miscWaitsFilter = excludeMiscWaits
            ? "AND r.wait_type NOT IN (N'XE_LIVE_TARGET_TVF')" : "";
        // CDC capture sessions are flagged server-side by the collector (is_cdc_capture). COALESCE
        // guards pre-migration / archived rows where the column is NULL.
        string cdcFilter = excludeCdc
            ? "AND COALESCE(r.is_cdc_capture, FALSE) = FALSE" : "";
        maxResults = Math.Clamp(maxResults, 1, 1000);

        command.CommandText = @$"
                SELECT
                    r.session_id,
                    r.database_name,
                    SUBSTRING(r.query_text, 1, 300) AS query_text,
                    r.total_elapsed_time_ms / 1000 AS elapsed_seconds,
                    r.cpu_time_ms,
                    r.reads,
                    r.writes,
                    r.wait_type,
                    r.blocking_session_id,
                    r.query_hash,
                    r.program_name
                FROM v_query_snapshots AS r
                WHERE r.server_id = $1
                    AND r.collection_time = (SELECT MAX(vqs.collection_time) FROM v_query_snapshots AS vqs WHERE vqs.server_id = $1)
                    AND r.collection_time >= NOW() - INTERVAL '10 MINUTES'
                    AND r.session_id > 50
                    {spServerDiagnosticsFilter}
                    {waitForFilter}
                    {backupsFilter}
                    {miscWaitsFilter}
                    {cdcFilter}
                    AND r.total_elapsed_time_ms >= $2
                ORDER BY r.total_elapsed_time_ms DESC
                LIMIT $3;";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = thresholdMs });
        command.Parameters.Add(new DuckDBParameter { Value = maxResults });

        var items = new List<LongRunningQueryInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LongRunningQueryInfo
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ElapsedSeconds = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                CpuTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                Reads = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                Writes = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                WaitType = reader.IsDBNull(7) ? null : reader.GetString(7),
                BlockingSessionId = reader.IsDBNull(8) ? null : (int?)reader.GetInt32(8),
                QueryHash = reader.IsDBNull(9) ? null : reader.GetString(9),
                /* Phase-5 A reconciliation: populate ProgramName so the shared
                   BuildLongRunningQueryContext renders the ("Program", ...) detail item the
                   Dashboard's alert already had. */
                ProgramName = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        return items;
    }
}

/* LongRunningQueryInfo and PoisonWaitDelta moved to PerformanceMonitor.Alerting (Phase-5 A0);
   the bare names resolve through the global using aliases in GlobalUsings.cs. */

public class WaitStatsRow
{
    public string WaitType { get; set; } = "";
    public long TotalWaitingTasks { get; set; }
    public long TotalWaitTimeMs { get; set; }
    public long TotalSignalWaitTimeMs { get; set; }
    public long ResourceWaitTimeMs => TotalWaitTimeMs - TotalSignalWaitTimeMs;
    public long SampleCount { get; set; }
    public double AvgWaitMsPerTask => TotalWaitingTasks > 0 ? (double)TotalWaitTimeMs / TotalWaitingTasks : 0;
    public string AvgWaitMsFormatted => AvgWaitMsPerTask < 0.1 ? "< 0.1 ms" : $"{AvgWaitMsPerTask:F1} ms";
    public string TotalWaitTimeFormatted => FormatMs(TotalWaitTimeMs);
    public string SignalWaitTimeFormatted => FormatMs(TotalSignalWaitTimeMs);
    public string ResourceWaitTimeFormatted => FormatMs(ResourceWaitTimeMs);
    public double SignalWaitPercent => TotalWaitTimeMs > 0 ? (double)TotalSignalWaitTimeMs / TotalWaitTimeMs * 100 : 0;

    private static string FormatMs(long ms)
    {
        if (ms < 1000) return $"{ms} ms";
        if (ms < 60000) return $"{ms / 1000.0:F1} sec";
        if (ms < 3600000) return $"{ms / 60000.0:F1} min";
        return $"{ms / 3600000.0:F1} hr";
    }
}

public class WaitStatsTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double WaitTimeMsPerSecond { get; set; }
    public double SignalWaitTimeMsPerSecond { get; set; }
    public double AvgMsPerWait { get; set; }
}
