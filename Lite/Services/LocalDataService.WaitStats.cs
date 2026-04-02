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

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets aggregated wait stats for a server over a time period, sorted by delta wait time.
    /// </summary>
    public async Task<List<WaitStatsRow>> GetWaitStatsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetWaitStatsAsync", "v_wait_stats top by delta");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
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
    /// Gets the distinct wait types that have been collected for a server.
    /// </summary>
    public async Task<List<string>> GetDistinctWaitTypesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    wait_type,
    SUM(delta_wait_time_ms) AS total_delta
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
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
    public async Task<List<WaitStatsTrendPoint>> GetWaitStatsTrendAsync(int serverId, string waitType, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        delta_wait_time_ms,
        delta_signal_wait_time_ms,
        delta_waiting_tasks,
        date_diff('second', LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   wait_type = $2
    AND   collection_time >= $3
    AND   collection_time <= $4
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE) / interval_seconds ELSE 0 END AS wait_time_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE) / interval_seconds ELSE 0 END AS signal_wait_time_ms_per_second,
    CASE WHEN delta_waiting_tasks > 0 THEN CAST(delta_wait_time_ms AS DOUBLE) / delta_waiting_tasks ELSE 0 END AS avg_ms_per_wait
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
    THEN CAST(delta_wait_time_ms AS DOUBLE) / delta_waiting_tasks
    ELSE 0 END AS avg_ms_per_wait
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
                AvgMsPerWait = reader.IsDBNull(3) ? 0 : reader.GetDouble(3)
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
SELECT
    session_id,
    database_name,
    elapsed_time_formatted,
    query_text,
    status,
    blocking_session_id,
    wait_type,
    wait_time_ms,
    wait_resource,
    cpu_time_ms,
    total_elapsed_time_ms,
    reads,
    writes,
    logical_reads,
    granted_query_memory_gb,
    transaction_isolation_level,
    dop,
    parallel_worker_count,
    query_plan,
    live_query_plan,
    collection_time,
    login_name,
    host_name,
    program_name,
    open_transaction_count,
    percent_complete
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   wait_type = $4
ORDER BY wait_time_ms DESC
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
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25))
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
SELECT
    session_id,
    database_name,
    elapsed_time_formatted,
    query_text,
    status,
    blocking_session_id,
    wait_type,
    wait_time_ms,
    wait_resource,
    cpu_time_ms,
    total_elapsed_time_ms,
    reads,
    writes,
    logical_reads,
    granted_query_memory_gb,
    transaction_isolation_level,
    dop,
    parallel_worker_count,
    query_plan,
    live_query_plan,
    collection_time,
    login_name,
    host_name,
    program_name,
    open_transaction_count,
    percent_complete
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY collection_time DESC
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
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25))
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
        bool excludeMiscWaits = true)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var thresholdMs = (long)thresholdMinutes * 60 * 1000;

        string spServerDiagnosticsFilter = excludeSpServerDiagnostics
            ? "AND r.wait_type NOT LIKE N'%SP_SERVER_DIAGNOSTICS%'" : "";
        string waitForFilter = excludeWaitFor
            ? "AND r.wait_type NOT IN (N'WAITFOR', N'BROKER_RECEIVE_WAITFOR')" : "";
        string backupsFilter = excludeBackups
            ? "AND r.wait_type NOT IN (N'BACKUPTHREAD', N'BACKUPIO')" : "";
        string miscWaitsFilter = excludeMiscWaits
            ? "AND r.wait_type NOT IN (N'XE_LIVE_TARGET_TVF')" : "";
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
                    r.blocking_session_id
                FROM v_query_snapshots AS r
                WHERE r.server_id = $1
                    AND r.collection_time = (SELECT MAX(vqs.collection_time) FROM v_query_snapshots AS vqs WHERE vqs.server_id = $1)
                    AND r.collection_time >= NOW() - INTERVAL '10 MINUTES'
                    AND r.session_id > 50
                    {spServerDiagnosticsFilter}
                    {waitForFilter}
                    {backupsFilter}
                    {miscWaitsFilter}
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
                BlockingSessionId = reader.IsDBNull(8) ? null : (int?)reader.GetInt32(8)
            });
        }

        return items;
    }
}

public class LongRunningQueryInfo
{
    public int SessionId { get; set; }
    public string DatabaseName { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string ProgramName { get; set; } = "";
    public long ElapsedSeconds { get; set; }
    public long CpuTimeMs { get; set; }
    public long Reads { get; set; }
    public long Writes { get; set; }
    public string? WaitType { get; set; }
    public int? BlockingSessionId { get; set; }
}

public class PoisonWaitDelta
{
    public string WaitType { get; set; } = "";
    public long DeltaMs { get; set; }
    public long DeltaTasks { get; set; }
    public double AvgMsPerWait { get; set; }
}

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
    public bool IsHighWait => TotalWaitTimeMs > 60000;

    private static string FormatMs(long ms)
    {
        if (ms < 1000) return $"{ms} ms";
        if (ms < 60000) return $"{ms / 1000.0:F1} sec";
        if (ms < 3600000) return $"{ms / 60000.0:F1} min";
        return $"{ms / 3600000.0:F1} hr";
    }
}

public class SelectableItem
{
    public string DisplayName { get; set; } = "";
    public bool IsSelected { get; set; }
}

public class WaitStatsTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double WaitTimeMsPerSecond { get; set; }
    public double SignalWaitTimeMsPerSecond { get; set; }
    public double AvgMsPerWait { get; set; }
}
