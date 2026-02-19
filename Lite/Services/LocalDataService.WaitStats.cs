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
ORDER BY collection_time DESC
LIMIT 3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

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
    /// Gets long-running queries from the latest collection snapshot.
    /// Returns sessions whose total elapsed time exceeds the given threshold.
    /// </summary>
    public async Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(int serverId, int thresholdMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var thresholdMs = (long)thresholdMinutes * 60 * 1000;

        command.CommandText = @"
SELECT
    session_id,
    database_name,
    SUBSTRING(query_text, 1, 300) AS query_text,
    total_elapsed_time_ms / 1000 AS elapsed_seconds,
    cpu_time_ms,
    reads,
    writes,
    wait_type,
    blocking_session_id
FROM v_query_snapshots
WHERE server_id = $1
AND collection_time = (SELECT MAX(collection_time) FROM v_query_snapshots WHERE server_id = $1)
AND session_id > 50
AND total_elapsed_time_ms >= $2
ORDER BY total_elapsed_time_ms DESC
LIMIT 5";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = thresholdMs });

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
