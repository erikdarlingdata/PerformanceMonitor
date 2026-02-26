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
    /// Gets recent waiting task snapshots for a server.
    /// </summary>
    public async Task<List<WaitingTaskRow>> GetWaitingTasksAsync(int serverId, int hoursBack = 1)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    session_id,
    wait_type,
    wait_duration_ms,
    blocking_session_id,
    resource_description,
    database_name
FROM waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
ORDER BY collection_time DESC, wait_duration_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });

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
                DatabaseName = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets waiting task duration trend grouped by wait type for charting.
    /// </summary>
    public async Task<List<WaitingTaskTrendPoint>> GetWaitingTaskTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    wait_type,
    SUM(wait_duration_ms) AS total_wait_ms
FROM waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   wait_type IS NOT NULL
GROUP BY
    collection_time,
    wait_type
ORDER BY
    collection_time,
    wait_type";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<WaitingTaskTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new WaitingTaskTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                WaitType = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalWaitMs = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets blocked session count trend grouped by database for charting.
    /// </summary>
    public async Task<List<BlockedSessionTrendPoint>> GetBlockedSessionTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    database_name,
    COUNT(*) AS blocked_count
FROM waiting_tasks
WHERE server_id = $1
AND   blocking_session_id > 0
AND   collection_time >= $2
AND   collection_time <= $3
AND   database_name IS NOT NULL
GROUP BY
    collection_time,
    database_name
ORDER BY
    collection_time,
    database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<BlockedSessionTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new BlockedSessionTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                BlockedCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2))
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
