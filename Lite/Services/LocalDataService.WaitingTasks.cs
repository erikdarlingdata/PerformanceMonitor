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
    public async Task<List<WaitingTaskRow>> GetWaitingTasksAsync(int serverId, int hoursBack = 1, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* #1240 parity: exclude the user's ignored (benign) wait types at DISPLAY time too, so waiting
           tasks already in the DuckDB (collected before a type was ignored) don't surface here — the same
           BuildExclusionClause the wait-stats reads use. */
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);

        /* The window's upper edge is $3, so the optional database list starts at $4. Bounding both edges
           (rather than only the lower one) is what lets an as_of anchor mean anything here. */
        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        command.CommandText = $@"
SELECT
    collection_time,
    session_id,
    wait_type,
    wait_duration_ms,
    blocking_session_id,
    resource_description,
    database_name
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3{dbClause}
{exclude}
ORDER BY collection_time DESC, wait_duration_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

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
    /// </summary>
    public async Task<List<WaitingTaskTrendPoint>> GetWaitingTaskTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        /* #1240 parity: exclude the user's ignored (benign) wait types at DISPLAY time (mirrors the
           wait-stats reads) so the Current Waits duration chart matches the Wait Stats tab. */
        var exclude = IgnoredWaitTypes.BuildExclusionClause(_ignoredWaitTypes.Value);
        command.CommandText = $@"
SELECT
    collection_time,
    wait_type,
    SUM(wait_duration_ms) AS total_wait_ms
FROM v_waiting_tasks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   wait_type IS NOT NULL
{exclude}
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
    public async Task<List<BlockedSessionTrendPoint>> GetBlockedSessionTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    collection_time,
    database_name,
    COUNT(*) AS blocked_count
FROM v_waiting_tasks
WHERE server_id = $1
AND   blocking_session_id > 0
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
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
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

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
