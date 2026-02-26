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
    /// Gets memory grant trend — total granted MB per collection snapshot for the Memory Overview overlay.
    /// </summary>
    public async Task<List<MemoryTrendPoint>> GetMemoryGrantTrendAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    0 AS total_server_memory_mb,
    0 AS target_server_memory_mb,
    0 AS buffer_pool_mb,
    SUM(granted_memory_mb) AS total_granted_mb
FROM memory_grant_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY collection_time
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<MemoryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                TotalGrantedMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets memory grant chart data aggregated by collection_time and pool_id
    /// for the Memory Grants sub-tab charts.
    /// </summary>
    public async Task<List<MemoryGrantChartPoint>> GetMemoryGrantChartDataAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    pool_id,
    SUM(available_memory_mb) AS available_memory_mb,
    SUM(granted_memory_mb) AS granted_memory_mb,
    SUM(used_memory_mb) AS used_memory_mb,
    SUM(grantee_count) AS grantee_count,
    SUM(waiter_count) AS waiter_count,
    SUM(timeout_error_count_delta) AS timeout_error_count_delta,
    SUM(forced_grant_count_delta) AS forced_grant_count_delta
FROM memory_grant_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY collection_time, pool_id
ORDER BY collection_time, pool_id";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<MemoryGrantChartPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryGrantChartPoint
            {
                CollectionTime = reader.GetDateTime(0),
                PoolId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                AvailableMemoryMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                GrantedMemoryMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                UsedMemoryMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                GranteeCount = reader.IsDBNull(5) ? 0 : (int)ToInt64(reader.GetValue(5)),
                WaiterCount = reader.IsDBNull(6) ? 0 : (int)ToInt64(reader.GetValue(6)),
                TimeoutErrorCountDelta = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                ForcedGrantCountDelta = reader.IsDBNull(8) ? 0 : ToInt64(reader.GetValue(8))
            });
        }
        return items;
    }
}

public class MemoryGrantChartPoint
{
    public DateTime CollectionTime { get; set; }
    public int PoolId { get; set; }
    public double AvailableMemoryMb { get; set; }
    public double GrantedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public int GranteeCount { get; set; }
    public int WaiterCount { get; set; }
    public long TimeoutErrorCountDelta { get; set; }
    public long ForcedGrantCountDelta { get; set; }
}
