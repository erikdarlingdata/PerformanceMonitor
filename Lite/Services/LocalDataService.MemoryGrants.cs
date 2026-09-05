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

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    collection_time,
    0 AS total_server_memory_mb,
    0 AS target_server_memory_mb,
    0 AS buffer_pool_mb,
    SUM(granted_memory_mb) AS total_granted_mb
FROM v_memory_grant_stats
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
    public async Task<List<MemoryGrantChartPoint>> GetMemoryGrantChartDataAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

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
FROM v_memory_grant_stats
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

    /// <summary>
    /// The resource-semaphore latest-snapshot read (the get_resource_semaphore MCP lens): every resource
    /// semaphore captured at the most recent collection in the window, with the full ceiling column set
    /// (target / max-target / total workspace memory, cumulative timeout/forced grant counts, and the
    /// per-interval deltas) — the same shape the Dashboard's get_resource_semaphore serves. Distinct from
    /// <see cref="GetMemoryGrantChartDataAsync"/>, which aggregates a per-pool grant subset for the charts;
    /// this returns one row per (pool_id, resource_semaphore_id) with the ceiling metrics intact.
    /// </summary>
    public async Task<List<ResourceSemaphoreRow>> GetResourceSemaphoreSnapshotAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
WITH latest AS
(
    SELECT MAX(collection_time) AS mx
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    collection_time,
    resource_semaphore_id,
    pool_id,
    target_memory_mb,
    max_target_memory_mb,
    total_memory_mb,
    available_memory_mb,
    granted_memory_mb,
    used_memory_mb,
    grantee_count,
    waiter_count,
    timeout_error_count,
    forced_grant_count,
    timeout_error_count_delta,
    forced_grant_count_delta
FROM v_memory_grant_stats
WHERE server_id = $1
AND   collection_time = (SELECT mx FROM latest)
ORDER BY pool_id, resource_semaphore_id";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<ResourceSemaphoreRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ResourceSemaphoreRow
            {
                CollectionTime = reader.GetDateTime(0),
                ResourceSemaphoreId = reader.IsDBNull(1) ? 0 : (int)ToInt64(reader.GetValue(1)),
                PoolId = reader.IsDBNull(2) ? 0 : (int)ToInt64(reader.GetValue(2)),
                TargetMemoryMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                MaxTargetMemoryMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalMemoryMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvailableMemoryMb = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                GrantedMemoryMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                UsedMemoryMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                GranteeCount = reader.IsDBNull(9) ? 0 : (int)ToInt64(reader.GetValue(9)),
                WaiterCount = reader.IsDBNull(10) ? 0 : (int)ToInt64(reader.GetValue(10)),
                TimeoutErrorCount = reader.IsDBNull(11) ? 0 : ToInt64(reader.GetValue(11)),
                ForcedGrantCount = reader.IsDBNull(12) ? 0 : ToInt64(reader.GetValue(12)),
                TimeoutErrorCountDelta = reader.IsDBNull(13) ? 0 : ToInt64(reader.GetValue(13)),
                ForcedGrantCountDelta = reader.IsDBNull(14) ? 0 : ToInt64(reader.GetValue(14))
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

/// <summary>One resource-semaphore latest-snapshot row (the get_resource_semaphore MCP lens): one
/// (pool_id, resource_semaphore_id) semaphore's full ceiling metrics at the most recent collection in the
/// window — target / max-target / total workspace memory, granted vs available/used, grantee/waiter counts,
/// and the cumulative + per-interval-delta timeout/forced-grant pressure counters.</summary>
public class ResourceSemaphoreRow
{
    public DateTime CollectionTime { get; set; }
    public int ResourceSemaphoreId { get; set; }
    public int PoolId { get; set; }
    public double TargetMemoryMb { get; set; }
    public double MaxTargetMemoryMb { get; set; }
    public double TotalMemoryMb { get; set; }
    public double AvailableMemoryMb { get; set; }
    public double GrantedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public int GranteeCount { get; set; }
    public int WaiterCount { get; set; }
    public long TimeoutErrorCount { get; set; }
    public long ForcedGrantCount { get; set; }
    public long TimeoutErrorCountDelta { get; set; }
    public long ForcedGrantCountDelta { get; set; }
}
