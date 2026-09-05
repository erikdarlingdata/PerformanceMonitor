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
    /// Gets the most recent memory stats snapshot for a server.
    /// </summary>
    public async Task<MemoryStatsRow?> GetLatestMemoryStatsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    total_physical_memory_mb,
    available_physical_memory_mb,
    total_page_file_mb,
    available_page_file_mb,
    system_memory_state,
    sql_memory_model,
    target_server_memory_mb,
    total_server_memory_mb,
    buffer_pool_mb,
    plan_cache_mb
FROM v_memory_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return new MemoryStatsRow
        {
            CollectionTime = reader.GetDateTime(0),
            TotalPhysicalMemoryMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
            AvailablePhysicalMemoryMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
            TotalPageFileMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
            AvailablePageFileMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
            SystemMemoryState = reader.IsDBNull(5) ? "" : reader.GetString(5),
            SqlMemoryModel = reader.IsDBNull(6) ? "" : reader.GetString(6),
            TargetServerMemoryMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
            TotalServerMemoryMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
            BufferPoolMb = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
            PlanCacheMb = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10))
        };
    }

    /// <summary>
    /// Gets memory stats trend for charting.
    /// </summary>
    public async Task<List<MemoryTrendPoint>> GetMemoryTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    collection_time,
    total_server_memory_mb,
    target_server_memory_mb,
    buffer_pool_mb,
    plan_cache_mb
FROM v_memory_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
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
                TotalServerMemoryMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                TargetServerMemoryMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                BufferPoolMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                PlanCacheMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4))
            });
        }

        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a memory sample, ignoring any window.
    /// <para>Lets an empty memory trend say WHICH kind of nothing it found. "No memory trend data" is true
    /// both of a quiet window and of a server the collector has never touched, and those want opposite
    /// responses — widen the window, versus go find out why collection is not running. Reads
    /// <c>v_memory_stats</c>, the same source <see cref="GetMemoryTrendAsync"/> reads, so it can never
    /// report "collected" for rows the trend cannot see. Darling's twin is
    /// <c>DarlingTrendReader.HasAnyMemoryStatAsync</c>; the two must stay in step so a user moving between
    /// the SKUs is not told a different story about the same state.</para>
    /// </summary>
    public async Task<bool> HasAnyMemoryStatAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_memory_stats
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets the distinct memory clerk types collected for a server, ordered by total memory descending.
    /// </summary>
    public async Task<List<string>> GetDistinctMemoryClerkTypesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    clerk_type
FROM v_memory_clerks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY clerk_type
ORDER BY SUM(memory_mb) DESC";

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
    /// Fetches the memory clerk trend for ALL selected clerk types in ONE query
    /// (replacing an N+1 query-per-clerk loop), grouped by clerk type.
    /// </summary>
    public async Task<Dictionary<string, List<MemoryClerkTrendPoint>>> GetMemoryClerkTrendsByTypesAsync(int serverId, List<string> clerkTypes, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetMemoryClerkTrendsByTypesAsync", "v_memory_clerks trends batched by type");
        var result = new Dictionary<string, List<MemoryClerkTrendPoint>>();
        if (clerkTypes.Count == 0) return result;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);
        var typeParams = string.Join(", ", clerkTypes.Select((_, i) => "$" + (i + 4)));

        command.CommandText = $@"
SELECT
    clerk_type,
    collection_time,
    memory_mb
FROM v_memory_clerks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   clerk_type IN ({typeParams})
ORDER BY clerk_type, collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var ct in clerkTypes)
            command.Parameters.Add(new DuckDBParameter { Value = ct });

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var ct = reader.GetString(0);
            if (!result.TryGetValue(ct, out var list))
            {
                list = new List<MemoryClerkTrendPoint>();
                result[ct] = list;
            }
            list.Add(new MemoryClerkTrendPoint
            {
                CollectionTime = reader.GetDateTime(1),
                MemoryMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }

        return result;
    }

    /// <summary>
    /// Gets memory pressure events (from RING_BUFFER_RESOURCE_MONITOR) for charting.
    /// </summary>
    public async Task<List<MemoryPressureEventRow>> GetMemoryPressureEventsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    sample_time,
    memory_notification,
    memory_indicators_process,
    memory_indicators_system
FROM v_memory_pressure_events
WHERE server_id = $1
AND   sample_time >= $2
AND   sample_time <= $3
ORDER BY sample_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<MemoryPressureEventRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryPressureEventRow
            {
                SampleTime = reader.GetDateTime(0),
                MemoryNotification = reader.IsDBNull(1) ? "" : reader.GetString(1),
                MemoryIndicatorsProcess = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                MemoryIndicatorsSystem = reader.IsDBNull(3) ? 0 : reader.GetInt32(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest memory clerk breakdown.
    /// </summary>
    public async Task<List<MemoryClerkRow>> GetLatestMemoryClerksAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT clerk_type, memory_mb
FROM v_memory_clerks
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_memory_clerks WHERE server_id = $1)
ORDER BY memory_mb DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<MemoryClerkRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryClerkRow
            {
                ClerkType = reader.GetString(0),
                MemoryMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1))
            });
        }

        return items;
    }
}

public class MemoryStatsRow
{
    public DateTime CollectionTime { get; set; }
    public double TotalPhysicalMemoryMb { get; set; }
    public double AvailablePhysicalMemoryMb { get; set; }
    public double TotalPageFileMb { get; set; }
    public double AvailablePageFileMb { get; set; }
    public string SystemMemoryState { get; set; } = "";
    public string SqlMemoryModel { get; set; } = "";
    public double TargetServerMemoryMb { get; set; }
    public double TotalServerMemoryMb { get; set; }
    public double BufferPoolMb { get; set; }
    public double PlanCacheMb { get; set; }
    public double UsedPhysicalMemoryMb => TotalPhysicalMemoryMb - AvailablePhysicalMemoryMb;
    public double MemoryUtilizationPercent => TotalPhysicalMemoryMb > 0 ? UsedPhysicalMemoryMb / TotalPhysicalMemoryMb * 100 : 0;
}

public class MemoryTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double TotalServerMemoryMb { get; set; }
    public double TargetServerMemoryMb { get; set; }
    public double BufferPoolMb { get; set; }
    public double PlanCacheMb { get; set; }
    public double TotalGrantedMb { get; set; }
}

public class MemoryClerkRow
{
    public string ClerkType { get; set; } = "";
    public double MemoryMb { get; set; }
    public string MemoryFormatted => MemoryMb >= 1024 ? $"{MemoryMb / 1024:F1} GB" : $"{MemoryMb:F1} MB";
}

public class MemoryClerkTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string ClerkType { get; set; } = "";
    public double MemoryMb { get; set; }
}

public class MemoryPressureEventRow
{
    public DateTime SampleTime { get; set; }
    public string MemoryNotification { get; set; } = "";
    public int MemoryIndicatorsProcess { get; set; }
    public int MemoryIndicatorsSystem { get; set; }
}
