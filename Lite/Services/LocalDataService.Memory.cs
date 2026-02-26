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
    public async Task<List<MemoryTrendPoint>> GetMemoryTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

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
    /// Gets the distinct memory clerk types collected for a server, ordered by total memory descending.
    /// </summary>
    public async Task<List<string>> GetDistinctMemoryClerkTypesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

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
    /// Gets memory clerk trend data for a single clerk type for charting.
    /// </summary>
    public async Task<List<MemoryClerkTrendPoint>> GetMemoryClerkTrendAsync(int serverId, string clerkType, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    memory_mb
FROM v_memory_clerks
WHERE server_id = $1
AND   clerk_type = $2
AND   collection_time >= $3
AND   collection_time <= $4
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = clerkType });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<MemoryClerkTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryClerkTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                MemoryMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1))
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
