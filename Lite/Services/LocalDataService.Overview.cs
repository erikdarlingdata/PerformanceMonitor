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
using System.Windows.Media;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets a summary of server health for the overview dashboard.
    /// </summary>
    public async Task<ServerSummaryItem?> GetServerSummaryAsync(int serverId, string displayName)
    {
        using var connection = await OpenConnectionAsync();

        double? cpuPercent = null;
        double? memoryMb = null;
        int blockingCount = 0;
        int deadlockCount = 0;
        DateTime? lastCollection = null;

        /* Latest CPU */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT sqlserver_cpu_utilization, sample_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
ORDER BY sample_time DESC
LIMIT 1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                cpuPercent = reader.IsDBNull(0) ? null : ToDouble(reader.GetValue(0));
                lastCollection = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
            }
        }

        /* Latest SQL Memory */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT total_server_memory_mb
FROM v_memory_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                memoryMb = reader.IsDBNull(0) ? null : ToDouble(reader.GetValue(0));
            }
        }

        /* Blocking count in last hour - uses XE blocked process reports */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT COUNT(*)
FROM v_blocked_process_reports
WHERE server_id = $1
AND   event_time >= $2";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-1) });
            var result = await cmd.ExecuteScalarAsync();
            blockingCount = result != null ? Convert.ToInt32(result) : 0;
        }

        /* Deadlock count in last hour */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT COUNT(*)
FROM v_deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-1) });
            var result = await cmd.ExecuteScalarAsync();
            deadlockCount = result != null ? Convert.ToInt32(result) : 0;
        }

        /* Last collection time from collection_log */
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SELECT MAX(collection_time)
FROM v_collection_log
WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            var result = await cmd.ExecuteScalarAsync();
            if (result != null && result != DBNull.Value)
            {
                lastCollection = Convert.ToDateTime(result);
            }
        }

        return new ServerSummaryItem
        {
            DisplayName = displayName,
            ServerId = serverId,
            CpuPercent = cpuPercent,
            MemoryMb = memoryMb,
            BlockingCount = blockingCount,
            DeadlockCount = deadlockCount,
            LastCollectionTime = lastCollection
        };
    }
}

public class ServerSummaryItem
{
    public string DisplayName { get; set; } = "";
    public string ServerName { get; set; } = "";
    public int ServerId { get; set; }
    public bool? IsOnline { get; set; }
    public double? CpuPercent { get; set; }
    public double? MemoryMb { get; set; }
    public int BlockingCount { get; set; }
    public int DeadlockCount { get; set; }
    public DateTime? LastCollectionTime { get; set; }

    public string CpuDisplay => CpuPercent.HasValue ? $"{CpuPercent:F0}%" : "--";
    public string MemoryDisplay => MemoryMb.HasValue ? $"{MemoryMb / 1024.0:F1} GB" : "--";
    public string BlockingDisplay => BlockingCount > 0 ? BlockingCount.ToString() : "0";
    public string DeadlockDisplay => DeadlockCount > 0 ? DeadlockCount.ToString() : "0";
    public string LastCollectionDisplay => LastCollectionTime.HasValue ? ServerTimeHelper.FormatServerTime(LastCollectionTime, "HH:mm:ss") : "Never";

    /* Connection status */
    public string StatusDisplay => IsOnline switch { true => "Online", false => "Offline", _ => "Unknown" };
    public SolidColorBrush StatusBrush => MakeBrush(IsOnline switch { true => "#81C784", false => "#E57373", _ => "#888888" });
    public bool IsOffline => IsOnline == false;

    /* Color coding */
    public SolidColorBrush CpuBrush => MakeBrush(CpuPercent >= 80 ? "#E57373" : CpuPercent >= 50 ? "#FFB74D" : "#81C784");
    public SolidColorBrush BlockingBrush => MakeBrush(BlockingCount > 0 ? "#FFB74D" : "#81C784");
    public SolidColorBrush DeadlockBrush => MakeBrush(DeadlockCount > 0 ? "#E57373" : "#81C784");
    public SolidColorBrush CardBorderBrush => MakeBrush(
        IsOnline == false ? "#E57373" :
        DeadlockCount > 0 ? "#E57373" :
        BlockingCount > 0 ? "#FFB74D" :
        CpuPercent >= 80 ? "#FFB74D" :
        "#2a2d35");

    private static SolidColorBrush MakeBrush(string hex)
    {
        var color = (Color)ColorConverter.ConvertFromString(hex);
        var brush = new SolidColorBrush(color);
        brush.Freeze();
        return brush;
    }
    public bool HasAlerts => BlockingCount > 0 || DeadlockCount > 0;
}
