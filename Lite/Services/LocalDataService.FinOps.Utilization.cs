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
    /// Computes utilization efficiency from cpu_utilization_stats + memory_stats (last 24 hours).
    /// </summary>
    public async Task<UtilizationEfficiencyRow?> GetUtilizationEfficiencyAsync(int serverId)
    {
        using var _q = TimeQuery("GetUtilizationEfficiencyAsync", "utilization efficiency stats");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        command.CommandText = @"
WITH cpu_stats AS (
    SELECT
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct,
        COUNT(*) AS cpu_samples
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
),
mem_latest AS (
    SELECT
        total_server_memory_mb,
        target_server_memory_mb,
        total_physical_memory_mb,
        buffer_pool_mb,
        max_workers_count,
        current_workers_count,
        CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0) AS memory_ratio
    FROM v_memory_stats
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
server_info AS (
    SELECT COALESCE(vcore_count, cpu_count) AS cpu_count
    FROM v_server_properties
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
)
SELECT
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    c.cpu_samples,
    m.total_server_memory_mb,
    m.target_server_memory_mb,
    m.total_physical_memory_mb,
    m.buffer_pool_mb,
    m.memory_ratio,
    m.max_workers_count,
    m.current_workers_count,
    s.cpu_count
FROM cpu_stats c
CROSS JOIN mem_latest m
LEFT JOIN server_info s ON true";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        var avgCpu = reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0));
        var maxCpu = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
        var p95Cpu = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
        var memRatio = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8));

        var status = "RIGHT_SIZED";
        if (avgCpu < 15 && maxCpu < 40 && memRatio < 0.5m)
            status = "OVER_PROVISIONED";
        else if (p95Cpu > 85 || memRatio > 0.95m)
            status = "UNDER_PROVISIONED";

        return new UtilizationEfficiencyRow
        {
            AvgCpuPct = avgCpu,
            MaxCpuPct = maxCpu,
            P95CpuPct = p95Cpu,
            CpuSamples = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3)),
            TotalMemoryMb = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
            TargetMemoryMb = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
            PhysicalMemoryMb = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
            BufferPoolMb = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
            MemoryRatio = memRatio,
            ProvisioningStatus = status,
            MaxWorkersCount = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
            CurrentWorkersCount = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
            CpuCount = reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11))
        };
    }

    /// <summary>
    /// Gets 7-day daily provisioning classification trend.
    /// </summary>
    public async Task<List<ProvisioningTrendRow>> GetProvisioningTrendAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddDays(-7);

        command.CommandText = @"
WITH daily_cpu AS (
    SELECT
        CAST(collection_time AS DATE) AS day,
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY CAST(collection_time AS DATE)
),
daily_mem AS (
    SELECT
        CAST(collection_time AS DATE) AS day,
        AVG(CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0)) AS avg_memory_ratio
    FROM v_memory_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY CAST(collection_time AS DATE)
)
SELECT
    c.day,
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.avg_memory_ratio, 0)
FROM daily_cpu c
LEFT JOIN daily_mem m ON m.day = c.day
ORDER BY c.day";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<ProvisioningTrendRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var avgCpu = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1));
            var maxCpu = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
            var p95Cpu = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3));
            var memRatio = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4));

            var status = "RIGHT_SIZED";
            if (avgCpu < 15 && maxCpu < 40 && memRatio < 0.5m)
                status = "OVER_PROVISIONED";
            else if (p95Cpu > 85 || memRatio > 0.95m)
                status = "UNDER_PROVISIONED";

            items.Add(new ProvisioningTrendRow
            {
                Day = reader.GetDateTime(0),
                AvgCpuPct = avgCpu,
                MaxCpuPct = maxCpu,
                P95CpuPct = p95Cpu,
                MemoryRatio = memRatio,
                Status = status
            });
        }
        return items;
    }

    /// <summary>
    /// Gets memory grant efficiency stats for the Optimization tab.
    /// Shows pool-level grant vs used efficiency from resource semaphore snapshots.
    /// </summary>
    public async Task<List<MemoryGrantEfficiencyRow>> GetMemoryGrantEfficiencyAsync(int serverId, int hoursBack = 24)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        command.CommandText = @"
SELECT
    CAST(collection_time AS DATE) AS day,
    AVG(granted_memory_mb) AS avg_granted_mb,
    AVG(used_memory_mb) AS avg_used_mb,
    CAST(AVG(used_memory_mb) * 100.0 / NULLIF(AVG(granted_memory_mb), 0) AS DECIMAL(5,1)) AS efficiency_pct,
    MAX(granted_memory_mb) AS peak_granted_mb,
    SUM(grantee_count) AS total_grantees,
    SUM(waiter_count) AS total_waiters,
    SUM(timeout_error_count_delta) AS timeout_errors,
    SUM(forced_grant_count_delta) AS forced_grants
FROM v_memory_grant_stats
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY CAST(collection_time AS DATE)
ORDER BY CAST(collection_time AS DATE)";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<MemoryGrantEfficiencyRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryGrantEfficiencyRow
            {
                Day = reader.GetDateTime(0),
                AvgGrantedMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                AvgUsedMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                EfficiencyPct = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                PeakGrantedMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalGrantees = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5)),
                TotalWaiters = reader.IsDBNull(6) ? 0 : ToInt64(reader.GetValue(6)),
                TimeoutErrors = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                ForcedGrants = reader.IsDBNull(8) ? 0 : ToInt64(reader.GetValue(8))
            });
        }
        return items;
    }
}
