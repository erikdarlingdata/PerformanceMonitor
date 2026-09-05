/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Utilization sub-tab reads — Lite's <c>LocalDataService.FinOps.Utilization.cs</c> ported to
/// Postgres. The DuckDB→PG rewrite is near-verbatim (positional <c>$1/$2</c> params, PERCENTILE_CONT,
/// <c>CAST(... AS DECIMAL(p,s))</c>, and <c>LEFT JOIN ... ON true</c> all run identically on PG); the ONE
/// substantive change is that Lite's <c>server_info</c> CTE reads <c>v_server_properties</c>, which Darling
/// has no view for, so it reads the collected <c>server_properties</c> base table directly (bare name
/// resolves through the connection's Search Path). SQL kept in <c>public const</c> so tests pin it.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Utilization efficiency from cpu_utilization_stats + memory_stats (last 24h) + latest
    /// server_properties for the core count. $1 server_id, $2 cutoff (naive UTC).
    /// </summary>
    public const string UtilizationEfficiencySql = @"
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
    FROM server_properties
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
/* Workspace-memory pressure, which is what being short of memory actually looks like: a query asked the
   resource semaphore for a grant and did not simply get it. Counts of events, so no threshold to tune
   (#2246). The utilization peak rides along to stop a CPU-quiet server that is straining its semaphore
   from being called idle. */
grants AS (
    SELECT
        MAX(waiter_count) AS max_grant_waiters,
        SUM(COALESCE(timeout_error_count_delta, 0)) AS grant_timeouts,
        SUM(COALESCE(forced_grant_count_delta, 0)) AS forced_grants,
        MAX(100.0 * granted_memory_mb / NULLIF(target_memory_mb, 0)) AS grant_utilization_pct
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
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
    s.cpu_count,
    COALESCE(g.max_grant_waiters, 0),
    COALESCE(g.grant_timeouts, 0),
    COALESCE(g.forced_grants, 0),
    COALESCE(g.grant_utilization_pct, 0)
FROM cpu_stats c
CROSS JOIN mem_latest m
LEFT JOIN server_info s ON true
LEFT JOIN grants g ON true";

    public async Task<UtilizationEfficiencyRow?> GetUtilizationEfficiencyAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        await using var command = _dataSource.CreateCommand(UtilizationEfficiencySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;

        var avgCpu = reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0));
        var maxCpu = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
        var p95Cpu = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
        var memRatio = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8));

        var maxWorkers = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9));
        var currentWorkers = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10));

        /* memory_ratio is still SELECTed and still displayed — it is a real fact about the instance — but it
           is no longer part of the verdict: Total over Target Server Memory converges at 1.0 on any warmed
           server, so it reported every server as under-provisioned (#2246). */
        var status = ProvisioningVerdict.Evaluate(
            avgCpu, maxCpu, p95Cpu,
            maxGrantWaiters: reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
            grantTimeouts: reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
            forcedGrants: reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
            grantUtilizationPercent: reader.IsDBNull(15) ? 0m : Convert.ToDecimal(reader.GetValue(15)),
            maxWorkers: maxWorkers,
            currentWorkers: currentWorkers);

        return new UtilizationEfficiencyRow
        {
            AvgCpuPct = avgCpu,
            MaxCpuPct = maxCpu,
            P95CpuPct = p95Cpu,
            CpuSamples = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
            TotalMemoryMb = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
            TargetMemoryMb = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
            PhysicalMemoryMb = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
            BufferPoolMb = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
            MemoryRatio = memRatio,
            ProvisioningStatus = status,
            MaxGrantWaiters = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
            GrantTimeouts = reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
            ForcedGrants = reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
            GrantUtilizationPct = reader.IsDBNull(15) ? 0m : Convert.ToDecimal(reader.GetValue(15)),
            MaxWorkersCount = maxWorkers,
            CurrentWorkersCount = currentWorkers,
            CpuCount = reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11))
        };
    }

    /// <summary>7-day daily provisioning classification trend. $1 server_id, $2 cutoff (naive UTC).</summary>
    public const string ProvisioningTrendSql = @"
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
        AVG(CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0)) AS avg_memory_ratio,
        MAX(max_workers_count) AS max_workers_count,
        MAX(current_workers_count) AS current_workers_count
    FROM v_memory_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY CAST(collection_time AS DATE)
),
/* Same pressure signals as the point-in-time read, per day, so a day cannot be classified by a rule
   the current verdict does not use (#2246). */
daily_grants AS (
    SELECT
        CAST(collection_time AS DATE) AS day,
        MAX(waiter_count) AS max_grant_waiters,
        SUM(COALESCE(timeout_error_count_delta, 0)) AS grant_timeouts,
        SUM(COALESCE(forced_grant_count_delta, 0)) AS forced_grants,
        MAX(100.0 * granted_memory_mb / NULLIF(target_memory_mb, 0)) AS grant_utilization_pct
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY CAST(collection_time AS DATE)
)
SELECT
    c.day,
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.avg_memory_ratio, 0),
    COALESCE(g.max_grant_waiters, 0),
    COALESCE(g.grant_timeouts, 0),
    COALESCE(g.forced_grants, 0),
    COALESCE(g.grant_utilization_pct, 0),
    COALESCE(m.max_workers_count, 0),
    COALESCE(m.current_workers_count, 0)
FROM daily_cpu c
LEFT JOIN daily_mem m ON m.day = c.day
LEFT JOIN daily_grants g ON g.day = c.day
ORDER BY c.day";

    public async Task<List<ProvisioningTrendRow>> GetProvisioningTrendAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddDays(-7);

        await using var command = _dataSource.CreateCommand(ProvisioningTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<ProvisioningTrendRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var avgCpu = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1));
            var maxCpu = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
            var p95Cpu = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3));
            var memRatio = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4));

            var status = ProvisioningVerdict.Evaluate(
                avgCpu, maxCpu, p95Cpu,
                maxGrantWaiters: reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                grantTimeouts: reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                forcedGrants: reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                grantUtilizationPercent: reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8)),
                maxWorkers: reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                currentWorkers: reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)));

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
    /// Memory-grant vs used efficiency per day from resource-semaphore snapshots (Optimization sub-tab).
    /// $1 server_id, $2 cutoff (naive UTC).
    /// </summary>
    public const string MemoryGrantEfficiencySql = @"
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

    public async Task<List<MemoryGrantEfficiencyRow>> GetMemoryGrantEfficiencyAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        await using var command = _dataSource.CreateCommand(MemoryGrantEfficiencySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<MemoryGrantEfficiencyRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryGrantEfficiencyRow
            {
                Day = reader.GetDateTime(0),
                AvgGrantedMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                AvgUsedMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                EfficiencyPct = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                PeakGrantedMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalGrantees = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                TotalWaiters = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                TimeoutErrors = reader.IsDBNull(7) ? 0 : Convert.ToInt64(reader.GetValue(7)),
                ForcedGrants = reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8))
            });
        }
        return items;
    }
}
