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
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>Utilization efficiency summary for one server over the last 24 hours. No display or WPF state. The four grant figures
/// are null when the window holds no memory grant sample (#7906): a grant collector that is off, gated by the engine edition or not
/// yet run is not the 0 waiters and 0% utilization a healthy idle server reads.</summary>
public sealed record UtilizationEfficiencyDto(
    decimal AvgCpuPct,
    int MaxCpuPct,
    decimal P95CpuPct,
    long CpuSamples,
    int TotalMemoryMb,
    int TargetMemoryMb,
    int PhysicalMemoryMb,
    int BufferPoolMb,
    decimal MemoryRatio,
    long? MaxGrantWaiters,
    long? GrantTimeouts,
    long? ForcedGrants,
    decimal? GrantUtilizationPct,
    int MaxWorkersCount,
    int? CurrentWorkersCount,
    int CpuCount,
    int EngineEdition,
    string ProvisioningStatus);

/// <summary>One day of the 7-day provisioning classification trend. <paramref name="Day"/> is the day the read bucketed by (<see cref="DayBucket"/>): a UTC date unless the caller gave the display's days.</summary>
public sealed record ProvisioningTrendDto(
    DateTime Day,
    decimal AvgCpuPct,
    int MaxCpuPct,
    decimal P95CpuPct,
    decimal MemoryRatio,
    string Status,
    decimal? StolenMemoryPct = null,
    string? StatusReason = null);

/// <summary>One day of pool-level memory-grant vs used efficiency. <paramref name="Day"/> is the day the read bucketed by (<see cref="DayBucket"/>): a UTC date unless the caller gave the display's days.</summary>
public sealed record MemoryGrantEfficiencyDto(
    DateTime Day,
    decimal AvgGrantedMb,
    decimal AvgUsedMb,
    decimal EfficiencyPct,
    decimal PeakGrantedMb,
    long TotalGrantees,
    long TotalWaiters,
    long TimeoutErrors,
    long ForcedGrants);

/// <summary>
/// One day of a per-day read: the calendar day its row is labelled with, and the half-open range [<paramref name="StartUtc"/>,
/// <paramref name="EndUtc"/>) of naive-UTC instants it holds. Adjacent buckets share a bound, so every sample lands in exactly one day.
/// <see cref="DarlingFinOpsUtilizationReader.UtcDays"/> builds UTC days on their midnights; the Viewer builds the selected time
/// display's days, whose bounds are the UTC instants that zone's midnights fall on.
/// </summary>
public readonly record struct DayBucket(DateTime Day, DateTime StartUtc, DateTime EndUtc);

/// <summary>
/// The FinOps Utilization reads (utilization efficiency, 7-day provisioning trend, memory-grant efficiency) as plain
/// DTOs with naive-UTC times, shared by the viewer and any other reader of the store. The DuckDB→PG rewrite of
/// Lite's <c>LocalDataService.FinOps.Utilization.cs</c> is near-verbatim (positional <c>$1/$2</c> params,
/// PERCENTILE_CONT, <c>CAST(... AS DECIMAL(p,s))</c>, and <c>LEFT JOIN ... ON true</c> all run identically on PG); the
/// ONE substantive change is that Lite's <c>server_info</c> CTE reads <c>v_server_properties</c>, which Darling has no
/// view for, so it reads the collected <c>server_properties</c> base table directly (bare name resolves through the
/// connection's Search Path). SQL kept in <c>public const</c> so tests pin it. The two per-day reads bucket by a days table
/// the caller passes (<see cref="DayBucket"/>): UTC days for a caller with no display, such as the MCP tools, and the selected
/// time display's days for the Viewer.
/// </summary>
public static class DarlingFinOpsUtilizationReader
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
/* cpu_count is the count CPU percent is measured against. On an Azure SQL Database (engine_edition 5) the stored cpu_count
   is the number of schedulers the database can see (a 1-vCore serverless database read 2), not what it is given, so there it
   is the vcore_count parsed from the service objective, and NULL for an objective that names no vCores (a DTU-model objective
   or an elastic pool): never the scheduler count. Every other edition reads as it always did. The same CASE is in the Lite read. */
server_info AS (
    SELECT CASE WHEN engine_edition = 5 THEN vcore_count ELSE COALESCE(vcore_count, cpu_count) END AS cpu_count, engine_edition, edition, ag_replica_role
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
    AND   COALESCE(resource_semaphore_id, 0) = 0 /* #7676: regular semaphore only, as before id 1 was collected */
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
    /* #7906: NULL when the window holds no grant sample, so a server whose grant collector is off, gated or not yet run does not read as
       0 waiters and 0% utilization. */
    g.max_grant_waiters,
    g.grant_timeouts,
    g.forced_grants,
    g.grant_utilization_pct,
    s.engine_edition,
    s.edition,
    s.ag_replica_role
FROM cpu_stats c
CROSS JOIN mem_latest m
LEFT JOIN server_info s ON true
LEFT JOIN grants g ON true";

    /// <summary>7-day daily provisioning classification trend. $1 server_id, $2 cutoff (naive UTC), $3 the days' labels (date[]),
    /// $4 their starts and $5 their ends (timestamp[], naive UTC, half-open): one element per <see cref="DayBucket"/>.</summary>
    public const string ProvisioningTrendSql = @"
WITH days AS (
    SELECT day, start_utc, end_utc
    FROM unnest($3::date[], $4::timestamp[], $5::timestamp[]) AS d(day, start_utc, end_utc)
),
daily_cpu AS (
    SELECT
        d.day,
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct,
        COUNT(*) AS cpu_samples
    FROM v_cpu_utilization_stats
    JOIN days d
      ON collection_time >= d.start_utc
     AND collection_time < d.end_utc
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY d.day
),
daily_mem AS (
    SELECT
        d.day,
        AVG(total_server_memory_mb) AS avg_total_mb,
        AVG(buffer_pool_mb) AS avg_buffer_pool_mb,
        AVG(CAST(total_server_memory_mb AS DECIMAL(10,2)) / NULLIF(target_server_memory_mb, 0)) AS avg_memory_ratio,
        MAX(max_workers_count) AS max_workers_count,
        MAX(current_workers_count) AS current_workers_count
    FROM v_memory_stats
    JOIN days d
      ON collection_time >= d.start_utc
     AND collection_time < d.end_utc
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY d.day
),
/* Same pressure signals as the point-in-time read, per day, so a day cannot be classified by a rule
   the current verdict does not use (#2246). */
daily_grants AS (
    SELECT
        d.day,
        MAX(waiter_count) AS max_grant_waiters,
        SUM(COALESCE(timeout_error_count_delta, 0)) AS grant_timeouts,
        SUM(COALESCE(forced_grant_count_delta, 0)) AS forced_grants,
        MAX(100.0 * granted_memory_mb / NULLIF(target_memory_mb, 0)) AS grant_utilization_pct
    FROM v_memory_grant_stats
    JOIN days d
      ON collection_time >= d.start_utc
     AND collection_time < d.end_utc
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   COALESCE(resource_semaphore_id, 0) = 0 /* #7676: regular semaphore only, as before id 1 was collected */
    GROUP BY d.day
)
SELECT
    c.day,
    c.avg_cpu_pct,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.avg_memory_ratio, 0),
    g.max_grant_waiters,
    g.grant_timeouts,
    g.forced_grants,
    g.grant_utilization_pct,
    COALESCE(m.max_workers_count, 0),
    m.current_workers_count,
    sp.engine_edition,
    sp.edition,
    c.cpu_samples,
    m.avg_total_mb,
    m.avg_buffer_pool_mb,
    sp.ag_replica_role
FROM daily_cpu c
LEFT JOIN daily_mem m ON m.day = c.day
LEFT JOIN daily_grants g ON g.day = c.day
LEFT JOIN (
    SELECT engine_edition, edition, ag_replica_role
    FROM server_properties
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
) sp ON true
ORDER BY c.day";

    /// <summary>
    /// Memory-grant vs used efficiency per day from resource-semaphore snapshots (Optimization sub-tab).
    /// $1 server_id, $2 cutoff (naive UTC), $3 the days' labels (date[]), $4 their starts and $5 their ends (timestamp[], naive UTC,
    /// half-open): one element per <see cref="DayBucket"/>.
    /// </summary>
    public const string MemoryGrantEfficiencySql = @"
WITH days AS (
    SELECT day, start_utc, end_utc
    FROM unnest($3::date[], $4::timestamp[], $5::timestamp[]) AS d(day, start_utc, end_utc)
)
SELECT
    d.day,
    AVG(granted_memory_mb) AS avg_granted_mb,
    AVG(used_memory_mb) AS avg_used_mb,
    CAST(AVG(used_memory_mb) * 100.0 / NULLIF(AVG(granted_memory_mb), 0) AS DECIMAL(5,1)) AS efficiency_pct,
    MAX(granted_memory_mb) AS peak_granted_mb,
    SUM(grantee_count) AS total_grantees,
    SUM(waiter_count) AS total_waiters,
    SUM(timeout_error_count_delta) AS timeout_errors,
    SUM(forced_grant_count_delta) AS forced_grants
FROM v_memory_grant_stats
JOIN days d
  ON collection_time >= d.start_utc
 AND collection_time < d.end_utc
WHERE server_id = $1
AND   collection_time >= $2
AND   COALESCE(resource_semaphore_id, 0) = 0 /* #7676: regular semaphore only, as before id 1 was collected */
GROUP BY d.day
ORDER BY d.day";

    /// <summary>The last-24-hour utilization efficiency, or <c>null</c> when the store has no row to join.</summary>
    public static async Task<UtilizationEfficiencyDto?> GetUtilizationEfficiencyAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        await using var command = dataSource.CreateCommand(UtilizationEfficiencySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var avgCpu = reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0));
        var maxCpu = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
        var p95Cpu = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
        var memRatio = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8));

        var maxWorkers = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9));
        /* NULL is "not collected" (an Azure SQL Database), not 0 workers in use: it shows as n/a and the verdict reads it as unknown. */
        int? currentWorkers = reader.IsDBNull(10) ? null : Convert.ToInt32(reader.GetValue(10));

        /* memory_ratio is still SELECTed and still displayed — it is a real fact about the instance — but it
           is no longer part of the verdict: Total over Target Server Memory converges at 1.0 on any warmed
           server, so it reported every server as under-provisioned (#2246).

           A window with no CPU sample at all has no verdict: its NULL average read as 0% CPU would call the
           server OVER_PROVISIONED. The row's empty status is its no-verdict value; the tab shows "No Data". */
        var status = reader.IsDBNull(0) ? "" : ProvisioningVerdict.Evaluate(
            avgCpu, maxCpu, p95Cpu,
            maxGrantWaiters: reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
            grantTimeouts: reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
            forcedGrants: reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
            grantUtilizationPercent: reader.IsDBNull(15) ? 0m : Convert.ToDecimal(reader.GetValue(15)),
            maxWorkers: maxWorkers,
            currentWorkers: currentWorkers,
            engineEdition: reader.IsDBNull(16) ? null : Convert.ToInt32(reader.GetValue(16)),
            edition: reader.IsDBNull(17) ? null : reader.GetString(17),
            cpuSamples: reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
            stolenMemoryPercent: (decimal)FinOpsUtilizationFigures.StolenMemoryPct(
                reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)), reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7))),
            isAgSecondary: ProvisioningVerdict.IsSecondaryReplicaRole(reader.IsDBNull(18) ? null : reader.GetString(18)));

        return new UtilizationEfficiencyDto(
            AvgCpuPct: avgCpu,
            MaxCpuPct: maxCpu,
            P95CpuPct: p95Cpu,
            CpuSamples: reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
            TotalMemoryMb: reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
            TargetMemoryMb: reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
            PhysicalMemoryMb: reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
            BufferPoolMb: reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
            MemoryRatio: memRatio,
            /* #7906: null with no grant sample in the window; the verdict above reads the same absence as no grant pressure. */
            MaxGrantWaiters: reader.IsDBNull(12) ? null : Convert.ToInt64(reader.GetValue(12)),
            GrantTimeouts: reader.IsDBNull(13) ? null : Convert.ToInt64(reader.GetValue(13)),
            ForcedGrants: reader.IsDBNull(14) ? null : Convert.ToInt64(reader.GetValue(14)),
            GrantUtilizationPct: reader.IsDBNull(15) ? null : Convert.ToDecimal(reader.GetValue(15)),
            MaxWorkersCount: maxWorkers,
            CurrentWorkersCount: currentWorkers,
            CpuCount: reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11)),
            EngineEdition: reader.IsDBNull(16) ? 0 : Convert.ToInt32(reader.GetValue(16)),
            ProvisioningStatus: status);
    }

    /// <summary>
    /// The 7-day daily provisioning classification trend, oldest day first, each row a UTC calendar day: the day a caller with no
    /// display, such as the MCP tools, names.
    /// </summary>
    public static Task<List<ProvisioningTrendDto>> GetProvisioningTrendAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var nowUtc = DateTime.UtcNow;
        var cutoff = nowUtc.AddDays(-7);
        return GetProvisioningTrendAsync(dataSource, serverId, cutoff, UtcDays(cutoff, nowUtc), commandTimeoutSeconds, cancellationToken);
    }

    /// <summary>
    /// The daily provisioning classification trend of the samples at or after <paramref name="fromUtc"/>, oldest day first, each row
    /// one of <paramref name="days"/>: the samples inside its bounds, labelled with its Day. The Viewer passes the selected time
    /// display's days, so a row reads in the same clock as every other time on its page.
    /// </summary>
    public static async Task<List<ProvisioningTrendDto>> GetProvisioningTrendAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime fromUtc, IReadOnlyList<DayBucket> days, int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(ProvisioningTrendSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified) });
        AddDayBuckets(command, days);

        var items = new List<ProvisioningTrendDto>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var avgCpu = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1));
            var maxCpu = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
            var p95Cpu = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3));
            var memRatio = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4));

            var maxGrantWaiters = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));
            var grantTimeouts = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6));
            var forcedGrants = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7));
            var maxWorkers = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9));
            var currentWorkers = reader.IsDBNull(10) ? (int?)null : Convert.ToInt32(reader.GetValue(10));
            var engineEdition = reader.IsDBNull(11) ? (int?)null : Convert.ToInt32(reader.GetValue(11));
            decimal? stolenMemoryPercent = reader.IsDBNull(14) || reader.IsDBNull(15) || Convert.ToDouble(reader.GetValue(14)) <= 0
                ? null
                : (decimal)((Convert.ToDouble(reader.GetValue(14)) - Convert.ToDouble(reader.GetValue(15))) / Convert.ToDouble(reader.GetValue(14)) * 100.0);
            var status = ProvisioningVerdict.Evaluate(
                avgCpu, maxCpu, p95Cpu,
                maxGrantWaiters: reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                grantTimeouts: reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                forcedGrants: reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                grantUtilizationPercent: reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8)),
                maxWorkers: reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                currentWorkers: reader.IsDBNull(10) ? (int?)null : Convert.ToInt32(reader.GetValue(10)),
                engineEdition: reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)),
                edition: reader.IsDBNull(12) ? null : reader.GetString(12),
                cpuSamples: Convert.ToInt64(reader.GetValue(13)),
                stolenMemoryPercent: reader.IsDBNull(14) || reader.IsDBNull(15) || Convert.ToDouble(reader.GetValue(14)) <= 0
                    ? null
                    : (decimal)((Convert.ToDouble(reader.GetValue(14)) - Convert.ToDouble(reader.GetValue(15))) / Convert.ToDouble(reader.GetValue(14)) * 100.0),
                isAgSecondary: ProvisioningVerdict.IsSecondaryReplicaRole(reader.IsDBNull(16) ? null : reader.GetString(16)));

            items.Add(new ProvisioningTrendDto(
                reader.GetDateTime(0),
                avgCpu,
                maxCpu,
                p95Cpu,
                memRatio,
                status,
                stolenMemoryPercent,
                /* The reason comes from the same place as the verdict, as the Viewer's badge does. */
                status == ProvisioningVerdict.UnderProvisioned
                    ? ProvisioningVerdict.UnderProvisionedReason(p95Cpu, maxGrantWaiters, grantTimeouts, forcedGrants, maxWorkers, currentWorkers, stolenMemoryPercent, engineEdition)
                    : null));
        }
        return items;
    }

    /// <summary>
    /// Memory-grant vs used efficiency over the last <paramref name="hoursBack"/> hours, each row a UTC calendar day: the day a caller
    /// with no display, such as the MCP tools, names.
    /// </summary>
    public static Task<List<MemoryGrantEfficiencyDto>> GetMemoryGrantEfficiencyAsync(
        NpgsqlDataSource dataSource, int serverId, int hoursBack, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var nowUtc = DateTime.UtcNow;
        var cutoff = nowUtc.AddHours(-hoursBack);
        return GetMemoryGrantEfficiencyAsync(dataSource, serverId, cutoff, UtcDays(cutoff, nowUtc), commandTimeoutSeconds, cancellationToken);
    }

    /// <summary>
    /// Memory-grant vs used efficiency of the samples at or after <paramref name="fromUtc"/>, each row one of <paramref name="days"/>:
    /// the samples inside its bounds, labelled with its Day. The Viewer passes the selected time display's days.
    /// </summary>
    public static async Task<List<MemoryGrantEfficiencyDto>> GetMemoryGrantEfficiencyAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime fromUtc, IReadOnlyList<DayBucket> days, int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(MemoryGrantEfficiencySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified) });
        AddDayBuckets(command, days);

        var items = new List<MemoryGrantEfficiencyDto>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryGrantEfficiencyDto(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0 : Convert.ToInt64(reader.GetValue(7)),
                reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8))));
        }
        return items;
    }

    /// <summary>
    /// The UTC calendar days from <paramref name="fromUtc"/> to <paramref name="toUtc"/>, each bounded by its midnights: what a read
    /// with no display buckets by, and the day the MCP <c>day</c> field names.
    /// </summary>
    public static List<DayBucket> UtcDays(DateTime fromUtc, DateTime toUtc)
    {
        var (first, last) = fromUtc <= toUtc ? (fromUtc, toUtc) : (toUtc, fromUtc);
        var days = new List<DayBucket>();
        for (var day = first.Date; day <= last.Date; day = day.AddDays(1))
        {
            days.Add(new DayBucket(day, day, day.AddDays(1)));
        }
        return days;
    }

    /// <summary>$3, $4 and $5 of a per-day read: the days' labels, starts and ends as three arrays, one element per day.</summary>
    private static void AddDayBuckets(NpgsqlCommand command, IReadOnlyList<DayBucket> days)
    {
        var labels = new DateOnly[days.Count];
        var starts = new DateTime[days.Count];
        var ends = new DateTime[days.Count];
        for (var i = 0; i < days.Count; i++)
        {
            labels[i] = DateOnly.FromDateTime(days[i].Day);
            starts[i] = DateTime.SpecifyKind(days[i].StartUtc, DateTimeKind.Unspecified);
            ends[i] = DateTime.SpecifyKind(days[i].EndUtc, DateTimeKind.Unspecified);
        }
        command.Parameters.Add(new NpgsqlParameter { Value = labels, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date });
        command.Parameters.Add(new NpgsqlParameter { Value = starts, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = ends, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp });
    }
}
