/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>
/// The figures the FinOps Utilization view derives from <see cref="UtilizationEfficiencyDto"/> and the database-size
/// totals: the stolen-memory and buffer-pool percentages, the free-space percentage, the health score and its band, and
/// the verdict's reason text. The WPF viewer and the MCP tools both call these, so there is one copy of each rule. The
/// two reads at the bottom give the server's monthly cost and the latest storage totals.
/// </summary>
public static class FinOpsUtilizationFigures
{
    /// <summary>Health-band names. Kept together so a rename touches one place; the cut points are those of
    /// <see cref="FinOpsHealthCalculator.ScoreColor"/>.</summary>
    public const string BandGood = "good";
    public const string BandFair = "fair";
    public const string BandPoor = "poor";

    /// <summary>Stolen memory: (total server memory - buffer pool) / total server memory, in percent; 0 when no total.</summary>
    public static double StolenMemoryPct(int totalMb, int bufferPoolMb) =>
        totalMb > 0
            ? (double)(totalMb - bufferPoolMb) / totalMb * 100.0
            : 0;

    /// <summary>The buffer pool's share of physical memory, in percent; 0 when no physical memory.</summary>
    public static double BufferPoolPct(int bufferPoolMb, int physicalMb) =>
        physicalMb > 0
            ? (double)bufferPoolMb / physicalMb * 100.0
            : 0;

    /// <summary>Free space as a percent of allocated; 100 when nothing is allocated (no snapshot, or an empty one).</summary>
    public static decimal FreeSpacePct(decimal allocatedMb, decimal freeMb) =>
        allocatedMb > 0 ? freeMb / allocatedMb * 100m : 100m;

    /// <summary>False when the 24-hour window held no CPU sample (an empty verdict).</summary>
    public static bool HasCpuSample(UtilizationEfficiencyDto d) => d.ProvisioningStatus.Length > 0;

    /// <summary>The health score for these figures, field by field. A window with no CPU sample leaves the CPU term
    /// out: its p95 is a 0 that came from nothing, and scoring that 0 would hand the server a full 100.</summary>
    public static int HealthScore(bool hasCpuSample, decimal p95CpuPct, int physicalMemoryMb, int bufferPoolMb, decimal freeSpacePct)
    {
        var bpRatio = physicalMemoryMb > 0 ? (decimal)bufferPoolMb / physicalMemoryMb : 0m;
        int? cpuScore = hasCpuSample ? FinOpsHealthCalculator.CpuScore(p95CpuPct) : null;
        return FinOpsHealthCalculator.Overall(
            cpuScore, FinOpsHealthCalculator.MemoryScore(bpRatio), FinOpsHealthCalculator.StorageScore(freeSpacePct));
    }

    /// <summary>The health score for a read result and the free-space percentage.</summary>
    public static int HealthScore(UtilizationEfficiencyDto d, decimal freeSpacePct) =>
        HealthScore(HasCpuSample(d), d.P95CpuPct, d.PhysicalMemoryMb, d.BufferPoolMb, freeSpacePct);

    /// <summary>The band a score falls in: good at 80 and above, fair at 60 and above, otherwise poor.</summary>
    public static string HealthBand(int score) => score >= 80 ? BandGood : score >= 60 ? BandFair : BandPoor;

    /// <summary>The sentence that says why the server got its verdict; empty for no verdict. The numbers in it are
    /// formatted in <paramref name="culture"/>, which is made the thread's current culture for the call and restored after.</summary>
    public static string Explanation(UtilizationEfficiencyDto d, CultureInfo culture)
    {
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = culture;

        try
        {
            var bpPct = BufferPoolPct(d.BufferPoolMb, d.PhysicalMemoryMb);
            var azureSqlDb = ServerHardwareScope.HardwareIsTheHosts(d.EngineEdition);
            return d.ProvisioningStatus switch
            {
                "RIGHT_SIZED" => ServerHardwareScope.RightSizedExplanation(d.AvgCpuPct, d.P95CpuPct, bpPct, azureSqlDb),
                "OVER_PROVISIONED" => ServerHardwareScope.OverProvisionedExplanation(d.AvgCpuPct, d.MaxCpuPct, bpPct, azureSqlDb),
                /* The reason comes from the same place as the verdict, so a server flagged for grant pressure or
                   worker saturation is not explained as a memory ratio that no longer decides anything (#2246). */
                "UNDER_PROVISIONED" => ProvisioningVerdict.UnderProvisionedReason(
                    d.P95CpuPct, d.MaxGrantWaiters, d.GrantTimeouts, d.ForcedGrants,
                    d.MaxWorkersCount, d.CurrentWorkersCount),
                ProvisioningVerdict.NotApplicable => ProvisioningVerdict.NotApplicableExplanation,
                _ => ""
            };
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    /// <summary>The server's monthly cost from the registry. $1 server_id. NULL reads as 0, as the viewer's server list does.</summary>
    public const string MonthlyCostSql = "SELECT COALESCE(monthly_cost_usd, 0) FROM servers WHERE server_id = $1";

    /// <summary>The server's monthly cost in USD; 0 when the server has no row or no cost set.</summary>
    public static async Task<decimal> GetMonthlyCostUsdAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(MonthlyCostSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull ? 0m : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
    }

    /// <summary>The windowed half of the latest-snapshot probe. $1 server_id, $2 window start; no upper bound.</summary>
    public const string LatestStorageSnapshotWindowedSql = @"
SELECT MAX(collection_time)
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time >= $2";

    /// <summary>The unbounded fallback, reached only when the windowed probe finds nothing. $1 server_id.</summary>
    public const string LatestStorageSnapshotFallbackSql = @"
SELECT MAX(collection_time)
FROM v_database_size_stats
WHERE server_id = $1";

    /// <summary>The allocated and free totals at one snapshot. $1 server_id, $2 collection_time. A row with no total
    /// (the Hyperscale log file) adds nothing to either; a row with no used size has no free space.</summary>
    public const string LatestStorageTotalsSql = @"
SELECT COALESCE(SUM(total_size_mb), 0),
       COALESCE(SUM(CASE WHEN total_size_mb IS NOT NULL AND used_size_mb IS NOT NULL THEN total_size_mb - used_size_mb END), 0)
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = $2";

    /// <summary>The allocated and free storage totals (MB) at the server's latest database-size snapshot; null when it
    /// has none. Equal to the viewer's allocated and free totals over its latest-size read.</summary>
    public static async Task<(decimal AllocatedMb, decimal FreeMb)?> GetLatestStorageTotalsAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        DateTime? snapshot = null;

        await using (var probe = dataSource.CreateCommand(LatestStorageSnapshotWindowedSql))
        {
            probe.CommandTimeout = commandTimeoutSeconds;
            probe.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            probe.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            if (await probe.ExecuteScalarAsync(cancellationToken) is DateTime windowed)
            {
                snapshot = windowed;
            }
        }

        if (snapshot is null)
        {
            await using var fallback = dataSource.CreateCommand(LatestStorageSnapshotFallbackSql);
            fallback.CommandTimeout = commandTimeoutSeconds;
            fallback.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            if (await fallback.ExecuteScalarAsync(cancellationToken) is DateTime unbounded)
            {
                snapshot = unbounded;
            }
        }

        if (snapshot is null)
        {
            return null;
        }

        await using var command = dataSource.CreateCommand(LatestStorageTotalsSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = snapshot.Value });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (Convert.ToDecimal(reader.GetValue(0), CultureInfo.InvariantCulture),
                Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture));
    }
}
