/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Viewer;

/*
 * FinOps tab row models — a COPY of Lite's LocalDataService.FinOps.cs model layer for the copy-parity
 * program (copy-don't-promote: the Darling viewer owns its own copy; Lite/Dashboard are untouched).
 * Two deliberate deviations from Lite's models, both because the headless store lacks the source:
 *   (1) The per-server FinOps COST attribution (MonthlyCost / MonthlyCostShare / AnnualCost) is dropped
 *       everywhere — that budget lives in Lite/Dashboard's ServerConnection config, which the Postgres
 *       store has no equivalent of. The health score (pure CPU/memory/storage math) is kept.
 *   (2) ServerPropertyRow drops the fields the collected server_properties table doesn't carry
 *       (sqlserver_start_time / host_os_version / ag_replica_role — Lite got them from a LIVE query the
 *       headless viewer can't run). Everything the collector DOES persist is surfaced.
 * The pure scoring helpers (FinOpsHealthCalculator, HighImpactScorer) are copied verbatim.
 */

/// <summary>7-day daily provisioning classification trend (Utilization sub-tab).</summary>
public sealed class ProvisioningTrendRow
{
    public DateTime Day { get; set; }
    public decimal AvgCpuPct { get; set; }
    public int MaxCpuPct { get; set; }
    public decimal P95CpuPct { get; set; }
    public decimal MemoryRatio { get; set; }
    public string Status { get; set; } = "";
    public string DayDisplay => Day.ToString("ddd MM/dd");
    public string StatusDisplay => Status.Replace("_", " ");
}

/// <summary>Pool-level memory-grant vs used efficiency per day (Optimization sub-tab).</summary>
public sealed class MemoryGrantEfficiencyRow
{
    public DateTime Day { get; set; }
    public decimal AvgGrantedMb { get; set; }
    public decimal AvgUsedMb { get; set; }
    public decimal EfficiencyPct { get; set; }
    public decimal PeakGrantedMb { get; set; }
    public long TotalGrantees { get; set; }
    public long TotalWaiters { get; set; }
    public long TimeoutErrors { get; set; }
    public long ForcedGrants { get; set; }
    public string DayDisplay => Day.ToString("ddd MM/dd");
    public decimal WastedMb => AvgGrantedMb - AvgUsedMb;
}

/// <summary>Top database by CPU/IO for the Utilization summary grids.</summary>
public sealed class TopResourceConsumerRow
{
    public string DatabaseName { get; set; } = "";
    public long CpuTimeMs { get; set; }
    public long ExecutionCount { get; set; }
    public decimal IoTotalMb { get; set; }
    public decimal PctCpu { get; set; }
    public decimal PctIo { get; set; }
    public long TotalCpuTimeMs { get; set; }
    public decimal AvgIoMb { get; set; }
}

/// <summary>Per-database allocated vs used space for the Utilization size chart (with star-width bars).</summary>
public sealed class DatabaseSizeSummaryRow
{
    public string DatabaseName { get; set; } = "";
    public decimal TotalMb { get; set; }
    public decimal? UsedMb { get; set; }
    public decimal FreeMb => UsedMb.HasValue ? TotalMb - UsedMb.Value : TotalMb;
    public decimal UsedPct => TotalMb > 0 && UsedMb.HasValue ? Math.Round(UsedMb.Value * 100m / TotalMb, 1) : 0;

    /* Star-width GridLength for XAML binding — drives the stacked bar proportions. */
    public System.Windows.GridLength UsedStarWidth =>
        new(Math.Max((double)(UsedMb ?? 0m), 0.1), System.Windows.GridUnitType.Star);
    public System.Windows.GridLength FreeStarWidth =>
        new(Math.Max((double)FreeMb, 0.1), System.Windows.GridUnitType.Star);
}

/// <summary>Utilization efficiency summary (Utilization sub-tab header + bars + health score).</summary>
public sealed class UtilizationEfficiencyRow
{
    public decimal AvgCpuPct { get; set; }
    public int MaxCpuPct { get; set; }
    public decimal P95CpuPct { get; set; }
    public long CpuSamples { get; set; }
    public int TotalMemoryMb { get; set; }
    public int TargetMemoryMb { get; set; }
    public int PhysicalMemoryMb { get; set; }
    public int BufferPoolMb { get; set; }
    public decimal MemoryRatio { get; set; }

    /// <summary>Peak resource-semaphore waiters over the window. Any waiter at all means a query asked for
    /// workspace memory and did not simply get it — the signal the verdict uses in place of the ratio that
    /// pinned at 1.0 (#2246).</summary>
    public long MaxGrantWaiters { get; set; }

    /// <summary>Grant timeouts accrued over the window (delta, not cumulative).</summary>
    public long GrantTimeouts { get; set; }

    /// <summary>Grants forced through below what was requested, over the window.</summary>
    public long ForcedGrants { get; set; }

    /// <summary>Peak granted-over-target workspace memory, as a percentage. Fleet max is 18.8%.</summary>
    public decimal GrantUtilizationPct { get; set; }

    public int MaxWorkersCount { get; set; }
    public int CurrentWorkersCount { get; set; }
    public int CpuCount { get; set; }
    public string ProvisioningStatus { get; set; } = "";

    // FinOps cost — proportional to the server's monthly budget (0 = hidden)
    public decimal MonthlyCost { get; set; }
    public decimal AnnualCost => MonthlyCost * 12m;

    // Health score
    public decimal FreeSpacePct { get; set; }
    public int HealthScore { get; set; }
    public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);
}

/// <summary>Per-database resource usage (Database Resources sub-tab).</summary>
public sealed class DatabaseResourceUsageRow
{
    public string DatabaseName { get; set; } = "";
    public long CpuTimeMs { get; set; }
    public long LogicalReads { get; set; }
    public long PhysicalReads { get; set; }
    public long LogicalWrites { get; set; }
    public long ExecutionCount { get; set; }
    public decimal IoReadMb { get; set; }
    public decimal IoWriteMb { get; set; }
    public long IoStallMs { get; set; }
    public decimal PctCpuShare { get; set; }
    public decimal PctIoShare { get; set; }
}

/// <summary>Per-application connection counts plus collected per-app resource + session-status metrics (Application Connections sub-tab). Timestamps are localized in the read.</summary>
public sealed class ApplicationConnectionRow
{
    public string ApplicationName { get; set; } = "";
    public int AvgConnections { get; set; }
    public int MaxConnections { get; set; }
    public int AvgRunning { get; set; }
    public int MaxRunning { get; set; }
    public int AvgSleeping { get; set; }
    public int MaxSleeping { get; set; }
    public int AvgDormant { get; set; }
    public int MaxDormant { get; set; }
    public long AvgCpuTimeMs { get; set; }
    public long MaxCpuTimeMs { get; set; }
    public long AvgReads { get; set; }
    public long MaxReads { get; set; }
    public long AvgWrites { get; set; }
    public long MaxWrites { get; set; }
    public long AvgLogicalReads { get; set; }
    public long MaxLogicalReads { get; set; }
    public long SampleCount { get; set; }
    public DateTime FirstSeenLocal { get; set; }
    public DateTime LastSeenLocal { get; set; }
}

/// <summary>Per-file database size + growth config (Database Sizes sub-tab).</summary>
public sealed class DatabaseSizeRow
{
    public string DatabaseName { get; set; } = "";
    public string FileTypeDesc { get; set; } = "";
    public string FileName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
    public decimal? UsedSizeMb { get; set; }
    public decimal? FreeSpaceMb => UsedSizeMb.HasValue ? TotalSizeMb - UsedSizeMb.Value : null;
    public decimal? UsedPct => UsedSizeMb.HasValue && TotalSizeMb > 0 ? Math.Round(UsedSizeMb.Value * 100m / TotalSizeMb, 1) : null;
    public string? VolumeMountPoint { get; set; }
    public decimal? VolumeTotalMb { get; set; }
    public decimal? VolumeFreeMb { get; set; }
    public string? RecoveryModel { get; set; }
    public decimal? AutoGrowthMb { get; set; }
    public bool? IsPercentGrowth { get; set; }
    public int? GrowthPct { get; set; }
    public int? VlfCount { get; set; }

    /// <summary>FinOps cost — proportional share of the server monthly budget by size (set by the loader).</summary>
    public decimal MonthlyCostShare { get; set; }

    public string GrowthDisplay => IsPercentGrowth switch
    {
        null  => "-",
        true  => GrowthPct.HasValue ? $"{GrowthPct}%" : "-",
        false => AutoGrowthMb == null || AutoGrowthMb == 0 ? "Disabled" : $"{AutoGrowthMb:N0} MB"
    };

    public decimal AutoGrowthSort => IsPercentGrowth switch
    {
        null  => -1m,
        true  => (decimal)(GrowthPct ?? -1),
        false => AutoGrowthMb ?? 0m
    };

    public string VlfCountDisplay => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase)
        ? (VlfCount?.ToString() ?? "-") : "N/A";

    public int VlfCountSort => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase)
        ? (VlfCount ?? 0) : -1;
}

/// <summary>
/// One server's inventory row (Server Inventory sub-tab), from the collected <c>server_properties</c>
/// table (Lite used a LIVE query — the headless viewer can't reach the target). The fields the collector
/// does not persist (start time / host OS / AG replica role) and the FinOps cost attribution are dropped.
/// </summary>
public sealed class ServerPropertyRow
{
    /// <summary>The store's server_id — carried so the loader can overlay this server's collected metrics; not shown in the grid.</summary>
    public int ServerId { get; set; }
    public string ServerName { get; set; } = "";
    public string Edition { get; set; } = "";
    public string ProductVersion { get; set; } = "";
    public string HostOsVersion { get; set; } = "";
    public int EngineEdition { get; set; }
    public int CpuCount { get; set; }
    public long PhysicalMemoryMb { get; set; }
    public int? SocketCount { get; set; }
    public int? CoresPerSocket { get; set; }
    /// <summary>The server's LOCAL start clock (sys.dm_os_sys_info) — stored verbatim, shown as-is like Lite.</summary>
    public DateTime? SqlServerStartTime { get; set; }
    /// <summary>
    /// When this server's CONFIG SNAPSHOT was taken — not a freshness heartbeat (#2359).
    ///
    /// <para><c>server_properties</c> ships with <c>FrequencyMinutes 0</c>, which the schedule table defines as
    /// "collect once on server load only (config snapshots)". So this is effectively the last time the service
    /// loaded this server, and on an install that has been up for a week every actively-monitored server shows a
    /// week-old value. It was called <c>LastUpdated</c>, which invited exactly the reading that made it a bug
    /// report: an operator sees a days-old date and concludes collection is broken.</para>
    ///
    /// <para><see cref="LastCollected"/> is the value that answers the question people were actually asking.</para>
    /// </summary>
    public DateTime? InventoryAsOf { get; set; }

    /// <summary>
    /// The newest collection of ANY kind for this server — <c>MAX(collection_time)</c> across
    /// <c>v_collection_log</c>, the same signal <c>list_servers</c> and the Overview cards use (#2359). This is
    /// the real freshness heartbeat, and it moves every sweep.
    /// </summary>
    public DateTime? LastCollected { get; set; }
    public bool? IsHadrEnabled { get; set; }
    public bool? IsClustered { get; set; }
    public string AgReplicaRole { get; set; } = "Standalone";

    /// <summary>
    /// Whether this server is still being monitored (#2359). Server Inventory deliberately lists every
    /// REGISTERED server, and a registered-but-disabled one keeps the <see cref="InventoryAsOf"/> it had when
    /// monitoring stopped — accurate, and read by everyone as a broken freshness column.
    ///
    /// <para>Disabled rows are kept rather than filtered: this is the FinOps tab, and a decommissioned
    /// server's cost history is exactly what someone opens it to look at. Dropping them would trade a
    /// confusing grid for a lying one.</para>
    /// </summary>
    public bool IsEnabled { get; set; } = true;

    /// <summary>What the grid shows for <see cref="IsEnabled"/> (#2359) — the one column that makes an old
    /// <see cref="InventoryAsOf"/> legible. "Stopped" rather than "Disabled" because the operator's question is
    /// what happened to the data, not what state a config row is in.</summary>
    public string MonitoringStatus => IsEnabled ? "Active" : "Stopped";

    public decimal? AvgCpuPct { get; set; }
    public decimal? StorageTotalGb { get; set; }
    public int? IdleDbCount { get; set; }
    public string? ProvisioningStatus { get; set; }

    /// <summary>
    /// Non-alarming note when this server's hardware inventory (CPU, memory, sockets) is absent, with the reason.
    /// Null when hardware is present, which is the normal case.
    ///
    /// <para>Lite's twin (<c>LocalDataService.FinOps.ServerProperties</c>) sets this by catching the
    /// <c>SqlException</c> from its own live <c>sys.dm_os_sys_info</c> read. The viewer cannot: it reads the
    /// collected store, not the target, so the permission failure happened in the collector minutes or hours
    /// earlier and is only visible here as NULL hardware columns. Distinguishing "unknown" from a real zero is
    /// what #1663 made possible — before it, a login without VIEW SERVER STATE lost the ENTIRE server_properties
    /// row, so there was nothing to annotate.</para>
    /// </summary>
    public string? HardwareUnavailableReason { get; set; }

    /// <summary>Per-server FinOps budget (servers.monthly_cost_usd from darling.json); 0 hides the cost columns.</summary>
    public decimal MonthlyCost { get; set; }
    public decimal AnnualCost => MonthlyCost * 12m;

    public string UptimeDisplay
    {
        get
        {
            if (SqlServerStartTime == null) return "";
            var uptime = DateTime.Now - SqlServerStartTime.Value;
            return $"{(int)uptime.TotalDays}d {uptime.Hours}h";
        }
    }
    public string HadrDisplay => IsHadrEnabled.HasValue ? (IsHadrEnabled.Value ? "Yes" : "No") : "";
    public string ClusteredDisplay => IsClustered.HasValue ? (IsClustered.Value ? "Yes" : "No") : "";
    public string AgReplicaRoleDisplay => string.Equals(AgReplicaRole, "Standalone", StringComparison.OrdinalIgnoreCase) ? "—" : AgReplicaRole;
    public string ProvisioningDisplay => ProvisioningStatus?.Replace("_", " ") ?? "";

    /// <summary>License-limit warning for Standard edition (CPU/RAM caps). Same math as Lite.</summary>
    public string? LicenseWarning
    {
        get
        {
            if (!Edition.Contains("Standard", StringComparison.OrdinalIgnoreCase)) return null;
            var warnings = new List<string>();
            if (CpuCount > 24) warnings.Add($"CPU: {CpuCount} cores (Standard limited to 24)");
            if (PhysicalMemoryMb > 131072) warnings.Add($"RAM: {PhysicalMemoryMb / 1024}GB (Standard limited to 128GB)");
            return warnings.Count > 0 ? string.Join("; ", warnings) : null;
        }
    }

    public int HealthScore { get; set; }
    public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);
}

/// <summary>Per-database storage growth vs 7d/30d ago (Storage Growth parent grid).</summary>
public sealed class StorageGrowthRow
{
    public string DatabaseName { get; set; } = "";
    public decimal CurrentSizeMb { get; set; }
    public decimal? Size7dAgoMb { get; set; }
    public decimal? Size30dAgoMb { get; set; }
    public decimal Growth7dMb { get; set; }
    public decimal Growth30dMb { get; set; }
    public decimal DailyGrowthRateMb { get; set; }
    public decimal GrowthPct30d { get; set; }
}

/// <summary>Database with zero query executions over the window (Optimization sub-tab). LastExecutionTime localized in read.</summary>
public sealed class IdleDatabaseRow
{
    public string DatabaseName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
    public int FileCount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
}

/// <summary>tempdb pressure metric current vs 24h peak (Optimization sub-tab).</summary>
public sealed class TempdbSummaryRow
{
    public string Metric { get; set; } = "";
    public decimal CurrentMb { get; set; }
    public decimal Peak24hMb { get; set; }
    public string Warning { get; set; } = "";
}

/// <summary>Wait time grouped by cost category (Optimization sub-tab).</summary>
public sealed class WaitCategorySummaryRow
{
    public string Category { get; set; } = "";
    public long TotalWaitTimeMs { get; set; }
    public long WaitingTasks { get; set; }
    public decimal PctOfTotal { get; set; }
    public string TopWaitType { get; set; } = "";
    public long TopWaitTimeMs { get; set; }

    /// <summary>FinOps cost — proportional share of the window's budget by wait-time fraction (set by the loader).</summary>
    public decimal MonthlyCostShare { get; set; }
}

/// <summary>Top-20 query by total CPU (Optimization sub-tab).</summary>
public sealed class ExpensiveQueryRow
{
    public string DatabaseName { get; set; } = "";
    public long TotalCpuMs { get; set; }
    public decimal AvgCpuMsPerExec { get; set; }
    public long TotalReads { get; set; }
    public decimal AvgReadsPerExec { get; set; }
    public long Executions { get; set; }
    public string QueryPreview { get; set; } = "";
    public string FullQueryText { get; set; } = "";

    /// <summary>FinOps cost — proportional share of the window's budget by CPU fraction (set by the loader).</summary>
    public decimal MonthlyCostShare { get; set; }

    /// <summary>The stored statement-level plan (query_stats.query_plan_xml, captured by Darling); opens in the Plan Viewer.</summary>
    public string? QueryPlanXml { get; set; }
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlanXml);
}

/// <summary>Pure health-score math (Utilization + Server Inventory). Copied verbatim from Lite.</summary>
public static class FinOpsHealthCalculator
{
    public static int CpuScore(decimal p95Pct)
    {
        if (p95Pct <= 70) return (int)(100 - p95Pct * 50 / 70);
        return (int)Math.Max(0, 50 - (p95Pct - 70) * 50 / 30);
    }

    public static int MemoryScore(decimal bufferPoolRatio)
    {
        if (bufferPoolRatio <= 0.30m) return 60;
        if (bufferPoolRatio <= 0.85m) return 100;
        if (bufferPoolRatio <= 0.95m) return (int)(100 - (bufferPoolRatio - 0.85m) * 800);
        return (int)Math.Max(0, 20 - (bufferPoolRatio - 0.95m) * 400);
    }

    public static int StorageScore(decimal freeSpacePct)
    {
        if (freeSpacePct >= 30) return 100;
        if (freeSpacePct >= 10) return (int)(50 + (freeSpacePct - 10) * 2.5m);
        return (int)(freeSpacePct * 5);
    }

    public static int Overall(int cpu, int memory, int storage) =>
        (int)(cpu * 0.40 + memory * 0.30 + storage * 0.30);

    public static string ScoreColor(int score) => score switch
    {
        >= 80 => "#27AE60",
        >= 60 => "#F39C12",
        _ => "#E74C3C"
    };
}

/// <summary>High-impact query row (High Impact sub-tab) — 80/20 impact score across six dimensions.</summary>
public sealed class HighImpactQueryRow
{
    public string QueryHash { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public long TotalExecutions { get; set; }
    public decimal TotalCpuMs { get; set; }
    public decimal TotalDurationMs { get; set; }
    public long TotalReads { get; set; }
    public long TotalWrites { get; set; }
    public decimal TotalMemoryMb { get; set; }
    public decimal CpuShare { get; set; }
    public decimal DurationShare { get; set; }
    public decimal ReadsShare { get; set; }
    public decimal WritesShare { get; set; }
    public decimal MemoryShare { get; set; }
    public decimal ExecutionsShare { get; set; }
    public int ImpactScore { get; set; }
    public string SampleQueryText { get; set; } = "";
    public string FullQueryText { get; set; } = "";

    /// <summary>The stored statement-level plan (query_stats.query_plan_xml, captured by Darling); opens in the Plan Viewer.</summary>
    public string? QueryPlanXml { get; set; }
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlanXml);

    /// <summary>True when this row can have its ACTUAL plan captured — it carries the query_hash the service
    /// re-executes by (identifier-only, resolved from query_stats). Gates the shared FinOps plan menu's "Get
    /// Actual Plan"; the Expensive Queries rows (grouped by text, no query_hash) lack it and fall back to disabled.</summary>
    public bool CanGetActualPlan => !string.IsNullOrEmpty(QueryHash);

    public string ImpactScoreColor => ImpactScore switch
    {
        >= 80 => "#E74C3C",
        >= 60 => "#F39C12",
        _ => "#27AE60"
    };
}

/// <summary>
/// Identifies top-N queries per resource dimension, computes PERCENT_RANK and share percentages, and
/// returns the "interesting" set sorted by impact score. Copied verbatim from Lite's HighImpactScorer.
/// </summary>
public static class HighImpactScorer
{
    public static List<HighImpactQueryRow> Score(List<HighImpactQueryRow> allRows, int topN = 10)
    {
        if (allRows.Count == 0) return allRows;

        var interesting = new HashSet<string>();
        foreach (var hash in allRows.OrderByDescending(r => r.TotalCpuMs).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalDurationMs).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalReads).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalWrites).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalMemoryMb).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalExecutions).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);

        var filtered = allRows.Where(r => interesting.Contains(r.QueryHash)).ToList();

        if (filtered.Count == 0) return filtered;

        var cpuValues = filtered.Select(r => r.TotalCpuMs).OrderBy(v => v).ToList();
        var durationValues = filtered.Select(r => r.TotalDurationMs).OrderBy(v => v).ToList();
        var readsValues = filtered.Select(r => (decimal)r.TotalReads).OrderBy(v => v).ToList();
        var writesValues = filtered.Select(r => (decimal)r.TotalWrites).OrderBy(v => v).ToList();
        var memoryValues = filtered.Select(r => r.TotalMemoryMb).OrderBy(v => v).ToList();
        var execValues = filtered.Select(r => (decimal)r.TotalExecutions).OrderBy(v => v).ToList();

        var totalCpu = filtered.Sum(r => r.TotalCpuMs);
        var totalDuration = filtered.Sum(r => r.TotalDurationMs);
        var totalReads = filtered.Sum(r => (decimal)r.TotalReads);
        var totalWrites = filtered.Sum(r => (decimal)r.TotalWrites);
        var totalMemory = filtered.Sum(r => r.TotalMemoryMb);
        var totalExecs = filtered.Sum(r => (decimal)r.TotalExecutions);

        foreach (var row in filtered)
        {
            var cpuPctl = PercentRank(cpuValues, row.TotalCpuMs);
            var durationPctl = PercentRank(durationValues, row.TotalDurationMs);
            var readsPctl = PercentRank(readsValues, (decimal)row.TotalReads);
            var writesPctl = PercentRank(writesValues, (decimal)row.TotalWrites);
            var memoryPctl = PercentRank(memoryValues, row.TotalMemoryMb);
            var execsPctl = PercentRank(execValues, (decimal)row.TotalExecutions);

            row.CpuShare = totalCpu > 0 ? Math.Round(100m * row.TotalCpuMs / totalCpu, 1) : 0;
            row.DurationShare = totalDuration > 0 ? Math.Round(100m * row.TotalDurationMs / totalDuration, 1) : 0;
            row.ReadsShare = totalReads > 0 ? Math.Round(100m * row.TotalReads / totalReads, 1) : 0;
            row.WritesShare = totalWrites > 0 ? Math.Round(100m * row.TotalWrites / totalWrites, 1) : 0;
            row.MemoryShare = totalMemory > 0 ? Math.Round(100m * row.TotalMemoryMb / totalMemory, 1) : 0;
            row.ExecutionsShare = totalExecs > 0 ? Math.Round(100m * row.TotalExecutions / totalExecs, 1) : 0;

            var pctlSum = cpuPctl + durationPctl + readsPctl + writesPctl + memoryPctl + execsPctl;
            row.ImpactScore = (int)(pctlSum / 6m * 100m);
        }

        return filtered.OrderByDescending(r => r.ImpactScore).ToList();
    }

    internal static decimal PercentRank(List<decimal> sortedValues, decimal value)
    {
        if (sortedValues.Count <= 1) return 0;
        int rank = sortedValues.Count(v => v < value);
        return Math.Min(1.0m, (decimal)rank / (sortedValues.Count - 1));
    }
}

/// <summary>Per-table size + growth for the Storage Growth object drill (indexes rolled up).</summary>
public sealed class ObjectSizeGrowthRow
{
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public decimal CurrentReservedMb { get; set; }
    public decimal CurrentUsedMb { get; set; }
    public long TotalRows { get; set; }
    public int IndexCount { get; set; }
    public decimal Growth7dMb { get; set; }
    public decimal Growth30dMb { get; set; }
    public decimal DailyGrowthRateMb { get; set; }
    public decimal GrowthPct30d { get; set; }
}

/// <summary>Per-index usage with unused/write-only classification (Storage Growth index drill). LastUserAccess localized in read.</summary>
public sealed class IndexUsageRow
{
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public string IndexTypeDesc { get; set; } = "";
    public int IndexId { get; set; }
    public decimal ReservedMb { get; set; }
    public long TotalRows { get; set; }
    public long UserSeeks { get; set; }
    public long UserScans { get; set; }
    public long UserLookups { get; set; }
    public long TotalReads { get; set; }
    public long UserUpdates { get; set; }
    public DateTime? LastUserAccess { get; set; }
    public string Classification { get; set; } = "";
}

/// <summary>Per-index locking/latch contention (Locking &amp; Contention sub-tab).</summary>
public sealed class IndexLockingRow
{
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public string IndexTypeDesc { get; set; } = "";
    public decimal ReservedMb { get; set; }
    public long TotalRows { get; set; }
    public long RowLockCount { get; set; }
    public long RowLockWaitCount { get; set; }
    public long RowLockWaitInMs { get; set; }
    public long PageLockCount { get; set; }
    public long PageLockWaitCount { get; set; }
    public long PageLockWaitInMs { get; set; }
    public long IndexLockPromotionCount { get; set; }
    public long PageLatchWaitInMs { get; set; }
    public long PageIoLatchWaitInMs { get; set; }
    public long PageLatchWaitCount { get; set; }
    public long PageIoLatchWaitCount { get; set; }

    /// <summary>
    /// Per-column 0..1 log color-scale intensities for the four *_wait_in_ms cells (#1138 §3B). Set by the
    /// loader after fetch via <see cref="PerformanceMonitor.Common.FinOpsHeatmapBuilder.ColumnLogIntensities"/>;
    /// bound to the cell background through HeatIntensityToBrushConverter. Not from the database.
    /// </summary>
    public double RowLockHeat { get; set; }
    public double PageLockHeat { get; set; }
    public double PageLatchHeat { get; set; }
    public double PageIoLatchHeat { get; set; }
}
