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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;

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

    /// <summary>From <c>memory_stats.total_physical_memory_mb</c>. On SQL Server and Managed Instance that is the machine's physical
    /// memory. On an Azure SQL Database (<see cref="EngineEdition"/> 5) the collector fills it from <c>committed_target_kb</c>,
    /// which is the database's own memory limit, not the host's RAM: the host's is <c>server_properties.physical_memory_mb</c>,
    /// which this row never reads. So the card shows it, and the health score and the verdict use it, on every edition.</summary>
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

    /// <summary>Workers in use at the latest sample. <c>null</c> where the collector cannot read it (an Azure SQL Database stores
    /// NULL), which the card shows as n/a: it is never 0, and the verdict treats it as unknown.</summary>
    public int? CurrentWorkersCount { get; set; }

    /// <summary>The CPU count CPU percent is measured against, 0 when there is none. On an Azure SQL Database
    /// (<see cref="EngineEdition"/> 5) that is the <c>vcore_count</c> parsed from the service objective, not the stored
    /// <c>cpu_count</c> (the schedulers the database can see, which can be higher than its vCores), and it is 0 for an objective
    /// that names none (a DTU-model objective or an elastic pool), which the card shows as n/a.</summary>
    public int CpuCount { get; set; }

    /// <summary>The engine edition of the server these figures describe (<c>SERVERPROPERTY('EngineEdition')</c>, 0 when unread).
    /// The card uses it to name the memory figure (Physical, or Memory limit on an Azure SQL Database) and to show a CPU count
    /// that is not applicable as n/a.</summary>
    public int EngineEdition { get; set; }
    public string ProvisioningStatus { get; set; } = "";

    /// <summary>
    /// False when the 24-hour window held no CPU sample at all. The row's <see cref="ProvisioningStatus"/> is then
    /// the empty no-verdict value, and <see cref="P95CpuPct"/> is a 0 that came from nothing rather than from a
    /// measured idle server. The right-sizing rules read this so a server that sent no CPU sample is not told to
    /// shrink.
    /// </summary>
    public bool HasCpuSample => ProvisioningStatus.Length > 0;

    // FinOps cost — proportional to the server's monthly budget (0 = hidden)
    public decimal MonthlyCost { get; set; }
    public decimal AnnualCost => MonthlyCost * 12m;

    // Health score
    public decimal FreeSpacePct { get; set; }
    public int HealthScore { get; set; }
    public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);

    /// <summary>
    /// The health score for these figures: CPU p95, the buffer pool's share of physical memory, and free storage. The memory
    /// term reads <see cref="PhysicalMemoryMb"/> and <see cref="BufferPoolMb"/>, which come from <c>memory_stats</c>. On an Azure
    /// SQL Database those are the database's own (its memory limit, not the host's RAM), so the memory term is worked the same way
    /// on every edition. A window with no CPU sample (<see cref="HasCpuSample"/> false) leaves the CPU term out: its p95 is a 0
    /// that came from nothing, and scoring that 0 would hand the server a full 100.
    /// </summary>
    public int ComputeHealthScore()
    {
        var bpRatio = PhysicalMemoryMb > 0 ? (decimal)BufferPoolMb / PhysicalMemoryMb : 0m;
        int? cpuScore = HasCpuSample ? FinOpsHealthCalculator.CpuScore(P95CpuPct) : null;
        return FinOpsHealthCalculator.Overall(
            cpuScore, FinOpsHealthCalculator.MemoryScore(bpRatio), FinOpsHealthCalculator.StorageScore(FreeSpacePct));
    }
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

    /// <summary>
    /// The UTC instants behind <see cref="FirstSeenLocal"/> and <see cref="LastSeenLocal"/> (#4766). A converted wall
    /// clock cannot say which of the two 01:30s of the repeated autumn hour it was, so the column text is worded from these.
    /// </summary>
    public DateTime FirstSeenUtc { get; set; }
    public DateTime LastSeenUtc { get; set; }

    /// <summary>
    /// The clock of the server these rows are for, stamped by the loader (#4766): its collected clock, else the viewer
    /// machine's offset (<see cref="ViewerTimeHelper.ClockForServerOrMachine"/>), the rule every list row uses. The
    /// FinOps tab loads the server it is opened for, which need not be the server tab that is active, so the times read
    /// that server's wall time in Server mode. Null (a row built without one) falls back to the active server's.
    /// </summary>
    public ServerClock? Clock { get; set; }

    /// <summary>
    /// What the First Seen and Last Seen columns show (#4766): the instant in the display mode on the row's server's clock,
    /// with its UTC offset added in the repeated autumn hour
    /// (<see cref="ViewerTimeHelper.FormatForDisplay(DateTime, TimeZoneInfo, string)"/>). The columns bind these
    /// and sort by <see cref="FirstSeenLocal"/> and <see cref="LastSeenLocal"/>, so the order stays chronological.
    /// </summary>
    public string FirstSeenText => ViewerTimeHelper.FormatForDisplay(FirstSeenUtc, DisplayZoneNow(Clock), "yyyy-MM-dd HH:mm");
    public string LastSeenText => ViewerTimeHelper.FormatForDisplay(LastSeenUtc, DisplayZoneNow(Clock), "yyyy-MM-dd HH:mm");

    /// <summary>The zone the current display mode names for <paramref name="rowClock"/>'s server, else the active server's.</summary>
    internal static TimeZoneInfo DisplayZoneNow(ServerClock? rowClock) =>
        ViewerTimeHelper.DisplayZoneFor(ViewerTimeHelper.CurrentDisplayMode, rowClock ?? ViewerTimeHelper.ActiveServerClock);
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

    /* Three of the four hardware cells below read as ABSENT for an Azure SQL Database (engine edition 5): its collected
       sys.dm_os_sys_info memory, socket count and cores per socket are the HOST's, not the database's allocation (a 1-vCore
       database read 0 sockets, 32 cores per socket and about 912 GB), and the grid draws an absent value as a blank cell. The
       CPU count is the database's own scheduler count (a 1-vCore database reads 2), so it is shown as stored. The stored values
       are kept behind the properties, so the order the loader assigns them in does not matter and no calculation loses
       its input. */
    private int _cpuCount;
    private long _physicalMemoryMb;
    private int? _socketCount;
    private int? _coresPerSocket;
    private string? _hardwareUnavailableReason;
    private bool HostHardware => ServerHardwareScope.HardwareIsTheHosts(EngineEdition);

    public int? CpuCount { get => _cpuCount; set => _cpuCount = value ?? 0; }
    public long? PhysicalMemoryMb { get => HostHardware ? null : _physicalMemoryMb; set => _physicalMemoryMb = value ?? 0L; }
    public int? SocketCount { get => HostHardware ? null : _socketCount; set => _socketCount = value; }
    public int? CoresPerSocket { get => HostHardware ? null : _coresPerSocket; set => _coresPerSocket = value; }
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
    /// The UTC instants behind <see cref="InventoryAsOf"/> and <see cref="LastCollected"/> (#4766). A converted wall clock
    /// cannot say which of the two 01:30s of the repeated autumn hour it was, so the column text is worded from these.
    /// </summary>
    public DateTime? InventoryAsOfUtc { get; set; }

    /// <summary>
    /// What the Inventory As Of column shows (#4766): the instant in the display mode, with its UTC offset added in the
    /// repeated autumn hour, and nothing for a server with no snapshot. The column binds this and sorts by
    /// <see cref="InventoryAsOf"/>.
    /// </summary>
    public string InventoryAsOfText => InventoryAsOfUtc.HasValue
        ? ViewerTimeHelper.FormatForDisplay(InventoryAsOfUtc.Value, ApplicationConnectionRow.DisplayZoneNow(Clock), "yyyy-MM-dd HH:mm")
        : "";

    /// <summary>
    /// The clock of THIS row's server, stamped by the loader (#4766): its collected clock, else the viewer machine's
    /// offset (<see cref="ViewerTimeHelper.ClockForServerOrMachine"/>). Server Inventory is one row per server, so each
    /// row's times read its own server's wall time in Server mode and not the active server tab's. Null (a row built
    /// without one) falls back to the active server's.
    /// </summary>
    public ServerClock? Clock { get; set; }

    /// <summary>
    /// The newest collection of ANY kind for this server — <c>MAX(collection_time)</c> across
    /// <c>v_collection_log</c>, the same signal <c>list_servers</c> and the Overview cards use (#2359). This is
    /// the real freshness heartbeat, and it moves every sweep.
    /// </summary>
    public DateTime? LastCollected { get; set; }

    /// <summary>The UTC instant behind <see cref="LastCollected"/>; see <see cref="InventoryAsOfUtc"/>.</summary>
    public DateTime? LastCollectedUtc { get; set; }

    /// <summary>What the Last Collected column shows (#4766); see <see cref="InventoryAsOfText"/>. The column sorts by <see cref="LastCollected"/>.</summary>
    public string LastCollectedText => LastCollectedUtc.HasValue
        ? ViewerTimeHelper.FormatForDisplay(LastCollectedUtc.Value, ApplicationConnectionRow.DisplayZoneNow(Clock), "yyyy-MM-dd HH:mm")
        : "";
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
    public string? HardwareUnavailableReason
    {
        /* An Azure SQL Database's blank hardware cells say why, in the column that already carries a read's own reason. */
        get => _hardwareUnavailableReason ?? (HostHardware ? ServerHardwareScope.InventoryHardwareNote : null);
        set => _hardwareUnavailableReason = value;
    }

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

/// <summary>Database with zero query executions over the window (Optimization sub-tab).
/// <see cref="LastExecutionTime"/> is <c>query_stats.last_execution_time</c>, the monitored server's own
/// wall clock, and <c>GetIdleDatabasesAsync</c> reads it VERBATIM — see that read's own comment for why,
/// and for the contrast with the collection_time-derived timestamps in the same port. The grid binds it
/// directly with a XAML <c>StringFormat</c>, so no renderer sees it in either SKU.</summary>
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

    /// <summary>
    /// The overall score: CPU 40%, memory 30%, storage 30%. A null <paramref name="cpu"/> means the window held no CPU
    /// sample: there is nothing to score, and scoring the 0 it reads as would be a full 100 made from nothing. The term is
    /// then left out, not scored as zero and not scored as a default, and memory and storage keep their weights over their
    /// own total (30:30 over 60).
    /// </summary>
    public static int Overall(int? cpu, int memory, int storage)
    {
        if (cpu is int cpuScore)
            return (int)(cpuScore * 0.40 + memory * 0.30 + storage * 0.30);

        /* integer weights, so no floating-point error can truncate 100 to 99 */
        return (memory * 30 + storage * 30) / 60;
    }

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

/// <summary>Per-index usage with unused/write-only classification (Storage Growth index drill).
/// <see cref="LastUserAccess"/> is a <c>GREATEST</c> over <c>index_object_stats</c>' four
/// <c>last_user_*</c> columns, all of them the monitored server's own wall clock, and the read takes it
/// VERBATIM — see that read's own comment.</summary>
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

    /// <summary>
    /// <c>schema.table</c> for the Locking &amp; Contention grid's Table column (#3576). The grid used to bind
    /// the bare <see cref="TableName"/>, which is ambiguous the moment two schemas hold a table of the same
    /// name — routine on Azure SQL DB, where per-tenant or per-environment schemas are the usual pattern — so
    /// two different <c>Orders</c> tables read as one. One computed string (rather than a converter or a second
    /// column) so the column sorts, filters, and CSV-exports on the qualified name as a unit. Falls back to the
    /// bare name when the schema is empty, the same shape as <c>ProcedureStatsRow.FullName</c>. Mirrors Lite's
    /// <c>IndexLockingRow.FullName</c> exactly.
    /// </summary>
    public string FullName => string.IsNullOrEmpty(SchemaName) ? TableName : $"{SchemaName}.{TableName}";

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
