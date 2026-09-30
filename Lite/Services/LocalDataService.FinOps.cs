/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

public class ProvisioningTrendRow
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

public class MemoryGrantEfficiencyRow
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

public class TopResourceConsumerRow
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

public class DatabaseSizeSummaryRow
{
    public string DatabaseName { get; set; } = "";
    public decimal TotalMb { get; set; }
    public decimal? UsedMb { get; set; }
    public decimal FreeMb => UsedMb.HasValue ? TotalMb - UsedMb.Value : TotalMb;
    public decimal UsedPct => TotalMb > 0 && UsedMb.HasValue ? Math.Round(UsedMb.Value * 100m / TotalMb, 1) : 0;

    /* Star-width GridLength for XAML binding — drives the stacked bar proportions */
    public System.Windows.GridLength UsedStarWidth =>
        new(Math.Max((double)(UsedMb ?? 0m), 0.1), System.Windows.GridUnitType.Star);
    public System.Windows.GridLength FreeStarWidth =>
        new(Math.Max((double)FreeMb, 0.1), System.Windows.GridUnitType.Star);
}

public class UtilizationEfficiencyRow
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

    /// <summary>The server's OWN CPU count, 0 when there is none. On an Azure SQL Database (<see cref="EngineEdition"/> 5) the
    /// stored <c>cpu_count</c> is the HOST's, so this is the <c>vcore_count</c> parsed from the service objective and 0 for an
    /// objective that names none (a DTU model), which the card shows as n/a. It is never the host's count.</summary>
    public int CpuCount { get; set; }

    /// <summary>The engine edition of the server these figures describe (<c>SERVERPROPERTY('EngineEdition')</c>, 0 when unread).
    /// On an Azure SQL Database (5) <see cref="PhysicalMemoryMb"/> is the HOST's, so the card does not show it, its verdict
    /// sentence cites no share of it, and the health score has no memory term. The verdict itself reads none of it.</summary>
    public int EngineEdition { get; set; }
    public string ProvisioningStatus { get; set; } = "";

    // FinOps cost — proportional to server monthly budget
    public decimal MonthlyCost { get; set; }
    public decimal AnnualCost => MonthlyCost * 12m;

    // Health score (Increment 6)
    public decimal FreeSpacePct { get; set; }
    public int HealthScore { get; set; }
    public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);

    /// <summary>
    /// The health score for these figures: CPU p95, the buffer pool's share of physical memory, and free storage. On an
    /// Azure SQL Database (<see cref="EngineEdition"/> 5) physical memory is the HOST's, so the memory term is left out, not
    /// scored as zero and not scored as a default, and CPU and storage carry the whole score. Anywhere else the score is
    /// what it always was.
    /// </summary>
    public int ComputeHealthScore()
    {
        var bpRatio = PhysicalMemoryMb > 0 ? (decimal)BufferPoolMb / PhysicalMemoryMb : 0m;
        int? memScore = ServerHardwareScope.HardwareIsTheHosts(EngineEdition) ? null : FinOpsHealthCalculator.MemoryScore(bpRatio);
        return FinOpsHealthCalculator.Overall(
            FinOpsHealthCalculator.CpuScore(P95CpuPct), memScore, FinOpsHealthCalculator.StorageScore(FreeSpacePct));
    }
}

public class DatabaseResourceUsageRow
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

public class ApplicationConnectionRow
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
    public DateTime FirstSeen { get; set; }
    public DateTime LastSeen { get; set; }

    /// <summary>
    /// The clock of the SELECTED server, stamped when the FinOps tab loads the rows (#4766): its own collected clock, else
    /// its open tab's, else the machine's (<see cref="ServerTimeHelper.ClockForServer(ServerClock?, ServerClock?)"/>), the
    /// same rule the PVS trend beside it uses. The FinOps tab lists whichever server its picker names, not the server of
    /// the tab that is active, so these times read that server's wall time in Server mode. Null (a row built without
    /// one) falls back to the active tab's clock.
    /// </summary>
    public ServerClock? Clock { get; set; }

    /// <summary>
    /// <see cref="FirstSeen"/> (naive UTC) on the clock of the selected display mode (#4766): UTC as stored, this machine's
    /// zone, or the selected server's own clock (<see cref="Clock"/>), the zone <see cref="FirstSeenText"/> words it in.
    /// This used to be <c>ToLocalTime()</c> in every mode, and then the active tab's clock, so Server and UTC mode showed
    /// another zone's time under a header every other grid read in the chosen mode. The First Seen column shows
    /// <see cref="FirstSeenText"/> and sorts by this.
    /// </summary>
    public DateTime FirstSeenLocal => DisplayZone.ToDisplay(FirstSeen, DisplayZoneNow);

    /// <summary><see cref="LastSeen"/> on the same clock as <see cref="FirstSeenLocal"/>; the Last Seen column sorts by it.</summary>
    public DateTime LastSeenLocal => DisplayZone.ToDisplay(LastSeen, DisplayZoneNow);

    /// <summary>
    /// What the First Seen column shows (#4766): <see cref="FirstSeen"/> as <see cref="ServerTimeHelper.FormatInstant"/>
    /// words it on the selected server's clock, so in the repeated autumn hour it carries its UTC offset
    /// ("2026-11-01 01:30 -04:00"). A DateTime bound with a XAML <c>StringFormat</c> could not, because it cannot say
    /// which of the two 01:30s it was.
    /// </summary>
    public string FirstSeenText => ServerTimeHelper.FormatInstant(FirstSeen, DisplayZoneNow, "yyyy-MM-dd HH:mm");

    /// <summary>What the Last Seen column shows (#4766); see <see cref="FirstSeenText"/>.</summary>
    public string LastSeenText => ServerTimeHelper.FormatInstant(LastSeen, DisplayZoneNow, "yyyy-MM-dd HH:mm");

    private TimeZoneInfo DisplayZoneNow =>
        ServerTimeHelper.DisplayZoneFor(ServerTimeHelper.CurrentDisplayMode, Clock ?? ServerTimeHelper.ActiveServerClock);
}

public class DatabaseSizeRow
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

    // FinOps cost — proportional share of server monthly budget
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

public class ServerPropertyRow
{
    public string ServerName { get; set; } = "";
    public string Edition { get; set; } = "";
    public string ProductVersion { get; set; } = "";
    public string HostOsVersion { get; set; } = "";
    public string? ProductLevel { get; set; }
    public string? ProductUpdateLevel { get; set; }
    public int EngineEdition { get; set; }

    /* The four hardware cells below read as ABSENT for an Azure SQL Database (engine edition 5): its sys.dm_os_sys_info
       values are the HOST's, not the database's allocation (a 1-vCore serverless database read 2 CPUs, 0 sockets, 32
       cores per socket and 911.9 GB), and the grid draws an absent value as a blank cell. The read values are kept
       behind the properties, so the order the loader assigns them in does not matter and no calculation loses its input. */
    private int _cpuCount;
    private long _physicalMemoryMb;
    private int? _socketCount;
    private int? _coresPerSocket;
    private string? _hardwareUnavailableReason;
    private bool HostHardware => ServerHardwareScope.HardwareIsTheHosts(EngineEdition);

    public int? CpuCount { get => HostHardware ? null : _cpuCount; set => _cpuCount = value ?? 0; }
    public long? PhysicalMemoryMb { get => HostHardware ? null : _physicalMemoryMb; set => _physicalMemoryMb = value ?? 0L; }
    public int? SocketCount { get => HostHardware ? null : _socketCount; set => _socketCount = value; }
    public int? CoresPerSocket { get => HostHardware ? null : _coresPerSocket; set => _coresPerSocket = value; }
    public DateTime? SqlServerStartTime { get; set; }
    public DateTime? LastUpdated { get; set; }
    public bool? IsHadrEnabled { get; set; }
    public bool? IsClustered { get; set; }
    public string AgReplicaRole { get; set; } = "Standalone";

    /// <summary>
    /// Set when the sys.dm_os_sys_info hardware read failed (e.g. an Azure SQL DB monitoring login
    /// without VIEW DATABASE STATE). Null when hardware inventory is available. Surfaced as a
    /// non-alarming note in the FinOps Server Inventory grid (#1535).
    /// </summary>
    public string? HardwareUnavailableReason
    {
        /* An Azure SQL Database's blank hardware cells say why, in the column that already carries a read's own reason. */
        get => _hardwareUnavailableReason ?? (HostHardware ? ServerHardwareScope.InventoryHardwareNote : null);
        set => _hardwareUnavailableReason = value;
    }

    public decimal? AvgCpuPct { get; set; }
    public decimal? StorageTotalGb { get; set; }
    public int? IdleDbCount { get; set; }
    public string? ProvisioningStatus { get; set; }

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

    // FinOps cost — from server config
    public decimal MonthlyCost { get; set; }
    public decimal AnnualCost => MonthlyCost * 12m;

    // License warning (Increment 5)
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

    // Health score (Increment 6)
    public int HealthScore { get; set; }
    public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);
}

public class StorageGrowthRow
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

public class IdleDatabaseRow
{
    public string DatabaseName { get; set; } = "";
    public decimal TotalSizeMb { get; set; }
    public int FileCount { get; set; }
    public DateTime? LastExecutionTime { get; set; }
}

public class TempdbSummaryRow
{
    public string Metric { get; set; } = "";
    public decimal CurrentMb { get; set; }
    public decimal Peak24hMb { get; set; }
    public string Warning { get; set; } = "";
}

public class WaitCategorySummaryRow
{
    public string Category { get; set; } = "";
    public long TotalWaitTimeMs { get; set; }
    public long WaitingTasks { get; set; }
    public decimal PctOfTotal { get; set; }
    public string TopWaitType { get; set; } = "";
    public long TopWaitTimeMs { get; set; }

    // FinOps cost — proportional share of server monthly budget based on wait time fraction
    public decimal MonthlyCostShare { get; set; }
}

public class ExpensiveQueryRow
{
    public string DatabaseName { get; set; } = "";
    public long TotalCpuMs { get; set; }
    public decimal AvgCpuMsPerExec { get; set; }
    public long TotalReads { get; set; }
    public decimal AvgReadsPerExec { get; set; }
    public long Executions { get; set; }
    public string QueryPreview { get; set; } = "";
    public string FullQueryText { get; set; } = "";

    // FinOps cost — proportional share of server monthly budget based on CPU fraction
    public decimal MonthlyCostShare { get; set; }
}

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
    /// The overall score: CPU 40%, memory 30%, storage 30%. A null <paramref name="memory"/> means memory is not
    /// applicable (an Azure SQL Database's physical memory is the host's), so the term is left out, not scored as zero and
    /// not scored as a default: CPU and storage keep their 40:30 weights over their own 70%.
    /// </summary>
    public static int Overall(int cpu, int? memory, int storage) =>
        memory is int mem
            ? (int)(cpu * 0.40 + mem * 0.30 + storage * 0.30)
            : (cpu * 40 + storage * 30) / 70; /* integer weights, so no floating-point error can truncate 100 to 99 */

    public static string ScoreColor(int score) => score switch
    {
        >= 80 => "#27AE60",
        >= 60 => "#F39C12",
        _ => "#E74C3C"
    };
}

public class IndexCleanupResultRow
{
    public string ScriptType { get; set; } = "";
    public string AdditionalInfo { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public string ConsolidationRule { get; set; } = "";
    public string TargetIndexName { get; set; } = "";
    public string SupersededInfo { get; set; } = "";
    public string IndexSizeGb { get; set; } = "";
    public string IndexRows { get; set; } = "";
    public string IndexReads { get; set; } = "";
    public string IndexWrites { get; set; } = "";
    public string OriginalIndexDefinition { get; set; } = "";
    public string Script { get; set; } = "";

    public decimal IndexSizeGbSort => NumericSortHelper.Parse(IndexSizeGb);
    public decimal IndexRowsSort => NumericSortHelper.Parse(IndexRows);
    public decimal IndexReadsSort => NumericSortHelper.Parse(IndexReads);
    public decimal IndexWritesSort => NumericSortHelper.Parse(IndexWrites);
}

public class IndexCleanupSummaryRow
{
    public string Level { get; set; } = "";
    public string DatabaseInfo { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string TablesAnalyzed { get; set; } = "";
    public string TotalIndexes { get; set; } = "";
    public string RemovableIndexes { get; set; } = "";
    public string MergeableIndexes { get; set; } = "";
    public string CompressableIndexes { get; set; } = "";
    public string PercentRemovable { get; set; } = "";
    public string CurrentSizeGb { get; set; } = "";
    public string SizeAfterCleanupGb { get; set; } = "";
    public string SpaceSavedGb { get; set; } = "";
    public string SpaceReductionPercent { get; set; } = "";
    public string CompressionSavingsPotential { get; set; } = "";
    public string CompressionSavingsPotentialTotal { get; set; } = "";
    public string ComputedColumnsWithUdfs { get; set; } = "";
    public string CheckConstraintsWithUdfs { get; set; } = "";
    public string FilteredIndexesNeedingIncludes { get; set; } = "";
    public string TotalRows { get; set; } = "";
    public string ReadsBreakdown { get; set; } = "";
    public string Writes { get; set; } = "";
    public string DailyWriteOpsSaved { get; set; } = "";
    public string LockWaitCount { get; set; } = "";
    public string DailyLockWaitsSaved { get; set; } = "";
    public string AvgLockWaitMs { get; set; } = "";
    public string LatchWaitCount { get; set; } = "";
    public string DailyLatchWaitsSaved { get; set; } = "";
    public string AvgLatchWaitMs { get; set; } = "";

    public decimal TotalIndexesSort => NumericSortHelper.Parse(TotalIndexes);
    public decimal RemovableIndexesSort => NumericSortHelper.Parse(RemovableIndexes);
    public decimal MergeableIndexesSort => NumericSortHelper.Parse(MergeableIndexes);
    public decimal CompressableIndexesSort => NumericSortHelper.Parse(CompressableIndexes);
    public decimal PercentRemovableSort => NumericSortHelper.Parse(PercentRemovable);
    public decimal CurrentSizeGbSort => NumericSortHelper.Parse(CurrentSizeGb);
    public decimal SizeAfterCleanupGbSort => NumericSortHelper.Parse(SizeAfterCleanupGb);
    public decimal SpaceSavedGbSort => NumericSortHelper.Parse(SpaceSavedGb);
    public decimal SpaceReductionPercentSort => NumericSortHelper.Parse(SpaceReductionPercent);
    public decimal TotalRowsSort => NumericSortHelper.Parse(TotalRows);
    public decimal WritesSort => NumericSortHelper.Parse(Writes);
    public decimal DailyWriteOpsSavedSort => NumericSortHelper.Parse(DailyWriteOpsSaved);
    public decimal LockWaitCountSort => NumericSortHelper.Parse(LockWaitCount);
    public decimal DailyLockWaitsSavedSort => NumericSortHelper.Parse(DailyLockWaitsSaved);
    public decimal AvgLockWaitMsSort => NumericSortHelper.Parse(AvgLockWaitMs);
    public decimal LatchWaitCountSort => NumericSortHelper.Parse(LatchWaitCount);
    public decimal DailyLatchWaitsSavedSort => NumericSortHelper.Parse(DailyLatchWaitsSaved);
    public decimal AvgLatchWaitMsSort => NumericSortHelper.Parse(AvgLatchWaitMs);
}

public class RecommendationRow
{
    public string Category { get; set; } = "";
    public string Severity { get; set; } = "";
    public string Confidence { get; set; } = "";
    public string Finding { get; set; } = "";
    public string Detail { get; set; } = "";
    public decimal? EstMonthlySavings { get; set; }
    public string EstMonthlySavingsDisplay => EstMonthlySavings.HasValue ? $"${EstMonthlySavings.Value:N0}" : "";
    public int SeveritySort => Severity switch
    {
        "High" => 1,
        "Medium" => 2,
        "Low" => 3,
        _ => 4
    };
}

/// <summary>
/// Identifies top-N queries per resource dimension, computes PERCENT_RANK
/// and share percentages, and returns the "interesting" set sorted by impact score.
/// Extracted from GetHighImpactQueriesAsync for testability.
/// </summary>
public static class HighImpactScorer
{
    /// <summary>
    /// Scores a list of query rows by identifying top-N per resource dimension,
    /// computing PERCENT_RANK and share percentages, and returning the interesting
    /// set sorted by impact score descending.
    /// </summary>
    public static List<HighImpactQueryRow> Score(List<HighImpactQueryRow> allRows, int topN = 10)
    {
        if (allRows.Count == 0) return allRows;

        // Step 1: Find "interesting" hashes — top N per dimension via UNION
        var interesting = new HashSet<string>();
        foreach (var hash in allRows.OrderByDescending(r => r.TotalCpuMs).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalDurationMs).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalReads).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalWrites).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalMemoryMb).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalExecutions).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);

        var filtered = allRows.Where(r => interesting.Contains(r.QueryHash)).ToList();

        if (filtered.Count == 0) return filtered;

        // Step 2: Compute PERCENT_RANK and share for the interesting set
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

public class HighImpactQueryRow
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

    public string ImpactScoreColor => ImpactScore switch
    {
        >= 80 => "#E74C3C",
        >= 60 => "#F39C12",
        _ => "#27AE60"
    };
}

internal static class NumericSortHelper
{
    internal static decimal Parse(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return -1m;
        var cleaned = s.Replace(",", "").Replace("%", "").Trim();
        return decimal.TryParse(cleaned, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : -1m;
    }
}
