/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitorDashboard.Services
{
    // ============================================
    // FinOps Model Classes
    // ============================================

    public class FinOpsDatabaseResourceUsage
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

    public class FinOpsUtilizationEfficiency
    {
        public decimal AvgCpuPct { get; set; }
        public int MaxCpuPct { get; set; }
        public decimal P95CpuPct { get; set; }
        public long CpuSamples { get; set; }
        public int TotalMemoryMb { get; set; }
        public int TargetMemoryMb { get; set; }
        public int PhysicalMemoryMb { get; set; }
        public decimal MemoryRatio { get; set; }
        public int MemoryUtilizationPct { get; set; }
        public int WorkerThreadsCurrent { get; set; }
        public int WorkerThreadsMax { get; set; }
        public decimal WorkerThreadRatio { get; set; }
        public int CpuCount { get; set; }
        public int BufferPoolMb { get; set; }
        public int TotalServerMemoryMb { get; set; }
        public string ProvisioningStatus { get; set; } = "";

        // FinOps cost — proportional to server monthly budget
        public decimal MonthlyCost { get; set; }
        public decimal AnnualCost => MonthlyCost * 12m;

        // Health score (Increment 6)
        public decimal FreeSpacePct { get; set; }
        public int HealthScore { get; set; }
        public string HealthScoreColor => FinOpsHealthCalculator.ScoreColor(HealthScore);
    }

    public class FinOpsApplicationResourceUsage
    {
        public string ApplicationName { get; set; } = "";
        public int AvgConnections { get; set; }
        public int MaxConnections { get; set; }
        public long SampleCount { get; set; }
        public DateTime FirstSeen { get; set; }
        public DateTime LastSeen { get; set; }
    }

    public class FinOpsServerInventory
    {
        public string ServerName { get; set; } = "";
        public string Edition { get; set; } = "";
        public string SqlVersion { get; set; } = "";
        public string HostOsVersion { get; set; } = "";
        public int CpuCount { get; set; }
        public long PhysicalMemoryMb { get; set; }
        public int? SocketCount { get; set; }
        public int? CoresPerSocket { get; set; }
        public int? EngineEdition { get; set; }
        public DateTime? SqlServerStartTime { get; set; }
        public DateTime? LastUpdated { get; set; }
        public bool? IsHadrEnabled { get; set; }
        public bool? IsClustered { get; set; }
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
        public string ProvisioningDisplay => ProvisioningStatus?.Replace("_", " ") ?? "";
        public string HadrDisplay => IsHadrEnabled.HasValue ? (IsHadrEnabled.Value ? "Yes" : "No") : "";
        public string ClusteredDisplay => IsClustered.HasValue ? (IsClustered.Value ? "Yes" : "No") : "";

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

    public class FinOpsDatabaseSizeStats
    {
        public DateTime CollectionTime { get; set; }
        public string DatabaseName { get; set; } = "";
        public int DatabaseId { get; set; }
        public int FileId { get; set; }
        public string FileTypeDesc { get; set; } = "";
        public string FileName { get; set; } = "";
        public string PhysicalName { get; set; } = "";
        public decimal TotalSizeMb { get; set; }
        public decimal UsedSizeMb { get; set; }
        public decimal FreeSpaceMb { get; set; }
        public decimal UsedPct { get; set; }
        public decimal AutoGrowthMb { get; set; }
        public decimal MaxSizeMb { get; set; }
        public string RecoveryModelDesc { get; set; } = "";
        public int CompatibilityLevel { get; set; }
        public string StateDesc { get; set; } = "";
        public string VolumeMountPoint { get; set; } = "";
        public decimal VolumeTotalMb { get; set; }
        public decimal VolumeFreeMb { get; set; }
        public bool? IsPercentGrowth { get; set; }
        public int? GrowthPct { get; set; }
        public int? VlfCount { get; set; }

        // FinOps cost — proportional share of server monthly budget
        public decimal MonthlyCostShare { get; set; }

        public string GrowthDisplay => IsPercentGrowth switch
        {
            null  => "-",
            true  => GrowthPct.HasValue ? $"{GrowthPct}%" : "-",
            false => AutoGrowthMb == 0 ? "Disabled" : $"{AutoGrowthMb:N0} MB"
        };

        public decimal AutoGrowthSort => IsPercentGrowth switch
        {
            null  => -1m,
            true  => (decimal)(GrowthPct ?? -1),
            false => AutoGrowthMb
        };

        public string VlfCountDisplay => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase)
            ? (VlfCount?.ToString() ?? "-") : "N/A";

        public int VlfCountSort => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase)
            ? (VlfCount ?? 0) : -1;
    }

    public class FinOpsTopResourceConsumer
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

    public class FinOpsDatabaseSizeSummary
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

    public class FinOpsStorageGrowthRow
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

    public class FinOpsIdleDatabase
    {
        public string DatabaseName { get; set; } = "";
        public decimal TotalSizeMb { get; set; }
        public int FileCount { get; set; }
        public DateTime? LastExecutionTime { get; set; }
    }

    public class FinOpsTempdbSummary
    {
        public string Metric { get; set; } = "";
        public decimal CurrentMb { get; set; }
        public decimal Peak24hMb { get; set; }
        public string Warning { get; set; } = "";
    }

    public class FinOpsWaitCategorySummary
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

    public class FinOpsExpensiveQuery
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

    public class IndexCleanupResult
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

    public class FinOpsProvisioningTrend
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

    public class FinOpsMemoryGrantEfficiency
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

    public class IndexCleanupSummary
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

    public class FinOpsRecommendation
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

    public class FinOpsHighImpactQuery
    {
        public string QueryHashDisplay { get; set; } = "";
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
}
