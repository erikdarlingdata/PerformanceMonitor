using System;

namespace PerformanceMonitorDashboard.Models
{
    public class QueryStatsItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public string? QueryHash { get; set; }
        public string? QueryPlanHash { get; set; }
        public string? SqlHandle { get; set; }
        public string? PlanHandle { get; set; }
        public string ObjectType { get; set; } = string.Empty;
        public string? ObjectName { get; set; }
        public DateTime? FirstExecutionTime { get; set; }
        public DateTime? LastExecutionTime { get; set; }

        // Cumulative values
        public long ExecutionCount { get; set; }
        public long TotalWorkerTime { get; set; }
        public long TotalElapsedTime { get; set; }
        public long TotalLogicalReads { get; set; }
        public long TotalLogicalWrites { get; set; }
        public long TotalPhysicalReads { get; set; }
        public long TotalRows { get; set; }
        public long TotalSpills { get; set; }

        // Averages (pre-calculated in view, in milliseconds for times)
        public double? AvgWorkerTimeMs { get; set; }
        public double? MinWorkerTimeMs { get; set; }
        public double? MaxWorkerTimeMs { get; set; }
        public double? AvgElapsedTimeMs { get; set; }
        public double? MinElapsedTimeMs { get; set; }
        public double? MaxElapsedTimeMs { get; set; }
        public long? AvgLogicalReads { get; set; }
        public long? AvgLogicalWrites { get; set; }
        public long? AvgPhysicalReads { get; set; }
        public long? MinPhysicalReads { get; set; }
        public long? MaxPhysicalReads { get; set; }
        public long? AvgRows { get; set; }
        public long? MinRows { get; set; }
        public long? MaxRows { get; set; }

        // DOP and memory
        public short? MinDop { get; set; }
        public short? MaxDop { get; set; }
        public long? MinGrantKb { get; set; }
        public long? MaxGrantKb { get; set; }
        public long? MinSpills { get; set; }
        public long? MaxSpills { get; set; }

        // Display helpers
        public double TotalWorkerTimeSec => TotalWorkerTime / 1000000.0;
        public double TotalElapsedTimeSec => TotalElapsedTime / 1000000.0;
        public double? MaxGrantMb => MaxGrantKb.HasValue ? MaxGrantKb.Value / 1024.0 : null;

        // CPU time aliases (Worker time = CPU time in SQL Server)
        public double TotalCpuTimeMs => TotalWorkerTime / 1000.0;
        public double? AvgCpuTimeMs => AvgWorkerTimeMs;
        public double TotalElapsedTimeMs => TotalElapsedTime / 1000.0;

        // XAML binding compatibility aliases
        public DateTime? CollectionTime => LastExecutionTime;
        public DateTime? CreationTime => FirstExecutionTime;
        // Min/Max values in microseconds for XAML columns that display μs
        public long? MinWorkerTime => MinWorkerTimeMs.HasValue ? (long)(MinWorkerTimeMs.Value * 1000) : null;
        public long? MaxWorkerTime => MaxWorkerTimeMs.HasValue ? (long)(MaxWorkerTimeMs.Value * 1000) : null;
        public long? MinElapsedTime => MinElapsedTimeMs.HasValue ? (long)(MinElapsedTimeMs.Value * 1000) : null;
        public long? MaxElapsedTime => MaxElapsedTimeMs.HasValue ? (long)(MaxElapsedTimeMs.Value * 1000) : null;

        // Query text and plan
        public string? QueryText { get; set; }
        public string? QueryPlanXml { get; set; }
    }
}
