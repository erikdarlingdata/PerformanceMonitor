using System;

namespace PerformanceMonitorDashboard.Models
{
    public class ProcedureStatsItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public int ObjectId { get; set; }
        public string? ObjectName { get; set; }
        public string? SchemaName { get; set; }
        public string? ProcedureName { get; set; }
        public string ObjectType { get; set; } = string.Empty;
        public string? TypeDesc { get; set; }
        public DateTime? FirstCachedTime { get; set; }
        public DateTime? LastExecutionTime { get; set; }

        // Cumulative values
        public long ExecutionCount { get; set; }
        public long TotalWorkerTime { get; set; }
        public long TotalElapsedTime { get; set; }
        public long TotalLogicalReads { get; set; }
        public long TotalLogicalWrites { get; set; }
        public long TotalPhysicalReads { get; set; }
        public long? TotalSpills { get; set; }

        // Averages (pre-calculated in view, in milliseconds for times)
        public double? AvgWorkerTimeMs { get; set; }
        public double? MinWorkerTimeMs { get; set; }
        public double? MaxWorkerTimeMs { get; set; }
        public double? AvgElapsedTimeMs { get; set; }
        public double? MinElapsedTimeMs { get; set; }
        public double? MaxElapsedTimeMs { get; set; }
        public long? AvgLogicalReads { get; set; }
        public long? MinLogicalReads { get; set; }
        public long? MaxLogicalReads { get; set; }
        public long? AvgLogicalWrites { get; set; }
        public long? MinLogicalWrites { get; set; }
        public long? MaxLogicalWrites { get; set; }
        public long? AvgPhysicalReads { get; set; }
        public long? MinPhysicalReads { get; set; }
        public long? MaxPhysicalReads { get; set; }
        public long? AvgSpills { get; set; }
        public long? MinSpills { get; set; }
        public long? MaxSpills { get; set; }

        // Display helpers
        public double TotalWorkerTimeSec => TotalWorkerTime / 1000000.0;
        public double TotalElapsedTimeSec => TotalElapsedTime / 1000000.0;
        public string FullObjectName => ObjectName ?? "";

        // CPU time aliases (Worker time = CPU time in SQL Server)
        public double TotalCpuTimeMs => TotalWorkerTime / 1000.0;
        public double? AvgCpuTimeMs => AvgWorkerTimeMs;
        public double TotalElapsedTimeMs => TotalElapsedTime / 1000.0;

        // XAML binding compatibility aliases
        public DateTime? CollectionTime => LastExecutionTime;
        public DateTime? CachedTime => FirstCachedTime;
        // Min/Max values in microseconds for XAML columns that display μs
        public long? MinWorkerTime => MinWorkerTimeMs.HasValue ? (long)(MinWorkerTimeMs.Value * 1000) : null;
        public long? MaxWorkerTime => MaxWorkerTimeMs.HasValue ? (long)(MaxWorkerTimeMs.Value * 1000) : null;
        public long? MinElapsedTime => MinElapsedTimeMs.HasValue ? (long)(MinElapsedTimeMs.Value * 1000) : null;
        public long? MaxElapsedTime => MaxElapsedTimeMs.HasValue ? (long)(MaxElapsedTimeMs.Value * 1000) : null;

        // Handles
        public string? SqlHandle { get; set; }
        public string? PlanHandle { get; set; }

        // Query plan
        public string? QueryPlanXml { get; set; }
    }
}
