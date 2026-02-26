using System;

namespace PerformanceMonitorDashboard.Models
{
    public class QueryStoreItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public long QueryId { get; set; }
        public string? ExecutionTypeDesc { get; set; }
        public string? ModuleName { get; set; }
        public DateTime? FirstExecutionTime { get; set; }
        public DateTime? LastExecutionTime { get; set; }

        // Execution count and plan info
        public long ExecutionCount { get; set; }
        public long PlanCount { get; set; }

        // Duration metrics (pre-calculated in ms by view)
        public double? AvgDurationMs { get; set; }
        public double? MinDurationMs { get; set; }
        public double? MaxDurationMs { get; set; }

        // CPU time metrics (pre-calculated in ms by view)
        public double? AvgCpuTimeMs { get; set; }
        public double? MinCpuTimeMs { get; set; }
        public double? MaxCpuTimeMs { get; set; }

        // Logical IO reads
        public long? AvgLogicalReads { get; set; }
        public long? MinLogicalReads { get; set; }
        public long? MaxLogicalReads { get; set; }

        // Logical IO writes
        public long? AvgLogicalWrites { get; set; }
        public long? MinLogicalWrites { get; set; }
        public long? MaxLogicalWrites { get; set; }

        // Physical IO reads
        public long? AvgPhysicalReads { get; set; }
        public long? MinPhysicalReads { get; set; }
        public long? MaxPhysicalReads { get; set; }

        // DOP
        public long? MinDop { get; set; }
        public long? MaxDop { get; set; }

        // Memory grant (8KB pages)
        public long? AvgMemoryPages { get; set; }
        public long? MinMemoryPages { get; set; }
        public long? MaxMemoryPages { get; set; }

        // Row count
        public long? AvgRowcount { get; set; }
        public long? MinRowcount { get; set; }
        public long? MaxRowcount { get; set; }

        // Tempdb space (8KB pages)
        public long? AvgTempdbPages { get; set; }
        public long? MinTempdbPages { get; set; }
        public long? MaxTempdbPages { get; set; }

        // Plan information
        public string? PlanType { get; set; }
        public bool IsForcedPlan { get; set; }
        public short? CompatibilityLevel { get; set; }

        // Plan forcing details
        public long? ForceFailureCount { get; set; }
        public string? LastForceFailureReasonDesc { get; set; }
        public string? PlanForcingType { get; set; }

        // CLR time (pre-calculated in ms)
        public double? MinClrTimeMs { get; set; }
        public double? MaxClrTimeMs { get; set; }

        // Physical IO reads (memory-optimized tables, SQL 2017+)
        public long? MinNumPhysicalIoReads { get; set; }
        public long? MaxNumPhysicalIoReads { get; set; }

        // Log bytes used (SQL 2017+)
        public long? MinLogBytesUsed { get; set; }
        public long? MaxLogBytesUsed { get; set; }

        // Handle
        public string? QueryPlanHash { get; set; }

        // Query text and plan
        public string? QuerySqlText { get; set; }
        public string? QueryPlanXml { get; set; }

        // Display helpers - memory in MB (8KB pages * 8 / 1024)
        public double? AvgMemoryMb => AvgMemoryPages.HasValue ? AvgMemoryPages.Value * 8.0 / 1024.0 : null;
        public double? MinMemoryMb => MinMemoryPages.HasValue ? MinMemoryPages.Value * 8.0 / 1024.0 : null;
        public double? MaxMemoryMb => MaxMemoryPages.HasValue ? MaxMemoryPages.Value * 8.0 / 1024.0 : null;

        // Tempdb in MB
        public double? AvgTempdbMb => AvgTempdbPages.HasValue ? AvgTempdbPages.Value * 8.0 / 1024.0 : null;
        public double? MinTempdbMb => MinTempdbPages.HasValue ? MinTempdbPages.Value * 8.0 / 1024.0 : null;
        public double? MaxTempdbMb => MaxTempdbPages.HasValue ? MaxTempdbPages.Value * 8.0 / 1024.0 : null;

        // Property aliases for XAML binding compatibility
        public string? QueryText => QuerySqlText;
        public long CountExecutions => ExecutionCount;
        public DateTime? CollectionTime => LastExecutionTime;
        public long? PlanId => PlanCount > 0 ? PlanCount : null; // Show plan count in PlanId column

        // IO aliases (XAML uses "Io" naming, model uses simplified names)
        public long? AvgLogicalIoReads => AvgLogicalReads;
        public long? MinLogicalIoReads => MinLogicalReads;
        public long? MaxLogicalIoReads => MaxLogicalReads;
        public long? AvgLogicalIoWrites => AvgLogicalWrites;
        public long? MinLogicalIoWrites => MinLogicalWrites;
        public long? MaxLogicalIoWrites => MaxLogicalWrites;
        public long? AvgPhysicalIoReads => AvgPhysicalReads;
        public long? MinPhysicalIoReads => MinPhysicalReads;
        public long? MaxPhysicalIoReads => MaxPhysicalReads;
    }
}
