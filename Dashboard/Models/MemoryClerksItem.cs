using System;

namespace PerformanceMonitorDashboard.Models
{
    public class MemoryClerksItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public string ClerkType { get; set; } = string.Empty;
        public short MemoryNodeId { get; set; }

        // Raw cumulative values
        public long? PagesKb { get; set; }
        public long? VirtualMemoryReservedKb { get; set; }
        public long? VirtualMemoryCommittedKb { get; set; }
        public long? AweAllocatedKb { get; set; }
        public long? SharedMemoryReservedKb { get; set; }
        public long? SharedMemoryCommittedKb { get; set; }

        // Delta calculations
        public long? PagesKbDelta { get; set; }
        public long? VirtualMemoryReservedKbDelta { get; set; }
        public long? VirtualMemoryCommittedKbDelta { get; set; }
        public long? AweAllocatedKbDelta { get; set; }
        public long? SharedMemoryReservedKbDelta { get; set; }
        public long? SharedMemoryCommittedKbDelta { get; set; }
        public int? SampleIntervalSeconds { get; set; }

        // Display helpers (convert KB to MB)
        public decimal PagesMb => (PagesKb ?? 0) / 1024.0m;
        public decimal VirtualMemoryReservedMb => (VirtualMemoryReservedKb ?? 0) / 1024.0m;
        public decimal VirtualMemoryCommittedMb => (VirtualMemoryCommittedKb ?? 0) / 1024.0m;
        public decimal AweAllocatedMb => (AweAllocatedKb ?? 0) / 1024.0m;

        // Analysis columns (from report.top_memory_consumers view logic)
        public decimal? PercentOfTotal { get; set; }
        public string ConcernLevel { get; set; } = string.Empty;
        public string ClerkDescription { get; set; } = string.Empty;
    }
}
