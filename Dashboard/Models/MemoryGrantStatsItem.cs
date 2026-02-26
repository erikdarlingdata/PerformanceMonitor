using System;

namespace PerformanceMonitorDashboard.Models
{
    public class MemoryGrantStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime ServerStartTime { get; set; }
        public short ResourceSemaphoreId { get; set; }
        public int PoolId { get; set; }

        // Memory metrics
        public decimal? TargetMemoryMb { get; set; }
        public decimal? MaxTargetMemoryMb { get; set; }
        public decimal? TotalMemoryMb { get; set; }
        public decimal? AvailableMemoryMb { get; set; }
        public decimal? GrantedMemoryMb { get; set; }
        public decimal? UsedMemoryMb { get; set; }

        // Point-in-time counts
        public int? GranteeCount { get; set; }
        public int? WaiterCount { get; set; }
        public long? TimeoutErrorCount { get; set; }
        public long? ForcedGrantCount { get; set; }

        // Delta columns (calculated by framework)
        public long? TimeoutErrorCountDelta { get; set; }
        public long? ForcedGrantCountDelta { get; set; }
        public int? SampleIntervalSeconds { get; set; }
    }
}
