using System;

namespace PerformanceMonitorDashboard.Models
{
    public class MemoryStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }

        // Memory clerks summary
        public decimal BufferPoolMb { get; set; }
        public decimal PlanCacheMb { get; set; }
        public decimal OtherMemoryMb { get; set; }
        public decimal TotalMemoryMb { get; set; }

        // Process memory
        public decimal PhysicalMemoryInUseMb { get; set; }
        public decimal AvailablePhysicalMemoryMb { get; set; }
        public int MemoryUtilizationPercentage { get; set; }

        // Server and target memory
        public decimal? TotalPhysicalMemoryMb { get; set; }
        public decimal? CommittedTargetMemoryMb { get; set; }

        // Pressure warnings
        public bool BufferPoolPressureWarning { get; set; }
        public bool PlanCachePressureWarning { get; set; }

        // Computed percentages (calculated in C#, matching SQL computed columns)
        public decimal? BufferPoolPercentage => TotalMemoryMb > 0
            ? BufferPoolMb * 100.0m / TotalMemoryMb
            : null;
        public decimal? PlanCachePercentage => TotalMemoryMb > 0
            ? PlanCacheMb * 100.0m / TotalMemoryMb
            : null;
    }
}
