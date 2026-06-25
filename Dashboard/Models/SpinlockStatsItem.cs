using System;

namespace PerformanceMonitorDashboard.Models
{
    public class SpinlockStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime ServerStartTime { get; set; }
        public string SpinlockName { get; set; } = string.Empty;

        // Cumulative values
        public long Collisions { get; set; }
        public long Spins { get; set; }
        public decimal SpinsPerCollision { get; set; }
        public long SleepTime { get; set; }
        public long Backoffs { get; set; }

        // Delta calculations
        public long? CollisionsDelta { get; set; }
        public long? SpinsDelta { get; set; }
        public long? SleepTimeDelta { get; set; }
        public long? BackoffsDelta { get; set; }
        public int? SampleIntervalSeconds { get; set; }

        // Computed helpers (matching SQL computed columns)
        public decimal? CollisionsPerSecond => SampleIntervalSeconds > 0 && CollisionsDelta.HasValue
            ? (decimal)CollisionsDelta.Value / SampleIntervalSeconds.Value
            : null;
        public decimal? SpinsPerSecond => SampleIntervalSeconds > 0 && SpinsDelta.HasValue
            ? (decimal)SpinsDelta.Value / SampleIntervalSeconds.Value
            : null;

        // Analysis columns (from report.top_spinlock_contention view logic)
        public string SpinlockDescription { get; set; } = string.Empty;
        public DateTime? LastSeen { get; set; }
    }
}
