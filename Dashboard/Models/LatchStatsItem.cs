using System;

namespace PerformanceMonitorDashboard.Models
{
    public class LatchStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime ServerStartTime { get; set; }
        public string LatchClass { get; set; } = string.Empty;

        // Cumulative values
        public long WaitingRequestsCount { get; set; }
        public long WaitTimeMs { get; set; }
        public long MaxWaitTimeMs { get; set; }

        // Delta calculations
        public long? WaitingRequestsCountDelta { get; set; }
        public long? WaitTimeMsDelta { get; set; }
        public long? MaxWaitTimeMsDelta { get; set; }
        public int? SampleIntervalSeconds { get; set; }

        // Computed helpers (matching SQL computed columns)
        public decimal? WaitTimeMsPerSecond => SampleIntervalSeconds > 0 && WaitTimeMsDelta.HasValue
            ? (decimal)WaitTimeMsDelta.Value / SampleIntervalSeconds.Value
            : null;
        public decimal? WaitingRequestsCountPerSecond => SampleIntervalSeconds > 0 && WaitingRequestsCountDelta.HasValue
            ? (decimal)WaitingRequestsCountDelta.Value / SampleIntervalSeconds.Value
            : null;

        // Display helpers
        public decimal WaitTimeSec => WaitTimeMs / 1000.0m;
        public decimal MaxWaitTimeSec => MaxWaitTimeMs / 1000.0m;

        // Analysis columns (from report.top_latch_contention view logic)
        public string Severity { get; set; } = string.Empty;
        public string LatchDescription { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
        public long? AvgWaitMsPerRequest { get; set; }
        public DateTime? LastSeen { get; set; }
    }
}
