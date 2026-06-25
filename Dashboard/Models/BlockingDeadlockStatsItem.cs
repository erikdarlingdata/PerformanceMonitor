// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class BlockingDeadlockStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public long BlockingEventCount { get; set; }
        public long TotalBlockingDurationMs { get; set; }
        public long MaxBlockingDurationMs { get; set; }
        public decimal AvgBlockingDurationMs { get; set; }
        public long DeadlockCount { get; set; }
        public long TotalDeadlockWaitTimeMs { get; set; }
        public long VictimCount { get; set; }
        public long BlockingEventCountDelta { get; set; }
        public long TotalBlockingDurationMsDelta { get; set; }
        public long MaxBlockingDurationMsDelta { get; set; }
        public long DeadlockCountDelta { get; set; }
        public long TotalDeadlockWaitTimeMsDelta { get; set; }
        public long VictimCountDelta { get; set; }
        public int SampleIntervalSeconds { get; set; }
    }
}
