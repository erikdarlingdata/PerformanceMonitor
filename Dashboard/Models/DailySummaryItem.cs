// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class DailySummaryItem
    {
        public DateTime SummaryDate { get; set; }
        public decimal? TotalWaitTimeSec { get; set; }
        public string TopWaitType { get; set; } = string.Empty;
        public long ExpensiveQueriesCount { get; set; }
        public long DeadlockCount { get; set; }
        public long BlockingEventsCount { get; set; }
        public long MemoryPressureEvents { get; set; }
        public long HighCpuEvents { get; set; }
        public long CollectorsFailing { get; set; }
        public string OverallHealth { get; set; } = string.Empty;
    }
}
