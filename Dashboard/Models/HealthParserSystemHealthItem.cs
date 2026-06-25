// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserSystemHealthItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public string State { get; set; } = string.Empty;
        public long? SpinlockBackoffs { get; set; }
        public string SickSpinlockType { get; set; } = string.Empty;
        public string SickSpinlockTypeAfterAv { get; set; } = string.Empty;
        public long? LatchWarnings { get; set; }
        public long? IsAccessViolationOccurred { get; set; }
        public long? WriteAccessViolationCount { get; set; }
        public long? TotalDumpRequests { get; set; }
        public long? IntervalDumpRequests { get; set; }
        public long? NonYieldingTasksReported { get; set; }
        public long? PageFaults { get; set; }
        public long? SystemCpuUtilization { get; set; }
        public long? SqlCpuUtilization { get; set; }
        public long? BadPagesDetected { get; set; }
        public long? BadPagesFixed { get; set; }
    }
}
