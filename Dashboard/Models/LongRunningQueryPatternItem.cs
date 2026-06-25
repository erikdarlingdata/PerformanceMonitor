// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class LongRunningQueryPatternItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public string QueryPattern { get; set; } = string.Empty;
        public long Executions { get; set; }
        public decimal AvgDurationSec { get; set; }
        public decimal MaxDurationSec { get; set; }
        public decimal AvgCpuSec { get; set; }
        public long AvgReads { get; set; }
        public long AvgWrites { get; set; }
        public string ConcernLevel { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
        public string SampleQueryText { get; set; } = string.Empty;
        public DateTime? LastExecution { get; set; }
    }
}
