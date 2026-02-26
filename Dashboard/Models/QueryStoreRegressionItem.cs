// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class QueryStoreRegressionItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public long QueryId { get; set; }
        public decimal BaselineDurationMs { get; set; }
        public decimal RecentDurationMs { get; set; }
        public decimal DurationRegressionPercent { get; set; }
        public decimal BaselineCpuMs { get; set; }
        public decimal RecentCpuMs { get; set; }
        public decimal CpuRegressionPercent { get; set; }
        public decimal BaselineReads { get; set; }
        public decimal RecentReads { get; set; }
        public decimal IoRegressionPercent { get; set; }
        public decimal AdditionalDurationMs { get; set; }
        public long BaselineExecCount { get; set; }
        public long RecentExecCount { get; set; }
        public int BaselinePlanCount { get; set; }
        public int RecentPlanCount { get; set; }
        public string Severity { get; set; } = string.Empty;
        public string QueryTextSample { get; set; } = string.Empty;
        public DateTime? LastExecutionTime { get; set; }
        public string? QueryPlanXml { get; set; }
    }
}
