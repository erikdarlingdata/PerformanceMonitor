// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class CriticalIssueItem
    {
        public long IssueId { get; set; }
        public DateTime LogDate { get; set; }
        public string Severity { get; set; } = string.Empty;
        public string ProblemArea { get; set; } = string.Empty;
        public string SourceCollector { get; set; } = string.Empty;
        public string AffectedDatabase { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public string InvestigateQuery { get; set; } = string.Empty;
        public decimal? ThresholdValue { get; set; }
        public decimal? ThresholdLimit { get; set; }
    }
}
