// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserIOIssueItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public string State { get; set; } = string.Empty;
        public long? IoLatchTimeouts { get; set; }
        public long? IntervalLongIos { get; set; }
        public long? TotalLongIos { get; set; }
        public string LongestPendingRequestsDurationMs { get; set; } = string.Empty;
        public string LongestPendingRequestsFilePath { get; set; } = string.Empty;
    }
}
