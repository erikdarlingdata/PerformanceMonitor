// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class TracePatternDetailItem
    {
        public long AnalysisId { get; set; }
        public DateTime CollectionTime { get; set; }
        public string EventName { get; set; } = string.Empty;
        public string? DatabaseName { get; set; }
        public string? LoginName { get; set; }
        public string? ApplicationName { get; set; }
        public string? HostName { get; set; }
        public int? Spid { get; set; }
        public long? DurationMs { get; set; }
        public long? CpuMs { get; set; }
        public long? Reads { get; set; }
        public long? Writes { get; set; }
        public long? RowCounts { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public string? SqlText { get; set; }
        public long? ObjectId { get; set; }
    }
}
