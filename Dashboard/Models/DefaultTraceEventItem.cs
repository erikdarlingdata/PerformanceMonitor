using System;

namespace PerformanceMonitorDashboard.Models
{
    public class DefaultTraceEventItem
    {
        public long EventId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime EventTime { get; set; }
        public string EventName { get; set; } = string.Empty;
        public int EventClass { get; set; }
        public int? Spid { get; set; }
        public string? DatabaseName { get; set; }
        public int? DatabaseId { get; set; }
        public string? LoginName { get; set; }
        public string? HostName { get; set; }
        public string? ApplicationName { get; set; }
        public string? ServerName { get; set; }
        public string? ObjectName { get; set; }
        public string? Filename { get; set; }
        public long? IntegerData { get; set; }
        public long? IntegerData2 { get; set; }
        public string? TextData { get; set; }
        public string? SessionLoginName { get; set; }
        public int? ErrorNumber { get; set; }
        public int? Severity { get; set; }
        public int? State { get; set; }
        public long? EventSequence { get; set; }
        public bool? IsSystem { get; set; }
        public int? RequestId { get; set; }
        public long? DurationUs { get; set; }
        public DateTime? EndTime { get; set; }

        // Display helpers
        public decimal? DurationMs => DurationUs.HasValue ? DurationUs.Value / 1000.0m : null;
        public decimal? GrowthMb => IntegerData.HasValue ? IntegerData.Value * 8.0m / 1024.0m : null;
    }
}
