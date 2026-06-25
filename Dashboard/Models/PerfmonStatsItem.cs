using System;

namespace PerformanceMonitorDashboard.Models
{
    public class PerfmonStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime ServerStartTime { get; set; }
        public string ObjectName { get; set; } = string.Empty;
        public string CounterName { get; set; } = string.Empty;
        public string InstanceName { get; set; } = string.Empty;
        public long CntrValue { get; set; }
        public long CntrType { get; set; }
        public long? CntrValueDelta { get; set; }
        public int? SampleIntervalSeconds { get; set; }
        public long? CntrValuePerSecond { get; set; }
    }
}
