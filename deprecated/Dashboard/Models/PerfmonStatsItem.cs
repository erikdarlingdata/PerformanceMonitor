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
        /// <summary>
        /// The interval rate, recomputed by the reader as <c>cntr_value_delta * 1.0 / sample_interval_seconds</c>
        /// (#3653): the table's own computed column is integer division, which truncated every sub-one-per-second
        /// counter to 0. Fractional on purpose.
        /// </summary>
        public double? CntrValuePerSecond { get; set; }
    }
}
