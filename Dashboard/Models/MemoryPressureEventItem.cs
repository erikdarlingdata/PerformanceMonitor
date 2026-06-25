using System;

namespace PerformanceMonitorDashboard.Models
{
    public class MemoryPressureEventItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime SampleTime { get; set; }
        public string MemoryNotification { get; set; } = string.Empty;
        public int MemoryIndicatorsProcess { get; set; }
        public int MemoryIndicatorsSystem { get; set; }
        public string Severity { get; set; } = string.Empty;
    }
}
