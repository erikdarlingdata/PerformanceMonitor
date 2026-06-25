using System;

namespace PerformanceMonitorDashboard.Models
{
    public class CpuUtilizationItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime SampleTime { get; set; }
        public int SqlServerCpuUtilization { get; set; }
        public int OtherProcessCpuUtilization { get; set; }
        public int TotalCpuUtilization { get; set; }
    }
}
