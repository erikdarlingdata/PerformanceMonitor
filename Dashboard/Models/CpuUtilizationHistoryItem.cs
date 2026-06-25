/*
 * SQL Server Performance Monitor Dashboard
 *
 * Model for CPU utilization history data points
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class CpuUtilizationHistoryItem
    {
        public DateTime CollectionTime { get; set; }
        public DateTime SampleTime { get; set; }
        public int SqlServerCpuUtilization { get; set; }
        public int OtherProcessCpuUtilization { get; set; }
        public int TotalCpuUtilization { get; set; }
    }
}
