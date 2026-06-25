/*
 * SQL Server Performance Monitor Dashboard
 *
 * Model for memory utilization history data points
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class MemoryHistoryItem
    {
        public DateTime CollectionTime { get; set; }
        public decimal BufferPoolMb { get; set; }
        public decimal PlanCacheMb { get; set; }
        public decimal OtherMemoryMb { get; set; }
        public decimal TotalMemoryMb { get; set; }
        public decimal PhysicalMemoryInUseMb { get; set; }
        public decimal AvailablePhysicalMemoryMb { get; set; }
        public int MemoryUtilizationPercentage { get; set; }
    }
}
