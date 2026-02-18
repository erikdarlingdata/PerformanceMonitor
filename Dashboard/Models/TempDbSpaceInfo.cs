/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

namespace PerformanceMonitorDashboard.Models
{
    public class TempDbSpaceInfo
    {
        public double TotalReservedMb { get; set; }
        public double UnallocatedMb { get; set; }
        public double UserObjectReservedMb { get; set; }
        public double InternalObjectReservedMb { get; set; }
        public double VersionStoreReservedMb { get; set; }
        public int TopConsumerSessionId { get; set; }
        public double TopConsumerMb { get; set; }

        public double UsedPercent => TotalReservedMb + UnallocatedMb > 0
            ? TotalReservedMb / (TotalReservedMb + UnallocatedMb) * 100
            : 0;
    }
}
