using System;

namespace PerformanceMonitorDashboard.Models
{
    public class TempdbStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }

        // File space usage (page counts)
        public long UserObjectReservedPageCount { get; set; }
        public long InternalObjectReservedPageCount { get; set; }
        public long VersionStoreReservedPageCount { get; set; }
        public long MixedExtentPageCount { get; set; }
        public long UnallocatedExtentPageCount { get; set; }

        // Calculated MB values (matching SQL computed columns)
        // Divide by 128 (1024/8) to avoid overflow - pages are 8KB, so pages/128 = MB
        public long UserObjectReservedMb => UserObjectReservedPageCount / 128;
        public long InternalObjectReservedMb => InternalObjectReservedPageCount / 128;
        public long VersionStoreReservedMb => VersionStoreReservedPageCount / 128;
        public long TotalReservedMb => (UserObjectReservedPageCount + InternalObjectReservedPageCount + VersionStoreReservedPageCount) / 128;
        public long UnallocatedMb => UnallocatedExtentPageCount / 128;

        // Top task consumer
        public int? TopTaskUserObjectsMb { get; set; }
        public int? TopTaskInternalObjectsMb { get; set; }
        public int? TopTaskTotalMb { get; set; }
        public int? TopTaskSessionId { get; set; }
        public int? TopTaskRequestId { get; set; }

        // Session counts
        public int TotalSessionsUsingTempdb { get; set; }
        public int SessionsWithUserObjects { get; set; }
        public int SessionsWithInternalObjects { get; set; }

        // Warning flags
        public bool VersionStoreHighWarning { get; set; }
        public bool AllocationContentionWarning { get; set; }

        // Analysis columns (from report.tempdb_pressure view logic)
        public decimal? VersionStorePercent { get; set; }
        public string PressureLevel { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
    }
}
