/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// Free space for a single distinct volume (mount point) on a monitored server,
    /// used by the low-disk alert. Azure SQL DB has no volume stats, so no rows are
    /// produced there and the alert never fires.
    /// </summary>
    public class VolumeFreeSpaceInfo
    {
        public string MountPoint { get; set; } = "";
        public double TotalMb { get; set; }
        public double FreeMb { get; set; }

        public double FreePercent => TotalMb > 0 ? FreeMb / TotalMb * 100 : 0;
        public double FreeGb => FreeMb / 1024.0;
    }
}
