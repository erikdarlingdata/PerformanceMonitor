/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

namespace PerformanceMonitorDashboard.Models
{
    public class PoisonWaitDelta
    {
        public string WaitType { get; set; } = "";
        public long DeltaMs { get; set; }
        public long DeltaTasks { get; set; }
        public double AvgMsPerWait { get; set; }
    }
}
