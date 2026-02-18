/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class AnomalousJobInfo
    {
        public string JobName { get; set; } = "";
        public string JobId { get; set; } = "";
        public long CurrentDurationSeconds { get; set; }
        public long AvgDurationSeconds { get; set; }
        public long P95DurationSeconds { get; set; }
        public decimal? PercentOfAverage { get; set; }
        public DateTime StartTime { get; set; }
    }
}
