/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class RunningJobItem
    {
        public string JobName { get; set; } = string.Empty;
        public Guid JobId { get; set; }
        public bool JobEnabled { get; set; }
        public DateTime StartTime { get; set; }
        public long CurrentDurationSeconds { get; set; }
        public string CurrentDurationFormatted { get; set; } = string.Empty;
        public long AvgDurationSeconds { get; set; }
        public string AvgDurationFormatted { get; set; } = string.Empty;
        public long P95DurationSeconds { get; set; }
        public string P95DurationFormatted =>
            P95DurationSeconds < 60 ? $"{P95DurationSeconds}s" :
            P95DurationSeconds < 3600 ? $"{P95DurationSeconds / 60}m {P95DurationSeconds % 60}s" :
            $"{P95DurationSeconds / 3600}h {(P95DurationSeconds % 3600) / 60}m";
        public long SuccessfulRunCount { get; set; }
        public bool IsRunningLong { get; set; }
        public decimal? PercentOfAverage { get; set; }
    }
}
