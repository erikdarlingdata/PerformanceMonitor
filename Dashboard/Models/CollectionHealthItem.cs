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
    public class CollectionHealthItem
    {
        public string CollectorName { get; set; } = string.Empty;
        public DateTime? LastSuccessTime { get; set; }
        public int HoursSinceSuccess { get; set; }
        public string HealthStatus { get; set; } = string.Empty;
        public decimal FailureRatePercent { get; set; }
        public long TotalRuns7d { get; set; }
        public long FailedRuns7d { get; set; }
        public long AvgDurationMs { get; set; }
        public long TotalRowsCollected7d { get; set; }
    }
}
