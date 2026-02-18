/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// Lightweight result from alert-only health queries.
    /// Contains only the metrics needed for alert evaluation (CPU, blocking, deadlocks, poison waits).
    /// Used by MainWindow's independent alert timer to avoid running all 9 NOC queries.
    /// </summary>
    public class AlertHealthResult
    {
        public int? CpuPercent { get; set; }
        public int? OtherCpuPercent { get; set; }
        public long TotalBlocked { get; set; }
        public decimal LongestBlockedSeconds { get; set; }
        public long DeadlockCount { get; set; }
        public List<PoisonWaitDelta> PoisonWaits { get; set; } = new();
        public List<LongRunningQueryInfo> LongRunningQueries { get; set; } = new();
        public TempDbSpaceInfo? TempDbSpace { get; set; }
        public List<AnomalousJobInfo> AnomalousJobs { get; set; } = new();
        public bool IsOnline { get; set; } = true;

        /// <summary>
        /// Total CPU = SQL + Other.
        /// </summary>
        public int? TotalCpuPercent
        {
            get
            {
                if (!CpuPercent.HasValue && !OtherCpuPercent.HasValue) return null;
                return (CpuPercent ?? 0) + (OtherCpuPercent ?? 0);
            }
        }
    }
}
