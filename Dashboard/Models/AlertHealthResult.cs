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

        /// <summary>
        /// Deadlock count for the alert window filtered by excluded databases.
        /// Sourced from collect.blocking_deadlock_stats when excluded databases are configured.
        /// When set, EvaluateAlertConditionsAsync uses this instead of the raw delta
        /// from the server-wide performance counter, matching how blocking alerts filter.
        /// Null when no databases are excluded (fall back to raw delta).
        /// </summary>
        public long? FilteredDeadlockCount { get; set; }
        public List<PoisonWaitDelta> PoisonWaits { get; set; } = new();
        public List<LongRunningQueryInfo> LongRunningQueries { get; set; } = new();
        public TempDbSpaceInfo? TempDbSpace { get; set; }

        /// <summary>
        /// Free space per distinct volume on the server, ordered worst (lowest free %) first.
        /// Empty on Azure SQL DB (no volume stats collected). Used by the low-disk alert.
        /// </summary>
        public List<VolumeFreeSpaceInfo> Volumes { get; set; } = new();
        public List<AnomalousJobInfo> AnomalousJobs { get; set; } = new();

        /// <summary>
        /// SQL Agent job runs that failed within the failed-job lookback window. Live
        /// msdb query — empty on Azure SQL DB (no Agent) or when the login lacks msdb /
        /// SQLAgentReaderRole access.
        /// </summary>
        public List<FailedJobInfo> RecentlyFailedJobs { get; set; } = new();
        public bool IsOnline { get; set; } = true;

        /// <summary>
        /// Capture types ("Blocking", "Deadlock") whose XE session is missing —
        /// the collector's latest collection_log status is SESSION_MISSING (#1086).
        /// Empty when both sessions are healthy.
        /// </summary>
        public List<string> MissingCaptureSessions { get; set; } = new();

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
