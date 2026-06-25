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
    /// <summary>
    /// Represents LCK (lock) wait stats data for a collection time point.
    /// </summary>
    public class LockWaitStatsItem
    {
        public DateTime CollectionTime { get; set; }
        public string WaitType { get; set; } = string.Empty;
        public decimal WaitTimeMsPerSecond { get; set; }
    }
}
