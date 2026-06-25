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
    public class WaitStatItem
    {
        public string WaitType { get; set; } = string.Empty;
        public long WaitTimeMs { get; set; }
        public decimal WaitTimeSec { get; set; }
        public long WaitingTasks { get; set; }
        public long SignalWaitMs { get; set; }
        public long ResourceWaitMs { get; set; }
        public decimal AvgWaitMsPerTask { get; set; }
        public DateTime LastSeen { get; set; }
    }
}
