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
    public class CpuPressureItem
    {
        public DateTime CollectionTime { get; set; }
        public int TotalSchedulers { get; set; }
        public int TotalRunnableTasks { get; set; }
        public decimal AvgRunnableTasksPerScheduler { get; set; }
        public int TotalWorkers { get; set; }
        public int MaxWorkers { get; set; }
        public decimal WorkerUtilizationPercent { get; set; }
        public decimal RunnablePercent { get; set; }
        public int TotalQueuedRequests { get; set; }
        public int TotalActiveRequests { get; set; }
        public string PressureLevel { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
        public bool WorkerThreadExhaustionWarning { get; set; }
        public bool RunnableTasksWarning { get; set; }
        public bool BlockedTasksWarning { get; set; }
        public bool QueuedRequestsWarning { get; set; }
        public bool PhysicalMemoryPressureWarning { get; set; }
    }
}
