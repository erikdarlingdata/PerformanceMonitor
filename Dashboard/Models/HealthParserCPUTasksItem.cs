// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserCPUTasksItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public string State { get; set; } = string.Empty;
        public long? MaxWorkers { get; set; }
        public long? WorkersCreated { get; set; }
        public long? WorkersIdle { get; set; }
        public long? TasksCompletedWithinInterval { get; set; }
        public long? PendingTasks { get; set; }
        public long? OldestPendingTaskWaitingTime { get; set; }
        public bool? HasUnresolvableDeadlockOccurred { get; set; }
        public bool? HasDeadlockedSchedulersOccurred { get; set; }
        public bool? DidBlockingOccur { get; set; }
    }
}
