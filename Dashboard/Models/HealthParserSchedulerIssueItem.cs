// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserSchedulerIssueItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public int? SchedulerId { get; set; }
        public int? CpuId { get; set; }
        public string Status { get; set; } = string.Empty;
        public bool? IsOnline { get; set; }
        public bool? IsRunnable { get; set; }
        public bool? IsRunning { get; set; }
        public string NonYieldingTimeMs { get; set; } = string.Empty;
        public string ThreadQuantumMs { get; set; } = string.Empty;
    }
}
