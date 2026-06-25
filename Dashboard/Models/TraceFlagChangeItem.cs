// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class TraceFlagChangeItem
    {
        public DateTime ChangeTime { get; set; }
        public int TraceFlag { get; set; }
        public string PreviousStatus { get; set; } = string.Empty;
        public string NewStatus { get; set; } = string.Empty;
        public string Scope { get; set; } = string.Empty;
        public string ChangeDescription { get; set; } = string.Empty;
        public bool IsGlobal { get; set; }
        public bool IsSession { get; set; }
    }
}
