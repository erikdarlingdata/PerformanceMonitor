// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class ServerConfigChangeItem
    {
        public DateTime ChangeTime { get; set; }
        public string ConfigurationName { get; set; } = string.Empty;
        public string OldValueConfigured { get; set; } = string.Empty;
        public string NewValueConfigured { get; set; } = string.Empty;
        public string OldValueInUse { get; set; } = string.Empty;
        public string NewValueInUse { get; set; } = string.Empty;
        public bool RequiresRestart { get; set; }
        public string ChangeDescription { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public bool IsDynamic { get; set; }
        public bool IsAdvanced { get; set; }
    }
}
