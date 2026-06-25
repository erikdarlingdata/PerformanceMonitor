// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class DatabaseConfigChangeItem
    {
        public DateTime ChangeTime { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public string SettingType { get; set; } = string.Empty;
        public string SettingName { get; set; } = string.Empty;
        public string OldValue { get; set; } = string.Empty;
        public string NewValue { get; set; } = string.Empty;
        public string ChangeDescription { get; set; } = string.Empty;
    }
}
