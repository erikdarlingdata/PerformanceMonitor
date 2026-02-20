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
    public class CurrentServerConfigItem
    {
        public string ConfigurationName { get; set; } = string.Empty;
        public string ValueConfigured { get; set; } = string.Empty;
        public string ValueInUse { get; set; } = string.Empty;
        public string ValueMinimum { get; set; } = string.Empty;
        public string ValueMaximum { get; set; } = string.Empty;
        public bool IsDynamic { get; set; }
        public bool IsAdvanced { get; set; }
        public string Description { get; set; } = string.Empty;
        public DateTime LastChanged { get; set; }
    }

    public class CurrentDatabaseConfigItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public string SettingType { get; set; } = string.Empty;
        public string SettingName { get; set; } = string.Empty;
        public string SettingValue { get; set; } = string.Empty;
        public DateTime LastChanged { get; set; }
    }

    public class CurrentTraceFlagItem
    {
        public int TraceFlag { get; set; }
        public bool Status { get; set; }
        public bool IsGlobal { get; set; }
        public bool IsSession { get; set; }
        public DateTime LastChanged { get; set; }
    }
}
