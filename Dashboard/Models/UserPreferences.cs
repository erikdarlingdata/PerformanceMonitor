/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitorDashboard.Models
{
    public class UserPreferences
    {
        // Default date range preferences (hours back)
        public int DefaultHoursBack { get; set; } = 24;

        // Per-tab date range preferences
        public int CollectionHealthHoursBack { get; set; } = 24;
        public int WaitStatsHoursBack { get; set; } = 24;
        public int CpuHoursBack { get; set; } = 24;
        public int MemoryHoursBack { get; set; } = 24;
        public int FileIoHoursBack { get; set; } = 24;
        public int ExpensiveQueriesHoursBack { get; set; } = 24;
        public int BlockingHoursBack { get; set; } = 24;

        // Whether to use custom dates (if true, ignore HoursBack)
        public bool CollectionHealthUseCustomDates { get; set; } = false;
        public bool WaitStatsUseCustomDates { get; set; } = false;
        public bool CpuUseCustomDates { get; set; } = false;
        public bool MemoryUseCustomDates { get; set; } = false;
        public bool FileIoUseCustomDates { get; set; } = false;
        public bool ExpensiveQueriesUseCustomDates { get; set; } = false;
        public bool BlockingUseCustomDates { get; set; } = false;

        // Custom date ranges (stored as ISO strings for JSON serialization)
        public string? CollectionHealthFromDate { get; set; }
        public string? CollectionHealthToDate { get; set; }
        public string? WaitStatsFromDate { get; set; }
        public string? WaitStatsToDate { get; set; }
        public string? CpuFromDate { get; set; }
        public string? CpuToDate { get; set; }
        public string? MemoryFromDate { get; set; }
        public string? MemoryToDate { get; set; }
        public string? FileIoFromDate { get; set; }
        public string? FileIoToDate { get; set; }
        public string? ExpensiveQueriesFromDate { get; set; }
        public string? ExpensiveQueriesToDate { get; set; }
        public string? BlockingFromDate { get; set; }
        public string? BlockingToDate { get; set; }

        // Auto-refresh settings (for dashboard tabs)
        public bool AutoRefreshEnabled { get; set; } = false;
        public int AutoRefreshIntervalSeconds { get; set; } = 60; // Default 1 minute

        // NOC landing page refresh settings
        public int NocRefreshIntervalSeconds { get; set; } = 30; // Default 30 seconds

        // Query logging settings
        public bool LogSlowQueries { get; set; } = true;
        public double SlowQueryThresholdSeconds { get; set; } = 2.0;

        // Method profiler settings
        public bool LogSlowMethods { get; set; } = true;

        // UI layout settings
        public bool SidebarCollapsed { get; set; } = false;

        // System tray and notification settings
        public bool MinimizeToTray { get; set; } = true;
        public bool NotificationsEnabled { get; set; } = true;
        public bool NotifyOnConnectionLost { get; set; } = true;
        public bool NotifyOnConnectionRestored { get; set; } = true;

        // Alert notification settings
        public bool NotifyOnBlocking { get; set; } = true;
        public int BlockingThresholdSeconds { get; set; } = 30; // Alert when blocking > X seconds
        public bool NotifyOnDeadlock { get; set; } = true;
        public int DeadlockThreshold { get; set; } = 1; // Alert when deadlocks >= X since last check
        public bool NotifyOnHighCpu { get; set; } = true;
        public int CpuThresholdPercent { get; set; } = 90; // Alert when CPU > X%
        public bool NotifyOnPoisonWaits { get; set; } = true;
        public int PoisonWaitThresholdMs { get; set; } = 500; // Alert when avg ms per wait > X
        public bool NotifyOnLongRunningQueries { get; set; } = true;
        public int LongRunningQueryThresholdMinutes { get; set; } = 30; // Alert when query runs > X minutes
        public bool NotifyOnTempDbSpace { get; set; } = true;
        public int TempDbSpaceThresholdPercent { get; set; } = 80; // Alert when TempDB used > X%
        public bool NotifyOnLongRunningJobs { get; set; } = true;
        public int LongRunningJobMultiplier { get; set; } = 3; // Alert when job runs > Nx historical average

        // SMTP email alert settings
        public bool SmtpEnabled { get; set; } = false;
        public string SmtpServer { get; set; } = "";
        public int SmtpPort { get; set; } = 587;
        public bool SmtpUseSsl { get; set; } = true;
        public string SmtpUsername { get; set; } = "";
        public string SmtpFromAddress { get; set; } = "";
        public string SmtpRecipients { get; set; } = "";

        // MCP server settings
        public bool McpEnabled { get; set; } = false;
        public int McpPort { get; set; } = 5150;

        // Navigation settings
        public bool FocusServerTabOnClick { get; set; } = true;

        // Update check settings
        public bool CheckForUpdatesOnStartup { get; set; } = true;

        // Alert suppression (persisted)
        public List<string> SilencedServers { get; set; } = new();
        public List<string> SilencedServerTabs { get; set; } = new();
        public List<string> SilencedSubTabs { get; set; } = new();

        // Acknowledged alert baselines (persisted, keyed by "serverId:tabName")
        public Dictionary<string, AlertBaseline> AcknowledgedBaselines { get; set; } = new();
    }

    /// <summary>
    /// Metric snapshot captured when user acknowledges an alert.
    /// Badge stays hidden unless conditions worsen beyond these values.
    /// Auto-cleared when the alert condition fully resolves.
    /// </summary>
    public class AlertBaseline
    {
        public decimal LongestBlockedSeconds { get; set; }
        public long DeadlocksSinceLastCheck { get; set; }
        public int RequestsWaitingForMemory { get; set; }
        public int? TotalCpuPercent { get; set; }
    }
}
