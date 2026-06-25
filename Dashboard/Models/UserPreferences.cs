/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Models
{
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum CpuAlertMode
    {
        /// <summary>sql_server_cpu + other_process_cpu — matches OS user+system, "is the box in trouble".</summary>
        Total,
        /// <summary>SQL Server scheduler ProcessUtilization only.</summary>
        SqlOnly
    }

    public class UserPreferences
    {
        // Time display mode: ServerTime, LocalTime, UTC
        public string TimeDisplayMode { get; set; } = "ServerTime";

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
        public CpuAlertMode CpuAlertMode { get; set; } = CpuAlertMode.Total; // Total non-idle CPU (default) or SQL scheduler only
        public bool NotifyOnPoisonWaits { get; set; } = true;
        public int PoisonWaitThresholdMs { get; set; } = 500; // Alert when avg ms per wait > X
        public bool NotifyOnLongRunningQueries { get; set; } = true;
        public int LongRunningQueryThresholdMinutes { get; set; } = 30; // Alert when query runs > X minutes
        public int LongRunningQueryMaxResults { get; set; } = 5; // Max number of long-running queries returned per check
        public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
        public bool LongRunningQueryExcludeWaitFor { get; set; } = true;
        public bool LongRunningQueryExcludeBackups { get; set; } = true;
        public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;
        public bool LongRunningQueryExcludeCdc { get; set; } = true; // Exclude CDC capture jobs (sp_MScdc_capture_job / sp_cdc_scan)
        public bool NotifyOnTempDbSpace { get; set; } = true;
        public int TempDbSpaceThresholdPercent { get; set; } = 80; // Alert when TempDB used > X%
        public bool NotifyOnLowDisk { get; set; } = true;
        public int LowDiskThresholdPercent { get; set; } = 10; // Alert when a volume's free space < X% (0 disables this check)
        public int LowDiskThresholdGb { get; set; } = 5;        // Alert when a volume's free space < X GB (0 disables this check)
        public bool NotifyOnLongRunningJobs { get; set; } = true;
        public int LongRunningJobMultiplier { get; set; } = 3; // Alert when job runs > Nx historical average
        public bool NotifyOnFailedJobs { get; set; } = true; // Alert when a SQL Agent job has recently failed
        public int FailedJobLookbackMinutes { get; set; } = 60; // Look back this many minutes for failed Agent job runs
        private int _alertCooldownMinutes = 5;
        public int AlertCooldownMinutes
        {
            get => _alertCooldownMinutes;
            set => _alertCooldownMinutes = Math.Clamp(value, 1, 120);
        }

        private int _emailCooldownMinutes = 15;
        public int EmailCooldownMinutes
        {
            get => _emailCooldownMinutes;
            set => _emailCooldownMinutes = Math.Clamp(value, 1, 120);
        }

        /* #1141: deadlock/blocking notification delivery — Summary (one batched card per cycle, the
           default) or PerEvent (one notification per distinct incident, capped). */
        public AlertNotificationMode AlertDeliveryMode { get; set; } = AlertNotificationMode.Summary;
        private int _alertPerEventMaxPerCycle = 10;
        public int AlertPerEventMaxPerCycle
        {
            get => _alertPerEventMaxPerCycle;
            set => _alertPerEventMaxPerCycle = Math.Clamp(value, 1, 100);
        }

        // SMTP email alert settings
        public bool SmtpEnabled { get; set; } = false;
        public string SmtpServer { get; set; } = "";
        public int SmtpPort { get; set; } = 587;
        public bool SmtpUseSsl { get; set; } = true;
        public string SmtpUsername { get; set; } = "";
        public string SmtpFromAddress { get; set; } = "";
        public string SmtpRecipients { get; set; } = "";

        // Teams webhook settings
        public bool TeamsWebhookEnabled { get; set; } = false;
        public string TeamsWebhookUrl { get; set; } = "";
        public string TeamsProxyAddress { get; set; } = "";

        // Slack webhook settings
        public bool SlackWebhookEnabled { get; set; } = false;
        public string SlackWebhookUrl { get; set; } = "";
        public string SlackProxyAddress { get; set; } = "";

        // MCP server settings
        public bool McpEnabled { get; set; } = false;
        public int McpPort { get; set; } = 5150;

        // Automated analysis production (D0): run the triage engine and persist
        // findings on the independent AnalysisIntervalMinutes cadence. Decoupled from
        // notification *delivery* (AnalysisNotificationsEnabled) — analysis runs and
        // persists regardless of whether findings are delivered. Default ON so the
        // Recommendations surface has data without the user opting in to alerts.
        public bool AnalysisEnabled { get; set; } = true;

        // Automated analysis notifications (Stage 2)
        // Bounds are enforced where these are consumed (the scheduler and the
        // notification service), not here — keeps the prefs surface simple and
        // lets clamps be visible at the consumption sites.
        public bool AnalysisNotificationsEnabled { get; set; } = false;
        public int AnalysisIntervalMinutes { get; set; } = 30;
        public double AnalysisNotifySeverity { get; set; } = 1.5;
        public int AnalysisNotifyCooldownMinutes { get; set; } = 360;
        public int AnalysisTimeoutSeconds { get; set; } = 120;

        // CSV export settings
        public string CsvSeparator { get; set; } = GetDefaultCsvSeparator();

        private static string GetDefaultCsvSeparator()
        {
            return System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator == "," ? ";" : ",";
        }

        // Navigation settings
        public bool FocusServerTabOnClick { get; set; } = true;

        // Color theme ("Dark" or "Light")
        public string ColorTheme { get; set; } = "Dark";

        // Update check settings
        public bool CheckForUpdatesOnStartup { get; set; } = true;

        // Alert database exclusions
        public List<string> AlertExcludedDatabases { get; set; } = new();

        // Default mute rule expiration ("1 hour", "24 hours", "7 days", "Never")
        public string MuteRuleDefaultExpiration { get; set; } = "24 hours";

        // Log alert dismiss/mute actions to file
        public bool LogAlertDismissals { get; set; } = true;

        // Alert suppression (persisted)
        public List<string> SilencedServers { get; set; } = new();
        public List<string> SilencedServerTabs { get; set; } = new();
        public List<string> SilencedSubTabs { get; set; } = new();

        // Acknowledged alert baselines (persisted, keyed by "serverId:tabName")
        public Dictionary<string, AlertBaseline> AcknowledgedBaselines { get; set; } = new();

        /* Failed-Agent-job tray watermark (persisted, keyed by serverId): the newest already-alerted
           failure's server-local run time, stored as DateTime.Ticks. Seeds the in-memory watermark
           on restart so a reopen does not re-fire toasts for failures still inside the lookback window
           that the user already saw and dismissed (the failed-job equivalent of #1145). Ticks keep the
           value basis-exact across JSON round-trips, free of DateTimeKind ambiguity. */
        public Dictionary<string, long> FailedJobAlertWatermarkTicks { get; set; } = new();
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

        /* Snapshot of the disk/failed-job badge conditions at acknowledge time (#754/#749), so the
           Overview badge stays hidden after ack until a NEW such condition appears (false -> true). */
        public bool HasLowDiskAlert { get; set; }
        public bool HasFailedJobAlert { get; set; }
    }
}
