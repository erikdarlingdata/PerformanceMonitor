/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Notifications;
using System.Windows.Threading;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite;

public enum CpuAlertMode
{
    /// <summary>sql_server_cpu + other_process_cpu — matches OS user+system, "is the box in trouble".</summary>
    Total,
    /// <summary>SQL Server scheduler ProcessUtilization only.</summary>
    SqlOnly
}

public partial class App : Application
{
    [DllImport("shell32.dll", SetLastError = true)]
    private static extern void SetCurrentProcessExplicitAppUserModelID([MarshalAs(UnmanagedType.LPWStr)] string appId);

    private const string MutexName = "PerformanceMonitorLite_SingleInstance";
    /* Version-aware single-instance + upgrade handoff (plans/single-instance-upgrade-handoff.md):
       a newer build launched over an older tray-resident one closes it and takes over instead of
       being handed back the stale in-memory version. The coordinator owns the mutex + the exit
       listener for the life of the owning process. */
    private const string ExitForUpgradeEventName = "PerformanceMonitorLite_ExitForUpgrade";
    private SingleInstanceCoordinator? _instanceCoordinator;

    /* Single-instance "surface the window" channel (#769, #1050). A second launch signals this named
       event and exits; the owning instance restores its window through WPF's own Show() path
       (MainWindow.RestoreFromTray). The old approach poked the HWND with raw Win32 ShowWindow, which
       leaves a tray-hidden (WPF Visibility.Hidden) window visible but blank — the root cause of the
       "blank window after relaunch" reports. */
    private const string ShowWindowEventName = "PerformanceMonitorLite_ShowWindow";
    private SingleInstanceSignal? _instanceSignal;
    private MainWindow? _mainWindow;

    /// <summary>
    /// Gets the application data directory where config and data files are stored.
    /// </summary>
    public static string DataDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the path to the DuckDB database file.
    /// </summary>
    public static string DatabasePath { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the per-user config directory path. Holds settings.json, schedules, and
    /// other per-user preferences. Stays in %LOCALAPPDATA% so Velopack updates can
    /// replace the app directory without losing data.
    /// </summary>
    public static string ConfigDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the machine-wide config directory under %ProgramData% used for files that
    /// should be shared across Windows users on the same machine — currently just
    /// servers.json (the list of monitored servers). Credentials remain per-user in
    /// Windows Credential Manager.
    /// </summary>
    public static string SharedConfigDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the archive directory path for Parquet files.
    /// </summary>
    public static string ArchiveDirectory { get; private set; } = string.Empty;

    /// <summary>
    /// Gets the default time range in hours for new server tabs.
    /// </summary>
    public static int DefaultTimeRangeHours { get; set; } = 4;

    /* Alert settings */
    public static bool AlertsEnabled { get; set; } = true;
    public static bool NotifyConnectionChanges { get; set; } = true;
    public static bool AlertCpuEnabled { get; set; } = true;
    public static int AlertCpuThreshold { get; set; } = 80;
    /// <summary>Which CPU metric the alert evaluates against. Total = sql_server_cpu + other_process_cpu (matches OS user+system). SqlOnly = SQL Server scheduler %.</summary>
    public static CpuAlertMode AlertCpuMode { get; set; } = CpuAlertMode.Total;
    public static bool AlertBlockingEnabled { get; set; } = true;
    public static int AlertBlockingThreshold { get; set; } = 1;
    public static bool AlertDeadlockEnabled { get; set; } = true;
    public static int AlertDeadlockThreshold { get; set; } = 1;
    public static bool AlertPoisonWaitEnabled { get; set; } = true;
    public static int AlertPoisonWaitThresholdMs { get; set; } = 500;
    public static bool AlertLongRunningQueryEnabled { get; set; } = true;
    public static int AlertLongRunningQueryThresholdMinutes { get; set; } = 30;
    public static int AlertLongRunningQueryMaxResults { get; set; } = 5;
    public static bool AlertLongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
    public static bool AlertLongRunningQueryExcludeWaitFor { get; set; } = true;
    public static bool AlertLongRunningQueryExcludeBackups { get; set; } = true;
    public static bool AlertLongRunningQueryExcludeMiscWaits { get; set; } = true;
    public static bool AlertLongRunningQueryExcludeCdc { get; set; } = true;
    public static List<string> AlertExcludedDatabases { get; set; } = new();
    public static bool AlertTempDbSpaceEnabled { get; set; } = true;
    public static int AlertTempDbSpaceThresholdPercent { get; set; } = 80;
    public static bool AlertLowDiskEnabled { get; set; } = true;
    public static int AlertLowDiskThresholdPercent { get; set; } = 10; // Alert when a volume's free space < X% (0 disables this check)
    public static int AlertLowDiskThresholdGb { get; set; } = 5;        // Alert when a volume's free space < X GB (0 disables this check)
    public static bool AlertLongRunningJobEnabled { get; set; } = true;
    public static int AlertLongRunningJobMultiplier { get; set; } = 3;
    public static bool AlertFailedJobEnabled { get; set; } = true;
    public static int AlertFailedJobLookbackMinutes { get; set; } = 60;  // Look back this many minutes for failed Agent job runs
    public static int AlertCooldownMinutes { get; set; } = 5;  // Tray notification cooldown between repeated alerts
    public static int EmailCooldownMinutes { get; set; } = 15; // Email cooldown between repeated alerts
    /* #1141: deadlock/blocking notification delivery — Summary (one batched card per cycle, the default)
       or PerEvent (one notification per distinct incident, capped, for per-incident ticketing). */
    public static AlertNotificationMode AlertDeliveryMode { get; set; } = AlertNotificationMode.Summary;
    public static int AlertPerEventMaxPerCycle { get; set; } = 10; // Max per-event notifications per cycle before "+N more"
    public static string MuteRuleDefaultExpiration { get; set; } = "24 hours"; // Default expiration for new mute rules
    public static bool LogAlertDismissals { get; set; } = true; // Log alert dismiss/mute actions to file

    /* Automated analysis production (D0): run the triage engine and persist findings on
       the independent AnalysisIntervalMinutes cadence. Decoupled from notification delivery
       (AnalysisNotificationsEnabled). Default ON so the recommendations data exists. */
    public static bool AnalysisEnabled { get; set; } = true;

    /* Automated analysis notifications (scheduled triage) */
    public static bool AnalysisNotificationsEnabled { get; set; } = false;  // Delivery gate — analysis runs regardless
    public static int AnalysisIntervalMinutes { get; set; } = 30;           // How often scheduled analysis runs
    public static double AnalysisNotifySeverity { get; set; } = 1.5;        // Minimum finding severity (0.0-2.0) to notify on
    public static int AnalysisNotifyCooldownMinutes { get; set; } = 360;    // Re-notify gap per finding (keyed by StoryPathHash)
    public static int AnalysisTimeoutSeconds { get; set; } = 120;           // Per-server analysis timeout

    /* Connection settings */
    public static int ConnectionTimeoutSeconds { get; set; } = 5;

    /* CSV export settings */
    public static string CsvSeparator { get; set; } = GetDefaultCsvSeparator();

    private static string GetDefaultCsvSeparator()
    {
        /* Auto-detect: use semicolon when the locale's decimal separator is a comma (Italian, German, French, etc.) */
        return System.Globalization.CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator == "," ? ";" : ",";
    }

    /* System tray settings */
    public static bool MinimizeToTray { get; set; } = true;

    /* Time display mode ("ServerTime", "LocalTime", "UTC") */
    public static string TimeDisplayMode { get; set; } = "ServerTime";

    /* Color theme ("Dark" or "Light") */
    public static string ColorTheme { get; set; } = "Dark";

    /* Update check settings */
    public static bool CheckForUpdatesOnStartup { get; set; } = true;

    /* Teams webhook settings */
    public static bool TeamsWebhookEnabled { get; set; } = false;
    public static string TeamsWebhookUrl { get; set; } = "";
    public static string TeamsProxyAddress { get; set; } = "";

    /* Slack webhook settings */
    public static bool SlackWebhookEnabled { get; set; } = false;
    public static string SlackWebhookUrl { get; set; } = "";
    public static string SlackProxyAddress { get; set; } = "";

    private const string TeamsWebhookCredentialKey = "TeamsWebhook";
    private const string SlackWebhookCredentialKey = "SlackWebhook";

    /// <summary>
    /// Gets a webhook URL from Windows Credential Manager.
    /// </summary>
    public static string GetWebhookUrl(string credentialKey)
    {
        try
        {
            var credService = new Services.CredentialService();
            var cred = credService.GetCredential(credentialKey);
            return cred?.Password ?? "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("App", $"Failed to retrieve webhook URL for {credentialKey}: {ex.Message}");
            return "";
        }
    }

    /// <summary>
    /// Saves a webhook URL to Windows Credential Manager.
    /// </summary>
    public static void SaveWebhookUrl(string credentialKey, string url)
    {
        try
        {
            var credService = new Services.CredentialService();
            if (string.IsNullOrWhiteSpace(url))
            {
                credService.DeleteCredential(credentialKey);
            }
            else
            {
                credService.SaveCredential(credentialKey, "webhook", url);
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("App", $"Failed to save webhook URL for {credentialKey}: {ex.Message}");
        }
    }

    /* SMTP email alert settings */
    public static bool SmtpEnabled { get; set; } = false;
    public static string SmtpServer { get; set; } = "";
    public static int SmtpPort { get; set; } = 587;
    public static bool SmtpUseSsl { get; set; } = true;
    public static string SmtpUsername { get; set; } = "";
    public static string SmtpFromAddress { get; set; } = "";
    public static string SmtpRecipients { get; set; } = "";

    private const string SmtpCredentialKey = "SMTP";

    /// <summary>
    /// Gets the SMTP password from Windows Credential Manager.
    /// </summary>
    public static string? GetSmtpPassword()
    {
        try
        {
            var credService = new Services.CredentialService();
            var cred = credService.GetCredential(SmtpCredentialKey);
            return cred?.Password;
        }
        catch (Exception ex)
        {
            AppLogger.Error("App", $"Failed to retrieve SMTP password: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Saves the SMTP password to Windows Credential Manager.
    /// </summary>
    public static void SaveSmtpPassword(string password)
    {
        try
        {
            var credService = new Services.CredentialService();
            credService.SaveCredential(SmtpCredentialKey, string.IsNullOrEmpty(SmtpUsername) ? "smtp" : SmtpUsername, password);
        }
        catch (Exception ex)
        {
            AppLogger.Error("App", $"Failed to save SMTP password: {ex.Message}");
        }
    }

    protected override void OnStartup(StartupEventArgs e)
    {
        SetCurrentProcessExplicitAppUserModelID("DarlingData.PerformanceMonitor.Lite");

        /* Single-instance with upgrade handoff. Runs synchronously, at the top of OnStartup before
           base.OnStartup and any window/data init, so we only open the shared DuckDB / bind the MCP
           port after any older instance has released them. A newer build closes an older tray-resident
           one and takes over; a same/newer one just surfaces the existing instance (today's behavior);
           an older-but-elevated one raises an actionable error. */
        _instanceCoordinator = new SingleInstanceCoordinator(new SingleInstanceOptions
        {
            MutexName = MutexName,
            ProcessName = "PerformanceMonitorLite",
            ExitEventName = ExitForUpgradeEventName,
            SurfaceRunningInstance = () => SingleInstanceSignal.TrySignal(ShowWindowEventName),
            GracefulSelfExit = () => Dispatcher.BeginInvoke(new Action(Shutdown)),
            Prompts = new MessageBoxHandoffPrompts("Performance Monitor Lite"),
            AutoConfirm = Array.Exists(e.Args, a => string.Equals(a, HandoffArgs.AutoConfirm, StringComparison.OrdinalIgnoreCase)),
            Log = msg => { try { AppLogger.Info("SingleInstance", msg); } catch { /* logger not yet initialized */ } },
        });

        if (!_instanceCoordinator.TryBecomeOwner())
        {
            Shutdown();
            return;
        }

        /* Own the "surface me" channel before anything slow runs, so a fast second launch finds it.
           The callback null-checks _mainWindow, so a signal that lands before the window exists is a
           harmless no-op (the window is about to show regardless). */
        _instanceSignal = new SingleInstanceSignal(ShowWindowEventName, OnSurfaceWindowRequested);

        base.OnStartup(e);

        // Right-click selects the DataGrid row under the cursor app-wide, so context-menu actions
        // (e.g. View Plan) act on the clicked row even after an auto-refresh cleared the selection.
        PerformanceMonitor.Ui.DataGridRowSelectionBehavior.Enable();

        // #1050: WPF's GPU render thread can zombie its surface across sleep/wake or RDP, leaving a
        // live-but-blank window. Software rendering removes the GPU dependency entirely. Charts are
        // unaffected — ScottPlot renders via SkiaSharp (CPU) into a bitmap, not WPF's GPU path.
        System.Windows.Media.RenderOptions.ProcessRenderMode =
            System.Windows.Interop.RenderMode.SoftwareOnly;

        // Initialize paths — store data in %LOCALAPPDATA% so Velopack updates
        // can replace the app directory without losing data
        var appDataRoot = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "PerformanceMonitorLite");
        DataDirectory = appDataRoot;
        ConfigDirectory = Path.Combine(appDataRoot, "config");
        DatabasePath = Path.Combine(appDataRoot, "monitor.duckdb");
        ArchiveDirectory = Path.Combine(appDataRoot, "archive");

        // Ensure directories exist
        Directory.CreateDirectory(ConfigDirectory);
        Directory.CreateDirectory(Path.Combine(appDataRoot, "archive"));

        // Load settings
        LoadDefaultTimeRange();
        LoadAlertSettings();

        // Wire the shared-UI time conversion hook before any chart/crosshair can
        // render. The lambda reads CurrentDisplayMode at call time, so later
        // display-mode switches are honored. Must precede the first window/chart.
        PerformanceMonitor.Ui.UiTimeContext.ConvertForDisplay =
            t => Services.ServerTimeHelper.ConvertForDisplay(t, Services.ServerTimeHelper.CurrentDisplayMode);

        // Apply saved color theme before the main window is shown
        ThemeManager.Apply(ColorTheme);

        // Initialize logging
        var logDirectory = Path.Combine(appDataRoot, "logs");
        AppLogger.Initialize(logDirectory);

        // Resolve shared (machine-wide) config directory AFTER logger init so migration/ACL events are logged
        SharedConfigDirectory = ResolveSharedConfigDirectory(ConfigDirectory);
        Helpers.MethodProfiler.Initialize(logDirectory);
        Helpers.QueryLogger.Initialize(logDirectory);
        AppLogger.Info("App", $"Starting PerformanceMonitorLite v{System.Reflection.Assembly.GetExecutingAssembly().GetName().Version}");
        AppLogger.Info("App", $"Data directory: {DataDirectory}");

        // Register global exception handlers
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        DispatcherUnhandledException += OnDispatcherUnhandledException;
        TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;

        // Create and show main window (StartupUri removed for Velopack custom Main)
        _mainWindow = new MainWindow();
        _mainWindow.Show();
    }

    /// <summary>
    /// Invoked on <see cref="SingleInstanceSignal"/>'s background thread when a second launch asks us
    /// to surface the window. Marshals to the UI thread and restores via WPF's Show() path (#1050).
    /// </summary>
    private void OnSurfaceWindowRequested()
    {
        Dispatcher.BeginInvoke(new Action(() => _mainWindow?.RestoreFromTray()));
    }

    /// <summary>
    /// Opens the upgrade-handoff "exit" channel once startup is past its risky init (DuckDB ready).
    /// Called by <see cref="MainWindow"/> after initialization so a newer build won't signal/kill us
    /// mid-init (#single-instance-upgrade-handoff). Safe to call more than once.
    /// </summary>
    public void EnableUpgradeHandoff() => _instanceCoordinator?.EnableUpgradeHandoff();

    protected override void OnExit(ExitEventArgs e)
    {
        AppLogger.Info("App", "Shutting down");

        _instanceSignal?.Dispose();

        AppLogger.Shutdown();

        /* Releases the mutex + disposes the exit-for-upgrade listener. */
        _instanceCoordinator?.Dispose();

        base.OnExit(e);
    }

    /// <summary>
    /// Resolves the machine-wide config directory under %ProgramData% (currently used
    /// only for servers.json) so multiple Windows users on the same machine see the
    /// same server list. On first directory creation, grants Authenticated Users
    /// Modify so any user can edit the file. One-time migrates an existing
    /// per-user servers.json from <paramref name="perUserConfigDirectory"/> if no
    /// shared servers.json exists yet; the old file is left in place as a backup.
    /// Credentials remain per-user in Windows Credential Manager.
    /// </summary>
    private static string ResolveSharedConfigDirectory(string perUserConfigDirectory)
    {
        string sharedDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
            "PerformanceMonitorLite",
            "config");

        bool directoryCreated = !Directory.Exists(sharedDir);
        Directory.CreateDirectory(sharedDir);

        if (directoryCreated)
        {
            TryGrantAuthenticatedUsersModify(sharedDir);
        }

        string sharedServers = Path.Combine(sharedDir, "servers.json");
        if (!File.Exists(sharedServers))
        {
            string legacyServers = Path.Combine(perUserConfigDirectory, "servers.json");
            if (File.Exists(legacyServers))
            {
                try
                {
                    File.Copy(legacyServers, sharedServers);
                    AppLogger.Info("App",
                        $"Migrated servers.json from '{legacyServers}' to '{sharedServers}'. " +
                        "The old file was left in place as a backup. " +
                        "Passwords in Windows Credential Manager remain per-user — other users on this machine will need to re-enter SQL passwords for each server.");
                }
                catch (Exception ex)
                {
                    AppLogger.Warn("App",
                        $"Failed to migrate servers.json from '{legacyServers}': {ex.Message}");
                }
            }
        }

        return sharedDir;
    }

    private static void TryGrantAuthenticatedUsersModify(string directoryPath)
    {
        try
        {
            var dirInfo = new DirectoryInfo(directoryPath);
            var security = dirInfo.GetAccessControl();
            var authenticatedUsers = new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null);

            security.AddAccessRule(new FileSystemAccessRule(
                authenticatedUsers,
                FileSystemRights.Modify | FileSystemRights.Synchronize,
                InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None,
                AccessControlType.Allow));

            dirInfo.SetAccessControl(security);
            AppLogger.Info("App",
                $"Granted Authenticated Users Modify on '{directoryPath}' so other Windows users on this machine can edit the shared server list.");
        }
        catch (Exception ex)
        {
            AppLogger.Warn("App",
                $"Could not set shared ACL on '{directoryPath}': {ex.Message}. Other Windows users may be unable to edit the server list until permissions are fixed manually.");
        }
    }

    private static void LoadDefaultTimeRange()
    {
        try
        {
            var path = Path.Combine(ConfigDirectory, "settings.json");
            if (!File.Exists(path)) return;

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            if (doc.RootElement.TryGetProperty("default_time_range_hours", out var val))
            {
                DefaultTimeRangeHours = val.GetInt32();
            }
        }
        catch { /* Use default */ }
    }

    public static void LoadAlertSettings()
    {
        try
        {
            var path = Path.Combine(ConfigDirectory, "settings.json");
            if (!File.Exists(path)) return;

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            var root = doc.RootElement;

            if (root.TryGetProperty("alerts_enabled", out var v)) AlertsEnabled = v.GetBoolean();
            if (root.TryGetProperty("notify_connection_changes", out v)) NotifyConnectionChanges = v.GetBoolean();
            if (root.TryGetProperty("alert_cpu_enabled", out v)) AlertCpuEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_cpu_threshold", out v)) AlertCpuThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_cpu_mode", out v) && Enum.TryParse<CpuAlertMode>(v.GetString(), out var mode))
                AlertCpuMode = mode;
            if (root.TryGetProperty("alert_blocking_enabled", out v)) AlertBlockingEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_blocking_threshold", out v)) AlertBlockingThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_deadlock_enabled", out v)) AlertDeadlockEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_deadlock_threshold", out v)) AlertDeadlockThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_poison_wait_enabled", out v)) AlertPoisonWaitEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_poison_wait_threshold_ms", out v)) AlertPoisonWaitThresholdMs = v.GetInt32();
            if (root.TryGetProperty("alert_long_running_query_enabled", out v)) AlertLongRunningQueryEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_query_threshold_minutes", out v)) AlertLongRunningQueryThresholdMinutes = v.GetInt32();
            if (root.TryGetProperty("alert_long_running_query_max_results", out v)) AlertLongRunningQueryMaxResults = (int)Math.Clamp(v.GetInt64(), 1, 1000);
            if (root.TryGetProperty("alert_long_running_query_exclude_sp_server_diagnostics", out v)) AlertLongRunningQueryExcludeSpServerDiagnostics = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_query_exclude_waitfor", out v)) AlertLongRunningQueryExcludeWaitFor = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_query_exclude_backups", out v)) AlertLongRunningQueryExcludeBackups = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_query_exclude_misc_waits", out v)) AlertLongRunningQueryExcludeMiscWaits = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_query_exclude_cdc", out v)) AlertLongRunningQueryExcludeCdc = v.GetBoolean();
            if (root.TryGetProperty("alert_excluded_databases", out v) && v.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                AlertExcludedDatabases = new List<string>();
                foreach (var elem in v.EnumerateArray())
                {
                    var db = elem.GetString();
                    if (!string.IsNullOrWhiteSpace(db)) AlertExcludedDatabases.Add(db);
                }
            }
            if (root.TryGetProperty("alert_tempdb_space_enabled", out v)) AlertTempDbSpaceEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_tempdb_space_threshold_percent", out v)) AlertTempDbSpaceThresholdPercent = v.GetInt32();
            if (root.TryGetProperty("alert_low_disk_enabled", out v)) AlertLowDiskEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_low_disk_threshold_percent", out v)) AlertLowDiskThresholdPercent = (int)Math.Clamp(v.GetInt64(), 0, 100);
            if (root.TryGetProperty("alert_low_disk_threshold_gb", out v)) AlertLowDiskThresholdGb = (int)Math.Max(0, v.GetInt64());
            if (root.TryGetProperty("alert_long_running_job_enabled", out v)) AlertLongRunningJobEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_job_multiplier", out v)) AlertLongRunningJobMultiplier = v.GetInt32();
            if (root.TryGetProperty("alert_failed_job_enabled", out v)) AlertFailedJobEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_failed_job_lookback_minutes", out v)) AlertFailedJobLookbackMinutes = (int)Math.Clamp(v.GetInt64(), 1, 1440);
            if (root.TryGetProperty("alert_cooldown_minutes", out v)) AlertCooldownMinutes = (int)Math.Clamp(v.GetInt64(), 1, 120);
            if (root.TryGetProperty("email_cooldown_minutes", out v)) EmailCooldownMinutes = (int)Math.Clamp(v.GetInt64(), 1, 120);
            if (root.TryGetProperty("alert_delivery_mode", out v) && Enum.TryParse<AlertNotificationMode>(v.GetString(), out var deliveryMode))
                AlertDeliveryMode = deliveryMode;
            if (root.TryGetProperty("alert_per_event_max_per_cycle", out v)) AlertPerEventMaxPerCycle = (int)Math.Clamp(v.GetInt64(), 1, 100);
            if (root.TryGetProperty("mute_rule_default_expiration", out v))
            {
                var exp = v.GetString();
                if (exp is "1 hour" or "24 hours" or "7 days" or "Never")
                    MuteRuleDefaultExpiration = exp;
            }
            if (root.TryGetProperty("log_alert_dismissals", out v)) LogAlertDismissals = v.GetBoolean();

            /* Connection settings */
            if (root.TryGetProperty("connection_timeout_seconds", out v))
            {
                var timeout = v.GetInt32();
                if (timeout >= 5 && timeout <= 60) ConnectionTimeoutSeconds = timeout;
            }

            /* CSV export settings */
            if (root.TryGetProperty("csv_separator", out v))
            {
                var sep = v.GetString();
                if (sep == "," || sep == ";" || sep == "\t") CsvSeparator = sep;
            }

            /* System tray settings */
            if (root.TryGetProperty("minimize_to_tray", out v)) MinimizeToTray = v.GetBoolean();

            /* Time display mode */
            if (root.TryGetProperty("time_display_mode", out v))
            {
                var t = v.GetString();
                if (t == "ServerTime" || t == "LocalTime" || t == "UTC")
                {
                    TimeDisplayMode = t;
                    if (Enum.TryParse<TimeDisplayMode>(t, out var tdm))
                        Services.ServerTimeHelper.CurrentDisplayMode = tdm;
                }
            }

            /* Color theme */
            if (root.TryGetProperty("color_theme", out v))
            {
                var t = v.GetString();
                if (t == "Dark" || t == "Light" || t == "CoolBreeze") ColorTheme = t;
            }

            /* Update check settings */
            if (root.TryGetProperty("check_for_updates_on_startup", out v)) CheckForUpdatesOnStartup = v.GetBoolean();

            /* Teams webhook settings */
            if (root.TryGetProperty("teams_webhook_enabled", out v)) TeamsWebhookEnabled = v.GetBoolean();
            if (root.TryGetProperty("teams_proxy_address", out v)) TeamsProxyAddress = v.GetString() ?? "";

            /* Slack webhook settings */
            if (root.TryGetProperty("slack_webhook_enabled", out v)) SlackWebhookEnabled = v.GetBoolean();
            if (root.TryGetProperty("slack_proxy_address", out v)) SlackProxyAddress = v.GetString() ?? "";

            /* Migrate webhook URLs from plaintext settings.json to Credential Manager */
            if (root.TryGetProperty("teams_webhook_url", out v))
            {
                var legacyUrl = v.GetString() ?? "";
                if (!string.IsNullOrWhiteSpace(legacyUrl))
                {
                    SaveWebhookUrl(TeamsWebhookCredentialKey, legacyUrl);
                }
            }
            if (root.TryGetProperty("slack_webhook_url", out v))
            {
                var legacyUrl = v.GetString() ?? "";
                if (!string.IsNullOrWhiteSpace(legacyUrl))
                {
                    SaveWebhookUrl(SlackWebhookCredentialKey, legacyUrl);
                }
            }

            /* Load webhook URLs from Credential Manager */
            TeamsWebhookUrl = GetWebhookUrl(TeamsWebhookCredentialKey);
            SlackWebhookUrl = GetWebhookUrl(SlackWebhookCredentialKey);

            /* SMTP settings */
            if (root.TryGetProperty("smtp_enabled", out v)) SmtpEnabled = v.GetBoolean();
            if (root.TryGetProperty("smtp_server", out v)) SmtpServer = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_port", out v)) SmtpPort = v.GetInt32();
            if (root.TryGetProperty("smtp_use_ssl", out v)) SmtpUseSsl = v.GetBoolean();
            if (root.TryGetProperty("smtp_username", out v)) SmtpUsername = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_from_address", out v)) SmtpFromAddress = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_recipients", out v)) SmtpRecipients = v.GetString() ?? "";

            if (root.TryGetProperty("analysis_enabled", out v)) AnalysisEnabled = v.GetBoolean();
            if (root.TryGetProperty("analysis_notifications_enabled", out v)) AnalysisNotificationsEnabled = v.GetBoolean();
            if (root.TryGetProperty("analysis_interval_minutes", out v)) AnalysisIntervalMinutes = (int)Math.Clamp(v.GetInt64(), 5, 360);
            if (root.TryGetProperty("analysis_notify_severity", out v)) AnalysisNotifySeverity = Math.Clamp(v.GetDouble(), 0.0, 2.0);
            if (root.TryGetProperty("analysis_notify_cooldown_minutes", out v)) AnalysisNotifyCooldownMinutes = (int)Math.Clamp(v.GetInt64(), 30, 10080);
            if (root.TryGetProperty("analysis_timeout_seconds", out v)) AnalysisTimeoutSeconds = (int)Math.Clamp(v.GetInt64(), 30, 600);
        }
        catch { /* Use defaults */ }
    }

    private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        var exception = e.ExceptionObject as Exception;

        /* Silently swallow Hardcodet TrayToolTip race condition (issue #422) when it
           escapes the Dispatcher path — happens during tray-Exit shutdown when the
           Dispatcher's exception hooks are torn down before the tray library finishes. */
        if (exception != null && IsTrayToolTipCrash(exception))
        {
            AppLogger.Warn("AppDomain", "Suppressed Hardcodet TrayToolTip crash (issue #422)");
            AppLogger.Flush();
            return;
        }

        AppLogger.Error("AppDomain", "Unhandled exception (terminating=" + e.IsTerminating + ")", exception);
        AppLogger.Flush();

        var details = FormatExceptionDetails(exception);
        MessageBox.Show(
            $"A fatal error occurred and the application must close.\n\n{details}",
            "Fatal Error",
            MessageBoxButton.OK,
            MessageBoxImage.Error);
    }

    private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        /* Silently swallow Hardcodet TrayToolTip race condition (issue #422).
           The crash occurs in Popup.CreateWindow when showing the custom visual tooltip
           and is harmless — the tooltip simply doesn't show that one time. */
        if (IsTrayToolTipCrash(e.Exception))
        {
            AppLogger.Warn("Dispatcher", "Suppressed Hardcodet TrayToolTip crash (issue #422)");
            e.Handled = true;
            return;
        }

        AppLogger.Error("Dispatcher", "Unhandled exception", e.Exception);
        AppLogger.Flush();

        var details = FormatExceptionDetails(e.Exception);
        MessageBox.Show(
            $"An error occurred:\n\n{details}\n\nThe application will attempt to continue.",
            "Error",
            MessageBoxButton.OK,
            MessageBoxImage.Error);

        e.Handled = true; /* Prevent application crash */
    }

    private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        AppLogger.Error("Task", "Unobserved task exception", e.Exception);
        AppLogger.Flush();
        e.SetObserved(); /* Prevent process termination */
    }

    /// <summary>
    /// Detects the Hardcodet TrayToolTip race condition crash (issue #422).
    /// </summary>
    private static bool IsTrayToolTipCrash(Exception ex)
    {
        return ex is System.ArgumentException
            && ex.Message.Contains("VisualTarget")
            && ex.StackTrace?.Contains("TaskbarIcon") == true;
    }

    private static string FormatExceptionDetails(Exception? ex)
    {
        if (ex == null) return "Unknown error";

        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"Type: {ex.GetType().FullName}");
        sb.AppendLine($"Message: {ex.Message}");
        sb.AppendLine();
        sb.AppendLine("Stack trace:");
        sb.AppendLine(ex.StackTrace);

        var inner = ex.InnerException;
        var depth = 1;
        while (inner != null)
        {
            sb.AppendLine();
            sb.AppendLine($"--- Inner Exception [{depth}] ---");
            sb.AppendLine($"Type: {inner.GetType().FullName}");
            sb.AppendLine($"Message: {inner.Message}");
            sb.AppendLine("Stack trace:");
            sb.AppendLine(inner.StackTrace);
            inner = inner.InnerException;
            depth++;
        }

        return sb.ToString();
    }
}
