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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Interop;
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
    /// other per-user preferences. Lives under the data root, which is deliberately a
    /// SIBLING of the install directory rather than inside it — Velopack's in-place
    /// update left data alone, but re-running Setup.exe deletes the install directory
    /// outright (#1832). See <see cref="Services.DataRootMigration"/>.
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

    /// <summary>#1659 opt-in: announce a server that is already down on its first-ever observation (an app
    /// started mid-outage otherwise never alerts — there was no edge to see). Default off: the classic
    /// edge-only behavior.</summary>
    public static bool NotifyConnectionDownAtStartup { get; set; }

    /// <summary>#1659 opt-in: re-announce a standing outage every N minutes (0 = off, the classic one-alert-
    /// per-outage behavior). Re-fires deliver under the SAME "Server Unreachable" metric name so
    /// webhook-driven automation keyed on it re-triggers.</summary>
    public static int ConnectionRefireMinutes { get; set; }

    /// <summary>#1696: the master switch for the Availability Group alert family — failover, replica
    /// disconnect/reconnect, sync fell behind, database suspended. Default on, matching Darling's
    /// notify_ag_health: a server with no AGs collects no AG rows so the alerts are silent anyway, and an
    /// operator who DOES run AGs should not have to find a switch to be told about a failover.</summary>
    public static bool NotifyAgHealth { get; set; } = true;

    /// <summary>#1696: "AG Sync Fell Behind" fires when a secondary's secondary_lag_seconds reaches this
    /// (0 = off). Default 300, the same as Darling's ag_lag_alert_seconds.</summary>
    public static int AgLagAlertSeconds { get; set; } = 300;

    /// <summary>#1696: the second, independent "AG Sync Fell Behind" trigger — the secondary's
    /// redo_queue_size in KILOBYTES (0 = off, the default). Off because a healthy redo queue size is entirely
    /// workload-dependent, and because a legitimate post-resume catch-up spike would otherwise page.</summary>
    public static long AgRedoQueueAlertKb { get; set; }

    public static bool AlertCpuEnabled { get; set; } = true;
    public static int AlertCpuThreshold { get; set; } = 80;
    /// <summary>Which CPU metric the alert evaluates against. Total = sql_server_cpu + other_process_cpu (matches OS user+system). SqlOnly = SQL Server scheduler %.</summary>
    public static CpuAlertMode AlertCpuMode { get; set; } = CpuAlertMode.Total;
    public static bool AlertBlockingEnabled { get; set; } = true;
    public static int AlertBlockingThreshold { get; set; } = 1;
    /// <summary>
    /// #1839: fire when the latest blocking snapshot's TOTAL blocked wait reaches this many seconds.
    /// 0 = off, and off ships by default — a count threshold can't tell one session blocked for an hour
    /// from one blocked for a second, but turning this on for everyone would change what existing
    /// installs alert about.
    /// </summary>
    public static int AlertBlockingWaitSecondsThreshold { get; set; }
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
    public static int AlertDiskCriticalFreePercent { get; set; } = 3;   // #2107: at/below this % free the low-disk alert grades CRITICAL (#1136 tier)
    public static int AlertDiskCriticalFreeGb { get; set; } = 2;        // #2107: at/below this many GB free is CRITICAL on any volume (OR-ed with the %)
    public static bool AlertPvsEnabled { get; set; } = true;            // #1984 ADR persistent version store pressure
    public static int AlertPvsThresholdPercent { get; set; } = 40;      // Alert when an ADR database's PVS >= X% of its data files (0 disables this check)
    public static int AlertPvsFloorGb { get; set; } = 1;                // AND-qualifier: the PVS must also be >= X GB (0 removes the floor)
    public static bool AlertFileGrowthEnabled { get; set; }             // #2349 database file growth -- OFF by default
    public static int AlertFileGrowthRiseMb { get; set; } = 10240;      // RISE gate: a file grew >= X MB in the window (0 disables this gate)
    public static int AlertFileGrowthVolumePercent { get; set; } = 60;  // LEVEL gate: a file is >= X% of its volume (0 disables this gate)
    public static int AlertFileGrowthLookbackMinutes { get; set; } = 60;// how far back the rise is measured
    public static bool AlertLongRunningJobEnabled { get; set; } = true;
    public static int AlertLongRunningJobMultiplier { get; set; } = 3;
    public static bool AlertFailedJobEnabled { get; set; } = true;
    public static int AlertFailedJobLookbackMinutes { get; set; } = 60;  // Look back this many minutes for failed Agent job runs
    /* Database-state alert: fire when a database's current state deviates from its expected
       (auto-seeded baseline or per-database override) state. Per-database expected states live in the
       config_database_state_expected table, not here — this is only the master enable. */
    public static bool AlertDatabaseStateEnabled { get; set; } = true;
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

    /* Collection settings */
    /* #2167: the Query Store history backfill (#2058) — fills gaps the live path never takes (a
       first-contact tail, an outage hole, a freshly restored database's imported catalog) in bounded
       background slices. Default ON. Turn it off when a heavy catch-up is costing the monitored server
       more than the history is worth; live collection is unaffected and re-enabling resumes exactly
       where the watermarks left off, so nothing is lost by pausing it. Darling's equivalent is a store
       column (V58) because a headless service has no window to click. */
    public static bool QueryStoreBackfillEnabled { get; set; } = true;

    /* System tray settings */
    public static bool MinimizeToTray { get; set; } = true;

    /* Time display mode ("ServerTime", "LocalTime", "UTC") */
    public static string TimeDisplayMode { get; set; } = "ServerTime";

    /* Color theme ("Dark" or "Light") */
    public static string ColorTheme { get; set; } = "Dark";

    /* NOC Overview tile sort ("Cpu" = CPU% descending default, or "Name") */
    public static ServerOverviewSortMode OverviewSortMode { get; set; } = ServerOverviewSort.Default;

    /// <summary>Sidebar fleet-tree groups the user has collapsed (#2020 2b-i-b), each a
    /// <c>FleetGroupKey.ToStorageString()</c> value ("Favorites" / "Untagged" / "Tag:{id}"), so expand/collapse
    /// survives a restart — the Lite twin of the viewer's <c>ViewerPreferences.CollapsedFleetGroups</c>.</summary>
    public static List<string> CollapsedFleetGroups { get; set; } = new();

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

    /* Generic webhook settings (#1506) — POSTs an operator-authored JSON body to any endpoint, so an
       alert can drive automation we ship no adapter for (PagerDuty, Opsgenie, n8n, or a GitHub
       repository_dispatch that re-runs a workflow). The URL and the headers JSON both carry bearer
       tokens, so — like the Teams/Slack URLs — they live in Credential Manager, never in settings.json;
       only the enable flag, the proxy, and the body template are plain prefs. */
    public static bool GenericWebhookEnabled { get; set; } = false;
    public static string GenericWebhookUrl { get; set; } = "";
    public static string GenericWebhookHeadersJson { get; set; } = "";
    public static string GenericWebhookBodyTemplate { get; set; } = "";
    public static string GenericWebhookProxyAddress { get; set; } = "";

    /* PagerDuty webhook settings — Events API v2. The routing key is a bearer secret like the Teams/Slack
       URLs, so it lives in Credential Manager, never in settings.json; only the enable flag and the EU-region
       toggle are plain prefs. */
    public static bool PagerDutyWebhookEnabled { get; set; } = false;
    public static string PagerDutyRoutingKey { get; set; } = "";
    public static bool PagerDutyUseEuRegion { get; set; } = false;
    public static string PagerDutyProxyAddress { get; set; } = "";

    private const string TeamsWebhookCredentialKey = "TeamsWebhook";
    private const string SlackWebhookCredentialKey = "SlackWebhook";
    private const string GenericWebhookCredentialKey = "GenericWebhook";
    private const string GenericWebhookHeadersCredentialKey = "GenericWebhookHeaders";
    private const string PagerDutyWebhookCredentialKey = "PagerDutyWebhook";

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

        /* Initialize paths. Data lives in %LOCALAPPDATA%\PerformanceMonitorLite-Data, NOT in
           %LOCALAPPDATA%\PerformanceMonitorLite — that second path is Velopack's install root, and
           re-running Setup.exe over an existing install renames it aside and deletes it, taking the
           store, the archive and settings.json with it (#1832). Local rather than roaming: the DuckDB
           store must not be dragged across a roaming profile. Migration runs first, before anything
           reads settings or opens the store; AppLogger buffers until Initialize, so its log lines
           land in the file a few statements later. */
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var appDataRoot = Path.Combine(localAppData, Services.DataRootMigration.DataRootName);

        var migration = Services.DataRootMigration.Migrate(
            Path.Combine(localAppData, Services.DataRootMigration.LegacyRootName),
            appDataRoot,
            message => AppLogger.Info("DataRoot", message));

        if (migration.Failed.Count > 0)
        {
            AppLogger.Error("DataRoot",
                $"{migration.Failed.Count} item(s) could not be moved out of the install directory and are " +
                $"NOT in use: {string.Join(", ", migration.Failed)}. Close any other Lite instance and restart " +
                "to retry.");
        }

        DataDirectory = appDataRoot;
        ConfigDirectory = Path.Combine(appDataRoot, "config");
        DatabasePath = Path.Combine(appDataRoot, "monitor.duckdb");
        ArchiveDirectory = Path.Combine(appDataRoot, "archive");

        // Ensure directories exist
        Directory.CreateDirectory(ConfigDirectory);
        Directory.CreateDirectory(Path.Combine(appDataRoot, "archive"));

        // Seed the per-user config dir from the copies bundled next to the exe on first run, so a
        // fresh install/extract has the editable defaults present. Critical for ignored_wait_types.json:
        // without it the wait filter is empty and benign waits flood the wait stats tab (#1240).
        Services.ConfigSeeder.SeedMissing(
            Path.Combine(AppContext.BaseDirectory, "config"),
            ConfigDirectory,
            new[] { "ignored_wait_types.json", "collection_schedule.json" });

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

        /* Entra MFA needs a parent window handle for the WAM broker, or interactive auth fails with
           0xwindow_handle_required instead of prompting (#2184). Registered once, before any window
           exists, because SqlAuthenticationProvider installs process-wide: the Add/Edit dialog's Test
           Connection and every collector connection are covered without per-site wiring. The handle
           itself is resolved lazily per prompt, so registering this early is safe. */
        Services.EntraInteractiveAuth.Register(ActiveWindowHandle);

        // Create and show main window (StartupUri removed for Velopack custom Main)
        _mainWindow = new MainWindow();
        _mainWindow.Show();
    }

    /// <summary>
    /// The window that should own an Entra MFA prompt, resolved at the moment MSAL asks (#2184).
    ///
    /// <para>Prefers whichever window is currently active over the main window, because a connection is
    /// usually triggered from the Add/Edit Server dialog — parenting the account picker to the main
    /// window behind it would let the picker appear behind the dialog the user is looking at. Falls back
    /// to the main window, then to <see cref="IntPtr.Zero"/>, which MSAL treats the same as no handle:
    /// the prompt fails rather than the app crashing, which is the right way round for an auth path.</para>
    ///
    /// <para>Resolved per call rather than captured once: a window's HWND does not exist until the
    /// window has been sourced, and the right parent is whichever window is in front now, not the one
    /// that existed at startup.</para>
    ///
    /// <para>Marshaled to the UI thread: MSAL invokes this from whatever thread SqlClient's token
    /// acquisition runs on — collector worker threads included — and WPF enforces dispatcher affinity
    /// on <see cref="Window"/> properties, so an off-thread read would throw rather than merely race.
    /// The blocking Invoke is safe here because no UI-thread path blocks on a SQL connection open
    /// (opens are async throughout; the UI stays pumping). If the dispatcher cannot deliver anyway
    /// (shutdown timing), Zero degrades to MSAL's normal no-handle failure instead of throwing from
    /// inside the auth callback.</para>
    /// </summary>
    private static IntPtr ActiveWindowHandle()
    {
        try
        {
            var dispatcher = Current?.Dispatcher;
            if (dispatcher is null)
                return IntPtr.Zero;

            return dispatcher.CheckAccess()
                ? ActiveWindowHandleOnUIThread()
                : dispatcher.Invoke(ActiveWindowHandleOnUIThread);
        }
        catch (Exception ex)
        {
            /* Zero reproduces the original #2184 symptom (0xwindow_handle_required), so a throw here
               must leave a trace - a silent fallback would be this bug's own shape one layer down. The
               log call is guarded because this can fire during shutdown, after the dispatcher and
               logger are gone, and logging must never be the thing that breaks auth. */
            try { AppLogger.Warn("App", $"Entra parent-window handle resolution failed; WAM will see no handle: {ex.Message}"); } catch { /* nothing left to log to */ }
            return IntPtr.Zero;
        }
    }

    private static IntPtr ActiveWindowHandleOnUIThread()
    {
        var app = Current;
        if (app is null)
            return IntPtr.Zero;

        Window? active = null;
        foreach (Window window in app.Windows)
        {
            if (window.IsActive)
            {
                active = window;
                break;
            }
        }

        var owner = active ?? app.MainWindow;
        return owner is null ? IntPtr.Zero : new WindowInteropHelper(owner).Handle;
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

    public static void LoadAlertSettings() => LoadAlertSettings(ConfigDirectory, GetWebhookUrl, SaveWebhookUrl);

    /// <summary>
    /// Path- and secret-store-injected form, so the "secrets load even without settings.json" contract is
    /// testable without touching the process-wide config directory or the real Credential Manager.
    /// <paramref name="writeSecret"/> is required rather than optional on purpose: this method WRITES to the
    /// credential store on the #1506 legacy-plaintext-URL path, and a test that forgot to intercept that
    /// would silently overwrite the operator's real webhook URL.
    /// </summary>
    internal static void LoadAlertSettings(
        string configDirectory,
        Func<string, string> readSecret,
        Action<string, string> writeSecret)
    {
        /* Webhook secrets live in Credential Manager, never in settings.json, so they load FIRST and
           unconditionally — before the early return below. They used to load at the tail of the
           settings.json parse, which meant a missing settings.json (exactly what the #1832 install-root
           wipe produced) left the Settings window's webhook boxes blank while the credentials were still
           there. Saving from that window then wrote the blank back, and SaveWebhookUrl DELETES on blank —
           so opening Settings once after the data loss destroyed the surviving webhook URLs too. */
        TeamsWebhookUrl = readSecret(TeamsWebhookCredentialKey);
        SlackWebhookUrl = readSecret(SlackWebhookCredentialKey);
        GenericWebhookUrl = readSecret(GenericWebhookCredentialKey);
        GenericWebhookHeadersJson = readSecret(GenericWebhookHeadersCredentialKey);
        PagerDutyRoutingKey = readSecret(PagerDutyWebhookCredentialKey);

        try
        {
            var path = Path.Combine(configDirectory, "settings.json");
            if (!File.Exists(path)) return;

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            var root = doc.RootElement;

            if (root.TryGetProperty("alerts_enabled", out var v)) AlertsEnabled = v.GetBoolean();
            if (root.TryGetProperty("notify_connection_changes", out v)) NotifyConnectionChanges = v.GetBoolean();
            if (root.TryGetProperty("notify_connection_down_at_startup", out v)) NotifyConnectionDownAtStartup = v.GetBoolean();
            if (root.TryGetProperty("connection_refire_minutes", out v)) ConnectionRefireMinutes = Math.Clamp(v.GetInt32(), 0, 1440);
            /* #1696 AG knobs, clamped on READ to the same ranges Darling clamps, so a hand-edited settings.json
               cannot drive a nonsense threshold in either app. */
            if (root.TryGetProperty("notify_ag_health", out v)) NotifyAgHealth = v.GetBoolean();
            if (root.TryGetProperty("ag_lag_alert_seconds", out v)) AgLagAlertSeconds = Math.Clamp(v.GetInt32(), 0, 86400);
            if (root.TryGetProperty("ag_redo_queue_alert_kb", out v)) AgRedoQueueAlertKb = Math.Clamp(v.GetInt64(), 0L, 1073741824L);
            if (root.TryGetProperty("alert_cpu_enabled", out v)) AlertCpuEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_cpu_threshold", out v)) AlertCpuThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_cpu_mode", out v) && Enum.TryParse<CpuAlertMode>(v.GetString(), out var mode))
                AlertCpuMode = mode;
            if (root.TryGetProperty("alert_blocking_enabled", out v)) AlertBlockingEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_blocking_threshold", out v)) AlertBlockingThreshold = v.GetInt32();
            if (root.TryGetProperty("alert_blocking_wait_seconds_threshold", out v)) AlertBlockingWaitSecondsThreshold = v.GetInt32();
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
            /* #2107: the CRITICAL tier floors, clamped like the WARNING thresholds above. */
            if (root.TryGetProperty("alert_disk_critical_free_percent", out v)) AlertDiskCriticalFreePercent = Math.Clamp(v.GetInt32(), 0, 100);
            if (root.TryGetProperty("alert_disk_critical_free_gb", out v)) AlertDiskCriticalFreeGb = (int)Math.Max(0, v.GetInt64());
            if (root.TryGetProperty("alert_pvs_enabled", out v)) AlertPvsEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_pvs_threshold_percent", out v)) AlertPvsThresholdPercent = (int)Math.Clamp(v.GetInt64(), 0, 100);
            if (root.TryGetProperty("alert_file_growth_enabled", out v)) AlertFileGrowthEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_file_growth_rise_mb", out v)) AlertFileGrowthRiseMb = (int)Math.Max(0, v.GetInt64());
            if (root.TryGetProperty("alert_file_growth_volume_percent", out v)) AlertFileGrowthVolumePercent = (int)Math.Clamp(v.GetInt64(), 0, 100);
            if (root.TryGetProperty("alert_file_growth_lookback_minutes", out v)) AlertFileGrowthLookbackMinutes = (int)Math.Clamp(v.GetInt64(), 5, 1440);
            if (root.TryGetProperty("alert_pvs_floor_gb", out v)) AlertPvsFloorGb = (int)Math.Max(0, v.GetInt64());
            if (root.TryGetProperty("alert_long_running_job_enabled", out v)) AlertLongRunningJobEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_long_running_job_multiplier", out v)) AlertLongRunningJobMultiplier = v.GetInt32();
            if (root.TryGetProperty("alert_failed_job_enabled", out v)) AlertFailedJobEnabled = v.GetBoolean();
            if (root.TryGetProperty("alert_failed_job_lookback_minutes", out v)) AlertFailedJobLookbackMinutes = (int)Math.Clamp(v.GetInt64(), 1, 1440);
            if (root.TryGetProperty("alert_database_state_enabled", out v)) AlertDatabaseStateEnabled = v.GetBoolean();
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

            /* NOC Overview tile sort */
            if (root.TryGetProperty("overview_sort_mode", out v)) OverviewSortMode = ServerOverviewSort.ParseMode(v.GetString());

            /* Sidebar fleet-tree collapsed groups (#2020 2b-i-b) */
            if (root.TryGetProperty("collapsed_fleet_groups", out v) && v.ValueKind == JsonValueKind.Array)
            {
                CollapsedFleetGroups = v.EnumerateArray()
                    .Where(e => e.ValueKind == JsonValueKind.String)
                    .Select(e => e.GetString()!)
                    .ToList();
            }

            /* Update check settings */
            if (root.TryGetProperty("check_for_updates_on_startup", out v)) CheckForUpdatesOnStartup = v.GetBoolean();

            /* Teams webhook settings */
            if (root.TryGetProperty("teams_webhook_enabled", out v)) TeamsWebhookEnabled = v.GetBoolean();
            if (root.TryGetProperty("teams_proxy_address", out v)) TeamsProxyAddress = v.GetString() ?? "";

            /* Slack webhook settings */
            if (root.TryGetProperty("slack_webhook_enabled", out v)) SlackWebhookEnabled = v.GetBoolean();
            if (root.TryGetProperty("slack_proxy_address", out v)) SlackProxyAddress = v.GetString() ?? "";

            /* Generic webhook settings (#1506). The URL + headers JSON are secrets and load from Credential
               Manager below; only these three are plain prefs. */
            if (root.TryGetProperty("generic_webhook_enabled", out v)) GenericWebhookEnabled = v.GetBoolean();
            if (root.TryGetProperty("generic_proxy_address", out v)) GenericWebhookProxyAddress = v.GetString() ?? "";
            if (root.TryGetProperty("generic_body_template", out v)) GenericWebhookBodyTemplate = v.GetString() ?? "";

            /* PagerDuty webhook settings. The routing key is a secret and loads from Credential Manager below;
               only the enable flag and EU-region toggle are plain prefs. */
            if (root.TryGetProperty("pagerduty_webhook_enabled", out v)) PagerDutyWebhookEnabled = v.GetBoolean();
            if (root.TryGetProperty("pagerduty_use_eu_region", out v)) PagerDutyUseEuRegion = v.GetBoolean();
            if (root.TryGetProperty("pagerduty_proxy_address", out v)) PagerDutyProxyAddress = v.GetString() ?? "";

            /* Migrate webhook URLs from plaintext settings.json to Credential Manager. A legacy plaintext
               URL still wins over whatever the store held, matching the old order (save, then read back);
               the live property is set here rather than re-reading, since we just wrote the value. */
            if (root.TryGetProperty("teams_webhook_url", out v))
            {
                var legacyUrl = v.GetString() ?? "";
                if (!string.IsNullOrWhiteSpace(legacyUrl))
                {
                    writeSecret(TeamsWebhookCredentialKey, legacyUrl);
                    TeamsWebhookUrl = legacyUrl;
                }
            }
            if (root.TryGetProperty("slack_webhook_url", out v))
            {
                var legacyUrl = v.GetString() ?? "";
                if (!string.IsNullOrWhiteSpace(legacyUrl))
                {
                    writeSecret(SlackWebhookCredentialKey, legacyUrl);
                    SlackWebhookUrl = legacyUrl;
                }
            }

            /* SMTP settings */
            if (root.TryGetProperty("smtp_enabled", out v)) SmtpEnabled = v.GetBoolean();
            if (root.TryGetProperty("smtp_server", out v)) SmtpServer = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_port", out v)) SmtpPort = v.GetInt32();
            if (root.TryGetProperty("smtp_use_ssl", out v)) SmtpUseSsl = v.GetBoolean();
            if (root.TryGetProperty("smtp_username", out v)) SmtpUsername = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_from_address", out v)) SmtpFromAddress = v.GetString() ?? "";
            if (root.TryGetProperty("smtp_recipients", out v)) SmtpRecipients = v.GetString() ?? "";

            if (root.TryGetProperty("analysis_enabled", out v)) AnalysisEnabled = v.GetBoolean();
            if (root.TryGetProperty("query_store_backfill_enabled", out v)) QueryStoreBackfillEnabled = v.GetBoolean();
            if (root.TryGetProperty("analysis_notifications_enabled", out v)) AnalysisNotificationsEnabled = v.GetBoolean();
            if (root.TryGetProperty("analysis_interval_minutes", out v)) AnalysisIntervalMinutes = (int)Math.Clamp(v.GetInt64(), 5, 360);
            if (root.TryGetProperty("analysis_notify_severity", out v)) AnalysisNotifySeverity = Math.Clamp(v.GetDouble(), 0.0, 2.0);
            if (root.TryGetProperty("analysis_notify_cooldown_minutes", out v)) AnalysisNotifyCooldownMinutes = (int)Math.Clamp(v.GetInt64(), 30, 10080);
            if (root.TryGetProperty("analysis_timeout_seconds", out v)) AnalysisTimeoutSeconds = (int)Math.Clamp(v.GetInt64(), 30, 600);
        }
        catch { /* Use defaults */ }
    }

    /// <summary>
    /// Reads settings.json (or starts fresh), applies <paramref name="mutate"/>, and writes it back
    /// indented; logs and swallows any error under <paramref name="what"/>. Shared by the single-value
    /// Save* methods (and MainWindow's Overview sort selector) so the read/merge/write/catch boilerplate
    /// lives in one place.
    /// </summary>
    public static void WriteSetting(string what, Action<JsonNode> mutate)
    {
        var settingsPath = Path.Combine(ConfigDirectory, "settings.json");
        try
        {
            JsonNode root = File.Exists(settingsPath)
                ? JsonNode.Parse(File.ReadAllText(settingsPath)) ?? new JsonObject()
                : new JsonObject();
            mutate(root);
            File.WriteAllText(settingsPath, root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save {what}: {ex.Message}");
        }
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
