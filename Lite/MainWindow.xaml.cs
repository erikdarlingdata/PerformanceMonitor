/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Xml.Linq;
using System.Net;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Threading;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite;

public partial class MainWindow : Window
{
    private readonly DuckDbInitializer _databaseInitializer;
    private readonly ServerManager _serverManager;
    private readonly ProfileManager _profileManager;
    private readonly ScheduleManager _scheduleManager;
    private RemoteCollectorService? _collectorService;
    private CollectionBackgroundService? _backgroundService;
    private CancellationTokenSource? _backgroundCts;
    private SystemTrayService? _trayService;
    private WindowResumeGuard? _resumeGuard;
    private readonly Dictionary<string, TabItem> _openServerTabs = new();
    private readonly Dictionary<string, (Action<int, int, DateTime?> AlertCounts, Action<int> ApplyTimeRange, Func<Task> ManualRefresh)> _tabEventHandlers = new();
    /* Server tab badge state for the non-blocking/deadlock conditions (#754/#749), keyed by the
       ServerConnection GUID (the same key as _openServerTabs). The alert sweep sets these; both the
       blocking/deadlock tab refresh and the sweep funnel through UpdateTabBadge, so the badge
       reflects all conditions. _lastBadgeCounts lets the sweep re-render with the last-known
       blocking/deadlock counts seen by the tab refresh. */
    private readonly Dictionary<string, bool> _badgeLowDisk = new();
    private readonly Dictionary<string, bool> _badgeFailedJob = new();
    private readonly Dictionary<string, (int Blocking, int Deadlock, DateTime? LatestEvent)> _lastBadgeCounts = new();
    private readonly Dictionary<string, bool> _previousConnectionStates = new();
    private readonly Dictionary<string, bool> _previousCollectorErrorStates = new();
    private readonly Dictionary<string, bool> _previousXeSessionFailureStates = new();
    private readonly Dictionary<string, DateTime> _lastCpuAlert = new();
    private readonly Dictionary<string, DateTime> _lastBlockingAlert = new();
    private readonly Dictionary<string, DateTime> _lastDeadlockAlert = new();
    private readonly Dictionary<string, DateTime> _lastPoisonWaitAlert = new();
    private readonly Dictionary<string, DateTime> _lastLongRunningQueryAlert = new();
    private readonly Dictionary<string, DateTime> _lastTempDbSpaceAlert = new();
    private readonly Dictionary<string, DateTime> _lastLowDiskAlert = new();
    private readonly Dictionary<string, DateTime> _lastLongRunningJobAlert = new();
    private readonly DispatcherTimer _statusTimer;
    private LocalDataService? _dataService;
    private McpHostService? _mcpService;
    private readonly AlertStateService _alertStateService = new();
    private readonly IAlertSettings _alertSettings = new AppAlertSettings();
    private readonly MuteRuleService _muteRuleService;
    private EmailAlertService _emailAlertService;
    /* Held so the edge-trigger watermark seed/persist (#1145) can reach the store directly. */
    private readonly DuckDbAlertHistoryStore _alertHistoryStore;

    /* Track active alert states for resolved notifications */
    private readonly Dictionary<string, bool> _activeCpuAlert = new();
    private readonly Dictionary<string, bool> _activeBlockingAlert = new();
    private readonly Dictionary<string, bool> _activeDeadlockAlert = new();
    private readonly Dictionary<string, bool> _activePoisonWaitAlert = new();
    private readonly Dictionary<string, bool> _activeLongRunningQueryAlert = new();
    private readonly Dictionary<string, bool> _activeTempDbSpaceAlert = new();
    private readonly Dictionary<string, bool> _activeLowDiskAlert = new();
    /* Worst free-% captured at the last low-disk alert per server (#754 follow-up): see the
       Dashboard counterpart. Without it a standing full volume re-fired — and re-recorded an
       alert-history row, defeating Dismiss — every cooldown. Gated by LowDiskAlertGate; removed on resolve. */
    private readonly Dictionary<string, double> _lastAlertedLowDiskPercent = new();
    private readonly Dictionary<string, bool> _activeLongRunningJobAlert = new();
    private readonly Dictionary<string, DateTime> _lastFailedJobAlert = new();
    /* Watermark of the most-recent failed-job run time already alerted per server. A failed run
       lingers in the lookback window for the whole window, so a plain level check would re-fire
       every cooldown; we only notify when a strictly newer failure appears. Bounded by server
       count, so no pruning needed. (Server-local run times mean a fall-back DST hour / NTP step
       could let one new failure tie the watermark and be skipped — a once-a-year, one-hour edge.) */
    private readonly Dictionary<string, DateTime> _lastAlertedFailedJobTime = new();

    /* Edge-trigger watermarks (#1091): the rolling 1-hour blocking/deadlock counts stay
       above the threshold for the whole hour an event lingers in the window, so a plain
       level check re-fires the same alert every cooldown. These hold the count at the last
       fired alert; we only re-notify when the count climbs past it (a genuinely new event),
       and reset to 0 when the window empties so the next event alerts again. */
    private readonly Dictionary<string, int> _lastAlertedBlockingCount = new();
    private readonly Dictionary<string, int> _lastAlertedDeadlockCount = new();

    /* Persistence for the two watermarks above (#1145): seeded from the alert store at
       startup (SeedEdgeTriggerWatermarksAsync) and upserted on change, so a restart does
       not reset the watermark to 0 and re-fire / re-post a webhook for events still
       lingering in the rolling 1-hour lookback window. The metric_name values are the
       persisted-row keys; they need not match the alert "Detected" metric names. */
    private const string BlockingWatermarkMetric = "Blocking Detected";
    private const string DeadlockWatermarkMetric = "Deadlocks Detected";

    public MainWindow()
    {
        InitializeComponent();

        // Initialize services (with loggers wired to AppLogger)
        _databaseInitializer = new DuckDbInitializer(App.DatabasePath, new AppLoggerAdapter<DuckDbInitializer>());
        /* Webhook service is constructed first and injected into the email service
           (Plan E E3c): the shared send core fans out to it. The history store is shared
           by both so the webhook service can seed its cooldown across restart (#1145). */
        _alertHistoryStore = new DuckDbAlertHistoryStore(_databaseInitializer);
        var webhookAlertService = new WebhookAlertService(
            _alertSettings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>(), _alertHistoryStore);
        _emailAlertService = new EmailAlertService(
            _alertSettings,
            _alertHistoryStore,
            webhookAlertService,
            new AppLoggerAdapter<EmailAlertService>());
        _muteRuleService = new MuteRuleService(
            new DuckDbMuteRuleStore(_databaseInitializer),
            new AppLoggerAdapter<MuteRuleService>());
        _serverManager = new ServerManager(App.SharedConfigDirectory, logger: new AppLoggerAdapter<ServerManager>());
        // Two-phase wiring (§3.1): build the ProfileManager (one-way ServerManager injection for the
        // referential-integrity query), then late-inject it back as the ServerManager's IProfileLookup
        // so CheckConnectionAsync resolves profile-backed servers through the same fail-closed logic.
        // Coupling stays acyclic: ServerManager → IProfileLookup ← ProfileManager, ProfileManager → ServerManager.
        _profileManager = new ProfileManager(_serverManager, new AppLoggerAdapter<ProfileManager>());
        _serverManager.ProfileLookup = _profileManager;
        _scheduleManager = new ScheduleManager(App.ConfigDirectory);

        // Status bar update timer
        _statusTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(30) };
        _statusTimer.Tick += async (s, e) =>
        {
            UpdateStatusBar();
            await RefreshOverviewAsync();
            CheckConnectionsAndNotify();

            /* Auto-refresh alert history if the tab is active */
            if (ServerTabControl.SelectedItem == AlertsTab)
                AlertsHistoryContent.RefreshAlerts();
        };

        // Initialize database and UI
        Loaded += MainWindow_Loaded;
        Closing += MainWindow_Closing;
        ServerTabControl.SelectionChanged += ServerTabControl_SelectionChanged;
    }

    /// <summary>
    /// The one true window-restore path. Minimize-to-tray calls <see cref="Window.Hide"/>, which
    /// sets WPF <see cref="UIElement.Visibility"/> = Hidden. Only <see cref="Window.Show"/> reconciles
    /// that state and re-runs layout/render — a raw Win32 ShowWindow leaves the HWND visible but the
    /// WPF tree un-arranged, i.e. a blank window (#1050). Every restore entry point — tray double-click,
    /// the "Show Window" menu, the sleep/unlock resume guard, and the second-instance signal — routes
    /// here so the window can never be left visible-but-blank. Must be called on the UI thread.
    /// </summary>
    public void RestoreFromTray()
    {
        Show();
        ShowInTaskbar = true;
        WindowState = WindowState.Normal;
        Activate();
    }

    private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
    {
        try
        {
            StatusText.Text = "Initializing database...";

            // Initialize the DuckDB database
            await _databaseInitializer.InitializeAsync();

            /* Restore edge-trigger watermarks now — after the DB (and the watermark table)
               exist, but before ANY alert sweep can read the watermark dicts. RefreshServerList()
               below ends in a fire-and-forget RefreshOverviewAsync() → CheckPerformanceAlerts, so
               seeding here (not just before the explicit RefreshOverviewAsync later) keeps a
               restart from re-firing / re-posting alerts for events still in the lookback
               window (#1145), independent of whether the DuckDB reads ever yield. */
            await SeedEdgeTriggerWatermarksAsync();

            // Initialize the collection engine (with loggers wired to AppLogger)
            _collectorService = new RemoteCollectorService(
                _databaseInitializer,
                _serverManager,
                _scheduleManager,
                new AppLoggerAdapter<RemoteCollectorService>());

            var archiveService = new ArchiveService(_databaseInitializer, App.ArchiveDirectory, new AppLoggerAdapter<ArchiveService>());
            var retentionService = new RetentionService(App.ArchiveDirectory, new AppLoggerAdapter<RetentionService>());

            // Routes high-severity analysis findings to email/Slack/Teams; the background
            // service runs scheduled analysis and hands findings to it.
            /* serverId resolver: Lite uses the finding's stable int id as a string (Plan E E3c). */
            var analysisNotificationService = new AnalysisNotificationService(
                _emailAlertService, _alertSettings, f => f.ServerId.ToString(), new AppLoggerAdapter<AnalysisNotificationService>());

            _backgroundService = new CollectionBackgroundService(
                _collectorService, _databaseInitializer, archiveService, retentionService, _serverManager,
                analysisNotificationService,
                new AppLoggerAdapter<CollectionBackgroundService>());

            // Start background collection.
            // Off the UI thread on purpose: DuckDB.NET is synchronous and Lite has no
            // ConfigureAwait(false), so starting this from the Loaded handler would run the entire
            // collection/checkpoint/archive pipeline on the WPF dispatcher (per-minute jank, and a
            // multi-second-to-minutes freeze on archive/reset). A pool thread has no
            // SynchronizationContext, so StartAsync and every subsequent continuation stay off-UI.
            // Safe: the pipeline only touches DuckDB + the email/webhook notification service; the
            // UI reads data by polling DuckDB on its own timers, fully decoupled.
            _backgroundCts = new CancellationTokenSource();
            _ = Task.Run(() => _backgroundService.StartAsync(_backgroundCts.Token));

            // Initialize system tray
            _trayService = new SystemTrayService(this, RestoreFromTray, _backgroundService);
            _trayService.Initialize();

            /* #1050: restore the window from the tray on resume/unlock if a sleep- or lock-driven
               minimize hid it. ??= so a repeated Loaded can't double-subscribe (static SystemEvents). */
            _resumeGuard ??= new WindowResumeGuard(this, RestoreFromTray);

            // Initialize data service for overview
            _dataService = new LocalDataService(_databaseInitializer);

            // Load mute rules from database
            await _muteRuleService.LoadAsync();

            // Initialize alerts history tab
            AlertsHistoryContent.Initialize(_dataService);
            AlertsHistoryContent.MuteRuleService = _muteRuleService;
            AlertsHistoryContent.AlertsDismissed += OnAlertHistoryDismissed;

            // Initialize FinOps tab
            FinOpsContent.Initialize(_dataService, _serverManager);

            // Initialize Recommendations tab (advise-only)
            RecommendationsContent.Initialize(_databaseInitializer, _serverManager);

            // Start MCP server if enabled
            await StartMcpServerAsync();

            // Load servers
            RefreshServerList();

            // Update status
            UpdateStatusBar();
            _statusTimer.Start();

            await RefreshOverviewAsync();
            StatusText.Text = "Ready - Collection active";

            /* Now past the risky DuckDB init — open the single-instance "exit for upgrade" channel so a
               newer build can ask us to step aside cleanly (#single-instance-upgrade-handoff). */
            (Application.Current as App)?.EnableUpgradeHandoff();

            _ = CheckForUpdatesOnStartupAsync();
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Error: {ex.Message}";
            MessageBox.Show(
                $"Failed to initialize the application:\n\n{ex.Message}",
                "Initialization Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
        }
    }

    private async Task CheckForUpdatesOnStartupAsync()
    {
        try
        {
            await Task.Delay(5000); // Don't slow down startup

            if (!App.CheckForUpdatesOnStartup) return;

            // Try Velopack first (supports download + apply)
            try
            {
                var mgr = new Velopack.UpdateManager(
                    new Velopack.Sources.GithubSource(
                        "https://github.com/erikdarlingdata/PerformanceMonitor", null, false));

                var newVersion = await mgr.CheckForUpdatesAsync();
                if (newVersion != null)
                {
                    Dispatcher.Invoke(() =>
                    {
                        Title = $"Performance Monitor Lite — Update v{newVersion.TargetFullRelease.Version} available (Help > About)";
                    });
                    return;
                }
            }
            catch
            {
                // Velopack packages may not exist yet — fall through
            }

            // Fallback: GitHub Releases API check
            var result = await UpdateCheckService.CheckForUpdateAsync();
            if (result?.IsUpdateAvailable == true)
            {
                Dispatcher.Invoke(() =>
                {
                    Title = $"Performance Monitor Lite — Update {result.LatestVersion} available (Help > About)";
                });
            }
        }
        catch
        {
            // Never crash on update check failure
        }
    }

    private bool _closingCleanupStarted;
    private bool _closingCleanupDone;

    private async void MainWindow_Closing(object? sender, System.ComponentModel.CancelEventArgs e)
    {
        /* async void Closing handler: at the first await WPF would otherwise proceed to close the
           window — and since this is the last window, begin app shutdown — so the cleanup
           continuations below could be abandoned mid-flight (the graceful collector stop was
           effectively dead on the common close path). Cancel this close, run the cleanup to
           completion, then Close() again; the second pass returns early and closes for real. */
        if (_closingCleanupDone) return;
        e.Cancel = true;
        if (_closingCleanupStarted) return;
        _closingCleanupStarted = true;

        // Dispose system tray
        _resumeGuard?.Dispose();
        _trayService?.Dispose();

        // Stop background collection with timeout
        _backgroundCts?.Cancel();

        await StopMcpServerAsync();

        if (_backgroundService != null)
        {
            try
            {
                using var shutdownCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _backgroundService.StopAsync(shutdownCts.Token);
            }
            catch (OperationCanceledException)
            {
                /* Shutdown timed out, proceeding anyway */
            }
        }

        // Stop all server tab refresh timers
        foreach (var tab in _openServerTabs.Values)
        {
            if (tab.Content is ServerTab serverTab)
            {
                serverTab.StopRefresh();
            }
        }

        _statusTimer.Stop();

        _closingCleanupDone = true;

        /* Re-close on the next dispatcher cycle, not synchronously here. If the awaits above all
           completed without ever suspending (MCP off, collector already idle), we're still inside
           WPF's Closing event with Window._isClosing == true, and a synchronous Close() re-enters
           InternalClose → VerifyNotClosing() throws "Cannot ... call Close ... while a Window is
           closing" (#1050 follow-up). BeginInvoke lets this Closing event fully unwind — clearing
           _isClosing — before the real close runs; the second pass returns early on
           _closingCleanupDone and the window closes for real. */
        _ = Dispatcher.BeginInvoke(new Action(Close));
    }

    private void ServerTabControl_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        // Only respond to tab selection changes, not child control selection events that bubble up
        if (e.OriginalSource != ServerTabControl) return;

        /* Restore the selected tab's UTC offset so charts use the correct server timezone */
        if (ServerTabControl.SelectedItem is TabItem { Content: ServerTab serverTab })
        {
            ServerTimeHelper.UtcOffsetMinutes = serverTab.UtcOffsetMinutes;
            StatusText.Text = $"Connected to {serverTab.Server.DisplayNameWithIntent}";
        }

        /* Refresh alerts tab when selected */
        if (ServerTabControl.SelectedItem == AlertsTab)
        {
            AlertsHistoryContent.RefreshAlerts();
        }

        /* Refresh recommendations tab when selected (picks up newly-collected findings) */
        if (ServerTabControl.SelectedItem == RecommendationsTab)
        {
            _ = RecommendationsContent.RefreshDataAsync();
        }

        UpdateCollectorHealth();
    }

    private async Task StartMcpServerAsync()
    {
        var mcpSettings = McpSettings.Load(App.ConfigDirectory);
        if (!mcpSettings.Enabled) return;

        try
        {
            bool portInUse = await PortUtilityService.IsTcpPortListeningAsync(mcpSettings.Port, IPAddress.Loopback);
            if (portInUse)
            {
                AppLogger.Error("MCP", $"Port {mcpSettings.Port} is already in use — MCP server not started");
                return;
            }

            _mcpService = new McpHostService(_dataService!, _serverManager, _muteRuleService, _databaseInitializer, mcpSettings.Port);
            _ = _mcpService.StartAsync(_backgroundCts!.Token);
        }
        catch (Exception ex)
        {
            AppLogger.Error("MCP", $"Failed to start MCP server: {ex.Message}");
        }
    }

    private async Task StopMcpServerAsync()
    {
        if (_mcpService != null)
        {
            try
            {
                using var shutdownCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await _mcpService.StopAsync(shutdownCts.Token);
            }
            catch (OperationCanceledException)
            {
                /* MCP shutdown timed out */
            }
            _mcpService = null;
        }
    }

    private void RefreshServerList()
    {
        var servers = _serverManager.GetAllServers();
        foreach (var server in servers)
        {
            server.IsOnline = _serverManager.GetConnectionStatus(server.Id).IsOnline;
            server.HasCollectorErrors = _collectorService != null
                && server.IsOnline == true
                && _collectorService.GetHealthSummary(server).ErroringCollectors > 0;
        }
        ServerListView.ItemsSource = servers;

        // Update UI based on server count
        if (servers.Count == 0 && _openServerTabs.Count == 0)
        {
            EmptyStatePanel.Visibility = Visibility.Visible;
            ServerTabControl.Visibility = Visibility.Collapsed;
        }
        else
        {
            EmptyStatePanel.Visibility = Visibility.Collapsed;
            ServerTabControl.Visibility = Visibility.Visible;
        }

        ServerCountText.Text = $"Servers: {servers.Count}";

        // Refresh FinOps server dropdown when server list changes
        FinOpsContent.RefreshServerList();
        RecommendationsContent.RefreshServerList();

        // Refresh overview when server list changes
        _ = RefreshOverviewAsync();
    }

    private void UpdateStatusBar()
    {
        // Update database size
        var fileSizeMb = _databaseInitializer.GetDatabaseSizeMb();
        var usedSizeMb = _databaseInitializer.GetUsedDataSizeMb();
        if (fileSizeMb > 0)
        {
            DatabaseSizeText.Text = usedSizeMb.HasValue
                ? $"Database: {usedSizeMb.Value:F1} / {fileSizeMb:F1} MB"
                : $"Database: {fileSizeMb:F1} MB";
        }
        else
        {
            DatabaseSizeText.Text = "Database: New";
        }

        // Update collection status
        if (_backgroundService != null)
        {
            if (_backgroundService.IsCollecting)
            {
                CollectionStatusText.Text = "Collection: Running";
            }
            else if (_backgroundService.IsPaused)
            {
                CollectionStatusText.Text = "Collection: Paused";
            }
            else if (_backgroundService.LastCollectionTime.HasValue)
            {
                var ago = DateTime.UtcNow - _backgroundService.LastCollectionTime.Value;
                CollectionStatusText.Text = $"Collection: {ago.TotalSeconds:F0}s ago";
            }
            else
            {
                CollectionStatusText.Text = "Collection: Starting...";
            }
        }
        else
        {
            CollectionStatusText.Text = "Collection: Stopped";
        }

        // Update collector health
        UpdateCollectorHealth();
    }

    private void UpdateCollectorHealth()
    {
        if (_collectorService == null)
        {
            CollectorHealthText.Text = "";
            return;
        }

        int? selectedServerId = null;
        if (ServerTabControl.SelectedItem is TabItem { Content: ServerTab serverTab })
        {
            selectedServerId = serverTab.ServerId;
        }

        var health = _collectorService.GetHealthSummary(selectedServerId);

        if (health.TotalCollectors == 0)
        {
            CollectorHealthText.Text = "";
            return;
        }

        if (health.LoggingFailures > 0)
        {
            CollectorHealthText.Text = $"Logging: BROKEN ({health.LoggingFailures} failures)";
            CollectorHealthText.Foreground = System.Windows.Media.Brushes.Red;
            CollectorHealthText.ToolTip = $"collection_log INSERT is failing.\nThis means collector errors are invisible.\nCheck the log file for details.";
        }
        else if (health.ErroringCollectors > 0)
        {
            var names = string.Join(", ", health.Errors.Select(e => e.CollectorName));
            CollectorHealthText.Text = $"Collectors: {health.ErroringCollectors} erroring";
            CollectorHealthText.Foreground = System.Windows.Media.Brushes.OrangeRed;
            CollectorHealthText.ToolTip = $"Failing: {names}\n\n" +
                string.Join("\n", health.Errors.Select(e =>
                    $"{e.CollectorName}: {e.ConsecutiveErrors}x consecutive - {e.LastErrorMessage}"));
        }
        else if (health.XeSessionFailures.Count > 0)
        {
            /* XE session couldn't be created (#1086). Permission failures don't
               increment ConsecutiveErrors, so without this branch the status bar
               would show OK while blocking/deadlock capture is dead. */
            var names = string.Join(", ", health.XeSessionFailures.Select(e => e.CollectorName));
            CollectorHealthText.Text = $"Capture down: {names}";
            CollectorHealthText.Foreground = System.Windows.Media.Brushes.OrangeRed;
            CollectorHealthText.ToolTip = string.Join("\n", health.XeSessionFailures.Select(e =>
                $"{e.CollectorName}: {e.XeSessionMessage}"));
        }
        else
        {
            CollectorHealthText.Text = $"Collectors: {health.TotalCollectors} OK";
            CollectorHealthText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            CollectorHealthText.ToolTip = null;
        }
    }

    private async Task RefreshOverviewAsync()
    {
        if (_dataService == null) return;

        var servers = _serverManager.GetAllServers();
        if (servers.Count == 0) return;

        try
        {
            var summaries = new List<ServerSummaryItem>();
            foreach (var server in servers)
            {
                try
                {
                    var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
                    var summary = await Task.Run(() => _dataService.GetServerSummaryAsync(serverId, server.DisplayNameWithIntent));
                    if (summary != null)
                    {
                        summary.ServerName = server.ServerName;
                        var connStatus = _serverManager.GetConnectionStatus(server.Id);
                        summary.IsOnline = connStatus.IsOnline;
                        if (_collectorService != null && connStatus.IsOnline == true)
                            summary.HasCollectorErrors = _collectorService.GetHealthSummary(server).ErroringCollectors > 0;
                        summaries.Add(summary);
                    }
                }
                catch (Exception ex)
                {
                    AppLogger.Info("Overview", $"Failed to get summary for {server.DisplayName}: {ex.Message}");
                }
            }

            OverviewItemsControl.ItemsSource = summaries;

            foreach (var summary in summaries)
            {
                CheckPerformanceAlerts(summary);
            }
        }
        catch (Exception ex)
        {
            AppLogger.Info("Overview", $"RefreshOverviewAsync failed: {ex.Message}");
        }
    }

    private void ServerListView_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (ServerListView.SelectedItem is ServerConnection server)
        {
            ConnectToServer(server);
        }
    }

    private void OverviewCard_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        if (e.ClickCount == 2 && sender is FrameworkElement fe && fe.DataContext is ServerSummaryItem summary)
        {
            var server = _serverManager.GetAllServers()
                .FirstOrDefault(s => s.ServerName == summary.ServerName);
            if (server != null)
            {
                ConnectToServer(server);
            }
        }
    }

    private async void ConnectToServer(ServerConnection server)
    {
        // Check if tab already open
        if (_openServerTabs.TryGetValue(server.Id, out var existingTab))
        {
            ServerTabControl.SelectedItem = existingTab;
            return;
        }

        // Clear MFA cancellation flag when user explicitly connects
        // This gives them a fresh attempt at authentication
        var currentStatus = _serverManager.GetConnectionStatus(server.Id);
        if (server.AuthenticationType == AuthenticationTypes.EntraMFA && currentStatus.UserCancelledMfa)
        {
            currentStatus.UserCancelledMfa = false;
            StatusText.Text = "Retrying MFA authentication...";
        }

        // Ensure connection status is populated with UTC offset before opening tab
        // This is critical for timezone-correct chart display
        var status = _serverManager.GetConnectionStatus(server.Id);
        if (!status.UtcOffsetMinutes.HasValue)
        {
            StatusText.Text = "Checking server connection...";
            // Allow interactive auth (MFA) when user explicitly opens a server
            status = await _serverManager.CheckConnectionAsync(server.Id, allowInteractiveAuth: true);
        }

        var utcOffset = status.UtcOffsetMinutes ?? 0;
        var serverTab = new ServerTab(server, _databaseInitializer, _serverManager.CredentialResolver, utcOffset, status.HasMsdbAccess, status.SqlEngineEdition == 5);
        var tabHeader = CreateTabHeader(server);
        var tabItem = new TabItem
        {
            Header = tabHeader,
            Content = serverTab
        };

        /* Subscribe to events — store handlers so we can unsubscribe on tab close */
        var serverId = server.Id;
        Action<int, int, DateTime?> alertHandler = (blockingCount, deadlockCount, latestEventTime) =>
        {
            Dispatcher.Invoke(() => UpdateTabBadge(tabHeader, serverId, blockingCount, deadlockCount, latestEventTime));
        };
        Action<int> timeRangeHandler = (selectedIndex) =>
        {
            Dispatcher.Invoke(() =>
            {
                foreach (var tab in _openServerTabs.Values)
                {
                    if (tab.Content is ServerTab st && st != serverTab)
                    {
                        st.SetTimeRangeIndex(selectedIndex);
                    }
                }
            });
        };
        Func<Task> refreshHandler = async () =>
        {
            if (_collectorService != null)
            {
                var onLoadCollectors = _scheduleManager.GetOnLoadCollectorsForServer(server.Id);
                foreach (var collector in onLoadCollectors)
                {
                    try
                    {
                        await _collectorService.RunCollectorAsync(server, collector.Name);
                    }
                    catch (Exception ex)
                    {
                        AppLogger.Info("MainWindow", $"Re-collection of {collector.Name} failed: {ex.Message}");
                    }
                }
            }
        };

        serverTab.AlertCountsChanged += alertHandler;
        serverTab.ApplyTimeRangeRequested += timeRangeHandler;
        serverTab.ManualRefreshRequested += refreshHandler;
        _tabEventHandlers[server.Id] = (alertHandler, timeRangeHandler, refreshHandler);

        _openServerTabs[server.Id] = tabItem;
        ServerTabControl.Items.Add(tabItem);
        ServerTabControl.SelectedItem = tabItem;

        // Show the tab control, hide empty state
        EmptyStatePanel.Visibility = Visibility.Collapsed;
        ServerTabControl.Visibility = Visibility.Visible;

        _serverManager.UpdateLastConnected(server.Id);

        // Show existing historical data immediately
        serverTab.RefreshData();

        // Then collect fresh data and refresh again
        if (_collectorService != null)
        {
            StatusText.Text = $"Collecting data from {server.DisplayNameWithIntent}...";
            try
            {
                await Task.Run(() => _collectorService.RunAllCollectorsForServerAsync(server));
                StatusText.Text = $"Connected to {server.DisplayNameWithIntent} - Data loaded";
                serverTab.RefreshData();
                UpdateCollectorHealth();
                _ = RefreshOverviewAsync();
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Connected to {server.DisplayNameWithIntent} - Collection error: {ex.Message}";
            }
        }
        else
        {
            StatusText.Text = $"Connected to {server.DisplayNameWithIntent}";
        }
    }

    private StackPanel CreateTabHeader(ServerConnection server)
    {
        var panel = new StackPanel { Orientation = Orientation.Horizontal };

        var tabLabel = server.ReadOnlyIntent ? $"{server.DisplayName} (RO)" : server.DisplayName;
        panel.Children.Add(new TextBlock
        {
            Text = tabLabel,
            VerticalAlignment = VerticalAlignment.Center,
            Margin = new Thickness(0, 0, 4, 0)
        });

        /* Alert badge - hidden by default, shown when blocking/deadlocks detected */
        var badge = new System.Windows.Controls.Border
        {
            Background = System.Windows.Media.Brushes.OrangeRed,
            CornerRadius = new CornerRadius(7),
            Padding = new Thickness(5, 1, 5, 1),
            Margin = new Thickness(0, 0, 4, 0),
            VerticalAlignment = VerticalAlignment.Center,
            Visibility = Visibility.Collapsed,
            Cursor = Cursors.Hand,
            Child = new TextBlock
            {
                FontSize = 10,
                FontWeight = FontWeights.Bold,
                Foreground = System.Windows.Media.Brushes.White,
                VerticalAlignment = VerticalAlignment.Center
            }
        };
        badge.Tag = "AlertBadge";

        /* Add context menu to badge for acknowledge/silence functionality */
        var serverId = server.Id;
        var contextMenu = new ContextMenu();

        var acknowledgeItem = new MenuItem
        {
            Header = "Acknowledge Alert",
            Tag = serverId,
            Icon = new TextBlock { Text = "✓", FontWeight = FontWeights.Bold }
        };
        acknowledgeItem.Click += AcknowledgeServerAlert_Click;

        var silenceItem = new MenuItem
        {
            Header = "Silence This Server",
            Tag = serverId,
            Icon = new TextBlock { Text = "🔇" }
        };
        silenceItem.Click += SilenceServer_Click;

        var unsilenceItem = new MenuItem
        {
            Header = "Unsilence",
            Tag = serverId,
            Icon = new TextBlock { Text = "🔔" }
        };
        unsilenceItem.Click += UnsilenceServer_Click;

        contextMenu.Items.Add(acknowledgeItem);
        contextMenu.Items.Add(silenceItem);
        contextMenu.Items.Add(new Separator());
        contextMenu.Items.Add(unsilenceItem);

        /* Update menu items based on state when opened */
        contextMenu.Opened += (s, args) =>
        {
            var isSilenced = _alertStateService.IsServerSilenced(serverId);
            var hasAlert = badge.Visibility == Visibility.Visible;

            acknowledgeItem.IsEnabled = hasAlert;
            silenceItem.IsEnabled = !isSilenced;
            unsilenceItem.IsEnabled = isSilenced;
        };

        badge.ContextMenu = contextMenu;

        /* Left-click the badge to acknowledge/clear it — the right-click menu was
           undiscoverable, so a plain click is the obvious affordance (issue #1092). */
        badge.MouseLeftButtonUp += (s, e) =>
        {
            AcknowledgeServerBadge(serverId);
            e.Handled = true;
        };

        panel.Children.Add(badge);

        var closeButton = new Button
        {
            Content = "x",
            FontSize = 10,
            Padding = new Thickness(4, 0, 4, 0),
            VerticalAlignment = VerticalAlignment.Center,
            Cursor = Cursors.Hand
        };
        closeButton.Click += (s, e) => CloseServerTab(server.Id);
        panel.Children.Add(closeButton);

        return panel;
    }

    private void UpdateTabBadge(StackPanel tabHeader, string serverId, int blockingCount, int deadlockCount, DateTime? latestEventTime)
    {
        var totalAlerts = blockingCount + deadlockCount;

        /* Remember the blocking/deadlock counts so the alert sweep can re-render this badge with
           them when it updates the low-disk / failed-job state (#754/#749). */
        _lastBadgeCounts[serverId] = (blockingCount, deadlockCount, latestEventTime);

        bool hasLowDisk = _badgeLowDisk.TryGetValue(serverId, out var ld) && ld;
        bool hasFailedJob = _badgeFailedJob.TryGetValue(serverId, out var fj) && fj;

        /* Delegate count tracking and acknowledgement clearing to AlertStateService.
           Uses latestEventTime to only clear ack when genuinely new events arrive,
           not when the user just switches time ranges. */
        bool shouldShow = _alertStateService.UpdateAlertCounts(serverId, blockingCount, deadlockCount, hasLowDisk, hasFailedJob, latestEventTime);

        foreach (var child in tabHeader.Children)
        {
            if (child is System.Windows.Controls.Border border && border.Tag as string == "AlertBadge")
            {
                if (shouldShow)
                {
                    border.Visibility = Visibility.Visible;
                    border.Background = deadlockCount > 0
                        ? System.Windows.Media.Brushes.Red
                        : System.Windows.Media.Brushes.OrangeRed;

                    if (border.Child is TextBlock text)
                    {
                        /* Blocking+deadlock count, or "!" when the only live condition is a low-disk
                           breach / failed job (those aren't counts). */
                        text.Text = totalAlerts > 0
                            ? (totalAlerts > 99 ? "99+" : totalAlerts.ToString())
                            : "!";

                        var extras = new System.Collections.Generic.List<string>();
                        if (hasLowDisk) extras.Add("low disk");
                        if (hasFailedJob) extras.Add("failed job");
                        var extraLine = extras.Count > 0 ? $"\n{string.Join(", ", extras)}" : "";
                        text.ToolTip = $"Blocking: {blockingCount}, Deadlocks: {deadlockCount}{extraLine}\nClick to dismiss · Right-click for options";
                    }
                }
                else
                {
                    border.Visibility = Visibility.Collapsed;
                }
                break;
            }
        }
    }

    /// <summary>
    /// Re-renders a server's tab badge from the alert sweep after its low-disk / failed-job state
    /// changed (#754/#749), reusing the last blocking/deadlock counts the tab refresh saw so the two
    /// inputs stay combined. No-op when the server has no open tab. Keyed by ServerConnection GUID.
    /// </summary>
    private void RefreshServerBadgeExtras(string serverId)
    {
        if (!_openServerTabs.TryGetValue(serverId, out var tab) || tab.Header is not StackPanel tabHeader)
            return;

        var (b, d, ev) = _lastBadgeCounts.TryGetValue(serverId, out var counts)
            ? counts
            : (0, 0, (DateTime?)null);
        UpdateTabBadge(tabHeader, serverId, b, d, ev);
    }

    private void AcknowledgeServerAlert_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
        {
            AcknowledgeServerBadge(serverId);
        }
    }

    /// <summary>
    /// Acknowledges a server's alerts and immediately hides its tab badge.
    /// Shared by the badge left-click, the right-click "Acknowledge" menu, and
    /// Alert History "Dismiss All" so every path clears the badge consistently (issue #1092).
    /// </summary>
    private void AcknowledgeServerBadge(string serverId)
    {
        _alertStateService.AcknowledgeAlert(serverId);
        HideServerBadge(serverId);
    }

    /// <summary>
    /// Collapses the alert badge on a server's tab header, if one is present.
    /// </summary>
    private void HideServerBadge(string serverId)
    {
        if (_openServerTabs.TryGetValue(serverId, out var tab) && tab.Header is StackPanel panel)
        {
            foreach (var child in panel.Children)
            {
                if (child is System.Windows.Controls.Border border && border.Tag as string == "AlertBadge")
                {
                    border.Visibility = Visibility.Collapsed;
                    break;
                }
            }
        }
    }

    /// <summary>
    /// When alerts are cleared from Alert History via "Dismiss All", acknowledge the matching
    /// server tab badge(s) so the at-a-glance indicator stays consistent with the cleared list
    /// (issue #1092). The argument is the DB server_id filter that was in effect; null means the
    /// list spanned all servers, so every open tab is acknowledged. The badge tracks blocking/
    /// deadlock counts (a separate system from the notification alerts the list shows), so this
    /// uses the same acknowledge-until-new-event semantics as the badge's own context menu.
    /// </summary>
    private void OnAlertHistoryDismissed(int? dbServerId)
    {
        foreach (var kvp in _openServerTabs)
        {
            if (kvp.Value.Content is ServerTab st && (dbServerId == null || st.ServerId == dbServerId.Value))
            {
                AcknowledgeServerBadge(kvp.Key);
            }
        }
    }

    private void SilenceServer_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
        {
            _alertStateService.SilenceServer(serverId);

            /* Find and hide the badge for this server */
            if (_openServerTabs.TryGetValue(serverId, out var tab) && tab.Header is StackPanel panel)
            {
                foreach (var child in panel.Children)
                {
                    if (child is System.Windows.Controls.Border border && border.Tag as string == "AlertBadge")
                    {
                        border.Visibility = Visibility.Collapsed;
                        break;
                    }
                }
            }
        }
    }

    private void UnsilenceServer_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
        {
            _alertStateService.UnsilenceServer(serverId);

            /* The next refresh cycle will show the badge if there are alerts */
        }
    }

    private void CloseServerTab(string serverId)
    {
        if (_openServerTabs.TryGetValue(serverId, out var tab))
        {
            if (tab.Content is ServerTab serverTab)
            {
                /* Unsubscribe event handlers to prevent memory leaks */
                if (_tabEventHandlers.TryGetValue(serverId, out var handlers))
                {
                    serverTab.AlertCountsChanged -= handlers.AlertCounts;
                    serverTab.ApplyTimeRangeRequested -= handlers.ApplyTimeRange;
                    serverTab.ManualRefreshRequested -= handlers.ManualRefresh;
                    _tabEventHandlers.Remove(serverId);
                }

                serverTab.StopRefresh();
                serverTab.DisposeChartHelpers();

                /* Clear delta cache for this server to free memory */
                _collectorService?.DeltaCalculator?.ClearServer(serverTab.ServerId);
            }

            ServerTabControl.Items.Remove(tab);
            _openServerTabs.Remove(serverId);

            /* Clean up alert state for this server */
            _alertStateService.RemoveServerState(serverId);

            /* #1128 review fix: drop the per-server badge state so a stale low-disk / failed-job flag
               doesn't flash on reopen, and the dicts don't grow with tab churn. */
            _badgeLowDisk.Remove(serverId);
            _badgeFailedJob.Remove(serverId);
            _lastBadgeCounts.Remove(serverId);

            // Show empty state if no tabs open
            if (_openServerTabs.Count == 0)
            {
                var servers = _serverManager.GetAllServers();
                if (servers.Count == 0)
                {
                    EmptyStatePanel.Visibility = Visibility.Visible;
                    ServerTabControl.Visibility = Visibility.Collapsed;
                }
            }
        }
    }

    private void AddServerButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new AddServerDialog(_serverManager, _profileManager) { Owner = this };
        if (dialog.ShowDialog() == true && dialog.AddedServer != null)
        {
            RefreshServerList();
            StatusText.Text = $"Added server: {dialog.AddedServer.DisplayNameWithIntent}";
        }
    }

    private void ManageServersButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new ManageServersWindow(_serverManager, _profileManager) { Owner = this };
        window.ShowDialog();

        if (window.ServersChanged)
        {
            // Purge collector health for servers that were removed
            if (_collectorService != null)
            {
                var currentServerIds = new HashSet<int>(
                    _serverManager.GetAllServers().Select(s =>
                        RemoteCollectorService.GetDeterministicHashCode(
                            RemoteCollectorService.GetServerNameForStorage(s))));
                _collectorService.ClearHealthExcept(currentServerIds);
            }

            RefreshServerList();
        }
    }

    private async void SettingsButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new SettingsWindow(_scheduleManager, _serverManager, _backgroundService, _mcpService, _muteRuleService) { Owner = this };
        window.ShowDialog();
        UpdateStatusBar();

        if (window.McpSettingsChanged)
        {
            await StopMcpServerAsync();
            await StartMcpServerAsync();
        }
    }

    private void AboutButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new Windows.AboutWindow { Owner = this };
        window.ShowDialog();
    }

    private void ViewLogButton_Click(object sender, RoutedEventArgs e)
    {
        var logFile = AppLogger.GetCurrentLogFile();
        try
        {
            if (System.IO.File.Exists(logFile))
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = logFile,
                    UseShellExecute = true
                });
            }
            else
            {
                System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                {
                    FileName = AppLogger.GetLogDirectory(),
                    UseShellExecute = true
                });
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not open log file: {ex.Message}", "Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void OpenLogFolderButton_Click(object sender, RoutedEventArgs e)
    {
        var logDir = AppLogger.GetLogDirectory();
        try
        {
            System.IO.Directory.CreateDirectory(logDir);
            System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
            {
                FileName = logDir,
                UseShellExecute = true
            });
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not open log folder: {ex.Message}", "Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void ImportSettingsButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new Microsoft.Win32.OpenFolderDialog
        {
            Title = "Select Previous Lite Install Folder"
        };

        if (dialog.ShowDialog() != true) return;

        var oldConfigDir = System.IO.Path.Combine(dialog.FolderName, "config");
        var serversJsonPath = System.IO.Path.Combine(oldConfigDir, "servers.json");
        if (!System.IO.File.Exists(serversJsonPath))
        {
            MessageBox.Show(
                "No config\\servers.json found in the selected folder.\n\nSelect the root folder of a previous Lite installation.",
                "Import Settings",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return;
        }

        try
        {
            // Import server connections (upsert by server name)
            var (imported, skipped) = _serverManager.ImportServersFromFile(serversJsonPath);

            // Import credential profiles from the SHARED config dir (M-1: NOT the per-user copy loop
            // below — profiles.json, like servers.json, lives in App.SharedConfigDirectory, so it must
            // be imported via ProfileManager which is backed by that dir). Source path is built from
            // the SAME oldConfigDir variable serversJsonPath uses (M1-R2).
            int profilesImported = 0;
            var profilesJsonPath = System.IO.Path.Combine(oldConfigDir, "profiles.json");
            if (System.IO.File.Exists(profilesJsonPath))
            {
                try
                {
                    (profilesImported, _) = _profileManager.ImportProfilesFromFile(profilesJsonPath);
                }
                catch (Exception pex)
                {
                    AppLogger.Warn("Import", $"Failed to import profiles.json: {pex.Message}");
                }
            }

            // Copy config files that don't already exist in the current install
            var settingsFiles = new[] { "settings.json", "collection_schedule.json", "ignored_wait_types.json" };
            int settingsCopied = 0;

            foreach (var fileName in settingsFiles)
            {
                var source = System.IO.Path.Combine(oldConfigDir, fileName);
                var target = System.IO.Path.Combine(App.ConfigDirectory, fileName);

                if (System.IO.File.Exists(source) && !System.IO.File.Exists(target))
                {
                    System.IO.File.Copy(source, target);
                    settingsCopied++;
                }
            }

            // Copy alert_state.json from old root directory
            var oldAlertState = System.IO.Path.Combine(dialog.FolderName, "alert_state.json");
            var currentAlertState = System.IO.Path.Combine(App.DataDirectory, "alert_state.json");
            if (System.IO.File.Exists(oldAlertState) && !System.IO.File.Exists(currentAlertState))
            {
                System.IO.File.Copy(oldAlertState, currentAlertState);
                settingsCopied++;
            }

            var message = $"Imported {imported} server connection(s).";
            if (skipped > 0)
                message += $"\nSkipped {skipped} duplicate(s) (already configured).";
            if (profilesImported > 0)
                message += $"\nImported {profilesImported} credential profile(s).";
            if (settingsCopied > 0)
                message += $"\nCopied {settingsCopied} settings file(s).";
            if (imported > 0)
                message += "\n\nCredentials from the previous install are preserved.\nIf any connections fail to authenticate, re-enter the password in Manage Servers.";
            if (profilesImported > 0)
                message += "\n\nCredential profile secrets are NOT importable (they live only in Windows Credential Manager, per user).\nEdit each imported profile once in Manage Servers → Credential Profiles to re-enter its secret.";
            if (settingsCopied > 0)
                message += "\n\nRestart the application to apply imported settings.";

            MessageBox.Show(message, "Import Settings", MessageBoxButton.OK, MessageBoxImage.Information);

            if (imported > 0)
                RefreshServerList();
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to import settings: {ex.Message}", "Import Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void ImportDataButton_Click(object sender, RoutedEventArgs e)
    {
        /* Open folder browser to select the old Lite install directory */
        var dialog = new Microsoft.Win32.OpenFolderDialog
        {
            Title = "Select Previous Lite Install Folder",
            Multiselect = false
        };

        if (dialog.ShowDialog(this) != true || string.IsNullOrWhiteSpace(dialog.FolderName))
        {
            return;
        }

        var sourceFolder = dialog.FolderName;

        /* Validate that monitor.duckdb exists in the selected folder */
        if (!DataImportService.ValidateSourceFolder(sourceFolder))
        {
            MessageBox.Show(
                "The selected folder does not contain a monitor.duckdb file.\n\n" +
                "Please select the folder where the previous Lite application was installed.",
                "Invalid Folder",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
            return;
        }

        /* Prevent double-clicks */
        ImportDataButton.IsEnabled = false;
        ImportDataButtonText.Text = "Importing...";
        StatusText.Text = "Importing data from previous install...";

        try
        {
            var importService = new DataImportService(_databaseInitializer, App.ArchiveDirectory);

            /* The tryLockOldDb callback runs on the UI thread to show the retry dialog */
            var result = await Task.Run(async () =>
                await importService.RunImportAsync(sourceFolder, async _ =>
                {
                    var answer = MessageBoxResult.Cancel;
                    await Dispatcher.InvokeAsync(() =>
                    {
                        answer = MessageBox.Show(
                            "Could not lock the database to flush current data.\n\n" +
                            "Close the previous Lite application and click OK to try again.",
                            "Database Locked",
                            MessageBoxButton.OKCancel,
                            MessageBoxImage.Warning);
                    });
                    return answer == MessageBoxResult.OK;
                }));

            if (result.Success)
            {
                StatusText.Text = "Import complete — refreshing views...";
                await _serverManager.CheckAllConnectionsAsync();
                RefreshServerList();
                UpdateStatusBar();
                StatusText.Text = "Import complete";

                MessageBox.Show(
                    $"Import completed successfully.\n\n" +
                    $"Tables flushed from old database: {result.TablesFlushed}\n" +
                    $"Parquet files imported: {result.FilesImported}\n\n" +
                    "Historical data is now available in all views.",
                    "Import Complete",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
            }
            else
            {
                StatusText.Text = "Import cancelled or failed";
                if (!string.IsNullOrEmpty(result.ErrorMessage))
                {
                    MessageBox.Show(
                        result.ErrorMessage,
                        "Import Failed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error);
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("DataImport", "Unhandled import error", ex);
            StatusText.Text = "Import failed";
            MessageBox.Show(
                $"An unexpected error occurred during import:\n\n{ex.Message}",
                "Import Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
        }
        finally
        {
            ImportDataButton.IsEnabled = true;
            ImportDataButtonText.Text = "Import Data";
        }
    }

    /// <summary>
    /// Gets the ServerConnection from a context menu click on a server list item.
    /// </summary>
    private ServerConnection? GetServerFromContextMenu(object sender)
    {
        if (sender is not MenuItem menuItem) return null;
        var contextMenu = menuItem.Parent as ContextMenu;
        var border = contextMenu?.PlacementTarget as FrameworkElement;
        return border?.DataContext as ServerConnection;
    }

    private void ServerContextMenu_Connect_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server != null) ConnectToServer(server);
    }

    private void ServerContextMenu_Disconnect_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server != null) CloseServerTab(server.Id);
    }

    private void ServerContextMenu_ToggleFavorite_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server != null)
        {
            _serverManager.ToggleFavorite(server.Id);
            RefreshServerList();
        }
    }

    private void ServerContextMenu_Edit_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server == null) return;

        var dialog = new AddServerDialog(_serverManager, _profileManager, server) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            RefreshServerList();
        }
    }

    private void ServerContextMenu_Remove_Click(object sender, RoutedEventArgs e)
    {
        var server = GetServerFromContextMenu(sender);
        if (server == null) return;

        var result = MessageBox.Show(
            $"Remove server '{server.DisplayNameWithIntent}'?",
            "Remove Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result == MessageBoxResult.Yes)
        {
            CloseServerTab(server.Id);
            _collectorService?.ClearHealthForServer(
                RemoteCollectorService.GetDeterministicHashCode(
                    RemoteCollectorService.GetServerNameForStorage(server)));
            _serverManager.DeleteServer(server.Id);
            RefreshServerList();
            StatusText.Text = $"Removed server: {server.DisplayNameWithIntent}";
        }
    }

    private bool _sidebarCollapsed;

    private void ToggleSidebar_Click(object sender, RoutedEventArgs e)
    {
        _sidebarCollapsed = !_sidebarCollapsed;

        if (_sidebarCollapsed)
        {
            SidebarColumn.Width = new GridLength(40);
            SidebarTitle.Visibility = Visibility.Collapsed;
            SidebarSubtitle.Visibility = Visibility.Collapsed;
            if (sender is System.Windows.Controls.Button btn) btn.Content = "»";
        }
        else
        {
            SidebarColumn.Width = new GridLength(280);
            SidebarTitle.Visibility = Visibility.Visible;
            SidebarSubtitle.Visibility = Visibility.Visible;
            if (sender is System.Windows.Controls.Button btn) btn.Content = "«";
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            StatusText.Text = "Refreshing...";

            // Check all server connections
            await _serverManager.CheckAllConnectionsAsync();

            RefreshServerList();
            UpdateStatusBar();

            StatusText.Text = "Refresh complete";
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Refresh failed: {ex.Message}";
        }
    }

    private void CheckConnectionsAndNotify()
    {
        try
        {
            var servers = _serverManager.GetAllServers();
            bool needsRefresh = false;
            foreach (var server in servers)
            {
                var status = _serverManager.GetConnectionStatus(server.Id);
                server.IsOnline = status?.IsOnline;
                if (status?.IsOnline == null) continue;

                bool isOnline = status.IsOnline == true;
                var healthSummary = _collectorService != null && isOnline
                    ? _collectorService.GetHealthSummary(server)
                    : null;
                bool hasErrors = healthSummary?.ErroringCollectors > 0;
                server.HasCollectorErrors = hasErrors;

                if (_previousConnectionStates.TryGetValue(server.Id, out var wasOnline))
                {
                    if (App.AlertsEnabled && App.NotifyConnectionChanges)
                    {
                        if (wasOnline && !isOnline)
                        {
                            _trayService?.ShowNotification(
                                "Server Offline",
                                $"{server.DisplayNameWithIntent} is unreachable: {status.ErrorMessage ?? "unknown error"}",
                                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Error);
                        }
                        else if (!wasOnline && isOnline)
                        {
                            _trayService?.ShowNotification(
                                "Server Online",
                                $"{server.DisplayNameWithIntent} is back online",
                                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
                        }
                    }

                    if (wasOnline != isOnline)
                    {
                        needsRefresh = true;
                    }
                }
                else
                {
                    /* First time seeing this server's status — need to refresh */
                    needsRefresh = true;
                }

                if (_previousCollectorErrorStates.TryGetValue(server.Id, out var prevHasErrors) && prevHasErrors != hasErrors)
                    needsRefresh = true;

                /* One-time balloon when blocking/deadlock capture can't start because the
                   XE session couldn't be created (#1086). Edge-triggered on the false→true
                   transition so it doesn't re-fire every poll while the condition persists. */
                bool xeSessionDown = healthSummary?.XeSessionFailures.Count > 0;
                _previousXeSessionFailureStates.TryGetValue(server.Id, out var wasXeSessionDown);

                if (App.AlertsEnabled && xeSessionDown && !wasXeSessionDown)
                {
                    var captures = string.Join(" and ", healthSummary!.XeSessionFailures
                        .Select(f => f.CollectorName == "blocked_process_report" ? "blocking" : "deadlock"));
                    var reason = healthSummary.XeSessionFailures[0].XeSessionMessage ?? "unknown error";

                    _trayService?.ShowNotification(
                        "Capture Not Running",
                        $"{server.DisplayNameWithIntent}: {captures} capture can't start — {reason}",
                        Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning);
                }

                _previousXeSessionFailureStates[server.Id] = xeSessionDown;
                _previousConnectionStates[server.Id] = isOnline;
                _previousCollectorErrorStates[server.Id] = hasErrors;
            }

            if (needsRefresh)
            {
                RefreshServerList();
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("ConnectionAlerts", $"Connection check notify failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Seeds the in-memory edge-trigger watermarks from the persisted store (#1145) so a
    /// restart does not reset them to 0 and re-fire / re-post a webhook for blocking/deadlock
    /// events still lingering in the rolling 1-hour lookback window. Runs once at startup,
    /// after the DB is initialized and before the first alert sweep.
    /// </summary>
    private async Task SeedEdgeTriggerWatermarksAsync()
    {
        try
        {
            var rows = await _alertHistoryStore.LoadEdgeTriggerWatermarksAsync();
            foreach (var (serverId, metricName, watermark) in rows)
            {
                var key = serverId.ToString();
                if (metricName == BlockingWatermarkMetric)
                {
                    _lastAlertedBlockingCount[key] = watermark;
                }
                else if (metricName == DeadlockWatermarkMetric)
                {
                    _lastAlertedDeadlockCount[key] = watermark;
                }
            }

            /* Failed-job watermark (time-based): seed so a restart does not re-fire tray toasts
               for failures still inside the lookback window that the user already saw and dismissed
               before the restart — the failed-job equivalent of the blocking/deadlock seed above. */
            var failedJobRows = await _alertHistoryStore.LoadFailedJobWatermarksAsync();
            foreach (var (serverId, watermark) in failedJobRows)
            {
                _lastAlertedFailedJobTime[serverId.ToString()] = watermark;
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("Alerts", $"Failed to seed edge-trigger watermarks: {ex.Message}");
        }
    }

    }
