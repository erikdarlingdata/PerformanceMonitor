/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Threading;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using System.Reflection;
using PerformanceMonitorDashboard.Controls;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;
using System.ComponentModel;
using System.Windows.Data;
using System.Xml.Linq;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard
{
    public partial class MainWindow : Window
    {
        private readonly ServerManager _serverManager;
        private readonly Dictionary<string, TabItem> _openTabs;
        private readonly UserPreferencesService _preferencesService;
        private readonly ObservableCollection<ServerListItem> _serverListItems;
        private readonly DispatcherTimer _displayRefreshTimer;
        private readonly DispatcherTimer _connectionStatusTimer;
        private NotificationService? _notificationService;
        private WindowResumeGuard? _resumeGuard;
        private readonly AlertStateService _alertStateService;
        private readonly MuteRuleService _muteRuleService;
        private readonly Dictionary<string, bool> _previousConnectionStates;
        private readonly Dictionary<string, Border> _tabBadges;
        private readonly Dictionary<string, ServerHealthStatus> _latestHealthStatus;
        private bool _sidebarCollapsed = false;
        private bool _isReallyClosing = false;
        private TabItem? _nocTab;
        private LandingPage? _landingPage;
        private TabItem? _alertsTab;
        private TabItem? _planViewerTab;
        private TabItem? _finOpsTab;
        private Controls.FinOpsContent? _finOpsContent;
        private AlertsHistoryContent? _alertsHistoryContent;

        private readonly Dictionary<string, EventHandler> _alertAcknowledgedHandlers = new();

        private McpHostService? _mcpHostService;
        private CancellationTokenSource? _mcpCts;

        // Independent alert engine - runs regardless of which tab is active
        private readonly DispatcherTimer _alertCheckTimer;
        private readonly EmailAlertService _emailAlertService;
        private readonly WebhookAlertService _webhookAlertService;
        private readonly JsonAlertHistoryStore _alertHistoryStore;
        private readonly CredentialService _credentialService;

        /// <summary>
        /// Gated Apply Fix orchestrator (PR-B). The only non-core holder of the
        /// remediation machinery; threaded into the Alerts history UI so the alert
        /// detail dialog can drive a confirmed, audited apply/un-apply.
        /// </summary>
        private readonly Services.Remediation.RemediationApplyService _remediationApplyService;

        // Scheduled analysis-finding notifications — separate cadence and gating from
        // the threshold-alert engine above. Owns its own DispatcherTimer internally;
        // re-Configured after every settings save. Field name avoids colliding with
        // _notificationService (the tray-notification service constructed in Loaded).
        private readonly AnalysisNotificationService _analysisNotificationService;
        private readonly AnalysisScheduler _analysisScheduler;
        private readonly ConcurrentDictionary<string, DateTime> _lastBlockingAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastDeadlockAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastHighCpuAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeBlockingAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeDeadlockAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeHighCpuAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastPoisonWaitAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activePoisonWaitAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastLongRunningQueryAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeLongRunningQueryAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastTempDbSpaceAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeTempDbSpaceAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastLowDiskAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeLowDiskAlert = new();
        /* Worst free-% captured at the last low-disk alert per server (#754 follow-up): a full
           volume is a standing condition, so without this the alert re-fired — and re-recorded an
           alert-history row, defeating Dismiss — every cooldown. Gated by LowDiskAlertGate to notify
           only on a fresh or worsening breach; removed when the condition resolves. */
        private readonly ConcurrentDictionary<string, double> _lastAlertedLowDiskPercent = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastLongRunningJobAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeLongRunningJobAlert = new();
        /* Whether a failed Agent job sits in the lookback window for this server, for the server
           tab badge (#749). Set each alert cycle (true while a failure is in-window, false once it
           ages out), so the badge auto-resolves. Read by UpdateTabBadge. */
        private readonly ConcurrentDictionary<string, bool> _activeFailedJobAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastFailedJobAlert = new();
        /* Watermark of the most-recent failed-job run time already alerted per server. A failed
           run lingers in the lookback window for the whole window, so a plain level check would
           re-fire every cooldown; we only notify when a strictly newer failure appears. Bounded
           by server count, so no pruning needed. (Server-local run times mean a fall-back DST hour
           / NTP step could let one new failure tie the watermark and be skipped — a once-a-year,
           one-hour edge.) */
        private readonly ConcurrentDictionary<string, DateTime> _lastAlertedFailedJobTime = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastCaptureDownAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeCaptureDownAlert = new();
        private readonly ConcurrentDictionary<string, DateTime> _lastCollectionStoppedAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeCollectionStoppedAlert = new();
        private readonly ConcurrentDictionary<string, long> _previousDeadlockCounts = new();
        /* Time of the last NEW deadlock per server, used to de-flap the "Deadlocks Cleared"
           notification (#1091): deadlock detection is edge-triggered off a delta, so the check
           right after a deadlock has a zero delta and would otherwise immediately fire "Cleared".
           We instead keep the alert active until a deadlock-quiet window has elapsed, matching
           Lite's "no deadlocks in the last hour" semantics. */
        private readonly ConcurrentDictionary<string, DateTime> _lastDeadlockActivity = new();
        private static readonly TimeSpan DeadlockQuietWindow = TimeSpan.FromHours(1);

        private const double ExpandedWidth = 250;
        private const double CollapsedWidth = 52;
        private const string NocTabId = "__NOC_OVERVIEW__";
        private const string AlertsTabId = "__ALERTS_HISTORY__";
        private const string PlanViewerTabId = "__PLAN_VIEWER__";
        private const string FinOpsTabId = "__FINOPS__";

        public MainWindow()
        {
            InitializeComponent();

            _serverManager = new ServerManager();
            _openTabs = new Dictionary<string, TabItem>();
            _preferencesService = new UserPreferencesService();
            _alertStateService = new AlertStateService();
            _muteRuleService = new MuteRuleService(new JsonMuteRuleStore(), new LoggerAdapter<MuteRuleService>());
            /* Shared MuteRuleService no longer loads in its ctor (Plan E E3b). The store
               sync-loads its file in its own ctor, so this LoadAsync completes synchronously
               here — preserving Dashboard's prior load-then-purge-in-ctor startup timing. */
            _muteRuleService.LoadAsync().GetAwaiter().GetResult();
            _serverListItems = new ObservableCollection<ServerListItem>();
            _previousConnectionStates = new Dictionary<string, bool>();
            _tabBadges = new Dictionary<string, Border>();
            _latestHealthStatus = new Dictionary<string, ServerHealthStatus>();

            ServerListView.ItemsSource = _serverListItems;

            _credentialService = new CredentialService();
            /* Gated Apply Fix orchestrator (PR-B): registry + executor + audit over the
               existing per-server monitoring connection. No elevation — reuses the
               same credentials the rest of the Dashboard already holds. */
            _remediationApplyService = new Services.Remediation.RemediationApplyService(_serverManager, _credentialService);
            /* Saved-prefs settings adapter shared by the three alert services (Plan E E1). */
            var alertSettings = new DashboardAlertSettings(_preferencesService);
            /* Alert-history store owns the alert_history.json list + management API (Plan E E2).
               Held as a field so SaveAlertLog (and the Alerts UI / MCP via its Current static)
               reach it directly rather than forwarding through EmailAlertService (E3c Phase 6). */
            _alertHistoryStore = new JsonAlertHistoryStore(_preferencesService);
            /* Webhook service is constructed first and injected into the email service
               (Plan E E3c): the shared lib service carries no Current static, so Dashboard
               keeps this handle for the email fan-out and any MCP/health consumers. The
               history store (built at line above) is passed so the webhook cooldown is seeded
               across restart (#1145) — without it a restart inside the cooldown window
               re-posts a Teams/Slack alert delivered just before the restart. */
            _webhookAlertService = new WebhookAlertService(alertSettings, EmailAlertService.Branding, new LoggerAdapter<WebhookAlertService>(), _alertHistoryStore);
            _emailAlertService = new EmailAlertService(alertSettings, _alertHistoryStore, _webhookAlertService, new LoggerAdapter<EmailAlertService>());

            _alertCheckTimer = new DispatcherTimer();
            _alertCheckTimer.Tick += AlertCheckTimer_Tick;

            /* Scheduled analysis-finding notifications. Constructed alongside the
               alert engine (all dependencies exist by this point); started by
               _analysisScheduler.Configure() in MainWindow_Loaded. */
            /* serverId resolver (Plan E E3c): use the matching ServerConnection.Id (GUID string)
               so alert_history.json keys stay consistent with the threshold-alert engine; fall
               back to the finding's stable int id if the lookup misses (server removed mid-cycle). */
            _analysisNotificationService = new AnalysisNotificationService(
                _emailAlertService,
                alertSettings,
                finding => _serverManager.GetAllServers()
                    .FirstOrDefault(s => string.Equals(s.ServerName, finding.ServerName, StringComparison.OrdinalIgnoreCase))
                    ?.Id ?? finding.ServerId.ToString(),
                new LoggerAdapter<AnalysisNotificationService>(),
                /* Suppress analysis-finding emails for servers the user silenced via
                   "Silence All Alerts" — matches the threshold-alert guard. */
                _alertStateService.IsAnySilencingActive,
                /* WS2: always pop a tray balloon for a notify-worthy finding — the same visible
                   signal threshold alerts already raise, so a local-only user with no email/webhook
                   still sees findings. Late-bound to the _notificationService field (built later in
                   Loaded); ShowNotification honors the notifications-enabled pref + marshals to the
                   UI thread, so it is safe to invoke from the analysis cycle. */
                showTrayNotification: (title, message) =>
                    _notificationService?.ShowNotification(title, message, NotificationType.Warning));
            _analysisScheduler = new AnalysisScheduler(
                _serverManager, _credentialService, _preferencesService, _analysisNotificationService);

            _displayRefreshTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(30)
            };
            _displayRefreshTimer.Tick += DisplayRefreshTimer_Tick;

            _connectionStatusTimer = new DispatcherTimer();
            _connectionStatusTimer.Tick += ConnectionStatusTimer_Tick;

            Loaded += MainWindow_Loaded;
            StateChanged += MainWindow_StateChanged;
            Closing += MainWindow_Closing;
            ServerTabControl.SelectionChanged += ServerTabControl_SelectionChanged;
        }

        protected override void OnSourceInitialized(EventArgs e)
        {
            base.OnSourceInitialized(e);

            // Hook into window messages to handle single-instance activation
            var source = HwndSource.FromHwnd(new WindowInteropHelper(this).Handle);
            source?.AddHook(WndProc);
        }

        private IntPtr WndProc(IntPtr hwnd, int msg, IntPtr wParam, IntPtr lParam, ref bool handled)
        {
            if (msg == NativeMethods.WM_SHOWMONITOR)
            {
                // Another instance tried to start - bring this window to front (#769)
                Show();
                if (WindowState == WindowState.Minimized)
                    WindowState = WindowState.Normal;
                Activate();
                Topmost = true;  // Temporarily set topmost to ensure visibility
                Topmost = false;
                handled = true;
            }
            return IntPtr.Zero;
        }

        private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
        {
            // Sync preferences
            var startupPrefs = _preferencesService.GetPreferences();
            TabHelpers.CsvSeparator = startupPrefs.CsvSeparator;
            MuteRuleDialog.DefaultExpiration = startupPrefs.MuteRuleDefaultExpiration;
            // #1831: the startup override that forced ServerTime here is gone — it existed because
            // chart axes rendered raw server time regardless of the dropdown, and the least
            // misleading option was pinning the dropdown to match. The shared axis formatter now
            // converts through UiTimeContext at render, so the SAVED preference is restored
            // instead — the force was the only startup assignment, so without this the setting
            // was written on every change and read never.
            if (Enum.TryParse<TimeDisplayMode>(startupPrefs.TimeDisplayMode, out var savedDisplayMode))
            {
                Helpers.ServerTimeHelper.CurrentDisplayMode = savedDisplayMode;
            }

            // Wire the shared-UI time conversion hook before any chart/crosshair can
            // render (ahead of the tab-opening awaits below). The lambda reads
            // CurrentDisplayMode at call time, so later display-mode switches are honored.
            PerformanceMonitor.Ui.UiTimeContext.ConvertForDisplay =
                t => Helpers.ServerTimeHelper.ConvertForDisplay(t, Helpers.ServerTimeHelper.CurrentDisplayMode);

            await LoadServerListAsync();
            InitializeNotificationService();
            OpenNocTab();
            OpenAlertsTab();
            ServerTabControl.SelectedItem = _nocTab; /* Keep Overview as the active tab */
            LoadSidebarState();
            ConfigureConnectionStatusTimer();
            ConfigureAlertCheckTimer();
            _analysisScheduler.Configure();
            UpdateAlertBadge();
            StartMcpServerIfEnabled();

            _displayRefreshTimer.Start();

            await CheckAllConnectionsAsync();

            /* Past startup init (MCP bound, services configured) — open the single-instance "exit for
               upgrade" channel so a newer build can ask us to step aside cleanly
               (#single-instance-upgrade-handoff). */
            (Application.Current as App)?.EnableUpgradeHandoff();

            _ = CheckForUpdatesOnStartupAsync();
        }

        private async Task CheckForUpdatesOnStartupAsync()
        {
            try
            {
                await Task.Delay(5000); // Don't slow down startup

                var prefs = _preferencesService.GetPreferences();
                if (!prefs.CheckForUpdatesOnStartup) return;

                // Try Velopack first (supports download + apply)
                try
                {
                    var mgr = new Velopack.UpdateManager(
                        new Velopack.Sources.GithubSource(
                            "https://github.com/erikdarlingdata/PerformanceMonitor", null, false));

                    var newVersion = await mgr.CheckForUpdatesAsync();
                    if (newVersion != null)
                    {
                        _notificationService?.ShowNotification(
                            "Update Available",
                            $"Performance Monitor {newVersion.TargetFullRelease.Version} is available. Use Help > About to download and install.",
                            NotificationType.Info);
                        return;
                    }
                }
                catch
                {
                    // Velopack packages may not exist yet — fall through to legacy check
                }

                // Fallback: GitHub Releases API check (notification only)
                var result = await UpdateCheckService.CheckForUpdateAsync();
                if (result?.IsUpdateAvailable == true)
                {
                    _notificationService?.ShowNotification(
                        "Update Available",
                        $"Performance Monitor {result.LatestVersion} is available (you have {result.CurrentVersion}). Check About for details.",
                        NotificationType.Info);
                }
            }
            catch
            {
                // Never crash on update check failure
            }
        }

        private async void StartMcpServerIfEnabled()
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.McpEnabled)
            {
                return;
            }

            try
            {
                bool portInUse = await PortUtilityService.IsTcpPortListeningAsync(prefs.McpPort, IPAddress.Loopback);
                if (portInUse)
                {
                    Logger.Error($"[MCP] Port {prefs.McpPort} is already in use — MCP server not started");
                    return;
                }

                _mcpHostService = new McpHostService(_serverManager, _credentialService, _muteRuleService, _preferencesService, prefs.McpPort);
                _mcpCts = new CancellationTokenSource();
                _ = _mcpHostService.StartAsync(_mcpCts.Token);
            }
            catch (Exception ex)
            {
                Logger.Error($"[MCP] Failed to start MCP server: {ex.Message}", ex);
            }
        }

        private async Task StopMcpServerAsync()
        {
            if (_mcpHostService != null)
            {
                try
                {
                    _mcpCts?.Cancel();
                    using var shutdownCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await _mcpHostService.StopAsync(shutdownCts.Token);
                }
                catch (Exception ex)
                {
                    Logger.Error($"[MCP] Error stopping MCP server: {ex.Message}", ex);
                }
                _mcpHostService = null;
                _mcpCts?.Dispose();
                _mcpCts = null;
            }
        }

        private async void RestartMcpServerIfNeeded(bool wasEnabled, int oldPort)
        {
            var prefs = _preferencesService.GetPreferences();
            bool changed = prefs.McpEnabled != wasEnabled || prefs.McpPort != oldPort;
            if (!changed) return;

            await StopMcpServerAsync();
            StartMcpServerIfEnabled();
        }

        private void InitializeNotificationService()
        {
            _notificationService = new NotificationService(this, _preferencesService);
            _notificationService.Initialize();

            /* #1050: restore the window from the tray on resume/unlock if a sleep- or lock-driven
               minimize hid it. ??= so a repeated Loaded can't double-subscribe (static SystemEvents). */
            _resumeGuard ??= new WindowResumeGuard(this, _notificationService.ShowMainWindow);
        }

        private void MainWindow_StateChanged(object? sender, EventArgs e)
        {
            if (WindowState == WindowState.Minimized)
            {
                var prefs = _preferencesService.GetPreferences();
                if (prefs.MinimizeToTray)
                {
                    Hide();
                }
            }
        }

        private void MainWindow_Closing(object? sender, System.ComponentModel.CancelEventArgs e)
        {
            var prefs = _preferencesService.GetPreferences();

            // If minimize to tray is enabled and we're not really closing, minimize instead
            if (prefs.MinimizeToTray && !_isReallyClosing)
            {
                e.Cancel = true;
                WindowState = WindowState.Minimized;
                Hide();
                return;
            }

            // Clean up MCP server
            try { Task.Run(StopMcpServerAsync).Wait(TimeSpan.FromSeconds(10)); }
            catch { /* shutdown best-effort */ }

            // Stop the scheduled-analysis timer + cancel its in-flight cycle so the
            // per-server Task.Delay timers can drop out cleanly instead of waiting
            // out their full timeout during shutdown.
            _analysisScheduler?.Stop();

            // Save alert history to disk
            _alertHistoryStore?.SaveAlertLog();

            // Clean up notification service (real-close path only — the X-button minimize-to-tray
            // branch above returns early, so the resume guard stays alive while the app runs)
            _resumeGuard?.Dispose();
            _notificationService?.Dispose();
        }

        public void ExitApplication()
        {
            _isReallyClosing = true;
            Close();
        }

        private void DisplayRefreshTimer_Tick(object? sender, EventArgs e)
        {
            foreach (var item in _serverListItems)
            {
                item.RefreshTimestampDisplay();
            }
        }

        private bool _isCheckingConnections;

        private async void ConnectionStatusTimer_Tick(object? sender, EventArgs e)
        {
            /* Skip if the previous check is still running so slow servers don't stack overlapping
               connection sweeps at the minimum interval. */
            if (_isCheckingConnections) return;
            _isCheckingConnections = true;
            try { await CheckAllConnectionsAsync(); }
            finally { _isCheckingConnections = false; }
        }

        private void ConfigureConnectionStatusTimer()
        {
            var prefs = _preferencesService.GetPreferences();

            if (prefs.NotificationsEnabled)
            {
                var intervalSeconds = (prefs.AutoRefreshEnabled && prefs.AutoRefreshIntervalSeconds > 0)
                    ? prefs.AutoRefreshIntervalSeconds
                    : 60;
                _connectionStatusTimer.Interval = TimeSpan.FromSeconds(intervalSeconds);
                _connectionStatusTimer.Start();
            }
            else
            {
                _connectionStatusTimer.Stop();
            }
        }

        private void LoadSidebarState()
        {
            var prefs = _preferencesService.GetPreferences();
            _sidebarCollapsed = prefs.SidebarCollapsed;
            ApplySidebarState();
        }

        private void SaveSidebarState()
        {
            var prefs = _preferencesService.GetPreferences();
            prefs.SidebarCollapsed = _sidebarCollapsed;
            _preferencesService.SavePreferences(prefs);
        }

        private void SidebarToggle_Click(object sender, RoutedEventArgs e)
        {
            _sidebarCollapsed = !_sidebarCollapsed;
            ApplySidebarState();
            SaveSidebarState();
        }

        private void ApplySidebarState()
        {
            if (_sidebarCollapsed)
            {
                SidebarColumn.Width = new GridLength(CollapsedWidth);
                SidebarHeaderText.Visibility = Visibility.Collapsed;
                ServerListView.Visibility = Visibility.Collapsed;
                SidebarFooter.Visibility = Visibility.Collapsed;
                SidebarToggleIcon.Text = "»";
                SidebarToggleButton.ToolTip = "Expand sidebar";
                SidebarToggleButton.Margin = new Thickness(0);
                SidebarToggleButton.HorizontalAlignment = HorizontalAlignment.Center;
            }
            else
            {
                SidebarColumn.Width = new GridLength(ExpandedWidth);
                SidebarHeaderText.Visibility = Visibility.Visible;
                ServerListView.Visibility = Visibility.Visible;
                SidebarFooter.Visibility = Visibility.Visible;
                SidebarToggleIcon.Text = "«";
                SidebarToggleButton.ToolTip = "Collapse sidebar";
                SidebarToggleButton.Margin = new Thickness(8, 0, 0, 0);
                SidebarToggleButton.HorizontalAlignment = HorizontalAlignment.Right;
            }
        }

        private async System.Threading.Tasks.Task LoadServerListAsync()
        {
            var servers = _serverManager.GetAllServers();

            _serverListItems.Clear();
            foreach (var server in servers)
            {
                var status = _serverManager.GetConnectionStatus(server.Id);
                _serverListItems.Add(new ServerListItem(server, status));
            }

            // Add default sort for the list of servers by server display name.
            _serverListItems.OrderBy(s => s.DisplayName).ToList().ForEach(s => _serverListItems.Move(_serverListItems.IndexOf(s), _serverListItems.Count - 1));

            // Also refresh the landing page if it exists
            if (_landingPage != null)
            {
                await _landingPage.ReloadServersAsync();
            }
        }

        private async System.Threading.Tasks.Task CheckAllConnectionsAsync()
        {
            var prefs = _preferencesService.GetPreferences();

            var tasks = _serverListItems.Select(async item =>
            {
                var newStatus = await _serverManager.CheckConnectionAsync(item.Id);

                Dispatcher.Invoke(() =>
                {
                    // Check for status change before updating
                    bool wasOnline = _previousConnectionStates.TryGetValue(item.Id, out var prev) && prev;
                    bool isOnline = newStatus.IsOnline == true;

                    // Update the UI
                    item.RefreshStatus(newStatus);

                    // Send notifications on status changes (skip first check)
                    if (_previousConnectionStates.ContainsKey(item.Id))
                    {
                        /* "Silence All Alerts" suppresses connection up/down notifications too —
                           match the threshold-alert guard so a silenced server produces no tray,
                           email, or history row. State tracking below still runs unconditionally,
                           so unsilencing resumes from the correct baseline. */
                        bool silenced = _alertStateService.IsAnySilencingActive(item.Id);

                        if (!silenced && wasOnline && !isOnline && prefs.NotifyOnConnectionLost)
                        {
                            _notificationService?.ShowServerOfflineNotification(
                                item.DisplayName,
                                newStatus.ErrorMessage);

                            var errorDetail = newStatus.ErrorMessage ?? "Connection failed";
                            _emailAlertService.RecordAlert(item.Id, item.DisplayName, "Server Unreachable",
                                errorDetail, "Online", true, "email");
                            _ = _emailAlertService.TrySendAlertEmailAsync(
                                "Server Unreachable",
                                item.DisplayName,
                                errorDetail,
                                "Online",
                                item.Id);
                        }
                        else if (!silenced && !wasOnline && isOnline && prefs.NotifyOnConnectionRestored)
                        {
                            _notificationService?.ShowConnectionRestoredNotification(item.DisplayName);

                            _emailAlertService.RecordAlert(item.Id, item.DisplayName, "Server Restored",
                                "Online", "Online", true, "email");
                            _ = _emailAlertService.TrySendAlertEmailAsync(
                                "Server Restored",
                                item.DisplayName,
                                "Connection restored",
                                "Online",
                                item.Id);
                        }
                    }

                    // Track current state for next check
                    _previousConnectionStates[item.Id] = isOnline;
                });
            });
            await System.Threading.Tasks.Task.WhenAll(tasks);
        }

        private async void RefreshAllStatus_Click(object sender, RoutedEventArgs e)
        {
            RefreshAllButton.IsEnabled = false;
            RefreshAllButton.Content = "Checking...";

            try
            {
                await CheckAllConnectionsAsync();
            }
            finally
            {
                RefreshAllButton.IsEnabled = true;
                RefreshAllButton.Content = "↻ Refresh All Status";
            }
        }

        private async void CheckConnection_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                var newStatus = await _serverManager.CheckConnectionAsync(item.Id);
                item.RefreshStatus(newStatus);
            }
        }

        private async void ServerListView_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                await OpenServerTabAsync(item.Server);
            }
        }

        private async void OpenServerTab_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                await OpenServerTabAsync(item.Server);
            }
        }

        private async Task OpenServerTabAsync(ServerConnection server)
        {
            if (_openTabs.TryGetValue(server.Id, out var existingTab))
            {
                ServerTabControl.SelectedItem = existingTab;
                return;
            }

            /* Set server UTC offset for chart axis bounds */
            var connStatus = _serverManager.GetConnectionStatus(server.Id);
            if (!connStatus.UtcOffsetMinutes.HasValue)
            {
                /* Background check hasn't run yet — fetch offset synchronously so
                   the first tab open doesn't default to local timezone. */
                try
                {
                    await _serverManager.CheckConnectionAsync(server.Id);
                    connStatus = _serverManager.GetConnectionStatus(server.Id);
                }
                catch { /* Fall through to local offset default */ }
            }
            var utcOffset = connStatus.UtcOffsetMinutes ?? (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;
            Helpers.ServerTimeHelper.UtcOffsetMinutes = utcOffset;

            ServerTab serverTab;
            try
            {
                serverTab = new ServerTab(server, utcOffset);
            }
            catch (Exception ex)
            {
                var inner = ex.InnerException?.Message ?? ex.Message;
                System.Windows.MessageBox.Show(
                    $"Failed to open server tab for '{server.DisplayNameWithIntent}'.\n\n" +
                    $"This is usually caused by a missing Visual C++ Redistributable (x64) " +
                    $"or an OS compatibility issue with the SkiaSharp rendering library.\n\n" +
                    $"Download the latest VC++ Redistributable from:\n" +
                    $"https://aka.ms/vs/17/release/vc_redist.x64.exe\n\n" +
                    $"Error: {inner}",
                    "Chart Initialization Error",
                    System.Windows.MessageBoxButton.OK,
                    System.Windows.MessageBoxImage.Error);
                return;
            }

            // WS1b-2: thread the shared gated remediation service to the Recommendations sub-tab
            // (the same instance the Alerts tab uses), enabling Apply + the informed-consent gate.
            serverTab.RemediationApplyService = _remediationApplyService;

            EventHandler alertHandler = (_, _) =>
            {
                _alertHistoryStore.HideAllAlerts(8760, server.DisplayNameWithIntent);
                UpdateAlertBadge();
                _alertsHistoryContent?.RefreshAlerts();
            };
            serverTab.AlertAcknowledged += alertHandler;
            _alertAcknowledgedHandlers[server.Id] = alertHandler;

            var headerPanel = new StackPanel { Orientation = Orientation.Horizontal };
            var headerText = new TextBlock
            {
                Text = server.ReadOnlyIntent ? $"{server.DisplayName} (RO)" : server.DisplayName,
                VerticalAlignment = VerticalAlignment.Center
            };
            var closeButton = new Button
            {
                Style = (Style)FindResource("TabCloseButton"),
                Tag = server.Id
            };
            closeButton.Click += CloseTab_Click;

            var badge = new Border
            {
                Style = (Style)FindResource("AlertBadge"),
                Visibility = Visibility.Collapsed,
                Cursor = Cursors.Hand,
                ToolTip = "Click to dismiss · Right-click for options",
                Child = new TextBlock
                {
                    Text = "!",
                    FontWeight = FontWeights.Bold,
                    Foreground = Brushes.White
                }
            };

            /* Left-click the badge to acknowledge/clear it — the right-click menu was
               undiscoverable, so a plain click is the obvious affordance (issue #1092,
               matching the Lite app). */
            badge.MouseLeftButtonUp += (s, e) =>
            {
                AcknowledgeServerAlerts(server.Id);
                e.Handled = true;
            };

            headerPanel.Children.Add(headerText);
            headerPanel.Children.Add(badge);
            headerPanel.Children.Add(closeButton);

            // Create context menu for alert suppression
            var contextMenu = new ContextMenu();
            var acknowledgeItem = new MenuItem
            {
                Header = "Acknowledge Alerts",
                Tag = server.Id,
                Icon = new TextBlock { Text = "✓", FontWeight = FontWeights.Bold }
            };
            acknowledgeItem.Click += AcknowledgeServerAlerts_Click;
            var silenceItem = new MenuItem
            {
                Header = "Silence All Alerts",
                Tag = server.Id,
                Icon = new TextBlock { Text = "🔇" }
            };
            silenceItem.Click += SilenceServer_Click;
            var unsilenceItem = new MenuItem
            {
                Header = "Unsilence",
                Tag = server.Id,
                Icon = new TextBlock { Text = "🔔" }
            };
            unsilenceItem.Click += UnsilenceServer_Click;

            contextMenu.Items.Add(acknowledgeItem);
            contextMenu.Items.Add(silenceItem);
            contextMenu.Items.Add(new Separator());
            contextMenu.Items.Add(unsilenceItem);

            // Capture badge reference for closure
            var localBadge = badge;

            // Update menu items based on silenced state and alert presence when opened
            contextMenu.Opened += (s, args) =>
            {
                var isSilenced = _alertStateService.IsAnySilencingActive(server.Id);
                var hasAlert = localBadge.Visibility == Visibility.Visible;

                // Acknowledge only enabled if there's a visible alert
                acknowledgeItem.IsEnabled = hasAlert;
                silenceItem.IsEnabled = !isSilenced;
                unsilenceItem.IsEnabled = isSilenced;
            };

            // Add transparent background to ensure hit-testing works
            headerPanel.Background = Brushes.Transparent;

            _tabBadges[server.Id] = badge;

            var tabItem = new TabItem
            {
                Header = headerPanel,
                Content = serverTab,
                Tag = server.Id,
                ContextMenu = contextMenu  // Attach to TabItem for reliable right-click
            };

            ServerTabControl.Items.Add(tabItem);
            _openTabs[server.Id] = tabItem;

            var prefs = _preferencesService.GetPreferences();
            if (prefs.FocusServerTabOnClick)
            {
                ServerTabControl.SelectedItem = tabItem;
            }

            _serverManager.UpdateLastConnected(server.Id);
        }

        private void OpenNocTab()
        {
            // If NOC tab already exists, just select it
            if (_nocTab != null && ServerTabControl.Items.Contains(_nocTab))
            {
                ServerTabControl.SelectedItem = _nocTab;
                return;
            }

            // Create the landing page
            _landingPage = new LandingPage(_serverManager);
            _landingPage.ServerCardClicked += LandingPage_ServerCardClicked;

            // Create tab header with close button
            var headerPanel = new StackPanel { Orientation = Orientation.Horizontal };
            var headerText = new TextBlock
            {
                Text = "Overview",
                VerticalAlignment = VerticalAlignment.Center,
                FontWeight = FontWeights.SemiBold
            };
            var closeButton = new Button
            {
                Style = (Style)FindResource("TabCloseButton"),
                Tag = NocTabId
            };
            closeButton.Click += CloseTab_Click;
            headerPanel.Children.Add(headerText);
            headerPanel.Children.Add(closeButton);

            _nocTab = new TabItem
            {
                Header = headerPanel,
                Content = _landingPage,
                Tag = NocTabId
            };

            // Insert at the beginning
            ServerTabControl.Items.Insert(0, _nocTab);
            ServerTabControl.SelectedItem = _nocTab;
        }

        private void NocOverview_Click(object sender, RoutedEventArgs e)
        {
            OpenNocTab();
        }

        private void AlertsHistory_Click(object sender, RoutedEventArgs e)
        {
            OpenAlertsTab();
        }

        private void FinOps_Click(object sender, RoutedEventArgs e)
        {
            OpenFinOpsTab();
        }

        private void OpenAlertsTab()
        {
            if (_alertsTab != null && ServerTabControl.Items.Contains(_alertsTab))
            {
                ServerTabControl.SelectedItem = _alertsTab;
                _alertsHistoryContent?.RefreshAlerts();
                return;
            }

            _alertsHistoryContent = new AlertsHistoryContent();
            _alertsHistoryContent.MuteRuleService = _muteRuleService;
            _alertsHistoryContent.RemediationApplyService = _remediationApplyService;
            _alertsHistoryContent.AlertsDismissed += (_, _) => UpdateAlertBadge();

            var headerPanel = new StackPanel { Orientation = Orientation.Horizontal };
            var headerText = new TextBlock
            {
                Text = "Alert History",
                VerticalAlignment = VerticalAlignment.Center,
                FontWeight = FontWeights.SemiBold
            };
            var closeButton = new Button
            {
                Style = (Style)FindResource("TabCloseButton"),
                Tag = AlertsTabId
            };
            closeButton.Click += CloseTab_Click;
            headerPanel.Children.Add(headerText);
            headerPanel.Children.Add(closeButton);

            _alertsTab = new TabItem
            {
                Header = headerPanel,
                Content = _alertsHistoryContent,
                Tag = AlertsTabId
            };

            /* Insert after NOC tab if present, otherwise at position 0 */
            var insertIndex = _nocTab != null && ServerTabControl.Items.Contains(_nocTab) ? 1 : 0;
            ServerTabControl.Items.Insert(insertIndex, _alertsTab);
            ServerTabControl.SelectedItem = _alertsTab;

            _alertsHistoryContent.RefreshAlerts();
        }

        private void OpenFinOpsTab()
        {
            if (_finOpsTab != null && ServerTabControl.Items.Contains(_finOpsTab))
            {
                ServerTabControl.SelectedItem = _finOpsTab;
                _ = _finOpsContent?.RefreshDataAsync();
                return;
            }

            // Ensure at least one server is configured
            var servers = _serverManager.GetAllServers();
            if (servers.Count == 0)
            {
                MessageBox.Show("Add at least one server before opening FinOps.", "No Servers",
                    MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            _finOpsContent = new Controls.FinOpsContent();
            _finOpsContent.Initialize(_serverManager, _credentialService);

            var headerPanel = new StackPanel { Orientation = Orientation.Horizontal };
            var headerText = new TextBlock
            {
                Text = "FinOps",
                VerticalAlignment = VerticalAlignment.Center,
                FontWeight = FontWeights.SemiBold
            };
            var closeButton = new Button
            {
                Style = (Style)FindResource("TabCloseButton"),
                Tag = FinOpsTabId
            };
            closeButton.Click += CloseTab_Click;
            headerPanel.Children.Add(headerText);
            headerPanel.Children.Add(closeButton);

            _finOpsTab = new TabItem
            {
                Header = headerPanel,
                Content = _finOpsContent,
                Tag = FinOpsTabId
            };

            /* Insert after Alerts tab if present, else after NOC, else at 0 */
            var insertIndex = 0;
            if (_alertsTab != null && ServerTabControl.Items.Contains(_alertsTab))
                insertIndex = ServerTabControl.Items.IndexOf(_alertsTab) + 1;
            else if (_nocTab != null && ServerTabControl.Items.Contains(_nocTab))
                insertIndex = ServerTabControl.Items.IndexOf(_nocTab) + 1;

            ServerTabControl.Items.Insert(insertIndex, _finOpsTab);
            ServerTabControl.SelectedItem = _finOpsTab;
        }

        private void CloseTab_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.Tag is string tabId)
            {
                if (tabId == NocTabId)
                {
                    // Close the NOC tab
                    if (_nocTab != null)
                    {
                        ServerTabControl.Items.Remove(_nocTab);
                        _nocTab = null;
                        _landingPage = null;
                    }
                }
                else if (tabId == AlertsTabId)
                {
                    if (_alertsTab != null)
                    {
                        ServerTabControl.Items.Remove(_alertsTab);
                        _alertsTab = null;
                        _alertsHistoryContent?.Cleanup();
                        _alertsHistoryContent = null;
                    }
                }
                else if (tabId == FinOpsTabId)
                {
                    if (_finOpsTab != null)
                    {
                        ServerTabControl.Items.Remove(_finOpsTab);
                        _finOpsTab = null;
                        _finOpsContent = null;
                    }
                }
                else if (tabId == PlanViewerTabId)
                {
                    if (_planViewerTab != null)
                    {
                        // Each plan sub-tab's viewer is rooted by the static ThemeChanged event —
                        // Cleanup() each before discarding the tab control so none leak.
                        if (_mainPlanTabControl != null)
                            foreach (var item in _mainPlanTabControl.Items)
                                if (item is TabItem { Content: Grid g } && g.Children.Count > 1
                                    && g.Children[1] is PerformanceMonitor.Ui.PlanViewerControl pv)
                                    pv.Cleanup();
                        ServerTabControl.Items.Remove(_planViewerTab);
                        _planViewerTab = null;
                        _mainPlanTabControl = null;
                        // Re-arm the "+"-sentinel deferral latch for the next open (matches the shared
                        // controller's Reset(); a fresh _mainPlanTabControl is built by OpenPlanViewerTab).
                        _addTabInsertDeferred = false;
                    }
                }
                else if (_openTabs.TryGetValue(tabId, out var tabToClose))
                {
                    if (tabToClose.Content is ServerTab serverTab)
                    {
                        if (_alertAcknowledgedHandlers.TryGetValue(tabId, out var handler))
                        {
                            serverTab.AlertAcknowledged -= handler;
                            _alertAcknowledgedHandlers.Remove(tabId);
                        }
                        serverTab.CleanupOnClose();
                    }
                    _openTabs.Remove(tabId);
                    _tabBadges.Remove(tabId);
                    ServerTabControl.Items.Remove(tabToClose);
                }
            }
        }

        private void ServerTabControl_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Only respond to tab selection changes, not child control selection events that bubble up
            if (e.OriginalSource != ServerTabControl) return;

            /* Restore the selected tab's UTC offset so charts use the correct server timezone */
            if (ServerTabControl.SelectedItem is TabItem { Content: ServerTab serverTab })
            {
                Helpers.ServerTimeHelper.UtcOffsetMinutes = serverTab.UtcOffsetMinutes;
            }
        }

        private async void LandingPage_ServerCardClicked(object? sender, ServerConnection server)
        {
            await OpenServerTabAsync(server);
        }

        private async void AddServer_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new AddServerDialog();
            if (dialog.ShowDialog() == true)
            {
                var server = dialog.ServerConnection;
                var username = dialog.Username;
                var password = dialog.Password;

                try
                {
                    _serverManager.AddServer(server, username, password);
                    await LoadServerListAsync();

                    MessageBox.Show(
                        $"Server '{server.DisplayNameWithIntent}' added successfully!\n\n" +
                        (server.AuthenticationType == AuthenticationTypes.Windows ? "Using Windows Authentication" : $"Using {server.AuthenticationDisplay} — credentials saved securely to Windows Credential Manager"),
                        "Server Added",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                }
                catch (Exception ex)
                {
                    MessageBox.Show(
                        $"Failed to add server:\n\n{ex.Message}",
                        "Error Adding Server",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error
                    );
                }
            }
        }

        private async void EditServer_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                var server = item.Server;
                var dialog = new AddServerDialog(server);
                if (dialog.ShowDialog() == true)
                {
                    var updatedServer = dialog.ServerConnection;
                    var username = dialog.Username;
                    var password = dialog.Password;

                    try
                    {
                        _serverManager.UpdateServer(updatedServer, username, password);
                        await LoadServerListAsync();

                        if (_openTabs.TryGetValue(server.Id, out var tabItem))
                        {
                            if (tabItem.Header is StackPanel headerPanel &&
                                headerPanel.Children[0] is TextBlock headerText)
                            {
                                headerText.Text = updatedServer.ReadOnlyIntent ? $"{updatedServer.DisplayName} (RO)" : updatedServer.DisplayName;
                            }
                        }

                        MessageBox.Show(
                            $"Server '{updatedServer.DisplayNameWithIntent}' updated successfully!\n\n" +
                            (updatedServer.AuthenticationType == AuthenticationTypes.Windows ? "Using Windows Authentication" : $"Using {updatedServer.AuthenticationDisplay} — credentials updated securely in Windows Credential Manager"),
                            "Server Updated",
                            MessageBoxButton.OK,
                            MessageBoxImage.Information
                        );
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(
                            $"Failed to update server:\n\n{ex.Message}",
                            "Error Updating Server",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                }
            }
        }

        private async void CheckServerVersion_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is not ServerListItem item) return;
            var server = item.Server;

            try
            {
                string? installedVersion = await _serverManager.GetInstalledVersionAsync(server);

                if (installedVersion == null)
                {
                    MessageBox.Show(
                        $"No PerformanceMonitor installation found on '{server.DisplayNameWithIntent}'.",
                        "Not Installed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                    return;
                }

                string appVersion = Assembly.GetExecutingAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                    ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0";

                string normalizedInstalled = VersionText.Normalize(installedVersion);
                string normalizedApp = VersionText.Normalize(appVersion);

                Version? installed = VersionText.Parse(installedVersion);
                Version? app = VersionText.Parse(appVersion);
                if (installed != null && app != null && installed < app)
                {
                    var result = MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' has v{normalizedInstalled} installed.\n\nv{normalizedApp} is available. Open the server editor to upgrade?",
                        "Update Available",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Information);

                    if (result == MessageBoxResult.Yes)
                    {
                        var dialog = new AddServerDialog(server);
                        if (dialog.ShowDialog() == true)
                        {
                            _serverManager.UpdateServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                            await LoadServerListAsync();
                        }
                    }
                }
                else
                {
                    MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' is up to date (v{normalizedInstalled}).",
                        "No Updates",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to check version:\n\n{ex.Message}",
                    "Connection Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
        }

        private async void RemoveServer_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                var server = item.Server;
                var dialog = new RemoveServerDialog(server.DisplayNameWithIntent);
                dialog.Owner = this;

                if (dialog.ShowDialog() == true)
                {
                    // Drop the database first if requested (before we delete credentials)
                    if (dialog.DropDatabase)
                    {
                        try
                        {
                            await _serverManager.DropMonitorDatabaseAsync(server);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show(
                                $"Could not drop the PerformanceMonitor database on '{server.DisplayNameWithIntent}':\n\n{ex.Message}\n\nThe server will still be removed from the Dashboard.",
                                "Database Drop Failed",
                                MessageBoxButton.OK,
                                MessageBoxImage.Warning
                            );
                        }
                    }

                    if (_openTabs.TryGetValue(server.Id, out var tabItem))
                    {
                        if (tabItem.Content is ServerTab st) st.CleanupOnClose();
                        _openTabs.Remove(server.Id);
                        ServerTabControl.Items.Remove(tabItem);
                    }

                    // Clean up alert state and cached health for this server
                    _alertStateService.RemoveServerState(server.Id);
                    _latestHealthStatus.Remove(server.Id);

                    _serverManager.DeleteServer(server.Id);
                    await LoadServerListAsync();

                    MessageBox.Show(
                        $"Server '{server.DisplayNameWithIntent}' removed successfully!",
                        "Server Removed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                }
            }
        }

        private void ServerContextMenu_Opened(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                ToggleFavoriteMenuItem.Header = item.IsFavorite ? "Remove from Favorites" : "Set as Favorite";
            }
        }

        private async void ToggleFavorite_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                var server = item.Server;
                server.IsFavorite = !server.IsFavorite;
                _serverManager.UpdateServer(server, null, null);
                await LoadServerListAsync();
            }
        }

        private async void ManageServers_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new ManageServersWindow(_serverManager);
            dialog.Owner = this;

            if (dialog.ShowDialog() == true && dialog.ServersModified)
            {
                await LoadServerListAsync();
            }
        }

        private void Settings_Click(object sender, RoutedEventArgs e)
        {
            var oldPrefs = _preferencesService.GetPreferences();
            bool wasEnabled = oldPrefs.McpEnabled;
            int oldPort = oldPrefs.McpPort;

            var dialog = new SettingsWindow(_preferencesService, _muteRuleService);
            dialog.Owner = this;
            if (dialog.ShowDialog() == true)
            {
                ConfigureConnectionStatusTimer();
                ConfigureAlertCheckTimer();
                _analysisScheduler.Configure();
                _landingPage?.RefreshAutoRefreshSettings();

                foreach (TabItem tab in ServerTabControl.Items)
                {
                    if (tab.Content is ServerTab serverTab)
                    {
                        serverTab.RefreshAutoRefreshSettings();
                    }
                }

                RestartMcpServerIfNeeded(wasEnabled, oldPort);
            }
        }

        private void Help_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new AboutWindow();
            dialog.Owner = this;
            dialog.ShowDialog();
        }

        private void ViewLogButton_Click(object sender, RoutedEventArgs e)
        {
            var logFile = Logger.GetCurrentLogFile();
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
                        FileName = Logger.GetLogDirectory(),
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

        /// <summary>
        /// Exposes the AlertStateService for coordination with LandingPage.
        /// </summary>
        public AlertStateService AlertStateService => _alertStateService;

        /// <summary>
        /// Updates a server tab badge visibility based on health status.
        /// </summary>
        public void UpdateTabBadge(string serverId, ServerHealthStatus? status)
        {
            /* Fold the alert engine's per-server low-disk / failed-job state into the status so the
               Overview badge reflects them too (#754/#749). Both badge-update paths (the alert engine
               and the LandingPage refresh) funnel through here, so injecting once keeps them
               consistent — a path that lacks disk/job data can't blank the badge. */
            if (status != null)
            {
                status.HasLowDiskAlert = _activeLowDiskAlert.TryGetValue(serverId, out var ldActive) && ldActive;
                status.HasFailedJobAlert = _activeFailedJobAlert.TryGetValue(serverId, out var fjActive) && fjActive;
            }

            // Cache latest health status for acknowledge baseline snapshots
            if (status != null)
                _latestHealthStatus[serverId] = status;
            else
                _latestHealthStatus.Remove(serverId);

            if (_tabBadges.TryGetValue(serverId, out var badge))
            {
                var shouldShow = _alertStateService.ShouldShowBadge(serverId, "Overview", status);
                badge.Visibility = shouldShow ? Visibility.Visible : Visibility.Collapsed;

                // Use critical style for severe conditions
                if (shouldShow && status != null)
                {
                    var hasCritical = status.LongestBlockedSeconds >= 60
                                   || status.DeadlocksSinceLastCheck > 0
                                   || (status.TotalCpuPercent.HasValue && status.TotalCpuPercent.Value >= 95);

                    badge.Style = (Style)FindResource(hasCritical ? "AlertBadgeCritical" : "AlertBadge");
                }
            }
        }

        /// <summary>
        /// Updates all server tab badges with current health data from LandingPage.
        /// </summary>
        public void UpdateAllTabBadges(Dictionary<string, ServerHealthStatus> healthData)
        {
            foreach (var kvp in _tabBadges)
            {
                healthData.TryGetValue(kvp.Key, out var status);
                UpdateTabBadge(kvp.Key, status);
            }
        }

        /// <summary>
        /// Updates a server tab badge from AlertHealthResult (used by the alert engine).
        /// Constructs a minimal ServerHealthStatus for the badge evaluation.
        /// </summary>
        private void UpdateTabBadgeFromAlertHealth(string serverId, AlertHealthResult health, long prevDeadlockCount)
        {
            if (!_tabBadges.ContainsKey(serverId)) return;

            /* Build a minimal ServerHealthStatus with the fields ShouldShowBadge needs */
            var server = _serverManager.GetAllServers().FirstOrDefault(s => s.Id == serverId);
            if (server == null) return;

            var status = new ServerHealthStatus(server)
            {
                IsOnline = health.IsOnline,
                CpuPercent = health.CpuPercent,
                OtherCpuPercent = health.OtherCpuPercent,
                LongestBlockedSeconds = health.LongestBlockedSeconds,
                TotalBlocked = health.TotalBlocked
            };

            /* Set deadlock count twice to generate a delta.
               First set establishes baseline (delta=0), second set creates actual delta.
               Uses the previous count captured BEFORE EvaluateAlertConditionsAsync updated it. */
            status.DeadlockCount = prevDeadlockCount;
            status.DeadlockCount = health.DeadlockCount;

            UpdateTabBadge(serverId, status);

            /* Also update sub-tab badges (Locking, Memory, Resource Metrics) in open ServerTab instances */
            foreach (var tabItem in ServerTabControl.Items.OfType<TabItem>())
            {
                if (tabItem.Content is ServerTab serverTab && serverTab.ServerId == serverId)
                {
                    serverTab.UpdateBadges(status, _alertStateService);
                    break;
                }
            }
        }

        #region Alert Suppression Context Menu Handlers

        private void AcknowledgeServerAlerts_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
            {
                AcknowledgeServerAlerts(serverId);
            }
        }

        /// <summary>
        /// Acknowledges all alerts for a server and clears its tab badge (and sub-tab badges).
        /// Shared by the tab badge left-click and the right-click "Acknowledge Alerts" menu so both
        /// paths behave identically (issue #1092 — parity with the Lite app's clearable badge).
        /// </summary>
        private void AcknowledgeServerAlerts(string serverId)
        {
            // Look up cached health status for baseline snapshot
            _latestHealthStatus.TryGetValue(serverId, out var status);
            _alertStateService.AcknowledgeAllAlerts(serverId, status);

            // Hide badge immediately
            if (_tabBadges.TryGetValue(serverId, out var badge))
            {
                badge.Visibility = Visibility.Collapsed;
            }

            // Also update sub-tab badges in the ServerTab if it's open
            if (_openTabs.TryGetValue(serverId, out var tabItem) && tabItem.Content is ServerTab serverTab)
            {
                serverTab.UpdateBadges(null, _alertStateService);
            }

            // Hide alerts in the email alert log so the sidebar badge updates
            var server = _serverManager.GetAllServers().FirstOrDefault(s => s.Id == serverId);
            if (server != null)
            {
                _alertHistoryStore.HideAllAlerts(8760, server.DisplayNameWithIntent);
                UpdateAlertBadge();
                _alertsHistoryContent?.RefreshAlerts();
            }
        }

        private void SilenceServer_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
            {
                _alertStateService.SilenceServer(serverId);

                // Hide badge immediately
                if (_tabBadges.TryGetValue(serverId, out var badge))
                {
                    badge.Visibility = Visibility.Collapsed;
                }

                // Also update sub-tab badges in the ServerTab if it's open
                if (_openTabs.TryGetValue(serverId, out var tabItem) && tabItem.Content is ServerTab serverTab)
                {
                    serverTab.UpdateBadges(null, _alertStateService);
                }
            }
        }

        private void UnsilenceServer_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
            {
                _alertStateService.UnsilenceServer(serverId);
                _alertStateService.UnsilenceServerTab(serverId);
            }
        }

        #endregion
    }
}
