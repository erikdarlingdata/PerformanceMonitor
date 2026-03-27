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
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Controls;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;
using System.ComponentModel;
using System.Windows.Data;
using System.Xml.Linq;

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

        private McpHostService? _mcpHostService;
        private CancellationTokenSource? _mcpCts;

        // Independent alert engine - runs regardless of which tab is active
        private readonly DispatcherTimer _alertCheckTimer;
        private readonly EmailAlertService _emailAlertService;
        private readonly CredentialService _credentialService;
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
        private readonly ConcurrentDictionary<string, DateTime> _lastLongRunningJobAlert = new();
        private readonly ConcurrentDictionary<string, bool> _activeLongRunningJobAlert = new();
        private readonly ConcurrentDictionary<string, long> _previousDeadlockCounts = new();

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
            _muteRuleService = new MuteRuleService();
            _serverListItems = new ObservableCollection<ServerListItem>();
            _previousConnectionStates = new Dictionary<string, bool>();
            _tabBadges = new Dictionary<string, Border>();
            _latestHealthStatus = new Dictionary<string, ServerHealthStatus>();

            ServerListView.ItemsSource = _serverListItems;

            _credentialService = new CredentialService();
            _emailAlertService = new EmailAlertService(_preferencesService);
            _ = new WebhookAlertService(_preferencesService);

            _alertCheckTimer = new DispatcherTimer();
            _alertCheckTimer.Tick += AlertCheckTimer_Tick;

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
                // Another instance tried to start - bring this window to front
                Show();
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
            // Charts always render in server time; force the dropdown to match on startup
            // so the display isn't misleading. The preference is still saved when changed.
            Helpers.ServerTimeHelper.CurrentDisplayMode = Helpers.TimeDisplayMode.ServerTime;

            await LoadServerListAsync();
            InitializeNotificationService();
            OpenNocTab();
            OpenAlertsTab();
            ServerTabControl.SelectedItem = _nocTab; /* Keep Overview as the active tab */
            LoadSidebarState();
            ConfigureConnectionStatusTimer();
            ConfigureAlertCheckTimer();
            UpdateAlertBadge();
            StartMcpServerIfEnabled();

            _displayRefreshTimer.Start();

            await CheckAllConnectionsAsync();

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

            // Save alert history to disk
            _emailAlertService?.SaveAlertLog();

            // Clean up notification service
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

        private async void ConnectionStatusTimer_Tick(object? sender, EventArgs e)
        {
            await CheckAllConnectionsAsync();
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
                        if (wasOnline && !isOnline && prefs.NotifyOnConnectionLost)
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
                        else if (!wasOnline && isOnline && prefs.NotifyOnConnectionRestored)
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
                    $"Failed to open server tab for '{server.DisplayName}'.\n\n" +
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
            serverTab.AlertAcknowledged += (_, _) =>
            {
                _emailAlertService.HideAllAlerts(8760, server.DisplayName);
                UpdateAlertBadge();
                _alertsHistoryContent?.RefreshAlerts();
            };

            var headerPanel = new StackPanel { Orientation = Orientation.Horizontal };
            var headerText = new TextBlock
            {
                Text = server.DisplayName,
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
                Child = new TextBlock
                {
                    Text = "!",
                    FontWeight = FontWeights.Bold,
                    Foreground = Brushes.White
                }
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
                        ServerTabControl.Items.Remove(_planViewerTab);
                        _planViewerTab = null;
                        _mainPlanTabControl = null;
                    }
                }
                else if (_openTabs.TryGetValue(tabId, out var tabToClose))
                {
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
                        $"Server '{server.DisplayName}' added successfully!\n\n" +
                        (server.AuthenticationType == Models.AuthenticationTypes.Windows ? "Using Windows Authentication" : $"Using {server.AuthenticationDisplay} — credentials saved securely to Windows Credential Manager"),
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
                                headerText.Text = updatedServer.DisplayName;
                            }
                        }

                        MessageBox.Show(
                            $"Server '{updatedServer.DisplayName}' updated successfully!\n\n" +
                            (updatedServer.AuthenticationType == Models.AuthenticationTypes.Windows ? "Using Windows Authentication" : $"Using {updatedServer.AuthenticationDisplay} — credentials updated securely in Windows Credential Manager"),
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

        private async void RemoveServer_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                var server = item.Server;
                var dialog = new RemoveServerDialog(server.DisplayName);
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
                                $"Could not drop the PerformanceMonitor database on '{server.DisplayName}':\n\n{ex.Message}\n\nThe server will still be removed from the Dashboard.",
                                "Database Drop Failed",
                                MessageBoxButton.OK,
                                MessageBoxImage.Warning
                            );
                        }
                    }

                    if (_openTabs.TryGetValue(server.Id, out var tabItem))
                    {
                        _openTabs.Remove(server.Id);
                        ServerTabControl.Items.Remove(tabItem);
                    }

                    // Clean up alert state and cached health for this server
                    _alertStateService.RemoveServerState(server.Id);
                    _latestHealthStatus.Remove(server.Id);

                    _serverManager.DeleteServer(server.Id);
                    await LoadServerListAsync();

                    MessageBox.Show(
                        $"Server '{server.DisplayName}' removed successfully!",
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

        /// <summary>
        /// Exposes the AlertStateService for coordination with LandingPage.
        /// </summary>
        public AlertStateService AlertStateService => _alertStateService;

        /// <summary>
        /// Updates a server tab badge visibility based on health status.
        /// </summary>
        public void UpdateTabBadge(string serverId, ServerHealthStatus? status)
        {
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

        #region Independent Alert Engine

        private void ConfigureAlertCheckTimer()
        {
            var prefs = _preferencesService.GetPreferences();

            if (prefs.NotificationsEnabled)
            {
                // Use auto-refresh interval if configured, otherwise default to 60 seconds
                var intervalSeconds = (prefs.AutoRefreshEnabled && prefs.AutoRefreshIntervalSeconds > 0)
                    ? prefs.AutoRefreshIntervalSeconds
                    : 60;
                _alertCheckTimer.Interval = TimeSpan.FromSeconds(intervalSeconds);
                _alertCheckTimer.Start();
            }
            else
            {
                _alertCheckTimer.Stop();
            }
        }

        private async void AlertCheckTimer_Tick(object? sender, EventArgs e)
        {
            await CheckAllServerAlertsAsync();

            /* Auto-refresh alert history if the tab is open */
            _alertsHistoryContent?.RefreshAlerts();

            UpdateAlertBadge();
        }

        private void UpdateAlertBadge()
        {
            var alerts = _emailAlertService.GetAlertHistory(hoursBack: 24, limit: 100);
            var count = alerts.Count;

            if (count > 0)
            {
                AlertBadgeText.Text = count > 99 ? "99+" : count.ToString();
                AlertBadge.Visibility = Visibility.Visible;
            }
            else
            {
                AlertBadge.Visibility = Visibility.Collapsed;
            }
        }

        /// <summary>
        /// Checks all servers for alert conditions using lightweight queries.
        /// Runs independently of the LandingPage UI refresh.
        /// </summary>
        private async Task CheckAllServerAlertsAsync()
        {
            if (_notificationService == null) return;

            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotificationsEnabled) return;

            var servers = _serverManager.GetAllServers();
            var tasks = servers.Select(async server =>
            {
                try
                {
                    var connectionString = server.GetConnectionString(_credentialService);
                    var databaseService = new DatabaseService(connectionString);
                    var connStatus = _serverManager.GetConnectionStatus(server.Id);
                    var health = await databaseService.GetAlertHealthAsync(connStatus.SqlEngineEdition, prefs.LongRunningQueryThresholdMinutes, prefs.LongRunningJobMultiplier, prefs.LongRunningQueryMaxResults, prefs.LongRunningQueryExcludeSpServerDiagnostics, prefs.LongRunningQueryExcludeWaitFor, prefs.LongRunningQueryExcludeBackups, prefs.LongRunningQueryExcludeMiscWaits, prefs.AlertExcludedDatabases);

                    if (health.IsOnline)
                    {
                        /* Capture previous deadlock count BEFORE EvaluateAlertConditionsAsync updates it,
                           so the badge delta calculation sees the correct baseline. */
                        var prevDeadlockCount = _previousDeadlockCounts.TryGetValue(server.Id, out var pdc) ? pdc : 0;

                        await EvaluateAlertConditionsAsync(server.Id, server.DisplayName, health, databaseService);

                        /* Update tab badges from alert health data.
                           This ensures badges update even when the NOC view isn't active. */
                        await Dispatcher.InvokeAsync(() => UpdateTabBadgeFromAlertHealth(server.Id, health, prevDeadlockCount));
                    }
                }
                catch (Exception ex)
                {
                    Logger.Warning($"Alert check failed for {server.DisplayName}: {ex.Message}");
                }
            });

            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Evaluates alert conditions for a single server and fires notifications/emails.
        /// Uses cooldown tracking to prevent notification spam.
        /// </summary>
        private async Task EvaluateAlertConditionsAsync(
            string serverId, string serverName, AlertHealthResult health, DatabaseService databaseService)
        {
            var prefs = _preferencesService.GetPreferences();
            var alertCooldown = TimeSpan.FromMinutes(prefs.AlertCooldownMinutes);

            if (_alertStateService.IsAnySilencingActive(serverId))
            {
                return;
            }

            var now = ServerTimeHelper.ServerNow;

            /* Blocking alerts */
            bool blockingExceeded = prefs.NotifyOnBlocking
                && health.LongestBlockedSeconds >= prefs.BlockingThresholdSeconds;

            if (blockingExceeded)
            {
                _activeBlockingAlert[serverId] = true;
                if (!_lastBlockingAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Blocking Detected" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastBlockingAlert[serverId] = now;

                    var blockingContext = await BuildBlockingContextAsync(databaseService, prefs.AlertExcludedDatabases);
                    var detailText = ContextToDetailText(blockingContext)
                        ?? $"Blocked Sessions: {(int)health.TotalBlocked}\nLongest Wait: {(int)health.LongestBlockedSeconds}s";

                    if (!isMuted)
                    {
                        _notificationService?.ShowBlockingNotification(
                            serverName,
                            (int)health.TotalBlocked,
                            (int)health.LongestBlockedSeconds);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Blocking Detected",
                        $"{(int)health.TotalBlocked} session(s), longest {(int)health.LongestBlockedSeconds}s",
                        $"{prefs.BlockingThresholdSeconds}s", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Blocking Detected",
                            serverName,
                            $"{(int)health.TotalBlocked} session(s), longest {(int)health.LongestBlockedSeconds}s",
                            $"{prefs.BlockingThresholdSeconds}s",
                            serverId,
                            blockingContext);
                    }
                }
            }
            else if (_activeBlockingAlert.TryRemove(serverId, out var wasBlocking) && wasBlocking)
            {
                _notificationService?.ShowNotification("Blocking Cleared",
                    $"{serverName}: No active blocking");
                _emailAlertService.RecordAlert(serverId, serverName, "Blocking Cleared",
                    "0", $"{prefs.BlockingThresholdSeconds}s", true, "tray");
            }

            /* Deadlock alerts — independent delta tracking */
            long deadlockDelta = 0;
            if (_previousDeadlockCounts.TryGetValue(serverId, out var prevCount))
            {
                deadlockDelta = health.DeadlockCount - prevCount;
                if (deadlockDelta < 0) deadlockDelta = 0; // handle counter reset
            }
            _previousDeadlockCounts[serverId] = health.DeadlockCount;

            /* Use the database-filtered count when excluded databases are configured,
               matching how blocking alerts filter before the threshold check.
               Falls back to the raw delta when no databases are excluded. */
            var effectiveDeadlockDelta = health.FilteredDeadlockCount ?? deadlockDelta;

            bool deadlocksExceeded = prefs.NotifyOnDeadlock
                && effectiveDeadlockDelta >= prefs.DeadlockThreshold;

            if (deadlocksExceeded)
            {
                _activeDeadlockAlert[serverId] = true;
                if (!_lastDeadlockAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Deadlocks Detected" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastDeadlockAlert[serverId] = now;

                    var deadlockContext = await BuildDeadlockContextAsync(databaseService, prefs.AlertExcludedDatabases);
                    var detailText = ContextToDetailText(deadlockContext)
                        ?? $"New Deadlocks: {effectiveDeadlockDelta}";

                    if (!isMuted)
                    {
                        _notificationService?.ShowDeadlockNotification(
                            serverName,
                            (int)effectiveDeadlockDelta);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Deadlocks Detected",
                        effectiveDeadlockDelta.ToString(),
                        prefs.DeadlockThreshold.ToString(), !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Deadlocks Detected",
                            serverName,
                            effectiveDeadlockDelta.ToString(),
                            prefs.DeadlockThreshold.ToString(),
                            serverId,
                            deadlockContext);
                    }
                }
            }
            else if (_activeDeadlockAlert.TryRemove(serverId, out var wasDeadlock) && wasDeadlock)
            {
                _notificationService?.ShowNotification("Deadlocks Cleared",
                    $"{serverName}: No deadlocks since last check");
                _emailAlertService.RecordAlert(serverId, serverName, "Deadlocks Cleared",
                    "0", prefs.DeadlockThreshold.ToString(), true, "tray");
            }

            /* High CPU alerts */
            bool cpuExceeded = prefs.NotifyOnHighCpu
                && health.TotalCpuPercent.HasValue
                && health.TotalCpuPercent.Value >= prefs.CpuThresholdPercent;

            if (cpuExceeded)
            {
                var totalCpu = health.TotalCpuPercent!.Value;
                _activeHighCpuAlert[serverId] = true;
                if (!_lastHighCpuAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "High CPU" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastHighCpuAlert[serverId] = now;

                    if (!isMuted)
                    {
                        _notificationService?.ShowHighCpuNotification(
                            serverName,
                            totalCpu);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "High CPU",
                        $"{totalCpu:F0}%",
                        $"{prefs.CpuThresholdPercent}%", !isMuted, isMuted ? "muted" : "tray", muted: isMuted,
                        detailText: $"  CPU: {totalCpu:F0}%\n  Threshold: {prefs.CpuThresholdPercent}%");

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "High CPU",
                            serverName,
                            $"{totalCpu:F0}%",
                            $"{prefs.CpuThresholdPercent}%",
                            serverId);
                    }
                }
            }
            else if (_activeHighCpuAlert.TryRemove(serverId, out var wasCpu) && wasCpu)
            {
                var cpuText = health.TotalCpuPercent.HasValue ? $"{health.TotalCpuPercent.Value:F0}%" : "N/A";
                _notificationService?.ShowNotification("CPU Resolved",
                    $"{serverName}: CPU back to {cpuText}");
                _emailAlertService.RecordAlert(serverId, serverName, "CPU Resolved",
                    cpuText, $"{prefs.CpuThresholdPercent}%", true, "tray");
            }

            /* Poison wait alerts */
            var triggeredWaits = prefs.NotifyOnPoisonWaits
                ? health.PoisonWaits.FindAll(w => w.AvgMsPerWait >= prefs.PoisonWaitThresholdMs)
                : new List<PoisonWaitDelta>();

            if (triggeredWaits.Count > 0)
            {
                _activePoisonWaitAlert[serverId] = true;
                if (!_lastPoisonWaitAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var worst = triggeredWaits[0];
                    var allWaitNames = string.Join(", ", triggeredWaits.ConvertAll(w => $"{w.WaitType} ({w.AvgMsPerWait:F0}ms)"));

                    /* Poison wait mute check uses the worst (highest avg ms/wait) triggered wait type.
                       Limitation: if a user mutes a specific wait type that isn't the worst, the alert
                       still fires. Conversely, muting the worst type suppresses the entire alert even
                       if other unmuted poison waits are present. */
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Poison Wait", WaitType = worst.WaitType };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastPoisonWaitAlert[serverId] = now;
                    var poisonContext = BuildPoisonWaitContext(triggeredWaits);
                    var detailText = ContextToDetailText(poisonContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowPoisonWaitNotification(serverName, worst.WaitType, worst.AvgMsPerWait);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Poison Wait",
                        allWaitNames,
                        $"{prefs.PoisonWaitThresholdMs}ms avg", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Poison Wait",
                            serverName,
                            allWaitNames,
                            $"{prefs.PoisonWaitThresholdMs}ms avg",
                            serverId,
                            poisonContext);
                    }
                }
            }
            else if (_activePoisonWaitAlert.TryRemove(serverId, out var wasPoisonWait) && wasPoisonWait)
            {
                _notificationService?.ShowNotification("Poison Waits Cleared",
                    $"{serverName}: Poison wait avg below threshold");
                _emailAlertService.RecordAlert(serverId, serverName, "Poison Waits Cleared",
                    "0", $"{prefs.PoisonWaitThresholdMs}ms avg", true, "tray");
            }

            /* Long-running query alerts */
            var lrqList = health.LongRunningQueries;
            if (prefs.AlertExcludedDatabases.Count > 0)
                lrqList = lrqList
                    .Where(q => string.IsNullOrEmpty(q.DatabaseName) ||
                        !prefs.AlertExcludedDatabases.Any(e =>
                            string.Equals(e, q.DatabaseName, StringComparison.OrdinalIgnoreCase)))
                    .ToList();

            bool longRunningTriggered = prefs.NotifyOnLongRunningQueries
                && lrqList.Count > 0;

            if (longRunningTriggered)
            {
                _activeLongRunningQueryAlert[serverId] = true;
                if (!_lastLongRunningQueryAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var worst = lrqList[0];
                    var elapsedMinutes = worst.ElapsedSeconds / 60;
                    var preview = Truncate(worst.QueryText, 80);

                    var muteCtx = new AlertMuteContext
                    {
                        ServerName = serverName,
                        MetricName = "Long-Running Query",
                        DatabaseName = worst.DatabaseName,
                        QueryText = worst.QueryText
                    };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastLongRunningQueryAlert[serverId] = now;
                    var lrqContext = BuildLongRunningQueryContext(lrqList);
                    var detailText = ContextToDetailText(lrqContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowLongRunningQueryNotification(
                            serverName, worst.SessionId, elapsedMinutes, preview);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Query",
                        $"Session #{worst.SessionId} running {elapsedMinutes}m",
                        $"{prefs.LongRunningQueryThresholdMinutes}m", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Long-Running Query",
                            serverName,
                            $"{lrqList.Count} query(s), longest {elapsedMinutes}m",
                            $"{prefs.LongRunningQueryThresholdMinutes}m",
                            serverId,
                            lrqContext);
                    }
                }
            }
            else if (_activeLongRunningQueryAlert.TryRemove(serverId, out var wasLongRunning) && wasLongRunning)
            {
                _notificationService?.ShowNotification("Long-Running Queries Cleared",
                    $"{serverName}: No queries over threshold");
                _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Queries Cleared",
                    "0", $"{prefs.LongRunningQueryThresholdMinutes}m", true, "tray");
            }

            /* TempDB space alerts */
            bool tempDbExceeded = prefs.NotifyOnTempDbSpace
                && health.TempDbSpace != null
                && health.TempDbSpace.UsedPercent >= prefs.TempDbSpaceThresholdPercent;

            if (tempDbExceeded)
            {
                var tempDb = health.TempDbSpace!;
                _activeTempDbSpaceAlert[serverId] = true;
                if (!_lastTempDbSpaceAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "TempDB Space" };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastTempDbSpaceAlert[serverId] = now;
                    var tempDbContext = BuildTempDbSpaceContext(tempDb);
                    var detailText = ContextToDetailText(tempDbContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowTempDbSpaceNotification(serverName, tempDb.UsedPercent);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "TempDB Space",
                        $"{tempDb.UsedPercent:F0}% used ({tempDb.TotalReservedMb:F0} MB)",
                        $"{prefs.TempDbSpaceThresholdPercent}%", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "TempDB Space",
                            serverName,
                            $"{tempDb.UsedPercent:F0}% used ({tempDb.TotalReservedMb:F0} MB)",
                            $"{prefs.TempDbSpaceThresholdPercent}%",
                            serverId,
                            tempDbContext);
                    }
                }
            }
            else if (_activeTempDbSpaceAlert.TryRemove(serverId, out var wasTempDb) && wasTempDb)
            {
                var pct = health.TempDbSpace != null ? $"{health.TempDbSpace.UsedPercent:F0}%" : "N/A";
                _notificationService?.ShowNotification("TempDB Space Resolved",
                    $"{serverName}: TempDB usage back to {pct}");
                _emailAlertService.RecordAlert(serverId, serverName, "TempDB Space Resolved",
                    pct, $"{prefs.TempDbSpaceThresholdPercent}%", true, "tray");
            }

            /* Anomalous Agent job alerts */
            bool anomalousJobsTriggered = prefs.NotifyOnLongRunningJobs
                && health.AnomalousJobs.Count > 0;

            if (anomalousJobsTriggered)
            {
                _activeLongRunningJobAlert[serverId] = true;
                var worst = health.AnomalousJobs[0];
                var jobKey = $"{serverId}:{worst.JobId}:{worst.StartTime:O}";

                if (!_lastLongRunningJobAlert.TryGetValue(jobKey, out var lastAlert) || (now - lastAlert) >= alertCooldown)
                {
                    var currentMinutes = worst.CurrentDurationSeconds / 60;

                    var muteCtx = new AlertMuteContext { ServerName = serverName, MetricName = "Long-Running Job", JobName = worst.JobName };
                    bool isMuted = _muteRuleService.IsAlertMuted(muteCtx);
                    _lastLongRunningJobAlert[jobKey] = now;
                    var jobContext = BuildAnomalousJobContext(health.AnomalousJobs);
                    var detailText = ContextToDetailText(jobContext);

                    if (!isMuted)
                    {
                        _notificationService?.ShowLongRunningJobNotification(
                            serverName, worst.JobName, currentMinutes, worst.PercentOfAverage ?? 0);
                    }

                    _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Job",
                        $"{worst.JobName} at {worst.PercentOfAverage:F0}% of avg ({currentMinutes}m)",
                        $"{prefs.LongRunningJobMultiplier}x avg", !isMuted, isMuted ? "muted" : "tray", muted: isMuted, detailText: detailText);

                    if (!isMuted)
                    {
                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Long-Running Job",
                            serverName,
                            $"{health.AnomalousJobs.Count} job(s) exceeding {prefs.LongRunningJobMultiplier}x average",
                            $"{prefs.LongRunningJobMultiplier}x historical avg",
                            serverId,
                            jobContext);
                    }
                }
            }
            else if (_activeLongRunningJobAlert.TryRemove(serverId, out var wasJob) && wasJob)
            {
                _notificationService?.ShowNotification("Long-Running Jobs Cleared",
                    $"{serverName}: No jobs exceeding threshold");
                _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Jobs Cleared",
                    "0", $"{prefs.LongRunningJobMultiplier}x avg", true, "tray");
            }
        }

        private static string Truncate(string text, int maxLength = 300)
        {
            if (string.IsNullOrEmpty(text)) return "";
            text = text.Replace('\r', ' ').Replace('\n', ' ').Trim();
            return text.Length <= maxLength ? text : text.Substring(0, maxLength) + "...";
        }

        private static string? ContextToDetailText(AlertContext? context)
        {
            if (context == null || context.Details.Count == 0) return null;
            var sb = new System.Text.StringBuilder();
            foreach (var detail in context.Details)
            {
                if (sb.Length > 0) sb.AppendLine();
                sb.AppendLine(detail.Heading);
                foreach (var (label, value) in detail.Fields)
                    sb.AppendLine($"  {label}: {value}");
            }
            return sb.ToString().TrimEnd();
        }

        private static async Task<AlertContext?> BuildBlockingContextAsync(DatabaseService databaseService, List<string>? excludedDatabases = null)
        {
            try
            {
                var events = await databaseService.GetBlockingEventsAsync(hoursBack: 1);
                if (events == null || events.Count == 0) return null;

                if (excludedDatabases != null && excludedDatabases.Count > 0)
                {
                    events = events
                        .Where(e => string.IsNullOrEmpty(e.DatabaseName) ||
                            !excludedDatabases.Any(ex =>
                                string.Equals(ex, e.DatabaseName, StringComparison.OrdinalIgnoreCase)))
                        .ToList();
                    if (events.Count == 0) return null;
                }

                var context = new AlertContext();
                var firstXml = (string?)null;

                foreach (var e in events.GetRange(0, Math.Min(3, events.Count)))
                {
                    var item = new AlertDetailItem
                    {
                        Heading = $"Session #{e.Spid}",
                        Fields = new()
                    };

                    if (!string.IsNullOrEmpty(e.DatabaseName))
                        item.Fields.Add(("Database", e.DatabaseName));
                    if (!string.IsNullOrEmpty(e.QueryText))
                        item.Fields.Add(("Query", Truncate(e.QueryText)));
                    if (e.WaitTimeMs.HasValue)
                        item.Fields.Add(("Wait Time", $"{e.WaitTimeMs:N0} ms"));
                    if (!string.IsNullOrEmpty(e.LockMode))
                        item.Fields.Add(("Lock Mode", e.LockMode));
                    if (!string.IsNullOrEmpty(e.ClientApp))
                        item.Fields.Add(("Client App", e.ClientApp));

                    context.Details.Add(item);
                    firstXml ??= e.BlockedProcessReportXml;
                }

                if (!string.IsNullOrEmpty(firstXml))
                {
                    context.AttachmentXml = firstXml;
                    context.AttachmentFileName = "blocked_process_report.xml";
                }

                return context;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to fetch blocking detail for email: {ex.Message}");
                return null;
            }
        }

        private static async Task<AlertContext?> BuildDeadlockContextAsync(DatabaseService databaseService, List<string>? excludedDatabases = null)
        {
            try
            {
                var deadlocks = await databaseService.GetDeadlocksAsync(hoursBack: 1);
                if (deadlocks == null || deadlocks.Count == 0) return null;

                if (excludedDatabases != null && excludedDatabases.Count > 0)
                {
                    deadlocks = deadlocks
                        .Where(d => !IsDeadlockExcluded(d, excludedDatabases))
                        .ToList();
                    if (deadlocks.Count == 0) return null;
                }

                var context = new AlertContext();
                var firstGraph = (string?)null;

                // Group participants by deadlock event so victim + survivor are shown together
                var deadlockEvents = deadlocks
                    .GroupBy(d => d.EventDate)
                    .Take(3);

                foreach (var deadlockEvent in deadlockEvents)
                {
                    foreach (var d in deadlockEvent)
                    {
                        var role = string.Equals(d.DeadlockGroup, "victim", StringComparison.OrdinalIgnoreCase)
                            ? "victim" : "survivor";
                        var heading = $"Deadlock — Session #{d.Spid} ({role})";

                        var item = new AlertDetailItem
                        {
                            Heading = heading,
                            Fields = new()
                        };

                        if (!string.IsNullOrEmpty(d.DatabaseName))
                            item.Fields.Add(("Database", d.DatabaseName));
                        if (!string.IsNullOrEmpty(d.Query))
                            item.Fields.Add(("Query", Truncate(d.Query)));
                        if (!string.IsNullOrEmpty(d.WaitResource))
                            item.Fields.Add(("Wait Resource", d.WaitResource));
                        if (!string.IsNullOrEmpty(d.LockMode))
                            item.Fields.Add(("Lock Mode", d.LockMode));
                        if (!string.IsNullOrEmpty(d.ClientApp))
                            item.Fields.Add(("Client App", d.ClientApp));

                        context.Details.Add(item);
                        firstGraph ??= d.DeadlockGraph;
                    }
                }

                if (!string.IsNullOrEmpty(firstGraph))
                {
                    context.AttachmentXml = firstGraph;
                    context.AttachmentFileName = "deadlock_graph.xml";
                }

                return context;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to fetch deadlock detail for email: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Returns true if a deadlock should be excluded based on the deadlock graph XML.
        /// A deadlock is only excluded when ALL process nodes have a currentdbname in the excluded list.
        /// Cross-database deadlocks involving any non-excluded database will still be reported.
        /// </summary>
        private static bool IsDeadlockExcluded(DeadlockItem deadlock, List<string> excludedDatabases)
        {
            if (string.IsNullOrEmpty(deadlock.DeadlockGraph)) return false;
            try
            {
                var doc = XElement.Parse(deadlock.DeadlockGraph);
                var dbNames = doc.Descendants("process")
                    .Select(p => p.Attribute("currentdbname")?.Value)
                    .Where(n => !string.IsNullOrEmpty(n))
                    .Cast<string>()
                    .ToList();
                if (dbNames.Count == 0) return false;
                return dbNames.All(db => excludedDatabases.Any(e =>
                    string.Equals(e, db, StringComparison.OrdinalIgnoreCase)));
            }
            catch { return false; }
        }

        private static AlertContext? BuildPoisonWaitContext(List<PoisonWaitDelta> triggeredWaits)
        {
            if (triggeredWaits.Count == 0) return null;

            var context = new AlertContext();
            foreach (var w in triggeredWaits)
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = w.WaitType,
                    Fields = new()
                    {
                        ("Avg ms/wait", $"{w.AvgMsPerWait:F1}"),
                        ("Delta wait ms", $"{w.DeltaMs:N0}"),
                        ("Delta tasks", $"{w.DeltaTasks:N0}")
                    }
                });
            }
            return context;
        }

        private static AlertContext? BuildLongRunningQueryContext(List<LongRunningQueryInfo> queries)
        {
            if (queries.Count == 0) return null;

            var context = new AlertContext();
            foreach (var q in queries.GetRange(0, Math.Min(3, queries.Count)))
            {
                var item = new AlertDetailItem
                {
                    Heading = $"Session #{q.SessionId} — {q.ElapsedSeconds / 60}m {q.ElapsedSeconds % 60}s",
                    Fields = new()
                };

                if (!string.IsNullOrEmpty(q.DatabaseName))
                    item.Fields.Add(("Database", q.DatabaseName));
                if (!string.IsNullOrEmpty(q.ProgramName))
                    item.Fields.Add(("Program", q.ProgramName));
                if (!string.IsNullOrEmpty(q.QueryText))
                    item.Fields.Add(("Query", Truncate(q.QueryText)));
                item.Fields.Add(("CPU Time", $"{q.CpuTimeMs:N0} ms"));
                item.Fields.Add(("Reads", $"{q.Reads:N0}"));
                item.Fields.Add(("Writes", $"{q.Writes:N0}"));
                if (!string.IsNullOrEmpty(q.WaitType))
                    item.Fields.Add(("Wait Type", q.WaitType));
                if (q.BlockingSessionId.HasValue && q.BlockingSessionId.Value > 0)
                    item.Fields.Add(("Blocked By", $"Session #{q.BlockingSessionId.Value}"));

                context.Details.Add(item);
            }
            return context;
        }

        private static AlertContext? BuildAnomalousJobContext(List<AnomalousJobInfo> jobs)
        {
            if (jobs.Count == 0) return null;

            var context = new AlertContext();
            foreach (var j in jobs.GetRange(0, Math.Min(3, jobs.Count)))
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = j.JobName,
                    Fields = new()
                    {
                        ("Current Duration", FormatDuration(j.CurrentDurationSeconds)),
                        ("Avg Duration", FormatDuration(j.AvgDurationSeconds)),
                        ("P95 Duration", FormatDuration(j.P95DurationSeconds)),
                        ("% of Average", j.PercentOfAverage.HasValue ? $"{j.PercentOfAverage:F0}%" : "N/A"),
                        ("Started", j.StartTime.ToString("yyyy-MM-dd HH:mm:ss"))
                    }
                });
            }
            return context;
        }

        private static string FormatDuration(long seconds)
        {
            if (seconds < 60) return $"{seconds}s";
            if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
            return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
        }

        private static AlertContext? BuildTempDbSpaceContext(TempDbSpaceInfo tempDb)
        {
            var context = new AlertContext();
            context.Details.Add(new AlertDetailItem
            {
                Heading = $"TempDB — {tempDb.UsedPercent:F0}% Used",
                Fields = new()
                {
                    ("Total Reserved", $"{tempDb.TotalReservedMb:F0} MB"),
                    ("Unallocated", $"{tempDb.UnallocatedMb:F0} MB"),
                    ("User Objects", $"{tempDb.UserObjectReservedMb:F0} MB"),
                    ("Internal Objects", $"{tempDb.InternalObjectReservedMb:F0} MB"),
                    ("Version Store", $"{tempDb.VersionStoreReservedMb:F0} MB"),
                    ("Top Consumer", tempDb.TopConsumerSessionId > 0
                        ? $"Session #{tempDb.TopConsumerSessionId} ({tempDb.TopConsumerMb:F0} MB)"
                        : "None")
                }
            });
            return context;
        }

        #endregion

        #region Alert Suppression Context Menu Handlers

        private void AcknowledgeServerAlerts_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
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
                    _emailAlertService.HideAllAlerts(8760, server.DisplayName);
                    UpdateAlertBadge();
                    _alertsHistoryContent?.RefreshAlerts();
                }
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

        #region Main Window Plan Viewer

        private const string PlanAddTabId = "__PLAN_ADD_TAB__";
        private TabControl? _mainPlanTabControl;
        private Grid? _planViewerContainer;

        private void OpenPlanViewer_Click(object sender, RoutedEventArgs e)
        {
            if (_planViewerTab != null && ServerTabControl.Items.Contains(_planViewerTab))
            {
                AddNewEmptyPlanSubTab();
                ServerTabControl.SelectedItem = _planViewerTab;
                Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() => _planViewerContainer?.Focus()));
                return;
            }
            OpenPlanViewerTab();
        }

        private void OpenPlanViewerTab()
        {
            if (_planViewerTab != null && ServerTabControl.Items.Contains(_planViewerTab))
            {
                ServerTabControl.SelectedItem = _planViewerTab;
                return;
            }

            _mainPlanTabControl = new TabControl
            {
                Background = System.Windows.Media.Brushes.Transparent,
                BorderThickness = new Thickness(0)
            };

            // "+" tab at the end of the inner strip
            var addTabHeader = new TextBlock
            {
                Text = "+",
                FontSize = 14,
                FontWeight = FontWeights.Bold,
                VerticalAlignment = VerticalAlignment.Center,
                ToolTip = "Open a new plan sub-tab"
            };
            var addTab = new TabItem
            {
                Header = addTabHeader,
                Tag = PlanAddTabId,
                Content = new Grid() // no content needed
            };
            _mainPlanTabControl.Items.Add(addTab);

            _mainPlanTabControl.SelectionChanged += (_, _) =>
            {
                if (_mainPlanTabControl.SelectedItem is TabItem { Tag: string t } && t == PlanAddTabId)
                {
                    var newSub = AddNewEmptyPlanSubTab();
                    _mainPlanTabControl.SelectedItem = newSub;
                }
            };

            var container = new Grid();
            container.AllowDrop = true;
            container.Focusable = true;
            container.DragOver += MainWindowPlanViewer_DragOver;
            container.Drop += MainWindowPlanViewer_Drop;
            container.KeyDown += MainWindowPlanViewer_KeyDown;
            container.Children.Add(_mainPlanTabControl);
            _planViewerContainer = container;

            var header = new StackPanel { Orientation = Orientation.Horizontal };
            header.Children.Add(new TextBlock
            {
                Text = "Plan Viewer",
                VerticalAlignment = VerticalAlignment.Center,
                FontWeight = FontWeights.SemiBold
            });
            var closeBtn = new Button
            {
                Style = (Style)FindResource("TabCloseButton"),
                Tag = PlanViewerTabId
            };
            closeBtn.Click += CloseTab_Click;
            header.Children.Add(closeBtn);

            _planViewerTab = new TabItem
            {
                Header = header,
                Content = container,
                Tag = PlanViewerTabId
            };

            ServerTabControl.Items.Add(_planViewerTab);
            ServerTabControl.SelectedItem = _planViewerTab;

            // Open the first empty sub-tab immediately
            AddNewEmptyPlanSubTab();
            Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() => _planViewerContainer?.Focus()));
        }

        /// <summary>
        /// Adds a new empty "New Plan" sub-tab to the inner plan TabControl and selects it.
        /// Returns the newly created sub-tab.
        /// </summary>
        private TabItem AddNewEmptyPlanSubTab()
        {
            // --- Empty state layer ---
            var emptyState = new Grid();
            var dashedRect = new System.Windows.Shapes.Rectangle
            {
                Margin = new Thickness(24),
                Stroke = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                StrokeThickness = 1.5,
                StrokeDashArray = new System.Windows.Media.DoubleCollection { 6, 4 },
                RadiusX = 10, RadiusY = 10,
                Opacity = 0.25
            };
            var emptyStack = new StackPanel { HorizontalAlignment = HorizontalAlignment.Center, VerticalAlignment = VerticalAlignment.Center };
            emptyStack.Children.Add(new TextBlock
            {
                Text = "\uE896",
                FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                FontSize = 52,
                HorizontalAlignment = HorizontalAlignment.Center,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                Opacity = 0.45,
                Margin = new Thickness(0, 0, 0, 12)
            });
            emptyStack.Children.Add(new TextBlock
            {
                Text = "New Plan",
                FontSize = 20,
                FontWeight = FontWeights.Light,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                HorizontalAlignment = HorizontalAlignment.Center
            });
            emptyStack.Children.Add(new TextBlock
            {
                Text = "Open or paste execution plan XML to render it",
                FontSize = 13,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 8, 0, 0)
            });
            var btnPanel = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Center, Margin = new Thickness(0, 20, 0, 0) };
            var openBtn = new Button { Content = "Open .sqlplan File", Height = 28, Padding = new Thickness(12, 0, 12, 0), ToolTip = "Open a .sqlplan or .xml file from disk" };
            var pasteBtn = new Button { Content = "Paste XML", Height = 28, Padding = new Thickness(12, 0, 12, 0), Margin = new Thickness(8, 0, 0, 0), ToolTip = "Paste execution plan XML to render it (or use Ctrl+V)" };
            btnPanel.Children.Add(openBtn);
            btnPanel.Children.Add(pasteBtn);
            emptyStack.Children.Add(btnPanel);
            emptyStack.Children.Add(new TextBlock
            {
                Text = "or drag & drop a .sqlplan file anywhere in this area",
                FontSize = 11,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 12, 0, 0)
            });
            emptyState.Children.Add(dashedRect);
            emptyState.Children.Add(emptyStack);

            // --- Viewer layer (hidden until a plan is loaded) ---
            var viewer = new Controls.PlanViewerControl
            {
                Visibility = Visibility.Collapsed
            };

            // Sub-tab content grid: index 0 = emptyState, index 1 = viewer
            var subTabContent = new Grid();
            subTabContent.Children.Add(emptyState);
            subTabContent.Children.Add(viewer);

            // --- Sub-tab header: label + close button ---
            var initialLabel = GetUniqueSubTabLabel("New Plan");
            var labelBlock = new TextBlock
            {
                Text = initialLabel,
                VerticalAlignment = VerticalAlignment.Center,
                ToolTip = initialLabel
            };
            var subCloseBtn = new Button { Style = (Style)FindResource("TabCloseButton") };
            var subTabHeader = new StackPanel { Orientation = Orientation.Horizontal };
            subTabHeader.Children.Add(labelBlock);
            subTabHeader.Children.Add(subCloseBtn);

            var subTab = new TabItem { Header = subTabHeader, Content = subTabContent };

            subCloseBtn.Tag = subTab;
            subCloseBtn.Click += (_, _) =>
            {
                _mainPlanTabControl!.Items.Remove(subTab);
                // If only the "+" tab remains, re-open a fresh empty sub-tab
                if (_mainPlanTabControl.Items.Count == 1 &&
                    _mainPlanTabControl.Items[0] is TabItem { Tag: string t2 } && t2 == PlanAddTabId)
                {
                    AddNewEmptyPlanSubTab();
                }
            };

            // Wire per-sub-tab buttons with closures over this sub-tab
            openBtn.Click += (_, _) =>
            {
                var dialog = new Microsoft.Win32.OpenFileDialog
                {
                    Filter = "SQL Plan Files (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                    DefaultExt = ".sqlplan",
                    Multiselect = true
                };
                if (dialog.ShowDialog() != true) return;
                var isFirst = true;
                foreach (var fileName in dialog.FileNames)
                {
                    try
                    {
                        var xml = System.IO.File.ReadAllText(fileName);
                        var targetTab = isFirst ? subTab : AddNewEmptyPlanSubTab();
                        LoadPlanIntoSubTab(targetTab, xml, System.IO.Path.GetFileName(fileName));
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show($"Failed to open file:\n\n{ex.Message}", "Error",
                            MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                    isFirst = false;
                }
            };

            pasteBtn.Click += (_, _) =>
            {
                var xml = Clipboard.GetText();
                if (!string.IsNullOrWhiteSpace(xml))
                {
                    LoadPlanIntoSubTab(subTab, xml, "Pasted Plan");
                    return;
                }
                MessageBox.Show("The clipboard does not contain any text.", "Paste Plan XML",
                    MessageBoxButton.OK, MessageBoxImage.Information);
            };

            // Insert before the "+" tab
            var addTabIndex = -1;
            for (var i = 0; i < _mainPlanTabControl!.Items.Count; i++)
            {
                if (_mainPlanTabControl.Items[i] is TabItem { Tag: string t3 } && t3 == PlanAddTabId)
                {
                    addTabIndex = i;
                    break;
                }
            }
            if (addTabIndex >= 0)
                _mainPlanTabControl.Items.Insert(addTabIndex, subTab);
            else
                _mainPlanTabControl.Items.Add(subTab);

            _mainPlanTabControl.SelectedItem = subTab;
            return subTab;
        }

        /// <summary>
        /// Loads plan XML into an existing sub-tab (replacing whatever was there before).
        /// Updates the sub-tab header label and shows the viewer layer.
        /// </summary>
        private void LoadPlanIntoSubTab(TabItem subTab, string planXml, string label, string? queryText = null)
        {
            try { System.Xml.Linq.XDocument.Parse(planXml); }
            catch (System.Xml.XmlException ex)
            {
                MessageBox.Show(
                    $"The plan XML is not valid:\n\n{ex.Message}",
                    "Invalid Plan XML",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
                return;
            }

            if (subTab.Content is not Grid subTabContent) return;
            if (subTabContent.Children.Count < 2) return;

            var emptyState = subTabContent.Children[0] as FrameworkElement;
            var viewer = subTabContent.Children[1] as Controls.PlanViewerControl;
            if (viewer == null) return;

            viewer.LoadPlan(planXml, label, queryText);
            emptyState!.Visibility = Visibility.Collapsed;
            viewer.Visibility = Visibility.Visible;

            // Update header label (unique)
            var uniqueLabel = GetUniqueSubTabLabel(label);
            if (subTab.Header is StackPanel headerPanel &&
                headerPanel.Children[0] is TextBlock headerLabel)
            {
                headerLabel.Text = uniqueLabel.Length > 30 ? uniqueLabel[..30] + "\u2026" : uniqueLabel;
                headerLabel.ToolTip = uniqueLabel;
            }
        }

        /// <summary>
        /// Returns a label that is unique among current inner plan sub-tab headers.
        /// If <paramref name="baseLabel"/> is already taken, appends " (1)", " (2)", …
        /// </summary>
        private string GetUniqueSubTabLabel(string baseLabel)
        {
            var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in _mainPlanTabControl!.Items)
            {
                if (item is TabItem { Tag: string t } && t == PlanAddTabId) continue;
                if (item is TabItem subTab &&
                    subTab.Header is StackPanel sp &&
                    sp.Children[0] is TextBlock tb)
                    existing.Add(tb.ToolTip as string ?? tb.Text);
            }
            if (!existing.Contains(baseLabel)) return baseLabel;
            var counter = 1;
            string candidate;
            do { candidate = $"{baseLabel} ({counter++})"; }
            while (existing.Contains(candidate));
            return candidate;
        }

        /// <summary>
        /// Returns the currently active real plan sub-tab (skips the "+" tab).
        /// </summary>
        private TabItem? GetActivePlanSubTab()
        {
            if (_mainPlanTabControl == null) return null;
            if (_mainPlanTabControl.SelectedItem is TabItem { Tag: string t } && t == PlanAddTabId)
                return null;
            return _mainPlanTabControl.SelectedItem as TabItem;
        }

        private void MainWindowPlanViewer_DragOver(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop))
            {
                var files = e.Data.GetData(DataFormats.FileDrop) as string[];
                if (files?.Any(IsPlanFile) == true)
                {
                    e.Effects = DragDropEffects.Copy;
                    e.Handled = true;
                    return;
                }
            }
            e.Effects = DragDropEffects.None;
            e.Handled = true;
        }

        private void MainWindowPlanViewer_Drop(object sender, DragEventArgs e)
        {
            if (!e.Data.GetDataPresent(DataFormats.FileDrop)) return;
            var planFiles = (e.Data.GetData(DataFormats.FileDrop) as string[])
                ?.Where(IsPlanFile).ToArray();
            if (planFiles == null || planFiles.Length == 0) return;
            LoadMainWindowPlanFromFileIntoActiveTab(planFiles[0]);
            for (var i = 1; i < planFiles.Length; i++)
            {
                var newTab = AddNewEmptyPlanSubTab();
                try
                {
                    var xml = System.IO.File.ReadAllText(planFiles[i]);
                    LoadPlanIntoSubTab(newTab, xml, System.IO.Path.GetFileName(planFiles[i]));
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to open file:\n\n{ex.Message}", "Error",
                        MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private void MainWindowPlanViewer_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
        {
            if (e.Key == System.Windows.Input.Key.V &&
                System.Windows.Input.Keyboard.Modifiers == System.Windows.Input.ModifierKeys.Control &&
                e.OriginalSource is not System.Windows.Controls.TextBox)
            {
                var xml = Clipboard.GetText();
                if (!string.IsNullOrWhiteSpace(xml))
                {
                    e.Handled = true;
                    LoadPlanIntoActivePlanSubTab(xml, "Pasted Plan");
                }
            }
        }

        private void LoadMainWindowPlanFromFileIntoActiveTab(string path)
        {
            try
            {
                var xml = System.IO.File.ReadAllText(path);
                LoadPlanIntoActivePlanSubTab(xml, System.IO.Path.GetFileName(path));
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to open file:\n\n{ex.Message}", "Error",
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void LoadPlanIntoActivePlanSubTab(string planXml, string label)
        {
            var activeSubTab = GetActivePlanSubTab();
            if (activeSubTab != null)
                LoadPlanIntoSubTab(activeSubTab, planXml, label);
        }

        private static bool IsPlanFile(string path)
        {
            var ext = System.IO.Path.GetExtension(path);
            return string.Equals(ext, ".sqlplan", StringComparison.OrdinalIgnoreCase)
                || string.Equals(ext, ".xml", StringComparison.OrdinalIgnoreCase);
        }

        #endregion
    }
}
