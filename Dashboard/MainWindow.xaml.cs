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
        private readonly Dictionary<string, bool> _previousConnectionStates;
        private readonly Dictionary<string, Border> _tabBadges;
        private readonly Dictionary<string, ServerHealthStatus> _latestHealthStatus;
        private bool _sidebarCollapsed = false;
        private bool _isReallyClosing = false;
        private TabItem? _nocTab;
        private LandingPage? _landingPage;
        private TabItem? _alertsTab;
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
        private static readonly TimeSpan AlertCooldown = TimeSpan.FromMinutes(5);
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

        public MainWindow()
        {
            InitializeComponent();

            _serverManager = new ServerManager();
            _openTabs = new Dictionary<string, TabItem>();
            _preferencesService = new UserPreferencesService();
            _alertStateService = new AlertStateService();
            _serverListItems = new ObservableCollection<ServerListItem>();
            _previousConnectionStates = new Dictionary<string, bool>();
            _tabBadges = new Dictionary<string, Border>();
            _latestHealthStatus = new Dictionary<string, ServerHealthStatus>();

            ServerListView.ItemsSource = _serverListItems;

            _credentialService = new CredentialService();
            _emailAlertService = new EmailAlertService(_preferencesService);

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
            // Sync CSV separator from preferences
            var startupPrefs = _preferencesService.GetPreferences();
            TabHelpers.CsvSeparator = startupPrefs.CsvSeparator;

            LoadServerList();
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
                var prefs = _preferencesService.GetPreferences();
                if (!prefs.CheckForUpdatesOnStartup) return;

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

        private void StartMcpServerIfEnabled()
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.McpEnabled)
            {
                return;
            }

            try
            {
                _mcpHostService = new McpHostService(_serverManager, _credentialService, prefs.McpPort);
                _mcpCts = new CancellationTokenSource();
                _ = _mcpHostService.StartAsync(_mcpCts.Token);
            }
            catch (Exception ex)
            {
                Logger.Error($"[MCP] Failed to start MCP server: {ex.Message}", ex);
            }
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
            if (_mcpHostService != null)
            {
                try
                {
                    _mcpCts?.Cancel();
                    Task.Run(() => _mcpHostService.StopAsync(CancellationToken.None)).Wait(TimeSpan.FromSeconds(5));
                }
                catch (Exception ex)
                {
                    Logger.Error($"[MCP] Error stopping MCP server: {ex.Message}", ex);
                }
            }

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

        private void LoadServerList()
        {
            var servers = _serverManager.GetAllServers();

            _serverListItems.Clear();
            foreach (var server in servers)
            {
                var status = _serverManager.GetConnectionStatus(server.Id);
                _serverListItems.Add(new ServerListItem(server, status));
            }

            // Also refresh the landing page if it exists
            if (_landingPage != null)
            {
                _ = _landingPage.ReloadServersAsync();
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
                        }
                        else if (!wasOnline && isOnline && prefs.NotifyOnConnectionRestored)
                        {
                            _notificationService?.ShowConnectionRestoredNotification(item.DisplayName);
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

        private void ServerListView_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                OpenServerTab(item.Server);
            }
        }

        private void OpenServerTab_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                OpenServerTab(item.Server);
            }
        }

        private void OpenServerTab(ServerConnection server)
        {
            if (_openTabs.TryGetValue(server.Id, out var existingTab))
            {
                ServerTabControl.SelectedItem = existingTab;
                return;
            }

            /* Set server UTC offset for chart axis bounds */
            var connStatus = _serverManager.GetConnectionStatus(server.Id);
            var utcOffset = connStatus.UtcOffsetMinutes ?? (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;
            Helpers.ServerTimeHelper.UtcOffsetMinutes = utcOffset;

            var serverTab = new ServerTab(server, utcOffset);
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

        private void OpenAlertsTab()
        {
            if (_alertsTab != null && ServerTabControl.Items.Contains(_alertsTab))
            {
                ServerTabControl.SelectedItem = _alertsTab;
                _alertsHistoryContent?.RefreshAlerts();
                return;
            }

            _alertsHistoryContent = new AlertsHistoryContent();
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
            /* Restore the selected tab's UTC offset so charts use the correct server timezone */
            if (ServerTabControl.SelectedItem is TabItem { Content: ServerTab serverTab })
            {
                Helpers.ServerTimeHelper.UtcOffsetMinutes = serverTab.UtcOffsetMinutes;
            }
        }

        private void LandingPage_ServerCardClicked(object? sender, ServerConnection server)
        {
            OpenServerTab(server);
        }

        private void AddServer_Click(object sender, RoutedEventArgs e)
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
                    LoadServerList();

                    MessageBox.Show(
                        $"Server '{server.DisplayName}' added successfully!\n\n" +
                        (server.UseWindowsAuth ? "Using Windows Authentication" : "Credentials saved securely to Windows Credential Manager"),
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

        private void EditServer_Click(object sender, RoutedEventArgs e)
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
                        LoadServerList();

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
                            (updatedServer.UseWindowsAuth ? "Using Windows Authentication" : "Credentials updated securely in Windows Credential Manager"),
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
                    LoadServerList();

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

        private void ToggleFavorite_Click(object sender, RoutedEventArgs e)
        {
            if (ServerListView.SelectedItem is ServerListItem item)
            {
                var server = item.Server;
                server.IsFavorite = !server.IsFavorite;
                _serverManager.UpdateServer(server, null, null);
                LoadServerList();
            }
        }

        private void ManageServers_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new ManageServersWindow(_serverManager);
            dialog.Owner = this;

            if (dialog.ShowDialog() == true && dialog.ServersModified)
            {
                LoadServerList();
            }
        }

        private void Settings_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new SettingsWindow(_preferencesService);
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
                    var health = await databaseService.GetAlertHealthAsync(connStatus.SqlEngineEdition, prefs.LongRunningQueryThresholdMinutes, prefs.LongRunningJobMultiplier);

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
                if (!_lastBlockingAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    _notificationService?.ShowBlockingNotification(
                        serverName,
                        (int)health.TotalBlocked,
                        (int)health.LongestBlockedSeconds);
                    _lastBlockingAlert[serverId] = now;

                    _emailAlertService.RecordAlert(serverId, serverName, "Blocking Detected",
                        $"{(int)health.TotalBlocked} session(s), longest {(int)health.LongestBlockedSeconds}s",
                        $"{prefs.BlockingThresholdSeconds}s", true, "tray");

                    var blockingContext = await BuildBlockingContextAsync(databaseService);

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "Blocking Detected",
                        serverName,
                        $"{(int)health.TotalBlocked} session(s), longest {(int)health.LongestBlockedSeconds}s",
                        $"{prefs.BlockingThresholdSeconds}s",
                        serverId,
                        blockingContext);
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

            bool deadlocksExceeded = prefs.NotifyOnDeadlock
                && deadlockDelta >= prefs.DeadlockThreshold;

            if (deadlocksExceeded)
            {
                _activeDeadlockAlert[serverId] = true;
                if (!_lastDeadlockAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    _notificationService?.ShowDeadlockNotification(
                        serverName,
                        (int)deadlockDelta);
                    _lastDeadlockAlert[serverId] = now;

                    _emailAlertService.RecordAlert(serverId, serverName, "Deadlocks Detected",
                        deadlockDelta.ToString(),
                        prefs.DeadlockThreshold.ToString(), true, "tray");

                    var deadlockContext = await BuildDeadlockContextAsync(databaseService);

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "Deadlocks Detected",
                        serverName,
                        deadlockDelta.ToString(),
                        prefs.DeadlockThreshold.ToString(),
                        serverId,
                        deadlockContext);
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
                if (!_lastHighCpuAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    _notificationService?.ShowHighCpuNotification(
                        serverName,
                        totalCpu);
                    _lastHighCpuAlert[serverId] = now;

                    _emailAlertService.RecordAlert(serverId, serverName, "High CPU",
                        $"{totalCpu:F0}%",
                        $"{prefs.CpuThresholdPercent}%", true, "tray");

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "High CPU",
                        serverName,
                        $"{totalCpu:F0}%",
                        $"{prefs.CpuThresholdPercent}%",
                        serverId);
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
                if (!_lastPoisonWaitAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    var worst = triggeredWaits[0];
                    _notificationService?.ShowPoisonWaitNotification(serverName, worst.WaitType, worst.AvgMsPerWait);
                    _lastPoisonWaitAlert[serverId] = now;

                    var allWaitNames = string.Join(", ", triggeredWaits.ConvertAll(w => $"{w.WaitType} ({w.AvgMsPerWait:F0}ms)"));
                    _emailAlertService.RecordAlert(serverId, serverName, "Poison Wait",
                        allWaitNames,
                        $"{prefs.PoisonWaitThresholdMs}ms avg", true, "tray");

                    var poisonContext = BuildPoisonWaitContext(triggeredWaits);

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "Poison Wait",
                        serverName,
                        allWaitNames,
                        $"{prefs.PoisonWaitThresholdMs}ms avg",
                        serverId,
                        poisonContext);
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
            bool longRunningTriggered = prefs.NotifyOnLongRunningQueries
                && health.LongRunningQueries.Count > 0;

            if (longRunningTriggered)
            {
                _activeLongRunningQueryAlert[serverId] = true;
                if (!_lastLongRunningQueryAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    var worst = health.LongRunningQueries[0];
                    var elapsedMinutes = worst.ElapsedSeconds / 60;
                    var preview = Truncate(worst.QueryText, 80);
                    _notificationService?.ShowLongRunningQueryNotification(
                        serverName, worst.SessionId, elapsedMinutes, preview);
                    _lastLongRunningQueryAlert[serverId] = now;

                    _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Query",
                        $"Session #{worst.SessionId} running {elapsedMinutes}m",
                        $"{prefs.LongRunningQueryThresholdMinutes}m", true, "tray");

                    var lrqContext = BuildLongRunningQueryContext(health.LongRunningQueries);

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "Long-Running Query",
                        serverName,
                        $"{health.LongRunningQueries.Count} query(s), longest {elapsedMinutes}m",
                        $"{prefs.LongRunningQueryThresholdMinutes}m",
                        serverId,
                        lrqContext);
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
                if (!_lastTempDbSpaceAlert.TryGetValue(serverId, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    _notificationService?.ShowTempDbSpaceNotification(serverName, tempDb.UsedPercent);
                    _lastTempDbSpaceAlert[serverId] = now;

                    _emailAlertService.RecordAlert(serverId, serverName, "TempDB Space",
                        $"{tempDb.UsedPercent:F0}% used ({tempDb.TotalReservedMb:F0} MB)",
                        $"{prefs.TempDbSpaceThresholdPercent}%", true, "tray");

                    var tempDbContext = BuildTempDbSpaceContext(tempDb);

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "TempDB Space",
                        serverName,
                        $"{tempDb.UsedPercent:F0}% used ({tempDb.TotalReservedMb:F0} MB)",
                        $"{prefs.TempDbSpaceThresholdPercent}%",
                        serverId,
                        tempDbContext);
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

                if (!_lastLongRunningJobAlert.TryGetValue(jobKey, out var lastAlert) || (now - lastAlert) >= AlertCooldown)
                {
                    var currentMinutes = worst.CurrentDurationSeconds / 60;
                    _notificationService?.ShowLongRunningJobNotification(
                        serverName, worst.JobName, currentMinutes, worst.PercentOfAverage ?? 0);
                    _lastLongRunningJobAlert[jobKey] = now;

                    _emailAlertService.RecordAlert(serverId, serverName, "Long-Running Job",
                        $"{worst.JobName} at {worst.PercentOfAverage:F0}% of avg ({currentMinutes}m)",
                        $"{prefs.LongRunningJobMultiplier}x avg", true, "tray");

                    var jobContext = BuildAnomalousJobContext(health.AnomalousJobs);

                    await _emailAlertService.TrySendAlertEmailAsync(
                        "Long-Running Job",
                        serverName,
                        $"{health.AnomalousJobs.Count} job(s) exceeding {prefs.LongRunningJobMultiplier}x average",
                        $"{prefs.LongRunningJobMultiplier}x historical avg",
                        serverId,
                        jobContext);
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
            text = text.Trim();
            return text.Length <= maxLength ? text : text.Substring(0, maxLength) + "...";
        }

        private static async Task<AlertContext?> BuildBlockingContextAsync(DatabaseService databaseService)
        {
            try
            {
                var events = await databaseService.GetBlockingEventsAsync(hoursBack: 1);
                if (events == null || events.Count == 0) return null;

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

        private static async Task<AlertContext?> BuildDeadlockContextAsync(DatabaseService databaseService)
        {
            try
            {
                var deadlocks = await databaseService.GetDeadlocksAsync(hoursBack: 1);
                if (deadlocks == null || deadlocks.Count == 0) return null;

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
    }
}
