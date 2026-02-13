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
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Threading;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;

namespace PerformanceMonitorLite;

public partial class MainWindow : Window
{
    private readonly DuckDbInitializer _databaseInitializer;
    private readonly ServerManager _serverManager;
    private readonly ScheduleManager _scheduleManager;
    private RemoteCollectorService? _collectorService;
    private CollectionBackgroundService? _backgroundService;
    private CancellationTokenSource? _backgroundCts;
    private SystemTrayService? _trayService;
    private readonly Dictionary<string, TabItem> _openServerTabs = new();
    private readonly Dictionary<string, bool> _previousConnectionStates = new();
    private readonly Dictionary<string, DateTime> _lastCpuAlert = new();
    private readonly Dictionary<string, DateTime> _lastBlockingAlert = new();
    private readonly Dictionary<string, DateTime> _lastDeadlockAlert = new();
    private static readonly TimeSpan AlertCooldown = TimeSpan.FromMinutes(5);
    private readonly DispatcherTimer _statusTimer;
    private LocalDataService? _dataService;
    private McpHostService? _mcpService;
    private readonly AlertStateService _alertStateService = new();
    private EmailAlertService _emailAlertService;

    /* Track active alert states for resolved notifications */
    private readonly Dictionary<string, bool> _activeCpuAlert = new();
    private readonly Dictionary<string, bool> _activeBlockingAlert = new();
    private readonly Dictionary<string, bool> _activeDeadlockAlert = new();

    /* Track previous alert counts to detect new alerts (for clearing acknowledgements) */
    private readonly Dictionary<string, (int Blocking, int Deadlock)> _previousAlertCounts = new();

    public MainWindow()
    {
        InitializeComponent();

        // Initialize services
        _databaseInitializer = new DuckDbInitializer(App.DatabasePath);
        _emailAlertService = new EmailAlertService(_databaseInitializer);
        _serverManager = new ServerManager(App.ConfigDirectory);
        _scheduleManager = new ScheduleManager(App.ConfigDirectory);

        // Status bar update timer
        _statusTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(30) };
        _statusTimer.Tick += async (s, e) => { UpdateStatusBar(); await RefreshOverviewAsync(); CheckConnectionsAndNotify(); };

        // Initialize database and UI
        Loaded += MainWindow_Loaded;
        Closing += MainWindow_Closing;
        ServerTabControl.SelectionChanged += ServerTabControl_SelectionChanged;
    }

    private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
    {
        try
        {
            StatusText.Text = "Initializing database...";

            // Initialize the DuckDB database
            await _databaseInitializer.InitializeAsync();

            // Initialize the collection engine
            _collectorService = new RemoteCollectorService(
                _databaseInitializer,
                _serverManager,
                _scheduleManager);

            var archiveService = new ArchiveService(_databaseInitializer, App.ArchiveDirectory);
            var retentionService = new RetentionService(App.ArchiveDirectory);

            _backgroundService = new CollectionBackgroundService(_collectorService, archiveService, retentionService, _serverManager);

            // Start background collection
            _backgroundCts = new CancellationTokenSource();
            _ = _backgroundService.StartAsync(_backgroundCts.Token);

            // Initialize system tray
            _trayService = new SystemTrayService(this, _backgroundService);
            _trayService.Initialize();

            // Initialize data service for overview
            _dataService = new LocalDataService(_databaseInitializer);

            // Start MCP server if enabled
            var mcpSettings = McpSettings.Load(App.ConfigDirectory);
            if (mcpSettings.Enabled)
            {
                _mcpService = new McpHostService(_dataService, _serverManager, mcpSettings.Port);
                _ = _mcpService.StartAsync(_backgroundCts!.Token);
            }

            // Load servers
            RefreshServerList();

            // Update status
            UpdateStatusBar();
            _statusTimer.Start();

            await RefreshOverviewAsync();
            StatusText.Text = "Ready - Collection active";
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

    private async void MainWindow_Closing(object? sender, System.ComponentModel.CancelEventArgs e)
    {
        // Dispose system tray
        _trayService?.Dispose();

        // Stop background collection with timeout
        _backgroundCts?.Cancel();

        if (_mcpService != null)
        {
            try
            {
                using var mcpShutdownCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                await _mcpService.StopAsync(mcpShutdownCts.Token);
            }
            catch (OperationCanceledException)
            {
                /* MCP shutdown timed out */
            }
        }

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
    }

    private void ServerTabControl_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        /* Restore the selected tab's UTC offset so charts use the correct server timezone */
        if (ServerTabControl.SelectedItem is TabItem { Content: ServerTab serverTab })
        {
            ServerTimeHelper.UtcOffsetMinutes = serverTab.UtcOffsetMinutes;
        }
    }

    private void RefreshServerList()
    {
        var servers = _serverManager.GetAllServers();
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

        // Refresh overview when server list changes
        _ = RefreshOverviewAsync();
    }

    private void UpdateStatusBar()
    {
        // Update database size
        var sizeMb = _databaseInitializer.GetDatabaseSizeMb();
        DatabaseSizeText.Text = sizeMb > 0
            ? $"Database: {sizeMb:F1} MB"
            : "Database: New";

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

        var health = _collectorService.GetHealthSummary();

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
        else
        {
            CollectorHealthText.Text = $"Collectors: {health.TotalCollectors} OK";
            CollectorHealthText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush");
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
                    var serverId = RemoteCollectorService.GetDeterministicHashCode(server.ServerName);
                    var summary = await _dataService.GetServerSummaryAsync(serverId, server.DisplayName);
                    if (summary != null)
                    {
                        summary.ServerName = server.ServerName;
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
        if (server.AuthenticationType == "EntraMFA" && currentStatus.UserCancelledMfa)
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
        var serverTab = new ServerTab(server, _databaseInitializer, _serverManager.CredentialService, utcOffset);
        var tabHeader = CreateTabHeader(server);
        var tabItem = new TabItem
        {
            Header = tabHeader,
            Content = serverTab
        };

        /* Subscribe to alert counts for badge updates */
        var serverId = server.Id;
        serverTab.AlertCountsChanged += (blockingCount, deadlockCount) =>
        {
            Dispatcher.Invoke(() => UpdateTabBadge(tabHeader, serverId, blockingCount, deadlockCount));
        };

        /* Subscribe to "Apply to All" time range propagation */
        serverTab.ApplyTimeRangeRequested += (selectedIndex) =>
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

        /* Re-collect on-load data (config, trace flags) when refresh button is clicked */
        serverTab.ManualRefreshRequested += async () =>
        {
            if (_collectorService != null)
            {
                var onLoadCollectors = _scheduleManager.GetOnLoadCollectors();
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
            StatusText.Text = $"Collecting data from {server.DisplayName}...";
            try
            {
                await _collectorService.RunAllCollectorsForServerAsync(server);
                StatusText.Text = $"Connected to {server.DisplayName} - Data loaded";
                serverTab.RefreshData();
                UpdateCollectorHealth();
                _ = RefreshOverviewAsync();
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Connected to {server.DisplayName} - Collection error: {ex.Message}";
            }
        }
        else
        {
            StatusText.Text = $"Connected to {server.DisplayName}";
        }
    }

    private StackPanel CreateTabHeader(ServerConnection server)
    {
        var panel = new StackPanel { Orientation = Orientation.Horizontal };

        panel.Children.Add(new TextBlock
        {
            Text = server.DisplayName,
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

    private void UpdateTabBadge(StackPanel tabHeader, string serverId, int blockingCount, int deadlockCount)
    {
        var totalAlerts = blockingCount + deadlockCount;

        /* Check if new alerts arrived - if so, clear any acknowledgement */
        if (_previousAlertCounts.TryGetValue(serverId, out var previous))
        {
            if (blockingCount > previous.Blocking || deadlockCount > previous.Deadlock)
            {
                /* New alerts - clear acknowledgement so badge shows again */
                _alertStateService.ClearAcknowledgement(serverId);
            }
        }
        _previousAlertCounts[serverId] = (blockingCount, deadlockCount);

        /* Check suppression state */
        bool shouldShow = totalAlerts > 0 && _alertStateService.ShouldShowAlerts(serverId);

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
                        text.Text = totalAlerts > 99 ? "99+" : totalAlerts.ToString();
                        text.ToolTip = $"Blocking: {blockingCount}, Deadlocks: {deadlockCount}\nRight-click to dismiss";
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

    private void AcknowledgeServerAlert_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Tag is string serverId)
        {
            _alertStateService.AcknowledgeAlert(serverId);

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
                serverTab.StopRefresh();
            }

            ServerTabControl.Items.Remove(tab);
            _openServerTabs.Remove(serverId);

            /* Clean up alert state for this server */
            _alertStateService.RemoveServerState(serverId);
            _previousAlertCounts.Remove(serverId);

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
        var dialog = new AddServerDialog(_serverManager) { Owner = this };
        if (dialog.ShowDialog() == true && dialog.AddedServer != null)
        {
            RefreshServerList();
            StatusText.Text = $"Added server: {dialog.AddedServer.DisplayName}";
        }
    }

    private void ManageServersButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new ManageServersWindow(_serverManager) { Owner = this };
        window.ShowDialog();

        if (window.ServersChanged)
        {
            RefreshServerList();
        }
    }

    private void SettingsButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new SettingsWindow(_scheduleManager, _backgroundService, _mcpService) { Owner = this };
        window.ShowDialog();
        UpdateStatusBar();
    }

    private void AboutButton_Click(object sender, RoutedEventArgs e)
    {
        var window = new Windows.AboutWindow { Owner = this };
        window.ShowDialog();
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

        var dialog = new AddServerDialog(_serverManager, server) { Owner = this };
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
            $"Remove server '{server.DisplayName}'?",
            "Remove Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result == MessageBoxResult.Yes)
        {
            CloseServerTab(server.Id);
            _serverManager.DeleteServer(server.Id);
            RefreshServerList();
            StatusText.Text = $"Removed server: {server.DisplayName}";
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
            foreach (var server in servers)
            {
                var status = _serverManager.GetConnectionStatus(server.Id);
                if (status?.IsOnline == null) continue;

                bool isOnline = status.IsOnline == true;

                if (_previousConnectionStates.TryGetValue(server.Id, out var wasOnline))
                {
                    if (App.AlertsEnabled && App.NotifyConnectionChanges)
                    {
                        if (wasOnline && !isOnline)
                        {
                            _trayService?.ShowNotification(
                                "Server Offline",
                                $"{server.DisplayName} is unreachable: {status.ErrorMessage ?? "unknown error"}",
                                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Error);
                        }
                        else if (!wasOnline && isOnline)
                        {
                            _trayService?.ShowNotification(
                                "Server Online",
                                $"{server.DisplayName} is back online",
                                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
                        }
                    }

                    if (wasOnline != isOnline)
                    {
                        RefreshServerList();
                    }
                }

                _previousConnectionStates[server.Id] = isOnline;
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("ConnectionAlerts", $"Connection check notify failed: {ex.Message}");
        }
    }

    private async void CheckPerformanceAlerts(ServerSummaryItem summary)
    {
        if (!App.AlertsEnabled || _trayService == null) return;

        var key = summary.ServerId.ToString();
        var now = DateTime.UtcNow;

        /* Skip popup/email alerts if user has acknowledged or silenced this server */
        bool suppressPopups = !_alertStateService.ShouldShowAlerts(key);

        /* CPU alerts */
        bool cpuExceeded = App.AlertCpuEnabled
            && summary.CpuPercent.HasValue
            && summary.CpuPercent.Value >= App.AlertCpuThreshold;

        if (cpuExceeded)
        {
            _activeCpuAlert[key] = true;
            if (!suppressPopups && (!_lastCpuAlert.TryGetValue(key, out var lastCpu) || now - lastCpu >= AlertCooldown))
            {
                _trayService.ShowNotification(
                    "High CPU",
                    $"{summary.DisplayName}: CPU at {summary.CpuPercent:F0}% (threshold: {App.AlertCpuThreshold}%)",
                    Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning);
                _lastCpuAlert[key] = now;

                await _emailAlertService.TrySendAlertEmailAsync(
                    "High CPU",
                    summary.DisplayName,
                    $"{summary.CpuPercent:F0}%",
                    $"{App.AlertCpuThreshold}%",
                    summary.ServerId);
            }
        }
        else if (_activeCpuAlert.TryGetValue(key, out var wasCpu) && wasCpu)
        {
            _activeCpuAlert[key] = false;
            _trayService.ShowNotification(
                "CPU Resolved",
                $"{summary.DisplayName}: CPU back to {summary.CpuPercent:F0}%",
                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
        }

        /* Blocking alerts */
        bool blockingExceeded = App.AlertBlockingEnabled
            && summary.BlockingCount >= App.AlertBlockingThreshold;

        if (blockingExceeded)
        {
            _activeBlockingAlert[key] = true;
            if (!suppressPopups && (!_lastBlockingAlert.TryGetValue(key, out var lastBlocking) || now - lastBlocking >= AlertCooldown))
            {
                _trayService.ShowNotification(
                    "Blocking Detected",
                    $"{summary.DisplayName}: {summary.BlockingCount} blocking session(s)",
                    Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning);
                _lastBlockingAlert[key] = now;

                var blockingContext = await BuildBlockingContextAsync(summary.ServerId);

                await _emailAlertService.TrySendAlertEmailAsync(
                    "Blocking Detected",
                    summary.DisplayName,
                    summary.BlockingCount.ToString(),
                    App.AlertBlockingThreshold.ToString(),
                    summary.ServerId,
                    blockingContext);
            }
        }
        else if (_activeBlockingAlert.TryGetValue(key, out var wasBlocking) && wasBlocking)
        {
            _activeBlockingAlert[key] = false;
            _trayService.ShowNotification(
                "Blocking Cleared",
                $"{summary.DisplayName}: No active blocking",
                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
        }

        /* Deadlock alerts */
        bool deadlocksExceeded = App.AlertDeadlockEnabled
            && summary.DeadlockCount >= App.AlertDeadlockThreshold;

        if (deadlocksExceeded)
        {
            _activeDeadlockAlert[key] = true;
            if (!suppressPopups && (!_lastDeadlockAlert.TryGetValue(key, out var lastDeadlock) || now - lastDeadlock >= AlertCooldown))
            {
                _trayService.ShowNotification(
                    "Deadlocks Detected",
                    $"{summary.DisplayName}: {summary.DeadlockCount} deadlock(s) in the last hour",
                    Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Error);
                _lastDeadlockAlert[key] = now;

                var deadlockContext = await BuildDeadlockContextAsync(summary.ServerId);

                await _emailAlertService.TrySendAlertEmailAsync(
                    "Deadlocks Detected",
                    summary.DisplayName,
                    summary.DeadlockCount.ToString(),
                    App.AlertDeadlockThreshold.ToString(),
                    summary.ServerId,
                    deadlockContext);
            }
        }
        else if (_activeDeadlockAlert.TryGetValue(key, out var wasDeadlock) && wasDeadlock)
        {
            _activeDeadlockAlert[key] = false;
            _trayService.ShowNotification(
                "Deadlocks Cleared",
                $"{summary.DisplayName}: No deadlocks in the last hour",
                Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
        }
    }

        private static string TruncateText(string text, int maxLength = 300)
        {
            if (string.IsNullOrEmpty(text)) return "";
            text = text.Trim();
            return text.Length <= maxLength ? text : text.Substring(0, maxLength) + "...";
        }

        private async Task<AlertContext?> BuildBlockingContextAsync(int serverId)
        {
            try
            {
                if (_dataService == null) return null;

                var events = await _dataService.GetRecentBlockedProcessReportsAsync(serverId, hoursBack: 1);
                if (events == null || events.Count == 0) return null;

                var context = new AlertContext();
                var firstXml = (string?)null;

                foreach (var e in events.Take(3))
                {
                    var item = new AlertDetailItem
                    {
                        Heading = $"Blocked #{e.BlockedSpid} by #{e.BlockingSpid}",
                        Fields = new()
                    };

                    if (!string.IsNullOrEmpty(e.DatabaseName))
                        item.Fields.Add(("Database", e.DatabaseName));
                    if (!string.IsNullOrEmpty(e.BlockedSqlText))
                        item.Fields.Add(("Blocked Query", TruncateText(e.BlockedSqlText)));
                    if (!string.IsNullOrEmpty(e.BlockingSqlText))
                        item.Fields.Add(("Blocking Query", TruncateText(e.BlockingSqlText)));
                    item.Fields.Add(("Wait Time", e.WaitTimeFormatted));
                    if (!string.IsNullOrEmpty(e.LockMode))
                        item.Fields.Add(("Lock Mode", e.LockMode));

                    context.Details.Add(item);
                    if (firstXml == null && e.HasReportXml)
                        firstXml = e.BlockedProcessReportXml;
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
                AppLogger.Error("EmailAlert", $"Failed to fetch blocking detail for email: {ex.Message}");
                return null;
            }
        }

        private async Task<AlertContext?> BuildDeadlockContextAsync(int serverId)
        {
            try
            {
                if (_dataService == null) return null;

                var deadlocks = await _dataService.GetRecentDeadlocksAsync(serverId, hoursBack: 1);
                if (deadlocks == null || deadlocks.Count == 0) return null;

                var context = new AlertContext();
                var firstGraph = (string?)null;

                foreach (var d in deadlocks.Take(3))
                {
                    var item = new AlertDetailItem
                    {
                        Heading = "Deadlock Victim",
                        Fields = new()
                    };

                    if (!string.IsNullOrEmpty(d.VictimSqlText))
                        item.Fields.Add(("Victim SQL", TruncateText(d.VictimSqlText)));
                    if (!string.IsNullOrEmpty(d.ProcessSummary))
                        item.Fields.Add(("Processes", d.ProcessSummary));

                    context.Details.Add(item);
                    if (firstGraph == null && d.HasDeadlockXml)
                        firstGraph = d.DeadlockGraphXml;
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
                AppLogger.Error("EmailAlert", $"Failed to fetch deadlock detail for email: {ex.Message}");
                return null;
            }
        }
    }
