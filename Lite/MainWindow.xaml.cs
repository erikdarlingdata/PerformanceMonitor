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
    private readonly Dictionary<string, DateTime> _lastPoisonWaitAlert = new();
    private readonly Dictionary<string, DateTime> _lastLongRunningQueryAlert = new();
    private readonly Dictionary<string, DateTime> _lastTempDbSpaceAlert = new();
    private readonly Dictionary<string, DateTime> _lastLongRunningJobAlert = new();
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
    private readonly Dictionary<string, bool> _activePoisonWaitAlert = new();
    private readonly Dictionary<string, bool> _activeLongRunningQueryAlert = new();
    private readonly Dictionary<string, bool> _activeTempDbSpaceAlert = new();
    private readonly Dictionary<string, bool> _activeLongRunningJobAlert = new();

    public MainWindow()
    {
        InitializeComponent();

        // Initialize services (with loggers wired to AppLogger)
        _databaseInitializer = new DuckDbInitializer(App.DatabasePath, new AppLoggerAdapter<DuckDbInitializer>());
        _emailAlertService = new EmailAlertService(_databaseInitializer);
        _serverManager = new ServerManager(App.ConfigDirectory, logger: new AppLoggerAdapter<ServerManager>());
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

    private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
    {
        try
        {
            StatusText.Text = "Initializing database...";

            // Initialize the DuckDB database
            await _databaseInitializer.InitializeAsync();

            // Initialize the collection engine (with loggers wired to AppLogger)
            _collectorService = new RemoteCollectorService(
                _databaseInitializer,
                _serverManager,
                _scheduleManager,
                new AppLoggerAdapter<RemoteCollectorService>());

            var archiveService = new ArchiveService(_databaseInitializer, App.ArchiveDirectory, new AppLoggerAdapter<ArchiveService>());
            var retentionService = new RetentionService(App.ArchiveDirectory, new AppLoggerAdapter<RetentionService>());

            _backgroundService = new CollectionBackgroundService(
                _collectorService, _databaseInitializer, archiveService, retentionService, _serverManager,
                new AppLoggerAdapter<CollectionBackgroundService>());

            // Start background collection
            _backgroundCts = new CancellationTokenSource();
            _ = _backgroundService.StartAsync(_backgroundCts.Token);

            // Initialize system tray
            _trayService = new SystemTrayService(this, _backgroundService);
            _trayService.Initialize();

            // Initialize data service for overview
            _dataService = new LocalDataService(_databaseInitializer);

            // Initialize alerts history tab
            AlertsHistoryContent.Initialize(_dataService);

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
            if (!App.CheckForUpdatesOnStartup) return;

            var result = await UpdateCheckService.CheckForUpdateAsync();
            if (result?.IsUpdateAvailable == true)
            {
                var answer = MessageBox.Show(
                    $"Performance Monitor {result.LatestVersion} is available (you have {result.CurrentVersion}).\n\nWould you like to open the download page?",
                    "Update Available",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Information);

                if (answer == MessageBoxResult.Yes)
                {
                    System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = result.ReleaseUrl,
                        UseShellExecute = true
                    });
                }
            }
        }
        catch
        {
            // Never crash on update check failure
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

        /* Refresh alerts tab when selected */
        if (ServerTabControl.SelectedItem == AlertsTab)
        {
            AlertsHistoryContent.RefreshAlerts();
        }

        UpdateCollectorHealth();
    }

    private void RefreshServerList()
    {
        var servers = _serverManager.GetAllServers();
        foreach (var server in servers)
        {
            server.IsOnline = _serverManager.GetConnectionStatus(server.Id).IsOnline;
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
                        var connStatus = _serverManager.GetConnectionStatus(server.Id);
                        summary.IsOnline = connStatus.IsOnline;
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
        var serverTab = new ServerTab(server, _databaseInitializer, _serverManager.CredentialService, utcOffset);
        var tabHeader = CreateTabHeader(server);
        var tabItem = new TabItem
        {
            Header = tabHeader,
            Content = serverTab
        };

        /* Subscribe to alert counts for badge updates */
        var serverId = server.Id;
        serverTab.AlertCountsChanged += (blockingCount, deadlockCount, latestEventTime) =>
        {
            Dispatcher.Invoke(() => UpdateTabBadge(tabHeader, serverId, blockingCount, deadlockCount, latestEventTime));
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

    private void UpdateTabBadge(StackPanel tabHeader, string serverId, int blockingCount, int deadlockCount, DateTime? latestEventTime)
    {
        var totalAlerts = blockingCount + deadlockCount;

        /* Delegate count tracking and acknowledgement clearing to AlertStateService.
           Uses latestEventTime to only clear ack when genuinely new events arrive,
           not when the user just switches time ranges. */
        bool shouldShow = _alertStateService.UpdateAlertCounts(serverId, blockingCount, deadlockCount, latestEventTime);

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
            bool needsRefresh = false;
            foreach (var server in servers)
            {
                var status = _serverManager.GetConnectionStatus(server.Id);
                server.IsOnline = status?.IsOnline;
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
                        needsRefresh = true;
                    }
                }
                else
                {
                    /* First time seeing this server's status — need to refresh */
                    needsRefresh = true;
                }

                _previousConnectionStates[server.Id] = isOnline;
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

        /* Poison wait alerts */
        if (App.AlertPoisonWaitEnabled && _dataService != null)
        {
            try
            {
                var poisonWaits = await _dataService.GetLatestPoisonWaitAvgsAsync(summary.ServerId);
                var triggered = poisonWaits.FindAll(w => w.AvgMsPerWait >= App.AlertPoisonWaitThresholdMs);

                if (triggered.Count > 0)
                {
                    _activePoisonWaitAlert[key] = true;
                    if (!suppressPopups && (!_lastPoisonWaitAlert.TryGetValue(key, out var lastPoisonWait) || now - lastPoisonWait >= AlertCooldown))
                    {
                        var worst = triggered[0];
                        var allWaitNames = string.Join(", ", triggered.ConvertAll(w => $"{w.WaitType} ({w.AvgMsPerWait:F0}ms)"));
                        _trayService.ShowNotification(
                            "Poison Wait",
                            $"{summary.DisplayName}: {worst.WaitType} avg {worst.AvgMsPerWait:F0}ms/wait",
                            Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Error);
                        _lastPoisonWaitAlert[key] = now;

                        var poisonContext = BuildPoisonWaitContext(triggered);

                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Poison Wait",
                            summary.DisplayName,
                            allWaitNames,
                            $"{App.AlertPoisonWaitThresholdMs}ms avg",
                            summary.ServerId,
                            poisonContext);
                    }
                }
                else if (_activePoisonWaitAlert.TryGetValue(key, out var wasPoisonWait) && wasPoisonWait)
                {
                    _activePoisonWaitAlert[key] = false;
                    _trayService.ShowNotification(
                        "Poison Waits Cleared",
                        $"{summary.DisplayName}: Poison wait avg below threshold",
                        Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
                }
            }
            catch (Exception ex)
            {
                AppLogger.Error("Alerts", $"Failed to check poison waits for {summary.DisplayName}: {ex.Message}");
            }
        }

        /* Long-running query alerts */
        if (App.AlertLongRunningQueryEnabled && _dataService != null)
        {
            try
            {
                var longRunning = await _dataService.GetLongRunningQueriesAsync(summary.ServerId, App.AlertLongRunningQueryThresholdMinutes);

                if (longRunning.Count > 0)
                {
                    _activeLongRunningQueryAlert[key] = true;
                    if (!suppressPopups && (!_lastLongRunningQueryAlert.TryGetValue(key, out var lastLrq) || now - lastLrq >= AlertCooldown))
                    {
                        var worst = longRunning[0];
                        var elapsedMinutes = worst.ElapsedSeconds / 60;
                        var preview = TruncateText(worst.QueryText, 80);
                        var previewSuffix = string.IsNullOrEmpty(preview) ? "" : $" — {preview}";
                        _trayService.ShowNotification(
                            "Long-Running Query",
                            $"{summary.DisplayName}: Session #{worst.SessionId} running {elapsedMinutes}m{previewSuffix}",
                            Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning);
                        _lastLongRunningQueryAlert[key] = now;

                        var lrqContext = BuildLongRunningQueryContext(longRunning);

                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Long-Running Query",
                            summary.DisplayName,
                            $"{longRunning.Count} query(s), longest {elapsedMinutes}m",
                            $"{App.AlertLongRunningQueryThresholdMinutes}m",
                            summary.ServerId,
                            lrqContext);
                    }
                }
                else if (_activeLongRunningQueryAlert.TryGetValue(key, out var wasLongRunning) && wasLongRunning)
                {
                    _activeLongRunningQueryAlert[key] = false;
                    _trayService.ShowNotification(
                        "Long-Running Queries Cleared",
                        $"{summary.DisplayName}: No queries over threshold",
                        Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
                }
            }
            catch (Exception ex)
            {
                AppLogger.Error("Alerts", $"Failed to check long-running queries for {summary.DisplayName}: {ex.Message}");
            }
        }

        /* TempDB space alerts */
        if (App.AlertTempDbSpaceEnabled && _dataService != null)
        {
            try
            {
                var tempDb = await _dataService.GetLatestTempDbSpaceAsync(summary.ServerId);

                if (tempDb != null && tempDb.UsedPercent >= App.AlertTempDbSpaceThresholdPercent)
                {
                    _activeTempDbSpaceAlert[key] = true;
                    if (!suppressPopups && (!_lastTempDbSpaceAlert.TryGetValue(key, out var lastTempDb) || now - lastTempDb >= AlertCooldown))
                    {
                        _trayService.ShowNotification(
                            "TempDB Space",
                            $"{summary.DisplayName}: TempDB {tempDb.UsedPercent:F0}% used",
                            Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning);
                        _lastTempDbSpaceAlert[key] = now;

                        var tempDbContext = BuildTempDbSpaceContext(tempDb);

                        await _emailAlertService.TrySendAlertEmailAsync(
                            "TempDB Space",
                            summary.DisplayName,
                            $"{tempDb.UsedPercent:F0}% used ({tempDb.TotalReservedMb:F0} MB)",
                            $"{App.AlertTempDbSpaceThresholdPercent}%",
                            summary.ServerId,
                            tempDbContext);
                    }
                }
                else if (_activeTempDbSpaceAlert.TryGetValue(key, out var wasTempDb) && wasTempDb)
                {
                    _activeTempDbSpaceAlert[key] = false;
                    var pct = tempDb != null ? $"{tempDb.UsedPercent:F0}%" : "N/A";
                    _trayService.ShowNotification(
                        "TempDB Space Resolved",
                        $"{summary.DisplayName}: TempDB usage back to {pct}",
                        Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
                }
            }
            catch (Exception ex)
            {
                AppLogger.Error("Alerts", $"Failed to check TempDB space for {summary.DisplayName}: {ex.Message}");
            }
        }

        /* Anomalous Agent job alerts */
        if (App.AlertLongRunningJobEnabled && _dataService != null)
        {
            try
            {
                var anomalousJobs = await _dataService.GetAnomalousJobsAsync(summary.ServerId, App.AlertLongRunningJobMultiplier);

                if (anomalousJobs.Count > 0)
                {
                    _activeLongRunningJobAlert[key] = true;
                    var worst = anomalousJobs[0];
                    var jobKey = $"{key}:{worst.JobId}:{worst.StartTime:O}";

                    if (!suppressPopups && (!_lastLongRunningJobAlert.TryGetValue(jobKey, out var lastJob) || now - lastJob >= AlertCooldown))
                    {
                        var currentMinutes = worst.CurrentDurationSeconds / 60;
                        _trayService.ShowNotification(
                            "Long-Running Job",
                            $"{summary.DisplayName}: {worst.JobName} at {worst.PercentOfAverage:F0}% of avg ({currentMinutes}m)",
                            Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning);
                        _lastLongRunningJobAlert[jobKey] = now;

                        var jobContext = BuildAnomalousJobContext(anomalousJobs);

                        await _emailAlertService.TrySendAlertEmailAsync(
                            "Long-Running Job",
                            summary.DisplayName,
                            $"{anomalousJobs.Count} job(s) exceeding {App.AlertLongRunningJobMultiplier}x average",
                            $"{App.AlertLongRunningJobMultiplier}x historical avg",
                            summary.ServerId,
                            jobContext);
                    }
                }
                else if (_activeLongRunningJobAlert.TryGetValue(key, out var wasJob) && wasJob)
                {
                    _activeLongRunningJobAlert[key] = false;
                    _trayService.ShowNotification(
                        "Long-Running Jobs Cleared",
                        $"{summary.DisplayName}: No jobs exceeding threshold",
                        Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Info);
                }
            }
            catch (Exception ex)
            {
                AppLogger.Error("Alerts", $"Failed to check anomalous jobs for {summary.DisplayName}: {ex.Message}");
            }
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
                if (!string.IsNullOrEmpty(q.QueryText))
                    item.Fields.Add(("Query", TruncateText(q.QueryText)));
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
    }
