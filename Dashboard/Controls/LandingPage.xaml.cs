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
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class LandingPage : UserControl
    {
        private readonly ServerManager _serverManager;
        private readonly UserPreferencesService _preferencesService;
        private readonly CredentialService _credentialService;
        private readonly ObservableCollection<ServerHealthStatus> _serverHealthStatuses;
        private readonly DispatcherTimer _refreshTimer;
        private readonly DispatcherTimer _displayTimer;

        public event EventHandler<ServerConnection>? ServerCardClicked;

        public LandingPage(ServerManager? serverManager = null)
        {
            InitializeComponent();

            _serverManager = serverManager ?? new ServerManager();
            _preferencesService = new UserPreferencesService();
            _credentialService = new CredentialService();
            _serverHealthStatuses = new ObservableCollection<ServerHealthStatus>();

            ServerCardsPanel.ItemsSource = _serverHealthStatuses;

            // Timer for refreshing health data
            _refreshTimer = new DispatcherTimer();
            _refreshTimer.Tick += RefreshTimer_Tick;

            // Timer for updating timestamp displays
            _displayTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(30)
            };
            _displayTimer.Tick += DisplayTimer_Tick;

            Loaded += LandingPage_Loaded;
            Unloaded += LandingPage_Unloaded;
        }

        private async void LandingPage_Loaded(object sender, RoutedEventArgs e)
        {
            LoadServers();
            ConfigureRefreshTimer();
            _displayTimer.Start();

            await RefreshAllServersAsync();
        }

        private void LandingPage_Unloaded(object sender, RoutedEventArgs e)
        {
            _refreshTimer.Stop();
            _displayTimer.Stop();
        }

        private void LoadServers()
        {
            var servers = _serverManager.GetAllServers();

            _serverHealthStatuses.Clear();

            foreach (var server in servers)
            {
                _serverHealthStatuses.Add(new ServerHealthStatus(server));
            }

            // Sort servers alphabetically by display name
            _serverHealthStatuses.OrderBy(s => s.Server.DisplayName).ToList().ForEach(s => _serverHealthStatuses.Move(_serverHealthStatuses.IndexOf(s), _serverHealthStatuses.Count - 1));

            UpdateSubtitle();
            UpdateEmptyState();
        }

        private void UpdateSubtitle()
        {
            var count = _serverHealthStatuses.Count;
            SubtitleText.Text = count == 1
                ? "Monitoring 1 server"
                : $"Monitoring {count} servers";
        }

        private void UpdateEmptyState()
        {
            if (_serverHealthStatuses.Count == 0)
            {
                EmptyStatePanel.Visibility = Visibility.Visible;
                ServerCardsPanel.Visibility = Visibility.Collapsed;
            }
            else
            {
                EmptyStatePanel.Visibility = Visibility.Collapsed;
                ServerCardsPanel.Visibility = Visibility.Visible;
            }
        }

        private void ConfigureRefreshTimer()
        {
            var prefs = _preferencesService.GetPreferences();

            if (prefs.AutoRefreshEnabled && prefs.AutoRefreshIntervalSeconds > 0)
            {
                _refreshTimer.Interval = TimeSpan.FromSeconds(prefs.AutoRefreshIntervalSeconds);
                _refreshTimer.Start();
                AutoRefreshStatusText.Text = $"{prefs.AutoRefreshIntervalSeconds}s";
            }
            else
            {
                _refreshTimer.Stop();
                AutoRefreshStatusText.Text = "Off";
            }
        }

        private async void RefreshTimer_Tick(object? sender, EventArgs e)
        {
            await RefreshAllServersAsync();
        }

        private void DisplayTimer_Tick(object? sender, EventArgs e)
        {
            foreach (var status in _serverHealthStatuses)
            {
                status.RefreshTimestampDisplay();
            }
        }

        /// <summary>
        /// Refreshes all servers asynchronously - each server updates independently.
        /// Online servers don't wait for offline servers.
        /// </summary>
        private async Task RefreshAllServersAsync()
        {
            // Fire off all refresh tasks in parallel - each one is independent
            var tasks = _serverHealthStatuses.Select(status => RefreshServerAsync(status));
            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Refreshes a single server's health status.
        /// Runs independently of other servers.
        /// </summary>
        private async Task RefreshServerAsync(ServerHealthStatus status)
        {
            try
            {
                var server = status.Server;
                var connectionString = server.GetConnectionString(_credentialService);
                var databaseService = new DatabaseService(connectionString);

                // This will update the status object's properties, which fires PropertyChanged
                // and the UI updates automatically via data binding
                await databaseService.RefreshNocHealthStatusAsync(status);

                // Populate installed monitor version from connectivity check
                var connStatus = _serverManager.GetConnectionStatus(server.Id);
                status.MonitorVersion = connStatus.InstalledMonitorVersion;

                // Update tab badges in MainWindow
                UpdateTabBadges(status);
            }
            catch (Exception)
            {
                // RefreshNocHealthStatusAsync already handles exceptions internally
                // and sets IsOnline = false with ErrorMessage
            }
        }

        /// <summary>
        /// Updates tab badges in MainWindow for the given server.
        /// </summary>
        private void UpdateTabBadges(ServerHealthStatus status)
        {
            var mainWindow = Window.GetWindow(this) as MainWindow;
            if (mainWindow == null) return;

            // Update server tab badge in main tab control
            mainWindow.UpdateTabBadge(status.ServerId, status);

            // Update sub-tab badges in open ServerTab instances
            foreach (var tabItem in mainWindow.ServerTabControl.Items.OfType<TabItem>())
            {
                if (tabItem.Content is ServerTab serverTab && serverTab.ServerId == status.ServerId)
                {
                    serverTab.UpdateBadges(status, mainWindow.AlertStateService);
                    break;
                }
            }
        }

        private async void RefreshAll_Click(object sender, RoutedEventArgs e)
        {
            RefreshAllButton.IsEnabled = false;
            RefreshAllButton.Content = "Refreshing...";

            try
            {
                await RefreshAllServersAsync();
            }
            finally
            {
                RefreshAllButton.IsEnabled = true;
                RefreshAllButton.Content = "↻ Refresh All";
            }
        }

        private void ServerHealthCard_CardClicked(object? sender, ServerHealthStatus status)
        {
            ServerCardClicked?.Invoke(this, status.Server);
        }

        private void ServerHealthCard_EditServerRequested(object? sender, ServerHealthStatus status)
        {
            var server = status.Server;
            var dialog = new AddServerDialog(server);
            dialog.Owner = Window.GetWindow(this);

            if (dialog.ShowDialog() == true)
            {
                try
                {
                    _serverManager.UpdateServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                    _ = ReloadServersAsync();
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to update server:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private async void ServerHealthCard_CheckVersionRequested(object? sender, ServerHealthStatus status)
        {
            var server = status.Server;

            try
            {
                string? installedVersion = await _serverManager.GetInstalledVersionAsync(server);

                if (installedVersion == null)
                {
                    MessageBox.Show(
                        $"No PerformanceMonitor installation found on '{server.DisplayNameWithIntent}'.",
                        "Not Installed", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
                }

                string appVersion = Assembly.GetExecutingAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                    ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0";
                int plusIndex = appVersion.IndexOf('+');
                if (plusIndex >= 0) appVersion = appVersion[..plusIndex];

                static string Normalize(string v) =>
                    Version.TryParse(v, out var p) ? new Version(p.Major, p.Minor, p.Build).ToString() : v;

                string normalizedInstalled = Normalize(installedVersion);
                string normalizedApp = Normalize(appVersion);

                if (Version.TryParse(normalizedInstalled, out var installed) &&
                    Version.TryParse(normalizedApp, out var app) &&
                    installed < app)
                {
                    var result = MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' has v{normalizedInstalled} installed.\n\nv{normalizedApp} is available. Open the server editor to upgrade?",
                        "Update Available", MessageBoxButton.YesNo, MessageBoxImage.Information);

                    if (result == MessageBoxResult.Yes)
                    {
                        ServerHealthCard_EditServerRequested(sender, status);
                    }
                }
                else
                {
                    MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' is up to date (v{normalizedInstalled}).",
                        "No Updates", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to check version:\n\n{ex.Message}", "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// Reloads the server list (call when servers are added/removed).
        /// </summary>
        public async Task ReloadServersAsync()
        {
            LoadServers();
            await RefreshAllServersAsync();
        }

        /// <summary>
        /// Reconfigures the auto-refresh timer (call when settings change).
        /// </summary>
        public void RefreshAutoRefreshSettings()
        {
            ConfigureRefreshTimer();
        }
    }
}
