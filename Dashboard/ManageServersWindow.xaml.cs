/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Installer.Core;
using Installer.Core.Models;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace PerformanceMonitorDashboard
{
    public partial class ManageServersWindow : Window
    {
        private readonly ServerManager _serverManager;
        private readonly ICredentialService _credentialService;
        public bool ServersModified { get; private set; }

        public int OutdatedCount { get; private set; }

        // Holds the most recent results from CheckAllVersions_Click so UpgradeAll can reuse them.
        private List<ServerVersionInfo>? _lastVersionCheckResults;

        // Cancellation support for bulk upgrades
        private CancellationTokenSource? _upgradeCts;

        public ManageServersWindow(ServerManager serverManager, ICredentialService? credentialService = null)
        {
            InitializeComponent();
            _serverManager = serverManager;
            _credentialService = credentialService ?? new CredentialService();
            ServersModified = false;
            LoadServers();
            _ = LoadInstalledVersionsAsync();

            /* Ensure we cancel upgrades if the window is closed */
            this.Closing += ManageServersWindow_Closing;
        }

        private async void ManageServersWindow_Closing(object? sender, System.ComponentModel.CancelEventArgs e)
        {
            if (_upgradeCts != null && !_upgradeCts.IsCancellationRequested)
            {
                try
                {
                    _upgradeCts.Cancel();
                }
                catch (OperationCanceledException)
                {
                    // Cancellation is expected.
                }
                catch (Exception)
                {   // Log any exceptions during cancellation 
                    AppendUpgradeLog("An unexpected error while closing the window.", "Warning");
                }
            }
        }

        private sealed record PerServerUpgradeResult(
            string ServerId,
            string ServerDisplay,
            int UpgradesSucceeded,
            int UpgradesFailed,
            int StepsSucceeded,
            int StepsFailed,
            bool Success);

        private static string GetAppVersion()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var infoVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
            if (!string.IsNullOrEmpty(infoVersion))
            {
                /* Strip any +metadata suffix (e.g. "2.4.1+abc123" -> "2.4.1") */
                int plusIndex = infoVersion.IndexOf('+');
                return plusIndex >= 0 ? infoVersion[..plusIndex] : infoVersion;
            }

            var version = assembly.GetName().Version;
            if (version != null)
            {
                /* Normalize 4-part to 3-part: "2.4.1.0" -> "2.4.1" */
                return $"{version.Major}.{version.Minor}.{version.Build}";
            }

            return "0.0.0";
        }

        private void LoadServers()
        {
            // Build a ServerVersionInfo list and bind the grid to it so the UI always works with the wrapper type.
            var servers = _serverManager.GetAllServers().ToList();
            _lastVersionCheckResults = servers.Select(s => new ServerVersionInfo
            {
                Server = s,
                InstalledVersion = string.IsNullOrEmpty(s.InstalledVersion) ? "" : s.InstalledVersion,
                NeedsUpgrade = false
            }).ToList();

            ServersDataGrid.ItemsSource = _lastVersionCheckResults;
            ServersDataGrid.Items.Refresh();
        }

        private async Task LoadInstalledVersionsAsync()
        {
            if (_lastVersionCheckResults == null)
            {
                if (ServersDataGrid.ItemsSource is not IEnumerable<ServerVersionInfo> src) return;
                _lastVersionCheckResults = src.ToList();
            }

            var list = _lastVersionCheckResults.ToList();

            var probeTasks = list.Select(async entry =>
            {
                try
                {
                    string? version = await _serverManager.GetInstalledVersionAsync(entry.Server);
                    return (Entry: entry, Version: version, Unreachable: false);
                }
                catch
                {
                    return (Entry: entry, Version: (string?)null, Unreachable: true);
                }
            }).ToList();

            var results = await Task.WhenAll(probeTasks);

            await Dispatcher.InvokeAsync(() =>
            {
                foreach (var res in results)
                {
                    if (res.Unreachable)
                    {
                        res.Entry.InstalledVersion = "Unreachable";
                    }
                    else
                    {
                        res.Entry.InstalledVersion = string.IsNullOrEmpty(res.Version) ? "Not installed" : NormalizeVersion(res.Version!);
                    }

                    // keep underlying ServerConnection in sync for other code paths
                    res.Entry.Server.InstalledVersion = res.Entry.InstalledVersion;
                }

                ServersDataGrid.Items.Refresh();
            });
        }

        private static string NormalizeVersion(string version)
        {
            int plusIndex = version.IndexOf('+');
            string trimmed = plusIndex >= 0 ? version[..plusIndex] : version;
            return Version.TryParse(trimmed, out var v)
                ? new Version(v.Major, v.Minor, v.Build).ToString()
                : trimmed;
        }

        private void ServersDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!Helpers.TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            EditSelectedServer();
        }

        private void AddServer_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new AddServerDialog();
            dialog.Owner = this;

            if (dialog.ShowDialog() == true)
            {
                try
                {
                    _serverManager.AddServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                    LoadServers();
                    ServersModified = true;

                    MessageBox.Show(
                        $"Server '{dialog.ServerConnection.DisplayNameWithIntent}' added successfully!",
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
            EditSelectedServer();
        }

        private void EditSelectedServer()
        {
            if (ServersDataGrid.SelectedItem is ServerVersionInfo entry)
            {
                var server = entry.Server;
                var dialog = new AddServerDialog(server) { Owner = this };

                if (dialog.ShowDialog() == true)
                {
                    try
                    {
                        _serverManager.UpdateServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                        LoadServers();
                        ServersModified = true;

                        MessageBox.Show(
                            $"Server '{dialog.ServerConnection.DisplayNameWithIntent}' updated successfully!",
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
            else
            {
                MessageBox.Show(
                    "Please select a server to edit.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
        }

        private void ToggleFavorite_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is ServerVersionInfo entry)
            {
                var server = entry.Server;
                server.IsFavorite = !server.IsFavorite;
                _serverManager.UpdateServer(server, null, null);
                LoadServers();
                ServersModified = true;
            }
            else
            {
                MessageBox.Show(
                    "Please select a server to toggle favorite status.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
        }

        private async void RemoveServer_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is ServerVersionInfo entry)
            {
                var server = entry.Server;
                var dialog = new RemoveServerDialog(server.DisplayNameWithIntent) { Owner = this };

                if (dialog.ShowDialog() == true)
                {
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

                    _serverManager.DeleteServer(server.Id);
                    LoadServers();
                    ServersModified = true;

                    MessageBox.Show(
                        $"Server '{server.DisplayNameWithIntent}' removed successfully!",
                        "Server Removed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                }
            }
            else
            {
                MessageBox.Show(
                    "Please select a server to remove.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
        }

        private async void CheckForUpdates_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is not ServerVersionInfo entry)
            {
                MessageBox.Show(
                    "Please select a server to check for updates.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var server = entry.Server;
            CheckUpdatesButton.IsEnabled = false;
            CheckUpdatesButton.Content = "Checking...";

            try
            {
                string? installedVersion;
                try
                {
                    installedVersion = await _serverManager.GetInstalledVersionAsync(server);
                }
                catch (Exception ex)
                {
                    entry.InstalledVersion = "Unreachable";
                    entry.Server.InstalledVersion = entry.InstalledVersion;
                    ServersDataGrid.Items.Refresh();

                    MessageBox.Show(
                        $"Could not reach '{server.DisplayNameWithIntent}':\n\n{ex.Message}",
                        "Connection Error",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error);
                    return;
                }

                entry.InstalledVersion = string.IsNullOrEmpty(installedVersion) ? "Not installed" : NormalizeVersion(installedVersion);
                entry.Server.InstalledVersion = entry.InstalledVersion;
                ServersDataGrid.Items.Refresh();

                if (string.IsNullOrEmpty(installedVersion))
                {
                    MessageBox.Show(
                        $"No PerformanceMonitor installation found on '{server.DisplayNameWithIntent}'.\n\nUse Edit to install.",
                        "Not Installed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                    return;
                }

                // existing version comparison logic follows unchanged...
                string appVersion = Assembly.GetExecutingAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                    ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0";
                int plusIndex = appVersion.IndexOf('+');
                if (plusIndex >= 0) appVersion = appVersion[..plusIndex];

                string Normalize(string v)
                {
                    if (Version.TryParse(v, out var parsed))
                        return new Version(parsed.Major, parsed.Minor, parsed.Build).ToString();
                    return v;
                }

                string normalizedInstalled = Normalize(installedVersion);
                string normalizedApp = Normalize(appVersion);

                if (Version.TryParse(normalizedInstalled, out var installed) &&
                    Version.TryParse(normalizedApp, out var app) &&
                    installed < app)
                {
                    var result = MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' has v{normalizedInstalled} installed.\n\nv{normalizedApp} is available. Open the server editor to upgrade?",
                        "Update Available",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Information);

                    if (result == MessageBoxResult.Yes)
                    {
                        var dialog = new AddServerDialog(server) { Owner = this };

                        if (dialog.ShowDialog() == true)
                        {
                            try
                            {
                                _serverManager.UpdateServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                                LoadServers();
                                ServersModified = true;
                            }
                            catch (Exception ex)
                            {
                                MessageBox.Show(
                                    $"Failed to update server:\n\n{ex.Message}",
                                    "Error",
                                    MessageBoxButton.OK,
                                    MessageBoxImage.Error);
                            }
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
                    $"Failed to check for updates:\n\n{ex.Message}",
                    "Connection Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
            finally
            {
                CheckUpdatesButton.IsEnabled = true;
                CheckUpdatesButton.Content = "Check Server Version";
            }
        }

        private async void CheckAllVersions_Click(object sender, RoutedEventArgs e)
        {
            CheckAllVersionsButton.IsEnabled = false;
            CheckAllVersionsButton.Content = "Checking...";

            try
            {
                string appVersion = GetAppVersion();

                // Check all servers in parallel
                var tasks = _serverManager.GetAllServers().Select(async server =>
                {
                    try
                    {
                        var installed = await _serverManager.GetInstalledVersionAsync(server);
                        bool needsUpgrade = false;

                        if (installed != null &&
                            Version.TryParse(NormalizeVersion(installed), out var installedVer) &&
                            Version.TryParse(NormalizeVersion(appVersion), out var appVer))
                        {
                            needsUpgrade = installedVer < appVer;
                        }

                        return new ServerVersionInfo
                        {
                            Server = server,
                            InstalledVersion = installed != null ? NormalizeVersion(installed) : "Not installed",
                            NeedsUpgrade = needsUpgrade
                        };
                    }
                    catch
                    {
                        // Mark unreachable instead of leaving null so UI shows "Unreachable"
                        return new ServerVersionInfo
                        {
                            Server = server,
                            InstalledVersion = "Unreachable",
                            NeedsUpgrade = false
                        };
                    }
                });

                var results = await Task.WhenAll(tasks);

                // Persist results so UpgradeAll can reuse them
                _lastVersionCheckResults = results.ToList();

                // Update displayed InstalledVersion for each server row so grid shows values immediately
                foreach (var r in _lastVersionCheckResults)
                {
                    r.Server.InstalledVersion = r.InstalledVersion ?? "Not installed";
                }

                // Update the grid with version info
                ServersDataGrid.ItemsSource = _lastVersionCheckResults;
                ServersDataGrid.Items.Refresh();

                // Show/hide UpgradeAllButton based on whether any need upgrades
                OutdatedCount = results.Count(r => r.NeedsUpgrade);
                if (OutdatedCount > 0)
                {
                    UpgradeAllButton.Visibility = Visibility.Visible;
                    UpgradeAllButton.Content = $"Upgrade {OutdatedCount} Server{(OutdatedCount > 1 ? "s" : "")}";
                }
                else
                {
                    UpgradeAllButton.Visibility = Visibility.Collapsed;
                }
            }
            finally
            {
                CheckAllVersionsButton.IsEnabled = true;
                CheckAllVersionsButton.Content = "Check All Versions";
            }
        }

        private void AppendUpgradeLog(string message, string status)
        {
            if (string.IsNullOrEmpty(message))
                return;

            if (!Dispatcher.CheckAccess())
            {
                if (!Dispatcher.HasShutdownStarted)
                {
                    Dispatcher.Invoke(() => AppendUpgradeLog(message, status));
                }
                return;
            }

            string prefix = status switch
            {
                "Success" => "[OK] ",
                "Error" => "[ERROR] ",
                "Warning" => "[WARN] ",
                _ => ""
            };

            UpgradeLogTextBox.AppendText($"{prefix}{message}\n");
            UpgradeLogTextBox.ScrollToEnd();
        }

        /// <summary>
        /// Runs a full per-server upgrade flow:
        /// 1) Execute all applicable upgrades (migrations)
        /// 2) Execute installation files (install/validate)
        /// 3) Log installation history
        /// Returns a compact result summary for aggregation by the caller.
        /// </summary>
        private async Task<PerServerUpgradeResult> RunUpgradeAsync(
            ServerConnection server,
            string installerConnectionString,
            string installedVersion,
            string targetVersion,
            ScriptProvider provider,
            IProgress<InstallationProgress>? progress,
            CancellationToken cancellationToken)
        {
            int upgradesSucceeded = 0;
            int upgradesFailed = 0;
            int stepsSucceeded = 0;
            int stepsFailed = 0;

            try
            {
                AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Applying upgrades from v{installedVersion} to v{targetVersion}...", "Info");

                var (upSuccess, upFailure, upCount) = await InstallationService.ExecuteAllUpgradesAsync(
                    provider,
                    installerConnectionString,
                    installedVersion,
                    targetVersion,
                    progress,
                    cancellationToken).ConfigureAwait(false);

                upgradesSucceeded = upSuccess;
                upgradesFailed = upFailure;

                AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Upgrades applied: {upCount} (succeeded: {upSuccess}, failed: {upFailure})", upFailure == 0 ? "Success" : "Warning");

                // Execute installation (files, validations)
                bool cleanInstall = false;
                bool resetSchedule = false;

                var installResult = await InstallationService.ExecuteInstallationAsync(
                    installerConnectionString,
                    provider,
                    cleanInstall,
                    resetSchedule,
                    progress,
                    preValidationAction: null,
                    cancellationToken).ConfigureAwait(false);

                stepsSucceeded = installResult.FilesSucceeded;
                stepsFailed = installResult.FilesFailed;

                AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Installation completed: stepsSucceeded={stepsSucceeded}, stepsFailed={stepsFailed}", installResult.Success ? "Success" : "Warning");

                // Try to log installation history (best-effort)
                try
                {
                    await InstallationService.LogInstallationHistoryAsync(
                        installerConnectionString,
                        targetVersion,
                        targetVersion,
                        installResult.StartTime,
                        installResult.FilesSucceeded,
                        installResult.FilesFailed,
                        installResult.Success,
                        progress).ConfigureAwait(false);

                    AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Installation history recorded", "Success");
                }
                catch (Exception ex)
                {
                    AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Could not record installation history: {ex.Message}", "Warning");
                }

                bool overallSuccess = (upgradesFailed == 0 && stepsFailed == 0);
                return new PerServerUpgradeResult(server.Id, server.DisplayNameWithIntent, upgradesSucceeded, upgradesFailed, stepsSucceeded, stepsFailed, overallSuccess);
            }
            catch (OperationCanceledException)
            {
                AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Upgrade cancelled by user.", "Warning");
                return new PerServerUpgradeResult(server.Id, server.DisplayNameWithIntent, upgradesSucceeded, upgradesFailed, stepsSucceeded, stepsFailed, false);
            }
            catch (Exception ex)
            {
                AppendUpgradeLog($"[{server.DisplayNameWithIntent}] Upgrade failed: {ex.Message}", "Error");
                return new PerServerUpgradeResult(server.Id, server.DisplayNameWithIntent, upgradesSucceeded, upgradesFailed, stepsSucceeded, stepsFailed, false);
            }
        }

        private async void UpgradeAll_Click(object sender, RoutedEventArgs e)
        {
            string appVersion = GetAppVersion();

            // Confirm with the user
            var confirm = MessageBox.Show(
                $"Upgrade {OutdatedCount} server(s) to v{appVersion}?\n\n" +
                "All servers will be upgraded in parallel.",
                "Confirm Bulk Upgrade",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (confirm != MessageBoxResult.Yes) return;

            UpgradeProgressPanel.Visibility = Visibility.Visible;
            UpgradeAllButton.IsEnabled = false;
            CancelUpgradeButton.Visibility = Visibility.Visible;
            CancelUpgradeButton.IsEnabled = true;

            // Run upgrades in parallel using InstallationService directly
            // Reuse results from CheckAllVersions_Click when available

            // Prepare UI and log
            UpgradeLogTextBox.Clear();
            UpgradeProgressBar.Value = 0;
            UpgradeProgressText.Text = "Preparing upgrades...";

            if (_lastVersionCheckResults == null)
            {
                AppendUpgradeLog("No version scan results found. Please run 'Check All Versions' first.", "Warning");
                UpgradeAllButton.IsEnabled = true;
                CancelUpgradeButton.IsEnabled = false;
                UpgradeProgressText.Text = "Ready";
                return;
            }

            // create cancellation token source for this run
            _upgradeCts?.Dispose();
            _upgradeCts = new CancellationTokenSource();
            var cancellationToken = _upgradeCts.Token;

            var provider = ScriptProvider.FromEmbeddedResources();

            // Store the number of servers we are upgrading to calculate the denominator for the average
            int totalServersToUpgrade = _lastVersionCheckResults.Count(r => r.NeedsUpgrade);

            // Thread-safe dictionary to track the most recent progress of each server by its ID
            var serverProgressMap = new System.Collections.Concurrent.ConcurrentDictionary<string, double>();

            // Change the signature to accept a tuple: (ServerId, InstallationProgress)
            var centralProgress = new Progress<(string ServerId, InstallationProgress p)>(report =>
            {
                var serverId = report.ServerId;
                var p = report.p;

                Dispatcher.Invoke(() =>
                {
                    /* Only update the map if the payload actually contains progress data */
                    bool progressChanged = false;

                    if (p.ProgressPercent.HasValue)
                    {
                        serverProgressMap[serverId] = p.ProgressPercent.Value;
                        progressChanged = true;
                    }
                    else if (p.TotalSteps > 0)
                    {
                        serverProgressMap[serverId] = ((double)p.CurrentStep / (double)p.TotalSteps) * 100.0;
                        progressChanged = true;
                    }

                    /* Recalculate progress ONLY if a numeric update occurred */
                    if (progressChanged && !serverProgressMap.IsEmpty)
                    {
                        double overallProgress = serverProgressMap.Values.Sum() / totalServersToUpgrade;
                        UpgradeProgressBar.Value = overallProgress;
                    }

                    /* Update status text if present */
                    if (!string.IsNullOrEmpty(p.Message))
                    {
                        UpgradeProgressText.Text = p.Message;
                    }

                    /* Append to log while filtering out Debug messages */
                    if (p.Status != "Debug")
                    {
                        AppendUpgradeLog(p.Message, p.Status);
                    }
                });
            });

            var upgradeTargets = new List<(ServerConnection Server, string InstallerConnectionString, string InstalledVersion)>();

            foreach (var entry in _lastVersionCheckResults.Where(r => r.NeedsUpgrade))
            {
                var server = entry.Server;
                try
                {
                    // Determine credential values (if applicable)
                    string? username = null;
                    string? password = null;
                    bool useWindowsAuth = server.UseWindowsAuth;
                    bool useEntraAuth = server.AuthenticationType == AuthenticationTypes.EntraMFA;

                    if (server.AuthenticationType == AuthenticationTypes.SqlServer ||
                        server.AuthenticationType == AuthenticationTypes.EntraMFA)
                    {
                        var cred = _credentialService.GetCredential(server.Id);
                        if (cred.HasValue)
                        {
                            username = cred.Value.Username;
                            password = cred.Value.Password;
                        }
                    }

                    // Build a connection string suitable for installation operations (connect to master)
                    string installerConnStr = InstallationService.BuildConnectionString(
                        server.ServerName,
                        useWindowsAuth,
                        username,
                        password,
                        server.EncryptMode,
                        server.TrustServerCertificate,
                        useEntraAuth);

                    upgradeTargets.Add((server, installerConnStr, entry.InstalledVersion ?? "Unknown"));
                    AppendUpgradeLog($"Queued '{server.DisplayNameWithIntent}' for upgrade (v{entry.InstalledVersion ?? "Unknown"})", "Info");
                }
                catch (Exception ex)
                {
                    AppendUpgradeLog($"Failed to prepare '{server.DisplayNameWithIntent}': {ex.Message}", "Warning");
                }
            }

            if (upgradeTargets.Count == 0)
            {
                AppendUpgradeLog("No servers require upgrade.", "Info");
                UpgradeProgressText.Text = "No upgrades required";
                UpgradeAllButton.IsEnabled = true;
                CancelUpgradeButton.IsEnabled = false;
                CancelUpgradeButton.Visibility = Visibility.Collapsed;
                _upgradeCts?.Dispose();
                _upgradeCts = null;
                return;
            }

            // Run per-server workflows in parallel, using per-server progress wrappers so messages are prefixed.
            var tasks = upgradeTargets.Select(t =>
            {
                IProgress<InstallationProgress> serverProgress = new Progress<InstallationProgress>(p =>
                {
                    var prefixed = new InstallationProgress
                    {
                        Message = $"[{t.Server.DisplayNameWithIntent}] {p.Message}",
                        Status = p.Status,
                        ProgressPercent = p.ProgressPercent,
                        CurrentStep = p.CurrentStep,
                        TotalSteps = p.TotalSteps
                    };

                    // Report the unique server ID alongside the progress payload
                    ((IProgress<(string, InstallationProgress)>)centralProgress).Report((t.Server.Id, prefixed));
                });

                return RunUpgradeAsync(t.Server, t.InstallerConnectionString, t.InstalledVersion, appVersion, provider, serverProgress, cancellationToken);
            }).ToList();

            PerServerUpgradeResult[] results;
            try
            {
                results = await Task.WhenAll(tasks);
            }
            finally
            {
                // ensure CTS disposed in either case
                _upgradeCts?.Dispose();
                _upgradeCts = null;
            }

            // Map to aggregator inputs
            var inputs = results.Select(r => new AggregationInput(
    r.Success,
    r.UpgradesSucceeded,
    r.UpgradesFailed,
    r.StepsSucceeded,
    r.StepsFailed)).ToList();

            // Aggregate results by server success/failure and step counts
            var agg = UpgradeAggregator.Aggregate(inputs);

            // After all complete: show summary (include server counts and step counts)
            UpgradeProgressText.Text = $"Complete: {agg.ServerSuccessCount} of {upgradeTargets.Count} servers upgraded.";
            AppendUpgradeLog($"Bulk upgrade completed: {agg.ServerSuccessCount} succeeded, {agg.ServerFailCount} failed. {agg.StepsSucceeded} steps succeeded, {agg.StepsFailed} steps failed", agg.StepsFailed == 0 ? "Success" : "Warning");

            UpgradeAllButton.IsEnabled = false;
            CancelUpgradeButton.IsEnabled = false;
            CancelUpgradeButton.Visibility = Visibility.Collapsed;
        }

        // UI handler to cancel bulk upgrades. Wire this to a Cancel button in the XAML (e.g., CancelUpgradeButton).
        private void CancelUpgrade_Click(object sender, RoutedEventArgs e)
        {
            if (_upgradeCts != null && !_upgradeCts.IsCancellationRequested)
            {
                _upgradeCts.Cancel();
                AppendUpgradeLog("User requested cancellation of bulk upgrades.", "Warning");
                CancelUpgradeButton.IsEnabled = false;
            }
        }

        private void ExcludedDatabases_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is not ServerVersionInfo entry)
            {
                MessageBox.Show(
                    "Please select a server to configure excluded databases.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var dialog = new ExcludedDatabasesDialog(_serverManager, entry.Server) { Owner = this };
            if (dialog.ShowDialog() == true && dialog.ExclusionsModified)
            {
                ServersModified = true;
            }
        }

        private async void PurgeNow_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is not ServerVersionInfo entry)
            {
                MessageBox.Show(
                    "Please select a server to purge.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var server = entry.Server;
            var dialog = new PurgeNowDialog(server.DisplayNameWithIntent) { Owner = this };
            if (dialog.ShowDialog() != true) return;

            Cursor = System.Windows.Input.Cursors.Wait;
            try
            {
                var result = await _serverManager.RunDataRetentionAsync(
                    server,
                    dialog.RetentionDaysOverride);

                bool wasTruncate = dialog.RetentionDaysOverride == 0;
                string body;
                if (wasTruncate)
                {
                    body = $"All collector tables truncated.\n\n" +
                           $"Rows wiped: {result.RowsDeleted:N0}\n" +
                           $"Tables affected: {result.TableCount}\n" +
                           $"Status: {result.Status}\n" +
                           $"Duration: {result.DurationMs} ms";
                }
                else
                {
                    body = $"Purge complete.\n\n" +
                           $"Rows deleted: {result.RowsDeleted:N0}\n" +
                           $"Tables touched: {result.TableCount}\n" +
                           $"Status: {result.Status}\n" +
                           $"Duration: {result.DurationMs} ms";
                }

                if (!string.Equals(result.Status, "SUCCESS", StringComparison.Ordinal) &&
                    !string.IsNullOrEmpty(result.Message))
                {
                    body += $"\n\nMessage: {result.Message}";
                }

                MessageBox.Show(this, body, "Purge Complete",
                    MessageBoxButton.OK,
                    string.Equals(result.Status, "SUCCESS", StringComparison.Ordinal)
                        ? MessageBoxImage.Information
                        : MessageBoxImage.Warning);
            }
            catch (Exception ex)
            {
                MessageBox.Show(this,
                    $"Failed to run purge on '{server.DisplayNameWithIntent}':\n\n{ex.Message}",
                    "Purge Failed",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
            finally
            {
                Cursor = null;
            }
        }

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = Helpers.TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                        Clipboard.SetDataObject(cellContent, false);
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid?.SelectedItem != null)
                    Clipboard.SetDataObject(Helpers.TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new System.Text.StringBuilder();
                    var headers = new System.Collections.Generic.List<string>();
                    foreach (var column in dataGrid.Columns)
                        headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                    sb.AppendLine(string.Join("\t", headers));
                    foreach (var item in dataGrid.Items)
                        sb.AppendLine(Helpers.TabHelpers.GetRowAsText(dataGrid, item));
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var dialog = new Microsoft.Win32.SaveFileDialog
                    {
                        FileName = $"servers_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };
                    if (dialog.ShowDialog() == true)
                    {
                        var sb = new System.Text.StringBuilder();
                        var headers = new System.Collections.Generic.List<string>();
                        foreach (var column in dataGrid.Columns)
                            headers.Add(Helpers.TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column)));
                        sb.AppendLine(string.Join(",", headers));
                        foreach (var item in dataGrid.Items)
                        {
                            var values = Helpers.TabHelpers.GetRowValues(dataGrid, item);
                            sb.AppendLine(string.Join(",", values.Select(v => Helpers.TabHelpers.EscapeCsvField(v))));
                        }
                        System.IO.File.WriteAllText(dialog.FileName, sb.ToString());
                    }
                }
            }
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = ServersModified;
            Close();
        }
    }
}
