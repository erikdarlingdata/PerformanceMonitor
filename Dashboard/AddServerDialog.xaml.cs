/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Installer.Core;
using Installer.Core.Models;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class AddServerDialog : Window
    {
        private enum DialogState
        {
            Initial,
            Connected_NoDatabase,
            Connected_NeedsUpgrade,
            Connected_Current,
            Installing,
            InstallComplete,
            MonitoringCredentials
        }

        public ServerConnection ServerConnection { get; private set; }
        public string? Username { get; private set; }
        public string? Password { get; private set; }
        private bool _isEditMode;

        private CancellationTokenSource? _installCts;
        private Installer.Core.Models.ServerInfo? _coreServerInfo;
        private string? _installedVersion;
        private InstallationResult? _installResult;
        private string? _reportPath;
        private DialogState _currentState = DialogState.Initial;

        public AddServerDialog()
        {
            InitializeComponent();
            _isEditMode = false;
            ServerConnection = new ServerConnection();
            Title = "Add SQL Server";
        }

        public AddServerDialog(ServerConnection existingServer)
        {
            InitializeComponent();
            _isEditMode = true;
            ServerConnection = existingServer;
            Title = "Edit SQL Server";

            DisplayNameTextBox.Text = existingServer.DisplayName;
            ServerNameTextBox.Text = existingServer.ServerName;
            DescriptionTextBox.Text = existingServer.Description;
            IsFavoriteCheckBox.IsChecked = existingServer.IsFavorite;
            MonthlyCostTextBox.Text = existingServer.MonthlyCostUsd.ToString(System.Globalization.CultureInfo.InvariantCulture);

            // Load encryption settings
            EncryptModeComboBox.SelectedIndex = existingServer.EncryptMode switch
            {
                "Mandatory" => 1,
                "Strict" => 2,
                _ => 0 // Optional
            };
            TrustServerCertificateCheckBox.IsChecked = existingServer.TrustServerCertificate;
            ReadOnlyIntentCheckBox.IsChecked = existingServer.ReadOnlyIntent;

            if (existingServer.AuthenticationType == AuthenticationTypes.EntraMFA)
            {
                EntraMfaAuthRadio.IsChecked = true;

                var credentialService = new CredentialService();
                var cred = credentialService.GetCredential(existingServer.Id);
                if (cred.HasValue && !string.IsNullOrEmpty(cred.Value.Username))
                {
                    EntraMfaUsernameBox.Text = cred.Value.Username;
                }
            }
            else if (existingServer.AuthenticationType == AuthenticationTypes.SqlServer)
            {
                SqlAuthRadio.IsChecked = true;

                var credentialService = new CredentialService();
                var cred = credentialService.GetCredential(existingServer.Id);
                if (cred.HasValue)
                {
                    UsernameTextBox.Text = cred.Value.Username;
                    PasswordBox.Password = cred.Value.Password;
                }
            }
            else
            {
                WindowsAuthRadio.IsChecked = true;
            }
        }

        private void AuthType_Changed(object sender, RoutedEventArgs e)
        {
            if (SqlAuthPanel != null && EntraMfaPanel != null)
            {
                SqlAuthPanel.Visibility = SqlAuthRadio.IsChecked == true
                    ? System.Windows.Visibility.Visible
                    : System.Windows.Visibility.Collapsed;

                EntraMfaPanel.Visibility = EntraMfaAuthRadio.IsChecked == true
                    ? System.Windows.Visibility.Visible
                    : System.Windows.Visibility.Collapsed;
            }
        }

        private string GetSelectedEncryptMode()
        {
            return EncryptModeComboBox.SelectedIndex switch
            {
                1 => "Mandatory",
                2 => "Strict",
                _ => "Optional"
            };
        }

        private static SqlConnectionEncryptOption ParseEncryptOption(string mode)
        {
            return mode switch
            {
                "Mandatory" => SqlConnectionEncryptOption.Mandatory,
                "Strict" => SqlConnectionEncryptOption.Strict,
                _ => SqlConnectionEncryptOption.Optional
            };
        }

        private SqlConnectionStringBuilder BuildConnectionBuilder()
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = ServerNameTextBox.Text.Trim(),
                InitialCatalog = "PerformanceMonitor",
                ApplicationName = "PerformanceMonitorDashboard",
                ConnectTimeout = 10,
                TrustServerCertificate = TrustServerCertificateCheckBox.IsChecked == true,
                Encrypt = ParseEncryptOption(GetSelectedEncryptMode()),
                ApplicationIntent = ReadOnlyIntentCheckBox.IsChecked == true
                    ? ApplicationIntent.ReadOnly
                    : ApplicationIntent.ReadWrite
            };

            if (WindowsAuthRadio.IsChecked == true)
            {
                builder.IntegratedSecurity = true;
            }
            else if (SqlAuthRadio.IsChecked == true)
            {
                builder.IntegratedSecurity = false;
                builder.UserID = UsernameTextBox.Text.Trim();
                builder.Password = PasswordBox.Password;
            }
            else if (EntraMfaAuthRadio.IsChecked == true)
            {
                builder.IntegratedSecurity = false;
                builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive;
                var mfaUsername = EntraMfaUsernameBox.Text.Trim();
                if (!string.IsNullOrEmpty(mfaUsername))
                    builder.UserID = mfaUsername;
            }

            return builder;
        }

        private string BuildInstallerConnectionString()
        {
            string server = ServerNameTextBox.Text.Trim();
            bool useWindowsAuth = WindowsAuthRadio.IsChecked == true;
            bool useEntraAuth = EntraMfaAuthRadio.IsChecked == true;
            string? username = null;
            string? password = null;

            if (SqlAuthRadio.IsChecked == true)
            {
                username = UsernameTextBox.Text.Trim();
                password = PasswordBox.Password;
            }
            else if (useEntraAuth)
            {
                username = EntraMfaUsernameBox.Text.Trim();
            }

            return InstallationService.BuildConnectionString(
                server,
                useWindowsAuth,
                username,
                password,
                GetSelectedEncryptMode(),
                TrustServerCertificateCheckBox.IsChecked == true,
                useEntraAuth);
        }

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

        /// <summary>
        /// Normalize a version string to 3-part for comparison (e.g., "2.4.1.0" -> "2.4.1").
        /// </summary>
        private static string NormalizeVersion(string version)
        {
            if (Version.TryParse(version, out var parsed))
            {
                return $"{parsed.Major}.{parsed.Minor}.{parsed.Build}";
            }
            return version;
        }

        private bool ValidateInputs()
        {
            if (string.IsNullOrWhiteSpace(ServerNameTextBox.Text))
            {
                MessageBox.Show(
                    "Please enter a server name or address.",
                    "Validation Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning
                );
                return false;
            }

            if (SqlAuthRadio.IsChecked == true && string.IsNullOrWhiteSpace(UsernameTextBox.Text))
            {
                MessageBox.Show(
                    "Please enter a username for SQL Server authentication.",
                    "Validation Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning
                );
                return false;
            }

            return true;
        }

        private async System.Threading.Tasks.Task<(bool Connected, string? ErrorMessage, bool MfaCancelled, string? ServerVersion)> RunConnectionTestAsync(Button triggerButton)
        {
            triggerButton.IsEnabled = false;
            SaveButton.IsEnabled = false;

            StatusText.Text = EntraMfaAuthRadio.IsChecked == true
                ? "Testing connection — please complete authentication in the popup window..."
                : "Testing connection...";
            StatusText.Visibility = System.Windows.Visibility.Visible;

            bool connected = false;
            string? errorMessage = null;
            bool mfaCancelled = false;
            string? serverVersion = null;
            try
            {
                /* Connect to master (not PerformanceMonitor) so the test succeeds
                   even when the database doesn't exist yet — installation detection
                   happens after the connection test in DetectDatabaseStatusAsync() */
                var builder = BuildConnectionBuilder();
                builder.InitialCatalog = "master";
                await using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();
                using var cmd = new SqlCommand("SELECT @@VERSION", connection);
                var version = await cmd.ExecuteScalarAsync() as string;
                serverVersion = version?.Split('\n')[0]?.Trim();
                connected = true;
            }
            catch (Exception ex)
            {
                connected = false;
                errorMessage = ex.Message;
                if (EntraMfaAuthRadio.IsChecked == true && MfaAuthenticationHelper.IsMfaCancelledException(ex))
                    mfaCancelled = true;
            }
            finally
            {
                triggerButton.IsEnabled = true;
                SaveButton.IsEnabled = true;
                StatusText.Text = string.Empty;
                StatusText.Visibility = System.Windows.Visibility.Collapsed;
            }

            return (connected, errorMessage, mfaCancelled, serverVersion);
        }

        private async void CheckForUpdates_Click(object sender, RoutedEventArgs e)
        {
            if (!ValidateInputs()) return;

            CheckForUpdatesButton.IsEnabled = false;
            CheckForUpdatesButton.Content = "Checking...";

            try
            {
                await DetectDatabaseStatusAsync();
            }
            finally
            {
                CheckForUpdatesButton.IsEnabled = true;
                CheckForUpdatesButton.Content = "Check for Updates";
            }
        }

        private async void TestConnection_Click(object sender, RoutedEventArgs e)
        {
            if (!ValidateInputs()) return;

            var (connected, errorMessage, mfaCancelled, serverVersion) = await RunConnectionTestAsync(TestConnectionButton);

            if (connected)
            {
                var message = serverVersion != null
                    ? $"Successfully connected to {ServerNameTextBox.Text}!\n\n{serverVersion}"
                    : $"Successfully connected to {ServerNameTextBox.Text}!";
                MessageBox.Show(
                    message,
                    "Connection Successful",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );

                /* After successful connection, check database status */
                await DetectDatabaseStatusAsync();
            }
            else if (mfaCancelled)
            {
                MessageBox.Show(
                    "Authentication was cancelled. Click Test to try again.",
                    "Authentication Cancelled",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning
                );
            }
            else
            {
                var detail = errorMessage != null ? $"\n\nError: {errorMessage}" : string.Empty;
                MessageBox.Show(
                    $"Could not connect to {ServerNameTextBox.Text}.{detail}\n\nPlease check:\n" +
                    "• Server name/address is correct\n" +
                    "• Server is accessible from this machine\n" +
                    "• Firewall allows SQL Server connections\n" +
                    "• SQL Server service is running\n" +
                    "• You have the 'PerformanceMonitor' database and access to it",
                    "Connection Test Failed",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private async System.Threading.Tasks.Task DetectDatabaseStatusAsync()
        {
            try
            {
                StatusText.Text = "Checking database status...";
                StatusText.Visibility = Visibility.Visible;

                string installerConnStr = BuildInstallerConnectionString();
                string appVersion = GetAppVersion();

                /* Test connection via Installer.Core to get ServerInfo */
                _coreServerInfo = await InstallationService.TestConnectionAsync(installerConnStr);

                if (_coreServerInfo == null || !_coreServerInfo.IsConnected)
                {
                    StatusText.Text = string.Empty;
                    StatusText.Visibility = Visibility.Collapsed;
                    return;
                }

                if (!_coreServerInfo.IsSupportedVersion)
                {
                    DatabaseStatusText.Text = $"Warning: {_coreServerInfo.ProductMajorVersionName} is not supported. SQL Server 2016+ is required.";
                    DatabaseStatusPanel.Visibility = Visibility.Visible;
                    InstallUpgradeButton.Visibility = Visibility.Collapsed;
                    SkipInstallText.Visibility = Visibility.Collapsed;
                    StatusText.Text = string.Empty;
                    StatusText.Visibility = Visibility.Collapsed;
                    return;
                }

                /* Check installed version */
                _installedVersion = await InstallationService.GetInstalledVersionAsync(installerConnStr);

                if (_installedVersion == null)
                {
                    TransitionToState(DialogState.Connected_NoDatabase);
                }
                else
                {
                    string normalizedInstalled = NormalizeVersion(_installedVersion);
                    string normalizedApp = NormalizeVersion(appVersion);

                    if (Version.TryParse(normalizedInstalled, out var installedVer) &&
                        Version.TryParse(normalizedApp, out var appVer))
                    {
                        if (installedVer < appVer)
                        {
                            TransitionToState(DialogState.Connected_NeedsUpgrade);
                        }
                        else
                        {
                            TransitionToState(DialogState.Connected_Current);
                        }
                    }
                    else
                    {
                        TransitionToState(DialogState.Connected_Current);
                    }
                }
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Could not check database status: {ex.Message}";
                StatusText.Visibility = Visibility.Visible;
            }
        }

        private void TransitionToState(DialogState newState)
        {
            _currentState = newState;
            string appVersion = GetAppVersion();

            /* Reset panel visibility */
            DatabaseStatusPanel.Visibility = Visibility.Collapsed;
            InstallationPanel.Visibility = Visibility.Collapsed;
            MonitoringCredsPanel.Visibility = Visibility.Collapsed;
            ViewReportButton.Visibility = Visibility.Collapsed;
            StatusText.Text = string.Empty;
            StatusText.Visibility = Visibility.Collapsed;
            InstallUpgradeButton.Visibility = Visibility.Visible;
            SkipInstallText.Visibility = Visibility.Visible;

            switch (newState)
            {
                case DialogState.Connected_NoDatabase:
                    DatabaseStatusText.Text = $"No PerformanceMonitor database found on this server. Install v{appVersion}?";
                    InstallUpgradeButton.Content = "Install Now";
                    DatabaseStatusPanel.Visibility = Visibility.Visible;
                    InstallationPanel.Visibility = Visibility.Visible;
                    SaveButton.IsEnabled = false;
                    break;

                case DialogState.Connected_NeedsUpgrade:
                    string normalizedInstalled = NormalizeVersion(_installedVersion!);
                    DatabaseStatusText.Text = $"v{normalizedInstalled} installed — v{appVersion} available";
                    InstallUpgradeButton.Content = "Upgrade Now";
                    DatabaseStatusPanel.Visibility = Visibility.Visible;
                    InstallationPanel.Visibility = Visibility.Visible;
                    SaveButton.IsEnabled = true;
                    break;

                case DialogState.Connected_Current:
                    string normalizedCurrent = NormalizeVersion(_installedVersion!);
                    DatabaseStatusText.Text = $"PerformanceMonitor v{normalizedCurrent} is up to date.";
                    InstallUpgradeButton.Visibility = Visibility.Collapsed;
                    SkipInstallText.Visibility = Visibility.Collapsed;
                    DatabaseStatusPanel.Visibility = Visibility.Visible;
                    SaveButton.IsEnabled = true;
                    break;

                case DialogState.Installing:
                    DatabaseStatusPanel.Visibility = Visibility.Collapsed;
                    InstallationPanel.Visibility = Visibility.Visible;
                    AdvancedOptionsExpander.IsEnabled = false;
                    CancelInstallButton.IsEnabled = true;
                    SetFormEnabled(false);
                    break;

                case DialogState.InstallComplete:
                    InstallationPanel.Visibility = Visibility.Visible;
                    AdvancedOptionsExpander.IsEnabled = false;
                    CancelInstallButton.IsEnabled = false;
                    SetFormEnabled(true);
                    if (_reportPath != null)
                    {
                        ViewReportButton.Visibility = Visibility.Visible;
                    }
                    /* Transition to monitoring credentials if using SQL auth */
                    if (SqlAuthRadio.IsChecked == true)
                    {
                        TransitionToState(DialogState.MonitoringCredentials);
                        return;
                    }
                    SaveButton.IsEnabled = true;
                    SaveButton.Content = "Save & Connect";
                    break;

                case DialogState.MonitoringCredentials:
                    InstallationPanel.Visibility = Visibility.Visible;
                    AdvancedOptionsExpander.IsEnabled = false;
                    CancelInstallButton.IsEnabled = false;
                    MonitoringCredsPanel.Visibility = Visibility.Visible;
                    if (_reportPath != null)
                    {
                        ViewReportButton.Visibility = Visibility.Visible;
                    }
                    SetFormEnabled(true);
                    SaveButton.IsEnabled = true;
                    SaveButton.Content = "Save & Connect";
                    break;

                case DialogState.Initial:
                default:
                    SetFormEnabled(true);
                    SaveButton.IsEnabled = true;
                    SaveButton.Content = "Save";
                    break;
            }
        }

        private async void InstallOrUpgrade_Click(object sender, RoutedEventArgs e)
        {
            TransitionToState(DialogState.Installing);
            InstallLogTextBox.Clear();
            InstallProgressBar.Value = 0;
            InstallStatusText.Text = "Preparing installation...";

            _installCts = new CancellationTokenSource();
            var cancellationToken = _installCts.Token;

            try
            {
                var provider = ScriptProvider.FromEmbeddedResources();
                string installerConnStr = BuildInstallerConnectionString();
                string appVersion = GetAppVersion();
                bool cleanInstall = CleanInstallCheckBox.IsChecked == true;
                bool resetSchedule = ResetScheduleCheckBox.IsChecked == true;
                bool runValidation = ValidationCheckBox.IsChecked == true;
                bool installDeps = InstallDepsCheckBox.IsChecked == true;

                var progress = new Progress<InstallationProgress>(p =>
                {
                    Dispatcher.Invoke(() =>
                    {
                        /* Update progress bar */
                        if (p.ProgressPercent.HasValue)
                        {
                            InstallProgressBar.Value = p.ProgressPercent.Value;
                        }

                        /* Update status text */
                        if (!string.IsNullOrEmpty(p.Message))
                        {
                            InstallStatusText.Text = p.Message;
                        }

                        /* Append to log (filter out Debug messages) */
                        if (p.Status != "Debug")
                        {
                            AppendInstallLog(p.Message, p.Status);
                        }
                    });
                });

                /* Run upgrades if applicable (existing database, not clean install) */
                if (!cleanInstall && _installedVersion != null)
                {
                    AppendInstallLog($"Checking for upgrades from v{NormalizeVersion(_installedVersion)} to v{appVersion}...", "Info");

                    var (upgradeSuccess, upgradeFailure, upgradeCount) = await InstallationService.ExecuteAllUpgradesAsync(
                        provider,
                        installerConnStr,
                        _installedVersion,
                        appVersion,
                        progress,
                        cancellationToken);

                    if (upgradeCount > 0)
                    {
                        AppendInstallLog($"Upgrades complete: {upgradeSuccess} succeeded, {upgradeFailure} failed", upgradeFailure == 0 ? "Success" : "Warning");
                    }

                    if (upgradeFailure > 0)
                    {
                        AppendInstallLog("Upgrade failures detected. Continuing with full installation to ensure consistency...", "Warning");
                    }
                }

                /* Run main installation */
                AppendInstallLog("Starting main installation...", "Info");

                Func<System.Threading.Tasks.Task>? preValidationAction = null;
                if (installDeps)
                {
                    preValidationAction = async () =>
                    {
                        AppendInstallLog("Installing community dependencies...", "Info");
                        using var depInstaller = new DependencyInstaller();
                        await depInstaller.InstallDependenciesAsync(installerConnStr, progress, cancellationToken);
                    };
                }

                _installResult = await InstallationService.ExecuteInstallationAsync(
                    installerConnStr,
                    provider,
                    cleanInstall,
                    resetSchedule,
                    progress,
                    preValidationAction,
                    cancellationToken);

                /* Log installation history */
                try
                {
                    AppendInstallLog("Recording installation history...", "Info");
                    await InstallationService.LogInstallationHistoryAsync(
                        installerConnStr,
                        appVersion,
                        appVersion,
                        _installResult.StartTime,
                        _installResult.FilesSucceeded,
                        _installResult.FilesFailed,
                        _installResult.Success,
                        progress);
                    AppendInstallLog("Installation history recorded", "Success");
                }
                catch (Exception ex)
                {
                    AppendInstallLog($"Could not record installation history: {ex.Message}", "Warning");
                }

                /* Run validation if requested */
                if (runValidation && _installResult.Success)
                {
                    try
                    {
                        AppendInstallLog("Running post-install validation...", "Info");
                        var (collectorsSucceeded, collectorsFailed) = await InstallationService.RunValidationAsync(
                            installerConnStr,
                            progress,
                            cancellationToken);
                        AppendInstallLog($"Validation: {collectorsSucceeded} collectors succeeded, {collectorsFailed} failed",
                            collectorsFailed == 0 ? "Success" : "Warning");
                    }
                    catch (Exception ex)
                    {
                        AppendInstallLog($"Validation failed: {ex.Message}", "Warning");
                    }
                }

                /* Generate summary report */
                try
                {
                    _reportPath = InstallationService.GenerateSummaryReport(
                        ServerNameTextBox.Text.Trim(),
                        _coreServerInfo?.SqlServerVersion ?? "",
                        _coreServerInfo?.SqlServerEdition ?? "",
                        appVersion,
                        _installResult);
                    AppendInstallLog($"Report saved: {_reportPath}", "Info");
                }
                catch (Exception ex)
                {
                    AppendInstallLog($"Could not generate report: {ex.Message}", "Warning");
                }

                /* Update final status */
                await Dispatcher.InvokeAsync(() =>
                {
                    InstallProgressBar.Value = 100;
                    if (_installResult.Success)
                    {
                        InstallStatusText.Text = "Installation completed successfully!";
                        AppendInstallLog("Installation completed successfully!", "Success");
                    }
                    else
                    {
                        InstallStatusText.Text = $"Installation completed with {_installResult.FilesFailed} error(s).";
                        AppendInstallLog($"Installation completed with {_installResult.FilesFailed} error(s).", "Error");
                    }

                    TransitionToState(DialogState.InstallComplete);
                });
            }
            catch (OperationCanceledException)
            {
                Dispatcher.Invoke(() =>
                {
                    InstallStatusText.Text = "Installation cancelled.";
                    AppendInstallLog("Installation was cancelled by user.", "Warning");
                    InstallProgressBar.Value = 0;
                    TransitionToState(DialogState.Initial);
                    DatabaseStatusPanel.Visibility = Visibility.Visible;
                    InstallationPanel.Visibility = Visibility.Visible;
                    AdvancedOptionsExpander.IsEnabled = true;
                });
            }
            catch (Exception ex)
            {
                Dispatcher.Invoke(() =>
                {
                    InstallStatusText.Text = $"Installation failed: {ex.Message}";
                    AppendInstallLog($"Fatal error: {ex.Message}", "Error");
                    TransitionToState(DialogState.Initial);
                    DatabaseStatusPanel.Visibility = Visibility.Visible;
                    InstallationPanel.Visibility = Visibility.Visible;
                    AdvancedOptionsExpander.IsEnabled = true;
                });
            }
            finally
            {
                _installCts?.Dispose();
                _installCts = null;
            }
        }

        private void CancelInstall_Click(object sender, RoutedEventArgs e)
        {
            _installCts?.Cancel();
            CancelInstallButton.IsEnabled = false;
            InstallStatusText.Text = "Cancelling...";
        }

        private void AppendInstallLog(string message, string status)
        {
            if (string.IsNullOrEmpty(message))
                return;

            if (!Dispatcher.CheckAccess())
            {
                Dispatcher.Invoke(() => AppendInstallLog(message, status));
                return;
            }

            string prefix = status switch
            {
                "Success" => "[OK] ",
                "Error" => "[ERROR] ",
                "Warning" => "[WARN] ",
                _ => ""
            };

            InstallLogTextBox.AppendText($"{prefix}{message}\n");
            InstallLogTextBox.ScrollToEnd();
        }

        private void SkipInstall_Click(object sender, MouseButtonEventArgs e)
        {
            DatabaseStatusPanel.Visibility = Visibility.Collapsed;
            InstallationPanel.Visibility = Visibility.Collapsed;
            SaveButton.IsEnabled = true;
            SaveButton.Content = "Save";
            _currentState = DialogState.Initial;
        }

        private void ViewReport_Click(object sender, RoutedEventArgs e)
        {
            if (_reportPath != null && File.Exists(_reportPath))
            {
                try
                {
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = _reportPath,
                        UseShellExecute = true
                    });
                }
                catch (Exception ex)
                {
                    MessageBox.Show(
                        $"Could not open report: {ex.Message}",
                        "Error",
                        MessageBoxButton.OK,
                        MessageBoxImage.Warning
                    );
                }
            }
        }

        private void UseSameCredsCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (MonitorCredsFieldsPanel != null)
            {
                MonitorCredsFieldsPanel.Visibility = UseSameCredsCheckBox.IsChecked == true
                    ? Visibility.Collapsed
                    : Visibility.Visible;
            }
        }

        private void SetFormEnabled(bool enabled)
        {
            ServerNameTextBox.IsEnabled = enabled;
            DisplayNameTextBox.IsEnabled = enabled;
            WindowsAuthRadio.IsEnabled = enabled;
            SqlAuthRadio.IsEnabled = enabled;
            EntraMfaAuthRadio.IsEnabled = enabled;
            UsernameTextBox.IsEnabled = enabled;
            PasswordBox.IsEnabled = enabled;
            EntraMfaUsernameBox.IsEnabled = enabled;
            EncryptModeComboBox.IsEnabled = enabled;
            TrustServerCertificateCheckBox.IsEnabled = enabled;
            ReadOnlyIntentCheckBox.IsEnabled = enabled;
            IsFavoriteCheckBox.IsEnabled = enabled;
            MonthlyCostTextBox.IsEnabled = enabled;
            DescriptionTextBox.IsEnabled = enabled;
            TestConnectionButton.IsEnabled = enabled;
            SaveButton.IsEnabled = enabled;
        }

        private async void Save_Click(object sender, RoutedEventArgs e)
        {
            if (!ValidateInputs()) return;

            /* If we just finished installing, skip re-testing the connection */
            bool skipConnectionTest = _currentState == DialogState.InstallComplete ||
                                       _currentState == DialogState.MonitoringCredentials;

            if (!skipConnectionTest)
            {
                var (connected, errorMessage, mfaCancelled, _) = await RunConnectionTestAsync(SaveButton);

                if (!connected)
                {
                    if (mfaCancelled)
                    {
                        MessageBox.Show(
                            "Authentication was cancelled. Click Save to try again, or Cancel to abort.",
                            "Authentication Cancelled",
                            MessageBoxButton.OK,
                            MessageBoxImage.Warning
                        );
                        return;
                    }

                    var detail = errorMessage != null ? $"\n\nError: {errorMessage}" : string.Empty;
                    var result = MessageBox.Show(
                        $"Could not connect to {ServerNameTextBox.Text}.{detail}\n\n" +
                        "Do you still want to save this connection?",
                        "Connection Failed",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Warning
                    );

                    if (result != MessageBoxResult.Yes)
                        return;
                }
            }

            // Determine authentication type and credentials
            string authenticationType;
            if (WindowsAuthRadio.IsChecked == true)
            {
                authenticationType = AuthenticationTypes.Windows;
                Username = null;
                Password = null;
            }
            else if (EntraMfaAuthRadio.IsChecked == true)
            {
                authenticationType = AuthenticationTypes.EntraMFA;
                Username = EntraMfaUsernameBox.Text.Trim();
                Password = null;
            }
            else
            {
                authenticationType = AuthenticationTypes.SqlServer;

                /* Use monitoring credentials if provided */
                if (_currentState == DialogState.MonitoringCredentials &&
                    UseSameCredsCheckBox.IsChecked == false &&
                    !string.IsNullOrWhiteSpace(MonitorUsernameTextBox.Text))
                {
                    Username = MonitorUsernameTextBox.Text.Trim();
                    Password = MonitorPasswordBox.Password;
                }
                else
                {
                    Username = UsernameTextBox.Text.Trim();
                    Password = PasswordBox.Password;
                }
            }

            // Use server name as display name if not provided
            var displayName = string.IsNullOrWhiteSpace(DisplayNameTextBox.Text)
                ? ServerNameTextBox.Text.Trim()
                : DisplayNameTextBox.Text.Trim();

            if (_isEditMode)
            {
                ServerConnection.DisplayName = displayName;
                ServerConnection.ServerName = ServerNameTextBox.Text.Trim();
                ServerConnection.AuthenticationType = authenticationType;
                ServerConnection.Description = DescriptionTextBox.Text.Trim();
                ServerConnection.IsFavorite = IsFavoriteCheckBox.IsChecked == true;
                ServerConnection.EncryptMode = GetSelectedEncryptMode();
                ServerConnection.TrustServerCertificate = TrustServerCertificateCheckBox.IsChecked == true;
                ServerConnection.ReadOnlyIntent = ReadOnlyIntentCheckBox.IsChecked == true;
                if (decimal.TryParse(MonthlyCostTextBox.Text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var editCost) && editCost >= 0)
                    ServerConnection.MonthlyCostUsd = editCost;
            }
            else
            {
                decimal monthlyCost = 0m;
                if (decimal.TryParse(MonthlyCostTextBox.Text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var newCost) && newCost >= 0)
                    monthlyCost = newCost;

                ServerConnection = new ServerConnection
                {
                    DisplayName = displayName,
                    ServerName = ServerNameTextBox.Text.Trim(),
                    AuthenticationType = authenticationType,
                    Description = DescriptionTextBox.Text.Trim(),
                    IsFavorite = IsFavoriteCheckBox.IsChecked == true,
                    CreatedDate = DateTime.Now,
                    LastConnected = DateTime.Now,
                    EncryptMode = GetSelectedEncryptMode(),
                    TrustServerCertificate = TrustServerCertificateCheckBox.IsChecked == true,
                    ReadOnlyIntent = ReadOnlyIntentCheckBox.IsChecked == true,
                    MonthlyCostUsd = monthlyCost
                };
            }

            DialogResult = true;
            Close();
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            _installCts?.Cancel();
            DialogResult = false;
            Close();
        }

    }
}
