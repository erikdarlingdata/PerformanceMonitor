/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Documents;
using System.Windows.Media;
using PerformanceMonitorInstallerGui.Services;
using PerformanceMonitorInstallerGui.Utilities;

namespace PerformanceMonitorInstallerGui
{
    public partial class MainWindow : Window
    {
        private readonly InstallationService _installationService;
        private CancellationTokenSource? _cancellationTokenSource;
        private string? _connectionString;
        private string? _sqlDirectory;
        private string? _monitorRootDirectory;
        private List<string>? _sqlFiles;
        private ServerInfo? _serverInfo;
        private InstallationResult? _installationResult;
        private string? _installedVersion;

        /*
        UI brushes for log output
        */
        private static readonly SolidColorBrush SuccessBrush = new(Color.FromRgb(0x4C, 0xAF, 0x50)); // Green
        private static readonly SolidColorBrush ErrorBrush = new(Color.FromRgb(0xF4, 0x43, 0x36)); // Red
        private static readonly SolidColorBrush WarningBrush = new(Color.FromRgb(0xFF, 0xC1, 0x07)); // Yellow

        /*
        Cached version strings
        Display version includes git hash suffix for UI/logs
        Assembly version is clean (e.g. "2.0.0.0") for upgrade version comparison
        */
        private static readonly string AppVersion =
            Assembly.GetExecutingAssembly()
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString()
            ?? "Unknown";

        private static readonly string AppAssemblyVersion =
            Assembly.GetExecutingAssembly().GetName().Version?.ToString()
            ?? "Unknown";

        private static readonly char[] NewLineChars = { '\r', '\n' };

        public MainWindow()
        {
            try
            {
                InitializeComponent();
                _installationService = new InstallationService();

                /*Set window title with version*/
                Title = $"Performance Monitor Installer v{AppVersion}";

                /*Find installation files after window loads*/
                Loaded += MainWindow_Loaded;
            }
            catch (Exception ex)
            {
                Logger.LogToFile("MainWindow Constructor", ex);
                MessageBox.Show(
                    $"Constructor error:\n\n{ex.GetType().Name}: {ex.Message}\n\n" +
                    $"Inner: {ex.InnerException?.Message}\n\n" +
                    $"Log file: {Logger.LogFilePath}",
                    "Constructor Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
                throw;
            }
        }

        private void MainWindow_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                FindInstallationFiles();
            }
            catch (Exception ex)
            {
                Logger.LogToFile("MainWindow_Loaded", ex);
                MessageBox.Show(this,
                    $"Loaded error:\n\n{ex.GetType().Name}: {ex.Message}\n\n" +
                    $"Inner: {ex.InnerException?.Message}\n\n" +
                    $"Log file: {Logger.LogFilePath}",
                    "Loaded Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// Find SQL installation files on startup
        /// </summary>
        private void FindInstallationFiles()
        {
            var (sqlDirectory, monitorRootDirectory, sqlFiles) = InstallationService.FindInstallationFiles();

            _sqlDirectory = sqlDirectory;
            _monitorRootDirectory = monitorRootDirectory;
            _sqlFiles = sqlFiles;

            if (sqlDirectory != null)
            {
                LogMessage($"Found {sqlFiles.Count} SQL files in: {sqlDirectory}", "Info");
            }
            else
            {
                LogMessage("WARNING: No SQL installation files found.", "Warning");
                LogMessage("Make sure the installer is in the Monitor directory or a subdirectory.", "Warning");
                InstallButton.IsEnabled = false;

                MessageBox.Show(this,
                    "No SQL installation files found.\n\nMake sure the installer is located in the Monitor directory or a subdirectory.",
                    "Files Not Found",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
            }
        }

        /// <summary>
        /// Handle authentication type change
        /// </summary>
        private void AuthType_Changed(object sender, RoutedEventArgs e)
        {
            /*Controls may not exist yet during InitializeComponent*/
            if (UsernameTextBox == null || PasswordBox == null)
                return;

            bool useSqlAuth = SqlAuthRadio.IsChecked == true;

            UsernameTextBox.IsEnabled = useSqlAuth;
            PasswordBox.IsEnabled = useSqlAuth;
            UsernameLabel.Opacity = useSqlAuth ? 1.0 : 0.5;
            PasswordLabel.Opacity = useSqlAuth ? 1.0 : 0.5;

            InvalidateConnection();
        }

        /// <summary>
        /// Invalidate cached connection when any connection parameter changes
        /// </summary>
        private void ConnectionParameter_Changed(object sender, EventArgs e)
        {
            InvalidateConnection();
        }

        /// <summary>
        /// Clear cached connection state so the user must re-test before installing
        /// </summary>
        private void InvalidateConnection()
        {
            /*Controls may not exist yet during InitializeComponent*/
            if (InstallButton == null)
                return;

            _connectionString = null;
            _serverInfo = null;
            _installedVersion = null;
            InstallButton.IsEnabled = false;
        }

        /// <summary>
        /// Test connection button click
        /// </summary>
        private async void TestConnection_Click(object sender, RoutedEventArgs e)
        {
            Logger.LogToFile("TestConnection_Click", "Method started");

            string server = ServerTextBox.Text.Trim();
            if (string.IsNullOrEmpty(server))
            {
                MessageBox.Show(this, "Please enter a server name.", "Validation Error",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            bool useWindowsAuth = WindowsAuthRadio.IsChecked == true;
            string? username = useWindowsAuth ? null : UsernameTextBox.Text.Trim();
            string? password = useWindowsAuth ? null : PasswordBox.Password;

            if (!useWindowsAuth)
            {
                if (string.IsNullOrEmpty(username))
                {
                    MessageBox.Show(this, "Please enter a username for SQL Server Authentication.",
                        "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }
                if (string.IsNullOrEmpty(password))
                {
                    MessageBox.Show(this, "Please enter a password for SQL Server Authentication.",
                        "Validation Error", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }
            }

            TestConnectionButton.IsEnabled = false;
            TestConnectionButton.Content = "Testing...";

            try
            {
                Logger.LogToFile("TestConnection_Click", "Entering try block");

                string encryption = (EncryptionComboBox.SelectedItem as System.Windows.Controls.ComboBoxItem)?.Tag?.ToString() ?? "Mandatory";
                bool trustCertificate = TrustCertificateCheckBox.IsChecked ?? false;

                Logger.LogToFile("TestConnection_Click", $"Encryption: {encryption}, TrustCert: {trustCertificate}");

                _connectionString = InstallationService.BuildConnectionString(server, useWindowsAuth, username, password, encryption, trustCertificate);

                Logger.LogToFile("TestConnection_Click", "Connection string built, clearing log");

                ClearLog();
                LogMessage("Testing connection...", "Info");

                Logger.LogToFile("TestConnection_Click", "Calling TestConnectionAsync");

                _serverInfo = await InstallationService.TestConnectionAsync(_connectionString);

                Logger.LogToFile("TestConnection_Click", $"TestConnectionAsync returned, IsConnected: {_serverInfo?.IsConnected}");

                if (_serverInfo == null)
                {
                    LogMessage("Connection test returned no result", "Error");
                    InstallButton.IsEnabled = false;
                    return;
                }

                if (_serverInfo.IsConnected)
                {
                    LogMessage("Connection successful!", "Success");
                    LogMessage($"Server: {_serverInfo.ServerName}", "Info");
                    LogMessage($"Edition: {_serverInfo.SqlServerEdition}", "Info");

                    /*Show first line of version*/
                    string[] versionLines = _serverInfo.SqlServerVersion.Split(NewLineChars,
                        StringSplitOptions.RemoveEmptyEntries);
                    if (versionLines.Length > 0)
                    {
                        LogMessage($"Version: {versionLines[0]}", "Info");
                    }

                    /*Check for installed version*/
                    _installedVersion = await InstallationService.GetInstalledVersionAsync(_connectionString);
                    if (_installedVersion != null)
                    {
                        LogMessage($"Installed version: {_installedVersion}", "Info");

                        /*Check for applicable upgrades*/
                        if (_monitorRootDirectory != null)
                        {
                            var upgrades = InstallationService.GetApplicableUpgrades(
                                _monitorRootDirectory,
                                _installedVersion,
                                AppAssemblyVersion);
                            if (upgrades.Count > 0)
                            {
                                LogMessage($"Found {upgrades.Count} upgrade(s) to apply", "Warning");
                                foreach (var upgrade in upgrades)
                                {
                                    LogMessage($"  - {upgrade.FolderName}", "Info");
                                }
                            }
                        }
                    }
                    else
                    {
                        LogMessage("No existing installation detected (clean install)", "Info");
                    }

                    InstallButton.IsEnabled = _sqlFiles != null && _sqlFiles.Count > 0;

                    /*Show confirmation MessageBox*/
                    string installedVersionText = _installedVersion != null
                        ? $"Installed: {_installedVersion}"
                        : "No existing installation";
                    MessageBox.Show(this,
                        $"Connection successful!\n\n" +
                        $"Server: {_serverInfo.ServerName}\n" +
                        $"Edition: {_serverInfo.SqlServerEdition}\n" +
                        $"Version: {(versionLines.Length > 0 ? versionLines[0] : _serverInfo.SqlServerVersion)}\n\n" +
                        $"{installedVersionText}",
                        "Connection Test",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                }
                else
                {
                    LogMessage($"Connection failed: {_serverInfo.ErrorMessage}", "Error");
                    InstallButton.IsEnabled = false;
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile("TestConnection_Click", ex);
                LogMessage($"Error: {ex.Message}", "Error");
                MessageBox.Show(this, $"Connection test error:\n\n{ex.Message}\n\nSee log file for details:\n{Logger.LogFilePath}",
                    "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                TestConnectionButton.IsEnabled = true;
                TestConnectionButton.Content = "Test Connection";
            }
        }

        /// <summary>
        /// Install button click
        /// </summary>
        private async void Install_Click(object sender, RoutedEventArgs e)
        {
            if (_connectionString == null || _sqlFiles == null || _sqlDirectory == null)
            {
                MessageBox.Show(this, "Please test the connection first.", "Validation Error",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            /*Confirm clean install*/
            if (CleanInstallCheckBox.IsChecked == true)
            {
                var result = MessageBox.Show(this,
                    "WARNING: Clean install will DROP the existing PerformanceMonitor database and ALL collected data.\n\n" +
                    "This action CANNOT be undone!\n\n" +
                    "Are you sure you want to continue?",
                    "Confirm Clean Install",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Warning,
                    MessageBoxResult.No);

                if (result != MessageBoxResult.Yes)
                {
                    return;
                }
            }

            /*Dispose previous token source if it exists*/
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = _cancellationTokenSource.Token;

            SetUIState(installing: true);
            ClearLog();

            /*Capture log messages for the report*/
            var logMessages = new List<(string Message, string Status)>();

            LogMessage($"Performance Monitor Installer v{AppVersion}", "Info");
            logMessages.Add(($"Performance Monitor Installer v{AppVersion}", "Info"));
            LogMessage($"Copyright (c) {DateTime.Now.Year} Darling Data, LLC", "Info");
            logMessages.Add(($"Copyright (c) {DateTime.Now.Year} Darling Data, LLC", "Info"));
            LogMessage("", "Info");

            var progress = new Progress<InstallationProgress>(p =>
            {
                ReportProgress(p);
                logMessages.Add((p.Message, p.Status));
            });

            try
            {
                /*
                Execute upgrades if applicable (only when not doing clean install)
                */
                bool isCleanInstall = CleanInstallCheckBox.IsChecked == true;
                if (!isCleanInstall && _installedVersion != null && _monitorRootDirectory != null)
                {
                    var (upgradeSuccess, upgradeFailure, upgradeCount) = await InstallationService.ExecuteAllUpgradesAsync(
                        _monitorRootDirectory,
                        _connectionString,
                        _installedVersion,
                        AppAssemblyVersion,
                        progress,
                        cancellationToken);

                    if (upgradeCount > 0)
                    {
                        LogMessage("", "Info");
                        LogMessage($"Upgrades completed: {upgradeSuccess} scripts succeeded, {upgradeFailure} failed",
                            upgradeFailure == 0 ? "Success" : "Warning");
                        LogMessage("", "Info");
                    }
                }

                /*
                Execute installation
                Community dependencies install automatically before validation (98_validate)
                */
                bool resetSchedule = ResetScheduleCheckBox.IsChecked == true;
                bool preserveJobs = PreserveJobsCheckBox.IsChecked == true;
                _installationResult = await InstallationService.ExecuteInstallationAsync(
                    _connectionString,
                    _sqlFiles,
                    isCleanInstall,
                    resetSchedule,
                    preserveJobs,
                    progress,
                    preValidationAction: async () =>
                    {
                        await _installationService.InstallDependenciesAsync(
                            _connectionString,
                            progress,
                            cancellationToken);
                    },
                    cancellationToken);

                /*
                Run validation if requested
                */
                if (_installationResult.Success && ValidationCheckBox.IsChecked == true)
                {
                    LogMessage("Running validation (this may take a moment)...", "Info");
                    var (succeeded, failed) = await InstallationService.RunValidationAsync(
                        _connectionString,
                        progress,
                        cancellationToken);
                }

                /*
                Generate summary report
                */
                if (_serverInfo != null)
                {
                    /*Copy captured log messages to the result for the report*/
                    foreach (var (message, status) in logMessages)
                    {
                        _installationResult.LogMessages.Add((message, status));
                    }

                    _installationResult.ReportPath = InstallationService.GenerateSummaryReport(
                        _serverInfo.ServerName,
                        _serverInfo.SqlServerVersion,
                        _serverInfo.SqlServerEdition,
                        AppVersion,
                        _installationResult);

                    LogMessage("", "Info");
                    LogMessage($"Installation report saved to: {_installationResult.ReportPath}", "Info");
                }

                /*
                Final summary
                */
                LogMessage("", "Info");
                LogMessage("================================================================================", "Info");
                LogMessage("Installation Summary", "Info");
                LogMessage("================================================================================", "Info");

                if (_installationResult.Success)
                {
                    LogMessage("Installation completed successfully!", "Success");
                    LogMessage("", "Info");
                    LogMessage("NEXT STEPS:", "Info");
                    LogMessage("1. Ensure SQL Server Agent service is running", "Info");
                    LogMessage("2. Verify: SELECT * FROM PerformanceMonitor.report.collection_health;", "Info");
                    LogMessage("3. Monitor job history in SQL Server Agent", "Info");

                    TroubleshootButton.IsEnabled = true;
                    ViewReportButton.IsEnabled = _installationResult?.ReportPath != null;
                }
                else
                {
                    LogMessage($"Installation completed with {_installationResult.FilesFailed} error(s).", "Warning");
                    LogMessage("Review errors above for details.", "Warning");

                    ViewReportButton.IsEnabled = _installationResult?.ReportPath != null;
                }

                ProgressBar.Value = 100;
                ProgressText.Text = "100%";
            }
            catch (OperationCanceledException)
            {
                LogMessage("", "Info");
                LogMessage("Installation cancelled by user.", "Warning");
            }
            catch (Exception ex)
            {
                LogMessage("", "Info");
                LogMessage($"Installation failed: {ex.Message}", "Error");
            }
            finally
            {
                SetUIState(installing: false);
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }

            /*
            Show completion dialog after re-enabling UI so the
            dialog cannot block SetUIState(false) in the finally block.
            */
            if (_installationResult is not null)
            {
                if (_installationResult.Success)
                {
                    MessageBox.Show(this,
                        $"Installation completed successfully!\n\n" +
                        $"{_installationResult.FilesSucceeded} script(s) executed without errors.",
                        "Installation Complete",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                }
                else
                {
                    MessageBox.Show(this,
                        $"Installation completed with errors.\n\n" +
                        $"Succeeded: {_installationResult.FilesSucceeded}\n" +
                        $"Failed: {_installationResult.FilesFailed}\n\n" +
                        $"Review the log for details.",
                        "Installation Complete",
                        MessageBoxButton.OK,
                        MessageBoxImage.Warning);
                }
            }
        }

        /// <summary>
        /// Troubleshoot button click - runs 99_troubleshooting.sql
        /// </summary>
        private async void Troubleshoot_Click(object sender, RoutedEventArgs e)
        {
            if (_connectionString == null || _sqlDirectory == null)
            {
                return;
            }

            /*Dispose previous token source if it exists*/
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = _cancellationTokenSource.Token;

            SetUIState(installing: true);

            LogMessage("", "Info");
            LogMessage("================================================================================", "Info");
            LogMessage("Running Troubleshooting Script", "Info");
            LogMessage("================================================================================", "Info");

            var progress = new Progress<InstallationProgress>(ReportProgress);

            try
            {
                bool success = await InstallationService.RunTroubleshootingAsync(
                    _connectionString,
                    _sqlDirectory,
                    progress,
                    cancellationToken);

                if (success)
                {
                    LogMessage("Troubleshooting script completed successfully!", "Success");
                }
            }
            catch (OperationCanceledException)
            {
                LogMessage("Troubleshooting cancelled by user.", "Warning");
            }
            catch (Exception ex)
            {
                LogMessage($"Troubleshooting failed: {ex.Message}", "Error");
            }
            finally
            {
                SetUIState(installing: false);
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }
        }

        /// <summary>
        /// View report button click
        /// </summary>
        private void ViewReport_Click(object sender, RoutedEventArgs e)
        {
            if (_installationResult?.ReportPath == null)
            {
                return;
            }

            try
            {
                using var _ = Process.Start(new ProcessStartInfo
                {
                    FileName = _installationResult.ReportPath,
                    UseShellExecute = true
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, $"Could not open report: {ex.Message}", "Error",
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// Close button click
        /// </summary>
        private void Close_Click(object sender, RoutedEventArgs e)
        {
            _cancellationTokenSource?.Cancel();
            Close();
        }

        /// <summary>
        /// Clean up resources when window closes
        /// </summary>
        protected override void OnClosed(EventArgs e)
        {
            _cancellationTokenSource?.Dispose();
            _installationService?.Dispose();
            base.OnClosed(e);
        }

        /// <summary>
        /// Report progress from installation service
        /// </summary>
        private void ReportProgress(InstallationProgress progress)
        {
            LogMessage(progress.Message, progress.Status);

            if (progress.ProgressPercent.HasValue)
            {
                ProgressBar.Value = progress.ProgressPercent.Value;
                ProgressText.Text = $"{progress.ProgressPercent.Value}%";
            }
        }

        /// <summary>
        /// Log a message to the log text box
        /// </summary>
        private void LogMessage(string message, string status)
        {
            Brush brush = status switch
            {
                "Success" => SuccessBrush,
                "Error" => ErrorBrush,
                "Warning" => WarningBrush,
                _ => Brushes.White
            };

            string prefix = status switch
            {
                "Success" => "[OK] ",
                "Error" => "[ERROR] ",
                "Warning" => "[WARN] ",
                _ => ""
            };

            /*Create a new paragraph for each message*/
            var paragraph = new Paragraph(new Run(prefix + message))
            {
                Margin = new Thickness(0),
                Foreground = brush
            };

            LogTextBox.Document.Blocks.Add(paragraph);

            /*Auto-scroll to bottom*/
            LogTextBox.ScrollToEnd();
        }

        /// <summary>
        /// Clear the log text box
        /// </summary>
        private void ClearLog()
        {
            LogTextBox.Document.Blocks.Clear();
            var paragraph = new Paragraph();
            LogTextBox.Document.Blocks.Add(paragraph);
        }

        /// <summary>
        /// Set UI state for installing/not installing
        /// </summary>
        private void SetUIState(bool installing)
        {
            ServerTextBox.IsEnabled = !installing;
            WindowsAuthRadio.IsEnabled = !installing;
            SqlAuthRadio.IsEnabled = !installing;
            UsernameTextBox.IsEnabled = !installing && SqlAuthRadio.IsChecked == true;
            PasswordBox.IsEnabled = !installing && SqlAuthRadio.IsChecked == true;
            TestConnectionButton.IsEnabled = !installing;
            CleanInstallCheckBox.IsEnabled = !installing;
            ValidationCheckBox.IsEnabled = !installing;

            InstallButton.IsEnabled = !installing;
            InstallButton.Content = installing ? "Installing..." : "Install";

            TroubleshootButton.IsEnabled = !installing && _installationResult?.Success == true;
            ViewReportButton.IsEnabled = !installing && _installationResult?.ReportPath != null;

            if (!installing)
            {
                ProgressBar.Value = 0;
                ProgressText.Text = "0%";
            }
        }
    }
}
