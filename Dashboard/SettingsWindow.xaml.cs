/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class SettingsWindow : Window
    {
        private readonly IUserPreferencesService _preferencesService;
        private bool _isLoading = true;

        public SettingsWindow(IUserPreferencesService preferencesService)
        {
            InitializeComponent();

            _preferencesService = preferencesService;
            LoadSettings();
            _isLoading = false;
        }

        private void LoadSettings()
        {
            var prefs = _preferencesService.GetPreferences();

            // NOC refresh interval
            foreach (ComboBoxItem item in NocRefreshIntervalComboBox.Items)
            {
                if (item.Tag != null && int.Parse(item.Tag.ToString()!, CultureInfo.InvariantCulture) == prefs.NocRefreshIntervalSeconds)
                {
                    NocRefreshIntervalComboBox.SelectedItem = item;
                    break;
                }
            }

            // Default to 30 seconds if no match
            if (NocRefreshIntervalComboBox.SelectedItem == null)
            {
                NocRefreshIntervalComboBox.SelectedIndex = 1; // 30 seconds
            }

            // Auto-refresh settings
            AutoRefreshCheckBox.IsChecked = prefs.AutoRefreshEnabled;
            RefreshIntervalComboBox.IsEnabled = prefs.AutoRefreshEnabled;

            // Select the matching interval
            foreach (ComboBoxItem item in RefreshIntervalComboBox.Items)
            {
                if (item.Tag != null && int.Parse(item.Tag.ToString()!, CultureInfo.InvariantCulture) == prefs.AutoRefreshIntervalSeconds)
                {
                    RefreshIntervalComboBox.SelectedItem = item;
                    break;
                }
            }

            // Default to 1 minute if no match
            if (RefreshIntervalComboBox.SelectedItem == null)
            {
                RefreshIntervalComboBox.SelectedIndex = 2; // 1 minute
            }

            // Default time range
            foreach (ComboBoxItem item in DefaultTimeRangeComboBox.Items)
            {
                if (item.Tag != null && int.Parse(item.Tag.ToString()!, CultureInfo.InvariantCulture) == prefs.DefaultHoursBack)
                {
                    DefaultTimeRangeComboBox.SelectedItem = item;
                    break;
                }
            }

            // Default to 24 hours if no match
            if (DefaultTimeRangeComboBox.SelectedItem == null)
            {
                DefaultTimeRangeComboBox.SelectedIndex = 4; // 24 hours
            }

            // Navigation settings
            FocusServerTabCheckBox.IsChecked = prefs.FocusServerTabOnClick;

            // Query logging settings
            LogSlowQueriesCheckBox.IsChecked = prefs.LogSlowQueries;
            QueryLogger.SetEnabled(prefs.LogSlowQueries);
            QueryLogger.SetThreshold(prefs.SlowQueryThresholdSeconds);

            // Method profiler settings
            LogSlowMethodsCheckBox.IsChecked = prefs.LogSlowMethods;
            MethodProfiler.SetEnabled(prefs.LogSlowMethods);

            // MCP server settings
            McpEnabledCheckBox.IsChecked = prefs.McpEnabled;
            McpPortTextBox.Text = prefs.McpPort.ToString(CultureInfo.InvariantCulture);
            McpPortTextBox.IsEnabled = prefs.McpEnabled;
            UpdateMcpStatus(prefs);

            // System tray settings
            MinimizeToTrayCheckBox.IsChecked = prefs.MinimizeToTray;
            NotificationsEnabledCheckBox.IsChecked = prefs.NotificationsEnabled;
            NotifyConnectionLostCheckBox.IsChecked = prefs.NotifyOnConnectionLost;
            NotifyConnectionRestoredCheckBox.IsChecked = prefs.NotifyOnConnectionRestored;

            // Alert notification settings
            NotifyOnBlockingCheckBox.IsChecked = prefs.NotifyOnBlocking;
            BlockingThresholdTextBox.Text = prefs.BlockingThresholdSeconds.ToString(CultureInfo.InvariantCulture);
            NotifyOnDeadlockCheckBox.IsChecked = prefs.NotifyOnDeadlock;
            DeadlockThresholdTextBox.Text = prefs.DeadlockThreshold.ToString(CultureInfo.InvariantCulture);
            NotifyOnHighCpuCheckBox.IsChecked = prefs.NotifyOnHighCpu;
            CpuThresholdTextBox.Text = prefs.CpuThresholdPercent.ToString(CultureInfo.InvariantCulture);
            NotifyOnPoisonWaitsCheckBox.IsChecked = prefs.NotifyOnPoisonWaits;
            PoisonWaitThresholdTextBox.Text = prefs.PoisonWaitThresholdMs.ToString(CultureInfo.InvariantCulture);
            NotifyOnLongRunningQueriesCheckBox.IsChecked = prefs.NotifyOnLongRunningQueries;
            LongRunningQueryThresholdTextBox.Text = prefs.LongRunningQueryThresholdMinutes.ToString(CultureInfo.InvariantCulture);
            NotifyOnTempDbSpaceCheckBox.IsChecked = prefs.NotifyOnTempDbSpace;
            TempDbSpaceThresholdTextBox.Text = prefs.TempDbSpaceThresholdPercent.ToString(CultureInfo.InvariantCulture);
            NotifyOnLongRunningJobsCheckBox.IsChecked = prefs.NotifyOnLongRunningJobs;
            LongRunningJobMultiplierTextBox.Text = prefs.LongRunningJobMultiplier.ToString(CultureInfo.InvariantCulture);

            UpdateNotificationCheckboxStates();

            // SMTP email settings
            SmtpEnabledCheckBox.IsChecked = prefs.SmtpEnabled;
            SmtpServerTextBox.Text = prefs.SmtpServer;
            SmtpPortTextBox.Text = prefs.SmtpPort.ToString(CultureInfo.InvariantCulture);
            SmtpSslCheckBox.IsChecked = prefs.SmtpUseSsl;
            SmtpUsernameTextBox.Text = prefs.SmtpUsername;
            SmtpFromTextBox.Text = prefs.SmtpFromAddress;
            SmtpRecipientsTextBox.Text = prefs.SmtpRecipients;

            var password = EmailAlertService.GetSmtpPassword();
            if (!string.IsNullOrEmpty(password))
            {
                SmtpPasswordBox.Password = password;
            }

            UpdateSmtpControlStates();
        }

        private void NocRefreshIntervalComboBox_Changed(object sender, SelectionChangedEventArgs e)
        {
            // Just UI update, actual save happens on OK
        }

        private void AutoRefreshCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;

            RefreshIntervalComboBox.IsEnabled = AutoRefreshCheckBox.IsChecked == true;
        }

        private void RefreshIntervalComboBox_Changed(object sender, SelectionChangedEventArgs e)
        {
            // Just UI update, actual save happens on OK
        }

        private void DefaultTimeRangeComboBox_Changed(object sender, SelectionChangedEventArgs e)
        {
            // Just UI update, actual save happens on OK
        }

        private void LogSlowQueriesCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;

            // Update the QueryLogger immediately for this session
            QueryLogger.SetEnabled(LogSlowQueriesCheckBox.IsChecked == true);
        }

        private void LogSlowMethodsCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;

            // Update the MethodProfiler immediately for this session
            MethodProfiler.SetEnabled(LogSlowMethodsCheckBox.IsChecked == true);
        }

        private void MinimizeToTrayCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            // Just UI update, actual save happens on OK
        }

        private void NotificationsEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateNotificationCheckboxStates();
        }

        private void NotifyConnectionLostCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            // Just UI update, actual save happens on OK
        }

        private void NotifyConnectionRestoredCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            // Just UI update, actual save happens on OK
        }

        private void NotifyOnBlockingCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void NotifyOnDeadlockCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void NotifyOnHighCpuCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void NotifyOnPoisonWaitsCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void NotifyOnLongRunningQueriesCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void NotifyOnTempDbSpaceCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void NotifyOnLongRunningJobsCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateAlertNotificationStates();
        }

        private void RestoreAlertDefaultsButton_Click(object sender, RoutedEventArgs e)
        {
            BlockingThresholdTextBox.Text = "30";
            DeadlockThresholdTextBox.Text = "1";
            CpuThresholdTextBox.Text = "90";
            PoisonWaitThresholdTextBox.Text = "500";
            LongRunningQueryThresholdTextBox.Text = "30";
            TempDbSpaceThresholdTextBox.Text = "80";
            LongRunningJobMultiplierTextBox.Text = "3";
            UpdateAlertPreviewText();
        }

        private void UpdateAlertPreviewText()
        {
            var parts = new System.Collections.Generic.List<string>();

            if (NotifyOnBlockingCheckBox.IsChecked == true)
                parts.Add($"blocking > {BlockingThresholdTextBox.Text}s");
            if (NotifyOnDeadlockCheckBox.IsChecked == true)
                parts.Add($"deadlocks >= {DeadlockThresholdTextBox.Text}");
            if (NotifyOnHighCpuCheckBox.IsChecked == true)
                parts.Add($"CPU > {CpuThresholdTextBox.Text}%");
            if (NotifyOnPoisonWaitsCheckBox.IsChecked == true)
                parts.Add($"poison waits >= {PoisonWaitThresholdTextBox.Text}ms avg");
            if (NotifyOnLongRunningQueriesCheckBox.IsChecked == true)
                parts.Add($"queries > {LongRunningQueryThresholdTextBox.Text}min");
            if (NotifyOnTempDbSpaceCheckBox.IsChecked == true)
                parts.Add($"TempDB > {TempDbSpaceThresholdTextBox.Text}%");
            if (NotifyOnLongRunningJobsCheckBox.IsChecked == true)
                parts.Add($"jobs > {LongRunningJobMultiplierTextBox.Text}x avg");

            AlertPreviewText.Text = parts.Count > 0
                ? $"Will alert when: {string.Join(", ", parts)}"
                : "No alerts enabled";
        }

        private void UpdateAlertNotificationStates()
        {
            bool notificationsEnabled = NotificationsEnabledCheckBox.IsChecked == true;
            NotifyOnBlockingCheckBox.IsEnabled = notificationsEnabled;
            BlockingThresholdTextBox.IsEnabled = notificationsEnabled && NotifyOnBlockingCheckBox.IsChecked == true;
            NotifyOnDeadlockCheckBox.IsEnabled = notificationsEnabled;
            DeadlockThresholdTextBox.IsEnabled = notificationsEnabled && NotifyOnDeadlockCheckBox.IsChecked == true;
            NotifyOnHighCpuCheckBox.IsEnabled = notificationsEnabled;
            CpuThresholdTextBox.IsEnabled = notificationsEnabled && NotifyOnHighCpuCheckBox.IsChecked == true;
            NotifyOnPoisonWaitsCheckBox.IsEnabled = notificationsEnabled;
            PoisonWaitThresholdTextBox.IsEnabled = notificationsEnabled && NotifyOnPoisonWaitsCheckBox.IsChecked == true;
            NotifyOnLongRunningQueriesCheckBox.IsEnabled = notificationsEnabled;
            LongRunningQueryThresholdTextBox.IsEnabled = notificationsEnabled && NotifyOnLongRunningQueriesCheckBox.IsChecked == true;
            NotifyOnTempDbSpaceCheckBox.IsEnabled = notificationsEnabled;
            TempDbSpaceThresholdTextBox.IsEnabled = notificationsEnabled && NotifyOnTempDbSpaceCheckBox.IsChecked == true;
            NotifyOnLongRunningJobsCheckBox.IsEnabled = notificationsEnabled;
            LongRunningJobMultiplierTextBox.IsEnabled = notificationsEnabled && NotifyOnLongRunningJobsCheckBox.IsChecked == true;
            UpdateAlertPreviewText();
        }

        private void UpdateNotificationCheckboxStates()
        {
            bool notificationsEnabled = NotificationsEnabledCheckBox.IsChecked == true;
            NotifyConnectionLostCheckBox.IsEnabled = notificationsEnabled;
            NotifyConnectionRestoredCheckBox.IsEnabled = notificationsEnabled;
            UpdateAlertNotificationStates();
        }

        private void OpenQueryLogButton_Click(object sender, RoutedEventArgs e)
        {
            var logFile = QueryLogger.GetCurrentLogFile();

            if (File.Exists(logFile))
            {
                // Open the log file in the default text editor
                try
                {
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = logFile,
                        UseShellExecute = true
                    });
                }
                catch
                {
                    MessageBox.Show($"Could not open log file:\n{logFile}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            else
            {
                // No log file yet - offer to open the log directory
                var logDir = QueryLogger.GetLogDirectory();
                var result = MessageBox.Show(
                    $"No slow query log file exists yet for today.\n\nLog files are created when queries exceed the threshold.\n\nWould you like to open the log directory?\n\n{logDir}",
                    "No Log File",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Information);

                if (result == MessageBoxResult.Yes && Directory.Exists(logDir))
                {
                    try
                    {
                        Process.Start(new ProcessStartInfo
                        {
                            FileName = logDir,
                            UseShellExecute = true
                        });
                    }
                    catch
                    {
                        MessageBox.Show($"Could not open log directory:\n{logDir}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            }
        }

        private void OpenMethodProfileLogButton_Click(object sender, RoutedEventArgs e)
        {
            var logFile = MethodProfiler.GetCurrentLogFile();

            if (File.Exists(logFile))
            {
                // Open the log file in the default text editor
                try
                {
                    Process.Start(new ProcessStartInfo
                    {
                        FileName = logFile,
                        UseShellExecute = true
                    });
                }
                catch
                {
                    MessageBox.Show($"Could not open log file:\n{logFile}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            else
            {
                // No log file yet - offer to open the log directory
                var logDir = MethodProfiler.GetLogDirectory();
                var result = MessageBox.Show(
                    $"No method profile log file exists yet for today.\n\nLog files are created when methods exceed the threshold (500ms).\n\nWould you like to open the log directory?\n\n{logDir}",
                    "No Log File",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Information);

                if (result == MessageBoxResult.Yes && Directory.Exists(logDir))
                {
                    try
                    {
                        Process.Start(new ProcessStartInfo
                        {
                            FileName = logDir,
                            UseShellExecute = true
                        });
                    }
                    catch
                    {
                        MessageBox.Show($"Could not open log directory:\n{logDir}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            }
        }

        private void McpEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            McpPortTextBox.IsEnabled = McpEnabledCheckBox.IsChecked == true;
        }

        private void UpdateMcpStatus(Models.UserPreferences prefs)
        {
            if (prefs.McpEnabled)
            {
                McpStatusText.Text = $"Status: Running on http://localhost:{prefs.McpPort}";
                CopyMcpCommandButton.IsEnabled = true;
            }
            else
            {
                McpStatusText.Text = "Status: Disabled";
                CopyMcpCommandButton.IsEnabled = false;
            }
        }

        private void CopyMcpCommandButton_Click(object sender, RoutedEventArgs e)
        {
            var port = McpPortTextBox.Text;
            var command = $"claude mcp add --transport http --scope user sql-monitor http://localhost:{port}/";
            /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
            Clipboard.SetDataObject(command, false);
            McpStatusText.Text = "Copied to clipboard!";
        }

        private void OkButton_Click(object sender, RoutedEventArgs e)
        {
            var prefs = _preferencesService.GetPreferences();

            // Save NOC refresh interval
            if (NocRefreshIntervalComboBox.SelectedItem is ComboBoxItem nocIntervalItem && nocIntervalItem.Tag != null)
            {
                prefs.NocRefreshIntervalSeconds = int.Parse(nocIntervalItem.Tag.ToString()!, CultureInfo.InvariantCulture);
            }

            // Save auto-refresh settings
            prefs.AutoRefreshEnabled = AutoRefreshCheckBox.IsChecked == true;

            if (RefreshIntervalComboBox.SelectedItem is ComboBoxItem intervalItem && intervalItem.Tag != null)
            {
                prefs.AutoRefreshIntervalSeconds = int.Parse(intervalItem.Tag.ToString()!, CultureInfo.InvariantCulture);
            }

            // Save default time range
            if (DefaultTimeRangeComboBox.SelectedItem is ComboBoxItem rangeItem && rangeItem.Tag != null)
            {
                prefs.DefaultHoursBack = int.Parse(rangeItem.Tag.ToString()!, CultureInfo.InvariantCulture);
            }

            // Save navigation settings
            prefs.FocusServerTabOnClick = FocusServerTabCheckBox.IsChecked == true;

            // Save query logging settings
            prefs.LogSlowQueries = LogSlowQueriesCheckBox.IsChecked == true;
            QueryLogger.SetEnabled(prefs.LogSlowQueries);

            // Save method profiler settings
            prefs.LogSlowMethods = LogSlowMethodsCheckBox.IsChecked == true;
            MethodProfiler.SetEnabled(prefs.LogSlowMethods);

            // Save system tray settings
            prefs.MinimizeToTray = MinimizeToTrayCheckBox.IsChecked == true;
            prefs.NotificationsEnabled = NotificationsEnabledCheckBox.IsChecked == true;
            prefs.NotifyOnConnectionLost = NotifyConnectionLostCheckBox.IsChecked == true;
            prefs.NotifyOnConnectionRestored = NotifyConnectionRestoredCheckBox.IsChecked == true;

            // Save alert notification settings with validation
            var validationErrors = new System.Collections.Generic.List<string>();

            prefs.NotifyOnBlocking = NotifyOnBlockingCheckBox.IsChecked == true;
            if (int.TryParse(BlockingThresholdTextBox.Text, out int blockingThreshold) && blockingThreshold > 0)
                prefs.BlockingThresholdSeconds = blockingThreshold;
            else if (prefs.NotifyOnBlocking)
                validationErrors.Add("Blocking threshold must be a positive number");

            prefs.NotifyOnDeadlock = NotifyOnDeadlockCheckBox.IsChecked == true;
            if (int.TryParse(DeadlockThresholdTextBox.Text, out int deadlockThreshold) && deadlockThreshold > 0)
                prefs.DeadlockThreshold = deadlockThreshold;
            else if (prefs.NotifyOnDeadlock)
                validationErrors.Add("Deadlock threshold must be a positive number");

            prefs.NotifyOnHighCpu = NotifyOnHighCpuCheckBox.IsChecked == true;
            if (int.TryParse(CpuThresholdTextBox.Text, out int cpuThreshold) && cpuThreshold > 0 && cpuThreshold <= 100)
                prefs.CpuThresholdPercent = cpuThreshold;
            else if (prefs.NotifyOnHighCpu)
                validationErrors.Add("CPU threshold must be between 1 and 100");

            prefs.NotifyOnPoisonWaits = NotifyOnPoisonWaitsCheckBox.IsChecked == true;
            if (int.TryParse(PoisonWaitThresholdTextBox.Text, out int poisonWaitThreshold) && poisonWaitThreshold > 0)
                prefs.PoisonWaitThresholdMs = poisonWaitThreshold;
            else if (prefs.NotifyOnPoisonWaits)
                validationErrors.Add("Poison wait threshold must be a positive number");

            prefs.NotifyOnLongRunningQueries = NotifyOnLongRunningQueriesCheckBox.IsChecked == true;
            if (int.TryParse(LongRunningQueryThresholdTextBox.Text, out int lrqThreshold) && lrqThreshold > 0)
                prefs.LongRunningQueryThresholdMinutes = lrqThreshold;
            else if (prefs.NotifyOnLongRunningQueries)
                validationErrors.Add("Long-running query threshold must be a positive number");

            prefs.NotifyOnTempDbSpace = NotifyOnTempDbSpaceCheckBox.IsChecked == true;
            if (int.TryParse(TempDbSpaceThresholdTextBox.Text, out int tempDbThreshold) && tempDbThreshold > 0 && tempDbThreshold <= 100)
                prefs.TempDbSpaceThresholdPercent = tempDbThreshold;
            else if (prefs.NotifyOnTempDbSpace)
                validationErrors.Add("TempDB space threshold must be between 1 and 100");

            prefs.NotifyOnLongRunningJobs = NotifyOnLongRunningJobsCheckBox.IsChecked == true;
            if (int.TryParse(LongRunningJobMultiplierTextBox.Text, out int jobMultiplier) && jobMultiplier > 0)
                prefs.LongRunningJobMultiplier = jobMultiplier;
            else if (prefs.NotifyOnLongRunningJobs)
                validationErrors.Add("Job multiplier must be a positive number");

            if (validationErrors.Count > 0)
            {
                MessageBox.Show(
                    "Some alert thresholds have invalid values and were not changed:\n\n" +
                    string.Join("\n", validationErrors),
                    "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            }

            // Save SMTP email settings
            prefs.SmtpEnabled = SmtpEnabledCheckBox.IsChecked == true;
            prefs.SmtpServer = SmtpServerTextBox.Text?.Trim() ?? "";
            if (int.TryParse(SmtpPortTextBox.Text, out int smtpPort) && smtpPort > 0 && smtpPort <= 65535)
            {
                prefs.SmtpPort = smtpPort;
            }
            prefs.SmtpUseSsl = SmtpSslCheckBox.IsChecked == true;
            prefs.SmtpUsername = SmtpUsernameTextBox.Text?.Trim() ?? "";
            prefs.SmtpFromAddress = SmtpFromTextBox.Text?.Trim() ?? "";
            prefs.SmtpRecipients = SmtpRecipientsTextBox.Text?.Trim() ?? "";

            if (!string.IsNullOrEmpty(SmtpPasswordBox.Password))
            {
                EmailAlertService.SaveSmtpPassword(SmtpPasswordBox.Password, prefs.SmtpUsername);
            }

            // Save MCP server settings
            prefs.McpEnabled = McpEnabledCheckBox.IsChecked == true;
            if (int.TryParse(McpPortTextBox.Text, out int mcpPort) && mcpPort > 0 && mcpPort <= 65535)
            {
                prefs.McpPort = mcpPort;
            }

            _preferencesService.SavePreferences(prefs);

            DialogResult = true;
            Close();
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
            Close();
        }

        // ============================================
        // Email Alerts Tab
        // ============================================

        private void SmtpEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
        {
            if (_isLoading) return;
            UpdateSmtpControlStates();
        }

        private void UpdateSmtpControlStates()
        {
            bool enabled = SmtpEnabledCheckBox.IsChecked == true;
            SmtpServerTextBox.IsEnabled = enabled;
            SmtpPortTextBox.IsEnabled = enabled;
            SmtpSslCheckBox.IsEnabled = enabled;
            SmtpUsernameTextBox.IsEnabled = enabled;
            SmtpPasswordBox.IsEnabled = enabled;
            SmtpFromTextBox.IsEnabled = enabled;
            SmtpRecipientsTextBox.IsEnabled = enabled;
            TestEmailButton.IsEnabled = enabled;
            ValidateSmtpButton.IsEnabled = enabled;
        }

        private void ValidateSmtpButton_Click(object sender, RoutedEventArgs e)
        {
            var errors = new System.Collections.Generic.List<string>();

            if (string.IsNullOrWhiteSpace(SmtpServerTextBox.Text))
                errors.Add("SMTP server is required");
            if (!int.TryParse(SmtpPortTextBox.Text, out var port) || port < 1 || port > 65535)
                errors.Add("Port must be between 1 and 65535");
            if (string.IsNullOrWhiteSpace(SmtpFromTextBox.Text))
                errors.Add("From address is required");
            else if (!SmtpFromTextBox.Text.Trim().Contains('@'))
                errors.Add("From address must be a valid email");
            if (string.IsNullOrWhiteSpace(SmtpRecipientsTextBox.Text))
                errors.Add("At least one recipient is required");

            if (errors.Count == 0)
            {
                TestEmailStatusText.Text = "Settings look good. Use 'Send Test Email' to verify delivery.";
            }
            else
            {
                TestEmailStatusText.Text = "";
                MessageBox.Show(
                    "SMTP configuration has issues:\n\n" + string.Join("\n", errors),
                    "SMTP Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
        }

        private async void TestEmailButton_Click(object sender, RoutedEventArgs e)
        {
            // Build a temporary prefs object with current UI values for the test
            var testPrefs = new Models.UserPreferences
            {
                SmtpEnabled = true,
                SmtpServer = SmtpServerTextBox.Text?.Trim() ?? "",
                SmtpPort = int.TryParse(SmtpPortTextBox.Text, out var port) ? port : 587,
                SmtpUseSsl = SmtpSslCheckBox.IsChecked == true,
                SmtpUsername = SmtpUsernameTextBox.Text?.Trim() ?? "",
                SmtpFromAddress = SmtpFromTextBox.Text?.Trim() ?? "",
                SmtpRecipients = SmtpRecipientsTextBox.Text?.Trim() ?? ""
            };

            // Save password to credential store so SendEmailAsync can read it
            if (!string.IsNullOrEmpty(SmtpPasswordBox.Password))
            {
                EmailAlertService.SaveSmtpPassword(SmtpPasswordBox.Password, testPrefs.SmtpUsername);
            }

            TestEmailButton.IsEnabled = false;
            TestEmailButton.Content = "Sending...";
            TestEmailStatusText.Text = "";

            try
            {
                var emailService = EmailAlertService.Current ?? new EmailAlertService((UserPreferencesService)_preferencesService);
                var error = await emailService.SendTestEmailAsync(testPrefs);

                if (error == null)
                {
                    TestEmailStatusText.Text = "Test email sent successfully!";
                    MessageBox.Show("Test email sent successfully!", "Test Email", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                else
                {
                    TestEmailStatusText.Text = $"Failed: {error}";
                    MessageBox.Show($"Failed to send test email:\n\n{error}", "Test Email Failed", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
            catch (Exception ex)
            {
                TestEmailStatusText.Text = $"Error: {ex.Message}";
                MessageBox.Show($"Failed to send test email:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
            finally
            {
                TestEmailButton.Content = "Send Test Email";
                TestEmailButton.IsEnabled = true;
            }
        }
    }
}
