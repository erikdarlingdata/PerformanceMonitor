/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Windows;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class SettingsWindow : Window
{
    private readonly ScheduleManager _scheduleManager;
    private readonly CollectionBackgroundService? _backgroundService;
    private readonly McpHostService? _mcpService;

    public SettingsWindow(
        ScheduleManager scheduleManager,
        CollectionBackgroundService? backgroundService = null,
        McpHostService? mcpService = null)
    {
        InitializeComponent();
        _scheduleManager = scheduleManager;
        _backgroundService = backgroundService;
        _mcpService = mcpService;

        LoadSchedules();
        UpdateCollectionStatus();
        LoadMcpSettings();
        UpdateMcpStatus();
        LoadDefaultTimeRange();
        LoadAlertSettings();
        LoadSmtpSettings();
    }

    private void LoadSchedules()
    {
        ScheduleGrid.ItemsSource = _scheduleManager.GetAllSchedules();
    }

    private void UpdateCollectionStatus()
    {
        if (_backgroundService == null)
        {
            CollectionStatusText.Text = "Status: Not running";
            PauseResumeButton.IsEnabled = false;
            return;
        }

        if (_backgroundService.IsPaused)
        {
            CollectionStatusText.Text = "Status: Paused";
            PauseResumeButton.Content = "Resume Collection";
        }
        else
        {
            CollectionStatusText.Text = _backgroundService.IsCollecting
                ? "Status: Collecting..."
                : "Status: Active";
            PauseResumeButton.Content = "Pause Collection";
        }
    }

    private void LoadMcpSettings()
    {
        var settings = McpSettings.Load(App.ConfigDirectory);
        McpEnabledCheckBox.IsChecked = settings.Enabled;
        McpPortTextBox.Text = settings.Port.ToString();
    }

    private void UpdateMcpStatus()
    {
        if (_mcpService != null)
        {
            var settings = McpSettings.Load(App.ConfigDirectory);
            McpStatusText.Text = $"Status: Running on http://localhost:{settings.Port}";
        }
        else
        {
            var isEnabled = McpEnabledCheckBox.IsChecked == true;
            McpStatusText.Text = isEnabled
                ? "Status: Not running (restart app to apply)"
                : "Status: Disabled";
        }
    }

    private void PauseResumeButton_Click(object sender, RoutedEventArgs e)
    {
        if (_backgroundService == null)
        {
            return;
        }

        _backgroundService.IsPaused = !_backgroundService.IsPaused;
        UpdateCollectionStatus();
    }

    private void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        _scheduleManager.SaveSchedules();
        bool mcpChanged = SaveMcpSettings();
        SaveDefaultTimeRange();
        SaveAlertSettings();
        SaveSmtpSettings();

        var message = mcpChanged
            ? "Settings saved. MCP changes take effect after restarting the application."
            : "Settings saved.";
        MessageBox.Show(message, "Settings", MessageBoxButton.OK, MessageBoxImage.Information);
    }

    private bool SaveMcpSettings()
    {
        var settingsPath = Path.Combine(App.ConfigDirectory, "settings.json");

        try
        {
            JsonNode? root;
            if (File.Exists(settingsPath))
            {
                var json = File.ReadAllText(settingsPath);
                root = JsonNode.Parse(json) ?? new JsonObject();
            }
            else
            {
                root = new JsonObject();
            }

            var oldEnabled = root["mcp_enabled"]?.GetValue<bool>() ?? false;
            var oldPort = root["mcp_port"]?.GetValue<int>() ?? 5151;
            var newEnabled = McpEnabledCheckBox.IsChecked == true;
            int.TryParse(McpPortTextBox.Text, out var newPort);

            root["mcp_enabled"] = newEnabled;

            if (newPort > 0 && newPort < 65536)
            {
                root["mcp_port"] = newPort;
            }

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));

            return oldEnabled != newEnabled || oldPort != newPort;
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save MCP settings: {ex.Message}");
            return false;
        }
    }

    private void LoadDefaultTimeRange()
    {
        DefaultTimeRangeCombo.SelectedIndex = App.DefaultTimeRangeHours switch
        {
            1 => 0,
            4 => 1,
            12 => 2,
            24 => 3,
            168 => 4,
            _ => 1
        };
    }

    private void SaveDefaultTimeRange()
    {
        var hours = DefaultTimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 4
        };

        App.DefaultTimeRangeHours = hours;

        var settingsPath = Path.Combine(App.ConfigDirectory, "settings.json");
        try
        {
            JsonNode? root;
            if (File.Exists(settingsPath))
            {
                var json = File.ReadAllText(settingsPath);
                root = JsonNode.Parse(json) ?? new JsonObject();
            }
            else
            {
                root = new JsonObject();
            }

            root["default_time_range_hours"] = hours;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save default time range: {ex.Message}");
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

    private void LoadAlertSettings()
    {
        MinimizeToTrayCheckBox.IsChecked = App.MinimizeToTray;
        AlertsEnabledCheckBox.IsChecked = App.AlertsEnabled;
        NotifyConnectionCheckBox.IsChecked = App.NotifyConnectionChanges;
        AlertCpuCheckBox.IsChecked = App.AlertCpuEnabled;
        AlertCpuThresholdBox.Text = App.AlertCpuThreshold.ToString();
        AlertBlockingCheckBox.IsChecked = App.AlertBlockingEnabled;
        AlertBlockingThresholdBox.Text = App.AlertBlockingThreshold.ToString();
        AlertDeadlockCheckBox.IsChecked = App.AlertDeadlockEnabled;
        AlertDeadlockThresholdBox.Text = App.AlertDeadlockThreshold.ToString();
        AlertPoisonWaitCheckBox.IsChecked = App.AlertPoisonWaitEnabled;
        AlertPoisonWaitThresholdBox.Text = App.AlertPoisonWaitThresholdMs.ToString();
        AlertLongRunningQueryCheckBox.IsChecked = App.AlertLongRunningQueryEnabled;
        AlertLongRunningQueryThresholdBox.Text = App.AlertLongRunningQueryThresholdMinutes.ToString();
        AlertTempDbSpaceCheckBox.IsChecked = App.AlertTempDbSpaceEnabled;
        AlertTempDbSpaceThresholdBox.Text = App.AlertTempDbSpaceThresholdPercent.ToString();
        AlertLongRunningJobCheckBox.IsChecked = App.AlertLongRunningJobEnabled;
        AlertLongRunningJobMultiplierBox.Text = App.AlertLongRunningJobMultiplier.ToString();
        UpdateAlertControlStates();
    }

    private void SaveAlertSettings()
    {
        App.MinimizeToTray = MinimizeToTrayCheckBox.IsChecked == true;
        App.AlertsEnabled = AlertsEnabledCheckBox.IsChecked == true;
        App.NotifyConnectionChanges = NotifyConnectionCheckBox.IsChecked == true;
        App.AlertCpuEnabled = AlertCpuCheckBox.IsChecked == true;
        if (int.TryParse(AlertCpuThresholdBox.Text, out var cpu) && cpu > 0 && cpu <= 100)
            App.AlertCpuThreshold = cpu;
        App.AlertBlockingEnabled = AlertBlockingCheckBox.IsChecked == true;
        if (int.TryParse(AlertBlockingThresholdBox.Text, out var blocking) && blocking > 0)
            App.AlertBlockingThreshold = blocking;
        App.AlertDeadlockEnabled = AlertDeadlockCheckBox.IsChecked == true;
        if (int.TryParse(AlertDeadlockThresholdBox.Text, out var deadlock) && deadlock > 0)
            App.AlertDeadlockThreshold = deadlock;
        App.AlertPoisonWaitEnabled = AlertPoisonWaitCheckBox.IsChecked == true;
        if (int.TryParse(AlertPoisonWaitThresholdBox.Text, out var poisonWait) && poisonWait > 0)
            App.AlertPoisonWaitThresholdMs = poisonWait;
        App.AlertLongRunningQueryEnabled = AlertLongRunningQueryCheckBox.IsChecked == true;
        if (int.TryParse(AlertLongRunningQueryThresholdBox.Text, out var lrq) && lrq > 0)
            App.AlertLongRunningQueryThresholdMinutes = lrq;
        App.AlertTempDbSpaceEnabled = AlertTempDbSpaceCheckBox.IsChecked == true;
        if (int.TryParse(AlertTempDbSpaceThresholdBox.Text, out var tempDb) && tempDb > 0 && tempDb <= 100)
            App.AlertTempDbSpaceThresholdPercent = tempDb;
        App.AlertLongRunningJobEnabled = AlertLongRunningJobCheckBox.IsChecked == true;
        if (int.TryParse(AlertLongRunningJobMultiplierBox.Text, out var jobMult) && jobMult >= 2 && jobMult <= 20)
            App.AlertLongRunningJobMultiplier = jobMult;

        var settingsPath = Path.Combine(App.ConfigDirectory, "settings.json");
        try
        {
            JsonNode? root;
            if (File.Exists(settingsPath))
            {
                var json = File.ReadAllText(settingsPath);
                root = JsonNode.Parse(json) ?? new JsonObject();
            }
            else
            {
                root = new JsonObject();
            }

            root["minimize_to_tray"] = App.MinimizeToTray;
            root["alerts_enabled"] = App.AlertsEnabled;
            root["notify_connection_changes"] = App.NotifyConnectionChanges;
            root["alert_cpu_enabled"] = App.AlertCpuEnabled;
            root["alert_cpu_threshold"] = App.AlertCpuThreshold;
            root["alert_blocking_enabled"] = App.AlertBlockingEnabled;
            root["alert_blocking_threshold"] = App.AlertBlockingThreshold;
            root["alert_deadlock_enabled"] = App.AlertDeadlockEnabled;
            root["alert_deadlock_threshold"] = App.AlertDeadlockThreshold;
            root["alert_poison_wait_enabled"] = App.AlertPoisonWaitEnabled;
            root["alert_poison_wait_threshold_ms"] = App.AlertPoisonWaitThresholdMs;
            root["alert_long_running_query_enabled"] = App.AlertLongRunningQueryEnabled;
            root["alert_long_running_query_threshold_minutes"] = App.AlertLongRunningQueryThresholdMinutes;
            root["alert_tempdb_space_enabled"] = App.AlertTempDbSpaceEnabled;
            root["alert_tempdb_space_threshold_percent"] = App.AlertTempDbSpaceThresholdPercent;
            root["alert_long_running_job_enabled"] = App.AlertLongRunningJobEnabled;
            root["alert_long_running_job_multiplier"] = App.AlertLongRunningJobMultiplier;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save alert settings: {ex.Message}");
        }
    }

    private void AlertsEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        UpdateAlertControlStates();
    }

    private void UpdateAlertControlStates()
    {
        bool enabled = AlertsEnabledCheckBox.IsChecked == true;
        NotifyConnectionCheckBox.IsEnabled = enabled;
        AlertCpuCheckBox.IsEnabled = enabled;
        AlertCpuThresholdBox.IsEnabled = enabled;
        AlertBlockingCheckBox.IsEnabled = enabled;
        AlertBlockingThresholdBox.IsEnabled = enabled;
        AlertDeadlockCheckBox.IsEnabled = enabled;
        AlertDeadlockThresholdBox.IsEnabled = enabled;
        AlertPoisonWaitCheckBox.IsEnabled = enabled;
        AlertPoisonWaitThresholdBox.IsEnabled = enabled;
        AlertLongRunningQueryCheckBox.IsEnabled = enabled;
        AlertLongRunningQueryThresholdBox.IsEnabled = enabled;
        AlertTempDbSpaceCheckBox.IsEnabled = enabled;
        AlertTempDbSpaceThresholdBox.IsEnabled = enabled;
        AlertLongRunningJobCheckBox.IsEnabled = enabled;
        AlertLongRunningJobMultiplierBox.IsEnabled = enabled;
    }

    private void LoadSmtpSettings()
    {
        SmtpEnabledCheckBox.IsChecked = App.SmtpEnabled;
        SmtpServerBox.Text = App.SmtpServer;
        SmtpPortBox.Text = App.SmtpPort.ToString();
        SmtpSslCheckBox.IsChecked = App.SmtpUseSsl;
        SmtpUsernameBox.Text = App.SmtpUsername;
        SmtpFromBox.Text = App.SmtpFromAddress;
        SmtpRecipientsBox.Text = App.SmtpRecipients;

        /* Load password from credential store */
        var password = App.GetSmtpPassword();
        if (!string.IsNullOrEmpty(password))
        {
            SmtpPasswordBox.Password = password;
        }

        UpdateSmtpControlStates();
    }

    private void SaveSmtpSettings()
    {
        App.SmtpEnabled = SmtpEnabledCheckBox.IsChecked == true;
        App.SmtpServer = SmtpServerBox.Text?.Trim() ?? "";
        if (int.TryParse(SmtpPortBox.Text, out var port) && port > 0 && port < 65536)
            App.SmtpPort = port;
        App.SmtpUseSsl = SmtpSslCheckBox.IsChecked == true;
        App.SmtpUsername = SmtpUsernameBox.Text?.Trim() ?? "";
        App.SmtpFromAddress = SmtpFromBox.Text?.Trim() ?? "";
        App.SmtpRecipients = SmtpRecipientsBox.Text?.Trim() ?? "";

        /* Save password securely */
        if (!string.IsNullOrEmpty(SmtpPasswordBox.Password))
        {
            App.SaveSmtpPassword(SmtpPasswordBox.Password);
        }

        /* Save to settings.json */
        var settingsPath = Path.Combine(App.ConfigDirectory, "settings.json");
        try
        {
            JsonNode? root;
            if (File.Exists(settingsPath))
            {
                var json = File.ReadAllText(settingsPath);
                root = JsonNode.Parse(json) ?? new JsonObject();
            }
            else
            {
                root = new JsonObject();
            }

            root["smtp_enabled"] = App.SmtpEnabled;
            root["smtp_server"] = App.SmtpServer;
            root["smtp_port"] = App.SmtpPort;
            root["smtp_use_ssl"] = App.SmtpUseSsl;
            root["smtp_username"] = App.SmtpUsername;
            root["smtp_from_address"] = App.SmtpFromAddress;
            root["smtp_recipients"] = App.SmtpRecipients;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save SMTP settings: {ex.Message}");
        }
    }

    private void SmtpEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        UpdateSmtpControlStates();
    }

    private void UpdateSmtpControlStates()
    {
        bool enabled = SmtpEnabledCheckBox.IsChecked == true;
        SmtpServerBox.IsEnabled = enabled;
        SmtpPortBox.IsEnabled = enabled;
        SmtpSslCheckBox.IsEnabled = enabled;
        SmtpUsernameBox.IsEnabled = enabled;
        SmtpPasswordBox.IsEnabled = enabled;
        SmtpFromBox.IsEnabled = enabled;
        SmtpRecipientsBox.IsEnabled = enabled;
        TestEmailButton.IsEnabled = enabled;
    }

    private async void TestEmailButton_Click(object sender, RoutedEventArgs e)
    {
        /* Temporarily apply current UI values for the test */
        App.SmtpServer = SmtpServerBox.Text?.Trim() ?? "";
        if (int.TryParse(SmtpPortBox.Text, out var port))
            App.SmtpPort = port;
        App.SmtpUseSsl = SmtpSslCheckBox.IsChecked == true;
        App.SmtpUsername = SmtpUsernameBox.Text?.Trim() ?? "";
        App.SmtpFromAddress = SmtpFromBox.Text?.Trim() ?? "";
        App.SmtpRecipients = SmtpRecipientsBox.Text?.Trim() ?? "";

        if (!string.IsNullOrEmpty(SmtpPasswordBox.Password))
        {
            App.SaveSmtpPassword(SmtpPasswordBox.Password);
        }

        TestEmailButton.IsEnabled = false;
        TestEmailButton.Content = "Sending...";

        try
        {
            var error = await Services.EmailAlertService.SendTestEmailAsync();
            if (error == null)
            {
                MessageBox.Show("Test email sent successfully!", "Test Email", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send test email:\n\n{error}", "Test Email Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        finally
        {
            TestEmailButton.Content = "Send Test Email";
            TestEmailButton.IsEnabled = true;
        }
    }

    private void CloseButton_Click(object sender, RoutedEventArgs e)
    {
        Close();
    }
}
