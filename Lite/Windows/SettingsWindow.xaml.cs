/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Navigation;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class SettingsWindow : Window
{
    private readonly ScheduleManager _scheduleManager;
    private readonly ServerManager _serverManager;
    private readonly CollectionBackgroundService? _backgroundService;
    private readonly McpHostService? _mcpService;
    private readonly MuteRuleService? _muteRuleService;

    public SettingsWindow(
        ScheduleManager scheduleManager,
        ServerManager serverManager,
        CollectionBackgroundService? backgroundService = null,
        McpHostService? mcpService = null,
        MuteRuleService? muteRuleService = null)
    {
        InitializeComponent();
        _scheduleManager = scheduleManager;
        _serverManager = serverManager;
        _backgroundService = backgroundService;
        _mcpService = mcpService;
        _muteRuleService = muteRuleService;

        LoadServerScheduleSummary();
        UpdateCollectionStatus();
        LoadMcpSettings();
        UpdateMcpStatus();
        LoadDefaultTimeRange();
        LoadConnectionTimeout();
        LoadCsvSeparator();
        LoadColorTheme();
        LoadTimeDisplayMode();
        LoadAlertSettings();
        LoadSmtpSettings();
        LoadWebhookSettings();
    }

    private void LoadServerScheduleSummary()
    {
        var servers = _serverManager.GetAllServers();
        var rows = servers.Select(s => new ServerScheduleRow
        {
            ServerId = s.Id,
            ServerName = s.DisplayName,
            Preset = _scheduleManager.GetActivePresetForServer(s.Id),
            Status = _scheduleManager.HasServerOverride(s.Id) ? "Customized" : "Default"
        }).ToList();

        ServerScheduleGrid.ItemsSource = rows;
        DefaultPresetText.Text = _scheduleManager.GetActivePreset();
    }

    private void EditServerSchedule_Click(object sender, RoutedEventArgs e)
    {
        OpenServerScheduleEditor();
    }

    private void ServerScheduleGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        OpenServerScheduleEditor();
    }

    private void OpenServerScheduleEditor()
    {
        if (ServerScheduleGrid.SelectedItem is not ServerScheduleRow row) return;

        var server = _serverManager.GetAllServers().FirstOrDefault(s => s.Id == row.ServerId);
        if (server == null) return;

        var editor = new CollectorScheduleEditorWindow(_scheduleManager, _serverManager, server.Id, server.DisplayName) { Owner = this };
        editor.ShowDialog();

        if (editor.Saved)
        {
            LoadServerScheduleSummary();
        }
    }

    private void EditDefaultSchedule_Click(object sender, RoutedEventArgs e)
    {
        var editor = new CollectorScheduleEditorWindow(_scheduleManager, _serverManager) { Owner = this };
        editor.ShowDialog();

        if (editor.Saved)
        {
            LoadServerScheduleSummary();
        }
    }

    private void ApplyDefaultToAll_Click(object sender, RoutedEventArgs e)
    {
        var servers = _serverManager.GetAllServers();
        var customCount = servers.Count(s => _scheduleManager.HasServerOverride(s.Id));

        if (customCount == 0)
        {
            MessageBox.Show("All servers are already using the default schedule.", "Apply Default", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var result = MessageBox.Show(
            $"Remove custom schedules from {customCount} server(s) and revert them to the default schedule?\n\nThis cannot be undone.",
            "Apply Default to All",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (result != MessageBoxResult.Yes) return;

        foreach (var server in servers)
        {
            _scheduleManager.RemoveServerOverride(server.Id);
        }

        LoadServerScheduleSummary();
    }

    private class ServerScheduleRow
    {
        public string ServerId { get; set; } = "";
        public string ServerName { get; set; } = "";
        public string Preset { get; set; } = "";
        public string Status { get; set; } = "";
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

    private async void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        var (mcpChanged, mcpValid) = await SaveMcpSettingsAsync();
        SaveDefaultTimeRange();
        SaveConnectionTimeout();
        SaveCsvSeparator();
        SaveColorTheme();
        SaveTimeDisplayMode();
        bool alertsValid = SaveAlertSettings();
        SaveSmtpSettings();
        SaveWebhookSettings();

        _saved = true;
        if (mcpChanged) McpSettingsChanged = true;

        if (!alertsValid || !mcpValid) return;

        var message = mcpChanged
            ? "Settings saved. MCP changes take effect after restarting the application."
            : "Settings saved.";
        MessageBox.Show(message, "Settings", MessageBoxButton.OK, MessageBoxImage.Information);
    }

    private async Task<(bool Changed, bool Valid)> SaveMcpSettingsAsync()
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

            if (newEnabled && (newPort != oldPort || !oldEnabled))
            {
                if (newPort < 1024 || newPort > IPEndPoint.MaxPort)
                {
                    MessageBox.Show(
                        $"MCP port must be between 1024 and {IPEndPoint.MaxPort}.\nPorts 0–1023 are well-known privileged ports reserved by the operating system.",
                        "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return (true, false);
                }

                // CanBindTcpPortAsync attempts an actual bind, which is more reliable
                // than checking listeners (TOCTOU is still possible but less likely)
                bool canBind = await PortUtilityService.CanBindTcpPortAsync(newPort, IPAddress.Loopback);
                if (!canBind)
                {
                    MessageBox.Show(
                        $"Port {newPort} is already in use. Choose a different port for the MCP server.",
                        "Port Conflict", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return (true, false);
                }
            }

            root["mcp_enabled"] = newEnabled;

            bool portValid = true;
            if (newPort >= 1024 && newPort <= IPEndPoint.MaxPort)
            {
                root["mcp_port"] = newPort;
            }
            else
            {
                portValid = false;
            }

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));

            if (!portValid)
            {
                MessageBox.Show(
                    "MCP port failed validation - must be a valid TCP port number.\nOther MCP settings were saved.",
                    "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
            }

            return (oldEnabled != newEnabled || oldPort != newPort, portValid);
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save MCP settings: {ex.Message}");
            return (false, true);
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

    private async void AutoPortButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            int port = await PortUtilityService.GetFreeTcpPortAsync();
            McpPortTextBox.Text = port.ToString();
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not find an available port: {ex.Message}",
                "Error", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private void LoadConnectionTimeout()
    {
        ConnectionTimeoutBox.Text = App.ConnectionTimeoutSeconds.ToString();
    }

    private void SaveConnectionTimeout()
    {
        if (int.TryParse(ConnectionTimeoutBox.Text, out var timeout) && timeout >= 5 && timeout <= 60)
        {
            App.ConnectionTimeoutSeconds = timeout;
        }

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

            root["connection_timeout_seconds"] = App.ConnectionTimeoutSeconds;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save connection timeout: {ex.Message}");
        }
    }

    private void LoadCsvSeparator()
    {
        foreach (ComboBoxItem item in CsvSeparatorCombo.Items)
        {
            if (item.Tag?.ToString() == App.CsvSeparator)
            {
                CsvSeparatorCombo.SelectedItem = item;
                break;
            }
        }
        if (CsvSeparatorCombo.SelectedItem == null)
            CsvSeparatorCombo.SelectedIndex = 0;
    }

    private void SaveCsvSeparator()
    {
        if (CsvSeparatorCombo.SelectedItem is ComboBoxItem selected && selected.Tag is string sep)
        {
            App.CsvSeparator = sep;
        }

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

            root["csv_separator"] = App.CsvSeparator;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save CSV separator: {ex.Message}");
        }
    }

    private bool _isLoadingTheme;
    private readonly string _originalTheme = Helpers.ThemeManager.CurrentTheme;
    private bool _saved;
    public bool McpSettingsChanged { get; private set; }

    private void ColorThemeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_isLoadingTheme) return;
        if (ColorThemeCombo.SelectedItem is ComboBoxItem selected && selected.Tag is string theme)
            Helpers.ThemeManager.Apply(theme);
    }

    private void LoadColorTheme()
    {
        _isLoadingTheme = true;
        foreach (ComboBoxItem item in ColorThemeCombo.Items)
        {
            if (item.Tag?.ToString() == App.ColorTheme)
            {
                ColorThemeCombo.SelectedItem = item;
                break;
            }
        }
        if (ColorThemeCombo.SelectedItem == null)
            ColorThemeCombo.SelectedIndex = 0;
        _isLoadingTheme = false;
    }

    private void SaveColorTheme()
    {
        if (ColorThemeCombo.SelectedItem is ComboBoxItem selected && selected.Tag is string theme)
        {
            App.ColorTheme = theme;
            Helpers.ThemeManager.Apply(theme);
        }

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

            root["color_theme"] = App.ColorTheme;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save color theme: {ex.Message}");
        }
    }

    private void LoadTimeDisplayMode()
    {
        foreach (ComboBoxItem item in TimeDisplayModeCombo.Items)
        {
            if (item.Tag?.ToString() == App.TimeDisplayMode)
            {
                TimeDisplayModeCombo.SelectedItem = item;
                break;
            }
        }
        if (TimeDisplayModeCombo.SelectedItem == null)
            TimeDisplayModeCombo.SelectedIndex = 0;
    }

    private void SaveTimeDisplayMode()
    {
        if (TimeDisplayModeCombo.SelectedItem is ComboBoxItem selected && selected.Tag is string mode)
        {
            App.TimeDisplayMode = mode;
            if (System.Enum.TryParse<Helpers.TimeDisplayMode>(mode, out var tdm))
                ServerTimeHelper.CurrentDisplayMode = tdm;
        }

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

            root["time_display_mode"] = App.TimeDisplayMode;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save time display mode: {ex.Message}");
        }
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
        AlertLongRunningQueryMaxResultsBox.Text = App.AlertLongRunningQueryMaxResults.ToString();
        LrqExcludeSpServerDiagnosticsCheckBox.IsChecked = App.AlertLongRunningQueryExcludeSpServerDiagnostics;
        LrqExcludeWaitForCheckBox.IsChecked = App.AlertLongRunningQueryExcludeWaitFor;
        LrqExcludeBackupsCheckBox.IsChecked = App.AlertLongRunningQueryExcludeBackups;
        LrqExcludeMiscWaitsCheckBox.IsChecked = App.AlertLongRunningQueryExcludeMiscWaits;
        AlertExcludedDatabasesBox.Text = string.Join(", ", App.AlertExcludedDatabases);
        AlertTempDbSpaceCheckBox.IsChecked = App.AlertTempDbSpaceEnabled;
        AlertTempDbSpaceThresholdBox.Text = App.AlertTempDbSpaceThresholdPercent.ToString();
        AlertLongRunningJobCheckBox.IsChecked = App.AlertLongRunningJobEnabled;
        AlertLongRunningJobMultiplierBox.Text = App.AlertLongRunningJobMultiplier.ToString();
        AlertCooldownBox.Text = App.AlertCooldownMinutes.ToString();
        EmailCooldownBox.Text = App.EmailCooldownMinutes.ToString();
        MuteRuleDefaultExpirationCombo.SelectedIndex = App.MuteRuleDefaultExpiration switch
        {
            "1 hour" => 0,
            "24 hours" => 1,
            "7 days" => 2,
            _ => 3
        };
        LogAlertDismissalsCheckBox.IsChecked = App.LogAlertDismissals;
        UpdateAlertControlStates();
    }

    private bool SaveAlertSettings()
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
        if (int.TryParse(AlertLongRunningQueryMaxResultsBox.Text, out var lrqMax) && lrqMax >= 1 && lrqMax <= int.MaxValue)
            App.AlertLongRunningQueryMaxResults = lrqMax;
        App.AlertLongRunningQueryExcludeSpServerDiagnostics = LrqExcludeSpServerDiagnosticsCheckBox.IsChecked == true;
        App.AlertLongRunningQueryExcludeWaitFor = LrqExcludeWaitForCheckBox.IsChecked == true;
        App.AlertLongRunningQueryExcludeBackups = LrqExcludeBackupsCheckBox.IsChecked == true;
        App.AlertLongRunningQueryExcludeMiscWaits = LrqExcludeMiscWaitsCheckBox.IsChecked == true;
        App.AlertExcludedDatabases = AlertExcludedDatabasesBox.Text
            .Split(',')
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();
        App.AlertTempDbSpaceEnabled = AlertTempDbSpaceCheckBox.IsChecked == true;
        if (int.TryParse(AlertTempDbSpaceThresholdBox.Text, out var tempDb) && tempDb > 0 && tempDb <= 100)
            App.AlertTempDbSpaceThresholdPercent = tempDb;
        App.AlertLongRunningJobEnabled = AlertLongRunningJobCheckBox.IsChecked == true;
        if (int.TryParse(AlertLongRunningJobMultiplierBox.Text, out var jobMult) && jobMult >= 2 && jobMult <= 20)
            App.AlertLongRunningJobMultiplier = jobMult;
        var validationErrors = new List<string>();
        if (int.TryParse(AlertCooldownBox.Text, out var alertCooldown) && alertCooldown >= 1 && alertCooldown <= 120)
            App.AlertCooldownMinutes = alertCooldown;
        else
            validationErrors.Add("Tray notification cooldown must be between 1 and 120 minutes.");
        if (int.TryParse(EmailCooldownBox.Text, out var emailCooldown) && emailCooldown >= 1 && emailCooldown <= 120)
            App.EmailCooldownMinutes = emailCooldown;
        else
            validationErrors.Add("Email alert cooldown must be between 1 and 120 minutes.");
        App.MuteRuleDefaultExpiration = (MuteRuleDefaultExpirationCombo.SelectedItem as ComboBoxItem)?.Content?.ToString() ?? "24 hours";
        App.LogAlertDismissals = LogAlertDismissalsCheckBox.IsChecked == true;

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
            root["alert_long_running_query_max_results"] = App.AlertLongRunningQueryMaxResults;
            root["alert_long_running_query_exclude_sp_server_diagnostics"] = App.AlertLongRunningQueryExcludeSpServerDiagnostics;
            root["alert_long_running_query_exclude_waitfor"] = App.AlertLongRunningQueryExcludeWaitFor;
            root["alert_long_running_query_exclude_backups"] = App.AlertLongRunningQueryExcludeBackups;
            root["alert_long_running_query_exclude_misc_waits"] = App.AlertLongRunningQueryExcludeMiscWaits;
            var dbArray = new System.Text.Json.Nodes.JsonArray();
            foreach (var db in App.AlertExcludedDatabases) dbArray.Add(db);
            root["alert_excluded_databases"] = dbArray;
            root["alert_tempdb_space_enabled"] = App.AlertTempDbSpaceEnabled;
            root["alert_tempdb_space_threshold_percent"] = App.AlertTempDbSpaceThresholdPercent;
            root["alert_long_running_job_enabled"] = App.AlertLongRunningJobEnabled;
            root["alert_long_running_job_multiplier"] = App.AlertLongRunningJobMultiplier;
            root["alert_cooldown_minutes"] = App.AlertCooldownMinutes;
            root["email_cooldown_minutes"] = App.EmailCooldownMinutes;
            root["mute_rule_default_expiration"] = App.MuteRuleDefaultExpiration;
            root["log_alert_dismissals"] = App.LogAlertDismissals;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save alert settings: {ex.Message}");
        }

        if (validationErrors.Count > 0)
        {
            MessageBox.Show(
                "Some alert settings have invalid values and were not changed:\n\n" +
                string.Join("\n", validationErrors),
                "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        return true;
    }

    private void AlertsEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        UpdateAlertControlStates();
    }

    private void RestoreAlertDefaultsButton_Click(object sender, RoutedEventArgs e)
    {
        AlertCpuThresholdBox.Text = "80";
        AlertBlockingThresholdBox.Text = "1";
        AlertDeadlockThresholdBox.Text = "1";
        AlertPoisonWaitThresholdBox.Text = "500";
        AlertLongRunningQueryThresholdBox.Text = "30";
        AlertTempDbSpaceThresholdBox.Text = "80";
        AlertLongRunningJobMultiplierBox.Text = "3";
        AlertCooldownBox.Text = "5";
        EmailCooldownBox.Text = "15";
        AlertExcludedDatabasesBox.Text = "";
        MuteRuleDefaultExpirationCombo.SelectedIndex = 1; // 24 hours
        UpdateAlertPreviewText();
    }

    private void ManageMuteRulesButton_Click(object sender, RoutedEventArgs e)
    {
        if (_muteRuleService == null) return;
        var window = new ManageMuteRulesWindow(_muteRuleService) { Owner = this };
        window.ShowDialog();
    }

    private void UpdateAlertPreviewText()
    {
        var parts = new System.Collections.Generic.List<string>();

        if (AlertCpuCheckBox.IsChecked == true)
            parts.Add($"CPU > {AlertCpuThresholdBox.Text}%");
        if (AlertBlockingCheckBox.IsChecked == true)
            parts.Add($"blocking >= {AlertBlockingThresholdBox.Text}");
        if (AlertDeadlockCheckBox.IsChecked == true)
            parts.Add($"deadlocks >= {AlertDeadlockThresholdBox.Text}");
        if (AlertPoisonWaitCheckBox.IsChecked == true)
            parts.Add($"poison waits >= {AlertPoisonWaitThresholdBox.Text}ms avg");
        if (AlertLongRunningQueryCheckBox.IsChecked == true)
            parts.Add($"queries > {AlertLongRunningQueryThresholdBox.Text}min");
        if (AlertTempDbSpaceCheckBox.IsChecked == true)
            parts.Add($"TempDB > {AlertTempDbSpaceThresholdBox.Text}%");
        if (AlertLongRunningJobCheckBox.IsChecked == true)
            parts.Add($"jobs > {AlertLongRunningJobMultiplierBox.Text}x avg");

        AlertPreviewText.Text = parts.Count > 0
            ? $"Will alert when: {string.Join(", ", parts)}"
            : "No alerts enabled";
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
        UpdateAlertPreviewText();
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
        ValidateSmtpButton.IsEnabled = enabled;
    }

    private void ValidateSmtpButton_Click(object sender, RoutedEventArgs e)
    {
        var errors = new System.Collections.Generic.List<string>();

        if (string.IsNullOrWhiteSpace(SmtpServerBox.Text))
            errors.Add("SMTP server is required");
        if (!int.TryParse(SmtpPortBox.Text, out var port) || port < 1 || port > 65535)
            errors.Add("Port must be between 1 and 65535");
        if (string.IsNullOrWhiteSpace(SmtpFromBox.Text))
            errors.Add("From address is required");
        else if (!SmtpFromBox.Text.Trim().Contains('@'))
            errors.Add("From address must be a valid email");
        if (string.IsNullOrWhiteSpace(SmtpRecipientsBox.Text))
            errors.Add("At least one recipient is required");

        if (errors.Count == 0)
        {
            SmtpStatusText.Text = "Settings look good. Use 'Send Test Email' to verify delivery.";
        }
        else
        {
            SmtpStatusText.Text = "";
            MessageBox.Show(
                "SMTP configuration has issues:\n\n" + string.Join("\n", errors),
                "SMTP Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private async void TestEmailButton_Click(object sender, RoutedEventArgs e)
    {
        /* Save current App values so we can restore after the test */
        var origServer = App.SmtpServer;
        var origPort = App.SmtpPort;
        var origSsl = App.SmtpUseSsl;
        var origUsername = App.SmtpUsername;
        var origFrom = App.SmtpFromAddress;
        var origRecipients = App.SmtpRecipients;

        TestEmailButton.IsEnabled = false;
        TestEmailButton.Content = "Sending...";

        try
        {
            /* Temporarily apply current UI values for the test */
            App.SmtpServer = SmtpServerBox.Text?.Trim() ?? "";
            if (int.TryParse(SmtpPortBox.Text, out var port))
                App.SmtpPort = port;
            App.SmtpUseSsl = SmtpSslCheckBox.IsChecked == true;
            App.SmtpUsername = SmtpUsernameBox.Text?.Trim() ?? "";
            App.SmtpFromAddress = SmtpFromBox.Text?.Trim() ?? "";
            App.SmtpRecipients = SmtpRecipientsBox.Text?.Trim() ?? "";

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
            /* Restore original values — only Save should persist changes */
            App.SmtpServer = origServer;
            App.SmtpPort = origPort;
            App.SmtpUseSsl = origSsl;
            App.SmtpUsername = origUsername;
            App.SmtpFromAddress = origFrom;
            App.SmtpRecipients = origRecipients;

            TestEmailButton.Content = "Send Test Email";
            TestEmailButton.IsEnabled = true;
        }
    }

    // ============================================
    // Webhooks (Teams / Slack)
    // ============================================

    private void LoadWebhookSettings()
    {
        TeamsWebhookEnabledCheckBox.IsChecked = App.TeamsWebhookEnabled;
        TeamsWebhookUrlBox.Text = App.TeamsWebhookUrl;
        TeamsProxyAddressBox.Text = App.TeamsProxyAddress;
        SlackWebhookEnabledCheckBox.IsChecked = App.SlackWebhookEnabled;
        SlackWebhookUrlBox.Text = App.SlackWebhookUrl;
        SlackProxyAddressBox.Text = App.SlackProxyAddress;
        UpdateTeamsControlStates();
        UpdateSlackControlStates();
    }

    private void SaveWebhookSettings()
    {
        App.TeamsWebhookEnabled = TeamsWebhookEnabledCheckBox.IsChecked == true;
        App.TeamsWebhookUrl = TeamsWebhookUrlBox.Text?.Trim() ?? "";
        App.TeamsProxyAddress = TeamsProxyAddressBox.Text?.Trim() ?? "";
        App.SlackWebhookEnabled = SlackWebhookEnabledCheckBox.IsChecked == true;
        App.SlackWebhookUrl = SlackWebhookUrlBox.Text?.Trim() ?? "";
        App.SlackProxyAddress = SlackProxyAddressBox.Text?.Trim() ?? "";

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

            root["teams_webhook_enabled"] = App.TeamsWebhookEnabled;
            root["teams_webhook_url"] = App.TeamsWebhookUrl;
            root["teams_proxy_address"] = App.TeamsProxyAddress;
            root["slack_webhook_enabled"] = App.SlackWebhookEnabled;
            root["slack_webhook_url"] = App.SlackWebhookUrl;
            root["slack_proxy_address"] = App.SlackProxyAddress;

            var options = new JsonSerializerOptions { WriteIndented = true };
            File.WriteAllText(settingsPath, root.ToJsonString(options));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Settings", $"Failed to save webhook settings: {ex.Message}");
        }
    }

    private void TeamsWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        UpdateTeamsControlStates();
    }

    private void SlackWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        UpdateSlackControlStates();
    }

    private void UpdateTeamsControlStates()
    {
        bool enabled = TeamsWebhookEnabledCheckBox.IsChecked == true;
        TeamsWebhookUrlBox.IsEnabled = enabled;
        TeamsProxyAddressBox.IsEnabled = enabled;
        TestTeamsButton.IsEnabled = enabled;
    }

    private void UpdateSlackControlStates()
    {
        bool enabled = SlackWebhookEnabledCheckBox.IsChecked == true;
        SlackWebhookUrlBox.IsEnabled = enabled;
        SlackProxyAddressBox.IsEnabled = enabled;
        TestSlackButton.IsEnabled = enabled;
    }

    private async void TestTeamsButton_Click(object sender, RoutedEventArgs e)
    {
        TestTeamsButton.IsEnabled = false;
        TestTeamsButton.Content = "Sending...";

        try
        {
            var url = TeamsWebhookUrlBox.Text?.Trim() ?? "";
            var proxy = TeamsProxyAddressBox.Text?.Trim();
            var error = await WebhookAlertService.SendTestTeamsAsync(url, proxy);

            if (error == null)
            {
                MessageBox.Show("Teams test notification sent successfully!", "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send Teams test notification:\n\n{error}", "Test Webhook Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to send Teams test notification:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            TestTeamsButton.Content = "Send Test Notification";
            TestTeamsButton.IsEnabled = true;
        }
    }

    private async void TestSlackButton_Click(object sender, RoutedEventArgs e)
    {
        TestSlackButton.IsEnabled = false;
        TestSlackButton.Content = "Sending...";

        try
        {
            var url = SlackWebhookUrlBox.Text?.Trim() ?? "";
            var proxy = SlackProxyAddressBox.Text?.Trim();
            var error = await WebhookAlertService.SendTestSlackAsync(url, proxy);

            if (error == null)
            {
                MessageBox.Show("Slack test notification sent successfully!", "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send Slack test notification:\n\n{error}", "Test Webhook Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to send Slack test notification:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            TestSlackButton.Content = "Send Test Notification";
            TestSlackButton.IsEnabled = true;
        }
    }

    private void Hyperlink_RequestNavigate(object sender, RequestNavigateEventArgs e)
    {
        try
        {
            Process.Start(new ProcessStartInfo { FileName = e.Uri.AbsoluteUri, UseShellExecute = true });
        }
        catch { }
        e.Handled = true;
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.ExportToCsv(sender, "schedules");

    private void CloseButton_Click(object sender, RoutedEventArgs e)
    {
        if (!_saved)
            Helpers.ThemeManager.Apply(_originalTheme);
        Close();
    }

    protected override void OnClosing(System.ComponentModel.CancelEventArgs e)
    {
        if (!_saved)
            Helpers.ThemeManager.Apply(_originalTheme);
        base.OnClosing(e);
    }
}
