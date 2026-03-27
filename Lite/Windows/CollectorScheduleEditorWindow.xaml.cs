/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class CollectorScheduleEditorWindow : Window
{
    private readonly ScheduleManager _scheduleManager;
    private readonly ServerManager _serverManager;
    private readonly string? _serverId;
    private readonly string? _serverDisplayName;
    private List<CollectorSchedule> _editingSchedules = new();
    private bool _suppressPresetChange;
    private bool _isEditingDefault;

    /// <summary>
    /// True if the user saved changes.
    /// </summary>
    public bool Saved { get; private set; }

    /// <summary>
    /// Opens the editor for a specific server's schedule.
    /// </summary>
    public CollectorScheduleEditorWindow(
        ScheduleManager scheduleManager,
        ServerManager serverManager,
        string serverId,
        string serverDisplayName)
    {
        InitializeComponent();
        _scheduleManager = scheduleManager;
        _serverManager = serverManager;
        _serverId = serverId;
        _serverDisplayName = serverDisplayName;
        _isEditingDefault = false;

        Title = $"Collector Schedules - {serverDisplayName}";
        HeaderText.Text = $"Collector Schedules - {serverDisplayName}";
        SubHeaderText.Text = $"Server: {serverDisplayName}";

        SetupCopyFromServerCombo();
        LoadServerSchedule();
    }

    /// <summary>
    /// Opens the editor for the default schedule.
    /// </summary>
    public CollectorScheduleEditorWindow(
        ScheduleManager scheduleManager,
        ServerManager serverManager)
    {
        InitializeComponent();
        _scheduleManager = scheduleManager;
        _serverManager = serverManager;
        _isEditingDefault = true;

        Title = "Default Collector Schedule";
        HeaderText.Text = "Default Collector Schedule";
        SubHeaderText.Text = "This schedule applies to all servers without a custom override.";

        /* Hide server-specific controls */
        UseDefaultCheckBox.Visibility = Visibility.Collapsed;
        CopyFromDefaultButton.Visibility = Visibility.Collapsed;

        _editingSchedules = CloneScheduleList(_scheduleManager.GetDefaultSchedule());
        ScheduleGrid.ItemsSource = _editingSchedules;
        DetectActivePreset();
    }

    private void SetupCopyFromServerCombo()
    {
        var servers = _serverManager.GetAllServers()
            .Where(s => s.Id != _serverId)
            .ToList();

        if (servers.Count > 0)
        {
            CopyFromServerCombo.Visibility = Visibility.Visible;
            CopyFromServerButton.Visibility = Visibility.Visible;
            CopyFromServerCombo.DisplayMemberPath = "DisplayName";
            CopyFromServerCombo.SelectedValuePath = "Id";
            CopyFromServerCombo.ItemsSource = servers;
            CopyFromServerCombo.SelectedIndex = 0;
        }
    }

    private void LoadServerSchedule()
    {
        bool usesDefault = !_scheduleManager.HasServerOverride(_serverId!);
        UseDefaultCheckBox.IsChecked = usesDefault;

        _editingSchedules = CloneScheduleList(_scheduleManager.GetSchedulesForServer(_serverId!));
        ScheduleGrid.ItemsSource = _editingSchedules;
        UpdateEditableState(usesDefault);
        DetectActivePreset();
    }

    private void UpdateEditableState(bool usesDefault)
    {
        bool editable = !usesDefault;
        ScheduleGrid.IsReadOnly = usesDefault;
        ScheduleGrid.Opacity = usesDefault ? 0.6 : 1.0;
        PresetComboBox.IsEnabled = editable;
        CopyFromDefaultButton.IsEnabled = editable;
        CopyFromServerButton.IsEnabled = editable;
        CopyFromServerCombo.IsEnabled = editable;

        StatusText.Text = usesDefault
            ? "Using default schedule (read-only). Uncheck 'Use default schedule' to customize this server."
            : "Custom schedule. Changes apply only to this server.";
    }

    private void UseDefaultCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (_isEditingDefault) return;

        bool usesDefault = UseDefaultCheckBox.IsChecked == true;

        if (!usesDefault)
        {
            /* Switching from default to custom — copy current defaults as starting point */
            _editingSchedules = CloneScheduleList(_scheduleManager.GetDefaultSchedule());
            ScheduleGrid.ItemsSource = _editingSchedules;
        }
        else
        {
            /* Switching to default — show the default schedule (read-only) */
            _editingSchedules = CloneScheduleList(_scheduleManager.GetDefaultSchedule());
            ScheduleGrid.ItemsSource = _editingSchedules;
        }

        UpdateEditableState(usesDefault);
        DetectActivePreset();
    }

    private void DetectActivePreset()
    {
        _suppressPresetChange = true;
        try
        {
            string active = DetectPresetForList(_editingSchedules);
            for (int i = 0; i < PresetComboBox.Items.Count; i++)
            {
                if (PresetComboBox.Items[i] is ComboBoxItem item &&
                    string.Equals(item.Content?.ToString(), active, StringComparison.OrdinalIgnoreCase))
                {
                    PresetComboBox.SelectedIndex = i;
                    return;
                }
            }
            PresetComboBox.SelectedIndex = 0;
        }
        finally
        {
            _suppressPresetChange = false;
        }
    }

    private void PresetComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressPresetChange) return;
        if (PresetComboBox.SelectedItem is not ComboBoxItem selected) return;

        string presetName = selected.Content?.ToString() ?? "";
        if (presetName == "Custom") return;

        var result = MessageBox.Show(
            $"Apply the \"{presetName}\" preset?\n\nThis will change all collector frequencies. Enabled/disabled state and retention settings are not affected.",
            "Apply Collection Preset",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            DetectActivePreset();
            return;
        }

        ApplyPresetToList(_editingSchedules, presetName);
        ScheduleGrid.ItemsSource = null;
        ScheduleGrid.ItemsSource = _editingSchedules;
        DetectActivePreset();
    }

    private void CopyFromDefault_Click(object sender, RoutedEventArgs e)
    {
        var result = MessageBox.Show(
            "Replace this server's schedule with a copy of the default schedule?",
            "Copy from Default",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes) return;

        _editingSchedules = CloneScheduleList(_scheduleManager.GetDefaultSchedule());
        ScheduleGrid.ItemsSource = _editingSchedules;
        DetectActivePreset();
    }

    private void CopyFromServer_Click(object sender, RoutedEventArgs e)
    {
        if (CopyFromServerCombo.SelectedItem is not Models.ServerConnection selected) return;
        var sourceServerId = selected.Id;

        var result = MessageBox.Show(
            $"Replace this server's schedule with a copy of {selected.DisplayName}'s schedule?",
            "Copy from Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes) return;

        _editingSchedules = CloneScheduleList(_scheduleManager.GetSchedulesForServer(sourceServerId));
        ScheduleGrid.ItemsSource = _editingSchedules;
        DetectActivePreset();
    }

    private void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        if (_isEditingDefault)
        {
            /* Save to default schedule */
            foreach (var edited in _editingSchedules)
            {
                _scheduleManager.UpdateSchedule(edited.Name,
                    enabled: edited.Enabled,
                    frequencyMinutes: edited.FrequencyMinutes,
                    retentionDays: edited.RetentionDays);
            }
        }
        else if (UseDefaultCheckBox.IsChecked == true)
        {
            /* Revert to default — remove override */
            _scheduleManager.RemoveServerOverride(_serverId!);
        }
        else
        {
            /* Save per-server override */
            _scheduleManager.SetScheduleForServer(_serverId!, _editingSchedules);
        }

        Saved = true;
        Close();
    }

    private void CancelButton_Click(object sender, RoutedEventArgs e)
    {
        Close();
    }

    // ──────────────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────────────

    private static readonly Dictionary<string, Dictionary<string, int>> s_presets = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Aggressive"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 1, ["query_stats"] = 1, ["procedure_stats"] = 1,
            ["query_store"] = 2, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
            ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 2,
            ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 1,
            ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
            ["blocked_process_report"] = 1, ["running_jobs"] = 2
        },
        ["Balanced"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 1, ["query_stats"] = 1, ["procedure_stats"] = 1,
            ["query_store"] = 5, ["query_snapshots"] = 1, ["cpu_utilization"] = 1,
            ["file_io_stats"] = 1, ["memory_stats"] = 1, ["memory_clerks"] = 5,
            ["tempdb_stats"] = 1, ["perfmon_stats"] = 1, ["deadlocks"] = 1,
            ["memory_grant_stats"] = 1, ["waiting_tasks"] = 1,
            ["blocked_process_report"] = 1, ["running_jobs"] = 5
        },
        ["Low-Impact"] = new(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = 5, ["query_stats"] = 10, ["procedure_stats"] = 10,
            ["query_store"] = 30, ["query_snapshots"] = 5, ["cpu_utilization"] = 5,
            ["file_io_stats"] = 10, ["memory_stats"] = 10, ["memory_clerks"] = 30,
            ["tempdb_stats"] = 5, ["perfmon_stats"] = 5, ["deadlocks"] = 5,
            ["memory_grant_stats"] = 5, ["waiting_tasks"] = 5,
            ["blocked_process_report"] = 5, ["running_jobs"] = 30
        }
    };

    private static string DetectPresetForList(List<CollectorSchedule> schedules)
    {
        foreach (var (presetName, intervals) in s_presets)
        {
            bool matches = true;
            foreach (var (collector, freq) in intervals)
            {
                var schedule = schedules.FirstOrDefault(s =>
                    s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
                if (schedule != null && schedule.FrequencyMinutes != freq)
                {
                    matches = false;
                    break;
                }
            }
            if (matches) return presetName;
        }
        return "Custom";
    }

    private static void ApplyPresetToList(List<CollectorSchedule> schedules, string presetName)
    {
        if (!s_presets.TryGetValue(presetName, out var intervals)) return;

        foreach (var (collector, freq) in intervals)
        {
            var schedule = schedules.FirstOrDefault(s =>
                s.Name.Equals(collector, StringComparison.OrdinalIgnoreCase));
            if (schedule != null)
            {
                schedule.FrequencyMinutes = freq;
            }
        }
    }

    private static List<CollectorSchedule> CloneScheduleList(IReadOnlyList<CollectorSchedule> source)
    {
        return source.Select(s => new CollectorSchedule
        {
            Name = s.Name,
            Enabled = s.Enabled,
            FrequencyMinutes = s.FrequencyMinutes,
            RetentionDays = s.RetentionDays,
            Description = s.Description
        }).ToList();
    }
}
