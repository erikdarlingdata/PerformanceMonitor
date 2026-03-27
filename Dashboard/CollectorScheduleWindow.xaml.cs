/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class CollectorScheduleWindow : Window
    {
        private readonly DatabaseService _databaseService;
        private List<CollectorScheduleItem>? _schedules;
        private bool _suppressPresetChange;

        private static readonly Dictionary<string, Dictionary<string, int>> Presets = new(StringComparer.OrdinalIgnoreCase)
        {
            ["Aggressive"] = new(StringComparer.OrdinalIgnoreCase)
            {
                ["wait_stats_collector"] = 1,
                ["query_stats_collector"] = 1,
                ["memory_stats_collector"] = 1,
                ["memory_pressure_events_collector"] = 1,
                ["system_health_collector"] = 2,
                ["blocked_process_xml_collector"] = 1,
                ["deadlock_xml_collector"] = 1,
                ["process_blocked_process_xml"] = 2,
                ["blocking_deadlock_analyzer"] = 2,
                ["process_deadlock_xml"] = 2,
                ["query_store_collector"] = 2,
                ["procedure_stats_collector"] = 1,
                ["query_snapshots_collector"] = 1,
                ["file_io_stats_collector"] = 1,
                ["memory_grant_stats_collector"] = 1,
                ["cpu_scheduler_stats_collector"] = 1,
                ["memory_clerks_stats_collector"] = 2,
                ["perfmon_stats_collector"] = 1,
                ["cpu_utilization_stats_collector"] = 1,
                ["trace_analysis_collector"] = 1,
                ["default_trace_collector"] = 2,
                ["configuration_issues_analyzer"] = 1,
                ["latch_stats_collector"] = 1,
                ["spinlock_stats_collector"] = 1,
                ["tempdb_stats_collector"] = 1,
                ["plan_cache_stats_collector"] = 2,
                ["session_stats_collector"] = 1,
                ["waiting_tasks_collector"] = 1,
                ["running_jobs_collector"] = 2
            },
            ["Balanced"] = new(StringComparer.OrdinalIgnoreCase)
            {
                ["wait_stats_collector"] = 1,
                ["query_stats_collector"] = 2,
                ["memory_stats_collector"] = 1,
                ["memory_pressure_events_collector"] = 1,
                ["system_health_collector"] = 5,
                ["blocked_process_xml_collector"] = 1,
                ["deadlock_xml_collector"] = 1,
                ["process_blocked_process_xml"] = 5,
                ["blocking_deadlock_analyzer"] = 5,
                ["process_deadlock_xml"] = 5,
                ["query_store_collector"] = 2,
                ["procedure_stats_collector"] = 2,
                ["query_snapshots_collector"] = 1,
                ["file_io_stats_collector"] = 1,
                ["memory_grant_stats_collector"] = 1,
                ["cpu_scheduler_stats_collector"] = 1,
                ["memory_clerks_stats_collector"] = 5,
                ["perfmon_stats_collector"] = 5,
                ["cpu_utilization_stats_collector"] = 1,
                ["trace_analysis_collector"] = 2,
                ["default_trace_collector"] = 5,
                ["configuration_issues_analyzer"] = 1,
                ["latch_stats_collector"] = 1,
                ["spinlock_stats_collector"] = 1,
                ["tempdb_stats_collector"] = 1,
                ["plan_cache_stats_collector"] = 5,
                ["session_stats_collector"] = 1,
                ["waiting_tasks_collector"] = 1,
                ["running_jobs_collector"] = 1
            },
            ["Low-Impact"] = new(StringComparer.OrdinalIgnoreCase)
            {
                ["wait_stats_collector"] = 5,
                ["query_stats_collector"] = 10,
                ["memory_stats_collector"] = 10,
                ["memory_pressure_events_collector"] = 5,
                ["system_health_collector"] = 15,
                ["blocked_process_xml_collector"] = 5,
                ["deadlock_xml_collector"] = 5,
                ["process_blocked_process_xml"] = 10,
                ["blocking_deadlock_analyzer"] = 10,
                ["process_deadlock_xml"] = 10,
                ["query_store_collector"] = 30,
                ["procedure_stats_collector"] = 10,
                ["query_snapshots_collector"] = 5,
                ["file_io_stats_collector"] = 10,
                ["memory_grant_stats_collector"] = 5,
                ["cpu_scheduler_stats_collector"] = 5,
                ["memory_clerks_stats_collector"] = 30,
                ["perfmon_stats_collector"] = 5,
                ["cpu_utilization_stats_collector"] = 5,
                ["trace_analysis_collector"] = 10,
                ["default_trace_collector"] = 15,
                ["configuration_issues_analyzer"] = 5,
                ["latch_stats_collector"] = 5,
                ["spinlock_stats_collector"] = 5,
                ["tempdb_stats_collector"] = 5,
                ["plan_cache_stats_collector"] = 15,
                ["session_stats_collector"] = 5,
                ["waiting_tasks_collector"] = 5,
                ["running_jobs_collector"] = 30
            }
        };

        public CollectorScheduleWindow(DatabaseService databaseService, string? serverDisplayName = null)
        {
            InitializeComponent();
            _databaseService = databaseService;
            Loaded += CollectorScheduleWindow_Loaded;
            Closing += CollectorScheduleWindow_Closing;

            if (!string.IsNullOrEmpty(serverDisplayName))
            {
                Title = $"Collector Schedules - {serverDisplayName}";
            }
        }

        private void CollectorScheduleWindow_Closing(object? sender, CancelEventArgs e)
        {
            /* Unsubscribe from property change events to prevent memory leaks */
            if (_schedules != null)
            {
                foreach (var schedule in _schedules)
                {
                    schedule.PropertyChanged -= Schedule_PropertyChanged;
                }
            }
        }

        private async void CollectorScheduleWindow_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadSchedulesAsync();
        }

        private async System.Threading.Tasks.Task LoadSchedulesAsync()
        {
            try
            {
                _schedules = await _databaseService.GetCollectorSchedulesAsync();

                // Subscribe to property changes for auto-save
                foreach (var schedule in _schedules)
                {
                    schedule.PropertyChanged += Schedule_PropertyChanged;
                }

                ScheduleDataGrid.ItemsSource = _schedules;
                DetectActivePreset();
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load collector schedules:\n\n{ex.Message}",
                    "Error Loading Schedules",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private void DetectActivePreset()
        {
            if (_schedules == null) return;

            _suppressPresetChange = true;
            try
            {
                var currentIntervals = _schedules
                    .Where(s => s.FrequencyMinutes < 1440)
                    .ToDictionary(s => s.CollectorName, s => s.FrequencyMinutes, StringComparer.OrdinalIgnoreCase);

                foreach (var (presetName, presetIntervals) in Presets)
                {
                    bool matches = true;
                    foreach (var (collector, freq) in presetIntervals)
                    {
                        if (currentIntervals.TryGetValue(collector, out int current) && current != freq)
                        {
                            matches = false;
                            break;
                        }
                    }

                    if (matches)
                    {
                        for (int i = 0; i < PresetComboBox.Items.Count; i++)
                        {
                            if (PresetComboBox.Items[i] is ComboBoxItem item &&
                                string.Equals(item.Content?.ToString(), presetName, StringComparison.OrdinalIgnoreCase))
                            {
                                PresetComboBox.SelectedIndex = i;
                                return;
                            }
                        }
                    }
                }

                /* No preset matched */
                PresetComboBox.SelectedIndex = 0;
            }
            finally
            {
                _suppressPresetChange = false;
            }
        }

        private async void PresetComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_suppressPresetChange) return;
            if (PresetComboBox.SelectedItem is not ComboBoxItem selected) return;

            string presetName = selected.Content?.ToString() ?? "";
            if (presetName == "Custom") return;

            var result = MessageBox.Show(
                $"Apply the \"{presetName}\" preset?\n\nThis will change all collector frequencies. Enabled/disabled state and retention settings are not affected.",
                "Apply Collection Preset",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question
            );

            if (result != MessageBoxResult.Yes)
            {
                DetectActivePreset();
                return;
            }

            try
            {
                await _databaseService.ApplyCollectionPresetAsync(presetName);

                /* Unsubscribe, reload, resubscribe */
                if (_schedules != null)
                {
                    foreach (var schedule in _schedules)
                    {
                        schedule.PropertyChanged -= Schedule_PropertyChanged;
                    }
                }

                await LoadSchedulesAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to apply preset:\n\n{ex.Message}",
                    "Error Applying Preset",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );

                DetectActivePreset();
            }
        }

        private async void Schedule_PropertyChanged(object? sender, PropertyChangedEventArgs e)
        {
            if (sender is CollectorScheduleItem schedule)
            {
                // Only save for the editable properties
                if (e.PropertyName == nameof(CollectorScheduleItem.Enabled) ||
                    e.PropertyName == nameof(CollectorScheduleItem.FrequencyMinutes) ||
                    e.PropertyName == nameof(CollectorScheduleItem.RetentionDays))
                {
                    try
                    {
                        await _databaseService.UpdateCollectorScheduleAsync(
                            schedule.ScheduleId,
                            schedule.Enabled,
                            schedule.FrequencyMinutes,
                            schedule.RetentionDays
                        );

                        if (e.PropertyName == nameof(CollectorScheduleItem.FrequencyMinutes))
                        {
                            DetectActivePreset();
                        }
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(
                            $"Failed to save changes:\n\n{ex.Message}",
                            "Error Saving Schedule",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                }
            }
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            // Unsubscribe from old items
            if (_schedules != null)
            {
                foreach (var schedule in _schedules)
                {
                    schedule.PropertyChanged -= Schedule_PropertyChanged;
                }
            }

            await LoadSchedulesAsync();
        }

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is System.Windows.Controls.MenuItem menuItem && menuItem.Parent is System.Windows.Controls.ContextMenu contextMenu)
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
            if (sender is System.Windows.Controls.MenuItem menuItem && menuItem.Parent is System.Windows.Controls.ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid?.SelectedItem != null)
                    Clipboard.SetDataObject(Helpers.TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is System.Windows.Controls.MenuItem menuItem && menuItem.Parent is System.Windows.Controls.ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new System.Text.StringBuilder();
                    var headers = new List<string>();
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
            if (sender is System.Windows.Controls.MenuItem menuItem && menuItem.Parent is System.Windows.Controls.ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var dialog = new Microsoft.Win32.SaveFileDialog
                    {
                        FileName = $"collector_schedules_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };
                    if (dialog.ShowDialog() == true)
                    {
                        var sb = new System.Text.StringBuilder();
                        var headers = new List<string>();
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
            Close();
        }
    }
}
