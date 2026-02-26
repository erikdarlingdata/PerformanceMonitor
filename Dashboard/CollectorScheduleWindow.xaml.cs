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
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class CollectorScheduleWindow : Window
    {
        private readonly DatabaseService _databaseService;
        private List<CollectorScheduleItem>? _schedules;

        public CollectorScheduleWindow(DatabaseService databaseService)
        {
            InitializeComponent();
            _databaseService = databaseService;
            Loaded += CollectorScheduleWindow_Loaded;
            Closing += CollectorScheduleWindow_Closing;
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
