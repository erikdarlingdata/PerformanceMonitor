/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class AlertsHistoryContent : UserControl
    {
        public event EventHandler? AlertsDismissed;

        private List<AlertHistoryDisplayItem> _allAlerts = new();

        /* Column filter state */
        private readonly Dictionary<string, ColumnFilterState> _columnFilters = new();
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        public AlertsHistoryContent()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Refreshes the alert history from the in-memory log.
        /// </summary>
        public void RefreshAlerts()
        {
            LoadAlerts();
        }

        private void LoadAlerts()
        {
            var service = EmailAlertService.Current;
            if (service == null)
            {
                AlertsDataGrid.ItemsSource = null;
                NoAlertsMessage.Visibility = Visibility.Visible;
                AlertCountIndicator.Text = "";
                return;
            }

            var hoursBack = GetSelectedHoursBack();
            var entries = service.GetAlertHistory(hoursBack > 0 ? hoursBack : 8760, 500);

            _allAlerts = entries.Select(e => new AlertHistoryDisplayItem
            {
                AlertTime = e.AlertTime,
                ServerName = e.ServerName,
                MetricName = e.MetricName,
                CurrentValue = e.CurrentValue,
                ThresholdValue = e.ThresholdValue,
                NotificationType = e.NotificationType,
                StatusDisplay = GetStatusDisplay(e),
                IsResolved = e.MetricName.Contains("Cleared") || e.MetricName.Contains("Resolved"),
                IsCritical = e.MetricName.Contains("Deadlock") || e.MetricName.Contains("Poison"),
                IsWarning = !e.MetricName.Contains("Cleared") && !e.MetricName.Contains("Resolved")
                            && !e.MetricName.Contains("Deadlock") && !e.MetricName.Contains("Poison")
            }).ToList();

            ApplyFilters();
        }

        private void ApplyFilters()
        {
            var filtered = _allAlerts.AsEnumerable();

            /* Server filter */
            if (ServerFilterComboBox.SelectedIndex > 0 &&
                ServerFilterComboBox.SelectedItem is ComboBoxItem serverItem)
            {
                var serverName = serverItem.Content?.ToString();
                if (!string.IsNullOrEmpty(serverName))
                    filtered = filtered.Where(a => a.ServerName == serverName);
            }

            /* Column filters */
            if (_columnFilters.Count > 0)
            {
                filtered = filtered.Where(item =>
                {
                    foreach (var filter in _columnFilters.Values)
                    {
                        if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                            return false;
                    }
                    return true;
                });
            }

            var list = filtered.ToList();
            AlertsDataGrid.ItemsSource = list;
            NoAlertsMessage.Visibility = list.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            AlertCountIndicator.Text = list.Count > 0 ? $"{list.Count} alert(s)" : "";

            /* Populate server filter if needed */
            PopulateServerFilter();
        }

        private void PopulateServerFilter()
        {
            var servers = _allAlerts
                .Select(a => a.ServerName)
                .Where(s => !string.IsNullOrEmpty(s))
                .Distinct()
                .OrderBy(s => s)
                .ToList();

            var currentSelection = ServerFilterComboBox.SelectedIndex > 0
                ? (ServerFilterComboBox.SelectedItem as ComboBoxItem)?.Content?.ToString()
                : null;

            /* Only rebuild if the list changed */
            var existingServers = ServerFilterComboBox.Items
                .OfType<ComboBoxItem>()
                .Skip(1)
                .Select(i => i.Content?.ToString())
                .ToList();

            if (servers.SequenceEqual(existingServers)) return;

            ServerFilterComboBox.SelectionChanged -= ServerFilterComboBox_SelectionChanged;

            while (ServerFilterComboBox.Items.Count > 1)
                ServerFilterComboBox.Items.RemoveAt(1);

            foreach (var server in servers)
            {
                ServerFilterComboBox.Items.Add(new ComboBoxItem { Content = server });
            }

            /* Restore selection */
            if (currentSelection != null)
            {
                for (int i = 1; i < ServerFilterComboBox.Items.Count; i++)
                {
                    if ((ServerFilterComboBox.Items[i] as ComboBoxItem)?.Content?.ToString() == currentSelection)
                    {
                        ServerFilterComboBox.SelectedIndex = i;
                        break;
                    }
                }
            }

            ServerFilterComboBox.SelectionChanged += ServerFilterComboBox_SelectionChanged;
        }

        private int GetSelectedHoursBack()
        {
            if (TimeRangeComboBox.SelectedItem is ComboBoxItem item && item.Tag is string tagStr)
            {
                return int.TryParse(tagStr, out var hours) ? hours : 24;
            }
            return 24;
        }

        private static string GetStatusDisplay(AlertLogEntry entry)
        {
            if (entry.NotificationType == "email")
            {
                if (entry.AlertSent) return "Sent";
                return !string.IsNullOrEmpty(entry.SendError) ? "Failed" : "Not sent";
            }
            return entry.AlertSent ? "Delivered" : "Shown";
        }

        #region Event Handlers

        private void TimeRangeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (IsLoaded)
                LoadAlerts();
        }

        private void ServerFilterComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (IsLoaded)
                ApplyFilters();
        }

        private void RefreshButton_Click(object sender, RoutedEventArgs e)
        {
            LoadAlerts();
        }

        private void AlertsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            DismissSelectedButton.IsEnabled = AlertsDataGrid.SelectedItems.Count > 0;
        }

        private void DismissSelected_Click(object sender, RoutedEventArgs e)
        {
            var service = EmailAlertService.Current;
            if (service == null) return;

            var selected = AlertsDataGrid.SelectedItems
                .OfType<AlertHistoryDisplayItem>()
                .ToList();

            if (selected.Count == 0) return;

            var keys = selected
                .Select(s => (s.AlertTime, s.ServerName, s.MetricName))
                .ToList();

            service.HideAlerts(keys);
            LoadAlerts();
            AlertsDismissed?.Invoke(this, EventArgs.Empty);
        }

        private void DismissAll_Click(object sender, RoutedEventArgs e)
        {
            var service = EmailAlertService.Current;
            if (service == null) return;

            var displayCount = AlertsDataGrid.ItemsSource is ICollection<AlertHistoryDisplayItem> coll ? coll.Count : 0;
            if (displayCount == 0) return;

            var result = MessageBox.Show(
                $"Dismiss all {displayCount} visible alert(s)?\n\nDismissed alerts are hidden from this view but preserved in the log.",
                "Dismiss All Alerts",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);

            if (result != MessageBoxResult.Yes) return;

            var hoursBack = GetSelectedHoursBack();
            string? serverName = null;
            if (ServerFilterComboBox.SelectedIndex > 0 &&
                ServerFilterComboBox.SelectedItem is ComboBoxItem serverItem)
            {
                serverName = serverItem.Content?.ToString();
            }

            service.HideAllAlerts(hoursBack > 0 ? hoursBack : 8760, serverName);
            LoadAlerts();
            AlertsDismissed?.Invoke(this, EventArgs.Empty);
        }

        #endregion

        #region Column Filter Handlers

        private void AlertFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName);
        }

        private void ShowFilterPopup(Button button, string columnName)
        {
            if (_filterPopup == null)
            {
                _filterPopupContent = new ColumnFilterPopup();
                _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

                _filterPopup = new Popup
                {
                    Child = _filterPopupContent,
                    StaysOpen = false,
                    Placement = PlacementMode.Bottom,
                    AllowsTransparency = true
                };
            }

            _columnFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
                _columnFilters[e.FilterState.ColumnName] = e.FilterState;
            else
                _columnFilters.Remove(e.FilterState.ColumnName);

            ApplyFilters();
            UpdateFilterButtonStyles();
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void UpdateFilterButtonStyles()
        {
            foreach (var column in AlertsDataGrid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var filterButton = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton?.Tag is string columnName)
                    {
                        bool hasActive = _columnFilters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        var textBlock = new System.Windows.Controls.TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActive
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00))
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF))
                        };
                        filterButton.Content = textBlock;
                        filterButton.ToolTip = hasActive && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                    }
                }
            }
        }

        #endregion

        #region Context Menu Handlers

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                        Clipboard.SetDataObject(cellContent, false);
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid?.SelectedItem != null)
                    Clipboard.SetDataObject(TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new StringBuilder();
                    var headers = dataGrid.Columns
                        .OfType<DataGridBoundColumn>()
                        .Select(c => Helpers.DataGridClipboardBehavior.GetHeaderText(c))
                        .ToList();
                    sb.AppendLine(string.Join("\t", headers));

                    foreach (var item in dataGrid.Items)
                        sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));

                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"alert_history_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();
                            var sep = TabHelpers.CsvSeparator;
                            var headers = dataGrid.Columns
                                .OfType<DataGridBoundColumn>()
                                .Select(c => TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(c), sep))
                                .ToList();
                            sb.AppendLine(string.Join(sep, headers));

                            foreach (var item in dataGrid.Items)
                            {
                                var values = TabHelpers.GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(sep, values.Select(v => TabHelpers.EscapeCsvField(v, sep))));
                            }

                            File.WriteAllText(saveFileDialog.FileName, sb.ToString());
                            MessageBox.Show($"Data exported successfully to:\n{saveFileDialog.FileName}",
                                "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show($"Error exporting data:\n\n{ex.Message}",
                                "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        #endregion
    }

    public class AlertHistoryDisplayItem
    {
        public DateTime AlertTime { get; set; }
        public string TimeLocal => AlertTime.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
        public string ServerName { get; set; } = "";
        public string MetricName { get; set; } = "";
        public string CurrentValue { get; set; } = "";
        public string ThresholdValue { get; set; } = "";
        public string NotificationType { get; set; } = "";
        public string StatusDisplay { get; set; } = "";
        public bool IsResolved { get; set; }
        public bool IsCritical { get; set; }
        public bool IsWarning { get; set; }
    }
}
