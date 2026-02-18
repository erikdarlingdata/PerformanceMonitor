/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using System.Windows.Data;
using Microsoft.Win32;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class AlertsHistoryTab : UserControl
{
    private LocalDataService? _dataService;
    private DataGridFilterManager<AlertHistoryRow>? _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;

    public AlertsHistoryTab()
    {
        InitializeComponent();
    }

    /// <summary>
    /// Initializes the control with required dependencies.
    /// </summary>
    public void Initialize(LocalDataService dataService)
    {
        _dataService = dataService;
        _filterManager = new DataGridFilterManager<AlertHistoryRow>(AlertsDataGrid);
    }

    /// <summary>
    /// Refreshes the alert history data.
    /// </summary>
    public async void RefreshAlerts()
    {
        await LoadAlertsAsync();
    }

    private async System.Threading.Tasks.Task LoadAlertsAsync()
    {
        if (_dataService == null) return;

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();

            var alerts = await _dataService.GetAlertHistoryAsync(hoursBack, 500, serverId);

            if (_filterManager != null)
                _filterManager.UpdateData(alerts);
            else
                AlertsDataGrid.ItemsSource = alerts;

            var displayCount = AlertsDataGrid.ItemsSource is ICollection<AlertHistoryRow> coll ? coll.Count : alerts.Count;
            NoAlertsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;
            AlertCountIndicator.Text = displayCount > 0 ? $"{displayCount} alert(s)" : "";

            PopulateServerFilter(alerts);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AlertsHistory", $"Failed to load alert history: {ex.Message}");
        }
    }

    private void PopulateServerFilter(List<AlertHistoryRow> alerts)
    {
        var servers = alerts
            .Select(a => (a.ServerId, a.ServerName))
            .Where(s => !string.IsNullOrEmpty(s.ServerName))
            .Distinct()
            .OrderBy(s => s.ServerName)
            .ToList();

        var currentSelection = ServerFilterComboBox.SelectedIndex > 0
            ? (ServerFilterComboBox.SelectedItem as ComboBoxItem)?.Tag?.ToString()
            : null;

        var existingIds = ServerFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => i.Tag?.ToString())
            .ToList();

        var newIds = servers.Select(s => s.ServerId.ToString()).ToList();
        if (newIds.SequenceEqual(existingIds)) return;

        ServerFilterComboBox.SelectionChanged -= ServerFilterComboBox_SelectionChanged;

        while (ServerFilterComboBox.Items.Count > 1)
            ServerFilterComboBox.Items.RemoveAt(1);

        foreach (var (serverId, serverName) in servers)
        {
            ServerFilterComboBox.Items.Add(new ComboBoxItem
            {
                Content = serverName,
                Tag = serverId.ToString()
            });
        }

        if (currentSelection != null)
        {
            for (int i = 1; i < ServerFilterComboBox.Items.Count; i++)
            {
                if ((ServerFilterComboBox.Items[i] as ComboBoxItem)?.Tag?.ToString() == currentSelection)
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
            return int.TryParse(tagStr, out var hours) ? hours : 24;
        return 24;
    }

    private int? GetSelectedServerId()
    {
        if (ServerFilterComboBox.SelectedIndex > 0 &&
            ServerFilterComboBox.SelectedItem is ComboBoxItem item &&
            item.Tag is string tagStr &&
            int.TryParse(tagStr, out var serverId))
        {
            return serverId;
        }
        return null;
    }

    #region Column Filter Handlers

    private void FilterButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;

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

        ColumnFilterState? existingFilter = null;
        _filterManager?.Filters.TryGetValue(columnName, out existingFilter);
        _filterPopupContent!.Initialize(columnName, existingFilter);

        _filterPopup.PlacementTarget = button;
        _filterPopup.IsOpen = true;
    }

    private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;

        _filterManager?.SetFilter(e.FilterState);
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;
    }

    #endregion

    #region Event Handlers

    private async void TimeRangeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
            await LoadAlertsAsync();
    }

    private async void ServerFilterComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
            await LoadAlertsAsync();
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        await LoadAlertsAsync();
    }

    private void AlertsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        DismissSelectedButton.IsEnabled = AlertsDataGrid.SelectedItems.Count > 0;
    }

    private async void DismissSelected_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null) return;

        var selected = AlertsDataGrid.SelectedItems
            .OfType<AlertHistoryRow>()
            .ToList();

        if (selected.Count == 0) return;

        try
        {
            await _dataService.DismissAlertsAsync(selected);
            await LoadAlertsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("AlertsHistory", $"Failed to dismiss selected alerts: {ex.Message}");
        }
    }

    private async void DismissAll_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null) return;

        var displayCount = AlertsDataGrid.ItemsSource is System.Collections.ICollection coll ? coll.Count : 0;
        if (displayCount == 0) return;

        var result = MessageBox.Show(
            $"Dismiss all {displayCount} visible alert(s)?\n\nDismissed alerts are hidden from this view but remain in the database.",
            "Dismiss All Alerts",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes) return;

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();
            await _dataService.DismissAllVisibleAlertsAsync(hoursBack, serverId);
            await LoadAlertsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("AlertsHistory", $"Failed to dismiss all alerts: {ex.Message}");
        }
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentCell.Column == null || grid.CurrentItem == null) return;

        var value = GetCellValue(grid.CurrentCell.Column, grid.CurrentItem);
        if (value.Length > 0) Clipboard.SetDataObject(value, false);
    }

    private void CopyRow_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        var sb = new StringBuilder();
        foreach (var col in grid.Columns)
        {
            sb.Append(GetCellValue(col, grid.CurrentItem));
            sb.Append('\t');
        }
        Clipboard.SetDataObject(sb.ToString().TrimEnd('\t'), false);
    }

    private void CopyAllRows_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null) return;

        var sb = new StringBuilder();

        foreach (var col in grid.Columns)
        {
            sb.Append(col.Header?.ToString() ?? "");
            sb.Append('\t');
        }
        sb.AppendLine();

        foreach (var item in grid.Items)
        {
            foreach (var col in grid.Columns)
            {
                sb.Append(GetCellValue(col, item));
                sb.Append('\t');
            }
            sb.AppendLine();
        }

        Clipboard.SetDataObject(sb.ToString(), false);
    }

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null || grid.Items.Count == 0) return;

        var dialog = new SaveFileDialog
        {
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            DefaultExt = ".csv",
            FileName = $"alert_history_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (dialog.ShowDialog() != true) return;

        var sb = new StringBuilder();

        var headers = new List<string>();
        foreach (var col in grid.Columns)
            headers.Add(CsvEscape(col.Header?.ToString() ?? ""));
        sb.AppendLine(string.Join(",", headers));

        foreach (var item in grid.Items)
        {
            var values = new List<string>();
            foreach (var col in grid.Columns)
                values.Add(CsvEscape(GetCellValue(col, item)));
            sb.AppendLine(string.Join(",", values));
        }

        try
        {
            File.WriteAllText(dialog.FileName, sb.ToString(), Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to export: {ex.Message}", "Export Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    #endregion

    #region Helpers

    private static DataGrid? FindParentDataGrid(MenuItem menuItem)
    {
        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        while (target != null && target is not DataGrid)
            target = System.Windows.Media.VisualTreeHelper.GetParent(target) as FrameworkElement;
        return target as DataGrid;
    }

    private static string GetCellValue(DataGridColumn col, object item)
    {
        if (col is DataGridBoundColumn boundCol && boundCol.Binding is Binding binding)
        {
            var prop = item.GetType().GetProperty(binding.Path.Path);
            return prop?.GetValue(item)?.ToString() ?? "";
        }
        return "";
    }

    private static string CsvEscape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    #endregion
}
