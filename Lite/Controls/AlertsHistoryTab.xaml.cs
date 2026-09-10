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
using System.Windows.Threading;
using Microsoft.Win32;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

public partial class AlertsHistoryTab : UserControl
{
    private LocalDataService? _dataService;
    private DataGridFilterManager<AlertHistoryRow>? _filterManager;
    private readonly ScopedLoadGenerations _loads = new();
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private DateTime? _lastRefreshed;
    private readonly DispatcherTimer _staleDataTimer;

    public MuteRuleService? MuteRuleService { get; set; }

    /// <summary>
    /// Raised after "Dismiss All" clears the visible alerts, so the host can acknowledge the
    /// matching server tab badge(s). The argument is the server_id filter in effect at the time
    /// (null = all servers were shown). See issue #1092.
    /// </summary>
    public event Action<int?>? AlertsDismissed;

    public AlertsHistoryTab()
    {
        InitializeComponent();
        _staleDataTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(10) };
        _staleDataTimer.Tick += StaleDataTimer_Tick;
    }

    /// <summary>
    /// Initializes the control with required dependencies.
    /// </summary>
    public void Initialize(LocalDataService dataService)
    {
        _dataService = dataService;
        _filterManager = new DataGridFilterManager<AlertHistoryRow>(AlertsDataGrid);
        _staleDataTimer.Start();
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

        /* #2933: this tab has no load guard either, and its server/hours combos sit above the grid they
           scope — two changes overlap and the later-starting read can land first, leaving the grid
           answering for a filter the operator moved off. Same idiom as FinOpsTab's. */
        var gen = _loads.Claim(nameof(LoadAlertsAsync));

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();

            var alerts = await System.Threading.Tasks.Task.Run(() => _dataService.GetAlertHistoryAsync(hoursBack, 500, serverId));
            if (_loads.Superseded(nameof(LoadAlertsAsync), gen)) return;

            if (_filterManager != null)
                _filterManager.UpdateData(alerts);
            else
                AlertsDataGrid.ItemsSource = alerts;

            var displayCount = AlertsDataGrid.ItemsSource is ICollection<AlertHistoryRow> coll ? coll.Count : alerts.Count;
            NoAlertsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;
            AlertCountIndicator.Text = displayCount > 0 ? $"{displayCount} alert(s)" : "";
            AppLogger.Debug("AlertsHistory", $"Loaded {displayCount} alert(s) (query returned {alerts.Count}, hoursBack={hoursBack}, serverId={serverId?.ToString() ?? "all"})");

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

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

    #region Stale Data Indicator

    private void StaleDataTimer_Tick(object? sender, EventArgs e)
    {
        UpdateStaleDataIndicator();
    }

    private void UpdateStaleDataIndicator()
    {
        if (_lastRefreshed.HasValue)
        {
            var elapsed = DateTime.UtcNow - _lastRefreshed.Value;
            LastRefreshedIndicator.Text = elapsed.TotalSeconds < 5
                ? "Refreshed just now"
                : elapsed.TotalMinutes < 1
                    ? $"Refreshed {(int)elapsed.TotalSeconds}s ago"
                    : $"Refreshed {(int)elapsed.TotalMinutes}m ago";
        }

        // Show archival warning if ArchiveService is currently running
        if (ArchiveService.IsArchiving)
        {
            ArchivalWarning.Text = "⚠ Archival in progress";
            ArchivalWarning.Visibility = Visibility.Visible;
        }
        else
        {
            ArchivalWarning.Visibility = Visibility.Collapsed;
        }
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
        var selected = AlertsDataGrid.SelectedItems.OfType<AlertHistoryRow>().ToList();
        var liveCount = selected.Count(a => !a.IsArchived);
        DismissSelectedButton.IsEnabled = liveCount > 0;
    }

    private async void DismissSelected_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null) return;

        var selected = AlertsDataGrid.SelectedItems
            .OfType<AlertHistoryRow>()
            .ToList();

        if (selected.Count == 0) return;

        var liveAlerts = selected.Where(a => !a.IsArchived).ToList();
        var archivedCount = selected.Count - liveAlerts.Count;

        if (liveAlerts.Count == 0)
        {
            MessageBox.Show(
                "The selected alert(s) have been archived and cannot be dismissed.",
                "Cannot Dismiss",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
            return;
        }

        if (archivedCount > 0)
        {
            var confirmResult = MessageBox.Show(
                $"{archivedCount} of {selected.Count} selected alert(s) have been archived and cannot be dismissed.\n\n" +
                $"Dismiss the remaining {liveAlerts.Count} alert(s)?",
                "Partial Dismiss",
                MessageBoxButton.YesNo,
                MessageBoxImage.Question);
            if (confirmResult != MessageBoxResult.Yes) return;
        }

        try
        {
            var affected = await System.Threading.Tasks.Task.Run(() => _dataService.DismissAlertsAsync(liveAlerts));
            if (affected < liveAlerts.Count && App.LogAlertDismissals)
            {
                AppLogger.Warn("AlertsHistory", $"Dismiss selected: only {affected} of {liveAlerts.Count} live alert(s) were updated");
            }
            await LoadAlertsAsync();

            /* Clear the matching server tab badge(s) for the servers whose alerts were dismissed,
               matching Dismiss All's behavior (issue #1092). Selected rows can span servers when
               the filter is "all", so acknowledge each distinct server rather than the filter. */
            foreach (var dismissedServerId in liveAlerts.Select(a => a.ServerId).Distinct())
                AlertsDismissed?.Invoke(dismissedServerId);
        }
        catch (TimeoutException)
        {
            MessageBox.Show(
                "The database is currently busy (archival or maintenance in progress).\n\nPlease try again in a few moments.",
                "Dismiss Unavailable",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AlertsHistory", $"Failed to dismiss selected alerts: {ex.Message}");
        }
    }

    private async void DismissAll_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null) return;

        var allAlerts = (AlertsDataGrid.ItemsSource as IEnumerable<AlertHistoryRow>)?.ToList();
        if (allAlerts == null || allAlerts.Count == 0) return;

        var liveCount = allAlerts.Count(a => !a.IsArchived);
        var archivedCount = allAlerts.Count - liveCount;

        if (liveCount == 0)
        {
            MessageBox.Show(
                "All visible alerts have been archived and cannot be dismissed.",
                "Cannot Dismiss",
                MessageBoxButton.OK,
                MessageBoxImage.Information);
            return;
        }

        var message = $"Dismiss {liveCount} alert(s)?";
        if (archivedCount > 0)
            message += $"\n\n{archivedCount} archived alert(s) will remain visible.";
        message += "\n\nDismissed alerts are hidden from this view but remain in the database.";

        var result = MessageBox.Show(
            message,
            "Dismiss All Alerts",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes) return;

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();
            var affected = await System.Threading.Tasks.Task.Run(() => _dataService.DismissAllVisibleAlertsAsync(hoursBack, serverId));
            if (affected < liveCount && App.LogAlertDismissals)
            {
                AppLogger.Warn("AlertsHistory", $"Dismiss all: only {affected} of {liveCount} live alert(s) were updated");
            }
            await LoadAlertsAsync();

            /* Clear the matching server tab badge(s) so the indicator matches the cleared list (issue #1092). */
            AlertsDismissed?.Invoke(serverId);
        }
        catch (TimeoutException)
        {
            MessageBox.Show(
                "The database is currently busy (archival or maintenance in progress).\n\nPlease try again in a few moments.",
                "Dismiss Unavailable",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            AppLogger.Error("AlertsHistory", $"Failed to dismiss all alerts: {ex.Message}");
        }
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, "alert_history", App.CsvSeparator);

    #endregion

    #region Mute Handlers

    private void AlertsDataGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (sender is not DataGrid) return;

        // Walk up the visual tree from the click target to find the DataGridRow
        var source = e.OriginalSource as DependencyObject;
        while (source != null && source is not DataGridRow && source is not DataGridColumnHeader)
            source = System.Windows.Media.VisualTreeHelper.GetParent(source);

        // Ignore clicks on column headers or outside rows
        if (source is not DataGridRow row) return;
        if (row.DataContext is not AlertHistoryRow item) return;

        var owner = Window.GetWindow(this);
        var detailWindow = new Windows.AlertDetailWindow(item);
        if (owner != null) detailWindow.Owner = owner;
        detailWindow.ShowDialog();
    }

    private void ViewAlertDetails_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var dataGrid = FindParentDataGrid(menuItem);
        if (dataGrid?.SelectedItem is not AlertHistoryRow item) return;

        var owner = Window.GetWindow(this);
        var detailWindow = new Windows.AlertDetailWindow(item);
        if (owner != null) detailWindow.Owner = owner;
        detailWindow.ShowDialog();
    }

    private async void MuteThisAlert_Click(object sender, RoutedEventArgs e)
    {
        if (MuteRuleService == null) return;
        if (sender is not MenuItem menuItem) return;
        var dataGrid = FindParentDataGrid(menuItem);
        if (dataGrid?.SelectedItem is not AlertHistoryRow item) return;

        var context = new AlertMuteContext
        {
            ServerName = item.ServerName,
            MetricName = item.MetricName
        };
        context.PopulateFromDetailText(item.DetailText);

        var dialog = new Windows.MuteRuleDialog(context) { Owner = Window.GetWindow(this) };
        if (dialog.ShowDialog() == true)
        {
            await MuteRuleService.AddRuleAsync(dialog.Rule);
            await LoadAlertsAsync();
        }
    }

    private async void MuteSimilarAlerts_Click(object sender, RoutedEventArgs e)
    {
        if (MuteRuleService == null) return;
        if (sender is not MenuItem menuItem) return;
        var dataGrid = FindParentDataGrid(menuItem);
        if (dataGrid?.SelectedItem is not AlertHistoryRow item) return;

        var context = new AlertMuteContext
        {
            MetricName = item.MetricName
        };

        var dialog = new Windows.MuteRuleDialog(context) { Owner = Window.GetWindow(this) };
        if (dialog.ShowDialog() == true)
        {
            await MuteRuleService.AddRuleAsync(dialog.Rule);
            await LoadAlertsAsync();
        }
    }

    #endregion
}
