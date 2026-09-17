/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The full Alert History surface (W2a viewer copy-parity), copied from Lite's <c>AlertsHistoryTab</c>
/// (Lite/Controls/AlertsHistoryTab.xaml.cs) and rewired to the Darling Postgres store via
/// <see cref="ViewerDataService"/>: all-servers by default with a Server filter + Server column, the
/// Dismiss Selected / Dismiss All write path, column-filter headers, copy/export, count + last-refreshed
/// indicators, and Mute This / Mute Similar plus the Manage Mute Rules window. Darling has no parquet
/// archive tier, so Lite's archived-row handling (the IsArchived split, the "archival in progress"
/// banner, the partial-dismiss confirmation) is dropped — every row is live and dismissable. Load /
/// dismiss failures surface on the host's status bar via <see cref="StatusChanged"/>.
/// </summary>
public partial class AlertsHistoryTab : UserControl
{
    /// <summary>
    /// Whether alert dismiss + mute actions are written to the viewer log (the "Log alert dismiss and mute
    /// actions" Settings checkbox — <see cref="ViewerAppSettings.LogAlertDismissals"/>). Seeded from the app
    /// settings by <see cref="MainWindow"/> at startup and after the Settings window saves; a process-wide static
    /// because the viewer loads its app settings per-window (the twin of Lite's <c>App.LogAlertDismissals</c>).
    /// </summary>
    public static bool LogDismissals { get; set; } = true;

    private ViewerDataService? _dataService;
    private DataGridFilterManager<ViewerAlertRow>? _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private DateTime? _lastRefreshed;
    private readonly DispatcherTimer _staleDataTimer;

    /// <summary>Raised with a short status message on load/dismiss/mute outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public AlertsHistoryTab()
    {
        InitializeComponent();
        _staleDataTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(10) };
        _staleDataTimer.Tick += StaleDataTimer_Tick;
    }

    /// <summary>Wires the data service and the column-filter manager, and starts the stale-data ticker.</summary>
    public void Initialize(ViewerDataService dataService)
    {
        _dataService = dataService;
        _filterManager = new DataGridFilterManager<ViewerAlertRow>(AlertsDataGrid);

        /* V8 security hardening: a read-only (viewer-role) connection can't dismiss alerts, so disable
           the Dismiss buttons up front with an explaining tooltip (the Dismiss Selected button is also
           selection-gated below). Mute-from-alert and context-menu Dismiss stay clickable but surface
           the read-only message via StatusChanged if used — the reactive backstop. */
        if (dataService.IsReadOnly)
        {
            const string readOnlyTip = "This viewer is connected read-only (postgres.connectAs = \"viewer\"); alert dismissal is disabled.";
            DismissAllButton.IsEnabled = false;
            DismissAllButton.ToolTip = readOnlyTip;
            DismissSelectedButton.ToolTip = readOnlyTip;
        }

        _staleDataTimer.Start();
    }

    /// <summary>Reloads the alert history (the shell calls this when the Alerts tab becomes visible / on the timer).</summary>
    public Task RefreshAlertsAsync() => LoadAlertsAsync();

    private async Task LoadAlertsAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();
            var sinceUtc = DateTime.UtcNow.AddHours(-hoursBack);

            var alerts = await _dataService.GetAlertHistoryAsync(sinceUtc, serverId);

            if (_filterManager != null)
            {
                _filterManager.UpdateData(alerts);
            }
            else
            {
                AlertsDataGrid.ItemsSource = alerts;
            }

            var displayCount = AlertsDataGrid.Items.Count;
            NoAlertsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;
            AlertCountIndicator.Text = displayCount > 0 ? $"{displayCount} alert(s)" : "";

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

            PopulateServerFilter(alerts);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"failed to load alert history: {ex.Message}");
        }
    }

    /// <summary>
    /// Rebuilds the Server filter combo from the servers present in the current read (Lite's
    /// PopulateServerFilter): "All Servers" first, then each distinct server, preserving the selection.
    /// </summary>
    private void PopulateServerFilter(List<ViewerAlertRow> alerts)
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
        if (newIds.SequenceEqual(existingIds))
        {
            return;
        }

        ServerFilterComboBox.SelectionChanged -= ServerFilterComboBox_SelectionChanged;

        while (ServerFilterComboBox.Items.Count > 1)
        {
            ServerFilterComboBox.Items.RemoveAt(1);
        }

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
            for (var i = 1; i < ServerFilterComboBox.Items.Count; i++)
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
        {
            return int.TryParse(tagStr, out var hours) ? hours : 24;
        }
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
        if (sender is not Button button || button.Tag is not string columnName)
        {
            return;
        }

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
        {
            _filterPopup.IsOpen = false;
        }

        _filterManager?.SetFilter(e.FilterState);
        AlertCountIndicator.Text = AlertsDataGrid.Items.Count > 0 ? $"{AlertsDataGrid.Items.Count} alert(s)" : "";
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null)
        {
            _filterPopup.IsOpen = false;
        }
    }

    #endregion

    #region Stale Data Indicator

    private void StaleDataTimer_Tick(object? sender, EventArgs e) => UpdateStaleDataIndicator();

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
    }

    #endregion

    #region Event Handlers

    private async void TimeRangeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadAlertsAsync();
        }
    }

    private async void ServerFilterComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadAlertsAsync();
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await LoadAlertsAsync();

    private void AlertsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        /* Read-only connections never enable Dismiss Selected, regardless of selection (V8 hardening). */
        DismissSelectedButton.IsEnabled =
            _dataService?.IsReadOnly != true && AlertsDataGrid.SelectedItems.OfType<ViewerAlertRow>().Any();
    }

    private async void DismissSelected_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null)
        {
            return;
        }

        var selected = AlertsDataGrid.SelectedItems.OfType<ViewerAlertRow>().ToList();
        if (selected.Count == 0)
        {
            return;
        }

        try
        {
            var affected = await _dataService.DismissAlertsAsync(selected);
            /* Log the dismiss action when the operator opted in (mirrors Lite's App.LogAlertDismissals gate,
               including the partial-dismiss note when fewer rows updated than were selected). */
            if (LogDismissals)
            {
                ViewerLogger.Info("AlertsHistory", affected < selected.Count
                    ? $"Dismissed {affected} of {selected.Count} selected alert(s) (some were already dismissed)"
                    : $"Dismissed {affected} selected alert(s)");
            }
            await LoadAlertsAsync();
            StatusChanged?.Invoke($"dismissed {affected} alert(s) — {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"failed to dismiss selected alerts: {ex.Message}");
        }
    }

    private async void DismissAll_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null)
        {
            return;
        }

        var allAlerts = (AlertsDataGrid.ItemsSource as IEnumerable<ViewerAlertRow>)?.ToList();
        if (allAlerts == null || allAlerts.Count == 0)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Dismiss {allAlerts.Count} alert(s)?\n\nDismissed alerts are hidden from this view but remain in the store.",
            "Dismiss All Alerts",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();
            var sinceUtc = DateTime.UtcNow.AddHours(-hoursBack);
            var affected = await _dataService.DismissAllVisibleAlertsAsync(sinceUtc, serverId);
            if (LogDismissals)
            {
                var scope = serverId.HasValue ? $"server {serverId.Value}" : "all servers";
                ViewerLogger.Info("AlertsHistory", $"Dismissed all {affected} visible alert(s) ({scope}, last {hoursBack}h)");
            }
            await LoadAlertsAsync();
            StatusChanged?.Invoke($"dismissed {affected} alert(s) — {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"failed to dismiss all alerts: {ex.Message}");
        }
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, "alert_history", ViewerExportSettings.CsvSeparator);

    #endregion

    #region Detail + Mute Handlers

    private void AlertsDataGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (sender is not DataGrid)
        {
            return;
        }

        /* Walk up from the click target to the DataGridRow; ignore header / empty-area clicks. */
        var source = e.OriginalSource as DependencyObject;
        while (source != null && source is not DataGridRow && source is not DataGridColumnHeader)
        {
            source = System.Windows.Media.VisualTreeHelper.GetParent(source);
        }

        if (source is not DataGridRow row || row.DataContext is not ViewerAlertRow item)
        {
            return;
        }

        ShowDetail(item);
    }

    private void ViewAlertDetails_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem)
        {
            return;
        }
        var dataGrid = FindParentDataGrid(menuItem);
        if (dataGrid?.SelectedItem is not ViewerAlertRow item)
        {
            return;
        }

        ShowDetail(item);
    }

    private void ShowDetail(ViewerAlertRow item)
    {
        var detailWindow = new AlertDetailWindow(item);
        var owner = Window.GetWindow(this);
        if (owner != null)
        {
            detailWindow.Owner = owner;
        }
        detailWindow.ShowDialog();
    }

    private async void MuteThisAlert_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null || sender is not MenuItem menuItem)
        {
            return;
        }
        var dataGrid = FindParentDataGrid(menuItem);
        if (dataGrid?.SelectedItem is not ViewerAlertRow item)
        {
            return;
        }

        var context = new AlertMuteContext
        {
            ServerName = item.ServerName,
            MetricName = item.MetricName
        };
        /* #3309: pass the metric name so a custom alert ("Custom:<id>") skips detail_text pre-fill parsing -
           a custom rule has no Database/Wait Type/Job/Query dimension to pre-fill, and its user-authored name
           must not be able to forge a mute-context label line. */
        context.PopulateFromDetailText(item.DetailText, item.MetricName);

        await CreateMuteRuleAsync(context);
    }

    private async void MuteSimilarAlerts_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null || sender is not MenuItem menuItem)
        {
            return;
        }
        var dataGrid = FindParentDataGrid(menuItem);
        if (dataGrid?.SelectedItem is not ViewerAlertRow item)
        {
            return;
        }

        /* Metric-only mute: suppress every alert of this type, on any server. */
        var context = new AlertMuteContext
        {
            MetricName = item.MetricName
        };

        await CreateMuteRuleAsync(context);
    }

    private async Task CreateMuteRuleAsync(AlertMuteContext context)
    {
        if (_dataService == null)
        {
            return;
        }

        var dialog = new MuteRuleEditDialog(context) { Owner = Window.GetWindow(this) };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await _dataService.InsertMuteRuleAsync(dialog.Rule);
            /* The "Log alert dismiss and mute actions" opt-in covers mute creation too. */
            if (LogDismissals)
            {
                var expiry = dialog.Rule.ExpiresAtUtc.HasValue
                    ? dialog.Rule.ExpiresAtUtc.Value.ToString("u", System.Globalization.CultureInfo.InvariantCulture)
                    : "never";
                ViewerLogger.Info("AlertsHistory",
                    $"Created mute rule (server={dialog.Rule.ServerName ?? "any"}, metric={dialog.Rule.MetricName ?? "any"}, expires={expiry})");
            }
            await LoadAlertsAsync();
            StatusChanged?.Invoke($"mute rule created — {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"mute rule save failed: {ex.Message}");
        }
    }

    private void ManageMuteRules_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null)
        {
            return;
        }

        var window = new MuteRulesWindow(_dataService) { Owner = Window.GetWindow(this) };
        window.ShowDialog();
    }

    #endregion
}
