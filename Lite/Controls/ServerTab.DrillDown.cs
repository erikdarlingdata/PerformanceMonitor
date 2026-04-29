/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private void AddWaitDrillDownMenuItem(ScottPlot.WPF.WpfPlot chart, ContextMenu contextMenu)
    {
        contextMenu.Items.Insert(0, new Separator());
        var drillDownItem = new MenuItem { Header = "Show Queries With This Wait" };
        drillDownItem.Click += ShowQueriesForWaitType_Click;
        contextMenu.Items.Insert(0, drillDownItem);

        contextMenu.Opened += (s, _) =>
        {
            if (s is not ContextMenu cm) return;
            var pos = System.Windows.Input.Mouse.GetPosition(chart);
            var nearest = _waitStatsHover?.GetNearestSeries(pos);
            if (nearest.HasValue)
            {
                drillDownItem.Tag = (nearest.Value.Label, nearest.Value.Time);
                drillDownItem.Header = $"Show Queries With {nearest.Value.Label.Replace("_", "__")}";
                drillDownItem.IsEnabled = true;
            }
            else
            {
                drillDownItem.Tag = null;
                drillDownItem.Header = "Show Queries With This Wait";
                drillDownItem.IsEnabled = false;
            }
        };
    }

    private void ShowQueriesForWaitType_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        if (menuItem.Tag is not (string waitType, DateTime time)) return;

        // ±15 minute window around the clicked point (already in server local time from chart)
        var fromDate = time.AddMinutes(-30);
        var toDate = time.AddMinutes(30);

        var window = new Windows.WaitDrillDownWindow(
            _dataService, _serverId, waitType, 1, fromDate, toDate);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    // ── Generic Chart Drill-Down (#682) ──

    private void AddChartDrillDownMenuItem(
        ScottPlot.WPF.WpfPlot chart, ContextMenu contextMenu,
        Helpers.ChartHoverHelper? hover, string label, Action<DateTime> handler)
    {
        contextMenu.Items.Insert(0, new Separator());
        var item = new MenuItem { Header = label };
        contextMenu.Items.Insert(0, item);

        contextMenu.Opened += (s, _) =>
        {
            var pos = System.Windows.Input.Mouse.GetPosition(chart);
            var nearest = hover?.GetNearestSeries(pos);
            if (nearest.HasValue)
            {
                item.Tag = nearest.Value.Time;
                item.IsEnabled = true;
            }
            else
            {
                item.Tag = null;
                item.IsEnabled = false;
            }
        };

        item.Click += (s, _) =>
        {
            if (item.Tag is DateTime time)
                handler(time);
        };
    }

    private async void OnCpuDrillDown(DateTime time)
    {
        var fromDate = time.AddMinutes(-30);
        var toDate = time.AddMinutes(30);

        // Populate custom date pickers so user can explore other tabs
        SetDrillDownTimeRange(fromDate, toDate);

        // Navigate to Queries > Active Queries with ±15 min window
        MainTabControl.SelectedIndex = 2; // Queries
        QueriesSubTabControl.SelectedIndex = 1; // Active Queries
        var snapshots = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate);
        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = $"Drill-down: {ServerTimeHelper.FormatServerTime(fromDate.AddMinutes(-UtcOffsetMinutes), "HH:mm")} → {ServerTimeHelper.FormatServerTime(toDate.AddMinutes(-UtcOffsetMinutes), "HH:mm")}";
        _ = LoadActiveQueriesSlicerAsync();
    }

    private async void OnMemoryDrillDown(DateTime time)
    {
        var fromDate = time.AddMinutes(-30);
        var toDate = time.AddMinutes(30);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 2; // Queries
        QueriesSubTabControl.SelectedIndex = 1; // Active Queries
        var snapshots = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate);
        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = $"Drill-down: {ServerTimeHelper.FormatServerTime(fromDate.AddMinutes(-UtcOffsetMinutes), "HH:mm")} → {ServerTimeHelper.FormatServerTime(toDate.AddMinutes(-UtcOffsetMinutes), "HH:mm")}";
        _ = LoadActiveQueriesSlicerAsync();
    }

    private async void OnTempDbDrillDown(DateTime time)
    {
        var fromDate = time.AddMinutes(-30);
        var toDate = time.AddMinutes(30);
        SetDrillDownTimeRange(fromDate, toDate);

        // Navigate to Active Queries — TempDB spills are visible there
        MainTabControl.SelectedIndex = 2; // Queries
        QueriesSubTabControl.SelectedIndex = 1; // Active Queries
        var snapshots = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate);
        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = $"Drill-down: {ServerTimeHelper.FormatServerTime(fromDate.AddMinutes(-UtcOffsetMinutes), "HH:mm")} → {ServerTimeHelper.FormatServerTime(toDate.AddMinutes(-UtcOffsetMinutes), "HH:mm")}";
        _ = LoadActiveQueriesSlicerAsync();
    }

    private async void OnBlockingDrillDown(DateTime time)
    {
        var fromDate = time.AddMinutes(-30);
        var toDate = time.AddMinutes(30);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 8; // Blocking
        BlockingSubTabControl.SelectedIndex = 2; // Blocked Process Reports
        var bpr = await _dataService.GetRecentBlockedProcessReportsAsync(_serverId, 0, fromDate, toDate);
        _blockedProcessFilterMgr!.UpdateData(bpr);
    }

    private async void OnDeadlockDrillDown(DateTime time)
    {
        var fromDate = time.AddMinutes(-30);
        var toDate = time.AddMinutes(30);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 8; // Blocking
        BlockingSubTabControl.SelectedIndex = 3; // Deadlocks
        var dlr = await _dataService.GetRecentDeadlocksAsync(_serverId, 0, fromDate, toDate);
        _deadlockFilterMgr!.UpdateData(DeadlockProcessDetail.ParseFromRows(dlr));
    }

    private async void OnHeatmapDrillDown(DateTime bucketTimeUtc)
    {
        var serverTime = bucketTimeUtc.AddMinutes(UtcOffsetMinutes);
        var fromDate = serverTime.AddMinutes(-5);
        var toDate = serverTime.AddMinutes(10);

        AppLogger.Info("DrillDown", $"OnHeatmapDrillDown: bucketTimeUtc={bucketTimeUtc:O}, UtcOffsetMinutes={UtcOffsetMinutes}, serverTime={serverTime:O}, fromDate={fromDate:O}, toDate={toDate:O}");

        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 2; // Queries
        QueriesSubTabControl.SelectedIndex = 1; // Active Queries

        AppLogger.Info("DrillDown", $"Calling GetLatestQuerySnapshotsAsync with fromDate={fromDate:O}, toDate={toDate:O}");
        var snapshots = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate);
        AppLogger.Info("DrillDown", $"Got {snapshots.Count} snapshots");

        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = $"Drill-down: {fromDate:HH:mm} → {toDate:HH:mm} (server time)";
        _ = LoadActiveQueriesSlicerAsync();
    }

    /// <summary>
    /// Sets the time range combo to Custom and populates the date/time pickers
    /// so the user can navigate other tabs at the same time window.
    /// </summary>
    private void SetDrillDownTimeRange(DateTime fromServer, DateTime toServer)
    {
        // Pickers store time in the current display mode. Downstream reads use
        // DisplayTimeToServerTime() to convert back.
        var fromDisplay = ServerTimeHelper.ConvertForDisplay(fromServer, ServerTimeHelper.CurrentDisplayMode);
        var toDisplay = ServerTimeHelper.ConvertForDisplay(toServer, ServerTimeHelper.CurrentDisplayMode);

        // Switch to Custom without triggering a refresh
        _isRefreshing = true;
        try
        {
            TimeRangeCombo.SelectedIndex = 5; // Custom
            FromDatePicker.SelectedDate = fromDisplay.Date;
            FromHourCombo.SelectedIndex = fromDisplay.Hour;
            FromMinuteCombo.SelectedIndex = fromDisplay.Minute / 15;
            ToDatePicker.SelectedDate = toDisplay.Date;
            ToHourCombo.SelectedIndex = toDisplay.Hour;
            ToMinuteCombo.SelectedIndex = toDisplay.Minute / 15;

            // Make pickers visible
            var visibility = Visibility.Visible;
            FromDatePicker.Visibility = visibility;
            FromHourCombo.Visibility = visibility;
            FromMinuteCombo.Visibility = visibility;
            ToLabel.Visibility = visibility;
            ToDatePicker.Visibility = visibility;
            ToHourCombo.Visibility = visibility;
            ToMinuteCombo.Visibility = visibility;
        }
        finally
        {
            _isRefreshing = false;
        }
    }
}
