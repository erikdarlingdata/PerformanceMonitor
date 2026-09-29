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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// The window a drill opens around a clicked instant: <paramref name="minutesBefore"/> and
    /// <paramref name="minutesAfter"/> real minutes either side of <paramref name="centerUtc"/> (naive UTC), each end
    /// then read on the server's wall clock. Adding the minutes to the wall clock instead puts an end inside the
    /// skipped hour of a spring-forward day, where it converts to the same instant as the other end (#4766).
    /// </summary>
    internal static (DateTime From, DateTime To) GetDrillWindow(
        DateTime centerUtc, int minutesBefore, int minutesAfter, ServerClock serverClock)
        => (serverClock.ToServerLocal(centerUtc.AddMinutes(-minutesBefore)),
            serverClock.ToServerLocal(centerUtc.AddMinutes(minutesAfter)));

    private void AddWaitDrillDownMenuItem(ScottPlot.WPF.WpfPlot chart, ContextMenu contextMenu)
    {
        contextMenu.Items.Insert(0, new Separator());
        var drillDownItem = new MenuItem { Header = "Show _Queries With This Wait" };
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
                drillDownItem.Header = $"Show _Queries With {nearest.Value.Label.Replace("_", "__")}";
                drillDownItem.IsEnabled = true;
            }
            else
            {
                drillDownItem.Tag = null;
                drillDownItem.Header = "Show _Queries With This Wait";
                drillDownItem.IsEnabled = false;
            }
        };
    }

    private void ShowQueriesForWaitType_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        if (menuItem.Tag is not (string waitType, DateTime time)) return;

        // ±30 minute window around the clicked point (already in server local time from chart)
        var (fromDate, toDate) = GetDrillWindow(ToUtcFromServerLocal(time), 30, 30, _serverClock);

        var window = new Windows.WaitDrillDownWindow(
            _dataService, _serverId, waitType, 1, fromDate, toDate,
            _credentialResolver.GetConnectionString(_server));
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    // ── Generic Chart Drill-Down (#682) ──

    private void AddChartDrillDownMenuItem(
        ScottPlot.WPF.WpfPlot chart, ContextMenu contextMenu,
        ChartHoverHelper? hover, string label, Action<DateTime> handler)
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

    /// <summary>
    /// Navigates to Queries → Active Queries for a drill-down without triggering the
    /// MainTabControl_SelectionChanged auto-refresh (the caller loads its own filtered snapshot
    /// next; the auto-refresh would clobber it via an async race).
    /// </summary>
    private void SelectActiveQueriesForDrillDown()
    {
        _suppressActiveQueriesAutoRefresh = true;
        try
        {
            MainTabControl.SelectedIndex = 2; // Queries
            QueriesSubTabControl.SelectedIndex = 1; // Active Queries
        }
        finally
        {
            _suppressActiveQueriesAutoRefresh = false;
        }
    }

    /// <summary>
    /// Generic "Show Active Queries at This Time" drill-down for resource charts that have no
    /// more specific target (memory clerks/grants/pressure, tempdb size + file I/O, file I/O
    /// latency + throughput, current waits, perfmon).
    /// </summary>
    private async void OnActiveQueriesDrillDown(DateTime time)
    {
        var (fromDate, toDate) = GetDrillWindow(ToUtcFromServerLocal(time), 30, 30, _serverClock);
        SetDrillDownTimeRange(fromDate, toDate);

        SelectActiveQueriesForDrillDown();
        var snapshots = await System.Threading.Tasks.Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate));
        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = $"Drill-down: {ServerTimeHelper.FormatServerTime(ToUtcFromServerLocal(fromDate), "HH:mm")} → {ServerTimeHelper.FormatServerTime(ToUtcFromServerLocal(toDate), "HH:mm")}";
        _ = LoadActiveQueriesSlicerAsync();
    }

    private async void OnBlockingDrillDown(DateTime time)
    {
        var (fromDate, toDate) = GetDrillWindow(ToUtcFromServerLocal(time), 30, 30, _serverClock);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 8; // Blocking
        BlockingSubTabControl.SelectedIndex = 2; // Blocked Process Reports
        var bpr = await System.Threading.Tasks.Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, 0, fromDate, toDate));
        _blockedProcessFilterMgr!.UpdateData(bpr);
    }

    private async void OnDeadlockDrillDown(DateTime time)
    {
        var (fromDate, toDate) = GetDrillWindow(ToUtcFromServerLocal(time), 30, 30, _serverClock);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 8; // Blocking
        BlockingSubTabControl.SelectedIndex = 3; // Deadlocks
        var dlr = await System.Threading.Tasks.Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, 0, fromDate, toDate));
        _deadlockFilterMgr!.UpdateData(await ParseDeadlocksOffUiThreadAsync(dlr));
    }

    private async void OnHeatmapDrillDown(DateTime bucketTimeUtc)
    {
        var serverTime = ToServerLocal(bucketTimeUtc);
        var (fromDate, toDate) = GetDrillWindow(bucketTimeUtc, 5, 10, _serverClock);

        AppLogger.Info("DrillDown", $"OnHeatmapDrillDown: bucketTimeUtc={bucketTimeUtc:O}, OffsetMinutesAtBucket={_serverClock.OffsetMinutesAt(bucketTimeUtc)}, serverTime={serverTime:O}, fromDate={fromDate:O}, toDate={toDate:O}");

        SetDrillDownTimeRange(fromDate, toDate);

        SelectActiveQueriesForDrillDown();

        AppLogger.Info("DrillDown", $"Calling GetLatestQuerySnapshotsAsync with fromDate={fromDate:O}, toDate={toDate:O}");
        var snapshots = await System.Threading.Tasks.Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate));
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
