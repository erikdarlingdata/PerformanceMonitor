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
    /// <paramref name="minutesAfter"/> real minutes either side of <paramref name="centerUtc"/> (naive UTC), as a UTC
    /// pair like every other window (#4766). Arithmetic on the instant, so a drill in the repeated hour opens the
    /// sixty real minutes around the point clicked instead of collapsing to one instant.
    /// </summary>
    internal static (DateTime From, DateTime To) GetDrillWindow(
        DateTime centerUtc, int minutesBefore, int minutesAfter)
        => TimeWindows.Drill(centerUtc, minutesBefore, minutesAfter);

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

        // ±30 minute window around the clicked point (the chart plots the UTC instant, so the point is the centre as it is)
        var (fromDate, toDate) = GetDrillWindow(time, 30, 30);

        var window = new Windows.WaitDrillDownWindow(
            _dataService, _serverId, waitType, 1, GetPickerZone(), fromDate, toDate,
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
    /// The live-snapshot indicator's text while a drill-down window is shown (#4766): both ends worded as
    /// hours and minutes of <paramref name="zone"/>, the tab's own display zone, so the heatmap drill and the generic
    /// drill read the same and neither claims to be "server time" when the mode shows another zone.
    /// </summary>
    internal static string DrillDownIndicatorText(DateTime fromUtc, DateTime toUtc, TimeZoneInfo zone) =>
        $"Drill-down: {DisplayZone.Format(fromUtc, zone, "HH:mm")} → {DisplayZone.Format(toUtc, zone, "HH:mm")}";

    /// <summary>
    /// Generic "Show Active Queries at This Time" drill-down for resource charts that have no
    /// more specific target (memory clerks/grants/pressure, tempdb size + file I/O, file I/O
    /// latency + throughput, current waits, perfmon). The "Showing since" banner is refreshed for the drill
    /// window too (#4953), over the same UTC pair the grid read takes: left alone it keeps describing the last
    /// range read, so it can claim a cut the drill window does not have or miss one it does.
    /// </summary>
    private async void OnActiveQueriesDrillDown(DateTime time)
    {
        var (fromDate, toDate) = GetDrillWindow(time, 30, 30);
        SetDrillDownTimeRange(fromDate, toDate);

        SelectActiveQueriesForDrillDown();
        var snapshots = await System.Threading.Tasks.Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate));
        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = DrillDownIndicatorText(fromDate, toDate, GetPickerZone());
        _ = LoadActiveQueriesSlicerAsync();
        /* fromDate/toDate are the naive-UTC pair GetDrillWindow built (#4766). The grid read above took them as they
           are (GetTimeRange's custom-range branch) and the banner probe compares against the same UTC collection_time,
           so they go through unconverted, as the slicer handler's e.StartUtc/e.EndUtc do (#4279). */
        await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QuerySnapshots, ActiveQueriesWindowTruncatedBanner, fromDate, toDate);
    }

    private async void OnBlockingDrillDown(DateTime time)
    {
        var (fromDate, toDate) = GetDrillWindow(time, 30, 30);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 8; // Blocking
        BlockingSubTabControl.SelectedIndex = 2; // Blocked Process Reports
        var bpr = await System.Threading.Tasks.Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, 0, fromDate, toDate));
        _blockedProcessFilterMgr!.UpdateData(bpr);
    }

    private async void OnDeadlockDrillDown(DateTime time)
    {
        var (fromDate, toDate) = GetDrillWindow(time, 30, 30);
        SetDrillDownTimeRange(fromDate, toDate);

        MainTabControl.SelectedIndex = 8; // Blocking
        BlockingSubTabControl.SelectedIndex = 3; // Deadlocks
        var dlr = await System.Threading.Tasks.Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, 0, fromDate, toDate));
        _deadlockFilterMgr!.UpdateData(await ParseDeadlocksOffUiThreadAsync(dlr));
    }

    private async void OnHeatmapDrillDown(DateTime bucketTimeUtc)
    {
        var (fromDate, toDate) = GetDrillWindow(bucketTimeUtc, 5, 10);

        AppLogger.Info("DrillDown", $"OnHeatmapDrillDown: bucketTimeUtc={bucketTimeUtc:O}, OffsetMinutesAtBucket={_serverClock.OffsetMinutesAt(bucketTimeUtc)}, serverTime={_serverClock.ToServerLocal(bucketTimeUtc):O}, fromDate={fromDate:O}, toDate={toDate:O}");

        SetDrillDownTimeRange(fromDate, toDate);

        SelectActiveQueriesForDrillDown();

        AppLogger.Info("DrillDown", $"Calling GetLatestQuerySnapshotsAsync with fromDate={fromDate:O}, toDate={toDate:O}");
        var snapshots = await System.Threading.Tasks.Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromDate, toDate));
        AppLogger.Info("DrillDown", $"Got {snapshots.Count} snapshots");

        _querySnapshotsFilterMgr!.UpdateData(snapshots);
        LiveSnapshotIndicator.Text = DrillDownIndicatorText(fromDate, toDate, GetPickerZone());
        _ = LoadActiveQueriesSlicerAsync();
        /* The heatmap drill's window is the same naive-UTC pair the grid read took (see OnActiveQueriesDrillDown):
           the banner follows it, so it stops describing the last range read (#4953). */
        await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QuerySnapshots, ActiveQueriesWindowTruncatedBanner, fromDate, toDate);
    }

    /// <summary>
    /// Sets the time range combo to Custom and populates the date/time pickers
    /// so the user can navigate other tabs at the same time window.
    /// </summary>
    private void SetDrillDownTimeRange(DateTime fromUtc, DateTime toUtc)
    {
        /* The drill's window is held as the instants it names (#4766) and the pickers show it in the display zone. */
        _customRange.Set(fromUtc, toUtc);

        // Switch to Custom without triggering a refresh
        _isRefreshing = true;
        try
        {
            TimeRangeCombo.SelectedIndex = 5; // Custom
            RenderCustomRange();

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
