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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Blocking inner tab — upgraded to Lite's sub-tab structure (ServerTab.xaml:1435-1469): "Trends"
/// (lock-wait rate, blocking incidents, deadlocks) and "Current Waits" (waiting-task duration by wait
/// type, blocked sessions by database) precede the existing "Blocked Process Reports" grid, which is
/// re-hosted unchanged as the third sub-tab. The five trend-chart bodies are COPIES of Lite's
/// <c>ServerTab.Charts.cs</c> Update* methods, reads rewired to <see cref="ViewerDataService"/> Postgres.
/// Two render-body deviations from Lite: (1) the time axis runs through
/// <see cref="ViewerTimeHelper.ForDisplay"/> instead of Lite's per-server <c>UtcOffsetMinutes</c> shift;
/// (2) the per-server toolbar's settable window supplies the X-axis range. The spike-plot /
/// zero-line-when-empty shapes, the fixed
/// <see cref="ChartPalette.SeriesColor"/> identities for the single-series charts, and the cycling
/// <see cref="ChartPalette"/> colors for the multi-series charts are preserved. W1e adds Lite's full
/// Blocked Process Reports grid (widened + slicer + block-chain viewer) and the Deadlocks sub-tab
/// (deadlock-graph viewer); chart drill-downs remain deferred (no Active Queries surface yet).
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _lockWaitTrendHover;
    private ChartHoverHelper? _blockingTrendHover;
    private ChartHoverHelper? _deadlockTrendHover;
    private ChartHoverHelper? _blockingDurationHover;
    private ChartHoverHelper? _blockingTotalDurationHover;
    private ChartHoverHelper? _deadlockWaitHover;
    private ChartHoverHelper? _deadlockTotalWaitHover;
    private ChartHoverHelper? _currentWaitsDurationHover;
    private ChartHoverHelper? _currentWaitsBlockedHover;

    /* Blocking sub-tab order (mirrors LoadBlockingAsync's switch + Lite's BlockingSubTabControl): Trends,
       Blocking Stats, Current Waits, Blocked Process Reports, Deadlocks. Named so the chart drill-downs
       (OnBlockingDrillDown / OnDeadlockDrillDown) target the right sub-tab without a magic literal; the
       Blocking Stats severity sub-tab sits right after Trends (its count-only sibling). */
    private const int BlockingTrendsSubTabIndex = 0;
    private const int BlockingStatsSubTabIndex = 1;
    private const int BlockingCurrentWaitsSubTabIndex = 2;
    private const int BlockedProcessReportsSubTabIndex = 3;
    private const int DeadlocksSubTabIndex = 4;

    /// <summary>
    /// Applies the shared chrome to the five Blocking trend charts and wires their hover tooltips
    /// (Lite's per-chart units). Called from the constructor after <c>InitializeComponent</c> so the
    /// charts don't flash white before the tab's first load.
    /// </summary>
    private void InitializeBlockingCharts()
    {
        ApplyTheme(LockWaitTrendChart);
        LockWaitTrendChart.Refresh();
        ApplyTheme(BlockingTrendChart);
        BlockingTrendChart.Refresh();
        ApplyTheme(DeadlockTrendChart);
        DeadlockTrendChart.Refresh();
        ApplyTheme(BlockingDurationChart);
        BlockingDurationChart.Refresh();
        ApplyTheme(BlockingTotalDurationChart);
        BlockingTotalDurationChart.Refresh();
        ApplyTheme(DeadlockWaitChart);
        DeadlockWaitChart.Refresh();
        ApplyTheme(DeadlockTotalWaitChart);
        DeadlockTotalWaitChart.Refresh();
        ApplyTheme(CurrentWaitsDurationChart);
        CurrentWaitsDurationChart.Refresh();
        ApplyTheme(CurrentWaitsBlockedChart);
        CurrentWaitsBlockedChart.Refresh();

        _lockWaitTrendHover = new ChartHoverHelper(LockWaitTrendChart, "ms/sec");
        _blockingTrendHover = new ChartHoverHelper(BlockingTrendChart, "incidents");
        _deadlockTrendHover = new ChartHoverHelper(DeadlockTrendChart, "deadlocks");
        _blockingDurationHover = new ChartHoverHelper(BlockingDurationChart, "ms");
        _blockingTotalDurationHover = new ChartHoverHelper(BlockingTotalDurationChart, "ms");
        _deadlockWaitHover = new ChartHoverHelper(DeadlockWaitChart, "ms");
        _deadlockTotalWaitHover = new ChartHoverHelper(DeadlockTotalWaitChart, "ms");
        _currentWaitsDurationHover = new ChartHoverHelper(CurrentWaitsDurationChart, "ms");
        _currentWaitsBlockedHover = new ChartHoverHelper(CurrentWaitsBlockedChart, "sessions");

        /* The Blocked Process Reports + Deadlocks sub-tabs each carry a UTC slicer; dragging it re-reads
           its grid over the selection (Lite's OnBlockingSlicerChanged / OnDeadlockSlicerChanged). */
        BlockingSlicer.RangeChanged += OnBlockingSlicerChanged;
        DeadlockSlicer.RangeChanged += OnDeadlockSlicerChanged;
    }

    /// <summary>
    /// A Blocking sub-tab switch reloads through the shell's overlap-guarded
    /// <see cref="RefreshActiveInnerTabAsync"/> (the Blocking tab is the active inner tab whenever its
    /// sub-tabs are visible, so the loader dispatches to <see cref="LoadBlockingAsync"/> and loads only
    /// the newly-visible sub-tab). Gated on <see cref="System.Windows.FrameworkElement.IsLoaded"/> and
    /// the sub-TabControl's own selection so build-time and bubbled selections are ignored.
    /// </summary>
    private async void BlockingSubTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, BlockingSubTabs) || !IsLoaded)
        {
            return;
        }

        /* A chart drill-down (Blocking / Deadlocks trend charts) switches this sub-tab programmatically and
           runs its own targeted read; skip the generic loader so it doesn't race that. */
        if (_suppressDrillDownAutoRefresh)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Loads the Blocking tab's ACTIVE sub-tab only (mirrors Lite's subTabOnly gating): Trends reads the
    /// three trend series concurrently, Current Waits reads the two current-waits series concurrently,
    /// and Blocked Process Reports reads the XE-with-DMV-fallback grid. All window on the toolbar's
    /// settable range.
    /// </summary>
    private async Task LoadBlockingAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        switch (BlockingSubTabs.SelectedIndex)
        {
            case BlockingTrendsSubTabIndex:
                var lockWaitTask = _dataService.GetLockWaitTrendAsync(_server.ServerId, startUtc, endUtc);
                var blockingTask = _dataService.GetBlockingTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
                var deadlockTask = _dataService.GetDeadlockTrendAsync(_server.ServerId, startUtc, endUtc);
                var lockWaits = await lockWaitTask;
                var blocking = await blockingTask;
                var deadlocks = await deadlockTask;
                RenderLockWaitTrendChart(lockWaits);
                RenderBlockingTrendChart(blocking);
                RenderDeadlockTrendChart(deadlocks);
                break;
            case BlockingStatsSubTabIndex:
                /* Blocking SEVERITY: the duration aggregate reconciles with the count trend (same XE→DMV
                   source selection); the deadlock COUNT is the cheap sibling of the Trends tab's deadlock
                   trend, summed here for the summary strip. The deadlock SEVERITY aggregate (victim_count +
                   total/max/avg wait, parsed on-the-fly from deadlock_graph_xml) is drawn from the SAME
                   v_deadlocks/collection_time window as the count, so the two reconcile in period. */
                var durationStatsTask = _dataService.GetBlockingDurationStatsAsync(_server.ServerId, startUtc, endUtc);
                var deadlockCountTask = _dataService.GetDeadlockTrendAsync(_server.ServerId, startUtc, endUtc);
                var deadlockSeverityTask = _dataService.GetDeadlockSeverityStatsAsync(_server.ServerId, startUtc, endUtc);
                var durationStats = await durationStatsTask;
                var deadlockCounts = await deadlockCountTask;
                var deadlockSeverity = await deadlockSeverityTask;
                RenderBlockingDurationChart(durationStats);
                RenderBlockingTotalDurationChart(durationStats);
                RenderDeadlockWaitChart(deadlockSeverity);
                RenderDeadlockTotalWaitChart(deadlockSeverity);
                UpdateBlockingStatsSummary(durationStats, deadlockCounts, deadlockSeverity);
                break;
            case BlockingCurrentWaitsSubTabIndex:
                var durationTask = _dataService.GetWaitingTaskTrendAsync(_server.ServerId, startUtc, endUtc);
                var blockedTask = _dataService.GetBlockedSessionTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
                var duration = await durationTask;
                var blocked = await blockedTask;
                RenderCurrentWaitsDurationChart(duration);
                RenderCurrentWaitsBlockedChart(blocked);
                break;
            case BlockedProcessReportsSubTabIndex:
                await LoadBlockedProcessReportsAsync(startUtc, endUtc);
                break;
            case DeadlocksSubTabIndex:
            default:
                await LoadDeadlocksAsync(startUtc, endUtc);
                break;
        }
    }

    /// <summary>
    /// Loads the Blocked Process Reports sub-tab (W1e Lite parity): the widened XE-preferred / DMV-fallback
    /// grid bound through the filter manager (so active column filters survive the refresh) plus the UTC
    /// slicer over the same window. The grid reads the full window; dragging the slicer re-reads it
    /// over the selection (<see cref="OnBlockingSlicerChanged"/>). The Npgsql reads are genuinely async, so
    /// unlike Lite's DuckDB path there is no Task.Run wrap.
    /// </summary>
    private async Task LoadBlockedProcessReportsAsync(DateTime startUtc, DateTime endUtc)
    {
        var rows = await _dataService.GetRecentBlockedProcessReportsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _blockedProcessFilterMgr!.UpdateData(rows);
        await LoadBlockingSlicerAsync(startUtc, endUtc);
    }

    /// <summary>
    /// Loads the Deadlocks sub-tab (W1e Lite parity): reads the recent deadlock events, parses each graph
    /// into per-process detail rows OFF the UI thread (Lite's #1193 fix — the graph walk is CPU-bound XML
    /// work), binds them through the filter manager, and loads the UTC slicer.
    /// </summary>
    private async Task LoadDeadlocksAsync(DateTime startUtc, DateTime endUtc)
    {
        var rows = await _dataService.GetRecentDeadlocksAsync(_server.ServerId, startUtc, endUtc);
        var details = await ParseDeadlocksOffUiThreadAsync(rows);
        _deadlockFilterMgr!.UpdateData(details);
        await LoadDeadlockSlicerAsync(startUtc, endUtc);
    }

    // ── Blocking / Deadlock slicers (W1e) ──

    private string _blockingSlicerMetric = "Events";
    private List<TimeSliceBucket>? _blockingSlicerData;
    private List<TimeSliceBucket>? _deadlockSlicerData;

    private async Task LoadBlockingSlicerAsync(DateTime startUtc, DateTime endUtc)
    {
        var data = await _dataService.GetBlockingSlicerDataAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _blockingSlicerData = data;
        _blockingSlicerMetric = "Events";
        if (data.Count > 0)
            BlockingSlicer.LoadData(data, "Blocking Events", startUtc, endUtc);
    }

    private async Task LoadDeadlockSlicerAsync(DateTime startUtc, DateTime endUtc)
    {
        var data = await _dataService.GetDeadlockSlicerDataAsync(_server.ServerId, startUtc, endUtc);
        _deadlockSlicerData = data;
        if (data.Count > 0)
            DeadlockSlicer.LoadData(data, "Deadlocks", startUtc, endUtc);
    }

    /// <summary>The slicer sends UTC bounds; the viewer's reads take naive UTC directly (no clock shift).</summary>
    private async void OnBlockingSlicerChanged(object? sender, SlicerRangeEventArgs e)
    {
        try
        {
            var rows = await _dataService.GetRecentBlockedProcessReportsAsync(_server.ServerId, e.StartUtc, e.EndUtc, databaseNames: SelectedDatabaseFilter);
            _blockedProcessFilterMgr!.UpdateData(rows);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"blocking slicer failed: {ex.Message}");
        }
    }

    private async void OnDeadlockSlicerChanged(object? sender, SlicerRangeEventArgs e)
    {
        try
        {
            var rows = await _dataService.GetRecentDeadlocksAsync(_server.ServerId, e.StartUtc, e.EndUtc);
            _deadlockFilterMgr!.UpdateData(await ParseDeadlocksOffUiThreadAsync(rows));
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"deadlock slicer failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Re-metrics the blocking slicer overlay to the sorted column (Lite's BlockedProcessReportGrid_Sorting):
    /// sorting the grid by wait / blocker / blocked / database swaps the slicer's aggregate curve to match.
    /// </summary>
    private void BlockedProcessReportGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_blockingSlicerData == null || _blockingSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col))
        {
            if (e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
                col = b.Path.Path;
        }
        var (metric, label) = col switch
        {
            "WaitTimeMs" => ("TotalCpu", "Total Wait (sec)"),
            "BlockingSpid" => ("TotalElapsed", "Distinct Blockers"),
            "BlockedSpid" => ("TotalReads", "Distinct Blocked"),
            "DatabaseName" => ("TotalLogicalReads", "Distinct Databases"),
            _ => ("Events", "Blocking Events"),
        };

        if (metric == _blockingSlicerMetric) return;
        _blockingSlicerMetric = metric;

        foreach (var bucket in _blockingSlicerData)
        {
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "TotalElapsed" => bucket.TotalElapsed,
                "TotalReads" => bucket.TotalReads,
                "TotalLogicalReads" => bucket.TotalLogicalReads,
                _ => bucket.SessionCount,
            };
        }

        BlockingSlicer.UpdateMetric(label);
    }

    /* Parse each deadlock graph into its per-process rows on the thread pool — CPU-bound XML work that
       would hitch the dispatcher on the Blocking tab (Lite's #1193 fix). Only the grid bind stays on the UI. */
    private Task<List<DeadlockProcessDetail>> ParseDeadlocksOffUiThreadAsync(List<ViewerDeadlockRow> rows)
    {
        /* #1319: deadlocks have no database_name column (the per-process DB is inside the graph XML), so
           the global database filter is applied client-side on the parsed per-process rows. Snapshot the
           selection on the UI thread; empty = All (unfiltered). */
        var selected = _selectedDatabases.Count == 0
            ? null
            : new HashSet<string>(_selectedDatabases, StringComparer.OrdinalIgnoreCase);
        return Task.Run(() =>
        {
            var details = DeadlockProcessDetail.ParseFromRows(rows);
            return selected == null
                ? details
                : details.Where(d => selected.Contains(d.DatabaseName)).ToList();
        });
    }

    /// <summary>
    /// "View Blocked Query Plan" / "View Blocking Query Plan" for a blocked-process report row: opens the
    /// BEST-EFFORT plan the row already carries (blocked_process_reports.blocked_query_plan_xml /
    /// blocking_query_plan_xml, #1368 / V7) in the shared Plan Viewer host — NO live SQL, the same
    /// stored-plan surface Top Queries uses. The context items are gated per row on Has*QueryPlan (a NULL
    /// plan shows them disabled), so these only fire with a captured plan; the guard here is belt-and-braces
    /// (and covers the DMV-snapshot fallback rows, which never carry a plan). Lite recovered these plans
    /// live from the monitored server's cache — the headless viewer can't, so only the stored plan shows,
    /// and Lite's "Get Actual Plan" has no viewer equivalent (omitted everywhere).
    /// </summary>
    private void ViewBlockedQueryPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        if (FindParentDataGrid(menuItem)?.CurrentItem is not ViewerBlockedProcessRow row || !row.HasBlockedQueryPlan) return;
        _ = OpenPlanTab(row.BlockedQueryPlanXml!, $"Blocked Plan - SPID {row.BlockedSpid}", row.BlockedSqlText);
    }

    private void ViewBlockingQueryPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        if (FindParentDataGrid(menuItem)?.CurrentItem is not ViewerBlockedProcessRow row || !row.HasBlockingQueryPlan) return;
        _ = OpenPlanTab(row.BlockingQueryPlanXml!, $"Blocking Plan - SPID {row.BlockingSpid}", row.BlockingSqlText);
    }

    private void RenderLockWaitTrendChart(List<LockWaitTrendPoint> data)
    {
        ClearChart(LockWaitTrendChart);
        ApplyTheme(LockWaitTrendChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _lockWaitTrendHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = LockWaitTrendChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Lock Waits";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("LockWaits"));
            zeroLine.MarkerSize = 0;
            LockWaitTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
            LockWaitTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(LockWaitTrendChart);
            LockWaitTrendChart.Plot.YLabel("Lock Wait Time (ms/sec)");
            SetChartYLimitsWithLegendPadding(LockWaitTrendChart, 0, 1);
            ShowChartLegend(LockWaitTrendChart);
            LockWaitTrendChart.Refresh();
            return;
        }

        var grouped = data.GroupBy(d => d.WaitType).ToList();
        double globalMax = 0;

        for (int i = 0; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var times = group.Select(t => ViewerTimeHelper.ForDisplay(t.CollectionTime).ToOADate()).ToArray();
            var values = group.Select(t => t.WaitTimeMsPerSecond).ToArray();

            var plot = LockWaitTrendChart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
            _lockWaitTrendHover?.Add(plot, group.Key);

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        LockWaitTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        LockWaitTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(LockWaitTrendChart);
        LockWaitTrendChart.Plot.YLabel("Lock Wait Time (ms/sec)");
        SetChartYLimitsWithLegendPadding(LockWaitTrendChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(LockWaitTrendChart);
        LockWaitTrendChart.Refresh();
    }

    private void RenderBlockingTrendChart(List<BlockingTrendPoint> data)
    {
        ClearChart(BlockingTrendChart);
        ApplyTheme(BlockingTrendChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _blockingTrendHover?.Clear();
        if (data.Count == 0)
        {
            /* No blocking events — show a flat line at zero so the chart looks active */
            var zeroLine = BlockingTrendChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Blocking Incidents";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
            zeroLine.MarkerSize = 0;
            BlockingTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(BlockingTrendChart);
            BlockingTrendChart.Plot.YLabel("Blocking Incidents");
            SetChartYLimitsWithLegendPadding(BlockingTrendChart, 0, 1);
            ShowChartLegend(BlockingTrendChart);
            BlockingTrendChart.Refresh();
            return;
        }

        /* Build arrays with zero baseline between data points for spike effect */
        var expandedTimes = new List<double>();
        var expandedCounts = new List<double>();

        /* Add zero at start */
        expandedTimes.Add(rangeStart.ToOADate());
        expandedCounts.Add(0);

        foreach (var point in data.OrderBy(d => d.Time))
        {
            var time = ViewerTimeHelper.ForDisplay(point.Time).ToOADate();
            /* Go to zero just before the spike */
            expandedTimes.Add(time - 0.0001);
            expandedCounts.Add(0);
            /* Spike up */
            expandedTimes.Add(time);
            expandedCounts.Add(point.Count);
            /* Back to zero just after */
            expandedTimes.Add(time + 0.0001);
            expandedCounts.Add(0);
        }

        /* Add zero at end */
        expandedTimes.Add(rangeEnd.ToOADate());
        expandedCounts.Add(0);

        var plot = BlockingTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray()); /* synthetic spike baseline (±0.0001d offsets), NOT a cadence series - gap-breaking would lock onto the artificial deltas (#1944 review) */
        plot.LegendText = "Blocking Incidents";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
        plot.MarkerSize = 0; /* No markers, just lines */
        _blockingTrendHover?.Add(plot, "Blocking Incidents");

        BlockingTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        BlockingTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(BlockingTrendChart);
        BlockingTrendChart.Plot.YLabel("Blocking Incidents");
        SetChartYLimitsWithLegendPadding(BlockingTrendChart, 0, data.Max(d => d.Count));
        ShowChartLegend(BlockingTrendChart);
        BlockingTrendChart.Refresh();
    }

    private void RenderDeadlockTrendChart(List<BlockingTrendPoint> data)
    {
        ClearChart(DeadlockTrendChart);
        ApplyTheme(DeadlockTrendChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _deadlockTrendHover?.Clear();
        if (data.Count == 0)
        {
            /* No deadlocks — show a flat line at zero so the chart looks active */
            var zeroLine = DeadlockTrendChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Deadlocks";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
            zeroLine.MarkerSize = 0;
            DeadlockTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DeadlockTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(DeadlockTrendChart);
            DeadlockTrendChart.Plot.YLabel("Deadlocks");
            SetChartYLimitsWithLegendPadding(DeadlockTrendChart, 0, 1);
            ShowChartLegend(DeadlockTrendChart);
            DeadlockTrendChart.Refresh();
            return;
        }

        /* Build arrays with zero baseline between data points for spike effect */
        var expandedTimes = new List<double>();
        var expandedCounts = new List<double>();

        /* Add zero at start */
        expandedTimes.Add(rangeStart.ToOADate());
        expandedCounts.Add(0);

        foreach (var point in data.OrderBy(d => d.Time))
        {
            var time = ViewerTimeHelper.ForDisplay(point.Time).ToOADate();
            /* Go to zero just before the spike */
            expandedTimes.Add(time - 0.0001);
            expandedCounts.Add(0);
            /* Spike up */
            expandedTimes.Add(time);
            expandedCounts.Add(point.Count);
            /* Back to zero just after */
            expandedTimes.Add(time + 0.0001);
            expandedCounts.Add(0);
        }

        /* Add zero at end */
        expandedTimes.Add(rangeEnd.ToOADate());
        expandedCounts.Add(0);

        var plot = DeadlockTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray()); /* synthetic spike baseline (±0.0001d offsets), NOT a cadence series - gap-breaking would lock onto the artificial deltas (#1944 review) */
        plot.LegendText = "Deadlocks";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
        plot.MarkerSize = 0; /* No markers, just lines */
        _deadlockTrendHover?.Add(plot, "Deadlocks");

        DeadlockTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        DeadlockTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(DeadlockTrendChart);
        DeadlockTrendChart.Plot.YLabel("Deadlocks");
        SetChartYLimitsWithLegendPadding(DeadlockTrendChart, 0, data.Max(d => d.Count));
        ShowChartLegend(DeadlockTrendChart);
        DeadlockTrendChart.Refresh();
    }

    /// <summary>
    /// Max + Avg block duration (ms) per minute — the per-incident severity signal (how long an individual
    /// block lasts). Two connected-scatter series on one shared ms axis (Max ≥ Avg, comparable magnitudes),
    /// mirroring the Current Waits duration chart's render idiom. Total duration lands on its own chart
    /// (<see cref="RenderBlockingTotalDurationChart"/>) because its aggregate magnitude dwarfs these.
    /// </summary>
    private void RenderBlockingDurationChart(List<BlockingDurationStatsPoint> data)
    {
        ClearChart(BlockingDurationChart);
        ApplyTheme(BlockingDurationChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _blockingDurationHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = BlockingDurationChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Max Block Duration";
            zeroLine.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
            zeroLine.MarkerSize = 0;
            BlockingDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(BlockingDurationChart);
            BlockingDurationChart.Plot.YLabel("Block Duration (ms)");
            SetChartYLimitsWithLegendPadding(BlockingDurationChart, 0, 1);
            ShowChartLegend(BlockingDurationChart);
            BlockingDurationChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => ViewerTimeHelper.ForDisplay(d.Time).ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
        var maxValues = PadEnds(ordered.Select(d => (double)d.MaxDurationMs).ToArray(), 0, 0);
        var avgValues = PadEnds(ordered.Select(d => d.AvgDurationMs).ToArray(), 0, 0);

        var maxPlot = BlockingDurationChart.Plot.Add.TimeSeries(times, maxValues);
        maxPlot.LegendText = "Max Block Duration";
        maxPlot.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
        ChartStyle.StyleScatter(maxPlot);
        _blockingDurationHover?.Add(maxPlot, "Max Block Duration");

        var avgPlot = BlockingDurationChart.Plot.Add.TimeSeries(times, avgValues);
        avgPlot.LegendText = "Avg Block Duration";
        avgPlot.Color = ScottPlot.Color.FromHex(SeriesColors[1]);
        ChartStyle.StyleScatter(avgPlot);
        _blockingDurationHover?.Add(avgPlot, "Avg Block Duration");

        double globalMax = maxValues.Length > 0 ? maxValues.Max() : 0;

        BlockingDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
        BlockingDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(BlockingDurationChart);
        BlockingDurationChart.Plot.YLabel("Block Duration (ms)");
        SetChartYLimitsWithLegendPadding(BlockingDurationChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(BlockingDurationChart);
        BlockingDurationChart.Refresh();
    }

    /// <summary>
    /// Total block duration (ms) per minute — the aggregate volume×severity signal (the sum of every block's
    /// wait time in the bucket). Single connected-scatter series on its own chart/axis so its magnitude
    /// doesn't swamp the per-incident Max/Avg chart; colored with the blocking identity so it reads as a
    /// sibling of the Trends tab's blocking-incident chart.
    /// </summary>
    private void RenderBlockingTotalDurationChart(List<BlockingDurationStatsPoint> data)
    {
        ClearChart(BlockingTotalDurationChart);
        ApplyTheme(BlockingTotalDurationChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _blockingTotalDurationHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = BlockingTotalDurationChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Total Block Duration";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
            zeroLine.MarkerSize = 0;
            BlockingTotalDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingTotalDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(BlockingTotalDurationChart);
            BlockingTotalDurationChart.Plot.YLabel("Total Block Duration (ms)");
            SetChartYLimitsWithLegendPadding(BlockingTotalDurationChart, 0, 1);
            ShowChartLegend(BlockingTotalDurationChart);
            BlockingTotalDurationChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => ViewerTimeHelper.ForDisplay(d.Time).ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
        var totals = PadEnds(ordered.Select(d => (double)d.TotalDurationMs).ToArray(), 0, 0);

        var plot = BlockingTotalDurationChart.Plot.Add.TimeSeries(times, totals);
        plot.LegendText = "Total Block Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
        ChartStyle.StyleScatter(plot);
        _blockingTotalDurationHover?.Add(plot, "Total Block Duration");

        double globalMax = totals.Length > 0 ? totals.Max() : 0;

        BlockingTotalDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
        BlockingTotalDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(BlockingTotalDurationChart);
        BlockingTotalDurationChart.Plot.YLabel("Total Block Duration (ms)");
        SetChartYLimitsWithLegendPadding(BlockingTotalDurationChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(BlockingTotalDurationChart);
        BlockingTotalDurationChart.Refresh();
    }

    /// <summary>
    /// Max + Avg deadlock wait (ms) per minute — the per-process deadlock severity signal (how long the
    /// deadlocked processes had waited before the monitor broke the cycle). Two connected-scatter series on one
    /// shared ms axis (Max ≥ Avg, comparable magnitudes), the deadlock analog of
    /// <see cref="RenderBlockingDurationChart"/>. Total deadlock wait lands on its own chart
    /// (<see cref="RenderDeadlockTotalWaitChart"/>) because its aggregate magnitude dwarfs these.
    /// </summary>
    private void RenderDeadlockWaitChart(List<DeadlockSeverityStatsPoint> data)
    {
        ClearChart(DeadlockWaitChart);
        ApplyTheme(DeadlockWaitChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _deadlockWaitHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = DeadlockWaitChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Max Deadlock Wait";
            zeroLine.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
            zeroLine.MarkerSize = 0;
            DeadlockWaitChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DeadlockWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(DeadlockWaitChart);
            DeadlockWaitChart.Plot.YLabel("Deadlock Wait (ms)");
            SetChartYLimitsWithLegendPadding(DeadlockWaitChart, 0, 1);
            ShowChartLegend(DeadlockWaitChart);
            DeadlockWaitChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => ViewerTimeHelper.ForDisplay(d.Time).ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
        var maxValues = PadEnds(ordered.Select(d => (double)d.MaxWaitMs).ToArray(), 0, 0);
        var avgValues = PadEnds(ordered.Select(d => d.AvgWaitMs).ToArray(), 0, 0);

        var maxPlot = DeadlockWaitChart.Plot.Add.TimeSeries(times, maxValues);
        maxPlot.LegendText = "Max Deadlock Wait";
        maxPlot.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
        ChartStyle.StyleScatter(maxPlot);
        _deadlockWaitHover?.Add(maxPlot, "Max Deadlock Wait");

        var avgPlot = DeadlockWaitChart.Plot.Add.TimeSeries(times, avgValues);
        avgPlot.LegendText = "Avg Deadlock Wait";
        avgPlot.Color = ScottPlot.Color.FromHex(SeriesColors[1]);
        ChartStyle.StyleScatter(avgPlot);
        _deadlockWaitHover?.Add(avgPlot, "Avg Deadlock Wait");

        double globalMax = maxValues.Length > 0 ? maxValues.Max() : 0;

        DeadlockWaitChart.Plot.Axes.DateTimeTicksBottomDateChange();
        DeadlockWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(DeadlockWaitChart);
        DeadlockWaitChart.Plot.YLabel("Deadlock Wait (ms)");
        SetChartYLimitsWithLegendPadding(DeadlockWaitChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(DeadlockWaitChart);
        DeadlockWaitChart.Refresh();
    }

    /// <summary>
    /// Total deadlock wait (ms) per minute — the aggregate signal (the sum of every deadlocked process's wait
    /// time in the bucket). Single connected-scatter series on its own chart/axis so its magnitude doesn't
    /// swamp the per-process Max/Avg chart; colored with the Deadlocks identity so it reads as a sibling of the
    /// Trends tab's deadlock-count chart. The deadlock analog of <see cref="RenderBlockingTotalDurationChart"/>.
    /// </summary>
    private void RenderDeadlockTotalWaitChart(List<DeadlockSeverityStatsPoint> data)
    {
        ClearChart(DeadlockTotalWaitChart);
        ApplyTheme(DeadlockTotalWaitChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _deadlockTotalWaitHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = DeadlockTotalWaitChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Total Deadlock Wait";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
            zeroLine.MarkerSize = 0;
            DeadlockTotalWaitChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DeadlockTotalWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(DeadlockTotalWaitChart);
            DeadlockTotalWaitChart.Plot.YLabel("Total Deadlock Wait (ms)");
            SetChartYLimitsWithLegendPadding(DeadlockTotalWaitChart, 0, 1);
            ShowChartLegend(DeadlockTotalWaitChart);
            DeadlockTotalWaitChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => ViewerTimeHelper.ForDisplay(d.Time).ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
        var totals = PadEnds(ordered.Select(d => (double)d.TotalWaitMs).ToArray(), 0, 0);

        var plot = DeadlockTotalWaitChart.Plot.Add.TimeSeries(times, totals);
        plot.LegendText = "Total Deadlock Wait";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
        ChartStyle.StyleScatter(plot);
        _deadlockTotalWaitHover?.Add(plot, "Total Deadlock Wait");

        double globalMax = totals.Length > 0 ? totals.Max() : 0;

        DeadlockTotalWaitChart.Plot.Axes.DateTimeTicksBottomDateChange();
        DeadlockTotalWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(DeadlockTotalWaitChart);
        DeadlockTotalWaitChart.Plot.YLabel("Total Deadlock Wait (ms)");
        SetChartYLimitsWithLegendPadding(DeadlockTotalWaitChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(DeadlockTotalWaitChart);
        DeadlockTotalWaitChart.Refresh();
    }

    /// <summary>
    /// Prepends <paramref name="lead"/> and appends <paramref name="trail"/> so a sparse connected-scatter
    /// series spans the whole selected window instead of ending at its last event. Pass the window's
    /// rangeStart/rangeEnd for the times array and 0/0 for value arrays (no activity = zero duration). The
    /// window-pinned analog of the Trends charts' zero-baseline expansion — keeps all four Blocking-Stats
    /// charts aligned to the shared window rather than each ending at its own last data point.
    /// </summary>
    private static double[] PadEnds(double[] values, double lead, double trail)
    {
        var result = new double[values.Length + 2];
        result[0] = lead;
        Array.Copy(values, 0, result, 1, values.Length);
        result[^1] = trail;
        return result;
    }

    /// <summary>
    /// Window-rollup summary strip for the Blocking Stats sub-tab: total blocking events, total / max / avg
    /// block duration (the avg is EVENT-weighted — total ÷ events — not a mean of the per-minute averages),
    /// the deadlock COUNT, and the deadlock SEVERITY rollup (total victim processes + total deadlock wait) over
    /// the window. The deadlock count is the cheap incident signal; the victim_count / total-deadlock-wait are
    /// parsed on-the-fly from <c>deadlock_graph_xml</c> over the SAME v_deadlocks window (so they reconcile in
    /// period), the Darling equivalent of the Dashboard's <c>victim_count</c> / <c>total_deadlock_wait_time_ms</c>.
    /// Durations render with the viewer's blocking wait-time format (ms under a second, else sec).
    /// </summary>
    private void UpdateBlockingStatsSummary(
        List<BlockingDurationStatsPoint> stats,
        List<BlockingTrendPoint> deadlocks,
        List<DeadlockSeverityStatsPoint> deadlockSeverity)
    {
        long totalEvents = stats.Sum(s => (long)s.EventCount);
        long totalDuration = stats.Sum(s => s.TotalDurationMs);
        long maxDuration = stats.Count > 0 ? stats.Max(s => s.MaxDurationMs) : 0;
        long totalDeadlocks = deadlocks.Sum(d => (long)d.Count);
        long avgDuration = totalEvents > 0 ? (long)Math.Round((double)totalDuration / totalEvents) : 0;
        long totalVictims = deadlockSeverity.Sum(d => (long)d.VictimCount);
        long totalDeadlockWait = deadlockSeverity.Sum(d => d.TotalWaitMs);

        BlockingStatsEventCountText.Text = totalEvents.ToString("N0");
        BlockingStatsTotalDurationText.Text = totalEvents > 0 ? ViewerDataService.FormatWaitTime(totalDuration) : "--";
        BlockingStatsMaxDurationText.Text = totalEvents > 0 ? ViewerDataService.FormatWaitTime(maxDuration) : "--";
        BlockingStatsAvgDurationText.Text = totalEvents > 0 ? ViewerDataService.FormatWaitTime(avgDuration) : "--";
        BlockingStatsDeadlockCountText.Text = totalDeadlocks.ToString("N0");
        BlockingStatsDeadlockVictimsText.Text = totalVictims.ToString("N0");
        BlockingStatsDeadlockWaitText.Text = totalDeadlocks > 0 ? ViewerDataService.FormatWaitTime(totalDeadlockWait) : "--";
    }

    private void RenderCurrentWaitsDurationChart(List<WaitingTaskTrendPoint> data)
    {
        ClearChart(CurrentWaitsDurationChart);
        ApplyTheme(CurrentWaitsDurationChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _currentWaitsDurationHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = CurrentWaitsDurationChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Current Waits";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("CurrentWaits"));
            zeroLine.MarkerSize = 0;
            CurrentWaitsDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CurrentWaitsDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(CurrentWaitsDurationChart);
            CurrentWaitsDurationChart.Plot.YLabel("Total Wait Duration (ms)");
            SetChartYLimitsWithLegendPadding(CurrentWaitsDurationChart, 0, 1);
            ShowChartLegend(CurrentWaitsDurationChart);
            CurrentWaitsDurationChart.Refresh();
            return;
        }

        var grouped = data.GroupBy(d => d.WaitType).OrderBy(g => g.Key).ToList();
        double globalMax = 0;

        for (int i = 0; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var ordered = group.OrderBy(t => t.CollectionTime).ToList();
            var times = ordered.Select(t => ViewerTimeHelper.ForDisplay(t.CollectionTime).ToOADate()).ToArray();
            var values = ordered.Select(t => (double)t.TotalWaitMs).ToArray();

            var plot = CurrentWaitsDurationChart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
            _currentWaitsDurationHover?.Add(plot, group.Key);

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        CurrentWaitsDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
        CurrentWaitsDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(CurrentWaitsDurationChart);
        CurrentWaitsDurationChart.Plot.YLabel("Total Wait Duration (ms)");
        SetChartYLimitsWithLegendPadding(CurrentWaitsDurationChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(CurrentWaitsDurationChart);
        CurrentWaitsDurationChart.Refresh();
    }

    private void RenderCurrentWaitsBlockedChart(List<BlockedSessionTrendPoint> data)
    {
        ClearChart(CurrentWaitsBlockedChart);
        ApplyTheme(CurrentWaitsBlockedChart);

        var (winStartUtc, winEndUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(winStartUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(winEndUtc);

        _currentWaitsBlockedHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = CurrentWaitsBlockedChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Blocked Sessions";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("BlockedSessions"));
            zeroLine.MarkerSize = 0;
            CurrentWaitsBlockedChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CurrentWaitsBlockedChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(CurrentWaitsBlockedChart);
            CurrentWaitsBlockedChart.Plot.YLabel("Blocked Sessions");
            SetChartYLimitsWithLegendPadding(CurrentWaitsBlockedChart, 0, 1);
            ShowChartLegend(CurrentWaitsBlockedChart);
            CurrentWaitsBlockedChart.Refresh();
            return;
        }

        var grouped = data.GroupBy(d => d.DatabaseName).OrderBy(g => g.Key).ToList();
        double globalMax = 0;

        for (int i = 0; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var ordered = group.OrderBy(t => t.CollectionTime).ToList();
            var times = ordered.Select(t => ViewerTimeHelper.ForDisplay(t.CollectionTime).ToOADate()).ToArray();
            var values = ordered.Select(t => (double)t.BlockedCount).ToArray();

            var plot = CurrentWaitsBlockedChart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
            _currentWaitsBlockedHover?.Add(plot, group.Key);

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        CurrentWaitsBlockedChart.Plot.Axes.DateTimeTicksBottomDateChange();
        CurrentWaitsBlockedChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(CurrentWaitsBlockedChart);
        CurrentWaitsBlockedChart.Plot.YLabel("Blocked Sessions");
        SetChartYLimitsWithLegendPadding(CurrentWaitsBlockedChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(CurrentWaitsBlockedChart);
        CurrentWaitsBlockedChart.Refresh();
    }

    /// <summary>Tears down the Blocking trend hover helpers; called from the single Dispose() in Charts.cs.</summary>
    private void DisposeBlockingHelpers()
    {
        _lockWaitTrendHover?.Dispose();
        _blockingTrendHover?.Dispose();
        _deadlockTrendHover?.Dispose();
        _blockingDurationHover?.Dispose();
        _blockingTotalDurationHover?.Dispose();
        _deadlockWaitHover?.Dispose();
        _deadlockTotalWaitHover?.Dispose();
        _currentWaitsDurationHover?.Dispose();
        _currentWaitsBlockedHover?.Dispose();
    }
}
