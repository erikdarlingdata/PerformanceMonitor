/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * Darling Viewer W1d: a COPY of Lite's Overview control (Lite/Controls/CorrelatedTimelineLanesControl)
 * with only the data layer rewired to Postgres — the five correlated lanes (CPU, total wait ms/sec,
 * blocking + deadlocking, buffer pool MB, file-I/O latency) with baseline bands + anomaly markers, over
 * the shared PerformanceMonitor.Ui CorrelatedCrosshairManager and ChartStyle chrome. The render bodies
 * (UpdateCpuLane / UpdateBlockingLane / UpdateLane / ShowEmpty / SyncXAxes / theme helpers) are Lite's,
 * byte-for-byte. Rewires from Lite:
 *   - LocalDataService (DuckDB) -> ViewerDataService (Npgsql); reads take explicit naive-UTC windows
 *     (serverId, startUtc[, endUtc]) instead of Lite's (serverId, hoursBack, fromDate, toDate), and run
 *     genuinely async (no Task.Run wrap — Npgsql is async), matching the W1a/W1c viewer ports.
 *   - PerformanceMonitorLite.Analysis BaselineBucket + MetricNames -> the structurally-identical
 *     PerformanceMonitor.Darling.Analysis twins (PgBaselineProvider.cs).
 *   - AppLogger -> Debug.WriteLine.
 *   - Time axis: every lane plots collection_time (naive-UTC) as the UTC instant (#4766). The viewer's
 *     mode-aware Server/Local/UTC zone (ViewerTimeHelper.CurrentDisplayZone) only draws the tick labels and
 *     the crosshair label (Server mode uses the collected server time zone or
 *     server_properties.utc_offset_minutes, matching Lite's per-server ServerTimeHelper shift).
 *     The CPU lane's sample_time is the monitored server's LOCAL wall clock, so GetCpuUtilizationAsync
 *     de-skews it to naive UTC in SQL before it is plotted too (#1262: per-batch offset =
 *     round(MAX(sample_time) over the batch - collection_time) to 15 min, recovered from the batch — a
 *     data correction independent of the display mode). The CPU lane therefore aligns with the
 *     collection_time lanes on any server timezone; a UTC server is unchanged.
 * Deliberately dropped for v1 (noted in the PR): Lite's comparison-range ghost-line overlay (the whole
 * comparisonRange block, AddGhostLine, ComparisonLabel). The "Show Active Queries at This Time"
 * drill-down event is wired by the host ServerTab — ViewerServerTab.xaml.cs subscribes
 * ShowActiveQueriesRequested to OnActiveQueriesDrillDown, which navigates to the Active Queries surface.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

public partial class CorrelatedTimelineLanesControl : UserControl
{
    private ViewerDataService? _dataService;
    private int _serverId;
    private CorrelatedCrosshairManager? _crosshairManager;
    private bool _isRefreshing;

    public CorrelatedTimelineLanesControl()
    {
        InitializeComponent();
        // #6652: the lanes scroll at small sizes; the wheel over a lane stays that lane's zoom.
        PerformanceMonitor.Ui.ChartLayout.KeepWheelOnPlot(CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart);
        /* #8104: the lanes grid's Height follows the scroll viewer's viewport, and WPF clamps an explicit Height only by the
           element's own MinHeight, whatever its rows ask for. The floor is therefore the rows' total, the rule the shared
           ChartLayout guard applies to the chart tabs' grids, so the scroll viewer's extent holds every lane and scrolls when
           the viewport is shorter, instead of clipping the last lane under the window with no scrollbar. */
        LanesGrid.MinHeight = LanesGrid.RowDefinitions.Sum(r => r.MinHeight);
        /* No Unloaded → Dispose() handler: WPF fires Unloaded for transient
           reasons (tab virtualization, layout rebuilds) and Dispose() clears
           the crosshair manager's lane list, permanently breaking the crosshair
           until the ServerTab is rebuilt. The manager holds only managed state
           (a Popup + lane references) — letting GC clean it up with the control
           is fine. */
    }

    /// <summary>
    /// Initializes the control with the data service and server ID.
    /// Must be called before RefreshAsync.
    /// </summary>
    public void Initialize(ViewerDataService dataService, int serverId)
    {
        _dataService = dataService;
        _serverId = serverId;

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        var laneNames = new[] { "SQL CPU", "Wait Stats", "Blocking", "Buffer Pool", "I/O Latency" };
        var emptyLook = new ChartRenderHelper();
        for (var lane = 0; lane < charts.Length; lane++)
        {
            var chart = charts[lane];
            ApplyTheme(chart);
            /* #8086: until the first read comes back a lane shows the empty chart's sentence, not ScottPlot's default -10 to 10
               axes. The first update resets the chart, which drops this. */
            emptyLook.RefreshEmptyChart(chart, laneNames[lane], "");
            // Disable zoom/pan/drag but keep mouse events for crosshair
            chart.UserInputProcessor.UserActionResponses.Clear();
            SetupLaneDrillDown(chart);
        }

        _crosshairManager = new CorrelatedCrosshairManager { DisplayZoneProvider = ViewerTimeHelper.CurrentDisplayZone };
        _crosshairManager.AddLane(CpuChart, "SQL CPU", "%");
        _crosshairManager.AddLane(WaitStatsChart, "Wait Stats", "ms/sec");
        _crosshairManager.AddLane(BlockingChart, "Blocking", PerformanceMonitor.Ui.BlockingTrendLabels.ReportsUnit);
        _crosshairManager.AddLane(MemoryChart, "Buffer Pool", "MB");
        _crosshairManager.AddLane(FileIoChart, "I/O Latency", "ms");
    }

    /// <summary>
    /// Raised when the user picks "Show Active Queries at This Time" on a lane. The argument is the
    /// clicked time, a naive-UTC instant (the lanes' X axis is UTC). The host ServerTab subscribes this
    /// (ViewerServerTab.xaml.cs) to OnActiveQueriesDrillDown, which navigates to the Active Queries surface.
    /// </summary>
    public event Action<DateTime>? ShowActiveQueriesRequested;

    /// <summary>
    /// Adds a minimal right-click menu (just the Active Queries drill-down) to a lane. The lanes are a
    /// stripped-chrome view with pan/zoom disabled, so the clicked time is read straight from the X axis.
    /// </summary>
    private void SetupLaneDrillDown(ScottPlot.WPF.WpfPlot chart)
    {
        var menu = new ContextMenu();
        var item = new MenuItem { Header = "Show _Active Queries at This Time" };
        menu.Items.Add(item);

        menu.Opened += (s, _) =>
        {
            try
            {
                var pos = System.Windows.Input.Mouse.GetPosition(chart);
                var dpi = System.Windows.Media.VisualTreeHelper.GetDpi(chart);
                var pixel = new ScottPlot.Pixel((float)(pos.X * dpi.DpiScaleX), (float)(pos.Y * dpi.DpiScaleY));
                var t = DateTime.FromOADate(chart.Plot.GetCoordinates(pixel).X);
                // Empty-state lanes set the X axis to [-1, 1] (~year 1899); only offer the drill-down
                // when the click resolves to a real timestamp.
                bool valid = t.Year >= 2000;
                item.Tag = valid ? t : (DateTime?)null;
                item.IsEnabled = valid;
            }
            catch
            {
                item.Tag = null;
                item.IsEnabled = false;
            }
        };

        item.Click += (s, _) =>
        {
            if (item.Tag is DateTime t)
                ShowActiveQueriesRequested?.Invoke(t);
        };

        chart.PreviewMouseRightButtonDown += (s, e) =>
        {
            e.Handled = true;
            menu.PlacementTarget = chart;
            menu.Placement = System.Windows.Controls.Primitives.PlacementMode.MousePoint;
            menu.IsOpen = true;
        };
    }

    /// <summary>
    /// Refreshes all lane data for the given time range. The Overview passes the toolbar's window —
    /// hours-back for a preset, or the custom From/To when one is picked
    /// (<c>ViewerServerTab.LoadOverviewChartsAsync</c>), so the widest range the picker offers reaches these
    /// reads. Reads run against explicit naive-UTC windows derived here.
    ///
    /// <para>Ten reads go out at once against a ten-permit pool, which is why the fan-out width is declared
    /// below: at a 30-day range these were measured at 24.4-64.1 s EACH, and a deadline derived from one read
    /// in isolation is not a bound on that (#3004).</para>
    ///
    /// <para>A read that fails costs its lane, which is drawn as failed (<see cref="ShowFailed"/>), and the other lanes draw as
    /// usual. The return value names the reads that failed (<see cref="OverviewLaneReadFailures.Summary"/>), so the host does not
    /// call the tab refreshed; null when all ten completed.</para>
    /// </summary>
    public async Task<string?> RefreshAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (_dataService == null || _isRefreshing) return null;
        _isRefreshing = true;

        try
        {
            _crosshairManager?.PrepareForRefresh();

            /* Explicit naive-UTC window bounds for the viewer's (serverId, startUtc, endUtc) reads. */
            var startUtc = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);
            var endUtc = toDate ?? DateTime.UtcNow;

            /* Reads run concurrently — NpgsqlDataSource pools a connection for each; genuinely async so
               no Task.Run wrap (matches the W1a/W1c viewer ports). CPU reuses W1a's raw-sample read;
               blocking/deadlock/file-IO reuse W1c's trend reads; total-wait + memory are new (W1d). */
            /* Ten reads, one pool of ten permits: each one contends with the other nine, so each one
               needs the deadline a ten-wide batch was measured to cost rather than a solo read's.
               Declared here, before the first task, so every one of the ten inherits it. */
            using var readFanOut = ViewerReadFanOut.Of(10);

            var cpuTask = _dataService.GetCpuUtilizationAsync(_serverId, startUtc, endUtc);
            var waitTask = _dataService.GetTotalWaitTrendAsync(_serverId, startUtc, endUtc);
            var blockingTask = _dataService.GetBlockingTrendAsync(_serverId, startUtc, endUtc);
            var deadlockTask = _dataService.GetDeadlockTrendAsync(_serverId, startUtc, endUtc);
            var memoryTask = _dataService.GetMemoryTrendAsync(_serverId, startUtc, endUtc);
            var fileIoTask = _dataService.GetFileIoOverviewLatencyTrendAsync(_serverId, startUtc, endUtc);

            // Fetch baselines for band rendering — chart-unit-matched metrics. #7644: each lane's band is one step per local
            // hour of the window (the web Overview's shape), and each point is judged against its own hour's step.
            var cpuBaselineTask = _dataService.GetBaselineStepsForLaneAsync(_serverId, MetricNames.Cpu, startUtc, endUtc);
            var waitBaselineTask = _dataService.GetBaselineStepsForLaneAsync(_serverId, MetricNames.WaitMsPerSec, startUtc, endUtc);
            var ioBaselineTask = _dataService.GetBaselineStepsForLaneAsync(_serverId, MetricNames.IoLatencyChart, startUtc, endUtc);
            var blockingBaselineTask = _dataService.GetBaselineStepsForLaneAsync(_serverId, MetricNames.BlockingPerMinute, startUtc, endUtc);

            try
            {
                await Task.WhenAll(cpuTask, waitTask, blockingTask, deadlockTask, memoryTask, fileIoTask,
                    cpuBaselineTask, waitBaselineTask, ioBaselineTask, blockingBaselineTask);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"CorrelatedLanes data fetch failed: {ex.Message}");
            }

            var cpuBaseline = cpuBaselineTask.IsCompletedSuccessfully ? cpuBaselineTask.Result : null;
            var waitBaseline = waitBaselineTask.IsCompletedSuccessfully ? waitBaselineTask.Result : null;
            var ioBaseline = ioBaselineTask.IsCompletedSuccessfully ? ioBaselineTask.Result : null;
            var blockingBaseline = blockingBaselineTask.IsCompletedSuccessfully ? blockingBaselineTask.Result : null;

            // minAnomalyValue: absolute floor below which dots/arrows are suppressed even if outside band.
            // Prevents "1% CPU above 0.5% baseline" false alarms on idle servers.
            if (cpuTask.IsCompletedSuccessfully)
            {
                /* Total non-idle CPU = SQL + other-process (Lite's CpuUtilizationData.TotalCpu). The
                   viewer's raw read exposes the two components; sum them here for the Total series. */
                var sqlSeries = cpuTask.Result.Select(d => (d.SampleTime.ToOADate(), (double)d.SqlServerCpu)).ToList();
                /* #7682: a bucket without other-process CPU (SQL Server on Linux before 2025 CU1) has no total. Plot the
                   buckets that have one; with none, the SQL line is all that is known. */
                var measuredTotals = cpuTask.Result.Where(d => d.TotalCpu.HasValue).ToList();
                var totalSeries = measuredTotals.Count == 0
                    ? sqlSeries
                    : measuredTotals.Select(d => (d.SampleTime.ToOADate(), d.TotalCpu!.Value)).ToList();
                UpdateCpuLane(sqlSeries, totalSeries, cpuBaseline);
            }
            else
                ShowFailed(CpuChart, "CPU %");

            if (waitTask.IsCompletedSuccessfully)
                UpdateLane(WaitStatsChart, "Wait ms/sec",
                    waitTask.Result.Select(d => (d.CollectionTime.ToOADate(), d.WaitTimeMsPerSecond)).ToList(),
                    "#FFB74D", baseline: waitBaseline, minAnomalyValue: 100);
            else
                ShowFailed(WaitStatsChart, "Wait ms/sec");

            {
                var blockingData = blockingTask.IsCompletedSuccessfully
                    ? blockingTask.Result.Select(d => (d.Time.ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                var deadlockData = deadlockTask.IsCompletedSuccessfully
                    ? deadlockTask.Result.Select(d => (d.Time.ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                var blockingUnit = blockingTask.IsCompletedSuccessfully
                    ? PerformanceMonitor.Ui.BlockingTrendLabels.UnitFor(blockingTask.Result.Select(d => d.Source))
                    : PerformanceMonitor.Ui.BlockingTrendLabels.ReportsUnit;
                UpdateBlockingLane(blockingData, deadlockData, blockingBaseline, (endUtc - startUtc).TotalDays, blockingUnit);
                /* The lane draws the series that read; a series that did not is not "no events", so the lane says so. */
                if (!blockingTask.IsCompletedSuccessfully || !deadlockTask.IsCompletedSuccessfully)
                    MarkFailed(BlockingChart);
            }

            if (memoryTask.IsCompletedSuccessfully)
                UpdateLane(MemoryChart, "Buffer Pool MB",
                    memoryTask.Result.Select(d => (d.CollectionTime.ToOADate(), d.BufferPoolMb)).ToList(),
                    "#CE93D8");
            else
                ShowFailed(MemoryChart, "Memory MB");

            if (fileIoTask.IsCompletedSuccessfully)
            {
                /* #6970: the read already returns one weighted point per time, not one per file. */
                var ioGrouped = fileIoTask.Result
                    .Select(d => (d.CollectionTime.ToOADate(), d.ReadLatencyMs))
                    .ToList();
                /* #7624: the lane marks a point only when its reads reach the web strip's bar for the span they were counted over. */
                var ioPoints = fileIoTask.Result;
                var (ioReads, ioMinReads) = OverviewIoReadFloor.LaneInputs(
                    ioPoints.Select(d => (d.CollectionTime, d.Reads, d.BucketMinutes)).ToList(),
                    PgTargetScorer.IoBaselineBucketMinimumReads);
                UpdateLane(FileIoChart, "I/O ms", ioGrouped, "#81C784", baseline: ioBaseline, minAnomalyValue: 2,
                    pointReads: ioReads, minPointReads: ioMinReads);
            }
            else
                ShowFailed(FileIoChart, "I/O ms");

            /* VLines must be re-attached before SyncXAxes so they're part of
               the render set when the chart refreshes. */
            _crosshairManager?.ReattachVLines();
            SyncXAxes(hoursBack, fromDate, toDate);

            /* #4966: the blocking chart draws event counts, so an empty stretch reads as "nothing happened" and the chart says where
               its data starts. The note comes last, after the charts are drawn and synced, and on its own: the two coverage probes start only after the ten reads above have
               finished (they are not part of that WhenAll, whose ten-wide declaration still covers them), so they add no
               read to that batch, and a probe that fails costs the note and never the bars drawn above. */
            await ShowBlockingLaneDataStartAsync(
                startUtc, endUtc,
                blockingTask.IsCompletedSuccessfully ? blockingTask.Result.Select(d => d.Time) : [],
                deadlockTask.IsCompletedSuccessfully ? deadlockTask.Result.Select(d => d.Time) : []);

            /* The reads that failed, named in lane order for the host's status line; null when every lane read. A failed baseline
               read alone is logged and is not a failed refresh: its lane drew, and only the band is missing. */
            (string Read, Task Task)[] laneReads =
            [
                ("CPU %", cpuTask), ("Wait ms/sec", waitTask), ("Blocking", blockingTask), ("Deadlocks", deadlockTask),
                ("Buffer Pool MB", memoryTask), ("I/O ms", fileIoTask),
            ];
            (string Read, Task Task)[] baselineReads =
            [
                ("CPU baseline", cpuBaselineTask), ("Wait baseline", waitBaselineTask), ("I/O baseline", ioBaselineTask),
                ("Blocking baseline", blockingBaselineTask),
            ];
            if (OverviewLaneReadFailures.Summary(baselineReads) is { } failedBaselines)
                ViewerLogger.Info("CorrelatedLanes", failedBaselines);
            return OverviewLaneReadFailures.Status(laneReads, baselineReads);
        }
        finally
        {
            /* Safety net: if something threw between PrepareForRefresh() and the
               ReattachVLines() call above, VLines are still null. EnsureVLinesAttached
               creates them only for lanes where VLine is null, so it's idempotent. */
            _crosshairManager?.EnsureVLinesAttached();
            _isRefreshing = false;
        }
    }

    /// <summary>
    /// Raises or hides the blocking chart's "Showing since" note (#4966). The chart's two series, blocking reports and
    /// deadlocks, are event counts: an empty stretch before a series starts reads as "nothing happened". A window of 90
    /// minutes or less, which can never earn a note, starts no probe. The note goes through the server tab's shared step
    /// (<see cref="ViewerServerTab.UpdateTruncationBanner"/>), so it uses the charts' own clock, to the second.
    /// </summary>
    private async Task ShowBlockingLaneDataStartAsync(
        DateTime startUtc, DateTime endUtc, IEnumerable<DateTime> blockingBars, IEnumerable<DateTime> deadlockBars)
    {
        var probed = endUtc - startUtc > DurationTrendRouting.TruncationSlack;
        var blockingProbe = probed
            ? _dataService!.GetBlockingChartDataStartAsync(_serverId, startUtc, endUtc)
            : Task.FromResult<DateTime?>(null);
        var deadlockProbe = probed
            ? _dataService!.GetDeadlocksDataStartAsync(_serverId, startUtc, endUtc)
            : Task.FromResult<DateTime?>(null);

        var start = await ViewerBlockingLaneDataStart.ChooseAsync(blockingProbe, deadlockProbe, blockingBars, deadlockBars);
        ViewerServerTab.UpdateTruncationBanner(BlockingLaneDataStartBanner, start, startUtc);
    }

    internal void UpdateBlockingLane(List<(double Time, double Value)> blockingData,
        List<(double Time, double Value)> deadlockData, IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? baseline = null, double windowSpanDays = 0,
        string blockingUnit = PerformanceMonitor.Ui.BlockingTrendLabels.ReportsUnit)
    {
        ClearChart(BlockingChart);
        ApplyTheme(BlockingChart);

        // Register blocking and deadlock as separate named series for the tooltip
        var blockTimes = blockingData.Select(d => d.Time).ToArray();
        var blockValues = blockingData.Select(d => d.Value).ToArray();
        var deadTimes = deadlockData.Select(d => d.Time).ToArray();
        var deadValues = deadlockData.Select(d => d.Value).ToArray();

        // First series clears any previous data
        /* #7219: the blocking lane counts blocked-process reports, or DMV snapshot rows when the window holds no reports. */
        _crosshairManager?.SetLaneUnit(BlockingChart, blockingUnit);
        _crosshairManager?.SetLaneData(BlockingChart, blockTimes, blockValues, isEventBased: true,
            hoverToleranceDays: EventLaneGeometry.HoverToleranceDays(windowSpanDays));
        // Rename the auto-created series and add the second
        _crosshairManager?.AddLaneSeries(BlockingChart, "Deadlocks", "events",
            deadTimes, deadValues, isEventBased: true,
            hoverToleranceDays: EventLaneGeometry.HoverToleranceDays(windowSpanDays));

        if (blockingData.Count == 0 && deadlockData.Count == 0)
        {
            ShowEmpty(BlockingChart, "Block/Dead");
            return;
        }

        /* #6284: a bar follows the window it is drawn in, and the hover reaches as far as the bar does. A fixed 30-second
           bar is a sub-pixel sliver in a 24-hour window, and a hover that reached one minute read 0 over it. */
        double barWidth = EventLaneGeometry.BarWidthDays(windowSpanDays);
        double maxCount = 0;

        // Baseline band for blocking: added before the bars so it paints behind them (#7433)
        if (baseline is { Count: > 0 })
        {
            _crosshairManager?.SetLaneBaselineSteps(BlockingChart, baseline, isEventBased: true);

            PerformanceMonitor.Ui.BaselineBand.AddBand(BlockingChart.Plot, baseline, ChartPalette.AccentColor("BaselineBlocking"));
        }

        // Blocking bars — red
        if (blockingData.Count > 0)
        {
            var bars = blockingData.Select(d => new ScottPlot.Bar
            {
                Position = d.Time,
                Value = d.Value,
                Size = barWidth,
                FillColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking")),
                LineWidth = 0
            }).ToArray();
            BlockingChart.Plot.Add.Bars(bars);
            maxCount = Math.Max(maxCount, blockingData.Max(d => d.Value));
        }

        // Deadlock bars — slightly narrower so both are visible over the blocking bars
        if (deadlockData.Count > 0)
        {
            var bars = deadlockData.Select(d => new ScottPlot.Bar
            {
                Position = d.Time,
                Value = d.Value,
                Size = barWidth * 0.6,
                FillColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks")),
                LineWidth = 0
            }).ToArray();
            BlockingChart.Plot.Add.Bars(bars);
            maxCount = Math.Max(maxCount, deadlockData.Max(d => d.Value));
        }

        BlockingChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        BlockingChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;
        ReapplyAxisColors(BlockingChart);

        BlockingChart.Plot.Title("");
        BlockingChart.Plot.YLabel("");
        BlockingChart.Plot.Legend.IsVisible = false;
        BlockingChart.Plot.Axes.Margins(bottom: 0);
        BlockingChart.Plot.Axes.SetLimitsY(0, Math.Max(maxCount * 1.3, 2));

        BlockingChart.Refresh();
    }

    /* Two-series CPU lane: SQL CPU (blue) + Total non-idle CPU (orange).
       Total is what the alert evaluates by default (see CpuAlertMode), so
       plotting it directly avoids the "alert says 95% but chart shows 60%"
       confusion (PM #1004). Baseline band + anomaly markers stay on SQL
       because that's the metric the CPU baseline was computed from. */
    internal void UpdateCpuLane(
        List<(double Time, double Value)> sqlData,
        List<(double Time, double Value)> totalData,
        IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? baseline = null)
    {
        ClearChart(CpuChart);
        ApplyTheme(CpuChart);

        if (sqlData.Count == 0 && totalData.Count == 0)
        {
            ShowEmpty(CpuChart, "CPU %");
            return;
        }

        var sqlTimes = sqlData.Select(d => d.Time).ToArray();
        var sqlValues = sqlData.Select(d => d.Value).ToArray();
        var totalTimes = totalData.Select(d => d.Time).ToArray();
        var totalValues = totalData.Select(d => d.Value).ToArray();

        // Register SQL as the first (lane-default-named) series, Total as a named second series
        _crosshairManager?.SetLaneData(CpuChart, sqlTimes, sqlValues);
        _crosshairManager?.AddLaneSeries(CpuChart, "Total", "%", totalTimes, totalValues);

        // Baseline band — applies to SQL CPU (what the baseline was computed on)
        List<int>? cpuAnomalyIndices = null;
        if (baseline is { Count: > 0 })
        {
            _crosshairManager?.SetLaneBaselineSteps(CpuChart, baseline, 10);

            PerformanceMonitor.Ui.BaselineBand.AddBand(CpuChart.Plot, baseline, ChartPalette.AccentColor("BaselineCpu"));

            // Anomaly markers on SQL series only. #7644: each point is judged against its own hour's step, and a point in an hour with no step is not marked.
            var anomalyIndices = new List<int>();
            for (int i = 0; i < sqlValues.Length; i++)
            {
                var step = PerformanceMonitor.Ui.BaselineBand.HourStep.At(baseline, sqlTimes[i]);
                if (step is not null && ((sqlValues[i] > step.Upper && sqlValues[i] >= 10) || sqlValues[i] < step.Lower))
                    anomalyIndices.Add(i);
            }

            cpuAnomalyIndices = anomalyIndices;
        }

        if (totalValues.Length > 0)
        {
            var totalScatter = CpuChart.Plot.Add.TimeSeries(totalTimes, totalValues);
            totalScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("TotalCpu"));
            totalScatter.MarkerSize = 0;
            totalScatter.LineWidth = 1.5f;
            totalScatter.LegendText = "Total";
            totalScatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;
        }

        if (sqlValues.Length > 0)
        {
            var sqlScatter = CpuChart.Plot.Add.TimeSeries(sqlTimes, sqlValues);
            sqlScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlCpu"));
            sqlScatter.MarkerSize = 0;
            sqlScatter.LineWidth = 1.5f;
            sqlScatter.LegendText = "SQL";
            sqlScatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;
        }

        // #7678: the marks go AFTER the series lines so a run reads as the line's stretch redrawn thicker in the anomaly colour.
        if (cpuAnomalyIndices is not null)
            PerformanceMonitor.Ui.AnomalyMarks.Add(CpuChart.Plot, sqlTimes, sqlValues, cpuAnomalyIndices);

        CpuChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        if (CpuChart != FileIoChart)
            CpuChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        ReapplyAxisColors(CpuChart);

        CpuChart.Plot.Title("");
        CpuChart.Plot.YLabel("");
        CpuChart.Plot.Legend.IsVisible = false;
        CpuChart.Plot.Axes.Margins(bottom: 0);
        CpuChart.Plot.Axes.SetLimitsY(0, 105);

        CpuChart.Refresh();
    }

    internal void UpdateLane(ScottPlot.WPF.WpfPlot chart, string title,
        List<(double Time, double Value)> data, string colorHex,
        double? yMin = null, double? yMax = null, IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? baseline = null,
        double minAnomalyValue = 0, IReadOnlyList<long>? pointReads = null, long minPointReads = 0)
    {
        ClearChart(chart);
        ApplyTheme(chart);

        if (data.Count == 0)
        {
            ShowEmpty(chart, title);
            return;
        }

        var times = data.Select(d => d.Time).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        // Render baseline band FIRST (behind the data line)
        List<int>? laneAnomalyIndices = null;
        if (baseline is { Count: > 0 })
        {
            _crosshairManager?.SetLaneBaselineSteps(chart, baseline, minAnomalyValue);

            PerformanceMonitor.Ui.BaselineBand.AddBand(chart.Plot, baseline, colorHex);

            // Highlight anomalous points (outside ± 2σ band AND above absolute minimum). #7644: each point is judged against its own
            // hour's step, and a point in an hour with no step is not marked.
            var anomalyIndices = new List<int>();
            for (int i = 0; i < values.Length; i++)
            {
                var step = PerformanceMonitor.Ui.BaselineBand.HourStep.At(baseline, times[i]);
                if (step is null)
                    continue;
                /* #7624: a lane with a read floor (I/O) marks only a point whose reads reach it, as the web strip does. */
                var reads = pointReads != null && i < pointReads.Count ? pointReads[i] : 0;
                if (OverviewIoReadFloor.IsAnomaly(values[i], step.Upper, step.Lower, minAnomalyValue, reads, pointReads == null ? 0 : minPointReads))
                    anomalyIndices.Add(i);
            }

            laneAnomalyIndices = anomalyIndices;
        }

        var scatter = chart.Plot.Add.TimeSeries(times, values);
        scatter.Color = ScottPlot.Color.FromHex(colorHex);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LegendText = title;
        scatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;

        // #7678: the marks go AFTER the series line so a run reads as the line's stretch redrawn thicker in the anomaly colour.
        if (laneAnomalyIndices is not null)
            PerformanceMonitor.Ui.AnomalyMarks.Add(chart.Plot, times, values, laneAnomalyIndices);

        _crosshairManager?.SetLaneData(chart, times, values,
            reads: pointReads?.ToArray(), minReads: pointReads == null ? 0 : minPointReads);

        chart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        // Hide bottom tick labels on all lanes except the last (File I/O)
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        ReapplyAxisColors(chart);

        // Compact layout: hide Y label, minimize title, no legend
        chart.Plot.Title("");
        chart.Plot.YLabel("");
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);

        if (yMin.HasValue && yMax.HasValue)
            chart.Plot.Axes.SetLimitsY(yMin.Value, yMax.Value);
        else
        {
            var maxVal = data.Max(d => d.Value);
            var minVal = data.Min(d => d.Value);
            var padding = Math.Max((maxVal - minVal) * 0.1, 1);
            chart.Plot.Axes.SetLimitsY(Math.Max(0, minVal - padding), maxVal + padding);
        }

        chart.Refresh();
    }

    /// <summary>
    /// #4766: the X-axis window every lane is pinned to. The lanes plot the UTC instant, so the window is UTC as is:
    /// a custom range's bounds are UTC instants, and a preset range is hoursBack REAL hours ending at
    /// <paramref name="utcNow"/> (no wall-clock arithmetic in the display zone, so a clock change inside the window
    /// cannot move the start). utcNow is an explicit parameter so a test can pin the clock.
    /// </summary>
    internal static (DateTime Start, DateTime End) GetXAxisWindow(
        int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime utcNow)
    {
        var custom = fromDate.HasValue && toDate.HasValue;
        var startUtc = custom ? fromDate!.Value : utcNow.AddHours(-hoursBack);
        var endUtc = custom ? toDate!.Value : utcNow;
        return (startUtc, endUtc);
    }

    /// <summary>
    /// Makes the lanes share a time axis in BOTH senses: identical X-axis limits, and - since #2533 -
    /// identical left-hand pixel geometry, so the same instant sits at the same X pixel in all five.
    /// Limits alone were never enough; each plot still sized its own gutter from its own Y tick labels.
    /// </summary>
    /// <summary>When the server was first collected (#7386), set by the server tab from its own read. The lanes' shared X axis does
    /// not start before it. Null leaves the axis as picked.</summary>
    internal DateTime? ServerDataStartUtc { get; set; }

    internal void SyncXAxes(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (xStart, xEnd) = GetXAxisWindow(hoursBack, fromDate, toDate, DateTime.UtcNow);

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };

        /* #7386: all five lanes get the same start, so they keep one ruler. It is never later than the earliest point any lane
           drew, whichever relation fed that lane. */
        double xMin = ChartAxisStart.XStart(xStart, xEnd, ServerDataStartUtc, charts.Select(c => c.Plot).ToArray()).ToOADate();
        double xMax = xEnd.ToOADate();

        foreach (var chart in charts)
            chart.Plot.Axes.SetLimitsX(xMin, xMax);

        /* #2533: every lane's Y limits and X limits are final by now, so this is the one place that can
           see all five gutters at once and hand them the widest. It has to run on EVERY refresh, not
           once at Initialize: ClearChart() calls WpfPlot.Reset(), which swaps in a fresh Plot and takes
           any axis floor with it. */
        var gutter = LaneAxisAligner.AlignLeftGutters(charts);
        PlaceCaptions(gutter);

        foreach (var chart in charts)
            chart.Refresh();
    }

    private static void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Reset();
        PerformanceMonitor.Ui.ChartDisplayScale.Apply(chart); // #6678: Reset swaps in a plot at scale 1
        chart.Plot.Clear();
    }

    /* Render an empty lane as a live, gridded chart instead of a dead black box. This is most often
       the Blocking/Deadlocking lane on a healthy server (no events = good news), so make it match the
       populated lanes: keep the grid and show a 0-1 Y axis. We no longer blank the axes or add the old
       "No Data" text (which SyncXAxes pushed off-screen anyway). X limits and the vertical gridlines are
       set afterward by SyncXAxes so the time axis aligns with the other lanes; the left axis keeps its
       default numeric ticks, so 0/1 labels and horizontal gridlines render. The title arg is retained
       for call-site readability. */
    private void ShowEmpty(ScottPlot.WPF.WpfPlot chart, string title)
    {
        chart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        // Only the bottom (File I/O) lane shows time labels; the upper lanes hide them (matches UpdateLane).
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        ReapplyAxisColors(chart);

        chart.Plot.Title("");
        chart.Plot.YLabel("");
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);
        chart.Plot.Axes.SetLimitsY(0, 1);

        chart.Refresh();
    }

    /* A lane whose read failed is drawn as the empty lane is, then says so over it, so it cannot pass for a quiet healthy lane. */
    private void ShowFailed(ScottPlot.WPF.WpfPlot chart, string title)
    {
        ShowEmpty(chart, title);
        MarkFailed(chart);
    }

    /* The note is anchored to the lane's pixels, not its axes, so SyncXAxes leaves it where it is. */
    private static void MarkFailed(ScottPlot.WPF.WpfPlot chart)
    {
        var note = chart.Plot.Add.Annotation("read failed", ScottPlot.Alignment.MiddleCenter);
        note.LabelFontColor = ChartStyle.GetThemeColors().Text;
        note.LabelFontSize = 11;
        note.LabelBackgroundColor = ScottPlot.Colors.Transparent;
        note.LabelBorderColor = ScottPlot.Colors.Transparent;
        note.LabelShadowColor = ScottPlot.Colors.Transparent;
        chart.Refresh();
    }

    /// <summary>
    /// Reapplies theme to all lane charts (call on theme change).
    /// </summary>
    public void ReapplyTheme()
    {
        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            ApplyTheme(chart);
            chart.Refresh();
        }
    }

    /// <summary>
    /// #8162: each lane's label is a caption at the top left of its lane, over the plot and clear of the Y tick labels, so the
    /// lanes need no gutter of their own. The tick labels' width is the lanes' shared left gutter (<paramref name="gutter"/>, in
    /// the same units the layout uses: at a display scale above 100% it is not a device pixel count, and dividing it by the scale put "CPU %" over the top Y label), and the caption starts just past it.
    /// </summary>
    private void PlaceCaptions(double gutter)
    {
        var margin = new Thickness(gutter + 6, 3, 0, 0);
        foreach (var caption in new[] { CpuCaption, WaitStatsCaption, BlockingCaption, MemoryCaption, FileIoCaption })
            caption.Margin = margin;
    }

    /// <summary>#8162: the height a lane that shows no X labels keeps under its plot: its tick marks.</summary>
    private const float LaneTickRoom = 6f;

    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart)
    {
        // Lanes are a deliberately stripped chrome variant (legend hidden, no font-size / first-render
        // hook). Colors come from the shared ChartStyle so the per-theme hexes can't drift.
        var c = ChartStyle.GetThemeColors();
        chart.Plot.FigureBackground.Color = c.FigureBackground;
        chart.Plot.DataBackground.Color = c.DataBackground;
        chart.Plot.Axes.Color(c.Text);
        chart.Plot.Grid.MajorLineColor = c.Grid;
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = c.Text;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = c.Text;
        chart.Plot.Axes.Bottom.Label.ForeColor = c.Text;
        chart.Plot.Axes.Left.Label.ForeColor = c.Text;

        chart.Background = new System.Windows.Media.SolidColorBrush(
            System.Windows.Media.Color.FromRgb(c.FigureBackground.R, c.FigureBackground.G, c.FigureBackground.B));
    }

    /* #8162: a lane's plot fills its lane. This runs after DateTimeTicksBottomUtc, which swaps in a new bottom axis and takes any
       size limit with it. The lane label is a caption over the lane, so no top axis sits above the plot, and a lane that shows no
       X labels keeps no room for them (ScottPlot reserves the label height even when the labels are hidden, which left a band
       under every lane). The last lane keeps the room its labels need. */
    private void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Plot.Axes.Top.IsVisible = false;
        chart.Plot.Axes.Bottom.MaximumSize = chart == FileIoChart ? float.PositiveInfinity : LaneTickRoom;
        var text = ChartStyle.GetThemeColors().Text;
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Bottom.Label.ForeColor = text;
        chart.Plot.Axes.Left.Label.ForeColor = text;
    }
}
