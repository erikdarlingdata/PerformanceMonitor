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
 *   - Time axis: every lane plots collection_time (naive-UTC) through ViewerTimeHelper.ForDisplay, the
 *     viewer's mode-aware Server/Local/UTC conversion (Server mode adds the collected
 *     server_properties.utc_offset_minutes, matching Lite's per-server ServerTimeHelper shift).
 *     The CPU lane's sample_time is the monitored server's LOCAL wall clock, so GetCpuUtilizationAsync
 *     de-skews it to naive UTC in SQL before it too goes through ForDisplay (#1262: per-batch offset =
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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
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
        foreach (var chart in charts)
        {
            ApplyTheme(chart);
            // Disable zoom/pan/drag but keep mouse events for crosshair
            chart.UserInputProcessor.UserActionResponses.Clear();
            SetupLaneDrillDown(chart);
        }

        _crosshairManager = new CorrelatedCrosshairManager();
        _crosshairManager.AddLane(CpuChart, "SQL CPU", "%");
        _crosshairManager.AddLane(WaitStatsChart, "Wait Stats", "ms/sec");
        _crosshairManager.AddLane(BlockingChart, "Blocking", "events");
        _crosshairManager.AddLane(MemoryChart, "Buffer Pool", "MB");
        _crosshairManager.AddLane(FileIoChart, "I/O Latency", "ms");
    }

    /// <summary>
    /// Raised when the user picks "Show Active Queries at This Time" on a lane. The argument is the
    /// clicked time in the lanes' (viewer-local) X-axis space. The host ServerTab subscribes this
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
    /// Refreshes all lane data for the given time range. The viewer's Overview always passes a fixed
    /// 24-hour window (hoursBack = 24, fromDate/toDate null); the fromDate/toDate branch is kept for a
    /// future custom-range picker. Reads run against explicit naive-UTC windows derived here.
    /// </summary>
    public async Task RefreshAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (_dataService == null || _isRefreshing) return;
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
            var cpuTask = _dataService.GetCpuUtilizationAsync(_serverId, startUtc);
            var waitTask = _dataService.GetTotalWaitTrendAsync(_serverId, startUtc, endUtc);
            var blockingTask = _dataService.GetBlockingTrendAsync(_serverId, startUtc, endUtc);
            var deadlockTask = _dataService.GetDeadlockTrendAsync(_serverId, startUtc, endUtc);
            var memoryTask = _dataService.GetMemoryTrendAsync(_serverId, startUtc, endUtc);
            var fileIoTask = _dataService.GetFileIoLatencyTrendAsync(_serverId, startUtc, endUtc);

            // Fetch baselines for band rendering — chart-unit-matched metrics. referenceTime stays UTC
            // (the baseline buckets key on hour-of-day / day-of-week of the naive-UTC store timestamps).
            var referenceTime = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);
            var cpuBaselineTask = _dataService.GetBaselineForLaneAsync(_serverId, MetricNames.Cpu, referenceTime);
            var waitBaselineTask = _dataService.GetBaselineForLaneAsync(_serverId, MetricNames.WaitMsPerSec, referenceTime);
            var ioBaselineTask = _dataService.GetBaselineForLaneAsync(_serverId, MetricNames.IoLatency, referenceTime);
            var blockingBaselineTask = _dataService.GetBaselineForLaneAsync(_serverId, MetricNames.BlockingPerMinute, referenceTime);

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
                var sqlSeries = cpuTask.Result.Select(d => (ViewerTimeHelper.ForDisplay(d.SampleTime).ToOADate(), (double)d.SqlServerCpu)).ToList();
                var totalSeries = cpuTask.Result.Select(d => (ViewerTimeHelper.ForDisplay(d.SampleTime).ToOADate(), (double)(d.SqlServerCpu + d.OtherProcessCpu))).ToList();
                UpdateCpuLane(sqlSeries, totalSeries, cpuBaseline);
            }
            else
                ShowEmpty(CpuChart, "CPU %");

            if (waitTask.IsCompletedSuccessfully)
                UpdateLane(WaitStatsChart, "Wait ms/sec",
                    waitTask.Result.Select(d => (ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate(), d.WaitTimeMsPerSecond)).ToList(),
                    "#FFB74D", baseline: waitBaseline, minAnomalyValue: 100);
            else
                ShowEmpty(WaitStatsChart, "Wait ms/sec");

            {
                var blockingData = blockingTask.IsCompletedSuccessfully
                    ? blockingTask.Result.Select(d => (ViewerTimeHelper.ForDisplay(d.Time).ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                var deadlockData = deadlockTask.IsCompletedSuccessfully
                    ? deadlockTask.Result.Select(d => (ViewerTimeHelper.ForDisplay(d.Time).ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                UpdateBlockingLane(blockingData, deadlockData, blockingBaseline);
            }

            if (memoryTask.IsCompletedSuccessfully)
                UpdateLane(MemoryChart, "Buffer Pool MB",
                    memoryTask.Result.Select(d => (ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate(), d.BufferPoolMb)).ToList(),
                    "#CE93D8");
            else
                ShowEmpty(MemoryChart, "Memory MB");

            if (fileIoTask.IsCompletedSuccessfully)
            {
                var ioGrouped = fileIoTask.Result
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => (ViewerTimeHelper.ForDisplay(g.Key).ToOADate(), g.Average(x => x.AvgReadLatencyMs)))
                    .ToList();
                UpdateLane(FileIoChart, "I/O ms", ioGrouped, "#81C784", baseline: ioBaseline, minAnomalyValue: 2);
            }
            else
                ShowEmpty(FileIoChart, "I/O ms");

            /* VLines must be re-attached before SyncXAxes so they're part of
               the render set when the chart refreshes. */
            _crosshairManager?.ReattachVLines();
            SyncXAxes(hoursBack, fromDate, toDate);
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

    private void UpdateBlockingLane(List<(double Time, double Value)> blockingData,
        List<(double Time, double Value)> deadlockData, BaselineBucket? baseline = null)
    {
        ClearChart(BlockingChart);
        ApplyTheme(BlockingChart);

        // Register blocking and deadlock as separate named series for the tooltip
        var blockTimes = blockingData.Select(d => d.Time).ToArray();
        var blockValues = blockingData.Select(d => d.Value).ToArray();
        var deadTimes = deadlockData.Select(d => d.Time).ToArray();
        var deadValues = deadlockData.Select(d => d.Value).ToArray();

        // First series clears any previous data
        _crosshairManager?.SetLaneData(BlockingChart, blockTimes, blockValues, isEventBased: true);
        // Rename the auto-created series and add the second
        _crosshairManager?.AddLaneSeries(BlockingChart, "Deadlocks", "events",
            deadTimes, deadValues, isEventBased: true);

        if (blockingData.Count == 0 && deadlockData.Count == 0)
        {
            ShowEmpty(BlockingChart, "Block/Dead");
            return;
        }

        double barWidth = 30.0 / 86400.0;
        double maxCount = 0;

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

        // Baseline band for blocking
        if (baseline != null && baseline.SampleCount > 0 && baseline.EffectiveStdDev > 0)
        {
            var upper = baseline.Mean + 2 * baseline.EffectiveStdDev;
            var lower = Math.Max(0, baseline.Mean - 2 * baseline.EffectiveStdDev);

            _crosshairManager?.SetLaneBaseline(BlockingChart, lower, upper, isEventBased: true);

            var band = BlockingChart.Plot.Add.HorizontalSpan(lower, upper);
            band.FillStyle.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("BaselineBlocking")).WithAlpha(25);
            band.LineStyle.Width = 0;

            var meanLine = BlockingChart.Plot.Add.HorizontalLine(baseline.Mean);
            meanLine.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("BaselineBlocking")).WithAlpha(60);
            meanLine.LinePattern = ScottPlot.LinePattern.Dashed;
            meanLine.LineWidth = 1;
        }

        BlockingChart.Plot.Axes.DateTimeTicksBottomDateChange();
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
    private void UpdateCpuLane(
        List<(double Time, double Value)> sqlData,
        List<(double Time, double Value)> totalData,
        BaselineBucket? baseline = null)
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
        if (baseline != null && baseline.SampleCount > 0 && baseline.EffectiveStdDev > 0)
        {
            var upper = baseline.Mean + 2 * baseline.EffectiveStdDev;
            var lower = Math.Max(0, baseline.Mean - 2 * baseline.EffectiveStdDev);

            _crosshairManager?.SetLaneBaseline(CpuChart, lower, upper, 10);

            var band = CpuChart.Plot.Add.HorizontalSpan(lower, upper);
            band.FillStyle.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("BaselineCpu")).WithAlpha(25);
            band.LineStyle.Width = 0;

            var meanLine = CpuChart.Plot.Add.HorizontalLine(baseline.Mean);
            meanLine.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("BaselineCpu")).WithAlpha(60);
            meanLine.LinePattern = ScottPlot.LinePattern.Dashed;
            meanLine.LineWidth = 1;

            // Anomaly markers on SQL series only
            var anomalyIndices = new List<int>();
            for (int i = 0; i < sqlValues.Length; i++)
            {
                if ((sqlValues[i] > upper && sqlValues[i] >= 10) || sqlValues[i] < lower)
                    anomalyIndices.Add(i);
            }

            if (anomalyIndices.Count > 0)
            {
                var anomalyTimes = anomalyIndices.Select(i => sqlTimes[i]).ToArray();
                var anomalyVals = anomalyIndices.Select(i => sqlValues[i]).ToArray();
                var anomalyScatter = CpuChart.Plot.Add.TimeSeries(anomalyTimes, anomalyVals);
                anomalyScatter.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
                anomalyScatter.MarkerSize = 6;
                anomalyScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                anomalyScatter.LineWidth = 0;
            }
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

        CpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
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

    private void UpdateLane(ScottPlot.WPF.WpfPlot chart, string title,
        List<(double Time, double Value)> data, string colorHex,
        double? yMin = null, double? yMax = null, BaselineBucket? baseline = null,
        double minAnomalyValue = 0)
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
        if (baseline != null && baseline.SampleCount > 0 && baseline.EffectiveStdDev > 0)
        {
            var upper = baseline.Mean + 2 * baseline.EffectiveStdDev;
            var lower = Math.Max(0, baseline.Mean - 2 * baseline.EffectiveStdDev);

            _crosshairManager?.SetLaneBaseline(chart, lower, upper, minAnomalyValue);

            var band = chart.Plot.Add.HorizontalSpan(lower, upper);
            band.FillStyle.Color = ScottPlot.Color.FromHex(colorHex).WithAlpha(25);
            band.LineStyle.Width = 0;

            var meanLine = chart.Plot.Add.HorizontalLine(baseline.Mean);
            meanLine.Color = ScottPlot.Color.FromHex(colorHex).WithAlpha(60);
            meanLine.LinePattern = ScottPlot.LinePattern.Dashed;
            meanLine.LineWidth = 1;

            // Highlight anomalous points (outside ± 2σ band AND above absolute minimum)
            var anomalyIndices = new List<int>();
            for (int i = 0; i < values.Length; i++)
            {
                if ((values[i] > upper && values[i] >= minAnomalyValue) || values[i] < lower)
                    anomalyIndices.Add(i);
            }

            if (anomalyIndices.Count > 0)
            {
                var anomalyTimes = anomalyIndices.Select(i => times[i]).ToArray();
                var anomalyValues = anomalyIndices.Select(i => values[i]).ToArray();
                var anomalyScatter = chart.Plot.Add.TimeSeries(anomalyTimes, anomalyValues);
                anomalyScatter.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
                anomalyScatter.MarkerSize = 6;
                anomalyScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                anomalyScatter.LineWidth = 0;
            }
        }

        var scatter = chart.Plot.Add.TimeSeries(times, values);
        scatter.Color = ScottPlot.Color.FromHex(colorHex);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LegendText = title;
        scatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;

        _crosshairManager?.SetLaneData(chart, times, values);

        chart.Plot.Axes.DateTimeTicksBottomDateChange();
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
    /// Makes the lanes share a time axis in BOTH senses: identical X-axis limits, and - since #2533 -
    /// identical left-hand pixel geometry, so the same instant sits at the same X pixel in all five.
    /// Limits alone were never enough; each plot still sized its own gutter from its own Y tick labels.
    /// </summary>
    private void SyncXAxes(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        DateTime xStart, xEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            xStart = ViewerTimeHelper.ForDisplay(fromDate.Value);
            xEnd = ViewerTimeHelper.ForDisplay(toDate.Value);
        }
        else
        {
            xEnd = ViewerTimeHelper.ForDisplay(DateTime.UtcNow);
            xStart = xEnd.AddHours(-hoursBack);
        }

        double xMin = xStart.ToOADate();
        double xMax = xEnd.ToOADate();

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
            chart.Plot.Axes.SetLimitsX(xMin, xMax);

        /* #2533: every lane's Y limits and X limits are final by now, so this is the one place that can
           see all five gutters at once and hand them the widest. It has to run on EVERY refresh, not
           once at Initialize: ClearChart() calls WpfPlot.Reset(), which swaps in a fresh Plot and takes
           any axis floor with it. */
        LaneAxisAligner.AlignLeftGutters(charts);

        foreach (var chart in charts)
            chart.Refresh();
    }

    private static void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Reset();
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
        chart.Plot.Axes.DateTimeTicksBottomDateChange();
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

    private static void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart)
    {
        var text = ChartStyle.GetThemeColors().Text;
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Bottom.Label.ForeColor = text;
        chart.Plot.Axes.Left.Label.ForeColor = text;
    }
}
