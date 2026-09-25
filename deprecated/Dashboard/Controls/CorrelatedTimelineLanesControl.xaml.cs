/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Dashboard.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * SYNC WARNING: Lite has a matching copy at Lite/Controls/CorrelatedTimelineLanesControl.xaml.cs.
 * Changes here must be mirrored there.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorDashboard.Controls;

public partial class CorrelatedTimelineLanesControl : UserControl
{
    private DatabaseService? _dataService;
    private SqlServerBaselineProvider? _baselineProvider;
    private CorrelatedCrosshairManager? _crosshairManager;

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
    /// Initializes the control with the data service and optional baseline provider.
    /// Must be called before RefreshAsync.
    /// </summary>
    public void Initialize(DatabaseService dataService, SqlServerBaselineProvider? baselineProvider = null)
    {
        _dataService = dataService;
        _baselineProvider = baselineProvider;

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            TabHelpers.ApplyThemeToChart(chart);
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
    /// clicked time in the lanes' (server-local) X-axis space; the host navigates to Active Queries.
    /// </summary>
    public event Action<DateTime>? ShowActiveQueriesRequested;

    /// <summary>
    /// Adds a minimal right-click menu (just the Active Queries drill-down) to a lane. The lanes are a
    /// stripped-chrome view with pan/zoom disabled, so the clicked time is read straight from the X axis.
    /// </summary>
    private void SetupLaneDrillDown(ScottPlot.WPF.WpfPlot chart)
    {
        var menu = new ContextMenu();
        var item = new MenuItem { Header = "Show Active Queries at This Time" };
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
    /// #4305 (mirrors Lite's #4296): the server-local "current window" the ghost-line comparison is built
    /// from and aligned to. Under a custom range, fromDate/toDate already are server-local (ServerTab's
    /// pickers convert them via ServerTimeHelper.DisplayTimeToServerTime before calling RefreshAsync).
    /// Under a preset range (fromDate/toDate both null), this is the server's own local now --
    /// utcNow.AddMinutes(utcOffsetMinutes), i.e. ServerTimeHelper.ServerNow without reading that class's
    /// ambient static state directly -- NOT a raw UTC now: the correlated lanes' reads
    /// (GetCpuUtilizationAsync, GetTotalWaitStatsTrendAsync, etc.) treat a supplied fromDate/toDate as
    /// SERVER-LOCAL (their preset-range SQL branch filters on collection_time against SYSDATETIME(), not
    /// SYSUTCDATETIME()), so a UTC fallback shifted the reference window by the server's UTC offset on any
    /// server not on UTC. utcNow/utcOffsetMinutes are explicit parameters (not DateTime.UtcNow/
    /// ServerTimeHelper.UtcOffsetMinutes read directly) so a test can drive this deterministically for a
    /// server on either side of UTC, under a preset or a custom range.
    /// </summary>
    internal static (DateTime Start, DateTime End) GetCurrentWindowServerLocal(
        int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime utcNow, int utcOffsetMinutes)
    {
        var end = toDate ?? utcNow.AddMinutes(utcOffsetMinutes);
        var start = fromDate ?? end.AddHours(-hoursBack);
        return (start, end);
    }

    /// <summary>
    /// #4305 (mirrors Lite's #4296): the Server Trends tab's comparison range for the Resource Metrics
    /// correlated-lanes ghost-line overlay. #4313 reuses this same server-local window logic for
    /// ServerTab.xaml.cs's GetComparisonRange (Query Stats / Proc Stats / Query Store comparison grids)
    /// and for QueryPerformanceContent.Comparison.cs's RefreshComparisonAsync (via GetCurrentWindowServerLocal),
    /// so every comparison consumer -- Server Trends ghost line and the three grids -- now derives its
    /// current window from the same server-local basis instead of independently resampling a raw UTC now.
    /// CurrentFrom rides along in the return tuple so RefreshAsync's timeShift and ComparisonLabel
    /// reuse the SAME current-window start this built refFrom/refTo from, rather than resampling utcNow a
    /// second time (which, even on the corrected server-local basis, would not generally equal this call's
    /// utcNow and so would not produce an EXACT 1-day/7-day shift).
    /// </summary>
    internal static (DateTime From, DateTime To, DateTime CurrentFrom)? GetOverviewComparisonRange(
        int selectedIndex, int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime utcNow, int utcOffsetMinutes)
    {
        var (currentStart, currentEnd) = GetCurrentWindowServerLocal(hoursBack, fromDate, toDate, utcNow, utcOffsetMinutes);

        return selectedIndex switch
        {
            1 => (currentStart.AddDays(-1), currentEnd.AddDays(-1), currentStart),   // Yesterday
            2 => (currentStart.AddDays(-7), currentEnd.AddDays(-7), currentStart),   // Last week
            3 => (currentStart.AddDays(-7), currentEnd.AddDays(-7), currentStart),   // Same day last week
            _ => null
        };
    }

    /// <summary>
    /// Refreshes all lane data for the given time range.
    /// </summary>
    public async Task RefreshAsync(int hoursBack, DateTime? fromDate, DateTime? toDate,
        (DateTime From, DateTime To, DateTime CurrentFrom)? comparisonRange = null)
    {
        if (_dataService == null) return;

        _crosshairManager?.PrepareForRefresh();

        try
        {

        var cpuTask = _dataService.GetCpuUtilizationAsync(hoursBack, fromDate, toDate);
        var waitTask = _dataService.GetTotalWaitStatsTrendAsync(hoursBack, fromDate, toDate);
        var blockingTask = _dataService.GetBlockedSessionTrendAsync(hoursBack, fromDate, toDate);
        var deadlockTask = _dataService.GetDeadlockTrendAsync(hoursBack, fromDate, toDate);
        var memoryTask = _dataService.GetMemoryStatsAsync(hoursBack, fromDate, toDate);
        var fileIoTask = _dataService.GetFileIoLatencyTimeSeriesAsync(false, hoursBack, fromDate, toDate);

        // Fetch baselines for band rendering if provider is available. #4313: server-local window
        // start (GetCurrentWindowServerLocal's Start), not a UTC-anchored one -- GetBaselineAsync
        // buckets by analysisTime.Hour/DayOfWeek against collection_time (server-local, SYSDATETIME()),
        // so a UTC-basis referenceTime picked the wrong hour/day-of-week bucket on any server not on UTC.
        var referenceTime = GetCurrentWindowServerLocal(hoursBack, fromDate, toDate, DateTime.UtcNow, ServerTimeHelper.UtcOffsetMinutes).Start;
        Task<BaselineBucket?>? cpuBaselineTask = null;
        Task<BaselineBucket?>? waitBaselineTask = null;
        Task<BaselineBucket?>? ioBaselineTask = null;
        Task<BaselineBucket?>? blockingBaselineTask = null;
        Task<BaselineBucket?>? deadlockBaselineTask = null;

        if (_baselineProvider != null)
        {
            cpuBaselineTask = GetBaselineAsync(SqlServerMetricNames.Cpu, referenceTime);
            waitBaselineTask = GetBaselineAsync(SqlServerMetricNames.WaitStats, referenceTime);
            ioBaselineTask = GetBaselineAsync(SqlServerMetricNames.IoLatency, referenceTime);
            blockingBaselineTask = GetBaselineAsync(SqlServerMetricNames.Blocking, referenceTime);
            deadlockBaselineTask = GetBaselineAsync(SqlServerMetricNames.Deadlock, referenceTime);
        }

        try
        {
            var tasks = new List<Task> { cpuTask, waitTask, blockingTask, deadlockTask, memoryTask, fileIoTask };
            if (cpuBaselineTask != null) tasks.Add(cpuBaselineTask);
            if (waitBaselineTask != null) tasks.Add(waitBaselineTask);
            if (ioBaselineTask != null) tasks.Add(ioBaselineTask);
            if (blockingBaselineTask != null) tasks.Add(blockingBaselineTask);
            if (deadlockBaselineTask != null) tasks.Add(deadlockBaselineTask);
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"CorrelatedLanes: Data fetch failed: {ex.Message}");
        }

        var cpuBaseline = cpuBaselineTask is { IsCompletedSuccessfully: true } ? cpuBaselineTask.Result : null;
        var waitBaseline = waitBaselineTask is { IsCompletedSuccessfully: true } ? waitBaselineTask.Result : null;
        var ioBaseline = ioBaselineTask is { IsCompletedSuccessfully: true } ? ioBaselineTask.Result : null;
        var blockingBaseline = blockingBaselineTask is { IsCompletedSuccessfully: true } ? blockingBaselineTask.Result : null;
        var deadlockBaseline = deadlockBaselineTask is { IsCompletedSuccessfully: true } ? deadlockBaselineTask.Result : null;
        var blockingLaneBaseline = blockingBaseline ?? deadlockBaseline;

        // minAnomalyValue: absolute floor below which dots/arrows are suppressed even if outside band.
        // Prevents "1% CPU above 0.5% baseline" false alarms on idle servers.
        if (cpuTask.IsCompletedSuccessfully)
        {
            var ordered = cpuTask.Result.OrderBy(d => d.SampleTime).ToList();
            var sqlSeries = ordered.Select(d => (d.SampleTime.ToOADate(), (double)d.SqlServerCpuUtilization)).ToList();
            var totalSeries = ordered.Select(d => (d.SampleTime.ToOADate(), (double)d.TotalCpuUtilization)).ToList();
            UpdateCpuLane(sqlSeries, totalSeries, cpuBaseline);
        }
        else
            ShowEmpty(CpuChart, "CPU %");

        if (waitTask.IsCompletedSuccessfully)
            UpdateLane(WaitStatsChart, "Wait ms/sec",
                waitTask.Result.Select(d => (d.CollectionTime.ToOADate(), (double)d.WaitTimeMsPerSecond)).ToList(),
                "#FFB74D", baseline: waitBaseline, minAnomalyValue: 100);
        else
            ShowEmpty(WaitStatsChart, "Wait ms/sec");

        try
        {
            var blockingData = blockingTask.IsCompletedSuccessfully
                ? blockingTask.Result
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => (g.Key.ToOADate(), (double)g.Sum(x => x.BlockedCount)))
                    .ToList()
                : new List<(double, double)>();
            var deadlockData = deadlockTask.IsCompletedSuccessfully
                ? deadlockTask.Result
                    .Select(d => (d.CollectionTime.ToOADate(), (double)d.BlockedCount))
                    .ToList()
                : new List<(double, double)>();
            UpdateBlockingLane(blockingData, deadlockData, blockingLaneBaseline);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"CorrelatedLanes: Blocking lane failed: {ex}");
            ShowEmpty(BlockingChart, "Blocking & Deadlocking");
        }

        if (memoryTask.IsCompletedSuccessfully)
            UpdateLane(MemoryChart, "Buffer Pool MB",
                memoryTask.Result.Select(d => (d.CollectionTime.ToOADate(), (double)d.TotalMemoryMb)).ToList(),
                "#CE93D8");
        else
            ShowEmpty(MemoryChart, "Buffer Pool MB");

        if (fileIoTask.IsCompletedSuccessfully)
        {
            var ioGrouped = fileIoTask.Result
                .GroupBy(d => d.CollectionTime)
                .OrderBy(g => g.Key)
                .Select(g => (g.Key.ToOADate(), (double)g.Average(x => x.ReadLatencyMs)))
                .ToList();
            UpdateLane(FileIoChart, "I/O ms", ioGrouped, "#81C784", baseline: ioBaseline, minAnomalyValue: 2);
        }
        else
            ShowEmpty(FileIoChart, "I/O ms");

        // Comparison overlay — fetch reference period data and render as ghost lines
        if (comparisonRange.HasValue)
        {
            var refFrom = comparisonRange.Value.From;
            var refTo = comparisonRange.Value.To;
            // Time shift: offset to align reference data with current chart X axis. #4305: CurrentFrom
            // is the SAME server-local current-window start GetOverviewComparisonRange built refFrom
            // from, so this is exact arithmetic (currentStart - (currentStart - Ndays) = Ndays) rather
            // than a second, possibly UTC-basis, DateTime.UtcNow sample.
            var timeShift = comparisonRange.Value.CurrentFrom - refFrom;

            var refCpuTask = _dataService.GetCpuUtilizationAsync(0, refFrom, refTo);
            var refWaitTask = _dataService.GetTotalWaitStatsTrendAsync(0, refFrom, refTo);
            var refBlockingTask = _dataService.GetBlockedSessionTrendAsync(0, refFrom, refTo);
            var refMemoryTask = _dataService.GetMemoryStatsAsync(0, refFrom, refTo);
            var refIoTask = _dataService.GetFileIoLatencyTimeSeriesAsync(false, 0, refFrom, refTo);

            try { await Task.WhenAll(refCpuTask, refWaitTask, refBlockingTask, refMemoryTask, refIoTask); }
            catch (Exception ex) { Debug.WriteLine($"CorrelatedLanes: Comparison fetch failed: {ex.Message}"); }

            if (refCpuTask.IsCompletedSuccessfully)
                AddGhostLine(CpuChart, refCpuTask.Result
                    .Select(d => (d.SampleTime.Add(timeShift).ToOADate(), (double)d.TotalCpuUtilization)).ToList(), "#FF7043");

            if (refWaitTask.IsCompletedSuccessfully)
                AddGhostLine(WaitStatsChart, refWaitTask.Result
                    .Select(d => (d.CollectionTime.Add(timeShift).ToOADate(), (double)d.WaitTimeMsPerSecond)).ToList(), "#FFB74D");

            if (refBlockingTask.IsCompletedSuccessfully)
            {
                var refBlocking = refBlockingTask.Result
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => (g.Key.Add(timeShift).ToOADate(), (double)g.Sum(x => x.BlockedCount)))
                    .ToList();
                if (refBlocking.Count > 0)
                    AddGhostLine(BlockingChart, refBlocking, "#E57373");
            }

            if (refMemoryTask.IsCompletedSuccessfully)
                AddGhostLine(MemoryChart, refMemoryTask.Result
                    .Select(d => (d.CollectionTime.Add(timeShift).ToOADate(), (double)d.TotalMemoryMb)).ToList(), "#CE93D8");

            if (refIoTask.IsCompletedSuccessfully)
            {
                var refIo = refIoTask.Result
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => (g.Key.Add(timeShift).ToOADate(), (double)g.Average(x => x.ReadLatencyMs)))
                    .ToList();
                AddGhostLine(FileIoChart, refIo, "#81C784");
            }

            _crosshairManager?.SetComparisonLabel(ComparisonLabel(comparisonRange.Value));
        }

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
        }
    }

    /// <summary>
    /// Fetches a baseline bucket from the provider, wrapping in a nullable task.
    /// </summary>
    private async Task<BaselineBucket?> GetBaselineAsync(string metricName, DateTime referenceTime)
    {
        if (_baselineProvider == null) return null;
        try
        {
            var bucket = await _baselineProvider.GetBaselineAsync(metricName, referenceTime);
            return bucket.SampleCount > 0 ? bucket : null;
        }
        catch { return null; }
    }

    private void UpdateBlockingLane(List<(double Time, double Value)> blockingData,
        List<(double Time, double Value)> deadlockData, BaselineBucket? baseline = null)
    {
        ClearChart(BlockingChart);
        TabHelpers.ApplyThemeToChart(BlockingChart);

        var blockTimes = blockingData.Select(d => d.Time).ToArray();
        var blockValues = blockingData.Select(d => d.Value).ToArray();
        var deadTimes = deadlockData.Select(d => d.Time).ToArray();
        var deadValues = deadlockData.Select(d => d.Value).ToArray();

        _crosshairManager?.SetLaneData(BlockingChart, blockTimes, blockValues, isEventBased: true);
        _crosshairManager?.AddLaneSeries(BlockingChart, "Deadlocks", "events",
            deadTimes, deadValues, isEventBased: true);

        if (blockingData.Count == 0 && deadlockData.Count == 0)
        {
            ShowEmpty(BlockingChart, "Block/Dead");
            return;
        }

        double barWidth = 30.0 / 86400.0;
        double maxCount = 0;

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

        // Baseline for blocking — event-based metrics where zero is normal.
        // Even if EffectiveStdDev is 0 (all-zero baseline), still register the baseline
        // so the event-based indicator check (mean < 1 → any event is ▲) works.
        if (baseline != null && baseline.SampleCount > 0)
        {
            var effectiveStdDev = Math.Max(baseline.EffectiveStdDev, 0.01);
            var upper = baseline.Mean + 2 * effectiveStdDev;
            var lower = Math.Max(0, baseline.Mean - 2 * effectiveStdDev);

            _crosshairManager?.SetLaneBaseline(BlockingChart, lower, upper, isEventBased: true);

            // Only render the visual band if there's meaningful variance
            if (baseline.EffectiveStdDev > 0)
            {
                var band = BlockingChart.Plot.Add.HorizontalSpan(lower, upper);
                band.FillStyle.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("BaselineBlocking")).WithAlpha(25);
                band.LineStyle.Width = 0;

                var meanLine = BlockingChart.Plot.Add.HorizontalLine(baseline.Mean);
                meanLine.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("BaselineBlocking")).WithAlpha(60);
                meanLine.LinePattern = ScottPlot.LinePattern.Dashed;
                meanLine.LineWidth = 1;
            }
        }

        BlockingChart.Plot.Axes.DateTimeTicksBottomDateChange();
        BlockingChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;
        TabHelpers.ReapplyAxisColors(BlockingChart);

        BlockingChart.Plot.Title("");
        BlockingChart.Plot.YLabel("");
        BlockingChart.Plot.Legend.IsVisible = false;
        BlockingChart.Plot.Axes.Margins(bottom: 0);
        BlockingChart.Plot.Axes.SetLimitsY(0, Math.Max(maxCount * 1.3, 2));
    }

    /* Two-series CPU lane: SQL CPU (blue) + Total non-idle CPU (orange).
       Total is what the Lite alert evaluates by default (see CpuAlertMode in Lite/App.xaml.cs),
       so plotting it directly avoids the "alert says 95% but chart shows 60%" confusion (PM #1004).
       Baseline band + anomaly markers stay on SQL because that's the metric the CPU baseline
       was computed from. Dashboard does not yet have its own CpuAlertMode (deferred follow-up). */
    private void UpdateCpuLane(
        List<(double Time, double Value)> sqlData,
        List<(double Time, double Value)> totalData,
        BaselineBucket? baseline = null)
    {
        ClearChart(CpuChart);
        TabHelpers.ApplyThemeToChart(CpuChart);

        if (sqlData.Count == 0 && totalData.Count == 0)
        {
            ShowEmpty(CpuChart, "CPU %");
            return;
        }

        var sqlTimes = sqlData.Select(d => d.Time).ToArray();
        var sqlValues = sqlData.Select(d => d.Value).ToArray();
        var totalTimes = totalData.Select(d => d.Time).ToArray();
        var totalValues = totalData.Select(d => d.Value).ToArray();

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
                var anomalyScatter = CpuChart.Plot.Add.Scatter(anomalyTimes, anomalyVals);
                anomalyScatter.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
                anomalyScatter.MarkerSize = 6;
                anomalyScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                anomalyScatter.LineWidth = 0;
            }
        }

        if (totalValues.Length > 0)
        {
            var totalScatter = CpuChart.Plot.Add.Scatter(totalTimes, totalValues);
            totalScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("TotalCpu"));
            totalScatter.MarkerSize = 0;
            totalScatter.LineWidth = 1.5f;
            totalScatter.LegendText = "Total";
            totalScatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;
        }

        if (sqlValues.Length > 0)
        {
            var sqlScatter = CpuChart.Plot.Add.Scatter(sqlTimes, sqlValues);
            sqlScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlCpu"));
            sqlScatter.MarkerSize = 0;
            sqlScatter.LineWidth = 1.5f;
            sqlScatter.LegendText = "SQL";
            sqlScatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;
        }

        CpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
        if (CpuChart != FileIoChart)
            CpuChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        TabHelpers.ReapplyAxisColors(CpuChart);

        CpuChart.Plot.Title("");
        CpuChart.Plot.YLabel("");
        CpuChart.Plot.Legend.IsVisible = false;
        CpuChart.Plot.Axes.Margins(bottom: 0);
        CpuChart.Plot.Axes.SetLimitsY(0, 105);
    }

    private void UpdateLane(ScottPlot.WPF.WpfPlot chart, string title,
        List<(double Time, double Value)> data, string colorHex,
        double? yMin = null, double? yMax = null, BaselineBucket? baseline = null,
        double minAnomalyValue = 0)
    {
        ClearChart(chart);
        TabHelpers.ApplyThemeToChart(chart);

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
                var anomalyScatter = chart.Plot.Add.Scatter(anomalyTimes, anomalyValues);
                anomalyScatter.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
                anomalyScatter.MarkerSize = 6;
                anomalyScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                anomalyScatter.LineWidth = 0;
            }
        }

        var scatter = chart.Plot.Add.Scatter(times, values);
        scatter.Color = ScottPlot.Color.FromHex(colorHex);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LegendText = title;
        scatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;

        _crosshairManager?.SetLaneData(chart, times, values);

        chart.Plot.Axes.DateTimeTicksBottomDateChange();
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        TabHelpers.ReapplyAxisColors(chart);

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
    }

    private void SyncXAxes(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        DateTime xStart, xEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            xStart = fromDate.Value;
            xEnd = toDate.Value;
        }
        else
        {
            xEnd = ServerTimeHelper.ServerNow;
            xStart = xEnd.AddHours(-hoursBack);
        }

        double xMin = xStart.ToOADate();
        double xMax = xEnd.ToOADate();

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            chart.Plot.Axes.SetLimitsX(xMin, xMax);
            chart.Refresh();
        }
    }

    private static void AddGhostLine(ScottPlot.WPF.WpfPlot chart,
        List<(double Time, double Value)> data, string colorHex)
    {
        if (data.Count == 0) return;

        var times = data.Select(d => d.Time).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var scatter = chart.Plot.Add.Scatter(times, values);
        scatter.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("GhostLine")).WithAlpha(140);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LinePattern = ScottPlot.LinePattern.Dashed;
    }

    private static string ComparisonLabel((DateTime From, DateTime To, DateTime CurrentFrom) range)
    {
        // #4305: CurrentFrom is the same server-local current-window start the reference range was built
        // from (GetOverviewComparisonRange) -- not a second, independently-sampled fromDate ?? DateTime.UtcNow.
        var daysBack = (range.CurrentFrom - range.From).TotalDays;

        if (Math.Abs(daysBack - 1) < 0.5) return "yesterday";
        if (Math.Abs(daysBack - 7) < 0.5) return "last week";
        return $"{daysBack:N0}d ago";
    }

    private static void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Plot.Clear();
    }

    /* Render an empty lane as a live, gridded chart instead of a dead black box. This is most often
       the Blocking/Deadlocking lane on a healthy server (no events = good news), so make it match the
       populated lanes: keep the grid and show a 0-1 Y axis. We no longer blank the axes or add the old
       "No Data" text (which SyncXAxes pushed off-screen anyway). X limits and the vertical gridlines are
       set afterward by SyncXAxes so the time axis aligns with the other lanes; the left axis keeps its
       default numeric ticks, so 0/1 labels and horizontal gridlines render. SyncXAxes also issues the
       Refresh. The title arg is retained for call-site readability. */
    private void ShowEmpty(ScottPlot.WPF.WpfPlot chart, string title)
    {
        chart.Plot.Axes.DateTimeTicksBottomDateChange();
        // Only the bottom (File I/O) lane shows time labels; the upper lanes hide them (matches UpdateLane).
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        TabHelpers.ReapplyAxisColors(chart);

        chart.Plot.Title("");
        chart.Plot.YLabel("");
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);
        chart.Plot.Axes.SetLimitsY(0, 1);
    }

    /// <summary>
    /// Reapplies theme to all lane charts (call on theme change).
    /// </summary>
    public void ReapplyTheme()
    {
        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            TabHelpers.ApplyThemeToChart(chart);
            chart.Refresh();
        }
    }
}
