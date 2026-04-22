/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * SYNC WARNING: Dashboard has a matching copy at Dashboard/Controls/CorrelatedTimelineLanesControl.xaml.cs.
 * Changes here must be mirrored there.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class CorrelatedTimelineLanesControl : UserControl
{
    private LocalDataService? _dataService;
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
    public void Initialize(LocalDataService dataService, int serverId)
    {
        _dataService = dataService;
        _serverId = serverId;

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            ApplyTheme(chart);
            // Disable zoom/pan/drag but keep mouse events for crosshair
            chart.UserInputProcessor.UserActionResponses.Clear();
        }

        _crosshairManager = new CorrelatedCrosshairManager();
        _crosshairManager.AddLane(CpuChart, "CPU", "%");
        _crosshairManager.AddLane(WaitStatsChart, "Wait Stats", "ms/sec");
        _crosshairManager.AddLane(BlockingChart, "Blocking", "events");
        _crosshairManager.AddLane(MemoryChart, "Buffer Pool", "MB");
        _crosshairManager.AddLane(FileIoChart, "I/O Latency", "ms");
    }

    /// <summary>
    /// Refreshes all lane data for the given time range.
    /// </summary>
    public async Task RefreshAsync(int hoursBack, DateTime? fromDate, DateTime? toDate,
        (DateTime From, DateTime To)? comparisonRange = null)
    {
        if (_dataService == null || _isRefreshing) return;
        _isRefreshing = true;

        try
        {
            _crosshairManager?.PrepareForRefresh();

            var cpuTask = _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate);
            var waitTask = _dataService.GetTotalWaitTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var blockingTask = _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var deadlockTask = _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var memoryTask = _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var fileIoTask = _dataService.GetFileIoLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate);

            // Fetch baselines for band rendering — chart-unit-matched metrics
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
                AppLogger.Info("CorrelatedLanes", $"Data fetch failed: {ex.Message}");
            }

            var cpuBaseline = cpuBaselineTask.IsCompletedSuccessfully ? cpuBaselineTask.Result : null;
            var waitBaseline = waitBaselineTask.IsCompletedSuccessfully ? waitBaselineTask.Result : null;
            var ioBaseline = ioBaselineTask.IsCompletedSuccessfully ? ioBaselineTask.Result : null;
            var blockingBaseline = blockingBaselineTask.IsCompletedSuccessfully ? blockingBaselineTask.Result : null;

            var utcOffset = ServerTimeHelper.UtcOffsetMinutes;

            // minAnomalyValue: absolute floor below which dots/arrows are suppressed even if outside band.
            // Prevents "1% CPU above 0.5% baseline" false alarms on idle servers.
            if (cpuTask.IsCompletedSuccessfully)
                UpdateLane(CpuChart, "CPU %",
                    cpuTask.Result.Select(d => (d.SampleTime.ToOADate(), (double)d.SqlServerCpu)).ToList(),
                    "#4FC3F7", 0, 105, cpuBaseline, minAnomalyValue: 10);
            else
                ShowEmpty(CpuChart, "CPU %");

            if (waitTask.IsCompletedSuccessfully)
                UpdateLane(WaitStatsChart, "Wait ms/sec",
                    waitTask.Result.Select(d => (d.CollectionTime.AddMinutes(utcOffset).ToOADate(), d.WaitTimeMsPerSecond)).ToList(),
                    "#FFB74D", baseline: waitBaseline, minAnomalyValue: 100);
            else
                ShowEmpty(WaitStatsChart, "Wait ms/sec");

            {
                var blockingData = blockingTask.IsCompletedSuccessfully
                    ? blockingTask.Result.Select(d => (d.Time.AddMinutes(utcOffset).ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                var deadlockData = deadlockTask.IsCompletedSuccessfully
                    ? deadlockTask.Result.Select(d => (d.Time.AddMinutes(utcOffset).ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                UpdateBlockingLane(blockingData, deadlockData, blockingBaseline);
            }

            if (memoryTask.IsCompletedSuccessfully)
                UpdateLane(MemoryChart, "Buffer Pool MB",
                    memoryTask.Result.Select(d => (d.CollectionTime.AddMinutes(utcOffset).ToOADate(), d.BufferPoolMb)).ToList(),
                    "#CE93D8");
            else
                ShowEmpty(MemoryChart, "Memory MB");

            if (fileIoTask.IsCompletedSuccessfully)
            {
                var ioGrouped = fileIoTask.Result
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => (g.Key.AddMinutes(utcOffset).ToOADate(), g.Average(x => x.AvgReadLatencyMs)))
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
                // Time shift: offset to align reference data with current chart X axis
                var timeShift = (fromDate ?? DateTime.UtcNow.AddHours(-hoursBack)) - refFrom;

                var refCpuTask = _dataService.GetCpuUtilizationAsync(_serverId, 0, refFrom, refTo);
                var refWaitTask = _dataService.GetTotalWaitTrendAsync(_serverId, 0, refFrom, refTo);
                var refBlockingTask = _dataService.GetBlockingTrendAsync(_serverId, 0, refFrom, refTo);
                var refMemoryTask = _dataService.GetMemoryTrendAsync(_serverId, 0, refFrom, refTo);
                var refIoTask = _dataService.GetFileIoLatencyTrendAsync(_serverId, 0, refFrom, refTo);

                try { await Task.WhenAll(refCpuTask, refWaitTask, refBlockingTask, refMemoryTask, refIoTask); }
                catch (Exception ex) { AppLogger.Info("CorrelatedLanes", $"Comparison fetch failed: {ex.Message}"); }

                AppLogger.Info("CorrelatedLanes",
                    $"Comparison: refFrom={refFrom:o}, refTo={refTo:o}, shift={timeShift.TotalHours:F1}h, " +
                    $"cpuRows={refCpuTask.Result?.Count ?? 0}, waitRows={refWaitTask.Result?.Count ?? 0}");

                if (refCpuTask.IsCompletedSuccessfully)
                    AddGhostLine(CpuChart, refCpuTask.Result
                        .Select(d => (d.SampleTime.Add(timeShift).ToOADate(), (double)d.SqlServerCpu)).ToList(), "#4FC3F7");

                if (refWaitTask.IsCompletedSuccessfully)
                    AddGhostLine(WaitStatsChart, refWaitTask.Result
                        .Select(d => (d.CollectionTime.AddMinutes(utcOffset).Add(timeShift).ToOADate(), d.WaitTimeMsPerSecond)).ToList(), "#FFB74D");

                if (refBlockingTask.IsCompletedSuccessfully)
                {
                    var refBlocking = refBlockingTask.Result
                        .Select(d => (d.Time.AddMinutes(utcOffset).Add(timeShift).ToOADate(), (double)d.Count)).ToList();
                    if (refBlocking.Count > 0)
                        AddGhostLine(BlockingChart, refBlocking, "#E57373");
                }

                if (refMemoryTask.IsCompletedSuccessfully)
                    AddGhostLine(MemoryChart, refMemoryTask.Result
                        .Select(d => (d.CollectionTime.AddMinutes(utcOffset).Add(timeShift).ToOADate(), d.BufferPoolMb)).ToList(), "#CE93D8");

                if (refIoTask.IsCompletedSuccessfully)
                {
                    var refIo = refIoTask.Result
                        .GroupBy(d => d.CollectionTime)
                        .OrderBy(g => g.Key)
                        .Select(g => (g.Key.AddMinutes(utcOffset).Add(timeShift).ToOADate(), g.Average(x => x.AvgReadLatencyMs)))
                        .ToList();
                    AddGhostLine(FileIoChart, refIo, "#81C784");
                }

                // Register reference data with crosshair manager for tooltip
                _crosshairManager?.SetComparisonLabel(ComparisonLabel(comparisonRange.Value, fromDate, hoursBack));
            }

            /* VLines must be re-attached before SyncXAxes so they're part of
               the render set when the chart refreshes. */
            _crosshairManager?.ReattachVLines();
            SyncXAxes(hoursBack, fromDate, toDate, utcOffset);
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
                FillColor = ScottPlot.Color.FromHex("#E57373"),
                LineWidth = 0
            }).ToArray();
            BlockingChart.Plot.Add.Bars(bars);
            maxCount = Math.Max(maxCount, blockingData.Max(d => d.Value));
        }

        // Deadlock bars — yellow/amber, slightly narrower so both are visible
        if (deadlockData.Count > 0)
        {
            var bars = deadlockData.Select(d => new ScottPlot.Bar
            {
                Position = d.Time,
                Value = d.Value,
                Size = barWidth * 0.6,
                FillColor = ScottPlot.Color.FromHex("#FFD54F"),
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
            band.FillStyle.Color = ScottPlot.Color.FromHex("#E57373").WithAlpha(25);
            band.LineStyle.Width = 0;

            var meanLine = BlockingChart.Plot.Add.HorizontalLine(baseline.Mean);
            meanLine.Color = ScottPlot.Color.FromHex("#E57373").WithAlpha(60);
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
                var anomalyScatter = chart.Plot.Add.Scatter(anomalyTimes, anomalyValues);
                anomalyScatter.Color = ScottPlot.Color.FromHex("#FF5252");
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
    /// Sets identical X-axis limits across all lanes.
    /// </summary>
    private void SyncXAxes(int hoursBack, DateTime? fromDate, DateTime? toDate, double utcOffset)
    {
        DateTime xStart, xEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            xStart = fromDate.Value;
            xEnd = toDate.Value;
        }
        else
        {
            xEnd = DateTime.UtcNow.AddMinutes(utcOffset);
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

    /// <summary>
    /// Renders a semi-transparent dashed ghost line for comparison overlay.
    /// </summary>
    private static void AddGhostLine(ScottPlot.WPF.WpfPlot chart,
        List<(double Time, double Value)> data, string colorHex)
    {
        if (data.Count == 0) return;

        var times = data.Select(d => d.Time).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var scatter = chart.Plot.Add.Scatter(times, values);
        // White-ish ghost line — distinct from the primary colored line
        scatter.Color = ScottPlot.Colors.White.WithAlpha(140);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LinePattern = ScottPlot.LinePattern.Dashed;

        chart.Refresh();
    }

    private static string ComparisonLabel((DateTime From, DateTime To) range,
        DateTime? fromDate, int hoursBack)
    {
        var currentStart = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);
        var daysBack = (currentStart - range.From).TotalDays;

        if (Math.Abs(daysBack - 1) < 0.5) return "yesterday";
        if (Math.Abs(daysBack - 7) < 0.5) return "last week";
        return $"{daysBack:N0}d ago";
    }

    private static void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Reset();
        chart.Plot.Clear();
    }

    private static void ShowEmpty(ScottPlot.WPF.WpfPlot chart, string title)
    {
        ReapplyAxisColors(chart);
        var text = chart.Plot.Add.Text($"{title}\nNo Data", 0, 0);
        text.LabelFontColor = ScottPlot.Color.FromHex("#888888");
        text.LabelFontSize = 12;
        text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
        chart.Plot.HideGrid();
        chart.Plot.Axes.SetLimitsX(-1, 1);
        chart.Plot.Axes.SetLimitsY(-1, 1);
        chart.Plot.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.Axes.Left.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.Legend.IsVisible = false;
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
        ScottPlot.Color figureBackground, dataBackground, textColor, gridColor;

        if (ThemeManager.CurrentTheme == "CoolBreeze")
        {
            figureBackground = ScottPlot.Color.FromHex("#EEF4FA");
            dataBackground   = ScottPlot.Color.FromHex("#DAE6F0");
            textColor        = ScottPlot.Color.FromHex("#1A2A3A");
            gridColor        = ScottPlot.Color.FromHex("#A8BDD0").WithAlpha(120);
        }
        else if (ThemeManager.HasLightBackground)
        {
            figureBackground = ScottPlot.Color.FromHex("#FFFFFF");
            dataBackground   = ScottPlot.Color.FromHex("#F5F7FA");
            textColor        = ScottPlot.Color.FromHex("#1A1D23");
            gridColor        = ScottPlot.Colors.Black.WithAlpha(20);
        }
        else
        {
            figureBackground = ScottPlot.Color.FromHex("#22252b");
            dataBackground   = ScottPlot.Color.FromHex("#111217");
            textColor        = ScottPlot.Color.FromHex("#E4E6EB");
            gridColor        = ScottPlot.Colors.White.WithAlpha(40);
        }

        chart.Plot.FigureBackground.Color = figureBackground;
        chart.Plot.DataBackground.Color = dataBackground;
        chart.Plot.Axes.Color(textColor);
        chart.Plot.Grid.MajorLineColor = gridColor;
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
        chart.Plot.Axes.Left.Label.ForeColor = textColor;

        chart.Background = new System.Windows.Media.SolidColorBrush(
            System.Windows.Media.Color.FromRgb(figureBackground.R, figureBackground.G, figureBackground.B));
    }

    private static void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart)
    {
        var textColor = ThemeManager.CurrentTheme == "CoolBreeze"
            ? ScottPlot.Color.FromHex("#1A2A3A")
            : ThemeManager.HasLightBackground
                ? ScottPlot.Color.FromHex("#1A1D23")
                : ScottPlot.Color.FromHex("#E4E6EB");
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
        chart.Plot.Axes.Left.Label.ForeColor = textColor;
    }
}
