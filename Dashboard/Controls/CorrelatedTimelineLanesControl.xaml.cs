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
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls;

public partial class CorrelatedTimelineLanesControl : UserControl
{
    private DatabaseService? _dataService;
    private SqlServerBaselineProvider? _baselineProvider;
    private CorrelatedCrosshairManager? _crosshairManager;

    public CorrelatedTimelineLanesControl()
    {
        InitializeComponent();
        Unloaded += (_, _) => _crosshairManager?.Dispose();
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
        if (_dataService == null) return;

        _crosshairManager?.PrepareForRefresh();

        var cpuTask = _dataService.GetCpuUtilizationAsync(hoursBack, fromDate, toDate);
        var waitTask = _dataService.GetTotalWaitStatsTrendAsync(hoursBack, fromDate, toDate);
        var blockingTask = _dataService.GetBlockedSessionTrendAsync(hoursBack, fromDate, toDate);
        var deadlockTask = _dataService.GetDeadlockTrendAsync(hoursBack, fromDate, toDate);
        var memoryTask = _dataService.GetMemoryStatsAsync(hoursBack, fromDate, toDate);
        var fileIoTask = _dataService.GetFileIoLatencyTimeSeriesAsync(false, hoursBack, fromDate, toDate);

        // Fetch baselines for band rendering if provider is available
        var referenceTime = fromDate ?? DateTime.UtcNow.AddHours(-hoursBack);
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
            UpdateLane(CpuChart, "CPU %",
                cpuTask.Result.OrderBy(d => d.SampleTime)
                    .Select(d => (d.SampleTime.ToOADate(), (double)d.SqlServerCpuUtilization)).ToList(),
                "#4FC3F7", 0, 105, cpuBaseline, minAnomalyValue: 10);
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
            var timeShift = (fromDate ?? DateTime.UtcNow.AddHours(-hoursBack)) - refFrom;

            var refCpuTask = _dataService.GetCpuUtilizationAsync(0, refFrom, refTo);
            var refWaitTask = _dataService.GetTotalWaitStatsTrendAsync(0, refFrom, refTo);
            var refBlockingTask = _dataService.GetBlockedSessionTrendAsync(0, refFrom, refTo);
            var refMemoryTask = _dataService.GetMemoryStatsAsync(0, refFrom, refTo);
            var refIoTask = _dataService.GetFileIoLatencyTimeSeriesAsync(false, 0, refFrom, refTo);

            try { await Task.WhenAll(refCpuTask, refWaitTask, refBlockingTask, refMemoryTask, refIoTask); }
            catch (Exception ex) { Debug.WriteLine($"CorrelatedLanes: Comparison fetch failed: {ex.Message}"); }

            if (refCpuTask.IsCompletedSuccessfully)
                AddGhostLine(CpuChart, refCpuTask.Result
                    .Select(d => (d.SampleTime.Add(timeShift).ToOADate(), (double)d.SqlServerCpuUtilization)).ToList(), "#4FC3F7");

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

            _crosshairManager?.SetComparisonLabel(ComparisonLabel(comparisonRange.Value, fromDate, hoursBack));
        }

        _crosshairManager?.ReattachVLines();
        SyncXAxes(hoursBack, fromDate, toDate);
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
                FillColor = ScottPlot.Color.FromHex("#E57373"),
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
                FillColor = ScottPlot.Color.FromHex("#FFD54F"),
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
                band.FillStyle.Color = ScottPlot.Color.FromHex("#E57373").WithAlpha(25);
                band.LineStyle.Width = 0;

                var meanLine = BlockingChart.Plot.Add.HorizontalLine(baseline.Mean);
                meanLine.Color = ScottPlot.Color.FromHex("#E57373").WithAlpha(60);
                meanLine.LinePattern = ScottPlot.LinePattern.Dashed;
                meanLine.LineWidth = 1;
            }
        }

        BlockingChart.Plot.Axes.DateTimeTicksBottom();
        BlockingChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;
        TabHelpers.ReapplyAxisColors(BlockingChart);

        BlockingChart.Plot.Title("");
        BlockingChart.Plot.YLabel("");
        BlockingChart.Plot.Legend.IsVisible = false;
        BlockingChart.Plot.Axes.Margins(bottom: 0);
        BlockingChart.Plot.Axes.SetLimitsY(0, Math.Max(maxCount * 1.3, 2));
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

        chart.Plot.Axes.DateTimeTicksBottom();
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
        scatter.Color = ScottPlot.Colors.White.WithAlpha(140);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LinePattern = ScottPlot.LinePattern.Dashed;
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
        chart.Plot.Clear();
    }

    private static void ShowEmpty(ScottPlot.WPF.WpfPlot chart, string title)
    {
        TabHelpers.ReapplyAxisColors(chart);
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
