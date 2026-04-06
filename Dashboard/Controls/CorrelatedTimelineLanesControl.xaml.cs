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
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls;

public partial class CorrelatedTimelineLanesControl : UserControl
{
    private DatabaseService? _dataService;
    private CorrelatedCrosshairManager? _crosshairManager;
    private bool _isRefreshing;

    public CorrelatedTimelineLanesControl()
    {
        InitializeComponent();
        Unloaded += (_, _) => _crosshairManager?.Dispose();
    }

    /// <summary>
    /// Initializes the control with the data service.
    /// Must be called before RefreshAsync.
    /// </summary>
    public void Initialize(DatabaseService dataService)
    {
        _dataService = dataService;

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            TabHelpers.ApplyThemeToChart(chart);
            // Disable zoom/pan/drag but keep mouse events for crosshair
            chart.UserInputProcessor.UserActionResponses.Clear();
        }

        _crosshairManager = new CorrelatedCrosshairManager();
        _crosshairManager.AddLane(CpuChart, "CPU", "%", CpuValueLabel);
        _crosshairManager.AddLane(WaitStatsChart, "Wait Stats", "ms/sec", WaitStatsValueLabel);
        _crosshairManager.AddLane(BlockingChart, "Blocking", "events", BlockingValueLabel);
        _crosshairManager.AddLane(MemoryChart, "Memory", "MB", MemoryValueLabel);
        _crosshairManager.AddLane(FileIoChart, "I/O Latency", "ms", FileIoValueLabel);
    }

    /// <summary>
    /// Refreshes all lane data for the given time range.
    /// </summary>
    public async Task RefreshAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (_dataService == null || _isRefreshing) return;
        _isRefreshing = true;

        try
        {
            _crosshairManager?.PrepareForRefresh();

            var cpuTask = _dataService.GetCpuUtilizationAsync(hoursBack, fromDate, toDate);
            var waitTask = _dataService.GetTotalWaitStatsTrendAsync(hoursBack, fromDate, toDate);
            var blockingTask = _dataService.GetBlockedSessionTrendAsync(hoursBack, fromDate, toDate);
            var deadlockTask = _dataService.GetDeadlockTrendAsync(hoursBack, fromDate, toDate);
            var memoryTask = _dataService.GetMemoryStatsAsync(hoursBack, fromDate, toDate);
            var fileIoTask = _dataService.GetFileIoLatencyTimeSeriesAsync(false, hoursBack, fromDate, toDate);

            try
            {
                await Task.WhenAll(cpuTask, waitTask, blockingTask, deadlockTask, memoryTask, fileIoTask);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"CorrelatedLanes: Data fetch failed: {ex.Message}");
            }

            if (cpuTask.IsCompletedSuccessfully)
                UpdateLane(CpuChart, "CPU %",
                    cpuTask.Result.Select(d => (d.SampleTime.ToOADate(), (double)d.SqlServerCpuUtilization)).ToList(),
                    "#4FC3F7", 0, 105);
            else
                ShowEmpty(CpuChart, "CPU %");

            if (waitTask.IsCompletedSuccessfully)
                UpdateLane(WaitStatsChart, "Wait ms/sec",
                    waitTask.Result.Select(d => (d.CollectionTime.ToOADate(), (double)d.WaitTimeMsPerSecond)).ToList(),
                    "#FFB74D");
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
                UpdateBlockingLane(blockingData, deadlockData);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"CorrelatedLanes: Blocking lane failed: {ex}");
                ShowEmpty(BlockingChart, "Blocking & Deadlocking");
            }

            if (memoryTask.IsCompletedSuccessfully)
                UpdateLane(MemoryChart, "Memory MB",
                    memoryTask.Result.Select(d => (d.CollectionTime.ToOADate(), (double)d.TotalMemoryMb)).ToList(),
                    "#CE93D8");
            else
                ShowEmpty(MemoryChart, "Memory MB");

            if (fileIoTask.IsCompletedSuccessfully)
            {
                var ioGrouped = fileIoTask.Result
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => (g.Key.ToOADate(), (double)g.Average(x => x.ReadLatencyMs)))
                    .ToList();
                UpdateLane(FileIoChart, "I/O ms", ioGrouped, "#81C784");
            }
            else
                ShowEmpty(FileIoChart, "I/O ms");

            _crosshairManager?.ReattachVLines();
            SyncXAxes(hoursBack, fromDate, toDate);
        }
        finally
        {
            _isRefreshing = false;
        }
    }

    private void UpdateBlockingLane(List<(double Time, double Value)> blockingData,
        List<(double Time, double Value)> deadlockData)
    {
        ClearChart(BlockingChart);
        TabHelpers.ApplyThemeToChart(BlockingChart);

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

        BlockingChart.Plot.Axes.DateTimeTicksBottom();
        BlockingChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;
        TabHelpers.ReapplyAxisColors(BlockingChart);

        BlockingChart.Plot.Title("");
        BlockingChart.Plot.YLabel("");
        BlockingChart.Plot.Legend.IsVisible = false;
        BlockingChart.Plot.Axes.Margins(bottom: 0);
        BlockingChart.Plot.Axes.SetLimitsY(0, Math.Max(maxCount * 1.3, 2));

        BlockingChart.Refresh();
    }

    private void UpdateLane(ScottPlot.WPF.WpfPlot chart, string title,
        List<(double Time, double Value)> data, string colorHex,
        double? yMin = null, double? yMax = null)
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

        var scatter = chart.Plot.Add.Scatter(times, values);
        scatter.Color = ScottPlot.Color.FromHex(colorHex);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LegendText = title;
        scatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;

        _crosshairManager?.SetLaneData(chart, times, values);

        chart.Plot.Axes.DateTimeTicksBottom();
        // Hide bottom tick labels on all lanes except the last (File I/O)
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        TabHelpers.ReapplyAxisColors(chart);

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

    private static void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Reset();
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
            TabHelpers.ApplyThemeToChart(chart);
            chart.Refresh();
        }
    }
}
