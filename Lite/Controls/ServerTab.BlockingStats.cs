/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Controls;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The Blocking Stats sub-tab — the Lite port of the Darling viewer's Blocking Stats sub-tab: the blocking-
/// AND deadlock-SEVERITY companion to the count-only "Trends" sub-tab (the "are blocks/deadlocks getting
/// WORSE, not just more frequent" signal). A 2x2 chart grid + a window summary strip, everything computed
/// on-the-fly from rows Lite already keeps (no collector, no migration — the Lite equivalent of the
/// Dashboard's pre-aggregated <c>collect.blocking_deadlock_stats</c>). Blocking severity comes from the same
/// <c>blocked_process_reports</c> rows the count trend uses, with the identical <c>v_dmv_blocking_snapshots</c>
/// fallback so the two reconcile; deadlock severity is parsed from <c>deadlock_graph_xml</c> over the SAME
/// <c>v_deadlocks</c> window as the deadlock count. The four charts split Total from per-incident Max/Avg so
/// the aggregate magnitude doesn't swamp the per-incident axis. Rides Lite's shared chart idiom
/// (<see cref="ChartStyle"/> / <see cref="ChartHoverHelper"/>, <c>SeriesColors</c> / <see cref="ChartPalette"/>,
/// UTC X with ticks and hover worded in the display zone, Y-floor-at-0, window-pinned <c>SetLimitsX</c>), so the look
/// matches Lite's Blocking Trends charts exactly.
/// </summary>
public partial class ServerTab : UserControl
{
    private ChartHoverHelper? _blockingDurationHover;
    private ChartHoverHelper? _blockingTotalDurationHover;
    private ChartHoverHelper? _deadlockWaitHover;
    private ChartHoverHelper? _deadlockTotalWaitHover;

    /// <summary>Applies the shared chrome + hover to the four Blocking Stats charts up front (constructor),
    /// so they don't flash white before the sub-tab's first load — matching the Blocking trend charts.</summary>
    private void InitializeBlockingStatsCharts()
    {
        ApplyTheme(BlockingDurationChart);
        BlockingDurationChart.Refresh();
        ApplyTheme(BlockingTotalDurationChart);
        BlockingTotalDurationChart.Refresh();
        ApplyTheme(DeadlockWaitChart);
        DeadlockWaitChart.Refresh();
        ApplyTheme(DeadlockTotalWaitChart);
        DeadlockTotalWaitChart.Refresh();

        _blockingDurationHover = new ChartHoverHelper(BlockingDurationChart, "ms", displayZone: GetPickerZone);
        _blockingTotalDurationHover = new ChartHoverHelper(BlockingTotalDurationChart, "ms", displayZone: GetPickerZone);
        _deadlockWaitHover = new ChartHoverHelper(DeadlockWaitChart, "ms", displayZone: GetPickerZone);
        _deadlockTotalWaitHover = new ChartHoverHelper(DeadlockTotalWaitChart, "ms", displayZone: GetPickerZone);
    }

    /// <summary>
    /// Max + Avg block duration (ms) per minute — the per-incident severity signal (how long an individual
    /// block lasts). Two connected-scatter series on one shared ms axis (Max ≥ Avg, comparable magnitudes),
    /// mirroring the Current Waits duration chart's render idiom. Total duration lands on its own chart
    /// (<see cref="UpdateBlockingTotalDurationChart"/>) because its aggregate magnitude dwarfs these.
    /// </summary>
    private void UpdateBlockingDurationChart(List<BlockingDurationStatsPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(BlockingDurationChart);
        ApplyTheme(BlockingDurationChart);

        var (rangeStart, rangeEnd) = StatsChartRange(hoursBack, fromDate, toDate);

        _blockingDurationHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = BlockingDurationChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Max Block Duration";
            zeroLine.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
            zeroLine.MarkerSize = 0;
            BlockingDurationChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
            BlockingDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(BlockingDurationChart);
            BlockingDurationChart.Plot.YLabel("Block Duration (ms)");
            SetChartYLimitsWithLegendPadding(BlockingDurationChart, 0, 1);
            ShowChartLegend(BlockingDurationChart);
            BlockingDurationChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => d.Time.ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
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

        BlockingDurationChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
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
    private void UpdateBlockingTotalDurationChart(List<BlockingDurationStatsPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(BlockingTotalDurationChart);
        ApplyTheme(BlockingTotalDurationChart);

        var (rangeStart, rangeEnd) = StatsChartRange(hoursBack, fromDate, toDate);

        _blockingTotalDurationHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = BlockingTotalDurationChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Total Block Duration";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
            zeroLine.MarkerSize = 0;
            BlockingTotalDurationChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
            BlockingTotalDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(BlockingTotalDurationChart);
            BlockingTotalDurationChart.Plot.YLabel("Total Block Duration (ms)");
            SetChartYLimitsWithLegendPadding(BlockingTotalDurationChart, 0, 1);
            ShowChartLegend(BlockingTotalDurationChart);
            BlockingTotalDurationChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => d.Time.ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
        var totals = PadEnds(ordered.Select(d => (double)d.TotalDurationMs).ToArray(), 0, 0);

        var plot = BlockingTotalDurationChart.Plot.Add.TimeSeries(times, totals);
        plot.LegendText = "Total Block Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
        ChartStyle.StyleScatter(plot);
        _blockingTotalDurationHover?.Add(plot, "Total Block Duration");

        double globalMax = totals.Length > 0 ? totals.Max() : 0;

        BlockingTotalDurationChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
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
    /// shared ms axis (Max ≥ Avg), the deadlock analog of <see cref="UpdateBlockingDurationChart"/>. Total
    /// deadlock wait lands on its own chart (<see cref="UpdateDeadlockTotalWaitChart"/>).
    /// </summary>
    private void UpdateDeadlockWaitChart(List<DeadlockSeverityStatsPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(DeadlockWaitChart);
        ApplyTheme(DeadlockWaitChart);

        var (rangeStart, rangeEnd) = StatsChartRange(hoursBack, fromDate, toDate);

        _deadlockWaitHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = DeadlockWaitChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Max Deadlock Wait";
            zeroLine.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
            zeroLine.MarkerSize = 0;
            DeadlockWaitChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
            DeadlockWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(DeadlockWaitChart);
            DeadlockWaitChart.Plot.YLabel("Deadlock Wait (ms)");
            SetChartYLimitsWithLegendPadding(DeadlockWaitChart, 0, 1);
            ShowChartLegend(DeadlockWaitChart);
            DeadlockWaitChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => d.Time.ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
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

        DeadlockWaitChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
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
    /// Trends tab's deadlock-count chart. The deadlock analog of <see cref="UpdateBlockingTotalDurationChart"/>.
    /// </summary>
    private void UpdateDeadlockTotalWaitChart(List<DeadlockSeverityStatsPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(DeadlockTotalWaitChart);
        ApplyTheme(DeadlockTotalWaitChart);

        var (rangeStart, rangeEnd) = StatsChartRange(hoursBack, fromDate, toDate);

        _deadlockTotalWaitHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = DeadlockTotalWaitChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Total Deadlock Wait";
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
            zeroLine.MarkerSize = 0;
            DeadlockTotalWaitChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
            DeadlockTotalWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(DeadlockTotalWaitChart);
            DeadlockTotalWaitChart.Plot.YLabel("Total Deadlock Wait (ms)");
            SetChartYLimitsWithLegendPadding(DeadlockTotalWaitChart, 0, 1);
            ShowChartLegend(DeadlockTotalWaitChart);
            DeadlockTotalWaitChart.Refresh();
            return;
        }

        var ordered = data.OrderBy(d => d.Time).ToList();
        var times = PadEnds(ordered.Select(d => d.Time.ToOADate()).ToArray(), rangeStart.ToOADate(), rangeEnd.ToOADate());
        var totals = PadEnds(ordered.Select(d => (double)d.TotalWaitMs).ToArray(), 0, 0);

        var plot = DeadlockTotalWaitChart.Plot.Add.TimeSeries(times, totals);
        plot.LegendText = "Total Deadlock Wait";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
        ChartStyle.StyleScatter(plot);
        _deadlockTotalWaitHover?.Add(plot, "Total Deadlock Wait");

        double globalMax = totals.Length > 0 ? totals.Max() : 0;

        DeadlockTotalWaitChart.Plot.Axes.DateTimeTicksBottomUtc(GetPickerZone);
        DeadlockTotalWaitChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(DeadlockTotalWaitChart);
        DeadlockTotalWaitChart.Plot.YLabel("Total Deadlock Wait (ms)");
        SetChartYLimitsWithLegendPadding(DeadlockTotalWaitChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(DeadlockTotalWaitChart);
        DeadlockTotalWaitChart.Refresh();
    }

    /// <summary>
    /// Window-rollup summary strip for the Blocking Stats sub-tab: total blocking events, total / max / avg
    /// block duration (the avg is EVENT-weighted — total ÷ events — not a mean of the per-minute averages), the
    /// deadlock COUNT, and the deadlock SEVERITY rollup (total victim processes + total deadlock wait) over the
    /// window. The deadlock count is the cheap incident signal; the victim_count / total-deadlock-wait are
    /// parsed on-the-fly from <c>deadlock_graph_xml</c> over the SAME v_deadlocks window (so they reconcile in
    /// period), the Lite equivalent of the Dashboard's <c>victim_count</c> / <c>total_deadlock_wait_time_ms</c>.
    /// Durations render with Lite's blocking wait-time format (ms under a second, else sec).
    /// </summary>
    private void UpdateBlockingStatsSummary(
        List<BlockingDurationStatsPoint> stats,
        List<TrendPoint> deadlocks,
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
        BlockingStatsTotalDurationText.Text = totalEvents > 0 ? FormatWaitTime(totalDuration) : "--";
        BlockingStatsMaxDurationText.Text = totalEvents > 0 ? FormatWaitTime(maxDuration) : "--";
        BlockingStatsAvgDurationText.Text = totalEvents > 0 ? FormatWaitTime(avgDuration) : "--";
        BlockingStatsDeadlockCountText.Text = totalDeadlocks.ToString("N0");
        BlockingStatsDeadlockVictimsText.Text = totalVictims.ToString("N0");
        BlockingStatsDeadlockWaitText.Text = totalDeadlocks > 0 ? FormatWaitTime(totalDeadlockWait) : "--";
    }

    /// <summary>The Blocking Stats charts' X-axis window as UTC instants (#4766): the custom from/to range
    /// when set, else the last <paramref name="hoursBack"/> real hours ending now,
    /// matching the Blocking trend charts.</summary>
    private (DateTime rangeStart, DateTime rangeEnd) StatsChartRange(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (fromDate.HasValue && toDate.HasValue)
            return (fromDate.Value, toDate.Value);

        return GetChartWindow(hoursBack, null, null);
    }

    /// <summary>
    /// Prepends <paramref name="lead"/> and appends <paramref name="trail"/> so a sparse connected-scatter
    /// series spans the whole selected window instead of ending at its last event. Pass the window's
    /// rangeStart/rangeEnd for the times array and 0/0 for value arrays (no activity = zero duration). Keeps all
    /// four Blocking-Stats charts aligned to the shared window rather than each ending at its own last point.
    /// </summary>
    private static double[] PadEnds(double[] values, double lead, double trail)
    {
        var result = new double[values.Length + 2];
        result[0] = lead;
        Array.Copy(values, 0, result, 1, values.Length);
        result[^1] = trail;
        return result;
    }

    /// <summary>Lite's blocking wait-time rendering: &lt; 1000 ms as "N ms", else "N.N sec" (mirrors
    /// <see cref="BlockedProcessReportRow.WaitTimeFormatted"/> so the strip matches the grid).</summary>
    private static string FormatWaitTime(long waitTimeMs)
        => waitTimeMs < 1000 ? $"{waitTimeMs} ms" : $"{waitTimeMs / 1000.0:F1} sec";

    /// <summary>Tears down the Blocking Stats chart hover helpers; called from the single DisposeChartHelpers().</summary>
    public void DisposeBlockingStatsHelpers()
    {
        _blockingDurationHover?.Dispose();
        _blockingTotalDurationHover?.Dispose();
        _deadlockWaitHover?.Dispose();
        _deadlockTotalWaitHover?.Dispose();
    }
}
