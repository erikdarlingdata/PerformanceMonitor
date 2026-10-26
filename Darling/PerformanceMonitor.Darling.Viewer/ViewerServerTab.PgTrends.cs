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
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The four PostgreSQL trend charts (#6385): the web's Database Trend, Query Duration Trend, Wait Trend and I/O
/// Trend, as charts. The reads are <see cref="ViewerDataService"/>'s <c>GetPg*TrendAsync</c>, which run the same
/// <c>DarlingPgTrendReader</c> the MCP tools and the web run, on the same subject (the window's busiest) and the
/// same bucket width. Charts are built into the hosts the XAML declares the first time a tab loads, with the
/// shared theme, hover tooltips, copy/save menu, and the toolbar's time-zone display; an empty window says why
/// in the note above the host, the way the grids' notes do.
/// </summary>
public partial class ViewerServerTab
{
    private readonly Dictionary<string, (WpfPlot Chart, ChartHoverHelper Hover)> _pgTrendCharts = new();

    private sealed record PgTrendLine(string Label, double[] Times, double[] Values);

    /// <summary>One chart of a trend: its key, y-axis label, hover unit, and the lines it draws.</summary>
    private sealed record PgTrendPanel(string Key, string YLabel, string Unit, IReadOnlyList<PgTrendLine> Lines);

    private (WpfPlot Chart, ChartHoverHelper Hover) PgTrendChart(UniformGrid host, string key, string unit)
    {
        if (_pgTrendCharts.TryGetValue(key, out var existing))
        {
            return existing;
        }

        var chart = new WpfPlot { MinHeight = 200, Margin = new System.Windows.Thickness(2) };
        ApplyTheme(chart);
        chart.Refresh();
        var hover = new ChartHoverHelper(chart, unit, displayZone: ViewerTimeHelper.CurrentDisplayZone);
        BuildChartContextMenu(chart, key);
        host.Children.Add(chart);
        _pgTrendCharts[key] = (chart, hover);
        return _pgTrendCharts[key];
    }

    private static double[] Times<T>(IReadOnlyList<T> points, Func<T, DateTime> time) =>
        points.Select(p => time(p).ToOADate()).ToArray();

    private static double[] Values<T>(IReadOnlyList<T> points, Func<T, double?> value) =>
        points.Select(p => value(p) ?? double.NaN).ToArray();

    /// <summary>A line only when it has at least one measured value: a series of nothing but nulls (read latency
    /// with <c>track_io_timing</c> off, which is the default) draws no line rather than a flat zero.</summary>
    private static IEnumerable<PgTrendLine> Measured(params PgTrendLine[] lines) =>
        lines.Where(l => l.Values.Any(v => !double.IsNaN(v)));

    private void RenderPgTrendPanels(UniformGrid host, IEnumerable<PgTrendPanel> panels, DateTime startUtc, DateTime endUtc)
    {
        foreach (var panel in panels)
        {
            var (chart, hover) = PgTrendChart(host, panel.Key, panel.Unit);
            ClearChart(chart);
            hover.Clear();
            ApplyTheme(chart);

            if (panel.Lines.Count == 0)
            {
                /* The shared empty chart installs its own empty tick generators on a plain numeric axis. A date axis
                   refuses that ("Date axis must have a ITickGenerator generator"), which aborted the whole load and left
                   one oversized blank chart, so the date axis is set only for a chart that has lines (#6385). */
                RefreshEmptyChart(chart, panel.YLabel, panel.YLabel);
                continue;
            }

            chart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);

            double max = 0;
            var color = 0;
            foreach (var line in panel.Lines)
            {
                var plot = chart.Plot.Add.TimeSeries(line.Times, line.Values);
                plot.LegendText = line.Label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[color++ % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                hover.Add(plot, line.Label);
                var finite = line.Values.Where(v => !double.IsNaN(v)).ToArray();
                if (finite.Length > 0)
                {
                    max = Math.Max(max, finite.Max());
                }
            }

            chart.Plot.Axes.SetLimitsX(startUtc.ToOADate(), endUtc.ToOADate());
            ReapplyAxisColors(chart);
            chart.Plot.YLabel(panel.YLabel);
            SetChartYLimitsWithLegendPadding(chart, 0, max > 0 ? max : 1);
            ShowChartLegend(chart);
            chart.Refresh();
        }
    }

    private static string BucketSentence(int bucketMinutes) =>
        $"one point per {TrendBuckets.Word(bucketMinutes)}";

    /// <summary>The Trends sub-tab of Activity: database and query-duration trends, both over the toolbar's window.</summary>
    private async Task LoadPgActivityTrendsAsync(DateTime startUtc, DateTime endUtc)
    {
        var dbGated = PgCollectorIsGatedOff("pg_database_stats");
        var qGated = PgCollectorIsGatedOff("pg_statement_stats");
        var dbTask = dbGated
            ? Task.FromResult(ViewerDataService.NoPgTrend<DarlingPgTrendReader.PgDatabaseTrendPoint>(0))
            : _dataService.GetPgDatabaseTrendAsync(_server.ServerId, startUtc, endUtc);
        var qTask = qGated
            ? Task.FromResult(ViewerDataService.NoPgTrend<DarlingPgTrendReader.PgQueryDurationTrendPoint>(0))
            : _dataService.GetPgQueryDurationTrendAsync(_server.ServerId, startUtc, endUtc);
        await Task.WhenAll(dbTask, qTask);

        var db = dbTask.Result;
        var d = db.Points;
        PgDatabaseTrendNote.Text = d.Count == 0
            ? PanelNote("pg_database_stats", 0,
                "No differenced intervals for the busiest database in this window. A trend needs at least two snapshots, so a short window is legitimately empty here.")
            : $"Database Trend: {db.Subject}, {(d.Any(p => p.TempBytes > 0) ? "the biggest temp-file spiller in this window" : "the busiest by block access in this window (nothing spilled)")}, {BucketSentence(db.BucketMinutes)}. "
              + "The hit ratio is each interval's own, not the lifetime average, and Worst Hit % is the worst single interval in the bucket.";
        RenderPgTrendPanels(PgDatabaseTrendHost, new[]
        {
            new PgTrendPanel("PgDb_Percent", "Percent", "%", d.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Cache Hit %", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.CacheHitPct)),
                new PgTrendLine("Worst Hit %", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.WorstCacheHitPct)),
                new PgTrendLine("Rollback %", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.RollbackPct))).ToList()),
            new PgTrendPanel("PgDb_Xact", "Transactions/sec", "xact/s", d.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Xact/sec", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.TransactionsPerSecond))).ToList()),
            new PgTrendPanel("PgDb_Temp", "Temp Bytes", "bytes", d.Count == 0 ? Array.Empty<PgTrendLine>() : new[]
            {
                new PgTrendLine("Temp Bytes", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.TempBytes)),
            }),
            new PgTrendPanel("PgDb_Counts", "Temp Files and Deadlocks", "count", d.Count == 0 ? Array.Empty<PgTrendLine>() : new[]
            {
                new PgTrendLine("Temp Files", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.TempFiles)),
                new PgTrendLine("Deadlocks", Times(d, p => p.CollectionTimeUtc), Values(d, p => p.Deadlocks)),
            }),
        }, startUtc, endUtc);

        var q = qTask.Result;
        var qp = q.Points;
        PgQueryDurationTrendNote.Text = qp.Count == 0
            ? PanelNote("pg_statement_stats", 0,
                "No statement recorded execution time in this window, so there is nothing to follow.")
            : $"Query Duration Trend: query id {q.Subject}, the statement with the most execution time in this window, {BucketSentence(q.BucketMinutes)}. "
              + "A step that persists is a plan or index change.";
        RenderPgTrendPanels(PgQueryDurationTrendHost, new[]
        {
            new PgTrendPanel("PgQuery_Duration", "Mean ms/exec", "ms", qp.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Mean ms/exec", Times(qp, p => p.CollectionTimeUtc), Values(qp, p => p.Calls > 0 ? p.MeanExecMs : null)),
                new PgTrendLine("Peak ms/exec", Times(qp, p => p.CollectionTimeUtc), Values(qp, p => p.PeakMeanExecMs))).ToList()),
            new PgTrendPanel("PgQuery_Calls", "Calls/sec", "calls/s", qp.Count == 0 ? Array.Empty<PgTrendLine>() : new[]
            {
                new PgTrendLine("Calls/sec", Times(qp, p => p.CollectionTimeUtc), Values(qp, p => p.CallsPerSecond)),
            }),
        }, startUtc, endUtc);
    }

    /// <summary>The Waits tab's Wait Trend: the window's dominant wait event over time.</summary>
    private async Task LoadPgWaitTrendAsync(DateTime startUtc, DateTime endUtc)
    {
        var gated = PgCollectorIsGatedOff("pg_wait_sampling");
        var trend = gated
            ? ViewerDataService.NoPgTrend<DarlingPgTrendReader.PgWaitTrendPoint>(0)
            : await _dataService.GetPgWaitTrendAsync(_server.ServerId, startUtc, endUtc);
        var p = trend.Points;

        PgWaitTrendNote.Text = p.Count == 0
            ? PanelNote("pg_wait_sampling", 0,
                "No samples for a wait event in this window. A trend needs at least two snapshots to difference, so a short window is legitimately empty here even while the event is being sampled.")
            : $"Wait Trend: {trend.Subject}, the event that dominates this window, over time. Estimates from a sampling profiler, so the shape is the finding.";
        RenderPgTrendPanels(PgWaitTrendHost, new[]
        {
            new PgTrendPanel("PgWait_Trend", "Est. wait ms/sec", "ms/s", p.Count == 0 ? Array.Empty<PgTrendLine>() : new[]
            {
                new PgTrendLine(trend.Subject ?? "Wait", Times(p, x => x.CollectionTimeUtc), Values(p, x => x.EstimatedWaitMsPerSecond)),
            }),
        }, startUtc, endUtc);
    }

    /// <summary>The I/O tab's I/O Trend: the window's busiest (backend, context) pair over time.</summary>
    private async Task LoadPgIoTrendAsync(DateTime startUtc, DateTime endUtc)
    {
        var gated = PgCollectorIsGatedOff("pg_io_stats");
        var trend = gated
            ? new ViewerDataService.PgIoTrend(null, 0, Array.Empty<DarlingPgTrendReader.PgIoTrendPoint>(), false, false)
            : await _dataService.GetPgIoTrendAsync(_server.ServerId, startUtc, endUtc);
        var p = trend.Points;

        PgIoTrendNote.Text = p.Count == 0
            ? PanelNote("pg_io_stats", 0,
                "No I/O samples for the busiest backend and context in this window. A trend needs at least two snapshots to difference, so a short window is legitimately empty here.")
            : $"I/O Trend: {trend.Subject}, the busiest backend and context in this window, per second, {BucketSentence(trend.BucketMinutes)}. "
              + "Avg read ms is blank unless the server has track_io_timing on, which is off by default."
              + (trend.WriteCountersTracked ? "" : " This server has no write counters (Amazon Aurora does not write data files), so writes are not plotted.");
        RenderPgTrendPanels(PgIoTrendHost, new[]
        {
            new PgTrendPanel("PgIo_Ops", "Operations/sec", "ops/s", p.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Reads/sec", Times(p, x => x.CollectionTimeUtc), Values(p, x => x.ReadsPerSecond)),
                new PgTrendLine("Peak Reads/sec", Times(p, x => x.CollectionTimeUtc), Values(p, x => x.PeakReadsPerSecond)),
                new PgTrendLine("Writes/sec", Times(p, x => x.CollectionTimeUtc), Values(p, x => trend.WritesPerSecondOf(x))),
                new PgTrendLine("Evictions/sec", Times(p, x => x.CollectionTimeUtc), Values(p, x => x.IntervalSeconds > 0 ? x.Evictions / x.IntervalSeconds : null))).ToList()),
            new PgTrendPanel("PgIo_Hit", "Hit %", "%", p.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Hit %", Times(p, x => x.CollectionTimeUtc), Values(p, x => x.CacheHitPct))).ToList()),
            new PgTrendPanel("PgIo_ReadMs", "Avg Read (ms)", "ms", p.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Avg Read (ms)", Times(p, x => x.CollectionTimeUtc), Values(p, x => trend.TimingTracked ? x.AvgReadMs : null))).ToList()),
            new PgTrendPanel("PgIo_Bytes", "Read Bytes/sec", "bytes/s", p.Count == 0 ? Array.Empty<PgTrendLine>() : Measured(
                new PgTrendLine("Read Bytes/sec", Times(p, x => x.CollectionTimeUtc), Values(p, x => trend.BytesShown ? x.ReadBytesPerSecond : null))).ToList()),
        }, startUtc, endUtc);
    }
}
