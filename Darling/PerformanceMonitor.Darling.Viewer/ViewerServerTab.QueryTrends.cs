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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Queries → Performance Trends sub-tab (W1f-2): the 2×2 grid of per-second trend charts (query
/// duration, procedure duration, Query Store duration, execution count), copied from Lite's
/// <c>ServerTab.Charts.cs</c> (<c>Update{Query,Proc,QueryStore}DurationTrendChart</c> +
/// <c>UpdateExecutionCountTrendChart</c>, :993-1091) with the data layer rewired to
/// <see cref="ViewerDataService"/> Postgres reads. The only render-body change is the time axis: where
/// Lite shifts each point by its per-server <c>UtcOffsetMinutes</c>, the viewer runs the naive-UTC
/// <c>collection_time</c> through <see cref="ViewerTimeHelper.ForDisplay"/> — the same convention the
/// shell's other copied charts use. Hover tooltips are kept; Lite's per-chart "Show Active Queries at
/// This Time" context-menu drill-down is NOT ported (matching every other viewer chart — the viewer has
/// no chart context menus), so these stay hover-only.
///
/// <para><b>The baseline-discontinuity markers (#3653 A5).</b> The identity-epoch carriers (#3694, #3705)
/// forget a server's delta baselines when the target restarts, fails over, is renamed or has its statistics
/// reset; the four series here are all delta-family rates, so across such an instant they show a step that
/// is the instrument re-baselining, not the workload. The tab reads the window's markers once
/// (<see cref="ViewerDataService.GetBaselineDiscontinuitiesAsync"/> — the same Storage read the MCP trend
/// tools publish as <c>discontinuities[]</c>) and each chart draws one dashed vertical line per marker through
/// <see cref="ChartStyle.AddDiscontinuityMarker"/>, legend-named with the shared
/// <see cref="BaselineDiscontinuities.Sentence"/> on the display clock. Drawn only on a chart that has a
/// series — an empty chart has no axis to place a line on and its placeholder already says why it is empty.
/// The Lite twin is <c>ServerTab.Charts.cs</c>' four trend charts, drawing through the same helper.</para>
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _queryDurationTrendHover;
    private ChartHoverHelper? _procDurationTrendHover;
    private ChartHoverHelper? _queryStoreDurationTrendHover;
    private ChartHoverHelper? _executionCountTrendHover;

    /// <summary>Themes the four trend charts up front and wires their hover tooltips (Lite's per-chart
    /// units). Called from <see cref="InitializeQueriesTab"/> so the charts don't flash white before the
    /// Performance Trends sub-tab's first load.</summary>
    private void InitializeQueryTrendCharts()
    {
        ApplyTheme(QueryDurationTrendChart);
        QueryDurationTrendChart.Refresh();
        ApplyTheme(ProcDurationTrendChart);
        ProcDurationTrendChart.Refresh();
        ApplyTheme(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Refresh();
        ApplyTheme(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Refresh();

        _queryDurationTrendHover = new ChartHoverHelper(QueryDurationTrendChart, "ms/sec");
        _procDurationTrendHover = new ChartHoverHelper(ProcDurationTrendChart, "ms/sec");
        _queryStoreDurationTrendHover = new ChartHoverHelper(QueryStoreDurationTrendChart, "ms/sec");
        _executionCountTrendHover = new ChartHoverHelper(ExecutionCountTrendChart, "/sec");
    }

    private void DisposeQueryTrendHelpers()
    {
        _queryDurationTrendHover?.Dispose();
        _procDurationTrendHover?.Dispose();
        _queryStoreDurationTrendHover?.Dispose();
        _executionCountTrendHover?.Dispose();
    }

    /// <summary>Loads the Performance Trends sub-tab: the four per-second trend reads run concurrently
    /// (NpgsqlDataSource pools a connection each), then each result renders into its chart.</summary>
    private async Task LoadPerformanceTrendsAsync(DateTime startUtc, DateTime endUtc)
    {
        using var readFanOut = ViewerReadFanOut.Of(4);

        var queryDurationTask = _dataService.GetQueryDurationTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        var procDurationTask = _dataService.GetProcedureDurationTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        var queryStoreDurationTask = _dataService.GetQueryStoreDurationTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        var executionCountTask = _dataService.GetExecutionCountTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);

        await Task.WhenAll(queryDurationTask, procDurationTask, queryStoreDurationTask, executionCountTask);

        /* The four are joined; the marker read below runs alone and must not be priced against a contention
           count that is over — release the declared width here rather than at the closing brace, as the
           Memory and PostgreSQL tabs do (ViewerCommandTimeoutTests.NoFanOutScope_OutlivesItsJoin pins it). */
        readFanOut.Release();

        /* #3653 A5: one read for the four charts, after the fan-out (the fan-out is sized at four), and
           never fatal to the tab — a store that cannot answer this read still draws its series, unmarked,
           rather than drawing nothing. */
        IReadOnlyList<BaselineDiscontinuity> discontinuities;
        try
        {
            discontinuities = await _dataService.GetBaselineDiscontinuitiesAsync(_server.ServerId, startUtc, endUtc);
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ServerTab", $"[{_server.DisplayName}] baseline-discontinuity read failed; trend charts drawn without markers: {ex.Message}");
            discontinuities = Array.Empty<BaselineDiscontinuity>();
        }

        UpdateQueryDurationTrendChart(queryDurationTask.Result, startUtc, endUtc, discontinuities);
        UpdateProcDurationTrendChart(procDurationTask.Result, startUtc, endUtc, discontinuities);
        UpdateQueryStoreDurationTrendChart(queryStoreDurationTask.Result, startUtc, endUtc, discontinuities);
        UpdateExecutionCountTrendChart(executionCountTask.Result, startUtc, endUtc, discontinuities);
    }

    /// <summary>
    /// Draws the window's baseline discontinuities on one trend chart (#3653 A5): a dashed vertical line per
    /// marker at its instant on the display clock, legend-named with the shared sentence. Called after the
    /// series is added and before the chart's axis limits and legend are set, as
    /// <see cref="ChartStyle.AddDiscontinuityMarker"/> asks.
    /// </summary>
    private static void MarkDiscontinuities(ScottPlot.WPF.WpfPlot chart, IReadOnlyList<BaselineDiscontinuity> discontinuities)
    {
        foreach (var discontinuity in discontinuities)
        {
            var shown = ViewerTimeHelper.ForDisplay(discontinuity.At);
            ChartStyle.AddDiscontinuityMarker(chart, shown.ToOADate(), BaselineDiscontinuities.Sentence(shown, discontinuity.Reason));
        }
    }

    /// <summary>
    /// What a routed trend chart says about what it served (#3653) — the chart-side form of the MCP payload's
    /// <c>source</c> / <c>effective_start</c> / <c>window_truncated</c> triple, rendered as the plot title the way
    /// the heatmap titles itself. Always names the tier, because a series read from the hourly rollup is a
    /// different measurement from one read per collection (bucket-width denominator, up to two hours behind
    /// the clock) and a user comparing two loads of the same chart across the raw horizon must be able to
    /// see that the resolution changed underneath them. Names the first served point only when the series
    /// is truncated — when the tier did not hold the window's head — which is the one case where the axis
    /// (the requested window) and the data (what the store still had) disagree and the chart would
    /// otherwise present four days as seven. The same #1661 disclosure the FinOps expensive-queries panel
    /// makes for its text horizon: say what was served rather than presenting a slice as the whole. The
    /// tier is spelled with the payload's own word (<see cref="QueryTrendSeries.Source"/>: <c>raw</c> /
    /// <c>hourly</c>) so a user reading the chart and an agent reading <c>get_query_duration_trend</c> are
    /// told the same thing in the same vocabulary; the parenthetical says what the word means on a chart.
    /// A raw series whose buckets merged collections (<see cref="QueryTrendSeries.BucketMinutes"/> &gt; 0, #4234)
    /// names its width instead of claiming one point per collection.
    /// </summary>
    internal static string DescribeTrendCoverage(QueryTrendSeries series)
        => ComposeTrendCoverage(
            series.Tier == PerformanceMonitor.Darling.Storage.RetentionTier.Raw
                ? series.BucketMinutes > 0
                    ? $"{series.Source} (one point per {DescribeBucketWidth(series.BucketMinutes)})"
                    : $"{series.Source} (one point per collection)"
                : $"{series.Source} rollup (one point per hour)",
            series.Truncated ? series.EffectiveStartUtc : null,
            "the store no longer holds the rest of this window at this tier");

    /// <summary>The bucket width named in a raw trend's coverage title (#4234): minutes below an hour, whole hours from it.</summary>
    private static string DescribeBucketWidth(int minutes)
        => minutes == 1 ? "minute" : minutes % 60 == 0 ? (minutes == 60 ? "hour" : $"{minutes / 60} hours") : $"{minutes} minutes";

    /// <summary>
    /// The Query Store duration chart's coverage title (#3653) — the same idiom as
    /// <see cref="DescribeTrendCoverage"/>, through the same composer, with this trend's own facts in the two
    /// slots. The tier word is the payload's (<see cref="QueryStoreTrendSeries.Source"/>: <c>rollup+raw</c> /
    /// <c>raw</c>) and the parenthetical says what a point IS on each route, because #2736's seam changes the
    /// grain mid-series: one point per collection-hour bucket below the watermark, one per Query Store
    /// interval from it — the payload's <c>bucket</c> sentence, on the chart. The "data begins" clause hangs
    /// on <see cref="QueryStoreTrendSeries.HeadUnserved"/> — the rollup's measured floor sitting above the
    /// requested start — and its reason is this route's own: the corrected rollup has not MATERIALIZED the
    /// head (the rows may well exist and were deliberately not ranked, #2736), so the remedy is named, where
    /// the sibling charts' reason is a tier that no longer HOLDS the head. Same shape, honest words. Before
    /// this the chart carried no title at all, and a seven-day chart on a never-backfilled store plotted what
    /// the rollup had under an axis that said seven — the exact defect #3666 removed from the three charts
    /// beside it.
    /// </summary>
    internal static string DescribeQueryStoreTrendCoverage(QueryStoreTrendSeries series)
        => ComposeTrendCoverage(
            series.Route.UseRollup
                ? $"{series.Source} (one point per hour before {ViewerTimeHelper.ForDisplay(series.Route.RawStartUtc):yyyy-MM-dd HH:mm}, one per Query Store interval from it)"
                : $"{series.Source} (one point per Query Store interval)",
            series.HeadUnserved ? series.EffectiveStartUtc : null,
            "the corrected Query Store rollup has not materialized the rest of this window (--backfill-rollups reaches it)");

    /// <summary>
    /// The one composition every trend title here goes through (#3653): <c>Source: {what served}</c>, and only
    /// when the series' head was not served, <c>— data begins {head}; {why}</c>. One method so the two series
    /// types cannot drift in the shape of the sentence — the word order, the dash, the timestamp format —
    /// while each says its own truth in the two slots. Terse in the common case on purpose: a long banner on
    /// every chart teaches the eye to skip the one that matters.
    /// </summary>
    private static string ComposeTrendCoverage(string served, DateTime? dataBeginsUtc, string whyUnserved)
    {
        if (dataBeginsUtc is not DateTime head)
        {
            return $"Source: {served}";
        }

        var from = ViewerTimeHelper.ForDisplay(head);
        return $"Source: {served} — data begins {from:yyyy-MM-dd HH:mm}; {whyUnserved}";
    }

    /// <summary>Puts <see cref="DescribeTrendCoverage"/> on a chart as its title, coloured like its tick
    /// labels (the heatmap's title idiom) so it reads as chart chrome rather than as a series.</summary>
    private static void ShowTrendCoverage(ScottPlot.WPF.WpfPlot chart, QueryTrendSeries series)
        => ShowTrendCoverageTitle(chart, DescribeTrendCoverage(series));

    /// <summary>The Query Store chart's arm of <see cref="ShowTrendCoverage(ScottPlot.WPF.WpfPlot, QueryTrendSeries)"/> (#3653).</summary>
    private static void ShowTrendCoverage(ScottPlot.WPF.WpfPlot chart, QueryStoreTrendSeries series)
        => ShowTrendCoverageTitle(chart, DescribeQueryStoreTrendCoverage(series));

    private static void ShowTrendCoverageTitle(ScottPlot.WPF.WpfPlot chart, string coverage)
    {
        chart.Plot.Title(coverage);
        chart.Plot.Axes.Title.Label.ForeColor = chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
        chart.Plot.Axes.Title.Label.FontSize = 11;
        chart.Plot.Axes.Title.Label.Bold = false;
    }

    private void UpdateQueryDurationTrendChart(QueryTrendSeries series, DateTime startUtc, DateTime endUtc, IReadOnlyList<BaselineDiscontinuity> discontinuities)
    {
        var data = series.Points;
        ClearChart(QueryDurationTrendChart);
        ApplyTheme(QueryDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryDurationTrendChart, "Query Duration", "Duration (ms/sec)"); return; }
        ShowTrendCoverage(QueryDurationTrendChart, series);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();
        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _queryDurationTrendHover?.Clear();
        var plot = QueryDurationTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Query Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("QueryDuration"));
        ChartStyle.StyleScatter(plot);
        _queryDurationTrendHover?.Add(plot, "Query Duration");
        MarkDiscontinuities(QueryDurationTrendChart, discontinuities);

        QueryDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        QueryDurationTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(QueryDurationTrendChart);
        QueryDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryDurationTrendChart);
        QueryDurationTrendChart.Refresh();
    }

    private void UpdateProcDurationTrendChart(QueryTrendSeries series, DateTime startUtc, DateTime endUtc, IReadOnlyList<BaselineDiscontinuity> discontinuities)
    {
        var data = series.Points;
        ClearChart(ProcDurationTrendChart);
        ApplyTheme(ProcDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ProcDurationTrendChart, "Procedure Duration", "Duration (ms/sec)"); return; }
        ShowTrendCoverage(ProcDurationTrendChart, series);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();
        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _procDurationTrendHover?.Clear();
        var plot = ProcDurationTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Procedure Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("ProcedureDuration"));
        ChartStyle.StyleScatter(plot);
        _procDurationTrendHover?.Add(plot, "Procedure Duration");
        MarkDiscontinuities(ProcDurationTrendChart, discontinuities);

        ProcDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ProcDurationTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(ProcDurationTrendChart);
        ProcDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(ProcDurationTrendChart, 0, values.Max());
        ShowChartLegend(ProcDurationTrendChart);
        ProcDurationTrendChart.Refresh();
    }

    private void UpdateQueryStoreDurationTrendChart(QueryStoreTrendSeries series, DateTime startUtc, DateTime endUtc, IReadOnlyList<BaselineDiscontinuity> discontinuities)
    {
        var data = series.Points;
        ClearChart(QueryStoreDurationTrendChart);
        ApplyTheme(QueryStoreDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryStoreDurationTrendChart, "Query Store Duration", "Duration (ms/sec)"); return; }
        ShowTrendCoverage(QueryStoreDurationTrendChart, series);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();
        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _queryStoreDurationTrendHover?.Clear();
        var plot = QueryStoreDurationTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Query Store Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("QueryStoreDuration"));
        ChartStyle.StyleScatter(plot);
        _queryStoreDurationTrendHover?.Add(plot, "Query Store Duration");
        MarkDiscontinuities(QueryStoreDurationTrendChart, discontinuities);

        QueryStoreDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        QueryStoreDurationTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryStoreDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Refresh();
    }

    private void UpdateExecutionCountTrendChart(QueryTrendSeries series, DateTime startUtc, DateTime endUtc, IReadOnlyList<BaselineDiscontinuity> discontinuities)
    {
        var data = series.Points;
        ClearChart(ExecutionCountTrendChart);
        ApplyTheme(ExecutionCountTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ExecutionCountTrendChart, "Executions", "Executions/sec"); return; }
        ShowTrendCoverage(ExecutionCountTrendChart, series);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();
        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _executionCountTrendHover?.Clear();
        var plot = ExecutionCountTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Executions";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Executions"));
        ChartStyle.StyleScatter(plot);
        _executionCountTrendHover?.Add(plot, "Executions");
        MarkDiscontinuities(ExecutionCountTrendChart, discontinuities);

        ExecutionCountTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ExecutionCountTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Plot.YLabel("Executions/sec");
        SetChartYLimitsWithLegendPadding(ExecutionCountTrendChart, 0, values.Max());
        ShowChartLegend(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Refresh();
    }
}
