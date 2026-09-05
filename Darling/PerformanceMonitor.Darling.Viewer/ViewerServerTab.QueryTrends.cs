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

        UpdateQueryDurationTrendChart(queryDurationTask.Result, startUtc, endUtc);
        UpdateProcDurationTrendChart(procDurationTask.Result, startUtc, endUtc);
        UpdateQueryStoreDurationTrendChart(queryStoreDurationTask.Result, startUtc, endUtc);
        UpdateExecutionCountTrendChart(executionCountTask.Result, startUtc, endUtc);
    }

    private void UpdateQueryDurationTrendChart(List<QueryTrendPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(QueryDurationTrendChart);
        ApplyTheme(QueryDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryDurationTrendChart, "Query Duration", "Duration (ms/sec)"); return; }

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

        QueryDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        QueryDurationTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(QueryDurationTrendChart);
        QueryDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryDurationTrendChart);
        QueryDurationTrendChart.Refresh();
    }

    private void UpdateProcDurationTrendChart(List<QueryTrendPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(ProcDurationTrendChart);
        ApplyTheme(ProcDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ProcDurationTrendChart, "Procedure Duration", "Duration (ms/sec)"); return; }

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

        ProcDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ProcDurationTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(ProcDurationTrendChart);
        ProcDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(ProcDurationTrendChart, 0, values.Max());
        ShowChartLegend(ProcDurationTrendChart);
        ProcDurationTrendChart.Refresh();
    }

    private void UpdateQueryStoreDurationTrendChart(List<QueryTrendPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(QueryStoreDurationTrendChart);
        ApplyTheme(QueryStoreDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryStoreDurationTrendChart, "Query Store Duration", "Duration (ms/sec)"); return; }

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

        QueryStoreDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        QueryStoreDurationTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryStoreDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Refresh();
    }

    private void UpdateExecutionCountTrendChart(List<QueryTrendPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(ExecutionCountTrendChart);
        ApplyTheme(ExecutionCountTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ExecutionCountTrendChart, "Executions", "Executions/sec"); return; }

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

        ExecutionCountTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ExecutionCountTrendChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Plot.YLabel("Executions/sec");
        SetChartYLimitsWithLegendPadding(ExecutionCountTrendChart, 0, values.Max());
        ShowChartLegend(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Refresh();
    }
}
