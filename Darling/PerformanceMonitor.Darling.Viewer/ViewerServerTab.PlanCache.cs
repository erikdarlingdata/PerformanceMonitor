/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Plan Cache sub-tab (under the Memory tab, matching the Dashboard's Memory &gt; Plan Cache) — the
/// Darling-viewer surface for the plan_cache_stats collector. The trend chart plots single-use vs
/// multi-use plan-cache size (MB) over the settable window (the single-use bloat signal, the Dashboard's
/// Plan Cache chart shape), and the summary strip shows total plans + oldest-plan age. Loaded from
/// <c>LoadMemoryAsync</c> alongside the other Memory sub-tabs (the Memory tab's full-refresh branch). Chart
/// chrome flows through
/// the shared <see cref="ChartStyle"/> / <see cref="ChartPalette"/> and the ChartHelpers bridge, so the
/// Y-floor-at-0 fix applies, and the two series ride the same palette keys the Dashboard uses for cross-app
/// color identity.
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _planCacheHover;

    /// <summary>Applies the shared chrome + hover to the plan-cache chart up front (constructor).</summary>
    private void InitializePlanCacheChart()
    {
        ApplyTheme(PlanCacheChart);
        PlanCacheChart.Refresh();
        _planCacheHover = new ChartHoverHelper(PlanCacheChart, "MB");
    }

    /// <summary>
    /// Loads the Plan Cache sub-tab over the toolbar's settable window: the size trend and the summary
    /// totals read concurrently, then the chart and summary strip render. Called from
    /// <see cref="LoadMemoryAsync"/> (the Memory tab loads all its sub-tabs on activation).
    /// </summary>
    private async Task LoadPlanCacheAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(2);

        var trendTask = _dataService.GetPlanCacheTrendAsync(_server.ServerId, startUtc, endUtc);
        /* The summary strip's totals come from a dedicated uncapped aggregate (not a capped grid),
           so "Total Plans" is exact (Dashboard parity). */
        var summaryTask = _dataService.GetPlanCacheSummaryAsync(_server.ServerId, startUtc, endUtc);
        await Task.WhenAll(trendTask, summaryTask);

        RenderPlanCacheChart(trendTask.Result);
        RenderPlanCacheSummary(summaryTask.Result);
    }

    private void RenderPlanCacheChart(List<PlanCacheTrendPoint> data)
    {
        ClearChart(PlanCacheChart);
        _planCacheHover?.Clear();
        ApplyTheme(PlanCacheChart);

        var (startUtc, endUtc) = GetWindowUtc();
        PlanCacheChart.Plot.YLabel("Plan Cache Size (MB)");

        double globalMax = 0;
        if (data.Count > 0)
        {
            var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();

            var singleUse = data.Select(d => d.SingleUseSizeMb).ToArray();
            var singlePlot = PlanCacheChart.Plot.Add.TimeSeries(times, singleUse);
            singlePlot.LegendText = "Single-Use";
            singlePlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SinglePagePlans"));
            ChartStyle.StyleScatter(singlePlot);
            _planCacheHover?.Add(singlePlot, "Single-Use");

            var multiUse = data.Select(d => d.MultiUseSizeMb).ToArray();
            var multiPlot = PlanCacheChart.Plot.Add.TimeSeries(times, multiUse);
            multiPlot.LegendText = "Multi-Use";
            multiPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("MultiPagePlans"));
            ChartStyle.StyleScatter(multiPlot);
            _planCacheHover?.Add(multiPlot, "Multi-Use");

            globalMax = Math.Max(singleUse.DefaultIfEmpty(0).Max(), multiUse.DefaultIfEmpty(0).Max());
        }

        PlanCacheChart.Plot.Axes.DateTimeTicksBottomDateChange();
        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc);
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc);
        PlanCacheChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(PlanCacheChart);
        SetChartYLimitsWithLegendPadding(PlanCacheChart, 0, globalMax > 0 ? globalMax : 10);
        ShowChartLegend(PlanCacheChart);
        PlanCacheChart.Refresh();
    }

    /// <summary>The summary strip: TRUE total plans at the latest snapshot (uncapped, from the dedicated
    /// aggregate) + the oldest cached plan's age (a plan-cache-stability signal — older = more stable),
    /// mirroring the Dashboard's Plan Cache summary.</summary>
    private void RenderPlanCacheSummary(PlanCacheSummary summary)
    {
        if (summary.TotalPlans <= 0 && summary.OldestPlanCreateTime is null)
        {
            PlanCacheTotalPlansText.Text = "--";
            PlanCacheOldestPlanText.Text = "--";
            PlanCacheBloatLevelText.Text = "--";
            PlanCacheBloatLevelText.Foreground = System.Windows.Media.Brushes.Gray;
            PlanCacheRecommendationText.Text = "";
            return;
        }

        /* Derived single-use bloat badge + forced-parameterization hint (install/47 report.plan_cache_bloat
           parity): a pure client-side CASE on the single-use / total ratio Darling already reads. */
        var bloat = ViewerDataService.ClassifyPlanCacheBloat(summary.TotalPlans, summary.SingleUsePlans);
        PlanCacheBloatLevelText.Text = bloat.Level;
        PlanCacheBloatLevelText.Foreground = BloatLevelBrush(bloat.Level);
        PlanCacheRecommendationText.Text = bloat.Recommendation;

        PlanCacheTotalPlansText.Text = summary.TotalPlans.ToString("N0", CultureInfo.CurrentCulture);

        /* oldest_plan_create_time comes from a DMV (sys.dm_exec_query_stats.creation_time) in the monitored
           server's local clock, which the viewer — having no per-server offset — measures against UtcNow:
           exact for a UTC server, off by the server's offset otherwise (the same approximation the viewer
           already accepts for server-local sample_time; a reliable de-skew is deferred). Age is a coarse
           d/h/m stability bucket, so a few hours' offset rarely changes the qualitative read. */
        if (summary.OldestPlanCreateTime is not { } oldest)
        {
            PlanCacheOldestPlanText.Text = "--";
            return;
        }

        var age = DateTime.UtcNow - DateTime.SpecifyKind(oldest, DateTimeKind.Utc);
        if (age < TimeSpan.Zero) age = TimeSpan.Zero;
        PlanCacheOldestPlanText.Text = age.TotalDays >= 1
            ? $"{(int)age.TotalDays}d {age.Hours}h"
            : age.TotalHours >= 1
                ? $"{age.Hours}h {age.Minutes}m"
                : $"{age.Minutes}m";
    }

    /// <summary>Maps a plan-cache bloat level to its badge colour (CRITICAL red → HIGH orange → MEDIUM
    /// goldenrod → NORMAL green), mirroring the severity palette the warning highlights use elsewhere.</summary>
    private static System.Windows.Media.SolidColorBrush BloatLevelBrush(string level)
    {
        var hex = level switch
        {
            "CRITICAL" => "#FF6B6B",
            "HIGH" => "#FFA94D",
            "MEDIUM" => "#E0C341",
            _ => "#4CAF50",
        };
        var brush = new System.Windows.Media.SolidColorBrush(
            (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString(hex));
        brush.Freeze();
        return brush;
    }

    /// <summary>Tears down the plan-cache hover helper (mirrors the other tabs' dispose).</summary>
    public void DisposePlanCacheHelpers()
    {
        _planCacheHover?.Dispose();
    }
}
