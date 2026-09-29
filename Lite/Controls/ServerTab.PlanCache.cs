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
using System.Threading.Tasks;
using System.Windows.Controls;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The Plan Cache tab — the Lite port of the Darling viewer's Plan Cache surface (itself the
/// Dashboard-parity port of Memory &gt; Plan Cache). The trend chart plots single-use vs multi-use
/// plan-cache size (MB) over the settable window (the single-use bloat signal, the Dashboard's Plan Cache
/// chart shape), and the summary strip shows total plans + oldest-plan age + a derived single-use bloat
/// badge. Loads on tab activation via the RefreshVisibleTabAsync switch. The two series ride the same shared
/// <see cref="ChartPalette"/> keys ("SinglePagePlans" / "MultiPagePlans") the Dashboard uses, for
/// cross-app color identity; chrome flows through the shared ChartStyle + Y-floor-at-0 helpers.
/// </summary>
public partial class ServerTab : UserControl
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
    /// Loads the Plan Cache tab over the toolbar's settable window: the size trend and the (uncapped)
    /// summary totals read concurrently, then the chart and summary strip render. The summary strip's
    /// totals come from a dedicated uncapped aggregate, so "Total Plans" is exact.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshPlanCacheAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var trendTask = Helpers.MethodProfiler.TimeAsync("PlanCache.Trend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetPlanCacheTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var summaryTask = Helpers.MethodProfiler.TimeAsync("PlanCache.Summary", () => Task.Run(() => _dataService.GetPlanCacheSummaryAsync(_serverId, hoursBack, fromDate, toDate)));

            await System.Threading.Tasks.Task.WhenAll(trendTask, summaryTask);

            UpdatePlanCacheChart(trendTask.Result, hoursBack, fromDate, toDate);
            RenderPlanCacheSummary(summaryTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshPlanCacheAsync failed: {ex.Message}");
        }
    }

    /// <summary>The plan-cache size trend: a single-use series and a multi-use series (MB), on the shared
    /// "SinglePagePlans" / "MultiPagePlans" palette keys, a labelled zero-window when there is no data.
    /// Mirrors Darling's RenderPlanCacheChart.</summary>
    private void UpdatePlanCacheChart(List<PlanCacheTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(PlanCacheChart);
        ApplyTheme(PlanCacheChart);
        _planCacheHover?.Clear();

        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            (rangeStart, rangeEnd) = GetChartWindow(hoursBack, null, null);
        }

        double globalMax = 0;
        if (data.Count > 0)
        {
            var ordered = data.OrderBy(d => d.CollectionTime).ToList();
            var times = ordered.Select(d => ToServerLocal(d.CollectionTime).ToOADate()).ToArray();

            var singleUse = ordered.Select(d => d.SingleUseSizeMb).ToArray();
            var singlePlot = PlanCacheChart.Plot.Add.TimeSeries(times, singleUse);
            singlePlot.LegendText = "Single-Use";
            singlePlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SinglePagePlans"));
            ChartStyle.StyleScatter(singlePlot);
            _planCacheHover?.Add(singlePlot, "Single-Use");

            var multiUse = ordered.Select(d => d.MultiUseSizeMb).ToArray();
            var multiPlot = PlanCacheChart.Plot.Add.TimeSeries(times, multiUse);
            multiPlot.LegendText = "Multi-Use";
            multiPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("MultiPagePlans"));
            ChartStyle.StyleScatter(multiPlot);
            _planCacheHover?.Add(multiPlot, "Multi-Use");

            globalMax = Math.Max(singleUse.DefaultIfEmpty(0).Max(), multiUse.DefaultIfEmpty(0).Max());
        }

        PlanCacheChart.Plot.Axes.DateTimeTicksBottomDateChange();
        PlanCacheChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(PlanCacheChart);
        PlanCacheChart.Plot.YLabel("Plan Cache Size (MB)");
        SetChartYLimitsWithLegendPadding(PlanCacheChart, 0, globalMax > 0 ? globalMax : 10);
        ShowChartLegend(PlanCacheChart);
        PlanCacheChart.Refresh();
    }

    /// <summary>The summary strip: TRUE total plans at the latest snapshot (uncapped, from the dedicated
    /// aggregate), the oldest cached plan's age (a plan-cache-stability signal — older = more stable), and
    /// the derived single-use bloat badge + forced-parameterization hint. Mirrors Darling's
    /// RenderPlanCacheSummary.</summary>
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
           parity): a pure client-side CASE on the single-use / total ratio the reader already returns. */
        var bloat = LocalDataService.ClassifyPlanCacheBloat(summary.TotalPlans, summary.SingleUsePlans);
        PlanCacheBloatLevelText.Text = bloat.Level;
        PlanCacheBloatLevelText.Foreground = BloatLevelBrush(bloat.Level);
        PlanCacheRecommendationText.Text = bloat.Recommendation;

        PlanCacheTotalPlansText.Text = summary.TotalPlans.ToString("N0");

        /* oldest_plan_create_time comes from a DMV (sys.dm_exec_query_stats.creation_time) in the monitored
           server's local clock, which the viewer — having no per-server offset for this DMV column —
           measures against UtcNow: exact for a UTC server, off by the server's offset otherwise. Age is a
           coarse d/h/m stability bucket, so a few hours' offset rarely changes the qualitative read. */
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
