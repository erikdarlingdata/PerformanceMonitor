/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #4966: the "Showing since" notice on the two Queries-tab surfaces that hide where their data starts, Plan
/// Corrections and the Query Heatmap. Each call hands the shared step
/// (<c>RefreshWindowTruncatedBannerAsync</c> in ServerTab.Refresh.cs, which probes
/// <see cref="LocalDataService.GetQueryWindowFloorAsync"/> and words the banner through
/// <see cref="ApplyWindowFloorToBanner"/>) the SAME UTC window the surface's own read takes.
/// </summary>
public partial class ServerTab
{
    /// <summary>
    /// Plan Corrections (<c>v_plan_correction</c>, coverage from the <c>plan_correction</c> collector's runs: the
    /// table holds a row only while the engine has a recommendation, so a quiet first stretch is not a gap in what
    /// was collected). Called after the grid is bound, at the sub-tab switch and at the full refresh.
    /// <see cref="LocalDataService.GetPlanCorrectionsAsync"/> goes through <c>GetTimeRange</c> like the three
    /// Queries grids, so the banner takes the pair <see cref="LocalDataService.GetQueriesTabWindowUtc"/> hands them.
    /// </summary>
    private System.Threading.Tasks.Task RefreshPlanCorrectionsBannerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        return RefreshWindowTruncatedBannerAsync(QueryWindowRelation.PlanCorrection, PlanCorrectionsWindowTruncatedBanner, windowStart, windowEnd);
    }

    /// <summary>
    /// The Query Heatmap draws one column per 5-minute bucket that holds a row, and its X axis counts those columns,
    /// so a range that starts before the stored rows draws no empty span the way a time-axis chart does. It reads
    /// <c>v_query_stats</c> like the Top Queries grid, so it asks the same <see cref="QueryWindowRelation.QueryStats"/>
    /// question over the same window. Called after the chart is drawn, at the sub-tab switch, at the full refresh and
    /// when the metric changes (the metric re-reads over the tab's current window).
    /// </summary>
    private System.Threading.Tasks.Task RefreshQueryHeatmapBannerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        return RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStats, QueryHeatmapWindowTruncatedBanner, windowStart, windowEnd);
    }
}
