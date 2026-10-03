/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #4966: the "Showing since" notice on the surfaces that hide where their data starts: Plan Corrections and the Query
/// Heatmap on the Queries tab, and Memory Pressure Events on the Memory tab. Each call hands the shared step
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
    /// The grid reads only the newest <see cref="LocalDataService.PlanCorrectionGridCap"/> rows, so the banner goes
    /// through the cap-aware step: a read that hit the cap is worded from the oldest row it returned, even where the
    /// store covers the range.
    /// </summary>
    private System.Threading.Tasks.Task RefreshPlanCorrectionsBannerAsync(IReadOnlyCollection<PlanCorrectionRow> planCorrections, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        return RefreshCappedGridBannerAsync(QueryWindowRelation.PlanCorrection, PlanCorrectionsWindowTruncatedBanner, windowStart, windowEnd, planCorrections, LocalDataService.PlanCorrectionGridCap, row => row.CollectionTime);
    }

    /// <summary>
    /// The Query Heatmap draws one column per 5-minute bucket across the asked range (<see cref="LocalDataService.HeatmapColumns"/>),
    /// so a gap in the data is empty columns. The notice stays because an empty column cannot tell a server that did
    /// not exist yet from one that ran nothing. It reads <c>v_query_stats</c> like the Top Queries grid, so it asks the
    /// same <see cref="QueryWindowRelation.QueryStats"/> question over the same window. Called after the chart is
    /// drawn, at the sub-tab switch, at the full refresh and when the metric changes (the metric re-reads over the
    /// tab's current window).
    /// </summary>
    private System.Threading.Tasks.Task RefreshQueryHeatmapBannerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        return RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStats, QueryHeatmapWindowTruncatedBanner, windowStart, windowEnd);
    }

    /// <summary>
    /// Memory Pressure Events (<c>v_memory_pressure_events</c>, windowed on <c>sample_time</c> like the chart, with
    /// coverage from the <c>memory_pressure_events</c> collector's runs). The chart draws bars only where pressure was
    /// recorded, so a span with no data looked like a span with no pressure. <see cref="LocalDataService.GetMemoryPressureEventsAsync"/>
    /// goes through the same <c>GetTimeRange</c> call as the Queries grids, so the banner takes the same pair.
    /// </summary>
    private System.Threading.Tasks.Task RefreshMemoryPressureEventsBannerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        return RefreshWindowTruncatedBannerAsync(QueryWindowRelation.MemoryPressureEvents, MemoryPressureEventsWindowTruncatedBanner, windowStart, windowEnd);
    }
}
