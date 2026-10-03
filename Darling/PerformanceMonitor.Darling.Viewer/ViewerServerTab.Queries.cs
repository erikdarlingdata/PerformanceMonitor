/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Queries inner tab's sub-tab dispatch — TEN sub-tabs, counted off <c>QueriesSubTabControl</c> in
/// <c>ViewerServerTab.xaml</c> and matching the index constants below one for one. Seven are Lite's:
/// Performance Trends and Active Queries (W1f-2), the Top Queries / Top Procedures / Query Store grids
/// (W1f-1), Plan Corrections (#1952), and Query Heatmap last (W1f-2). Three are Darling-only — the LIVE
/// Current Active Queries tab, Query Store Regressions (Dashboard parity) and Query Store Clutter (#3797).
/// Copied from Lite's <c>ServerTab</c> (Refresh / Slicers / Grids partials) with reads rewired to the
/// <see cref="ViewerDataService"/> Postgres reads. A sub-tab switch reloads through the shell's
/// overlap-guarded <see cref="RefreshActiveInnerTabAsync"/> (the Queries tab is the active inner tab
/// whenever its sub-tabs are visible), and <see cref="LoadQueriesAsync"/> loads only the newly-visible
/// sub-tab (Lite's subTabOnly gating) over the toolbar's settable window. The three grids each carry a UTC
/// time-range slicer whose drag re-reads over the selection; sorting a grid re-labels the slicer's
/// aggregate curve (Lite's *Grid_Sorting). The Performance Trends charts / Active Queries grid+slicer /
/// Query Heatmap live in their own partials (<c>ViewerServerTab.QueryTrends.cs</c> /
/// <c>.ActiveQueries.cs</c> / <c>.QueryHeatmap.cs</c>). The grids also carry the slicer overlay-on-select
/// (#1409, <c>ViewerServerTab.SlicerOverlay.cs</c>) and the per-row double-click history windows
/// (<c>ViewerServerTab.History.cs</c>), so all four Lite interactions — drag-to-narrow, sort-driven metric
/// re-labeling, overlay-on-select, and double-click-for-history — are wired.
/// </summary>
public partial class ViewerServerTab
{
    /* Queries sub-tab order — matches Lite's QueriesSubTabControl (W1f-2), keeping Query Heatmap last, the
       Darling-only LIVE "Current Active Queries" tab inserted right after the stored "Active Queries" tab,
       the Query Store Regressions grid (Dashboard parity) inserted right after Query Store — matching
       the Dashboard's Query Store → Query Store Regressions adjacency — and the Darling-only Query Store
       Clutter grid (#3797) after that: Performance Trends, Active Queries, Current Active Queries (live),
       the three grids, Query Store Regressions, Query Store Clutter, Plan Corrections, Query Heatmap. Every
       reference below uses the NAMED constant, so inserting a tab only shifts these values — no
       literal-index caller needs touching. */
    private const int PerformanceTrendsSubTabIndex = 0;
    private const int ActiveQueriesSubTabIndex = 1;
    private const int CurrentActiveQueriesSubTabIndex = 2;
    private const int TopQueriesSubTabIndex = 3;
    private const int TopProceduresSubTabIndex = 4;
    private const int QueryStoreSubTabIndex = 5;
    private const int QueryStoreRegressionsSubTabIndex = 6;
    /* #3797, inserted after Query Store Regressions: the per-database clutter view answers the question the
       two Query Store grids above raise, and it is Darling-only for now — Lite has every input and no port
       (CrossAppMcpToolInventoryPinTests.KnownLiteMissingMcpTools carries the port, written out). */
    private const int QueryStoreClutterSubTabIndex = 7;
    private const int PlanCorrectionsSubTabIndex = 8;
    private const int QueryHeatmapSubTabIndex = 9;

    private string _queryStatsSlicerMetric = "TotalCpu";
    private List<TimeSliceBucket>? _queryStatsSlicerData;
    private string _procStatsSlicerMetric = "TotalCpu";
    private List<TimeSliceBucket>? _procStatsSlicerData;
    private string _queryStoreSlicerMetric = "TotalCpu";
    private List<TimeSliceBucket>? _queryStoreSlicerData;

    /// <summary>
    /// Wires the Queries tab up-front (from the constructor, after InitializeComponent): the three grids'
    /// in-grid bar-cell maxima recompute hook (<see cref="SetupBarCellMaxes"/>) and each slicer's
    /// RangeChanged handler (dragging a slicer re-reads its grid over the selection).
    /// </summary>
    private void InitializeQueriesTab()
    {
        SetupBarCellMaxes();

        QueryStatsSlicer.RangeChanged += OnQueryStatsSlicerChanged;
        ProcStatsSlicer.RangeChanged += OnProcStatsSlicerChanged;
        QueryStoreSlicer.RangeChanged += OnQueryStoreSlicerChanged;

        /* W1f-2 sub-tabs: theme the trend + heatmap charts up front, build the heatmap hover popup +
           drill-down menu, and wire the Active Queries slicer (each in its own partial). */
        InitializeQueryTrendCharts();
        InitializeActiveQueriesTab();
        InitializeQueryHeatmap();

        /* Default sub-tab is Performance Trends (no comparison), so start the Compare combo disabled. */
        UpdateCompareDropdownState();
    }

    /// <summary>Tears down the W1f-2 sub-tabs' chart hover helpers — forwarded from the tab's single
    /// <see cref="Dispose"/> so the whole tab tears down through one path.</summary>
    private void DisposeQueriesTabHelpers()
    {
        DisposeQueryTrendHelpers();
    }

    /// <summary>
    /// A Queries sub-tab switch reloads through the shell's overlap-guarded
    /// <see cref="RefreshActiveInnerTabAsync"/> (mirrors <see cref="BlockingSubTabs_SelectionChanged"/>).
    /// Gated on <see cref="System.Windows.FrameworkElement.IsLoaded"/> and the sub-TabControl's own
    /// selection so build-time and bubbled selections (the child grids, the Compare combo) are ignored.
    /// </summary>
    private async void QueriesSubTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, QueriesSubTabControl) || !IsLoaded)
        {
            return;
        }

        /* Comparison applies only to the three grid sub-tabs; disable the Compare combo elsewhere. */
        UpdateCompareDropdownState();

        /* A drill-down (heatmap / per-chart / Overview lane) switches to Active Queries programmatically and
           loads its own filtered snapshot; skip the auto-refresh so it doesn't clobber that via an async race
           (the guard is set/cleared around the tab switches in NavigateToActiveQueriesForWindowAsync). */
        if (_suppressDrillDownAutoRefresh)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Loads the Queries tab's ACTIVE sub-tab only (Lite's subTabOnly gating): Top Queries / Top
    /// Procedures / Query Store each read their grid + slicer + comparison over the toolbar's settable window.
    /// The shell's <see cref="LoadInnerTabAsync"/> owns the try/catch that surfaces failures.
    /// </summary>
    private async Task LoadQueriesAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        switch (QueriesSubTabControl.SelectedIndex)
        {
            case PerformanceTrendsSubTabIndex:
                await LoadPerformanceTrendsAsync(startUtc, endUtc);
                break;
            case ActiveQueriesSubTabIndex:
                await LoadActiveQueriesAsync(startUtc, endUtc);
                break;
            case CurrentActiveQueriesSubTabIndex:
                /* LIVE, on-demand only — never auto-fetch on tab selection or a toolbar-range refresh (a live
                   server hit is an explicit operator action). The Refresh button drives the fetch; the tab keeps
                   its last snapshot / hint until then. */
                break;
            case TopProceduresSubTabIndex:
                await LoadTopProceduresAsync(startUtc, endUtc);
                break;
            case QueryStoreSubTabIndex:
                await LoadQueryStoreAsync(startUtc, endUtc);
                break;
            case QueryStoreRegressionsSubTabIndex:
                await LoadQueryStoreRegressionsAsync(startUtc, endUtc);
                break;
            case QueryStoreClutterSubTabIndex:
                await LoadQueryStoreClutterAsync(startUtc, endUtc);
                break;
            case PlanCorrectionsSubTabIndex:
                await LoadPlanCorrectionsAsync(startUtc, endUtc);
                break;
            case QueryHeatmapSubTabIndex:
                await LoadQueryHeatmapAsync(startUtc, endUtc);
                break;
            case TopQueriesSubTabIndex:
            default:
                await LoadTopQueriesAsync(startUtc, endUtc);
                break;
        }
    }

    private async Task LoadTopQueriesAsync(DateTime startUtc, DateTime endUtc)
    {
        var floorTask = _dataService.GetQueryStatsWindowFloorAsync(_server.ServerId, startUtc, endUtc);
        var (rows, tier) = await _dataService.GetTopQueriesByCpuTierAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _queryStatsFilterMgr!.UpdateData(rows);
        SetDefaultSortIfNone(QueryStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
        /* #4231 stage 3: an hourly-routed page holds no per-caller detail (see
           ViewerDataService.GetTopQueriesByCpuTierAsync) — the raw-floor banner (#4231 stage 1/2) and this
           tier disclosure are independent facts, so both may show at once (a window aged past raw AND
           routed to hourly). */
        UpdateTruncationBanner(QueryStatsTruncationBanner, await DataStartOrNullAsync(floorTask, "Query Stats"), startUtc, tier == "hourly" ? HourlyTierSuffix : null);
        await LoadQueryStatsSlicerAsync(startUtc, endUtc);
        await RefreshQueryStatsComparisonAsync(startUtc, endUtc);
    }

    /// <summary>#4231 stage 3: the Queries-tab grid header's hourly-routing disclosure — appended to the
    /// existing "Showing since" banner (#4278) rather than a new widget, per the lane's ruling.</summary>
    private const string HourlyTierSuffix = " — aggregated hourly, per-caller detail unavailable";

    private async Task LoadTopProceduresAsync(DateTime startUtc, DateTime endUtc)
    {
        var floorTask = _dataService.GetProcedureStatsWindowFloorAsync(_server.ServerId, startUtc, endUtc);
        var (rows, tier) = await _dataService.GetTopProceduresByCpuTierAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _procStatsFilterMgr!.UpdateData(rows);
        SetDefaultSortIfNone(ProcedureStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
        /* #4231 stage 3b: an hourly-routed page holds no object_type/sql_handle/plan_handle — the raw-floor
           banner and this tier disclosure are independent facts, same reasoning as the Queries sub-tab. */
        UpdateTruncationBanner(ProcStatsTruncationBanner, await DataStartOrNullAsync(floorTask, "Procedure Stats"), startUtc, tier == "hourly" ? HourlyTierSuffix : null);
        await LoadProcStatsSlicerAsync(startUtc, endUtc);
        await RefreshProcStatsComparisonAsync(startUtc, endUtc);
    }

    private async Task LoadQueryStoreAsync(DateTime startUtc, DateTime endUtc)
    {
        var floorTask = _dataService.GetQueryStoreWindowFloorAsync(_server.ServerId, startUtc, endUtc);
        /* #3953 clause 4: only a genuine custom range is a literal end the gate must honor against
           applied_through. A preset's endUtc is GetWindowUtc()'s own DateTime.UtcNow (the viewer's clock, not
           the store's), so passing it as a literal here would send a slow-clocked viewer to raw on every
           ordinary read (M1) — null tells the gate this end is open. */
        var (rows, widePlan) = await _dataService.GetQueryStoreTopQueriesWithReachAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter, literalEndUtc: IsCustomRange ? endUtc : null);
        _queryStoreFilterMgr!.UpdateData(rows);
        SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
        UpdateTruncationBanner(QueryStoreTruncationBanner, await DataStartOrNullAsync(floorTask, "Query Store"), startUtc, widePlan: widePlan);
        await LoadQueryStoreSlicerAsync(startUtc, endUtc);
        await RefreshQueryStoreComparisonAsync(startUtc, endUtc);
    }

    /// <summary>
    /// #4231: the Queries-tab grid header's disclosure — "Showing since &lt;effective start&gt;" when
    /// <paramref name="floor"/> sits past <see cref="RawWindowFloor.IsTruncated"/>'s slack after
    /// <paramref name="requestedStartUtc"/>, collapsed otherwise. The chart-title idiom
    /// (<see cref="DescribeTrendCoverage"/>) states the same fact on Performance Trends; this is its grid-header
    /// form, in <c>yyyy-MM-dd HH:mm:ss</c>, with seconds as Lite's banner has them, in the display zone and on the
    /// invariant culture (<see cref="BannerTime"/>).
    /// </summary>
    internal static void UpdateTruncationBanner(TextBlock banner, DateTime? floor, DateTime requestedStartUtc, string? tierSuffix = null,
        QueryStoreIntervalWide.WideReadPlan? widePlan = null)
    {
        /* #4689: when the interval table served, the rows start at the plan's EffectiveStart, not at raw's
           floor. The banner names that start and the bound that set it; the slicer still reads raw, so a
           truncated raw floor is named beside it. The raw route's banner below is unchanged. */
        if (widePlan?.EffectiveStart is DateTime wideStart)
        {
            var wideTruncated = RawWindowFloor.IsTruncated(wideStart, requestedStartUtc);
            /* The slicer and comparison read raw whatever tier served the grid, so a truncated raw floor is
               named whether or not the grid itself was cut. */
            var slicerTruncated = RawWindowFloor.IsTruncated(floor, requestedStartUtc);
            if (wideTruncated || slicerTruncated || !string.IsNullOrEmpty(tierSuffix))
            {
                var slicerSince = slicerTruncated
                    ? BannerTime(RawWindowFloor.EffectiveStart(floor, requestedStartUtc))
                    : null;
                var text = wideTruncated
                    ? $"Showing since {BannerTime(wideStart)}{QueryStoreIntervalWide.BannerReason(widePlan.Value.StartBound)}"
                        + (slicerSince is null ? string.Empty : $" · slicer since {slicerSince}")
                    : slicerSince is null
                        ? $"Showing {BannerTime(requestedStartUtc)}"
                        : $"Slicer since {slicerSince} (the grid shows the full window)";

                banner.Text = text + tierSuffix;
                banner.Visibility = Visibility.Visible;
            }
            else
            {
                banner.Visibility = Visibility.Collapsed;
            }

            return;
        }

        var truncated = RawWindowFloor.IsTruncated(floor, requestedStartUtc);
        /* #4231 stage 3: the raw-floor truncation and the hourly-tier suffix are independent facts (a window
           can be BOTH aged past raw's floor and routed to the hourly rollup) — so the banner shows whenever
           either is true, not only when the raw floor was cut. */
        if (!truncated && string.IsNullOrEmpty(tierSuffix))
        {
            banner.Visibility = Visibility.Collapsed;
            return;
        }

        banner.Text = truncated
            ? $"Showing since {BannerTime(RawWindowFloor.EffectiveStart(floor, requestedStartUtc))}{tierSuffix}"
            : $"Showing {BannerTime(requestedStartUtc)}{tierSuffix}";
        banner.Visibility = Visibility.Visible;
    }

    /// <summary>
    /// The instant a "Showing since" banner names, in the display zone with the ambiguous-hour offset suffix (the same
    /// zone and suffix <see cref="ViewerTimeHelper.FormatForDisplay(DateTime, string)"/> gives) but on the invariant
    /// culture, as Lite's banner (<c>DisplayZone.Format</c>) and the web's notice are. A banner is one line of
    /// disclosure, not a grid cell, and a machine whose default calendar is not the Gregorian one (Thai Buddhist,
    /// Hijri) would otherwise print 2026 as 2569 or 1448. <c>FormatForDisplay</c> stays on the current culture on
    /// purpose for the grids' own columns. The time carries its seconds (Lite's banner does too): a grid that reaches
    /// back no further than its oldest row names that row, and a note in whole minutes could print the range's own
    /// start minute for a row some seconds into it, which reads as if the grid started at the range's start.
    /// This is the one formatter every "Showing since" note on the server tab goes through.
    /// </summary>
    private static string BannerTime(DateTime naiveUtc) =>
        PerformanceMonitor.Ui.DisplayZone.Format(naiveUtc, ViewerTimeHelper.CurrentDisplayZone(), "yyyy-MM-dd HH:mm:ss");

    /// <summary>
    /// Where a data-start probe says the data starts (Active Queries, Current Waits, and the Queries tab's Query Stats,
    /// Procedure Stats and Query Store window-floor probes: the caller starts the probe before its grid read and awaits
    /// it here), or null when the probe throws. The probe is only the "Showing since" disclosure, so its failure must
    /// not unwind the refresh past the slicer, the comparison or the charts that follow it: the answer goes on to
    /// <see cref="UpdateTruncationBanner"/>, for which a null floor names no cut (the banner hides, unless the hourly-tier
    /// suffix or the interval-table plan the Queries tab passes beside it still has something to say), the failure is
    /// logged (<paramref name="warn"/> takes the log source and the message, <see cref="ViewerLogger.Warn"/> by
    /// default), and the refresh goes on.
    /// </summary>
    internal static async Task<DateTime?> DataStartOrNullAsync(Task<DateTime?> probe, string surface, Action<string, string>? warn = null)
    {
        try
        {
            return await probe;
        }
        catch (Exception ex)
        {
            (warn ?? ViewerLogger.Warn)(
                "ViewerServerTab",
                $"{surface}: the data-start probe failed, so no \"Showing since\" banner is shown | {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Loads the Query Store Regressions grid — the Dashboard's regressions view (baseline-vs-recent Query
    /// Store performance) ported to Darling's store, over the toolbar's settable window. Like the Dashboard
    /// grid (and unlike the three per-source grids) it has no slicer / comparison / slicer-overlay: the read
    /// is already a baseline-vs-recent contrast, not a single time series. The default sort matches the
    /// Dashboard grid's (duration regression % descending) — a grid-view-only SortDescription that does NOT
    /// touch the read's additional-duration ORDER BY (the TVF's ranking).
    /// </summary>
    private async Task LoadQueryStoreRegressionsAsync(DateTime startUtc, DateTime endUtc)
    {
        /* #4966: the probe starts beside the read and asks about the baseline window (the data layer keys it there), and the banner
           step compares its answer with that window's start. The rows are ranked by added duration, not by time, so they name no
           earlier start and the cap rule does not apply. */
        var floorTask = _dataService.GetQueryStoreRegressionsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var rows = await _dataService.GetQueryStoreRegressionsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _queryStoreRegressionsFilterMgr!.UpdateData(rows);
        await ShowQueryStoreRegressionsDataStartAsync(QueryStoreRegressionsTruncationBanner, floorTask, startUtc);
        SetDefaultSortIfNone(QueryStoreRegressionsGrid, "DurationRegressionPercent", ListSortDirection.Descending);
    }

    /// <summary>
    /// Raises or hides the "Showing since" banner of the Query Store Regressions grid (#4966). The read compares the range with
    /// the baseline of the 7 days before it, so the notice keys on that EARLIER window: a server added two days ago covers the
    /// range whole but has two days of baseline, not seven, and a note compared with the range's own start would stay silent. The
    /// probe's answer (<paramref name="probe"/>, started beside the read: <see cref="ViewerDataService.GetQueryStoreRegressionsDataStartAsync"/>)
    /// is therefore compared with <see cref="ViewerDataService.QueryStoreRegressionsBaselineStart"/> of the range's start, through
    /// <see cref="UpdateTruncationBanner"/>. The probe is awaited through <see cref="DataStartOrNullAsync"/>, so a probe that
    /// throws costs this banner and nothing after it. <c>internal static</c> so the store-backed tests run the same step as the tab.
    /// </summary>
    /// <param name="banner">The grid's banner.</param>
    /// <param name="probe">The grid's data-start probe, started beside its read.</param>
    /// <param name="startUtc">The start of the RANGE the grid just drew (not of its baseline).</param>
    internal static async Task ShowQueryStoreRegressionsDataStartAsync(TextBlock banner, Task<DateTime?> probe, DateTime startUtc) =>
        UpdateTruncationBanner(
            banner,
            await DataStartOrNullAsync(probe, "Query Store Regressions"),
            ViewerDataService.QueryStoreRegressionsBaselineStart(startUtc));

    /// <summary>
    /// Loads the Plan Corrections grid (#1952) — the engine's own automatic plan correction recommendations
    /// over the toolbar's settable window. Like the regressions grid it has no slicer / comparison: the read
    /// is the engine's finding, not a time series over one metric. The default sort is the engine's own
    /// ranking (score descending) as a grid-view-only SortDescription, so the read's chronological ORDER BY
    /// stands.
    /// </summary>
    private async Task LoadPlanCorrectionsAsync(DateTime startUtc, DateTime endUtc)
    {
        var dataStartTask = _dataService.GetPlanCorrectionsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var rows = await _dataService.GetPlanCorrectionsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _planCorrectionFilterMgr!.UpdateData(rows);
        /* #4966: the read keeps the newest 200 rows, and the collector re-captures every open recommendation on each cycle, so a
           few recommendations fill the page within hours and a wide range is answered from its newest hours. A full page names its
           oldest row, over a range the store covers too; a read under the cap names the earlier of the coverage start and the
           earliest row shown. The rows window on collection_time, the column they show, so there is no earlier event time to name. */
        await ShowEventDataStartAsync(PlanCorrectionsTruncationBanner, dataStartTask, "Plan Corrections", startUtc, rows.Select(r => (DateTime?)r.CollectionTime), ViewerDataService.PlanCorrectionsRowCap);
        SetDefaultSortIfNone(PlanCorrectionGrid, "Score", ListSortDirection.Descending);
    }

    // ── Slicers (Lite's ServerTab.Slicers.cs; the slicer sends UTC bounds, the viewer reads take naive UTC) ──

    private async Task LoadQueryStatsSlicerAsync(DateTime startUtc, DateTime endUtc)
    {
        var data = await _dataService.GetQueryStatsSlicerDataAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _queryStatsSlicerData = data;
        _queryStatsSlicerMetric = "TotalCpu";
        if (data.Count > 0)
            QueryStatsSlicer.LoadData(data, "Total CPU (ms)", startUtc, endUtc);
    }

    private async void OnQueryStatsSlicerChanged(object? sender, SlicerRangeEventArgs e)
    {
        try
        {
            var floorTask = _dataService.GetQueryStatsWindowFloorAsync(_server.ServerId, e.StartUtc, e.EndUtc);
            var (rows, tier) = await _dataService.GetTopQueriesByCpuTierAsync(_server.ServerId, e.StartUtc, e.EndUtc, databaseNames: SelectedDatabaseFilter);
            _queryStatsFilterMgr!.UpdateData(rows);
            UpdateTruncationBanner(QueryStatsTruncationBanner, await DataStartOrNullAsync(floorTask, "Query Stats"), e.StartUtc, tier == "hourly" ? HourlyTierSuffix : null);
            await RefreshQueryStatsComparisonAsync(e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"query-stats slicer failed: {ex.Message}");
        }
    }

    private async Task LoadProcStatsSlicerAsync(DateTime startUtc, DateTime endUtc)
    {
        var data = await _dataService.GetProcStatsSlicerDataAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _procStatsSlicerData = data;
        _procStatsSlicerMetric = "TotalCpu";
        if (data.Count > 0)
            ProcStatsSlicer.LoadData(data, "Total CPU (ms)", startUtc, endUtc);
    }

    private async void OnProcStatsSlicerChanged(object? sender, SlicerRangeEventArgs e)
    {
        try
        {
            var floorTask = _dataService.GetProcedureStatsWindowFloorAsync(_server.ServerId, e.StartUtc, e.EndUtc);
            var (rows, tier) = await _dataService.GetTopProceduresByCpuTierAsync(_server.ServerId, e.StartUtc, e.EndUtc, databaseNames: SelectedDatabaseFilter);
            _procStatsFilterMgr!.UpdateData(rows);
            UpdateTruncationBanner(ProcStatsTruncationBanner, await DataStartOrNullAsync(floorTask, "Procedure Stats"), e.StartUtc, tier == "hourly" ? HourlyTierSuffix : null);
            await RefreshProcStatsComparisonAsync(e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"procedure-stats slicer failed: {ex.Message}");
        }
    }

    private async Task LoadQueryStoreSlicerAsync(DateTime startUtc, DateTime endUtc)
    {
        var data = await _dataService.GetQueryStoreSlicerDataAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _queryStoreSlicerData = data;
        _queryStoreSlicerMetric = "TotalCpu";
        if (data.Count > 0)
            QueryStoreSlicer.LoadData(data, "Total CPU (ms)", startUtc, endUtc);
    }

    private async void OnQueryStoreSlicerChanged(object? sender, SlicerRangeEventArgs e)
    {
        try
        {
            var floorTask = _dataService.GetQueryStoreWindowFloorAsync(_server.ServerId, e.StartUtc, e.EndUtc);
            /* #3953 clause 4: a slicer selection is always a literal, user-drawn sub-range, never a preset. */
            var rows = await _dataService.GetQueryStoreTopQueriesAsync(_server.ServerId, e.StartUtc, e.EndUtc, databaseNames: SelectedDatabaseFilter, literalEndUtc: e.EndUtc);
            _queryStoreFilterMgr!.UpdateData(rows);
            UpdateTruncationBanner(QueryStoreTruncationBanner, await DataStartOrNullAsync(floorTask, "Query Store"), e.StartUtc);
            await RefreshQueryStoreComparisonAsync(e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"query-store slicer failed: {ex.Message}");
        }
    }

    // ── Sort-driven slicer metric re-labeling (Lite's ServerTab.Grids.cs *Grid_Sorting) ──

    /// <summary>Sorting the Top Queries grid swaps the slicer's aggregate curve to match the sorted column.</summary>
    private void QueryStatsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_queryStatsSlicerData == null || _queryStatsSlicerData.Count == 0) return;

        var col = SortColumnPath(e.Column);
        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalElapsedMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgElapsedMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "TotalLogicalReads" => ("TotalReads", "Total Reads"),
            "AvgReads" => ("AvgReads", "Avg Reads"),
            "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
            "TotalPhysicalReads" => ("TotalPhysReads", "Total Physical Reads"),
            _ => ("TotalCpu", "Total CPU (ms)"),
        };

        if (metric == _queryStatsSlicerMetric) return;
        _queryStatsSlicerMetric = metric;

        foreach (var bucket in _queryStatsSlicerData)
        {
            var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "AvgCpu" => bucket.TotalCpu / n,
                "TotalElapsed" => bucket.TotalElapsed,
                "AvgElapsed" => bucket.TotalElapsed / n,
                "TotalReads" => bucket.TotalReads,
                "AvgReads" => bucket.TotalReads / n,
                "TotalWrites" => bucket.TotalWrites,
                "TotalPhysReads" => bucket.TotalPhysicalReads,
                _ => bucket.TotalCpu,
            };
        }

        QueryStatsSlicer.UpdateMetric(label);

        // Re-compute overlay with new metric if a row is selected
        if (QueryStatsGrid.SelectedItem != null)
            QueryStatsGrid_SelectionChanged(QueryStatsGrid, null!);
    }

    /// <summary>Sorting the Top Procedures grid swaps the slicer's aggregate curve to match the sorted column.</summary>
    private void ProcedureStatsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_procStatsSlicerData == null || _procStatsSlicerData.Count == 0) return;

        var col = SortColumnPath(e.Column);
        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalElapsedMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgElapsedMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "TotalLogicalReads" or "AvgReads" => ("TotalReads", "Total Reads"),
            "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
            /* #3556's Darling half: the #3547 bug one grid over — this arm mapped the physical sort to the
               LOGICAL series under a physical label. The shared slicer reader has always mapped ordinal 6's
               total_physical_reads into TotalPhysicalReads; only this handler pointed at the wrong series. */
            "TotalPhysicalReads" => ("TotalPhysReads", "Total Physical Reads"),
            _ => ("TotalCpu", "Total CPU (ms)"),
        };

        if (metric == _procStatsSlicerMetric) return;
        _procStatsSlicerMetric = metric;

        foreach (var bucket in _procStatsSlicerData)
        {
            var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "AvgCpu" => bucket.TotalCpu / n,
                "TotalElapsed" => bucket.TotalElapsed,
                "AvgElapsed" => bucket.TotalElapsed / n,
                "TotalReads" => bucket.TotalReads,
                "TotalWrites" => bucket.TotalWrites,
                "TotalPhysReads" => bucket.TotalPhysicalReads,
                _ => bucket.TotalCpu,
            };
        }

        ProcStatsSlicer.UpdateMetric(label);

        if (ProcedureStatsGrid.SelectedItem != null)
            ProcedureStatsGrid_SelectionChanged(ProcedureStatsGrid, null!);
    }

    /// <summary>Sorting the Query Store grid swaps the slicer's aggregate curve to match the sorted column.</summary>
    private void QueryStoreGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_queryStoreSlicerData == null || _queryStoreSlicerData.Count == 0) return;

        var col = SortColumnPath(e.Column);
        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuTimeMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalDurationMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgDurationMs" => ("AvgElapsed", "Avg Duration (ms)"),
            /* #3556: the plotted bucket values are execution-weighted slice TOTALS, so the old "Avg"
               labels under-claimed what the bars showed. Total labels, matching the physical arm below. */
            "AvgLogicalReads" => ("TotalReads", "Total Reads"),
            "AvgLogicalWrites" => ("TotalWrites", "Total Writes"),
            /* #3547's Darling half: this arm still mapped physical to the LOGICAL series under a physical
               label — the swap Lite fixed in #3550, never ported here. The reader side needed nothing: the
               slicer SQL computes total_physical_reads and the shared reader maps it. */
            "AvgPhysicalReads" => ("TotalPhysReads", "Total Physical Reads"),
            "TotalExecutions" => ("Sessions", "Executions"),
            _ => ("TotalCpu", "Total CPU (ms)"),
        };

        if (metric == _queryStoreSlicerMetric) return;
        _queryStoreSlicerMetric = metric;

        foreach (var bucket in _queryStoreSlicerData)
        {
            var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "AvgCpu" => bucket.TotalCpu / n,
                "TotalElapsed" => bucket.TotalElapsed,
                "AvgElapsed" => bucket.TotalElapsed / n,
                "TotalReads" => bucket.TotalReads,
                "TotalWrites" => bucket.TotalWrites,
                "TotalPhysReads" => bucket.TotalPhysicalReads,
                "Sessions" => bucket.SessionCount,
                _ => bucket.TotalCpu,
            };
        }

        QueryStoreSlicer.UpdateMetric(label);

        if (QueryStoreGrid.SelectedItem != null)
            QueryStoreGrid_SelectionChanged(QueryStoreGrid, null!);
    }

    /// <summary>The sorted column's member path (SortMemberPath, else the bound Binding path) — Lite's fallback.</summary>
    private static string SortColumnPath(DataGridColumn column)
    {
        var col = column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && column is DataGridBoundColumn bc &&
            bc.Binding is System.Windows.Data.Binding b)
        {
            col = b.Path.Path;
        }
        return col;
    }

    /// <summary>
    /// Sets the grid's default sort when the user has not sorted it yet (Lite's SetDefaultSortIfNone) —
    /// so the first render matches the read's ORDER BY. The filter manager preserves sort across reloads,
    /// so this only fires on the initial load.
    /// </summary>
    private static void SetDefaultSortIfNone(DataGrid grid, string bindingPath, ListSortDirection direction)
    {
        if (grid.Items.SortDescriptions.Count > 0) return;

        grid.Items.SortDescriptions.Add(new SortDescription(bindingPath, direction));
        foreach (var column in grid.Columns)
        {
            if (column.SortMemberPath == bindingPath ||
                (column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b && b.Path.Path == bindingPath))
            {
                column.SortDirection = direction;
                return;
            }
        }
    }
}
