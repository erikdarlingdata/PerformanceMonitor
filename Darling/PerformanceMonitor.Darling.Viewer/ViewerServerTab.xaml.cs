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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One monitored server's detail surface (headless-plan W0 IA inversion): a closable per-server tab
/// hosting an inner tab strip — Overview (CPU-utilization + wait-time-by-category trend charts),
/// Queries (top-50 query stats), Blocking (XE blocked-process reports with the DMV-snapshot fallback),
/// and Collection Health (per-collector last-run status). This is the per-server content relocated
/// out of MainWindow's old flat tab list; the load/render bodies are unchanged (pure re-hosting).
/// Loads are lazy per inner tab (Lite's visible-only rule): switching inner tabs loads the newly
/// visible one, and <see cref="RefreshActiveInnerTabAsync"/> — driven by the per-server toolbar's
/// auto-refresh timer (see ViewerServerTab.TimeRange.cs) only when this server tab is the visible
/// top-level tab, plus MainWindow's tab-activation reload — reloads just the active inner tab.
/// </summary>
public partial class ViewerServerTab : UserControl
{
    /* The per-server data window is no longer a fixed 24-hour constant: it comes from the toolbar's
       time-range dropdown / custom From-To (see ViewerServerTab.TimeRange.cs). Every inner-tab load
       reads GetWindowUtc() / GetWindowHoursBack() instead of the removed s_dataWindow. */

    /* Inner-tab order: the main resource run mirrors Lite's ServerTab relative order (Overview, Wait Stats,
       Queries, Plan Viewer, CPU, Memory, File I/O, tempdb, Blocking, Perfmon, Session Stats, Running Jobs,
       Configuration, Configuration Changes), followed by a Darling-specific DIAGNOSTICS TAIL:
       Daily Summary -> System Events -> Latches & Spinlocks -> Collection Health. Latches & Spinlocks and
       System Events are Darling-only tabs grouped into that tail rather than the resource run. These index
       constants are the source of truth for SelectedIndex navigation (drill-downs), so they MUST match the
       <TabItem> order in ViewerServerTab.xaml. */
    internal const int OverviewInnerTabIndex = 0;
    internal const int WaitStatsInnerTabIndex = 1;
    internal const int QueriesInnerTabIndex = 2;
    internal const int PlanViewerInnerTabIndex = 3;
    internal const int CpuInnerTabIndex = 4;
    internal const int MemoryInnerTabIndex = 5;
    internal const int FileIoInnerTabIndex = 6;
    internal const int TempDbInnerTabIndex = 7;
    internal const int BlockingInnerTabIndex = 8;
    internal const int PerfmonInnerTabIndex = 9;
    internal const int SessionStatsInnerTabIndex = 10;
    internal const int RunningJobsInnerTabIndex = 11;
    internal const int ConfigurationInnerTabIndex = 12;
    internal const int ConfigChangesInnerTabIndex = 13;

    /* Diagnostics tail (Daily Summary -> System Events -> Latches & Spinlocks -> Collection Health): the
       reworked per-server tab order groups the two Darling-only diagnostic tabs (System Events, Latches &
       Spinlocks) with Daily Summary and Collection Health at the end, instead of scattering them through the
       resource run (Latches & Spinlocks was formerly at position 3, System Events dead last). */
    internal const int DailySummaryInnerTabIndex = 14;
    internal const int SystemEventsInnerTabIndex = 15;
    internal const int LatchSpinlockInnerTabIndex = 16;
    internal const int HealthInnerTabIndex = 17;

    /* #1496 Long Queries — the opt-in completion trace surface, appended after the diagnostics tail so no
       existing inner-tab index shifts (drill-down navigation keys on these constants). MUST remain the last
       SQL SERVER <TabItem> in ViewerServerTab.xaml's InnerTabs. */
    internal const int LongQueriesInnerTabIndex = 18;

    /* ── The PostgreSQL run (#2530) ───────────────────────────────────────────────────────────────
       Indices 19-24 CONTINUE the same TabControl rather than living in a second one, and every index
       above stays exactly where it was. That is the whole reason for the layout: for a PostgreSQL server
       the nineteen SQL Server TabItems are collapsed and these six are shown, so the two sets never
       renumber each other and no drill-down that keys on a constant above can be broken by this feature.

       ViewerPostgresTabs is the registry these belong to — headers, panel-to-collector mapping and the
       framing notes live there, and ViewerPostgresTabsTests holds it to CollectorCatalog in both
       directions so a tenth PostgreSQL collector cannot ship without a screen. Keep these constants, the
       registry's InnerTabIndex values and the <TabItem> order in the XAML in step; two pins check it. */
    internal const int PgOverviewInnerTabIndex = 19;
    internal const int PgActivityInnerTabIndex = 20;
    internal const int PgVacuumInnerTabIndex = 21;
    internal const int PgWaitsInnerTabIndex = 22;
    internal const int PgIoInnerTabIndex = 23;
    internal const int PgReplicationInnerTabIndex = 24;
    internal const int PgStorageInnerTabIndex = 25;

    private readonly ViewerDataService _dataService;
    private readonly DarlingServer _server;
    private readonly ViewerServerStore? _serverStore;

    private bool _refreshInFlight;
    private bool _refreshRequested;

    /* #1319: per-server global database filter (display-only). Empty set = "All" (unfiltered). */
    private readonly HashSet<string> _selectedDatabases = new(StringComparer.OrdinalIgnoreCase);
    private List<SelectableItem> _databaseFilterItems = new();
    private bool _isUpdatingDatabaseFilterSelection;
    private int _databaseFilterTotalCount;
    private bool _databaseFilterDirty;

    /// <summary>The database filter as a reader argument: null (= All, unfiltered) when nothing is selected.</summary>
    private IReadOnlyList<string>? SelectedDatabaseFilter => _selectedDatabases.Count == 0 ? null : _selectedDatabases.ToList();

    /// <summary>Raised after a load so MainWindow can surface progress/errors in its status bar.</summary>
    public event Action<string>? StatusChanged;

    /// <param name="preferences">
    /// The user's persisted viewer defaults (Settings window) that seed this tab's toolbar — default time
    /// window and auto-refresh on/off + interval. Null (or omitted) uses a default-constructed
    /// <see cref="ViewerPreferences"/>, which reproduces the toolbar's historical XAML defaults (24h / on / 1m)
    /// exactly, so the tab is unchanged when no settings are supplied.
    /// </param>
    public ViewerServerTab(ViewerDataService dataService, DarlingServer server, ViewerPreferences? preferences = null, ViewerServerStore? serverStore = null)
    {
        _dataService = dataService;
        _server = server;
        _serverStore = serverStore;
        /* #1319: seed the per-server database filter from the viewer registry (viewer-servers.json). */
        if (_serverStore != null)
        {
            foreach (var db in _serverStore.GetViewFilterDatabases(_server.ServerName))
            {
                _selectedDatabases.Add(db);
            }
        }
        InitializeComponent();

        /* Per-server toolbar identity label: the display name is static (it also heads the tab), while the
           freshness readout to its right is filled on the first (and every) refresh from the shared
           collection-time read (UpdateServerFreshnessLabelAsync). */
        ServerNameText.Text = _server.DisplayName;
        UpdateDatabaseFilterLabel();

        /* Column-filter managers for the Configuration sub-grids (copied from Lite's ServerTab filter
           wiring) — after InitializeComponent so the named grids exist. */
        InitializeFilterManagers();

        /* Overview lanes (copied from Lite): init the data service + server up front so the lanes theme
           their chrome and wire the correlated crosshair before the first load. Wire the lanes' own
           right-click "Show Active Queries at This Time" drill-down (every lane: CPU / Wait Stats /
           Blocking / Buffer Pool / I/O Latency) to the Active Queries loader — the lanes plot X through
           ViewerTimeHelper.ForDisplay, so the event's argument is display-time, exactly what
           OnActiveQueriesDrillDown converts back to naive UTC. ThemeManager is fixed to Dark, so the shared
           chart chrome applies without any per-control theme plumbing. */
        OverviewLanes.Initialize(_dataService, _server.ServerId);
        OverviewLanes.ShowActiveQueriesRequested += OnActiveQueriesDrillDown;

        /* CPU + tempdb inner-tab charts (copied from Lite): theme up front + wire hover. */
        InitializeCpuTempDbCharts();

        /* CPU Scheduler sub-tab chart (cpu_scheduler_stats parity): pressure trend + latest-snapshot grid. */
        InitializeCpuSchedulerChart();

        /* #2530: a PostgreSQL target gets the PostgreSQL tab set. Done here, once, from the server record
           the sidebar already loaded — not from an async probe — because the tab strip must be right on the
           FIRST paint: the web half's review found that moving this decision into an async callback broke
           the page's render model twice over, and the viewer has an even shorter path to get it right
           since the engine kind arrives with the server row itself. */
        ApplyEngineTabSet();

        /* Memory inner-tab charts (copied from Lite): same up-front theme + hover for the five Memory
           charts (Overview trend, Clerks, Grant sizing/activity, Pressure events). */
        InitializeMemoryCharts();

        /* Plan Cache sub-tab chart (plan_cache_stats parity, under Memory): single-use vs multi-use trend. */
        InitializePlanCacheChart();

        /* Latches & Spinlocks inner-tab charts (latch_stats/spinlock_stats parity): the two per-second
           contention trend charts, themed up front + hover. */
        InitializeLatchSpinlockCharts();

        /* Session Stats inner-tab chart (session_summary_stats parity): the server-wide session-count trend,
           themed up front + hover. */
        InitializeSessionStatsChart();

        /* File I/O + Blocking-trend inner-tab charts (copied from Lite): same up-front theme + hover. */
        InitializeFileIoCharts();
        InitializeBlockingCharts();

        /* Queries tab (W1f-1): the three grids' bar-cell maxima hook + slicer RangeChanged wiring
           (copied from Lite's ServerTab). After InitializeComponent so the named grids/slicers exist. */
        InitializeQueriesTab();

        /* Collection Health's Duration Trends chart (copied from Lite): up-front theme + hover. */
        InitializeCollectionHealthChart();

        /* System Events inner tab (system_health parity): register the category grids' filter managers. */
        InitializeSystemEventsTab();

        /* System Events' Corruption + Contention counter charts (SYSTEM-component parity): theme the eight
           charts up front + wire hover, mirroring the Dashboard's SystemEventsContent. */
        InitializeSystemHealthCharts();

        /* Seed the toolbar's initial time window + auto-refresh state from the user's persisted defaults
           (the Settings window), replacing the XAML's hardcoded 24h / on / 1m. Set BEFORE
           InitializeAutoRefreshTimer, which reads AutoRefreshCheckBox + AutoRefreshIntervalCombo to decide
           whether and how fast to start the timer. Assigning SelectedIndex/IsChecked here is inert: the
           range handler no-ops while IsLoaded is false, and the checkbox handler no-ops while
           _autoRefreshTimer is still null — so seeding drives no premature reload or timer churn. */
        var toolbarDefaults = preferences ?? new ViewerPreferences();
        TimeRangeCombo.SelectedIndex = toolbarDefaults.DefaultTimeRangeIndex;
        AutoRefreshCheckBox.IsChecked = toolbarDefaults.AutoRefreshEnabled;
        AutoRefreshIntervalCombo.SelectedIndex = toolbarDefaults.AutoRefreshIntervalIndex;

        /* Per-server toolbar (this wave): populate the custom-range hour/minute combos and start the
           auto-refresh timer at the toolbar's interval (seeded above). The settable window these controls
           drive replaces the removed 24-hour s_dataWindow constant. */
        InitializeTimeComboBoxes();
        InitializeAutoRefreshTimer();

        /* Seed the toolbar's Server/Local/UTC picker from the persisted global mode (MainWindow set
           ViewerTimeHelper.CurrentDisplayMode from ViewerAppSettings on startup). Suppressed inside
           SetDisplayModeSelection, and the handler no-ops before IsLoaded anyway, so this drives no reload. */
        SetDisplayModeSelection(ViewerTimeHelper.CurrentDisplayMode);

        /* Right-click chart drill-downs (ViewerServerTab.DrillDown.cs) + slicer overlay-on-select
           (ViewerServerTab.SlicerOverlay.cs). Wired LAST — after every Initialize*Charts has created its
           hover helpers, which the drill-down menus read for the nearest-series time. WireChartDrillDowns
           builds each drill-down chart's copy/save/export menu (BuildChartContextMenu) and adds the drill-down
           to it; WireChartContextMenus gives the same menu to every chart that has NO drill-down. */
        WireChartDrillDowns();
        WireChartContextMenus();
        WireSlicerOverlays();

        /* Per-row double-click history windows (ViewerServerTab.History.cs) on the three query grids — a
           different event from the SelectionChanged slicer overlay wired just above; both coexist. */
        WireHistoryDrillDowns();
    }

    /// <summary>The server this tab is bound to; MainWindow keys open tabs by this for dedupe/close.</summary>
    public int ServerId => _server.ServerId;

    /// <summary>The server record, for the tab header label and status text.</summary>
    public DarlingServer Server => _server;

    /// <summary>
    /// Lazy per-inner-tab load: switching inner tabs loads the newly visible one. SelectionChanged is
    /// a bubbling routed event, so selections inside the tab content (a grid) reach here too — only
    /// react to the inner TabControl's own. Gated on <see cref="System.Windows.FrameworkElement.IsLoaded"/>
    /// so the selection raised while the TabControl is first built (before this tab is shown) is ignored:
    /// MainWindow drives the initial load when it makes this server tab visible, and the toolbar's
    /// auto-refresh timer keeps it fresh — so the control loads exactly once per event, not on construction too.
    /// </summary>
    private async void InnerTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, InnerTabs) || !IsLoaded)
        {
            return;
        }

        /* A drill-down navigation switches the inner tab programmatically and runs its own targeted read;
           skip the generic loader so it doesn't race that (mirrors the sub-tab handlers' guard). */
        if (_suppressDrillDownAutoRefresh)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Loads the ACTIVE inner tab, with the overlap guard mirroring MainWindow's aggregate-tab loop.
    /// If the inner tab switches while a load is in flight, the triggering event bounces off the guard,
    /// so after each load we loop when the selection moved on and load again — leaving no inner tab
    /// stranded. A trigger that arrives mid-load (the auto-refresh tick, an inner-tab switch) sets
    /// <see cref="_refreshRequested"/> instead of being dropped, so the running loop reloads once more.
    /// </summary>
    public async Task RefreshActiveInnerTabAsync()
    {
        if (_refreshInFlight)
        {
            _refreshRequested = true;
            return;
        }

        _refreshInFlight = true;
        try
        {
            /* Point the process-wide time helper at THIS server's UTC offset before rendering — only the
               visible tab renders (the viewer's visible-only rule), so its offset wins. Loaded once, cached. */
            await EnsureServerOffsetLoadedAsync();
            ApplyServerOffsetToHelper();

            /* Refresh the toolbar freshness readout in the same pass (after the offset is applied so it renders
               in the active display mode). Non-critical chrome — its own try/catch keeps it off the load path. */
            await UpdateServerFreshnessLabelAsync();

            /* #1591: same slot, same reasoning — a permission-denied collector is only visible on the Collection
               Health tab, which is why it went unnoticed. Badge it so it is discoverable from any tab. */
            await UpdatePermissionDeniedBadgeAsync();

            do
            {
                _refreshRequested = false;

                int loadedTab;
                do
                {
                    loadedTab = InnerTabs.SelectedIndex;
                    await LoadInnerTabAsync(loadedTab);
                }
                while (InnerTabs.SelectedIndex != loadedTab);
            }
            while (_refreshRequested);
        }
        finally
        {
            _refreshInFlight = false;
        }
    }

    /// <summary>
    /// #1591: badges the Collection Health tab header with the number of collectors that were permission-denied.
    /// Mirrors Lite's <c>RefreshPermissionDeniedBadgeAsync</c>. Best-effort chrome like the freshness readout
    /// above — a read failure leaves the plain header rather than disturbing the tab load.
    /// </summary>
    private async Task UpdatePermissionDeniedBadgeAsync()
    {
        /* #2530: Collection Health is a SQL Server tab and is collapsed for a PostgreSQL server, which
           gets the same answer on its own Overview tab instead. Writing a header nobody can see would be
           harmless; issuing the read every refresh to do it would not. */
        if (_server.IsPostgres)
        {
            return;
        }

        try
        {
            var denied = await _dataService.GetPermissionDeniedCollectorCountAsync(_server.ServerId);
            CollectionHealthTab.Header = denied > 0
                ? $"Collection Health ({denied} no permission)"
                : "Collection Health";
        }
        catch
        {
            /* Chrome only — never break the tab load over a badge. */
        }
    }

    /// <summary>
    /// Fills the toolbar's freshness readout with this server's newest collection time (the shared
    /// MAX(collection_time) read the sidebar dots use), rendered in the active Server/Local/UTC display mode
    /// exactly like the Manage Servers "Last Collected" column. A server the service hasn't collected yet is
    /// absent from the freshness map, shown as "no data collected yet". Best-effort chrome: a read failure
    /// blanks the readout rather than disturbing the tab load.
    /// </summary>
    private async Task UpdateServerFreshnessLabelAsync()
    {
        try
        {
            var freshness = await _dataService.GetServerFreshnessAsync();
            ServerFreshnessText.Text = freshness.TryGetValue(_server.ServerId, out var lastUtc)
                ? $"collected {ViewerTimeHelper.ForDisplay(lastUtc):yyyy-MM-dd HH:mm}"
                : "no data collected yet";
        }
        catch
        {
            ServerFreshnessText.Text = string.Empty;
        }
    }

    private async Task LoadInnerTabAsync(int tabIndex)
    {
        try
        {
            switch (tabIndex)
            {
                case WaitStatsInnerTabIndex:
                    await LoadWaitStatsAsync();
                    break;
                case LatchSpinlockInnerTabIndex:
                    await LoadLatchSpinlockAsync();
                    break;
                case QueriesInnerTabIndex:
                    await LoadQueriesAsync();
                    break;
                case PlanViewerInnerTabIndex:
                    /* No data feed: plans are pushed into the host by OpenPlanTab (a "View Plan" click),
                       not loaded on tab-switch. Explicit no-op so selecting it doesn't fall through to the
                       default Overview reload, and so the auto-refresh timer leaves any open plan tabs alone. */
                    break;
                case BlockingInnerTabIndex:
                    await LoadBlockingAsync();
                    break;
                case PerfmonInnerTabIndex:
                    await LoadPerfmonAsync();
                    break;
                case SessionStatsInnerTabIndex:
                    await LoadSessionStatsAsync();
                    break;
                case RunningJobsInnerTabIndex:
                    await LoadRunningJobsAsync();
                    break;
                case ConfigurationInnerTabIndex:
                    await LoadConfigurationAsync();
                    break;
                case ConfigChangesInnerTabIndex:
                    await LoadConfigChangesAsync();
                    break;
                case DailySummaryInnerTabIndex:
                    await LoadDailySummaryAsync();
                    break;
                case HealthInnerTabIndex:
                    await LoadHealthAsync();
                    break;
                case CpuInnerTabIndex:
                    await LoadCpuAsync();
                    break;
                case MemoryInnerTabIndex:
                    await LoadMemoryAsync();
                    break;
                case TempDbInnerTabIndex:
                    await LoadTempDbAsync();
                    break;
                case FileIoInnerTabIndex:
                    await LoadFileIoAsync();
                    break;
                case SystemEventsInnerTabIndex:
                    await LoadSystemEventsAsync();
                    break;
                case LongQueriesInnerTabIndex:
                    await LoadLongQueriesAsync();
                    break;

                /* #2530: the PostgreSQL run. Explicit arms, never the default: falling through to
                   LoadOverviewChartsAsync would point the SQL Server correlated-lane reads (CPU %, wait
                   ms/sec, buffer pool, file I/O latency — four collectors that cannot run on this engine)
                   at a PostgreSQL server and paint four permanently empty lanes, which is the exact defect
                   this issue is about. */
                case PgOverviewInnerTabIndex:
                    await LoadPgOverviewAsync();
                    break;
                case PgActivityInnerTabIndex:
                    await LoadPgActivityAsync();
                    break;
                case PgVacuumInnerTabIndex:
                    await LoadPgVacuumAsync();
                    break;
                case PgWaitsInnerTabIndex:
                    await LoadPgWaitsAsync();
                    break;
                case PgIoInnerTabIndex:
                    await LoadPgIoAsync();
                    break;
                case PgReplicationInnerTabIndex:
                    await LoadPgReplicationAsync();
                    break;
                case PgStorageInnerTabIndex:
                    await LoadPgStorageAsync();
                    break;

                case OverviewInnerTabIndex:
                default:
                    await LoadOverviewChartsAsync();
                    break;
            }

            StatusChanged?.Invoke($"{_server.DisplayName} — refreshed {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"refresh failed: {ex.Message}");
        }
    }

    private async Task LoadOverviewChartsAsync()
    {
        /* The lanes control reads its own six feeds + four baselines concurrently over the toolbar's
           window: hours-back for a preset, or the custom From/To when the user picked a custom range
           (the lanes bound their own X-axis to it). Replaces wave-4's CPU-trend + wait-category charts. */
        var (fromDate, toDate) = GetOverviewCustomRange();
        await OverviewLanes.RefreshAsync(GetWindowHoursBack(), fromDate, toDate);
    }

    /* LoadHealthAsync now lives in ViewerServerTab.CollectionHealth.cs (W1i moved it there with the
       Collection Health sub-tabs), and LoadQueriesAsync now lives in ViewerServerTab.Queries.cs — it
       dispatches to the Queries tab's active sub-tab (Top Queries / Top Procedures / Query Store),
       loading that grid + its slicer + (when Compare is active) its comparison grid. */

    /* LoadBlockingAsync now lives in ViewerServerTab.Blocking.cs — it dispatches to the Blocking tab's
       active sub-tab (Trends / Current Waits / Blocked Process Reports) instead of loading the grid
       directly. */

    /* The Overview's rendering now lives entirely in the copied CorrelatedTimelineLanesControl
       (OverviewLanes); wave-4's RenderCpuTrend / RenderWaitCategoryTrend and their reads
       (ViewerDataService.Trends.cs) were superseded by the lanes and removed. */
}
