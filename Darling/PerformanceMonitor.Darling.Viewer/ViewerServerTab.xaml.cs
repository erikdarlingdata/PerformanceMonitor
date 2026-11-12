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
       Indices 19-25 CONTINUE the same TabControl rather than living in a second one, and every index
       above stays exactly where it was. That is the whole reason for the layout: for a PostgreSQL server
       the nineteen SQL Server TabItems are collapsed and these seven are shown, so the two sets never
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
    private DarlingServer _server;
    private readonly ViewerServerStore? _serverStore;

    private bool _refreshInFlight;
    private bool _refreshRequested;

    /* #8549: when the refresh now queued (_refreshRequested) was first asked for, as a Stopwatch timestamp. The queued load's clock starts there. */
    private long _refreshQueuedAt;

    /* #1319: per-server global database filter (display-only). Empty set = "All" (unfiltered). */
    private readonly HashSet<string> _selectedDatabases = new(StringComparer.OrdinalIgnoreCase);
    private List<SelectableItem> _databaseFilterItems = new();
    private bool _isUpdatingDatabaseFilterSelection;
    private int _databaseFilterTotalCount;
    private List<string>? _databaseFilterCollectedNames;
    private bool _databaseFilterDirty;

    /// <summary>The database filter as a reader argument: null (= All, unfiltered) when nothing is selected.</summary>
    private IReadOnlyList<string>? SelectedDatabaseFilter => _selectedDatabases.Count == 0 ? null : _selectedDatabases.ToList();

    /// <summary>Raised after a load so MainWindow can surface progress/errors in its status bar.</summary>
    public event Action<string>? StatusChanged;

    /* #8549: the first open's clock. A tab's first open spent its time before the landing sub-tab's own load began (the server clock, data start,
       freshness and permission reads, the picker, the tab's construction) and wrote no SlowLoad line, because the only clock started inside
       LoadInnerTabAsync. One clock now runs from the tab's activation to the first stamp and the landing load's reads book on it too. */
    private readonly System.Diagnostics.Stopwatch _constructClock = System.Diagnostics.Stopwatch.StartNew();
    private long _constructMs;
    private ViewerLoadTimer? _firstOpenTimer;
    private bool _firstOpenTaken;

    /// <summary>Where a slow first open's line goes: the Viewer log's "SlowLoad" source. A test swaps it to read the line.</summary>
    internal Action<string> SlowLoadSink { get; set; } = line => ViewerLogger.Warn("SlowLoad", line);

    /// <summary>The first open's slow-load threshold. A test lowers it to read a line without waiting three seconds.</summary>
    internal int FirstOpenSlowThresholdMs { get; set; } = ViewerLoadTimer.SlowLoadThresholdMs;

    /// <param name="preferences">
    /// The user's persisted viewer defaults (Settings window) that seed this tab's toolbar — default time
    /// window and auto-refresh on/off + interval. Null (or omitted) uses a default-constructed
    /// <see cref="ViewerPreferences"/>, which reproduces the toolbar's historical XAML defaults (24h / on / 1m)
    /// exactly, so the tab is unchanged when no settings are supplied.
    /// </param>
    public ViewerServerTab(ViewerDataService dataService, DarlingServer server, ViewerPreferences? preferences = null, ViewerServerStore? serverStore = null)
    {
        _dataService = dataService;
        _scheduleOverrides = new ViewerScheduleOverridesCache(
            async () => await dataService.GetCollectorSchedulesAsync(),
            () => DateTime.UtcNow,
            ex => ViewerLogger.Warn("ViewerServerTab", $"collector schedule read failed, notes use the shipped cadence | {ex.GetType().Name}: {ex.Message}"));
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
        PerformanceMonitor.Ui.ChartPairLayout.Attach(CurrentWaitsScroll, CurrentWaitsGrid, CurrentWaitsFirstCard, CurrentWaitsSecondCard, CurrentWaitsGrid.RowDefinitions[2]); // #8219: side by side when the tab is too short for both stacked
        DataGridNumberHeaders.Apply(this); // #8223: a number column's header on the same edge as its values
        ViewerChartCards.Attach(this); // #8151: "0 throughout" in a card whose chart plots only zeros
        ApplyTheme(WaitStatsChart);
        _chartHelper.RefreshEmptyChart(WaitStatsChart, "Wait Stats", ""); // #8086: not the default -10 to 10 axes before the first read
        // #6652: the stacked-chart tabs scroll at small sizes; the wheel over a chart stays that chart's zoom.
        PerformanceMonitor.Ui.ChartLayout.KeepWheelOnPlot(
            QueryDurationTrendChart, ProcDurationTrendChart, QueryStoreDurationTrendChart, ExecutionCountTrendChart,
            FileIoReadChart, FileIoWriteChart, TempDbChart, TempDbSizeChart, TempDbFileIoChart,
            LockWaitTrendChart, BlockingTrendChart, DeadlockTrendChart,
            // #7385: the other chart groups that now scroll the same way
            MemoryGrantSizingChart, MemoryGrantActivityChart, FileIoReadThroughputChart, FileIoWriteThroughputChart,
            BlockingDurationChart, BlockingTotalDurationChart, DeadlockWaitChart, DeadlockTotalWaitChart,
            CurrentWaitsDurationChart, CurrentWaitsBlockedChart, LatchStatsChart, SpinlockStatsChart,
            BadPagesChart, DumpRequestsChart, AccessViolationsChart, WriteAccessViolationsChart,
            NonYieldingTasksChart, LatchWarningsChart, SickSpinlocksChart, CpuComparisonChart, CpuChart);
        /* #5565: every filtered grid below this tab keeps its column filters across a restart under this server. */
        ColumnFilterScope.SetServer(this, _server.ServerName);

        /* Per-server toolbar identity label (UpdateServer refreshes it after a rename (#7212)); the
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
           Blocking / Buffer Pool / I/O Latency) to the Active Queries loader — the lanes plot X as the
           UTC instant, so the event's argument is already naive UTC, the centre
           OnActiveQueriesDrillDown reads its window around. ThemeManager is fixed to Dark, so the shared
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

        /* #8103: a read-only seat sees Collection Health's write buttons off with the reason, before it clicks one. */
        InitializeCollectionHealthSeat();

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
        InitializeRangePicker(toolbarDefaults);
        AutoRefreshCheckBox.IsChecked = toolbarDefaults.AutoRefreshEnabled;
        AutoRefreshIntervalCombo.SelectedIndex = toolbarDefaults.AutoRefreshIntervalIndex;

        /* Per-server toolbar (this wave): start the auto-refresh timer at the toolbar's interval (seeded above). The
           settable window the shared time range picker drives replaces the removed 24-hour s_dataWindow constant. */
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
        _constructMs = _constructClock.ElapsedMilliseconds;
    }

    /// <summary>#8549: starts the first open's clock when the shell makes this tab visible, so the wait behind another tab's refresh counts. A
    /// tab built in the background (a restored session) is not slow until the reader first shows it, so a later show restarts the clock until
    /// the first load has begun. The tab's construction time is carried in as the clock's starting stretch.</summary>
    internal void BeginFirstOpen()
    {
        if (_firstOpenTaken)
        {
            return;
        }

        _firstOpenTimer = new ViewerLoadTimer(FirstOpenSlowThresholdMs, _constructMs);
        _firstOpenTimer.BookStep("tab construction", _constructMs);
    }

    /// <summary>#8549: the clock of a refresh that waited behind the one in flight. It starts at the moment the refresh was asked for, and the
    /// wait is booked as a phase, so the queued load's slow-load line counts the wait and names it.</summary>
    private ViewerLoadTimer QueuedRefreshTimer()
    {
        var waitedMs = (long)System.Diagnostics.Stopwatch.GetElapsedTime(_refreshQueuedAt).TotalMilliseconds;
        var timer = new ViewerLoadTimer(ViewerLoadTimer.SlowLoadThresholdMs, waitedMs);
        timer.BookPhaseEndingNow("waited for the refresh in flight", 0);
        return timer;
    }

    /// <summary>The first open's clock for the load now starting, or null when this is not the tab's first load. Handed out once.</summary>
    private ViewerLoadTimer? TakeFirstOpenTimer()
    {
        if (_firstOpenTaken)
        {
            return null;
        }

        _firstOpenTaken = true;
        var timer = _firstOpenTimer;
        if (timer is null)
        {
            /* A host that never showed the tab through the shell (a test, a deep link): the clock starts at the first load. */
            timer = new ViewerLoadTimer(FirstOpenSlowThresholdMs, _constructMs);
            timer.BookStep("tab construction", _constructMs);
        }

        timer.Lead = "server tab first open > ";
        _firstOpenTimer = null;
        return timer;
    }

    /// <summary>The server this tab is bound to; MainWindow keys open tabs by this for dedupe/close.</summary>
    public int ServerId => _server.ServerId;

    /// <summary>The server record, for the tab header label and status text.</summary>
    public DarlingServer Server => _server;

    /// <summary>
    /// Swaps in the reloaded server record after the operator renamed it (#7212), so the export file names and the Block Chain
    /// and Deadlock window titles use the new display name without closing and reopening the tab. The id is the same, so
    /// every read keeps its key.
    /// </summary>
    public void UpdateServer(DarlingServer server)
    {
        ArgumentNullException.ThrowIfNull(server);
        _server = server;
        ServerNameText.Text = server.DisplayName;
        ApplyPlanCorrectionEmptyTexts(PlanCorrectionTarget());
    }

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

        /* Compare works on three Queries sub-tabs only; off everywhere else (walk finding V10b). */
        UpdateCompareDropdownState();

        /* #8647: the sub-tab just picked says "Loading..." where its empty-state sentence goes until its first read returns. */
        ShowFirstPaintLoading();

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
    /// <para>The load runs through <see cref="Dispatcher.Invoke{TResult}(Func{TResult})"/>, which on this tab's own thread runs it at once
    /// with the dispatcher's synchronization context installed, so the code after each of its awaits resumes on this thread whatever
    /// context the caller's thread had. The app's UI thread has that context already, and the load runs as it would without the call. A
    /// host that drives the tab on its dispatcher thread with no context installed would otherwise resume the load on the thread pool and
    /// touch the time range picker from the wrong thread (#8506).</para>
    /// </summary>
    public Task RefreshActiveInnerTabAsync() => Dispatcher.Invoke(RefreshActiveInnerTabUnderDispatcherContextAsync);

    /// <summary>The body of <see cref="RefreshActiveInnerTabAsync"/>, entered with the dispatcher's synchronization context current.</summary>
    private async Task RefreshActiveInnerTabUnderDispatcherContextAsync()
    {
        if (_refreshInFlight)
        {
            if (!_refreshRequested)
            {
                _refreshQueuedAt = System.Diagnostics.Stopwatch.GetTimestamp();
            }

            _refreshRequested = true;
            return;
        }

        _refreshInFlight = true;
        /* #8549: the first load of the tab runs on the first open's clock, so the reads below, which run before or beside the landing
           sub-tab's own load, are booked by name. Null for every later refresh. */
        var open = TakeFirstOpenTimer();
        /* The clock of the load this refresh starts. A later refresh (a sub-tab pick, a range change, the auto-refresh tick) runs the same
           reads, so its clock starts here too: its slow-load line names them and its total counts the ones its sub-tab's load waits for. */
        var timer = open ?? new ViewerLoadTimer();
        try
        {
            /* The time from the tab's activation to this point: the shell's refresh slot was taken by another tab's refresh, or its own
               chrome ran first. Booked as a wait, not a read or a UI step. */
            open?.BookPhaseEndingNow("waited for the shell to start this refresh", _constructMs);

            /* Point the process-wide time helper at THIS server's UTC offset before rendering — only the
               visible tab renders (the viewer's visible-only rule), so its offset wins. Loaded once, cached. */
            await Timed(timer, "server clock read", RefreshServerClockAsync());
            ClientStep(timer, "apply server clock", ApplyServerClockToHelper);

            /* #7386: the server's data start, for the charts' X axis. One cached read for the whole tab. */
            await Timed(timer, "server data start read", RefreshServerDataStartAsync());

            /* Refresh the toolbar freshness readout in the same pass (after the offset is applied so it renders
               in the active display mode). Non-critical chrome — its own try/catch keeps it off the load path.
               #8549: it fills the header, and the sub-tab's load reads nothing from it, so it starts here and runs beside that load
               instead of ahead of it. It writes the readout from this thread when its read answers. */
            var freshnessRead = Timed(timer, "server freshness read", UpdateServerFreshnessLabelAsync());

            /* #1591: same slot, same reasoning — a permission-denied collector is only visible on the Collection
               Health tab, which is why it went unnoticed. Badge it so it is discoverable from any tab.
               #8549: beside the load too. Each of the two is booked on the load's clock when it ends, so one that ends while the load
               runs is on the load's slow-load line with its own time. One still out when the load ends is not on that line, and the
               line's total, which stops at the load's end, never counts it. */
            var permissionRead = Timed(timer, "permission denied count read", UpdatePermissionDeniedBadgeAsync());

            /* #5562: redraw the picker's resolved range from the clock (a range that slides with now) and refresh its
               "collected every N minutes" note (set for the page on screen in the load loop below, before each tab's load). */
            ClientStep(timer, "range picker redraw", RefreshRangePicker);

            do
            {
                _refreshRequested = false;

                int loadedTab;
                do
                {
                    loadedTab = InnerTabs.SelectedIndex;
                    ShowFirstPaintLoading(); // #8647: also the landing sub-tab of the first open, which has no selection event
                    /* L3: a tab switched to during this load changes the page on screen, so its note is set here too, not only
                       at the top of the pass. */
                    await Timed(timer, "collection interval read", UpdateSampleIntervalAsync());
                    /* The first pass hands this refresh's clock on, so the sub-tab's reads land in the same line; a second pass
                       (the reader switched tabs mid-load) is an ordinary load on its own clock. */
                    await LoadInnerTabAsync(loadedTab, timer);
                    timer = null;
                    /* #8647: whatever on the sub-tab still says "Loading..." after its read was never filled; it must not outlive the read. */
                    if (InnerTabs.SelectedIndex == loadedTab) FirstPaint.Resolve((InnerTabs.SelectedItem as TabItem)?.Content as DependencyObject);
                }
                while (InnerTabs.SelectedIndex != loadedTab);

                /* #8549: the header's two reads are awaited here, so the refresh ends with the readout and the badge written. A refresh
                   already asked for loads first, with the reads still beside it, and one asked for while they are awaited is seen by
                   the check below, so it is served and not left behind. Awaiting them a second time costs nothing. */
                if (!_refreshRequested)
                {
                    await freshnessRead;
                    await permissionRead;
                }

                /* A refresh asked for while this one ran loads next. Its clock starts at the ask, with the wait behind this load booked. */
                timer = _refreshRequested ? QueuedRefreshTimer() : null;
            }
            while (_refreshRequested);
        }
        finally
        {
            _refreshInFlight = false;
        }
    }

    /// <summary>
    /// #1591, #7241: badges the Collection Health tab header with the number of collectors refused on their latest
    /// attempt after their last success (<c>denied_since_last_success</c> in <c>get_collection_health</c>). Mirrors Lite's <c>RefreshPermissionDeniedBadgeAsync</c>. Best-effort chrome like the freshness readout
    /// above — a read failure leaves the plain header rather than disturbing the tab load.
    /// <see cref="ViewerDataService.GetPermissionDeniedCollectorCountAsync"/> reads the shared fleet-by-server
    /// rollup read (#4226), not a raw <c>collection_log</c> scan of its own — this refresh (auto default 1 min,
    /// plus tab activation) issues no such read per open tab any more.
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
    /// absent from the freshness map, shown as "no data collected yet"; so is a server whose whole history
    /// retention has dropped, which reads "no collection retained" instead (#3967), by the same registration
    /// rule the sidebar dot applies. Best-effort chrome: a read failure blanks the readout rather than
    /// disturbing the tab load.
    /// </summary>
    private async Task UpdateServerFreshnessLabelAsync()
    {
        try
        {
            var freshness = await _dataService.GetServerCollectionStateAsync();
            var found = freshness.TryGetValue(_server.ServerId, out var state);
            ServerFreshnessText.Text = FreshnessLabel(
                found ? state.LastCollectionUtc : null,
                _server.RegisteredAt, DateTime.UtcNow, TabDisplayZone, _server.ConnectDownSince, _server.LastConnectError,
                attempted: found ? state.Attempted : _server.Attempted);
        }
        catch
        {
            ServerFreshnessText.Text = string.Empty;
        }
    }

    /// <summary>
    /// The toolbar's freshness text (#6135): when the newest collection landed, and for a server the Overview card
    /// calls Offline the same <see cref="FleetRollup.OfflineNote"/> sentence, so a red dot and an old time are not
    /// all the header says. The offline rule is the card's own, <see cref="ServerSummaryItem.ClassifyFreshness(DateTime?, DateTime?, DateTime)"/>.
    /// The time is written in <paramref name="zone"/>, the tab's own server clock, because the process-wide clock may belong to
    /// another server's tab by the time this awaited read returns.
    /// </summary>
    internal static string FreshnessLabel(
        DateTime? lastCollectionUtc, DateTime? registeredAtUtc, DateTime nowUtc, TimeZoneInfo? zone = null,
        DateTime? connectDownSinceUtc = null, string? lastConnectError = null, bool attempted = false)
    {
        connectDownSinceUtc = ServerHealthClassifier.EffectiveConnectDownSince(lastCollectionUtc, connectDownSinceUtc);
        var offline = ServerSummaryItem.ClassifyFreshness(lastCollectionUtc, registeredAtUtc, nowUtc, connectDownSinceUtc, attempted) == ServerFreshness.Offline;
        /* #7590: the service's connect failed. The header says so, with the last values' age and the error, in place of the age-based note. */
        if (connectDownSinceUtc.HasValue)
        {
            var known = ServerHealthClassifier.LastKnownLabel(lastCollectionUtc, nowUtc);
            return $"{ServerHealthClassifier.UnreachableReason(connectDownSinceUtc.Value, lastConnectError).TrimEnd('.')}. Values shown are {known}";
        }

        if (lastCollectionUtc is null)
        {
            /* #8464: a server the collectors reach and are refused by on every attempt is offline with nothing collected, so the header
               says the collectors fail rather than pointing at retention; "no collection retained" is the server whose history retention
               dropped, and "no data collected yet" the one no collector has reached. */
            return offline
                ? attempted ? "offline: collecting, but every collection fails" : "no collection retained"
                : "no data collected yet";
        }

        var collected = $"collected {ViewerTimeHelper.FormatForDisplay(lastCollectionUtc.Value, zone ?? ViewerTimeHelper.CurrentDisplayZone(), "yyyy-MM-dd HH:mm")}";
        return offline ? $"{collected}. {FleetRollup.OfflineNote}" : collected;
    }

    /// <param name="tabIndex">The inner tab to load.</param>
    /// <param name="loadTimer">The load's phase clock; null starts a new one. A test hands in its own to read the phases the load booked (#8549).</param>
    internal async Task LoadInnerTabAsync(int tabIndex, ViewerLoadTimer? loadTimer = null)
    {
        var timer = loadTimer ?? new ViewerLoadTimer();
        try
        {
            /* #6155: a PostgreSQL panel's healthy sentence needs to know whether the server is offline before it is written. */
            if (_server.IsPostgres)
            {
                await RefreshPgOfflineAsync();
            }

            switch (tabIndex)
            {
                case WaitStatsInnerTabIndex:
                    await LoadWaitStatsAsync();
                    break;
                case LatchSpinlockInnerTabIndex:
                    await LoadLatchSpinlockAsync(timer);
                    break;
                case QueriesInnerTabIndex:
                    await LoadQueriesAsync(timer);
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
                    await LoadPerfmonAsync(timer);
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
                    await LoadDailySummaryAsync(timer);
                    break;
                case HealthInnerTabIndex:
                    await LoadHealthAsync(timer);
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
                    await LoadFileIoAsync(timer);
                    break;
                case SystemEventsInnerTabIndex:
                    await LoadSystemEventsAsync();
                    break;
                case LongQueriesInnerTabIndex:
                    await LoadLongQueriesAsync(timer);
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
                    await LoadOverviewChartsAsync(timer);
                    break;
            }

            /* #8549: a refresh queued behind this one reads the window the picker names now, and this load read the one before it,
               so it does not stamp "refreshed" over a header that already says the new range. The queued load stamps. */
            if (ViewerRangeRefreshStatus.StampsRefreshed(_refreshRequested))
            {
                StatusChanged?.Invoke($"{_server.DisplayName}: refreshed {ViewerTimeHelper.RefreshedStamp()}");
            }
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"refresh failed: {ex.Message}");
        }
        finally
        {
            /* Walk finding D15: a slow load (over ViewerLoadTimer.SlowLoadThresholdMs) names its tab and where the time went. */
            var slow = timer.Finish(InnerTabLoadName(tabIndex, (tabIndex >= 0 && tabIndex < InnerTabs.Items.Count ? InnerTabs.Items[tabIndex] as TabItem : null)?.Header));
            /* #5555: this load's clock is a local handed down to the loaders this method calls (Walk finding D15: a loader wraps each
               store read in Timed(timer, ...)), not a field. A read that starts outside this load (a Daily Summary drill into
               Top Queries, a slicer drag) passes null, so it cannot append its phases to this load's line, finished or live, or
               rename it. */
            if (slow is not null)
            {
                SlowLoadSink($"[{_server.DisplayName}] {slow}");
            }
        }
    }

    /// <summary>The name a slow-load line gives a tab that sets no sub-surface of its own: the tab's header text, or its index
    /// when the header is not plain text or the tab is unknown.</summary>
    internal static string InnerTabLoadName(int tabIndex, object? header) =>
        header is string text && !string.IsNullOrWhiteSpace(text) ? text.Trim() : $"inner tab {tabIndex}";

    private async Task LoadOverviewChartsAsync(ViewerLoadTimer? timer = null)
    {
        /* The lanes control reads its own six feeds + four baselines concurrently over the toolbar's
           window: hours-back for a preset, or the custom From/To when the user picked a custom range
           (the lanes bound their own X-axis to it). Replaces wave-4's CPU-trend + wait-category charts. */
        var (fromDate, toDate) = GetOverviewCustomRange();
        var failedReads = await OverviewLanes.RefreshAsync(GetWindowHoursBack(), fromDate, toDate, timer);
        /* A lane that did not read is drawn as failed, and the tab is not refreshed: the status line names the reads instead. */
        if (failedReads is not null)
            throw new InvalidOperationException(failedReads);
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
