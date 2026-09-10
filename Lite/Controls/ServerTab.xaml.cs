/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using ScottPlot;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private readonly ServerConnection _server;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    public int ServerId => _serverId;
    public ServerConnection Server => _server;
    private readonly CredentialResolver _credentialResolver;
    private readonly DispatcherTimer _refreshTimer;
    private bool _refreshPendingWhileHidden;
    private bool _isRefreshing;
    /* Re-entrancy latch for the Ctrl+V plan-paste handler (#2870). #2837's async clipboard retry keeps the UI
       pump responsive but yields the thread for the ~175 ms can't-open retry window; the old Thread.Sleep had
       incidentally serialized input, so a HELD Ctrl+V (OS key-repeat) could put several reads in flight at once
       and open several "Pasted Plan" tabs. While a paste is running the repeat is dropped (its key is still
       claimed via e.Handled). */
    private bool _pasteInProgress;
    // Guards the visible-tab auto-refresh during an Active Queries drill-down:
    // SelectActiveQueriesForDrillDown() sets this before flipping to Queries → Active Queries so
    // MainTabControl_SelectionChanged skips its refresh and doesn't clobber the filtered snapshot
    // the drill-down loads next (async race).
    private bool _suppressActiveQueriesAutoRefresh;
    private readonly ChartRenderHelper _chartHelper = new();
    private List<SelectableItem> _waitTypeItems = new();
    private List<SelectableItem> _perfmonCounterItems = new();
    /* #1319: per-server global database filter (display-only). Empty set = "All" (unfiltered). */
    private readonly HashSet<string> _selectedDatabases = new(StringComparer.OrdinalIgnoreCase);
    private List<SelectableItem> _databaseFilterItems = new();
    private bool _isUpdatingDatabaseFilterSelection;
    /// <summary>The database filter as a reader argument: null (= All, unfiltered) when nothing is selected.</summary>
    private IReadOnlyList<string>? SelectedDatabaseFilter => _selectedDatabases.Count == 0 ? null : _selectedDatabases.ToList();
    private ChartHoverHelper? _waitStatsHover;
    private ChartHoverHelper? _perfmonHover;
    private ChartHoverHelper? _cpuHover;
    private ChartHoverHelper? _memoryHover;
    private ChartHoverHelper? _tempDbHover;
    private ChartHoverHelper? _tempDbSizeHover;
    private ChartHoverHelper? _tempDbFileIoHover;
    private ChartHoverHelper? _fileIoReadHover;
    private ChartHoverHelper? _fileIoWriteHover;
    private ChartHoverHelper? _fileIoReadThroughputHover;
    private ChartHoverHelper? _fileIoWriteThroughputHover;
    private ChartHoverHelper? _collectorDurationHover;
    private ChartHoverHelper? _queryDurationTrendHover;
    private ChartHoverHelper? _procDurationTrendHover;
    private ChartHoverHelper? _queryStoreDurationTrendHover;
    private ChartHoverHelper? _executionCountTrendHover;
    private ChartHoverHelper? _lockWaitTrendHover;
    private ChartHoverHelper? _blockingTrendHover;
    private ChartHoverHelper? _deadlockTrendHover;
    private ChartHoverHelper? _memoryClerksHover;
    private ChartHoverHelper? _memoryGrantSizingHover;
    private ChartHoverHelper? _memoryGrantActivityHover;
    private ChartHoverHelper? _memoryPressureEventsHover;
    private ChartHoverHelper? _currentWaitsDurationHover;
    private ChartHoverHelper? _currentWaitsBlockedHover;

    /* Query heatmap */
    private HeatmapResult? _lastHeatmapResult;
    private ScottPlot.Plottables.Heatmap? _heatmapPlottable;
    private System.Windows.Controls.Primitives.Popup? _heatmapPopup;
    private TextBlock? _heatmapPopupText;

    /* Memory clerks picker */
    private List<SelectableItem> _memoryClerkItems = new();
    private bool _isUpdatingMemoryClerkSelection;

    /* Column filtering */
    private readonly Dictionary<DataGrid, IDataGridFilterManager> _filterManagers = new();
    private DataGridFilterManager<QuerySnapshotRow>? _querySnapshotsFilterMgr;
    private DataGridFilterManager<QueryStatsRow>? _queryStatsFilterMgr;
    private DataGridFilterManager<ProcedureStatsRow>? _procStatsFilterMgr;
    private DataGridFilterManager<QueryStoreRow>? _queryStoreFilterMgr;
    private DataGridFilterManager<PlanCorrectionRow>? _planCorrectionFilterMgr;
    private DataGridFilterManager<BlockedProcessReportRow>? _blockedProcessFilterMgr;
    private DataGridFilterManager<DeadlockProcessDetail>? _deadlockFilterMgr;
    private DataGridFilterManager<RunningJobRow>? _runningJobsFilterMgr;
    private DataGridFilterManager<LongQueryCompletionRow>? _longQueryFilterMgr;
    private DataGridFilterManager<ServerConfigRow>? _serverConfigFilterMgr;
    private DataGridFilterManager<DatabaseConfigRow>? _databaseConfigFilterMgr;
    private DataGridFilterManager<DatabaseScopedConfigRow>? _dbScopedConfigFilterMgr;
    private DataGridFilterManager<QueryStoreHealthRow>? _queryStoreHealthFilterMgr;
    private DataGridFilterManager<AutomaticTuningRow>? _automaticTuningFilterMgr;
    private DataGridFilterManager<TraceFlagRow>? _traceFlagsFilterMgr;
    private DataGridFilterManager<CollectorHealthRow>? _collectionHealthFilterMgr;
    private DataGridFilterManager<CollectionLogRow>? _collectionLogFilterMgr;
    private DataGridFilterManager<LatchStatsSnapshotRow>? _latchStatsFilterMgr;
    private DataGridFilterManager<SpinlockStatsSnapshotRow>? _spinlockStatsFilterMgr;
    /* System Events tab: one filter manager per grid category (the two chart sub-tabs have no grid). */
    private DataGridFilterManager<SchedulerIssueRow>? _seSchedulerFilterMgr;
    private DataGridFilterManager<SevereErrorRow>? _seSevereErrorFilterMgr;
    private DataGridFilterManager<MemoryConditionsRow>? _seMemoryConditionsFilterMgr;
    private DataGridFilterManager<MemoryBrokerRow>? _seMemoryBrokerFilterMgr;
    private DataGridFilterManager<MemoryNodeOomRow>? _seMemoryNodeOomFilterMgr;
    private DataGridFilterManager<SignificantWaitRow>? _seSignificantWaitsFilterMgr;
    private DataGridFilterManager<CpuTasksRow>? _seCpuTasksFilterMgr;
    private DataGridFilterManager<IoIssuesRow>? _seIoIssuesFilterMgr;
    private DataGridFilterManager<DefaultTraceEventRow>? _seDefaultTraceFilterMgr;
    /* Configuration Changes tab: one filter manager per sub-tab grid. */
    private DataGridFilterManager<ServerConfigChangeRow>? _serverConfigChangesFilterMgr;
    private DataGridFilterManager<DatabaseConfigChangeRow>? _databaseConfigChangesFilterMgr;
    private DataGridFilterManager<TraceFlagChangeRow>? _traceFlagChangesFilterMgr;
    private CancellationTokenSource? _actualPlanCts;

    public int UtcOffsetMinutes { get; }
    private readonly bool _hasMsdbAccess;
    private readonly bool _isAzureSqlDatabase;
    /* Live probe of the opt-in long-query completion collector's enabled flag (#1496), so the Long
       Queries tab shows an explicit "trace is OFF" empty-state banner when it is disabled — read fresh
       each refresh so toggling it in the schedule editor updates the banner without reopening the tab. */
    private readonly Func<bool> _isLongQueryTraceEnabled;

    /// <summary>
    /// Raised after each data refresh with alert counts for tab badge display.
    /// </summary>
    public event Action<int, int, DateTime?>? AlertCountsChanged; /* blockingCount, deadlockCount, latestEventTimeUtc */
    public event Action<int>? ApplyTimeRangeRequested; /* selectedIndex */
    public event Func<Task>? ManualRefreshRequested;
    public event Action<ServerConnection>? PersistServerRequested; /* #1319: persist ViewFilterDatabases via ServerManager */

    public ServerTab(ServerConnection server, DuckDbInitializer duckDb, CredentialResolver credentialResolver, int utcOffsetMinutes = 0, bool hasMsdbAccess = true, bool isAzureSqlDatabase = false, Func<bool>? isLongQueryTraceEnabled = null)
    {
        InitializeComponent();
        SetupBarCellMaxes();

        _server = server;
        /* Default to "enabled" when no probe is supplied so the empty-state banner never falsely claims
           the trace is off; MainWindow always wires the live schedule probe. */
        _isLongQueryTraceEnabled = isLongQueryTraceEnabled ?? (() => true);
        foreach (var db in _server.ViewFilterDatabases)
            _selectedDatabases.Add(db);
        _dataService = new LocalDataService(duckDb);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        _credentialResolver = credentialResolver;
        UtcOffsetMinutes = utcOffsetMinutes;
        _hasMsdbAccess = hasMsdbAccess;
        _isAzureSqlDatabase = isAzureSqlDatabase;
        ServerTimeHelper.UtcOffsetMinutes = utcOffsetMinutes;

        ServerNameText.Text = server.ReadOnlyIntent ? $"{server.DisplayName} (Read-Only)" : server.DisplayName;
        ConnectionStatusText.Text = "Connecting...";

        /* Apply default time range from settings */
        TimeRangeCombo.SelectedIndex = App.DefaultTimeRangeHours switch
        {
            1 => 0,
            4 => 1,
            12 => 2,
            24 => 3,
            168 => 4,
            _ => 1
        };

        /* Auto-refresh every 60 seconds */
        _refreshTimer = new DispatcherTimer
        {
            Interval = TimeSpan.FromSeconds(60)
        };
        _refreshTimer.Tick += async (s, e) =>
        {
            await RefreshAllDataAsync();
        };
        _refreshTimer.Start();

        /* When this tab isn't selected the timer skips its data refresh (see RefreshAllDataAsync);
           refresh once when it becomes visible again so it isn't showing stale data on return. */
        IsVisibleChanged += (s, e) =>
        {
            if (IsVisible && _refreshPendingWhileHidden)
            {
                _refreshPendingWhileHidden = false;
                _ = RefreshAllDataAsync();
            }
        };

        /* Show warning on Running Jobs tab if login lacks msdb access */
        if (!_hasMsdbAccess)
        {
            RunningJobsMsdbWarning.Visibility = System.Windows.Visibility.Visible;
        }

        /* Initialize time picker ComboBoxes */
        InitializeTimeComboBoxes();

        /* Sync time display mode picker */
        var modeTag = ServerTimeHelper.CurrentDisplayMode.ToString();
        for (int i = 0; i < TimeDisplayModeBox.Items.Count; i++)
        {
            if (TimeDisplayModeBox.Items[i] is ComboBoxItem item && item.Tag?.ToString() == modeTag)
            {
                TimeDisplayModeBox.SelectedIndex = i;
                break;
            }
        }

        /* Initialize column filter managers */
        InitializeFilterManagers();

        /* Fix DataGrid copy — StackPanel headers copy as type name without this */
        foreach (var grid in new DataGrid[] { QuerySnapshotsGrid, QueryStatsGrid, ProcedureStatsGrid,
            QueryStoreGrid, PlanCorrectionGrid, BlockedProcessReportGrid, DeadlockGrid, RunningJobsGrid,
            ServerConfigGrid, DatabaseConfigGrid, DatabaseScopedConfigGrid, AutomaticTuningGrid, TraceFlagsGrid,
            CollectionHealthGrid, CollectionLogGrid, LatchStatsGrid, SpinlockStatsGrid,
            SchedulerIssuesGrid, SevereErrorsGrid, MemoryConditionsGrid,
            MemoryBrokerGrid, MemoryNodeOomGrid, SignificantWaitsGrid, CpuTasksGrid, IoIssuesGrid,
            DefaultTraceGrid, ServerConfigChangesGrid, DatabaseConfigChangesGrid, TraceFlagChangesGrid })
        {
            grid.CopyingRowClipboardContent += DataGridClipboardBehavior.FixHeaderCopy;
        }

        /* Apply theme immediately so charts don't flash white before data loads */
        ApplyTheme(WaitStatsChart);
        ApplyTheme(QueryDurationTrendChart);
        ApplyTheme(ProcDurationTrendChart);
        ApplyTheme(QueryStoreDurationTrendChart);
        ApplyTheme(ExecutionCountTrendChart);
        ApplyTheme(CpuChart);
        ApplyTheme(MemoryChart);
        ApplyTheme(MemoryClerksChart);
        ApplyTheme(MemoryGrantSizingChart);
        ApplyTheme(MemoryGrantActivityChart);
        ApplyTheme(MemoryPressureEventsChart);
        ApplyTheme(FileIoReadChart);
        ApplyTheme(FileIoWriteChart);
        ApplyTheme(FileIoReadThroughputChart);
        ApplyTheme(FileIoWriteThroughputChart);
        ApplyTheme(TempDbChart);
        ApplyTheme(TempDbFileIoChart);
        ApplyTheme(LockWaitTrendChart);
        ApplyTheme(BlockingTrendChart);
        ApplyTheme(DeadlockTrendChart);
        ApplyTheme(CurrentWaitsDurationChart);
        ApplyTheme(CurrentWaitsBlockedChart);
        ApplyTheme(PerfmonChart);
        ApplyTheme(CollectorDurationChart);
        ApplyTheme(QueryHeatmapChart);

        /* Chart hover tooltips */
        CorrelatedLanes.Initialize(_dataService, _serverId);
        CorrelatedLanes.ShowActiveQueriesRequested += OnActiveQueriesDrillDown;
        _waitStatsHover = new ChartHoverHelper(WaitStatsChart, "ms/sec");
        _perfmonHover = new ChartHoverHelper(PerfmonChart, "");
        _cpuHover = new ChartHoverHelper(CpuChart, "%");
        _memoryHover = new ChartHoverHelper(MemoryChart, "GB");
        _tempDbHover = new ChartHoverHelper(TempDbChart, "MB");
        _tempDbSizeHover = new ChartHoverHelper(TempDbSizeChart, "MB");
        _tempDbFileIoHover = new ChartHoverHelper(TempDbFileIoChart, "ms");
        _fileIoReadHover = new ChartHoverHelper(FileIoReadChart, "ms");
        _fileIoWriteHover = new ChartHoverHelper(FileIoWriteChart, "ms");
        _fileIoReadThroughputHover = new ChartHoverHelper(FileIoReadThroughputChart, "MB/s");
        _fileIoWriteThroughputHover = new ChartHoverHelper(FileIoWriteThroughputChart, "MB/s");
        _collectorDurationHover = new ChartHoverHelper(CollectorDurationChart, "ms");
        _queryDurationTrendHover = new ChartHoverHelper(QueryDurationTrendChart, "ms/sec");
        _procDurationTrendHover = new ChartHoverHelper(ProcDurationTrendChart, "ms/sec");
        _queryStoreDurationTrendHover = new ChartHoverHelper(QueryStoreDurationTrendChart, "ms/sec");
        _executionCountTrendHover = new ChartHoverHelper(ExecutionCountTrendChart, "/sec");
        _lockWaitTrendHover = new ChartHoverHelper(LockWaitTrendChart, "ms/sec");
        _blockingTrendHover = new ChartHoverHelper(BlockingTrendChart, "incidents");
        _deadlockTrendHover = new ChartHoverHelper(DeadlockTrendChart, "deadlocks");
        _memoryClerksHover = new ChartHoverHelper(MemoryClerksChart, "MB");
        _memoryGrantSizingHover = new ChartHoverHelper(MemoryGrantSizingChart, "MB");
        _memoryGrantActivityHover = new ChartHoverHelper(MemoryGrantActivityChart, "");
        _memoryPressureEventsHover = new ChartHoverHelper(MemoryPressureEventsChart, "events");
        _currentWaitsDurationHover = new ChartHoverHelper(CurrentWaitsDurationChart, "ms");
        _currentWaitsBlockedHover = new ChartHoverHelper(CurrentWaitsBlockedChart, "sessions");

        /* Latch/spinlock charts: theme + hover up front (own partial, mirrors Darling's tab file) */
        InitializeLatchSpinlockCharts();

        /* CPU Scheduler / Plan Cache / Session Stats charts: theme + hover up front (own partials,
           mirror Darling's tab files) so they don't flash white before the tabs' first load. */
        InitializeCpuSchedulerChart();
        InitializePlanCacheChart();
        InitializeSessionStatsChart();

        /* System Events Corruption/Contention counter charts: theme + hover up front (own partial,
           mirrors Darling's SystemHealthCharts). */
        InitializeSystemHealthCharts();

        /* Blocking Stats severity charts: theme + hover up front (own partial, mirrors Darling's
           Blocking Stats sub-tab) so the four charts don't flash white before the sub-tab's first load. */
        InitializeBlockingStatsCharts();

        /* Query heatmap hover popup */
        _heatmapPopupText = new TextBlock
        {
            Foreground = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0xE0, 0xE0, 0xE0)),
            FontSize = 13,
            MaxWidth = 450,
            TextTrimming = TextTrimming.CharacterEllipsis
        };
        _heatmapPopup = new System.Windows.Controls.Primitives.Popup
        {
            PlacementTarget = QueryHeatmapChart,
            Placement = System.Windows.Controls.Primitives.PlacementMode.Relative,
            IsHitTestVisible = false,
            AllowsTransparency = true,
            Child = new Border
            {
                Background = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0x33, 0x33, 0x33)),
                BorderBrush = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0x55, 0x55, 0x55)),
                BorderThickness = new Thickness(1),
                CornerRadius = new CornerRadius(3),
                Padding = new Thickness(8, 4, 8, 4),
                Child = _heatmapPopupText
            }
        };
        /* Heatmap mouse events wired up in XAML */
        var heatmapMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(QueryHeatmapChart, "Query_Heatmap", revertAction: RevertChartAxes);
        var heatmapDrillDown = new MenuItem { Header = "Show _Active Queries at This Time" };
        heatmapMenu.Items.Insert(0, heatmapDrillDown);
        heatmapMenu.Items.Insert(1, new Separator());
        heatmapMenu.Opened += (s, _) =>
        {
            if (_lastHeatmapResult == null || _heatmapPlottable == null || _lastHeatmapResult.TimeBuckets.Length == 0)
            {
                heatmapDrillDown.IsEnabled = false;
                return;
            }
            var mpos = System.Windows.Input.Mouse.GetPosition(QueryHeatmapChart);
            var mdpi = VisualTreeHelper.GetDpi(QueryHeatmapChart);
            var mpixel = new ScottPlot.Pixel((float)(mpos.X * mdpi.DpiScaleX), (float)(mpos.Y * mdpi.DpiScaleY));
            var mcoords = QueryHeatmapChart.Plot.GetCoordinates(mpixel);
            var (mCol, _) = _heatmapPlottable.GetIndexes(mcoords);
            if (mCol >= 0 && mCol < _lastHeatmapResult.TimeBuckets.Length)
            {
                heatmapDrillDown.Tag = _lastHeatmapResult.TimeBuckets[mCol];
                heatmapDrillDown.IsEnabled = true;
            }
            else
            {
                heatmapDrillDown.IsEnabled = false;
            }
        };
        heatmapDrillDown.Click += (s, _) =>
        {
            if (heatmapDrillDown.Tag is DateTime bucketTime)
                OnHeatmapDrillDown(bucketTime);
        };

        /* Chart context menus (right-click save/export) */
        var waitStatsMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(WaitStatsChart, "Wait_Stats", revertAction: RevertChartAxes);
        AddWaitDrillDownMenuItem(WaitStatsChart, waitStatsMenu);
        var queryDurationTrendMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(QueryDurationTrendChart, "Query_Duration_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(QueryDurationTrendChart, queryDurationTrendMenu, _queryDurationTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var procDurationTrendMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(ProcDurationTrendChart, "Procedure_Duration_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(ProcDurationTrendChart, procDurationTrendMenu, _procDurationTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var queryStoreDurationTrendMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(QueryStoreDurationTrendChart, "QueryStore_Duration_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(QueryStoreDurationTrendChart, queryStoreDurationTrendMenu, _queryStoreDurationTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var executionCountTrendMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(ExecutionCountTrendChart, "Execution_Count_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(ExecutionCountTrendChart, executionCountTrendMenu, _executionCountTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var cpuMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(CpuChart, "CPU_Usage", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(CpuChart, cpuMenu, _cpuHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var memoryMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryChart, "Memory_Usage", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(MemoryChart, memoryMenu, _memoryHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var memoryClerksMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryClerksChart, "Memory_Clerks", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(MemoryClerksChart, memoryClerksMenu, _memoryClerksHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var memoryGrantSizingMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryGrantSizingChart, "Memory_Grant_Sizing", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(MemoryGrantSizingChart, memoryGrantSizingMenu, _memoryGrantSizingHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var memoryGrantActivityMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryGrantActivityChart, "Memory_Grant_Activity", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(MemoryGrantActivityChart, memoryGrantActivityMenu, _memoryGrantActivityHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var memoryPressureEventsMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryPressureEventsChart, "Memory_Pressure_Events", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(MemoryPressureEventsChart, memoryPressureEventsMenu, _memoryPressureEventsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var fileIoReadMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoReadChart, "File_IO_Read_Latency", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(FileIoReadChart, fileIoReadMenu, _fileIoReadHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var fileIoWriteMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoWriteChart, "File_IO_Write_Latency", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(FileIoWriteChart, fileIoWriteMenu, _fileIoWriteHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var fileIoReadThroughputMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoReadThroughputChart, "File_IO_Read_Throughput", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(FileIoReadThroughputChart, fileIoReadThroughputMenu, _fileIoReadThroughputHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var fileIoWriteThroughputMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoWriteThroughputChart, "File_IO_Write_Throughput", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(FileIoWriteThroughputChart, fileIoWriteThroughputMenu, _fileIoWriteThroughputHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var tempDbMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(TempDbChart, "TempDB_Stats", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(TempDbChart, tempDbMenu, _tempDbHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var tempDbSizeMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(TempDbSizeChart, "TempDB_Allocated_Size", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(TempDbSizeChart, tempDbSizeMenu, _tempDbSizeHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var tempDbFileIoMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(TempDbFileIoChart, "TempDB_File_IO", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(TempDbFileIoChart, tempDbFileIoMenu, _tempDbFileIoHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var lockWaitMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(LockWaitTrendChart, "Lock_Wait_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(LockWaitTrendChart, lockWaitMenu, _lockWaitTrendHover, "Show _Blocking at This Time", OnBlockingDrillDown);
        var blockingMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(BlockingTrendChart, "Blocking_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(BlockingTrendChart, blockingMenu, _blockingTrendHover, "Show _Blocking at This Time", OnBlockingDrillDown);
        var deadlockMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(DeadlockTrendChart, "Deadlock_Trends", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(DeadlockTrendChart, deadlockMenu, _deadlockTrendHover, "Show Deadloc_ks at This Time", OnDeadlockDrillDown);
        var currentWaitsDurationMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(CurrentWaitsDurationChart, "Current_Waits_Duration", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(CurrentWaitsDurationChart, currentWaitsDurationMenu, _currentWaitsDurationHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var currentWaitsBlockedMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(CurrentWaitsBlockedChart, "Current_Waits_Blocked", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(CurrentWaitsBlockedChart, currentWaitsBlockedMenu, _currentWaitsBlockedHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        var perfmonMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(PerfmonChart, "Perfmon_Counters", revertAction: RevertChartAxes);
        AddChartDrillDownMenuItem(PerfmonChart, perfmonMenu, _perfmonHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        Helpers.ContextMenuHelper.SetupChartContextMenu(CollectorDurationChart, "Collector_Duration", revertAction: RevertChartAxes);

        /* Subscribe for the life of the tab. Do NOT unsubscribe on Unloaded — a TabControl fires
           Unloaded when you switch to another tab, which would permanently detach this handler so
           the charts stop following theme changes after the first tab switch. Unsubscribed in
           DisposeChartHelpers (called from MainWindow.CloseServerTab) when the tab is closed. */
        ThemeManager.ThemeChanged += OnThemeChanged;

        ActiveQueriesSlicer.RangeChanged += OnActiveQueriesSlicerChanged;
        QueryStatsSlicer.RangeChanged += OnQueryStatsSlicerChanged;
        ProcStatsSlicer.RangeChanged += OnProcStatsSlicerChanged;
        QueryStoreSlicer.RangeChanged += OnQueryStoreSlicerChanged;
        BlockingSlicer.RangeChanged += OnBlockingSlicerChanged;
        DeadlockSlicer.RangeChanged += OnDeadlockSlicerChanged;

        /* Initial load is triggered by MainWindow.ConnectToServer calling RefreshData()
           after collectors finish - no Loaded handler needed */

        KeyDown += ServerTab_KeyDown;
        Focusable = true;
    }

    private async void ServerTab_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
    {
        if (e.Key == System.Windows.Input.Key.V &&
            System.Windows.Input.Keyboard.Modifiers == System.Windows.Input.ModifierKeys.Control &&
            e.OriginalSource is not System.Windows.Controls.TextBox &&
            PlanViewerTabItem.IsSelected)
        {
            // Claim the paste gesture during event routing, before the async read yields on the retry path
            // (#2837): setting e.Handled after the await would leave the key event unsuppressed.
            e.Handled = true;
            // Re-entrancy guard (#2870): the async retry window yields the UI thread, so a HELD Ctrl+V
            // (OS key-repeat) could otherwise put several reads in flight at once and open several tabs.
            // Drop repeats while a paste runs; the key stays claimed above so it can't fall through.
            if (_pasteInProgress) return;
            _pasteInProgress = true;
            try
            {
                var (ok, xml) = await ClipboardText.TryReadAsync();
                if (ok && !string.IsNullOrWhiteSpace(xml))
                {
                    // Await so the guard spans the off-thread parse, not just the clipboard read (#2870).
                    await OpenPlanTab(xml, "Pasted Plan");
                    PlanViewerTabItem.IsSelected = true;
                }
            }
            finally { _pasteInProgress = false; }
        }
    }

    /// <summary>
    /// Returns true if the custom date range is selected and both dates are set.
    /// </summary>
    private bool IsCustomRange => TimeRangeCombo.SelectedIndex == 5
        && FromDatePicker?.SelectedDate != null
        && ToDatePicker?.SelectedDate != null;

    /// <summary>
    /// When the user switches main tabs or sub-tabs, refresh only the visible sub-tab.
    /// All sub-tabs are loaded on first load and manual refresh — tab/sub-tab switches
    /// only need to refresh the one the user is looking at.
    /// </summary>
    private async void MainTabControl_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _dataService == null) return;
        if (_isRefreshing) return;
        if (e.Source != MainTabControl && e.Source != QueriesSubTabControl
            && e.Source != MemorySubTabControl && e.Source != BlockingSubTabControl
            && e.Source != SystemEventsSubTabControl && e.Source != ConfigChangesSubTabControl) return;

        UpdateCompareDropdownState();

        // A drill-down navigates here programmatically and loads its own filtered snapshot;
        // skip the auto-refresh so it doesn't clobber that data via an async race. The flag is
        // set/cleared around the tab switch in SelectActiveQueriesForDrillDown().
        if (_suppressActiveQueriesAutoRefresh) return;

        var hoursBack = GetHoursBack();
        DateTime? fromDate = null, toDate = null;
        if (IsCustomRange)
        {
            var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
            var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
            if (fromLocal.HasValue && toLocal.HasValue)
            {
                fromDate = ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                toDate = ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode);
            }
        }
        var navContext = MainTabControl.SelectedIndex == 2
            ? $"TabNav-Queries.sub{QueriesSubTabControl.SelectedIndex}"
            : $"TabNav-tab{MainTabControl.SelectedIndex}";
        using var _navTimer = Helpers.MethodProfiler.StartTiming(navContext);
        await RefreshVisibleTabAsync(hoursBack, fromDate, toDate, subTabOnly: true);
    }

    private async void LiveSnapshot_Click(object sender, RoutedEventArgs e)
    {
        LiveSnapshotButton.IsEnabled = false;
        LiveSnapshotIndicator.Text = "Querying...";

        try
        {
            var connectionString = _credentialResolver.GetConnectionString(_server);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = 15
            };

            // Live query plans require VIEW SERVER PERFORMANCE STATE on Azure SQL DB,
            // which DB-scoped logins don't have — skip them there. See #857.
            var query = RemoteCollectorService.BuildQuerySnapshotsQuery(supportsLiveQueryPlan: !_isAzureSqlDatabase, isAzureSqlDatabase: _isAzureSqlDatabase);

            await using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;

            using var reader = await command.ExecuteReaderAsync();
            var results = new List<QuerySnapshotRow>();
            var snapshotTime = DateTime.UtcNow;

            while (await reader.ReadAsync())
            {
                results.Add(new QuerySnapshotRow
                {
                    SessionId = Convert.ToInt32(reader.GetValue(0)),
                    DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    QueryPlan = reader.IsDBNull(4) ? null : reader.GetString(4),
                    LiveQueryPlan = reader.IsDBNull(5) ? null : reader.GetValue(5)?.ToString(),
                    Status = reader.IsDBNull(6) ? "" : reader.GetString(6),
                    BlockingSessionId = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                    WaitType = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    WaitTimeMs = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
                    WaitResource = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    CpuTimeMs = reader.IsDBNull(11) ? 0 : Convert.ToInt64(reader.GetValue(11)),
                    TotalElapsedTimeMs = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
                    Reads = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
                    Writes = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
                    LogicalReads = reader.IsDBNull(15) ? 0 : Convert.ToInt64(reader.GetValue(15)),
                    GrantedQueryMemoryGb = reader.IsDBNull(16) ? 0 : Convert.ToDouble(reader.GetValue(16)),
                    TransactionIsolationLevel = reader.IsDBNull(17) ? "" : reader.GetString(17),
                    Dop = reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18)),
                    ParallelWorkerCount = reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19)),
                    LoginName = reader.IsDBNull(20) ? "" : reader.GetString(20),
                    HostName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                    ProgramName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                    OpenTransactionCount = reader.IsDBNull(23) ? 0 : Convert.ToInt32(reader.GetValue(23)),
                    PercentComplete = reader.IsDBNull(24) ? 0m : Convert.ToDecimal(reader.GetValue(24)),
                    CollectionTime = snapshotTime
                });
            }

            _querySnapshotsFilterMgr!.UpdateData(results);
            LiveSnapshotIndicator.Text = $"LIVE at {DateTime.Now:HH:mm:ss} ({results.Count} queries)";
        }
        catch (Exception ex)
        {
            LiveSnapshotIndicator.Text = $"Error: {ex.Message}";
            AppLogger.Error("ServerTab", $"Live snapshot failed: {ex.Message}");
        }
        finally
        {
            LiveSnapshotButton.IsEnabled = true;
        }
    }

    private void OpenLogFile_Click(object sender, RoutedEventArgs e)
    {
        var logDir = System.IO.Path.Combine(App.DataDirectory, "logs");
        var logFile = System.IO.Path.Combine(logDir, $"lite_{DateTime.Now:yyyyMMdd}.log");

        if (File.Exists(logFile))
        {
            Process.Start(new ProcessStartInfo(logFile) { UseShellExecute = true });
        }
        else if (Directory.Exists(logDir))
        {
            Process.Start(new ProcessStartInfo(logDir) { UseShellExecute = true });
        }
    }

    /// <summary>
    /// Stops the refresh timer when the tab is removed.
    /// </summary>
    public void StopRefresh()
    {
        _refreshTimer.Stop();
    }

    public void DisposeChartHelpers()
    {
        ThemeManager.ThemeChanged -= OnThemeChanged;
        /* Closing the server tab with plan tabs still open would leak each PlanViewerControl via the
           static ThemeChanged event — clean them up here too (ClosePlanTab_Click only handles the
           per-tab close-button path). */
        foreach (var item in PlanTabControl.Items)
            if (item is TabItem { Content: PlanViewerControl pv }) pv.Cleanup();
        _waitStatsHover?.Dispose();
        _perfmonHover?.Dispose();
        _cpuHover?.Dispose();
        _memoryHover?.Dispose();
        _tempDbHover?.Dispose();
        _tempDbSizeHover?.Dispose();
        _tempDbFileIoHover?.Dispose();
        _fileIoReadHover?.Dispose();
        _fileIoWriteHover?.Dispose();
        _fileIoReadThroughputHover?.Dispose();
        _fileIoWriteThroughputHover?.Dispose();
        _collectorDurationHover?.Dispose();
        _queryDurationTrendHover?.Dispose();
        _procDurationTrendHover?.Dispose();
        _queryStoreDurationTrendHover?.Dispose();
        _executionCountTrendHover?.Dispose();
        _lockWaitTrendHover?.Dispose();
        _blockingTrendHover?.Dispose();
        _deadlockTrendHover?.Dispose();
        _memoryClerksHover?.Dispose();
        _memoryGrantSizingHover?.Dispose();
        _memoryGrantActivityHover?.Dispose();
        _memoryPressureEventsHover?.Dispose();
        _currentWaitsDurationHover?.Dispose();
        _currentWaitsBlockedHover?.Dispose();
        DisposeLatchSpinlockHelpers();
        DisposeCpuSchedulerHelpers();
        DisposePlanCacheHelpers();
        DisposeSessionStatsHelpers();
        DisposeSystemHealthChartHelpers();
        DisposeBlockingStatsHelpers();
    }
}
