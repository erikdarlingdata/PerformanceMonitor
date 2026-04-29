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

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private readonly ServerConnection _server;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    public int ServerId => _serverId;
    public ServerConnection Server => _server;
    private readonly CredentialService _credentialService;
    private readonly DispatcherTimer _refreshTimer;
    private bool _isRefreshing;
    private readonly Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();
    private List<SelectableItem> _waitTypeItems = new();
    private List<SelectableItem> _perfmonCounterItems = new();
    private Helpers.ChartHoverHelper? _waitStatsHover;
    private Helpers.ChartHoverHelper? _perfmonHover;
    private Helpers.ChartHoverHelper? _cpuHover;
    private Helpers.ChartHoverHelper? _memoryHover;
    private Helpers.ChartHoverHelper? _tempDbHover;
    private Helpers.ChartHoverHelper? _tempDbFileIoHover;
    private Helpers.ChartHoverHelper? _fileIoReadHover;
    private Helpers.ChartHoverHelper? _fileIoWriteHover;
    private Helpers.ChartHoverHelper? _fileIoReadThroughputHover;
    private Helpers.ChartHoverHelper? _fileIoWriteThroughputHover;
    private Helpers.ChartHoverHelper? _collectorDurationHover;
    private Helpers.ChartHoverHelper? _queryDurationTrendHover;
    private Helpers.ChartHoverHelper? _procDurationTrendHover;
    private Helpers.ChartHoverHelper? _queryStoreDurationTrendHover;
    private Helpers.ChartHoverHelper? _executionCountTrendHover;
    private Helpers.ChartHoverHelper? _lockWaitTrendHover;
    private Helpers.ChartHoverHelper? _blockingTrendHover;
    private Helpers.ChartHoverHelper? _deadlockTrendHover;
    private Helpers.ChartHoverHelper? _memoryClerksHover;
    private Helpers.ChartHoverHelper? _memoryGrantSizingHover;
    private Helpers.ChartHoverHelper? _memoryGrantActivityHover;
    private Helpers.ChartHoverHelper? _memoryPressureEventsHover;
    private Helpers.ChartHoverHelper? _currentWaitsDurationHover;
    private Helpers.ChartHoverHelper? _currentWaitsBlockedHover;

    /* Query heatmap */
    private HeatmapResult? _lastHeatmapResult;
    private ScottPlot.Plottables.Heatmap? _heatmapPlottable;
    private System.Windows.Controls.Primitives.Popup? _heatmapPopup;
    private TextBlock? _heatmapPopupText;

    /* Memory clerks picker */
    private List<SelectableItem> _memoryClerkItems = new();
    private bool _isUpdatingMemoryClerkSelection;

    /* Column filtering */
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private readonly Dictionary<DataGrid, IDataGridFilterManager> _filterManagers = new();
    private DataGridFilterManager<QuerySnapshotRow>? _querySnapshotsFilterMgr;
    private DataGridFilterManager<QueryStatsRow>? _queryStatsFilterMgr;
    private DataGridFilterManager<ProcedureStatsRow>? _procStatsFilterMgr;
    private DataGridFilterManager<QueryStoreRow>? _queryStoreFilterMgr;
    private DataGridFilterManager<BlockedProcessReportRow>? _blockedProcessFilterMgr;
    private DataGridFilterManager<DeadlockProcessDetail>? _deadlockFilterMgr;
    private DataGridFilterManager<RunningJobRow>? _runningJobsFilterMgr;
    private DataGridFilterManager<ServerConfigRow>? _serverConfigFilterMgr;
    private DataGridFilterManager<DatabaseConfigRow>? _databaseConfigFilterMgr;
    private DataGridFilterManager<DatabaseScopedConfigRow>? _dbScopedConfigFilterMgr;
    private DataGridFilterManager<TraceFlagRow>? _traceFlagsFilterMgr;
    private DataGridFilterManager<CollectorHealthRow>? _collectionHealthFilterMgr;
    private DataGridFilterManager<CollectionLogRow>? _collectionLogFilterMgr;
    private DateTime? _dailySummaryDate; // null = today
    private CancellationTokenSource? _actualPlanCts;

    public int UtcOffsetMinutes { get; }
    private readonly bool _hasMsdbAccess;
    private readonly bool _isAzureSqlDatabase;

    /// <summary>
    /// Raised after each data refresh with alert counts for tab badge display.
    /// </summary>
    public event Action<int, int, DateTime?>? AlertCountsChanged; /* blockingCount, deadlockCount, latestEventTimeUtc */
    public event Action<int>? ApplyTimeRangeRequested; /* selectedIndex */
    public event Func<Task>? ManualRefreshRequested;

    public ServerTab(ServerConnection server, DuckDbInitializer duckDb, CredentialService credentialService, int utcOffsetMinutes = 0, bool hasMsdbAccess = true, bool isAzureSqlDatabase = false)
    {
        InitializeComponent();

        _server = server;
        _dataService = new LocalDataService(duckDb);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        _credentialService = credentialService;
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
            await RefreshAllDataAsync(fullRefresh: false);
        };
        _refreshTimer.Start();

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
            QueryStoreGrid, BlockedProcessReportGrid, DeadlockGrid, RunningJobsGrid,
            ServerConfigGrid, DatabaseConfigGrid, DatabaseScopedConfigGrid, TraceFlagsGrid,
            CollectionHealthGrid, CollectionLogGrid })
        {
            grid.CopyingRowClipboardContent += Helpers.DataGridClipboardBehavior.FixHeaderCopy;
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
        _waitStatsHover = new Helpers.ChartHoverHelper(WaitStatsChart, "ms/sec");
        _perfmonHover = new Helpers.ChartHoverHelper(PerfmonChart, "");
        _cpuHover = new Helpers.ChartHoverHelper(CpuChart, "%");
        _memoryHover = new Helpers.ChartHoverHelper(MemoryChart, "GB");
        _tempDbHover = new Helpers.ChartHoverHelper(TempDbChart, "MB");
        _tempDbFileIoHover = new Helpers.ChartHoverHelper(TempDbFileIoChart, "ms");
        _fileIoReadHover = new Helpers.ChartHoverHelper(FileIoReadChart, "ms");
        _fileIoWriteHover = new Helpers.ChartHoverHelper(FileIoWriteChart, "ms");
        _fileIoReadThroughputHover = new Helpers.ChartHoverHelper(FileIoReadThroughputChart, "MB/s");
        _fileIoWriteThroughputHover = new Helpers.ChartHoverHelper(FileIoWriteThroughputChart, "MB/s");
        _collectorDurationHover = new Helpers.ChartHoverHelper(CollectorDurationChart, "ms");
        _queryDurationTrendHover = new Helpers.ChartHoverHelper(QueryDurationTrendChart, "ms/sec");
        _procDurationTrendHover = new Helpers.ChartHoverHelper(ProcDurationTrendChart, "ms/sec");
        _queryStoreDurationTrendHover = new Helpers.ChartHoverHelper(QueryStoreDurationTrendChart, "ms/sec");
        _executionCountTrendHover = new Helpers.ChartHoverHelper(ExecutionCountTrendChart, "/sec");
        _lockWaitTrendHover = new Helpers.ChartHoverHelper(LockWaitTrendChart, "ms/sec");
        _blockingTrendHover = new Helpers.ChartHoverHelper(BlockingTrendChart, "incidents");
        _deadlockTrendHover = new Helpers.ChartHoverHelper(DeadlockTrendChart, "deadlocks");
        _memoryClerksHover = new Helpers.ChartHoverHelper(MemoryClerksChart, "MB");
        _memoryGrantSizingHover = new Helpers.ChartHoverHelper(MemoryGrantSizingChart, "MB");
        _memoryGrantActivityHover = new Helpers.ChartHoverHelper(MemoryGrantActivityChart, "");
        _memoryPressureEventsHover = new Helpers.ChartHoverHelper(MemoryPressureEventsChart, "events");
        _currentWaitsDurationHover = new Helpers.ChartHoverHelper(CurrentWaitsDurationChart, "ms");
        _currentWaitsBlockedHover = new Helpers.ChartHoverHelper(CurrentWaitsBlockedChart, "sessions");

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
        var heatmapMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(QueryHeatmapChart, "Query_Heatmap");
        var heatmapDrillDown = new MenuItem { Header = "Show Active Queries at This Time" };
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
        var waitStatsMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(WaitStatsChart, "Wait_Stats");
        AddWaitDrillDownMenuItem(WaitStatsChart, waitStatsMenu);
        Helpers.ContextMenuHelper.SetupChartContextMenu(QueryDurationTrendChart, "Query_Duration_Trends");
        Helpers.ContextMenuHelper.SetupChartContextMenu(ProcDurationTrendChart, "Procedure_Duration_Trends");
        Helpers.ContextMenuHelper.SetupChartContextMenu(QueryStoreDurationTrendChart, "QueryStore_Duration_Trends");
        Helpers.ContextMenuHelper.SetupChartContextMenu(ExecutionCountTrendChart, "Execution_Count_Trends");
        var cpuMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(CpuChart, "CPU_Usage");
        AddChartDrillDownMenuItem(CpuChart, cpuMenu, _cpuHover, "Show Active Queries at This Time", OnCpuDrillDown);
        var memoryMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryChart, "Memory_Usage");
        AddChartDrillDownMenuItem(MemoryChart, memoryMenu, _memoryHover, "Show Active Queries at This Time", OnMemoryDrillDown);
        Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryClerksChart, "Memory_Clerks");
        Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryGrantSizingChart, "Memory_Grant_Sizing");
        Helpers.ContextMenuHelper.SetupChartContextMenu(MemoryGrantActivityChart, "Memory_Grant_Activity");
        Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoReadChart, "File_IO_Read_Latency");
        Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoWriteChart, "File_IO_Write_Latency");
        Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoReadThroughputChart, "File_IO_Read_Throughput");
        Helpers.ContextMenuHelper.SetupChartContextMenu(FileIoWriteThroughputChart, "File_IO_Write_Throughput");
        var tempDbMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(TempDbChart, "TempDB_Stats");
        AddChartDrillDownMenuItem(TempDbChart, tempDbMenu, _tempDbHover, "Show Active Queries at This Time", OnTempDbDrillDown);
        Helpers.ContextMenuHelper.SetupChartContextMenu(TempDbFileIoChart, "TempDB_File_IO");
        Helpers.ContextMenuHelper.SetupChartContextMenu(LockWaitTrendChart, "Lock_Wait_Trends");
        var blockingMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(BlockingTrendChart, "Blocking_Trends");
        AddChartDrillDownMenuItem(BlockingTrendChart, blockingMenu, _blockingTrendHover, "Show Blocking at This Time", OnBlockingDrillDown);
        var deadlockMenu = Helpers.ContextMenuHelper.SetupChartContextMenu(DeadlockTrendChart, "Deadlock_Trends");
        AddChartDrillDownMenuItem(DeadlockTrendChart, deadlockMenu, _deadlockTrendHover, "Show Deadlocks at This Time", OnDeadlockDrillDown);
        Helpers.ContextMenuHelper.SetupChartContextMenu(CurrentWaitsDurationChart, "Current_Waits_Duration");
        Helpers.ContextMenuHelper.SetupChartContextMenu(CurrentWaitsBlockedChart, "Current_Waits_Blocked");
        Helpers.ContextMenuHelper.SetupChartContextMenu(PerfmonChart, "Perfmon_Counters");
        Helpers.ContextMenuHelper.SetupChartContextMenu(CollectorDurationChart, "Collector_Duration");

        Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
        Unloaded += (_, _) => Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;

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

    private void ServerTab_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
    {
        if (e.Key == System.Windows.Input.Key.V &&
            System.Windows.Input.Keyboard.Modifiers == System.Windows.Input.ModifierKeys.Control &&
            e.OriginalSource is not System.Windows.Controls.TextBox &&
            PlanViewerTabItem.IsSelected)
        {
            var xml = System.Windows.Clipboard.GetText();
            if (!string.IsNullOrWhiteSpace(xml))
            {
                e.Handled = true;
                OpenPlanTab(xml, "Pasted Plan");
                PlanViewerTabItem.IsSelected = true;
            }
        }
    }

    private void InitializeTimeComboBoxes()
    {
        // Populate hour ComboBoxes (12-hour format with AM/PM)
        var hours = new List<string>();
        for (int h = 0; h < 24; h++)
        {
            var dt = DateTime.Today.AddHours(h);
            hours.Add(dt.ToString("HH:00")); // "00:00", "01:00", ..., "23:00"
        }

        FromHourCombo.ItemsSource = hours;
        ToHourCombo.ItemsSource = hours;
        FromHourCombo.SelectedIndex = 0;  // Default to 12 AM
        ToHourCombo.SelectedIndex = 23;   // Default to 11 PM

        // Populate minute ComboBoxes (15-minute intervals)
        var minutes = new List<string> { ":00", ":15", ":30", ":45" };
        FromMinuteCombo.ItemsSource = minutes;
        ToMinuteCombo.ItemsSource = minutes;
        FromMinuteCombo.SelectedIndex = 0; // Default to :00
        ToMinuteCombo.SelectedIndex = 3;   // Default to :45 (so 11:45 PM is end)
    }

    private DateTime? GetDateTimeFromPickers(DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
    {
        if (!datePicker.SelectedDate.HasValue) return null;

        var date = datePicker.SelectedDate.Value.Date;
        int hour = hourCombo.SelectedIndex >= 0 ? hourCombo.SelectedIndex : 0;
        int minute = minuteCombo.SelectedIndex >= 0 ? minuteCombo.SelectedIndex * 15 : 0;

        return date.AddHours(hour).AddMinutes(minute);
    }

    private void SetPickersFromDateTime(DateTime serverTime, DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
    {
        /* Convert server time to the current display mode for UI */
        var displayTime = ServerTimeHelper.ConvertForDisplay(serverTime, ServerTimeHelper.CurrentDisplayMode);
        datePicker.SelectedDate = displayTime.Date;
        hourCombo.SelectedIndex = displayTime.Hour;
        minuteCombo.SelectedIndex = displayTime.Minute / 15;
    }

    /// <summary>
    /// Gets the selected time range in hours.
    /// </summary>
    private int GetHoursBack()
    {
        return TimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 4
        };
    }

    /// <summary>
    /// Gets the UTC time range for slicer display, matching GetTimeRange in LocalDataService.
    /// </summary>
    private static (DateTime start, DateTime end) GetSlicerTimeRange(
        int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (fromDate.HasValue && toDate.HasValue)
        {
            var startUtc = fromDate.Value.AddMinutes(-ServerTimeHelper.UtcOffsetMinutes);
            var endUtc = toDate.Value.AddMinutes(-ServerTimeHelper.UtcOffsetMinutes);
            return (startUtc, endUtc);
        }

        return (DateTime.UtcNow.AddHours(-hoursBack), DateTime.UtcNow);
    }

    /// <summary>
    /// Sets the time range dropdown from outside (used by Apply to All).
    /// </summary>
    public void SetTimeRangeIndex(int index)
    {
        if (index >= 0 && index < TimeRangeCombo.Items.Count)
        {
            TimeRangeCombo.SelectedIndex = index;
        }
    }

    private void ApplyTimeRangeToAll_Click(object sender, RoutedEventArgs e)
    {
        ApplyTimeRangeRequested?.Invoke(TimeRangeCombo.SelectedIndex);
    }

    private void AutoRefreshCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (_refreshTimer == null) return;

        if (AutoRefreshCheckBox.IsChecked == true)
        {
            UpdateAutoRefreshInterval();
            _refreshTimer.Start();
        }
        else
        {
            _refreshTimer.Stop();
        }
    }

    private void AutoRefreshInterval_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (_refreshTimer == null) return;
        UpdateAutoRefreshInterval();
    }

    private void UpdateAutoRefreshInterval()
    {
        if (AutoRefreshIntervalCombo == null) return;

        _refreshTimer.Interval = AutoRefreshIntervalCombo.SelectedIndex switch
        {
            0 => TimeSpan.FromSeconds(30),
            1 => TimeSpan.FromMinutes(1),
            2 => TimeSpan.FromMinutes(5),
            _ => TimeSpan.FromMinutes(1)
        };
    }

    private async void RefreshDataButton_Click(object sender, RoutedEventArgs e)
    {
        RefreshDataButton.IsEnabled = false;
        try
        {
            if (ManualRefreshRequested != null)
            {
                await ManualRefreshRequested.Invoke();
            }
            /* Manual refresh loads all sub-tabs of the visible tab, not all 13 tabs */
            await RefreshAllDataAsync(fullRefresh: false);
        }
        finally
        {
            RefreshDataButton.IsEnabled = true;
        }
    }

    private void TimeDisplayMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        if (TimeDisplayModeBox.SelectedItem is not ComboBoxItem item) return;
        var tag = item.Tag?.ToString();
        var mode = tag switch
        {
            "LocalTime" => TimeDisplayMode.LocalTime,
            "UTC" => TimeDisplayMode.UTC,
            _ => TimeDisplayMode.ServerTime
        };
        if (mode == ServerTimeHelper.CurrentDisplayMode) return;

        // Re-convert custom range pickers from old display mode to new.
        // Suppress refreshes while updating pickers to avoid cascading queries.
        var oldMode = ServerTimeHelper.CurrentDisplayMode;
        _isRefreshing = true;
        try
        {
            if (IsCustomRange)
            {
                var fromPicker = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toPicker = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromPicker.HasValue && toPicker.HasValue)
                {
                    var fromServer = ServerTimeHelper.DisplayTimeToServerTime(fromPicker.Value, oldMode);
                    var toServer = ServerTimeHelper.DisplayTimeToServerTime(toPicker.Value, oldMode);
                    ServerTimeHelper.CurrentDisplayMode = mode;
                    var fromNew = ServerTimeHelper.ConvertForDisplay(fromServer, mode);
                    var toNew = ServerTimeHelper.ConvertForDisplay(toServer, mode);
                    FromDatePicker.SelectedDate = fromNew.Date;
                    FromHourCombo.SelectedIndex = fromNew.Hour;
                    FromMinuteCombo.SelectedIndex = fromNew.Minute / 15;
                    ToDatePicker.SelectedDate = toNew.Date;
                    ToHourCombo.SelectedIndex = toNew.Hour;
                    ToMinuteCombo.SelectedIndex = toNew.Minute / 15;
                }
                else
                {
                    ServerTimeHelper.CurrentDisplayMode = mode;
                }
            }
            else
            {
                ServerTimeHelper.CurrentDisplayMode = mode;
            }
        }
        finally
        {
            _isRefreshing = false;
        }

        // Refresh all DataGrid bindings so ServerTimeConverter re-evaluates
        QuerySnapshotsGrid.Items.Refresh();
        QueryStatsGrid.Items.Refresh();
        ProcedureStatsGrid.Items.Refresh();
        QueryStoreGrid.Items.Refresh();
        BlockedProcessReportGrid.Items.Refresh();
        DeadlockGrid.Items.Refresh();
        RunningJobsGrid.Items.Refresh();
        CollectionHealthGrid.Items.Refresh();
        CollectionLogGrid.Items.Refresh();

        // Refresh slicer labels
        ActiveQueriesSlicer.Redraw();
        QueryStatsSlicer.Redraw();
        ProcStatsSlicer.Redraw();
        QueryStoreSlicer.Redraw();
        BlockingSlicer.Redraw();
        DeadlockSlicer.Redraw();
    }

    private async void TimeRangeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;

        /* Show/hide custom date pickers and time ComboBoxes */
        var isCustom = TimeRangeCombo.SelectedIndex == 5;
        var visibility = isCustom ? Visibility.Visible : Visibility.Collapsed;

        if (FromDatePicker != null)
        {
            FromDatePicker.Visibility = visibility;
            FromHourCombo.Visibility = visibility;
            FromMinuteCombo.Visibility = visibility;
            ToLabel.Visibility = visibility;
            ToDatePicker.Visibility = visibility;
            ToHourCombo.Visibility = visibility;
            ToMinuteCombo.Visibility = visibility;

            if (isCustom && FromDatePicker.SelectedDate == null)
            {
                FromDatePicker.SelectedDate = DateTime.Today.AddDays(-1);
                ToDatePicker.SelectedDate = DateTime.Today;
            }
        }

        if (!isCustom)
        {
            await RefreshAllDataAsync(fullRefresh: false);
        }
    }

    private async void CustomDateRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;
        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            await RefreshAllDataAsync(fullRefresh: false);
        }
    }

    private async void CustomTimeCombo_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;
        /* Only refresh if we have valid dates selected */
        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            await RefreshAllDataAsync(fullRefresh: false);
        }
    }

    private void DatePicker_CalendarOpened(object sender, RoutedEventArgs e)
    {
        if (sender is DatePicker datePicker)
        {
            /* Use Dispatcher to ensure visual tree is ready */
            Dispatcher.BeginInvoke(new Action(() =>
            {
                var popup = datePicker.Template.FindName("PART_Popup", datePicker) as System.Windows.Controls.Primitives.Popup;
                if (popup?.Child is System.Windows.Controls.Calendar calendar)
                {
                    ApplyThemeToCalendar(calendar);
                }
            }));
        }
    }

    private void ApplyThemeToCalendar(System.Windows.Controls.Calendar calendar)
    {
        SolidColorBrush primaryBg, fg, borderBrush;

        if (Helpers.ThemeManager.CurrentTheme == "CoolBreeze")
        {
            primaryBg   = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#EEF4FA")!);
            fg          = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#1A2A3A")!);
            borderBrush = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#A8BDD0")!);
        }
        else if (Helpers.ThemeManager.HasLightBackground)
        {
            primaryBg   = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF));
            fg          = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0x1A, 0x1D, 0x23));
            borderBrush = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0xDE, 0xE2, 0xE6));
        }
        else
        {
            primaryBg   = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#111217")!);
            fg          = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#E4E6EB")!);
            borderBrush = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#2a2d35")!);
        }

        calendar.Background = primaryBg;
        calendar.Foreground = fg;
        calendar.BorderBrush = borderBrush;

        ApplyThemeRecursively(calendar, primaryBg, fg);
    }

    private void ApplyThemeRecursively(DependencyObject parent, Brush primaryBg, Brush fg)
    {
        bool HasLightBackground = Helpers.ThemeManager.HasLightBackground;
        for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);

            if (child is System.Windows.Controls.Primitives.CalendarItem calendarItem)
            {
                calendarItem.Background = primaryBg;
                calendarItem.Foreground = fg;
            }
            else if (child is System.Windows.Controls.Primitives.CalendarDayButton dayButton)
            {
                dayButton.Background = Brushes.Transparent;
                dayButton.Foreground = fg;
            }
            else if (child is System.Windows.Controls.Primitives.CalendarButton calButton)
            {
                calButton.Background = Brushes.Transparent;
                calButton.Foreground = fg;
            }
            else if (child is Button button)
            {
                button.Background = Brushes.Transparent;
                button.Foreground = fg;
            }
            else if (child is TextBlock textBlock)
            {
                textBlock.Foreground = fg;
            }
            else if (!HasLightBackground)
            {
                if (child is Border border && border.Background is SolidColorBrush bg && bg.Color.R > 200 && bg.Color.G > 200 && bg.Color.B > 200)
                    border.Background = primaryBg;
                else if (child is Grid grid && grid.Background is SolidColorBrush gridBg && gridBg.Color.R > 200 && gridBg.Color.G > 200 && gridBg.Color.B > 200)
                    grid.Background = primaryBg;
            }

            ApplyThemeRecursively(child, primaryBg, fg);
        }
    }

    /// <summary>
    /// Returns true if the custom date range is selected and both dates are set.
    /// </summary>
    private bool IsCustomRange => TimeRangeCombo.SelectedIndex == 5
        && FromDatePicker?.SelectedDate != null
        && ToDatePicker?.SelectedDate != null;

    private void BlockedProcessReportGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_blockingSlicerData == null || _blockingSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col))
        {
            if (e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
                col = b.Path.Path;
        }
        var (metric, label) = col switch
        {
            "WaitTimeMs" => ("TotalCpu", "Total Wait (sec)"),
            "BlockingSpid" => ("TotalElapsed", "Distinct Blockers"),
            "BlockedSpid" => ("TotalReads", "Distinct Blocked"),
            "DatabaseName" => ("TotalLogicalReads", "Distinct Databases"),
            _ => ("Events", "Blocking Events"),
        };

        if (metric == _blockingSlicerMetric) return;
        _blockingSlicerMetric = metric;

        foreach (var bucket in _blockingSlicerData)
        {
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "TotalElapsed" => bucket.TotalElapsed,
                "TotalReads" => bucket.TotalReads,
                "TotalLogicalReads" => bucket.TotalLogicalReads,
                _ => bucket.SessionCount,
            };
        }

        BlockingSlicer.UpdateMetric(label);
    }

    private async void OnBlockingSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);

            var bpr = await _dataService.GetRecentBlockedProcessReportsAsync(_serverId, 0, fromServer, toServer);
            _blockedProcessFilterMgr!.UpdateData(bpr);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnBlockingSlicerChanged failed: {ex.Message}");
        }
    }

    private async void OnDeadlockSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);

            var dlr = await _dataService.GetRecentDeadlocksAsync(_serverId, 0, fromServer, toServer);
            _deadlockFilterMgr!.UpdateData(DeadlockProcessDetail.ParseFromRows(dlr));
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnDeadlockSlicerChanged failed: {ex.Message}");
        }
    }

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
            && e.Source != MemorySubTabControl && e.Source != BlockingSubTabControl) return;

        UpdateCompareDropdownState();

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
        await RefreshVisibleTabAsync(hoursBack, fromDate, toDate, subTabOnly: true);
    }

    // ── Grid → Slicer Overlay (#683) ──

    private (DateTime? fromDate, DateTime? toDate) GetCurrentViewDates()
    {
        if (IsCustomRange)
        {
            var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
            var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
            if (fromLocal.HasValue && toLocal.HasValue)
                return (ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode),
                        ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode));
        }
        return (null, null);
    }

    /// <summary>
    /// Computes per-interval deltas from cumulative history values.
    /// Picks the metric field based on the current slicer sort metric.
    /// </summary>
    private static List<(DateTime TimeUtc, double Value)> ComputeQueryOverlayPoints(
        List<QueryStatsHistoryRow> history, string slicerMetric)
    {
        Func<QueryStatsHistoryRow, long> selector = slicerMetric switch
        {
            "TotalCpu" or "AvgCpu" => h => h.DeltaCpuUs,
            "TotalReads" or "AvgReads" => h => h.DeltaLogicalReads,
            "TotalWrites" => h => h.DeltaLogicalWrites,
            "TotalPhysReads" => h => h.DeltaPhysicalReads,
            _ => h => h.DeltaElapsedUs, // TotalElapsed, AvgElapsed, default
        };
        bool isMicroseconds = slicerMetric is "TotalCpu" or "AvgCpu" or "TotalElapsed" or "AvgElapsed";

        var points = new List<(DateTime TimeUtc, double Value)>();
        for (int i = 1; i < history.Count; i++)
        {
            var delta = selector(history[i]) - selector(history[i - 1]);
            if (delta > 0)
                points.Add((history[i].CollectionTime, isMicroseconds ? delta / 1000.0 : delta));
        }
        return points;
    }

    private static List<(DateTime TimeUtc, double Value)> ComputeProcOverlayPoints(
        List<ProcedureStatsHistoryRow> history, string slicerMetric)
    {
        Func<ProcedureStatsHistoryRow, long> selector = slicerMetric switch
        {
            "TotalCpu" or "AvgCpu" => h => h.DeltaCpuUs,
            "TotalReads" or "AvgReads" => h => h.DeltaLogicalReads,
            "TotalWrites" => h => h.DeltaLogicalWrites,
            "TotalPhysReads" => h => h.DeltaPhysicalReads,
            _ => h => h.DeltaElapsedUs,
        };
        bool isMicroseconds = slicerMetric is "TotalCpu" or "AvgCpu" or "TotalElapsed" or "AvgElapsed";

        var points = new List<(DateTime TimeUtc, double Value)>();
        for (int i = 1; i < history.Count; i++)
        {
            var delta = selector(history[i]) - selector(history[i - 1]);
            if (delta > 0)
                points.Add((history[i].CollectionTime, isMicroseconds ? delta / 1000.0 : delta));
        }
        return points;
    }

    private async void QueryStatsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (QueryStatsGrid.SelectedItem is not QueryStatsRow row || string.IsNullOrEmpty(row.QueryHash))
        {
            if (!_isRefreshing) QueryStatsSlicer.ClearOverlay();
            return;
        }

        try
        {
            var hoursBack = GetHoursBack();
            var (fromDate, toDate) = GetCurrentViewDates();
            var history = await _dataService.GetQueryStatsHistoryAsync(_serverId, row.DatabaseName, row.QueryHash, hoursBack, fromDate, toDate);

            var points = ComputeQueryOverlayPoints(history, _queryStatsSlicerMetric);
            QueryStatsSlicer.SetOverlay(points, row.QueryHash);
        }
        catch { QueryStatsSlicer.ClearOverlay(); }
    }

    private async void ProcedureStatsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (ProcedureStatsGrid.SelectedItem is not ProcedureStatsRow row || string.IsNullOrEmpty(row.ObjectName))
        {
            if (!_isRefreshing) ProcStatsSlicer.ClearOverlay();
            return;
        }

        try
        {
            var hoursBack = GetHoursBack();
            var (fromDate, toDate) = GetCurrentViewDates();
            var history = await _dataService.GetProcedureStatsHistoryAsync(_serverId, row.DatabaseName, row.SchemaName, row.ObjectName, hoursBack, fromDate, toDate);

            var points = ComputeProcOverlayPoints(history, _procStatsSlicerMetric);
            var label = row.ObjectName.Length > 30 ? row.ObjectName[..30] + "..." : row.ObjectName;
            ProcStatsSlicer.SetOverlay(points, label);
        }
        catch { ProcStatsSlicer.ClearOverlay(); }
    }

    private async void QueryStoreGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (QueryStoreGrid.SelectedItem is not QueryStoreRow row)
        {
            if (!_isRefreshing) QueryStoreSlicer.ClearOverlay();
            return;
        }

        try
        {
            var hoursBack = GetHoursBack();
            var (fromDate, toDate) = GetCurrentViewDates();
            var history = await _dataService.GetQueryStoreHistoryAsync(_serverId, row.DatabaseName, row.QueryId, row.PlanId, hoursBack, fromDate, toDate);

            // Query Store values are already per-interval averages, not cumulative
            Func<QueryStoreHistoryRow, double> selector = _queryStoreSlicerMetric switch
            {
                "TotalCpu" or "AvgCpu" => h => h.TotalCpuMs,
                "TotalReads" or "AvgReads" => h => h.AvgLogicalReads * h.ExecutionCount,
                _ => h => h.TotalDurationMs,
            };

            var points = history
                .Where(h => selector(h) > 0)
                .Select(h => (h.CollectionTime, selector(h)))
                .ToList();

            var qsLabel = !string.IsNullOrWhiteSpace(row.ModuleName)
                ? row.ModuleName
                : $"Query {row.QueryId} / Plan {row.PlanId}";
            QueryStoreSlicer.SetOverlay(points, qsLabel);
        }
        catch { QueryStoreSlicer.ClearOverlay(); }
    }

    private void QueryStatsGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (QueryStatsGrid.SelectedItem is not QueryStatsRow item) return;
        if (string.IsNullOrEmpty(item.DatabaseName) || string.IsNullOrEmpty(item.QueryHash)) return;

        var connStr = _server.GetConnectionString(_credentialService);
        var window = new Windows.QueryStatsHistoryWindow(_dataService, _serverId, item.DatabaseName, item.QueryHash, GetHoursBack(), connStr);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    private void ProcedureStatsGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (ProcedureStatsGrid.SelectedItem is not ProcedureStatsRow item) return;
        if (string.IsNullOrEmpty(item.DatabaseName) || string.IsNullOrEmpty(item.ObjectName)) return;

        var connStr = _server.GetConnectionString(_credentialService);
        var window = new Windows.ProcedureHistoryWindow(_dataService, _serverId, item.DatabaseName, item.SchemaName, item.ObjectName, GetHoursBack(), connStr);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    private void QueryStoreGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (QueryStoreGrid.SelectedItem is not QueryStoreRow item) return;
        if (string.IsNullOrEmpty(item.DatabaseName) || item.QueryId == 0) return;

        var connStr = _server.GetConnectionString(_credentialService);
        var window = new Windows.QueryStoreHistoryWindow(_dataService, _serverId, item.DatabaseName, item.QueryId, item.PlanId, item.QueryText, GetHoursBack(), connStr);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }


    private void CollectionHealthGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (CollectionHealthGrid.SelectedItem is not CollectorHealthRow item) return;

        var window = new Windows.CollectionLogWindow(_dataService, _serverId, item.CollectorName);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    private void DailySummaryToday_Click(object sender, RoutedEventArgs e)
    {
        _dailySummaryDate = null;
        DailySummaryDatePicker.SelectedDate = null;
        DailySummaryTodayButton.FontWeight = FontWeights.Bold;
        DailySummaryIndicator.Text = "Showing: Today (UTC)";
        DailySummaryRefresh_Click(sender, e);
    }

    private void DailySummaryDate_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (DailySummaryDatePicker.SelectedDate.HasValue)
        {
            _dailySummaryDate = DailySummaryDatePicker.SelectedDate.Value.Date;
            DailySummaryTodayButton.FontWeight = FontWeights.Normal;
            DailySummaryIndicator.Text = $"Showing: {_dailySummaryDate.Value:MMM d, yyyy}";
        }
    }

    private async void DailySummaryRefresh_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var result = await _dataService.GetDailySummaryAsync(_serverId, _dailySummaryDate);
            DailySummaryGrid.ItemsSource = result != null
                ? new List<DailySummaryRow> { result } : null;
            DailySummaryNoData.Visibility = result == null
                ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("DailySummary", $"Error refreshing: {ex.Message}");
        }
    }

    // ── Active Queries Slicer ──

    private async System.Threading.Tasks.Task LoadActiveQueriesSlicerAsync()
    {
        try
        {
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

            // For narrow time ranges (drill-downs), pad the query by ±1 hour
            // so hourly slicer buckets overlap the display range
            DateTime? queryFrom = fromDate, queryTo = toDate;
            if (fromDate.HasValue && toDate.HasValue && (toDate.Value - fromDate.Value).TotalHours < 2)
            {
                queryFrom = fromDate.Value.AddHours(-1);
                queryTo = toDate.Value.AddHours(1);
            }

            var data = await _dataService.GetActiveQuerySlicerDataAsync(_serverId, hoursBack, queryFrom, queryTo);
            _activeQueriesSlicerData = data;
            _activeQueriesSlicerMetric = "Sessions";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, queryFrom, queryTo);
            if (data.Count > 0)
                ActiveQueriesSlicer.LoadData(data, "Sessions", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadActiveQueriesSlicerAsync failed: {ex.Message}");
        }
    }

    private string _activeQueriesSlicerMetric = "Sessions";
    private List<Models.TimeSliceBucket>? _activeQueriesSlicerData;

    private void QuerySnapshotsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_activeQueriesSlicerData == null || _activeQueriesSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col))
        {
            // Fall back to binding path
            if (e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
                col = b.Path.Path;
        }
        var (metric, label) = col switch
        {
            "CpuTimeMs" => ("TotalCpu", "Total CPU (ms)"),
            "TotalElapsedTimeMs" => ("TotalElapsed", "Total Elapsed (ms)"),
            "Reads" => ("TotalReads", "Total Reads"),
            "LogicalReads" => ("TotalLogicalReads", "Total Logical Reads"),
            "Writes" => ("TotalWrites", "Total Writes"),
            _ => ("Sessions", "Sessions"),
        };

        if (metric == _activeQueriesSlicerMetric) return;
        _activeQueriesSlicerMetric = metric;

        foreach (var bucket in _activeQueriesSlicerData)
        {
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "TotalElapsed" => bucket.TotalElapsed,
                "TotalReads" => bucket.TotalReads,
                "TotalLogicalReads" => bucket.TotalLogicalReads,
                "TotalWrites" => bucket.TotalWrites,
                _ => bucket.SessionCount,
            };
        }

        ActiveQueriesSlicer.UpdateMetric(label);
    }

    private async void OnActiveQueriesSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            // Slicer sends UTC dates; GetTimeRange expects server time for fromDate/toDate
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);

            var snapshots = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromServer, toServer);
            _querySnapshotsFilterMgr!.UpdateData(snapshots);
            LiveSnapshotIndicator.Text = "";
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnActiveQueriesSlicerChanged failed: {ex.Message}");
        }
    }

    // ── Query Stats Slicer ──

    private string _queryStatsSlicerMetric = "TotalCpu";
    private List<Models.TimeSliceBucket>? _queryStatsSlicerData;

    private async System.Threading.Tasks.Task LoadQueryStatsSlicerAsync()
    {
        try
        {
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

            var data = await _dataService.GetQueryStatsSlicerDataAsync(_serverId, hoursBack, fromDate, toDate);
            _queryStatsSlicerData = data;
            _queryStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
            if (data.Count > 0)
                QueryStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadQueryStatsSlicerAsync failed: {ex.Message}");
        }
    }

    private async void OnQueryStatsSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);
            var queryStats = await _dataService.GetTopQueriesByCpuAsync(_serverId, 0, 50, fromServer, toServer, UtcOffsetMinutes);
            _queryStatsFilterMgr!.UpdateData(queryStats);
            await RefreshQueryStatsComparisonAsync(fromServer, toServer);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnQueryStatsSlicerChanged failed: {ex.Message}");
        }
    }

    private void QueryStatsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_queryStatsSlicerData == null || _queryStatsSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
            col = b.Path.Path;

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
                "TotalPhysReads" => bucket.TotalLogicalReads,
                _ => bucket.TotalCpu,
            };
        }

        QueryStatsSlicer.UpdateMetric(label);

        // Re-compute overlay with new metric if a row is selected
        if (QueryStatsGrid.SelectedItem != null)
            QueryStatsGrid_SelectionChanged(QueryStatsGrid, null!);
    }

    // ── Query Store Slicer ──

    private string _queryStoreSlicerMetric = "TotalCpu";
    private List<Models.TimeSliceBucket>? _queryStoreSlicerData;

    private async System.Threading.Tasks.Task LoadQueryStoreSlicerAsync()
    {
        try
        {
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

            var data = await _dataService.GetQueryStoreSlicerDataAsync(_serverId, hoursBack, fromDate, toDate);
            _queryStoreSlicerData = data;
            _queryStoreSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
            if (data.Count > 0)
                QueryStoreSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadQueryStoreSlicerAsync failed: {ex.Message}");
        }
    }

    private async void OnQueryStoreSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);
            var qsData = await _dataService.GetQueryStoreTopQueriesAsync(_serverId, 0, 50, fromServer, toServer);
            _queryStoreFilterMgr!.UpdateData(qsData);
            await RefreshQueryStoreComparisonAsync(fromServer, toServer);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnQueryStoreSlicerChanged failed: {ex.Message}");
        }
    }

    private void QueryStoreGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_queryStoreSlicerData == null || _queryStoreSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
            col = b.Path.Path;

        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuTimeMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalDurationMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgDurationMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "AvgLogicalReads" => ("TotalReads", "Avg Reads"),
            "AvgLogicalWrites" => ("TotalWrites", "Avg Writes"),
            "AvgPhysicalReads" => ("TotalReads", "Avg Physical Reads"),
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
                "Sessions" => bucket.SessionCount,
                _ => bucket.TotalCpu,
            };
        }

        QueryStoreSlicer.UpdateMetric(label);

        if (QueryStoreGrid.SelectedItem != null)
            QueryStoreGrid_SelectionChanged(QueryStoreGrid, null!);
    }

    // ── Procedure Stats Slicer ──

    private string _procStatsSlicerMetric = "TotalCpu";
    private List<Models.TimeSliceBucket>? _procStatsSlicerData;

    private async System.Threading.Tasks.Task LoadProcStatsSlicerAsync()
    {
        try
        {
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

            var data = await _dataService.GetProcStatsSlicerDataAsync(_serverId, hoursBack, fromDate, toDate);
            _procStatsSlicerData = data;
            _procStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
            if (data.Count > 0)
                ProcStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadProcStatsSlicerAsync failed: {ex.Message}");
        }
    }

    private async void OnProcStatsSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);
            var procStats = await _dataService.GetTopProceduresByCpuAsync(_serverId, 0, 50, fromServer, toServer, UtcOffsetMinutes);
            _procStatsFilterMgr!.UpdateData(procStats);
            await RefreshProcStatsComparisonAsync(fromServer, toServer);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnProcStatsSlicerChanged failed: {ex.Message}");
        }
    }

    private void ProcedureStatsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_procStatsSlicerData == null || _procStatsSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
            col = b.Path.Path;

        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalElapsedMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgElapsedMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "TotalLogicalReads" or "AvgReads" => ("TotalReads", "Total Reads"),
            "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
            "TotalPhysicalReads" => ("TotalReads", "Total Physical Reads"),
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
                _ => bucket.TotalCpu,
            };
        }

        ProcStatsSlicer.UpdateMetric(label);

        if (ProcedureStatsGrid.SelectedItem != null)
            ProcedureStatsGrid_SelectionChanged(ProcedureStatsGrid, null!);
    }

    private async void LiveSnapshot_Click(object sender, RoutedEventArgs e)
    {
        LiveSnapshotButton.IsEnabled = false;
        LiveSnapshotIndicator.Text = "Querying...";

        try
        {
            var connectionString = _server.GetConnectionString(_credentialService);
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
        _waitStatsHover?.Dispose();
        _perfmonHover?.Dispose();
        _cpuHover?.Dispose();
        _memoryHover?.Dispose();
        _tempDbHover?.Dispose();
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
    }

    private static void SetDefaultSortIfNone(DataGrid grid, string bindingPath, ListSortDirection direction)
    {
        if (grid.Items.SortDescriptions.Count > 0) return;

        grid.Items.SortDescriptions.Add(new SortDescription(bindingPath, direction));
        foreach (var column in grid.Columns)
        {
            if (column is DataGridBoundColumn bc &&
                bc.Binding is Binding b &&
                b.Path.Path == bindingPath)
            {
                column.SortDirection = direction;
                return;
            }
        }
    }
}
