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
using System.Threading.Tasks;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using System.Windows.Threading;
using Microsoft.Win32;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private readonly ServerConnection _server;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    public int ServerId => _serverId;
    private readonly CredentialService _credentialService;
    private readonly DispatcherTimer _refreshTimer;
    private readonly Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.Panels.LegendPanel?> _legendPanels = new();
    private List<SelectableItem> _waitTypeItems = new();
    private List<SelectableItem> _perfmonCounterItems = new();
    private Helpers.ChartHoverHelper? _waitStatsHover;
    private Helpers.ChartHoverHelper? _perfmonHover;
    private Helpers.ChartHoverHelper? _tempDbFileIoHover;
    private Helpers.ChartHoverHelper? _fileIoReadHover;
    private Helpers.ChartHoverHelper? _fileIoWriteHover;

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

    private static readonly HashSet<string> _defaultPerfmonCounters = new(StringComparer.OrdinalIgnoreCase)
    {
        "Batch Requests/sec",
        "Deadlocks/sec",
        "Query Optimizations/sec",
        "SQL Compilations/sec",
        "SQL Re-Compilations/sec"
    };

    private static readonly string[] SeriesColors = new[]
    {
        "#4FC3F7", "#E57373", "#81C784", "#FFD54F", "#BA68C8",
        "#FFB74D", "#4DD0E1", "#F06292", "#AED581", "#7986CB",
        "#FFF176", "#A1887F", "#FF7043", "#80DEEA", "#FFE082",
        "#CE93D8", "#EF9A9A", "#C5E1A5", "#FFCC80", "#B0BEC5"
    };

    public int UtcOffsetMinutes { get; }

    /// <summary>
    /// Raised after each data refresh with alert counts for tab badge display.
    /// </summary>
    public event Action<int, int, DateTime?>? AlertCountsChanged; /* blockingCount, deadlockCount, latestEventTimeUtc */
    public event Action<int>? ApplyTimeRangeRequested; /* selectedIndex */
    public event Func<Task>? ManualRefreshRequested;

    public ServerTab(ServerConnection server, DuckDbInitializer duckDb, CredentialService credentialService, int utcOffsetMinutes = 0)
    {
        InitializeComponent();

        _server = server;
        _dataService = new LocalDataService(duckDb);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(server.ServerName);
        _credentialService = credentialService;
        UtcOffsetMinutes = utcOffsetMinutes;
        ServerTimeHelper.UtcOffsetMinutes = utcOffsetMinutes;

        ServerNameText.Text = server.DisplayName;
        ConnectionStatusText.Text = server.ServerName;

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
        _refreshTimer.Tick += async (s, e) => await RefreshAllDataAsync();
        _refreshTimer.Start();

        /* Initialize time picker ComboBoxes */
        InitializeTimeComboBoxes();

        /* Initialize column filter managers */
        InitializeFilterManagers();

        /* Chart hover tooltips */
        _waitStatsHover = new Helpers.ChartHoverHelper(WaitStatsChart, "ms/sec");
        _perfmonHover = new Helpers.ChartHoverHelper(PerfmonChart, "");
        _tempDbFileIoHover = new Helpers.ChartHoverHelper(TempDbFileIoChart, "ms");
        _fileIoReadHover = new Helpers.ChartHoverHelper(FileIoReadChart, "ms");
        _fileIoWriteHover = new Helpers.ChartHoverHelper(FileIoWriteChart, "ms");

        /* Initial load is triggered by MainWindow.ConnectToServer calling RefreshData()
           after collectors finish - no Loaded handler needed */
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
        /* Convert server time to local time for display in UI */
        var localTime = ServerTimeHelper.ToLocalTime(serverTime);
        datePicker.SelectedDate = localTime.Date;
        hourCombo.SelectedIndex = localTime.Hour;
        minuteCombo.SelectedIndex = localTime.Minute / 15;
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
            await RefreshAllDataAsync();
        }
        finally
        {
            RefreshDataButton.IsEnabled = true;
        }
    }

    private async void TimeRangeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;

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
            await RefreshAllDataAsync();
        }
    }

    private async void CustomDateRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            await RefreshAllDataAsync();
        }
    }

    private async void CustomTimeCombo_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        /* Only refresh if we have valid dates selected */
        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            await RefreshAllDataAsync();
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
                    ApplyDarkThemeToCalendar(calendar);
                }
            }));
        }
    }

    private void ApplyDarkThemeToCalendar(System.Windows.Controls.Calendar calendar)
    {
        var darkBg = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#111217")!);
        var whiteFg = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#E4E6EB")!);
        var mutedFg = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#6B7280")!);

        calendar.Background = darkBg;
        calendar.Foreground = whiteFg;
        calendar.BorderBrush = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#2a2d35")!);

        ApplyDarkThemeRecursively(calendar, darkBg, whiteFg, mutedFg);
    }

    private void ApplyDarkThemeRecursively(DependencyObject parent, Brush darkBg, Brush whiteFg, Brush mutedFg)
    {
        for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);

            if (child is System.Windows.Controls.Primitives.CalendarItem calendarItem)
            {
                calendarItem.Background = darkBg;
                calendarItem.Foreground = whiteFg;
            }
            else if (child is System.Windows.Controls.Primitives.CalendarDayButton dayButton)
            {
                dayButton.Background = Brushes.Transparent;
                dayButton.Foreground = whiteFg;
            }
            else if (child is System.Windows.Controls.Primitives.CalendarButton calButton)
            {
                calButton.Background = Brushes.Transparent;
                calButton.Foreground = whiteFg;
            }
            else if (child is Button button)
            {
                button.Background = Brushes.Transparent;
                button.Foreground = whiteFg;
            }
            else if (child is TextBlock textBlock)
            {
                textBlock.Foreground = whiteFg;
            }
            else if (child is Border border)
            {
                if (border.Background is SolidColorBrush bg && bg.Color.R > 200 && bg.Color.G > 200 && bg.Color.B > 200)
                    border.Background = darkBg;
            }
            else if (child is Grid grid)
            {
                if (grid.Background is SolidColorBrush gridBg && gridBg.Color.R > 200 && gridBg.Color.G > 200 && gridBg.Color.B > 200)
                    grid.Background = darkBg;
            }

            ApplyDarkThemeRecursively(child, darkBg, whiteFg, mutedFg);
        }
    }

    /// <summary>
    /// Returns true if the custom date range is selected and both dates are set.
    /// </summary>
    private bool IsCustomRange => TimeRangeCombo.SelectedIndex == 5
        && FromDatePicker?.SelectedDate != null
        && ToDatePicker?.SelectedDate != null;

    /// <summary>
    /// Public entry point to trigger a data refresh from outside.
    /// </summary>
    public async void RefreshData()
    {
        await RefreshAllDataAsync();
    }

    private async System.Threading.Tasks.Task RefreshAllDataAsync()
    {
        var hoursBack = GetHoursBack();

        /* Get custom date range if selected, converting local picker dates/times to server time */
        DateTime? fromDate = null;
        DateTime? toDate = null;
        if (IsCustomRange)
        {
            var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
            var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
            if (fromLocal.HasValue && toLocal.HasValue)
            {
                fromDate = ServerTimeHelper.LocalToServerTime(fromLocal.Value);
                toDate = ServerTimeHelper.LocalToServerTime(toLocal.Value);
            }
        }

        try
        {
            var loadSw = Stopwatch.StartNew();

            /* Load all tabs in parallel */
            var snapshotsTask = _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate);
            var cpuTask = _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate);
            var memoryTask = _dataService.GetLatestMemoryStatsAsync(_serverId);
            var memoryTrendTask = _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var queryStatsTask = _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate);
            var procStatsTask = _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate);
            var fileIoTask = _dataService.GetLatestFileIoStatsAsync(_serverId);
            var fileIoTrendTask = _dataService.GetFileIoLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var tempDbTask = _dataService.GetTempDbTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var tempDbFileIoTask = _dataService.GetTempDbFileIoTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var deadlockTask = _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate);
            var blockedProcessTask = _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate);
            var waitTypesTask = _dataService.GetDistinctWaitTypesAsync(_serverId, hoursBack, fromDate, toDate);
            var perfmonCountersTask = _dataService.GetDistinctPerfmonCountersAsync(_serverId, hoursBack, fromDate, toDate);
            var queryStoreTask = _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate);
            var memoryGrantTrendTask = _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var serverConfigTask = SafeQueryAsync(() => _dataService.GetLatestServerConfigAsync(_serverId));
            var databaseConfigTask = SafeQueryAsync(() => _dataService.GetLatestDatabaseConfigAsync(_serverId));
            var databaseScopedConfigTask = SafeQueryAsync(() => _dataService.GetLatestDatabaseScopedConfigAsync(_serverId));
            var traceFlagsTask = SafeQueryAsync(() => _dataService.GetLatestTraceFlagsAsync(_serverId));
            var runningJobsTask = SafeQueryAsync(() => _dataService.GetRunningJobsAsync(_serverId));
            var collectionHealthTask = SafeQueryAsync(() => _dataService.GetCollectionHealthAsync(_serverId));
            var collectionLogTask = SafeQueryAsync(() => _dataService.GetRecentCollectionLogAsync(_serverId, hoursBack));
            /* Core data tasks */
            await System.Threading.Tasks.Task.WhenAll(
                snapshotsTask, cpuTask, memoryTask, memoryTrendTask,
                queryStatsTask, procStatsTask, fileIoTask, fileIoTrendTask, tempDbTask, tempDbFileIoTask,
                deadlockTask, blockedProcessTask, waitTypesTask, perfmonCountersTask,
                queryStoreTask, memoryGrantTrendTask,
                serverConfigTask, databaseConfigTask, databaseScopedConfigTask, traceFlagsTask,
                runningJobsTask, collectionHealthTask, collectionLogTask);

            /* Trend chart tasks - run separately so failures don't kill the whole refresh */
            var blockingTrendTask = SafeQueryAsync(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var deadlockTrendTask = SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var queryDurationTrendTask = SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var procDurationTrendTask = SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var queryStoreDurationTrendTask = SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var executionCountTrendTask = SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate));

            await System.Threading.Tasks.Task.WhenAll(
                blockingTrendTask, deadlockTrendTask,
                queryDurationTrendTask, procDurationTrendTask, queryStoreDurationTrendTask, executionCountTrendTask);

            loadSw.Stop();

            /* Log data counts and timing for diagnostics */
            AppLogger.DataDiag("ServerTab", $"[{_server.DisplayName}] serverId={_serverId} hoursBack={hoursBack} dataLoad={loadSw.ElapsedMilliseconds}ms");
            AppLogger.DataDiag("ServerTab", $"  Snapshots: {snapshotsTask.Result.Count}, CPU: {cpuTask.Result.Count}");
            AppLogger.DataDiag("ServerTab", $"  Memory: {(memoryTask.Result != null ? "1" : "null")}, MemoryTrend: {memoryTrendTask.Result.Count}");
            AppLogger.DataDiag("ServerTab", $"  QueryStats: {queryStatsTask.Result.Count}, ProcStats: {procStatsTask.Result.Count}");
            AppLogger.DataDiag("ServerTab", $"  FileIo: {fileIoTask.Result.Count}, FileIoTrend: {fileIoTrendTask.Result.Count}");
            AppLogger.DataDiag("ServerTab", $"  TempDb: {tempDbTask.Result.Count}, BlockedProcessReports: {blockedProcessTask.Result.Count}, Deadlocks: {deadlockTask.Result.Count}");
            AppLogger.DataDiag("ServerTab", $"  WaitTypes: {waitTypesTask.Result.Count}, PerfmonCounters: {perfmonCountersTask.Result.Count}, QueryStore: {queryStoreTask.Result.Count}");

            /* Update grids (via filter managers to preserve active filters) */
            _querySnapshotsFilterMgr!.UpdateData(snapshotsTask.Result);
            _queryStatsFilterMgr!.UpdateData(queryStatsTask.Result);
            _procStatsFilterMgr!.UpdateData(procStatsTask.Result);
            _blockedProcessFilterMgr!.UpdateData(blockedProcessTask.Result);
            _deadlockFilterMgr!.UpdateData(DeadlockProcessDetail.ParseFromRows(deadlockTask.Result));
            _queryStoreFilterMgr!.UpdateData(queryStoreTask.Result);
            _serverConfigFilterMgr!.UpdateData(serverConfigTask.Result);
            _databaseConfigFilterMgr!.UpdateData(databaseConfigTask.Result);
            _dbScopedConfigFilterMgr!.UpdateData(databaseScopedConfigTask.Result);
            _traceFlagsFilterMgr!.UpdateData(traceFlagsTask.Result);
            _runningJobsFilterMgr!.UpdateData(runningJobsTask.Result);
            _collectionHealthFilterMgr!.UpdateData(collectionHealthTask.Result);
            _collectionLogFilterMgr!.UpdateData(collectionLogTask.Result);
            UpdateCollectorDurationChart(collectionLogTask.Result);

            /* Update memory summary */
            UpdateMemorySummary(memoryTask.Result);

            /* Update charts */
            UpdateCpuChart(cpuTask.Result);
            UpdateMemoryChart(memoryTrendTask.Result, memoryGrantTrendTask.Result);
            UpdateTempDbChart(tempDbTask.Result);
            UpdateTempDbFileIoChart(tempDbFileIoTask.Result);
            UpdateFileIoCharts(fileIoTrendTask.Result);
            UpdateBlockingTrendChart(blockingTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateDeadlockTrendChart(deadlockTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateQueryDurationTrendChart(queryDurationTrendTask.Result);
            UpdateProcDurationTrendChart(procDurationTrendTask.Result);
            UpdateQueryStoreDurationTrendChart(queryStoreDurationTrendTask.Result);
            UpdateExecutionCountTrendChart(executionCountTrendTask.Result);

            /* Populate pickers (preserve selections) */
            PopulateWaitTypePicker(waitTypesTask.Result);
            PopulatePerfmonPicker(perfmonCountersTask.Result);

            /* Update picker-driven charts */
            await UpdateWaitStatsChartFromPickerAsync();
            await UpdatePerfmonChartFromPickerAsync();

            ConnectionStatusText.Text = $"{_server.ServerName} - Last refresh: {DateTime.Now:HH:mm:ss}";

            /* Notify parent of alert counts for tab badge.
               Include the latest event timestamp so acknowledgement is only
               cleared when genuinely new events arrive, not when the time range changes. */
            var blockingCount = blockedProcessTask.Result.Count;
            var deadlockCount = deadlockTask.Result.Count;
            DateTime? latestEventTime = null;
            if (blockingCount > 0 || deadlockCount > 0)
            {
                var latestBlocking = blockedProcessTask.Result.Max(r => (DateTime?)r.CollectionTime);
                var latestDeadlock = deadlockTask.Result.Max(r => (DateTime?)r.CollectionTime);
                latestEventTime = latestBlocking > latestDeadlock ? latestBlocking : latestDeadlock;
            }
            AlertCountsChanged?.Invoke(blockingCount, deadlockCount, latestEventTime);
        }
        catch (Exception ex)
        {
            ConnectionStatusText.Text = $"Error: {ex.Message}";
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshAllDataAsync failed: {ex}");
        }
    }

    /// <summary>
    /// Wraps a query in a try/catch so it returns an empty list on failure instead of faulting.
    /// </summary>
    private static async Task<List<T>> SafeQueryAsync<T>(Func<Task<List<T>>> query)
    {
        try
        {
            return await query();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"Trend query failed: {ex.Message}");
            return new List<T>();
        }
    }

    private void UpdateMemorySummary(MemoryStatsRow? stats)
    {
        if (stats == null)
        {
            PhysicalMemoryText.Text = "--";
            TotalServerMemoryText.Text = "--";
            TargetServerMemoryText.Text = "--";
            BufferPoolText.Text = "--";
            PlanCacheText.Text = "--";
            MemoryStateText.Text = "--";
            return;
        }

        PhysicalMemoryText.Text = FormatMb(stats.TotalPhysicalMemoryMb);
        TotalServerMemoryText.Text = FormatMb(stats.TotalServerMemoryMb);
        TargetServerMemoryText.Text = FormatMb(stats.TargetServerMemoryMb);
        BufferPoolText.Text = FormatMb(stats.BufferPoolMb);
        PlanCacheText.Text = FormatMb(stats.PlanCacheMb);
        MemoryStateText.Text = stats.SystemMemoryState;
    }

    private static string FormatMb(double mb)
    {
        return mb >= 1024 ? $"{mb / 1024:F1} GB" : $"{mb:F0} MB";
    }


    private void UpdateCpuChart(List<CpuUtilizationRow> data)
    {
        ClearChart(CpuChart);
        ApplyDarkTheme(CpuChart);

        if (data.Count == 0) { CpuChart.Refresh(); return; }

        var times = data.Select(d => d.SampleTime.ToOADate()).ToArray();
        var sqlCpu = data.Select(d => (double)d.SqlServerCpu).ToArray();
        var otherCpu = data.Select(d => (double)d.OtherProcessCpu).ToArray();

        var sqlPlot = CpuChart.Plot.Add.Scatter(times, sqlCpu);
        sqlPlot.LegendText = "SQL Server";
        sqlPlot.Color = ScottPlot.Color.FromHex("#4FC3F7");

        var otherPlot = CpuChart.Plot.Add.Scatter(times, otherCpu);
        otherPlot.LegendText = "Other";
        otherPlot.Color = ScottPlot.Color.FromHex("#E57373");

        CpuChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(CpuChart);
        CpuChart.Plot.YLabel("CPU %");
        CpuChart.Plot.Axes.SetLimitsY(0, 105);

        ShowChartLegend(CpuChart);
        CpuChart.Refresh();
    }

    private void UpdateMemoryChart(List<MemoryTrendPoint> data, List<MemoryTrendPoint> grantData)
    {
        ClearChart(MemoryChart);
        ApplyDarkTheme(MemoryChart);

        if (data.Count == 0) { MemoryChart.Refresh(); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var totalMem = data.Select(d => d.TotalServerMemoryMb / 1024.0).ToArray();
        var targetMem = data.Select(d => d.TargetServerMemoryMb / 1024.0).ToArray();
        var bufferPool = data.Select(d => d.BufferPoolMb / 1024.0).ToArray();

        var totalPlot = MemoryChart.Plot.Add.Scatter(times, totalMem);
        totalPlot.LegendText = "Total Server Memory";
        totalPlot.Color = ScottPlot.Color.FromHex("#4FC3F7");

        var targetPlot = MemoryChart.Plot.Add.Scatter(times, targetMem);
        targetPlot.LegendText = "Target Memory";
        targetPlot.Color = ScottPlot.Colors.Gray;
        targetPlot.LineStyle.Pattern = LinePattern.Dashed;

        var bpPlot = MemoryChart.Plot.Add.Scatter(times, bufferPool);
        bpPlot.LegendText = "Buffer Pool";
        bpPlot.Color = ScottPlot.Color.FromHex("#81C784");

        /* Memory grants trend line — show zero line when no grant data */
        double[] grantTimes, grantMb;
        if (grantData.Count > 0)
        {
            grantTimes = grantData.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            grantMb = grantData.Select(d => d.TotalGrantedMb / 1024.0).ToArray();
        }
        else
        {
            grantTimes = new[] { times.First(), times.Last() };
            grantMb = new[] { 0.0, 0.0 };
        }

        var grantPlot = MemoryChart.Plot.Add.Scatter(grantTimes, grantMb);
        grantPlot.LegendText = "Memory Grants";
        grantPlot.Color = ScottPlot.Color.FromHex("#FFB74D");

        MemoryChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(MemoryChart);
        MemoryChart.Plot.YLabel("Memory (GB)");

        var maxVal = totalMem.Max();
        SetChartYLimitsWithLegendPadding(MemoryChart, 0, maxVal);

        ShowChartLegend(MemoryChart);
        MemoryChart.Refresh();
    }

    private void UpdateTempDbChart(List<TempDbRow> data)
    {
        ClearChart(TempDbChart);
        ApplyDarkTheme(TempDbChart);

        if (data.Count == 0) { TempDbChart.Refresh(); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var userObj = data.Select(d => d.UserObjectReservedMb).ToArray();
        var internalObj = data.Select(d => d.InternalObjectReservedMb).ToArray();
        var versionStore = data.Select(d => d.VersionStoreReservedMb).ToArray();

        var userPlot = TempDbChart.Plot.Add.Scatter(times, userObj);
        userPlot.LegendText = "User Objects";
        userPlot.Color = ScottPlot.Color.FromHex("#4FC3F7");

        var internalPlot = TempDbChart.Plot.Add.Scatter(times, internalObj);
        internalPlot.LegendText = "Internal Objects";
        internalPlot.Color = ScottPlot.Color.FromHex("#FFD54F");

        var vsPlot = TempDbChart.Plot.Add.Scatter(times, versionStore);
        vsPlot.LegendText = "Version Store";
        vsPlot.Color = ScottPlot.Color.FromHex("#81C784");

        TempDbChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(TempDbChart);
        TempDbChart.Plot.YLabel("MB");

        var maxVal = new[] { userObj.Max(), internalObj.Max(), versionStore.Max() }.Max();
        SetChartYLimitsWithLegendPadding(TempDbChart, 0, maxVal);

        ShowChartLegend(TempDbChart);
        TempDbChart.Refresh();
    }

    private void UpdateTempDbFileIoChart(List<FileIoTrendPoint> data)
    {
        ClearChart(TempDbFileIoChart);
        _tempDbFileIoHover?.Clear();
        ApplyDarkTheme(TempDbFileIoChart);

        if (data.Count == 0) { TempDbFileIoChart.Refresh(); return; }

        var files = data
            .GroupBy(d => d.DatabaseName)
            .OrderByDescending(g => g.Sum(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs))
            .Take(12)
            .ToList();

        double maxLatency = 0;
        int colorIdx = 0;

        foreach (var fileGroup in files)
        {
            var points = fileGroup.OrderBy(d => d.CollectionTime).ToList();
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var latency = points.Select(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (latency.Length > 0)
            {
                var plot = TempDbFileIoChart.Plot.Add.Scatter(times, latency);
                plot.LegendText = fileGroup.Key;
                plot.Color = color;
                _tempDbFileIoHover?.Add(plot, fileGroup.Key);
                maxLatency = Math.Max(maxLatency, latency.Max());
            }
        }

        TempDbFileIoChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(TempDbFileIoChart);
        TempDbFileIoChart.Plot.YLabel("TempDB File I/O Latency (ms)");
        SetChartYLimitsWithLegendPadding(TempDbFileIoChart, 0, maxLatency > 0 ? maxLatency : 10);
        ShowChartLegend(TempDbFileIoChart);
        TempDbFileIoChart.Refresh();
    }

    private void UpdateFileIoCharts(List<FileIoTrendPoint> data)
    {
        ClearChart(FileIoReadChart);
        ClearChart(FileIoWriteChart);
        _fileIoReadHover?.Clear();
        _fileIoWriteHover?.Clear();
        ApplyDarkTheme(FileIoReadChart);
        ApplyDarkTheme(FileIoWriteChart);

        if (data.Count == 0) { FileIoReadChart.Refresh(); FileIoWriteChart.Refresh(); return; }

        /* Group by database, limit to top 12 by total stall */
        var databases = data
            .GroupBy(d => d.DatabaseName)
            .OrderByDescending(g => g.Sum(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs))
            .Take(12)
            .ToList();

        double readMax = 0, writeMax = 0;
        int colorIdx = 0;

        foreach (var dbGroup in databases)
        {
            var points = dbGroup.OrderBy(d => d.CollectionTime).ToList();
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var readLatency = points.Select(d => d.AvgReadLatencyMs).ToArray();
            var writeLatency = points.Select(d => d.AvgWriteLatencyMs).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (readLatency.Length > 0)
            {
                var readPlot = FileIoReadChart.Plot.Add.Scatter(times, readLatency);
                readPlot.LegendText = dbGroup.Key;
                readPlot.Color = color;
                _fileIoReadHover?.Add(readPlot, dbGroup.Key);
                readMax = Math.Max(readMax, readLatency.Max());
            }

            if (writeLatency.Length > 0)
            {
                var writePlot = FileIoWriteChart.Plot.Add.Scatter(times, writeLatency);
                writePlot.LegendText = dbGroup.Key;
                writePlot.Color = color;
                _fileIoWriteHover?.Add(writePlot, dbGroup.Key);
                writeMax = Math.Max(writeMax, writeLatency.Max());
            }
        }

        FileIoReadChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(FileIoReadChart);
        FileIoReadChart.Plot.YLabel("Read Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoReadChart, 0, readMax > 0 ? readMax : 10);
        ShowChartLegend(FileIoReadChart);
        FileIoReadChart.Refresh();

        FileIoWriteChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(FileIoWriteChart);
        FileIoWriteChart.Plot.YLabel("Write Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoWriteChart, 0, writeMax > 0 ? writeMax : 10);
        ShowChartLegend(FileIoWriteChart);
        FileIoWriteChart.Refresh();
    }

    /* ========== Blocking/Deadlock Trend Charts ========== */

    private void UpdateBlockingTrendChart(List<TrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(BlockingTrendChart);
        ApplyDarkTheme(BlockingTrendChart);

        /* Calculate X-axis range based on selected time window */
        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        if (data.Count == 0)
        {
            /* Show empty chart with correct time range */
            BlockingTrendChart.Plot.Axes.DateTimeTicksBottom();
            BlockingTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            BlockingTrendChart.Plot.Axes.SetLimitsY(0, 1);
            ReapplyAxisColors(BlockingTrendChart);
            BlockingTrendChart.Plot.YLabel("Blocking Incidents");
            BlockingTrendChart.Refresh();
            return;
        }

        /* Build arrays with zero baseline between data points for spike effect */
        var expandedTimes = new List<double>();
        var expandedCounts = new List<double>();

        /* Add zero at start */
        expandedTimes.Add(rangeStart.ToOADate());
        expandedCounts.Add(0);

        foreach (var point in data.OrderBy(d => d.Time))
        {
            var time = point.Time.AddMinutes(UtcOffsetMinutes).ToOADate();
            /* Go to zero just before the spike */
            expandedTimes.Add(time - 0.0001);
            expandedCounts.Add(0);
            /* Spike up */
            expandedTimes.Add(time);
            expandedCounts.Add(point.Count);
            /* Back to zero just after */
            expandedTimes.Add(time + 0.0001);
            expandedCounts.Add(0);
        }

        /* Add zero at end */
        expandedTimes.Add(rangeEnd.ToOADate());
        expandedCounts.Add(0);

        var plot = BlockingTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray());
        plot.LegendText = "Blocking Incidents";
        plot.Color = ScottPlot.Color.FromHex("#E57373");
        plot.MarkerSize = 0; /* No markers, just lines */

        BlockingTrendChart.Plot.Axes.DateTimeTicksBottom();
        BlockingTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(BlockingTrendChart);
        BlockingTrendChart.Plot.YLabel("Blocking Incidents");
        SetChartYLimitsWithLegendPadding(BlockingTrendChart, 0, data.Max(d => d.Count));
        ShowChartLegend(BlockingTrendChart);
        BlockingTrendChart.Refresh();
    }

    private void UpdateDeadlockTrendChart(List<TrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(DeadlockTrendChart);
        ApplyDarkTheme(DeadlockTrendChart);

        /* Calculate X-axis range based on selected time window */
        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        if (data.Count == 0)
        {
            /* Show empty chart with correct time range */
            DeadlockTrendChart.Plot.Axes.DateTimeTicksBottom();
            DeadlockTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            DeadlockTrendChart.Plot.Axes.SetLimitsY(0, 1);
            ReapplyAxisColors(DeadlockTrendChart);
            DeadlockTrendChart.Plot.YLabel("Deadlocks");
            DeadlockTrendChart.Refresh();
            return;
        }

        /* Build arrays with zero baseline between data points for spike effect */
        var expandedTimes = new List<double>();
        var expandedCounts = new List<double>();

        /* Add zero at start */
        expandedTimes.Add(rangeStart.ToOADate());
        expandedCounts.Add(0);

        foreach (var point in data.OrderBy(d => d.Time))
        {
            var time = point.Time.AddMinutes(UtcOffsetMinutes).ToOADate();
            /* Go to zero just before the spike */
            expandedTimes.Add(time - 0.0001);
            expandedCounts.Add(0);
            /* Spike up */
            expandedTimes.Add(time);
            expandedCounts.Add(point.Count);
            /* Back to zero just after */
            expandedTimes.Add(time + 0.0001);
            expandedCounts.Add(0);
        }

        /* Add zero at end */
        expandedTimes.Add(rangeEnd.ToOADate());
        expandedCounts.Add(0);

        var plot = DeadlockTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray());
        plot.LegendText = "Deadlocks";
        plot.Color = ScottPlot.Color.FromHex("#FFB74D");
        plot.MarkerSize = 0; /* No markers, just lines */

        DeadlockTrendChart.Plot.Axes.DateTimeTicksBottom();
        DeadlockTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(DeadlockTrendChart);
        DeadlockTrendChart.Plot.YLabel("Deadlocks");
        SetChartYLimitsWithLegendPadding(DeadlockTrendChart, 0, data.Max(d => d.Count));
        ShowChartLegend(DeadlockTrendChart);
        DeadlockTrendChart.Refresh();
    }

    /* ========== Performance Trend Charts ========== */

    private void UpdateQueryDurationTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(QueryDurationTrendChart);
        ApplyDarkTheme(QueryDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryDurationTrendChart, "Query Duration", "Duration (ms/sec)"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var plot = QueryDurationTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Query Duration";
        plot.Color = ScottPlot.Color.FromHex("#4FC3F7");

        QueryDurationTrendChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(QueryDurationTrendChart);
        QueryDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryDurationTrendChart);
        QueryDurationTrendChart.Refresh();
    }

    private void UpdateProcDurationTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(ProcDurationTrendChart);
        ApplyDarkTheme(ProcDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ProcDurationTrendChart, "Procedure Duration", "Duration (ms/sec)"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var plot = ProcDurationTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Procedure Duration";
        plot.Color = ScottPlot.Color.FromHex("#81C784");

        ProcDurationTrendChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(ProcDurationTrendChart);
        ProcDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(ProcDurationTrendChart, 0, values.Max());
        ShowChartLegend(ProcDurationTrendChart);
        ProcDurationTrendChart.Refresh();
    }

    private void UpdateQueryStoreDurationTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(QueryStoreDurationTrendChart);
        ApplyDarkTheme(QueryStoreDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryStoreDurationTrendChart, "Query Store Duration", "Duration (ms/sec)"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var plot = QueryStoreDurationTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Query Store Duration";
        plot.Color = ScottPlot.Color.FromHex("#FFB74D");

        QueryStoreDurationTrendChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryStoreDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Refresh();
    }

    private void UpdateExecutionCountTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(ExecutionCountTrendChart);
        ApplyDarkTheme(ExecutionCountTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ExecutionCountTrendChart, "Executions", "Executions/sec"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var plot = ExecutionCountTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Executions";
        plot.Color = ScottPlot.Color.FromHex("#BA68C8");

        ExecutionCountTrendChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Plot.YLabel("Executions/sec");
        SetChartYLimitsWithLegendPadding(ExecutionCountTrendChart, 0, values.Max());
        ShowChartLegend(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Refresh();
    }

    /* ========== Wait Stats Picker ========== */

    private static readonly string[] PoisonWaits = { "THREADPOOL", "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE" };
    private static readonly string[] UsualSuspectWaits = { "SOS_SCHEDULER_YIELD", "CXPACKET", "CXCONSUMER", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "WRITELOG" };
    private static readonly string[] UsualSuspectPrefixes = { "PAGELATCH_" };

    private static HashSet<string> GetDefaultWaitTypes(List<string> availableWaitTypes)
    {
        var defaults = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var w in PoisonWaits)
            if (availableWaitTypes.Contains(w)) defaults.Add(w);
        foreach (var w in UsualSuspectWaits)
            if (availableWaitTypes.Contains(w)) defaults.Add(w);
        foreach (var prefix in UsualSuspectPrefixes)
            foreach (var w in availableWaitTypes)
                if (w.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    defaults.Add(w);
        int added = 0;
        foreach (var w in availableWaitTypes)
        {
            if (defaults.Count >= 20) break;
            if (added >= 10) break;
            if (defaults.Add(w)) { added++; }
        }
        return defaults;
    }

    private bool _isUpdatingWaitTypeSelection;

    private void PopulateWaitTypePicker(List<string> waitTypes)
    {
        var previouslySelected = new HashSet<string>(_waitTypeItems.Where(i => i.IsSelected).Select(i => i.DisplayName));
        var topWaits = previouslySelected.Count == 0 ? GetDefaultWaitTypes(waitTypes) : null;
        _waitTypeItems = waitTypes.Select(w => new SelectableItem
        {
            DisplayName = w,
            IsSelected = previouslySelected.Contains(w) || (topWaits != null && topWaits.Contains(w))
        }).ToList();
        /* Sort checked items to top, then preserve original order (by total wait time desc) */
        RefreshWaitTypeListOrder();
    }

    private void RefreshWaitTypeListOrder()
    {
        if (_waitTypeItems == null) return;
        _waitTypeItems = _waitTypeItems
            .OrderByDescending(x => x.IsSelected)
            .ThenBy(x => x.DisplayName)
            .ToList();
        ApplyWaitTypeFilter();
        UpdateWaitTypeCount();
    }

    private void UpdateWaitTypeCount()
    {
        if (_waitTypeItems == null || WaitTypeCountText == null) return;
        int count = _waitTypeItems.Count(x => x.IsSelected);
        WaitTypeCountText.Text = $"{count} / 20 selected";
        WaitTypeCountText.Foreground = count >= 20
            ? new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#E57373")!)
            : (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush");
    }

    private void ApplyWaitTypeFilter()
    {
        var search = WaitTypeSearchBox?.Text?.Trim() ?? "";
        WaitTypesList.ItemsSource = null;
        if (string.IsNullOrEmpty(search))
            WaitTypesList.ItemsSource = _waitTypeItems;
        else
            WaitTypesList.ItemsSource = _waitTypeItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    private void WaitTypeSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyWaitTypeFilter();

    private void WaitTypeSelectAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingWaitTypeSelection = true;
        var topWaits = GetDefaultWaitTypes(_waitTypeItems.Select(x => x.DisplayName).ToList());
        foreach (var item in _waitTypeItems)
        {
            item.IsSelected = topWaits.Contains(item.DisplayName);
        }
        _isUpdatingWaitTypeSelection = false;
        RefreshWaitTypeListOrder();
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private void WaitTypeClearAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingWaitTypeSelection = true;
        var visible = (WaitTypesList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _waitTypeItems;
        foreach (var item in visible) item.IsSelected = false;
        _isUpdatingWaitTypeSelection = false;
        RefreshWaitTypeListOrder();
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private void WaitType_CheckChanged(object sender, RoutedEventArgs e)
    {
        if (_isUpdatingWaitTypeSelection) return;
        RefreshWaitTypeListOrder();
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private async System.Threading.Tasks.Task UpdateWaitStatsChartFromPickerAsync()
    {
        try
        {
            var selected = _waitTypeItems.Where(i => i.IsSelected).Take(20).ToList();

            ClearChart(WaitStatsChart);
            ApplyDarkTheme(WaitStatsChart);
            _waitStatsHover?.Clear();

            if (selected.Count == 0) { WaitStatsChart.Refresh(); return; }

            var hoursBack = GetHoursBack();
            DateTime? fromDate = null;
            DateTime? toDate = null;
            if (IsCustomRange)
            {
                var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromLocal.HasValue && toLocal.HasValue)
                {
                    fromDate = ServerTimeHelper.LocalToServerTime(fromLocal.Value);
                    toDate = ServerTimeHelper.LocalToServerTime(toLocal.Value);
                }
            }
            double globalMax = 0;

            for (int i = 0; i < selected.Count; i++)
            {
                var trend = await _dataService.GetWaitStatsTrendAsync(_serverId, selected[i].DisplayName, hoursBack, fromDate, toDate);
                if (trend.Count == 0) continue;

                var times = trend.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
                var waitTime = trend.Select(t => t.WaitTimeMsPerSecond).ToArray();

                var plot = WaitStatsChart.Plot.Add.Scatter(times, waitTime);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                _waitStatsHover?.Add(plot, selected[i].DisplayName);

                if (waitTime.Length > 0) globalMax = Math.Max(globalMax, waitTime.Max());
            }

            WaitStatsChart.Plot.Axes.DateTimeTicksBottom();
            DateTime rangeStart, rangeEnd;
            if (IsCustomRange && fromDate.HasValue && toDate.HasValue)
            {
                rangeStart = fromDate.Value;
                rangeEnd = toDate.Value;
            }
            else
            {
                rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
                rangeStart = rangeEnd.AddHours(-hoursBack);
            }
            WaitStatsChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(WaitStatsChart);
            WaitStatsChart.Plot.YLabel("Wait Time (ms/sec)");
            SetChartYLimitsWithLegendPadding(WaitStatsChart, 0, globalMax > 0 ? globalMax : 100);
            ShowChartLegend(WaitStatsChart);
            WaitStatsChart.Refresh();
        }
        catch
        {
            /* Ignore chart update errors */
        }
    }

    /* ========== Perfmon Picker ========== */

    private bool _isUpdatingPerfmonSelection;

    private void PopulatePerfmonPicker(List<string> counters)
    {
        var previouslySelected = new HashSet<string>(_perfmonCounterItems.Where(i => i.IsSelected).Select(i => i.DisplayName));
        _perfmonCounterItems = counters.Select(c => new SelectableItem
        {
            DisplayName = c,
            IsSelected = previouslySelected.Contains(c)
                || (previouslySelected.Count == 0 && _defaultPerfmonCounters.Contains(c))
        }).ToList();
        RefreshPerfmonListOrder();
    }

    private void RefreshPerfmonListOrder()
    {
        if (_perfmonCounterItems == null) return;
        _perfmonCounterItems = _perfmonCounterItems
            .OrderByDescending(x => x.IsSelected)
            .ThenBy(x => _perfmonCounterItems.IndexOf(x))
            .ToList();
        ApplyPerfmonFilter();
    }

    private void ApplyPerfmonFilter()
    {
        var search = PerfmonSearchBox?.Text?.Trim() ?? "";
        PerfmonCountersList.ItemsSource = null;
        if (string.IsNullOrEmpty(search))
            PerfmonCountersList.ItemsSource = _perfmonCounterItems;
        else
            PerfmonCountersList.ItemsSource = _perfmonCounterItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    private void PerfmonSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyPerfmonFilter();

    private void PerfmonSelectAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingPerfmonSelection = true;
        var visible = (PerfmonCountersList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _perfmonCounterItems;
        int count = visible.Count(i => i.IsSelected);
        foreach (var item in visible)
        {
            if (!item.IsSelected && count < 12)
            {
                item.IsSelected = true;
                count++;
            }
        }
        _isUpdatingPerfmonSelection = false;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private void PerfmonClearAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingPerfmonSelection = true;
        var visible = (PerfmonCountersList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _perfmonCounterItems;
        foreach (var item in visible) item.IsSelected = false;
        _isUpdatingPerfmonSelection = false;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private void PerfmonCounter_CheckChanged(object sender, RoutedEventArgs e)
    {
        if (_isUpdatingPerfmonSelection) return;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private async System.Threading.Tasks.Task UpdatePerfmonChartFromPickerAsync()
    {
        try
        {
            var selected = _perfmonCounterItems.Where(i => i.IsSelected).Take(12).ToList();

            ClearChart(PerfmonChart);
            _perfmonHover?.Clear();
            ApplyDarkTheme(PerfmonChart);

            if (selected.Count == 0) { PerfmonChart.Refresh(); return; }

            var hoursBack = GetHoursBack();
            DateTime? fromDate = null;
            DateTime? toDate = null;
            if (IsCustomRange)
            {
                var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromLocal.HasValue && toLocal.HasValue)
                {
                    fromDate = ServerTimeHelper.LocalToServerTime(fromLocal.Value);
                    toDate = ServerTimeHelper.LocalToServerTime(toLocal.Value);
                }
            }
            double globalMax = 0;

            for (int i = 0; i < selected.Count; i++)
            {
                var trend = await _dataService.GetPerfmonTrendAsync(_serverId, selected[i].DisplayName, hoursBack, fromDate, toDate);
                if (trend.Count == 0) continue;

                var times = trend.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
                var values = trend.Select(t => (double)t.DeltaValue).ToArray();

                var plot = PerfmonChart.Plot.Add.Scatter(times, values);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                _perfmonHover?.Add(plot, selected[i].DisplayName);

                if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
            }

            PerfmonChart.Plot.Axes.DateTimeTicksBottom();
            DateTime rangeStart, rangeEnd;
            if (IsCustomRange && fromDate.HasValue && toDate.HasValue)
            {
                rangeStart = fromDate.Value;
                rangeEnd = toDate.Value;
            }
            else
            {
                rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
                rangeStart = rangeEnd.AddHours(-hoursBack);
            }
            PerfmonChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(PerfmonChart);
            PerfmonChart.Plot.YLabel("Value");
            SetChartYLimitsWithLegendPadding(PerfmonChart, 0, globalMax > 0 ? globalMax : 100);
            ShowChartLegend(PerfmonChart);
            PerfmonChart.Refresh();
        }
        catch
        {
            /* Ignore chart update errors */
        }
    }

    /// <summary>
    /// Clears a chart and removes any existing legend panel to prevent duplication.
    /// </summary>
    private void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
        {
            chart.Plot.Axes.Remove(existingPanel);
            _legendPanels[chart] = null;
        }

        /* Reset fully — Plot.Clear() leaves stale DateTime axes behind,
           and DateTimeTicksBottom() replaces the axis object entirely.
           Resetting the plot object avoids tick generator type mismatches. */
        chart.Reset();
        chart.Plot.Clear();
    }

    /// <summary>
    /// Sets up an empty chart with dark theme, Y-axis label, legend, and "No Data" annotation.
    /// Matches Full Dashboard behavior for consistent UX.
    /// </summary>
    private void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
    {
        ReapplyAxisColors(chart);

        /* Add invisible scatter to create legend entry (matches data chart layout) */
        var placeholder = chart.Plot.Add.Scatter(new double[] { 0 }, new double[] { 0 });
        placeholder.LegendText = legendText;
        placeholder.Color = ScottPlot.Color.FromHex("#888888");
        placeholder.MarkerSize = 0;
        placeholder.LineWidth = 0;

        /* Add centered "No Data" text */
        var text = chart.Plot.Add.Text($"{legendText}\nNo Data", 0, 0);
        text.LabelFontColor = ScottPlot.Color.FromHex("#888888");
        text.LabelFontSize = 14;
        text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;

        /* Configure axes */
        chart.Plot.HideGrid();
        chart.Plot.Axes.SetLimitsX(-1, 1);
        chart.Plot.Axes.SetLimitsY(-1, 1);
        chart.Plot.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.Axes.Left.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.YLabel(yAxisLabel);

        /* Show legend to match data chart layout */
        ShowChartLegend(chart);
        chart.Refresh();
    }

    /// <summary>
    /// Shows legend on chart and tracks it for proper cleanup on next refresh.
    /// </summary>
    private void ShowChartLegend(ScottPlot.WPF.WpfPlot chart)
    {
        _legendPanels[chart] = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
        chart.Plot.Legend.FontSize = 13;
    }

    /// <summary>
    /// Applies the Darling Data dark theme to a ScottPlot chart.
    /// Matches Dashboard TabHelpers.ApplyDarkModeToChart exactly.
    /// </summary>
    private static void ApplyDarkTheme(ScottPlot.WPF.WpfPlot chart)
    {
        var darkBackground = ScottPlot.Color.FromHex("#22252b");
        var darkerBackground = ScottPlot.Color.FromHex("#111217");
        var textColor = ScottPlot.Color.FromHex("#9DA5B4");
        var gridColor = ScottPlot.Colors.White.WithAlpha(20);

        chart.Plot.FigureBackground.Color = darkBackground;
        chart.Plot.DataBackground.Color = darkerBackground;
        chart.Plot.Axes.Color(textColor);
        chart.Plot.Grid.MajorLineColor = gridColor;
        chart.Plot.Legend.BackgroundColor = darkBackground;
        chart.Plot.Legend.FontColor = ScottPlot.Color.FromHex("#E4E6EB");
        chart.Plot.Legend.OutlineColor = ScottPlot.Color.FromHex("#2a2d35");
        chart.Plot.Legend.Alignment = ScottPlot.Alignment.LowerCenter;
        chart.Plot.Legend.Orientation = ScottPlot.Orientation.Horizontal;
        chart.Plot.Axes.Margins(bottom: 0); /* No bottom margin - SetChartYLimitsWithLegendPadding handles Y-axis */

        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
        chart.Plot.Axes.Left.Label.ForeColor = textColor;
    }

    /// <summary>
    /// Reapplies dark mode text colors and font sizes after DateTimeTicksBottom() resets them.
    /// </summary>
    private static void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart)
    {
        var textColor = ScottPlot.Color.FromHex("#9DA5B4");
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
        chart.Plot.Axes.Left.Label.ForeColor = textColor;
    }

    /// <summary>
    /// Sets Y-axis limits with padding for bottom legend and top breathing room.
    /// </summary>
    private static void SetChartYLimitsWithLegendPadding(ScottPlot.WPF.WpfPlot chart, double dataYMin = 0, double dataYMax = 0)
    {
        if (dataYMin == 0 && dataYMax == 0)
        {
            var limits = chart.Plot.Axes.GetLimits();
            dataYMin = limits.Bottom;
            dataYMax = limits.Top;
        }
        if (dataYMax <= dataYMin) dataYMax = dataYMin + 100;

        double range = dataYMax - dataYMin;
        double topPadding = range * 0.05;

        /* Only add bottom padding if dataYMin is above zero - don't go negative */
        double yMin = dataYMin >= 0 ? 0 : dataYMin - (range * 0.10);
        double yMax = dataYMax + topPadding;

        chart.Plot.Axes.SetLimitsY(yMin, yMax);
    }

    /* DataGrid copy helpers */
    /// <summary>
    /// Finds the parent DataGrid from a context menu opened on a DataGridRow.
    /// </summary>
    private static DataGrid? FindParentDataGrid(MenuItem menuItem)
    {
        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        while (target != null && target is not DataGrid)
        {
            target = System.Windows.Media.VisualTreeHelper.GetParent(target) as FrameworkElement;
        }
        return target as DataGrid;
    }

    /// <summary>
    /// Gets a cell value from a row item for any column type (bound or template).
    /// Template columns are inspected for a TextBlock binding in their CellTemplate.
    /// </summary>
    private static string GetCellValue(DataGridColumn col, object item)
    {
        /* DataGridBoundColumn — binding is directly accessible */
        if (col is DataGridBoundColumn boundCol
            && boundCol.Binding is System.Windows.Data.Binding binding)
        {
            var prop = item.GetType().GetProperty(binding.Path.Path);
            return prop?.GetValue(item)?.ToString() ?? "";
        }

        /* DataGridTemplateColumn — instantiate the template and find a TextBlock binding */
        if (col is DataGridTemplateColumn templateCol && templateCol.CellTemplate != null)
        {
            var content = templateCol.CellTemplate.LoadContent();
            if (content is TextBlock textBlock)
            {
                var textBinding = System.Windows.Data.BindingOperations.GetBinding(textBlock, TextBlock.TextProperty);
                if (textBinding != null)
                {
                    var prop = item.GetType().GetProperty(textBinding.Path.Path);
                    return prop?.GetValue(item)?.ToString() ?? "";
                }
            }
        }

        return "";
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentCell.Column == null || grid.CurrentItem == null) return;

        var value = GetCellValue(grid.CurrentCell.Column, grid.CurrentItem);
        if (value.Length > 0) Clipboard.SetDataObject(value, false);
    }

    private void CopyRow_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        var sb = new StringBuilder();
        foreach (var col in grid.Columns)
        {
            sb.Append(GetCellValue(col, grid.CurrentItem));
            sb.Append('\t');
        }
        Clipboard.SetDataObject(sb.ToString().TrimEnd('\t'), false);
    }

    private void CopyAllRows_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null) return;

        var sb = new StringBuilder();

        /* Header */
        foreach (var col in grid.Columns)
        {
            sb.Append(col.Header?.ToString() ?? "");
            sb.Append('\t');
        }
        sb.AppendLine();

        /* Rows */
        foreach (var item in grid.Items)
        {
            foreach (var col in grid.Columns)
            {
                sb.Append(GetCellValue(col, item));
                sb.Append('\t');
            }
            sb.AppendLine();
        }

        Clipboard.SetDataObject(sb.ToString(), false);
    }

    private async void CopyReproScript_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        string? queryText = null;
        string? databaseName = null;
        string? planXml = null;
        string? isolationLevel = null;
        string source = "Query";

        switch (grid.CurrentItem)
        {
            case QuerySnapshotRow snapshot:
                queryText = snapshot.QueryText;
                databaseName = snapshot.DatabaseName;
                planXml = snapshot.QueryPlan;
                isolationLevel = snapshot.TransactionIsolationLevel;
                source = "Active Queries";
                break;

            case QueryStatsRow stats:
                queryText = stats.QueryText;
                databaseName = stats.DatabaseName;
                source = "Top Queries (dm_exec_query_stats)";
                /* Fetch plan on-demand from SQL Server */
                if (!string.IsNullOrEmpty(stats.QueryHash))
                {
                    try
                    {
                        var connStr = _server.GetConnectionString(_credentialService);
                        planXml = await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, stats.QueryHash);
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            case QueryStoreRow qs:
                queryText = qs.QueryText;
                databaseName = qs.DatabaseName;
                source = "Query Store";
                /* Fetch plan on-demand from Query Store */
                if (qs.PlanId > 0 && !string.IsNullOrEmpty(qs.DatabaseName))
                {
                    try
                    {
                        var connStr = _server.GetConnectionString(_credentialService);
                        planXml = await LocalDataService.FetchQueryStorePlanAsync(connStr, qs.DatabaseName, qs.PlanId);
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            default:
                /* Not a supported grid for repro scripts — copy query text if available */
                var textProp = grid.CurrentItem.GetType().GetProperty("QueryText");
                queryText = textProp?.GetValue(grid.CurrentItem)?.ToString();
                if (string.IsNullOrEmpty(queryText))
                {
                    return;
                }
                var dbProp = grid.CurrentItem.GetType().GetProperty("DatabaseName");
                databaseName = dbProp?.GetValue(grid.CurrentItem)?.ToString();
                break;
        }

        if (string.IsNullOrEmpty(queryText))
        {
            return;
        }

        var script = ReproScriptBuilder.BuildReproScript(queryText, databaseName, planXml, isolationLevel, source);

        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() operation.
           See: https://github.com/dotnet/wpf/issues/9901 */
        Clipboard.SetDataObject(script, false);
    }

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null || grid.Items.Count == 0) return;

        var dialog = new SaveFileDialog
        {
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            DefaultExt = ".csv",
            FileName = $"{_server.DisplayName}_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (dialog.ShowDialog() != true) return;

        var sb = new StringBuilder();

        /* Header */
        var headers = new List<string>();
        foreach (var col in grid.Columns)
        {
            headers.Add(CsvEscape(col.Header?.ToString() ?? ""));
        }
        sb.AppendLine(string.Join(",", headers));

        /* Rows */
        foreach (var item in grid.Items)
        {
            var values = new List<string>();
            foreach (var col in grid.Columns)
            {
                values.Add(CsvEscape(GetCellValue(col, item)));
            }
            sb.AppendLine(string.Join(",", values));
        }

        try
        {
            File.WriteAllText(dialog.FileName, sb.ToString(), Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to export: {ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
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


    private async void DownloadQueryStatsPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not QueryStatsRow row) return;
        if (string.IsNullOrEmpty(row.QueryHash)) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            var connStr = _server.GetConnectionString(_credentialService);
            var plan = await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, row.QueryHash);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show("No query plan found in the plan cache for this query hash. The plan may have been evicted.", "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }
            SavePlanFile(plan, $"QueryPlan_{row.QueryHash}");
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            btn.Content = "Save";
            btn.IsEnabled = true;
        }
    }

    private void DownloadSnapshotPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not QuerySnapshotRow row || row.QueryPlan == null) return;
        SavePlanFile(row.QueryPlan, $"EstimatedPlan_Session{row.SessionId}");
    }

    private void DownloadSnapshotLivePlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not QuerySnapshotRow row || row.LiveQueryPlan == null) return;
        SavePlanFile(row.LiveQueryPlan, $"ActualPlan_Session{row.SessionId}");
    }

    private void SavePlanFile(string planXml, string defaultName)
    {
        var dialog = new SaveFileDialog
        {
            Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
            DefaultExt = ".sqlplan",
            FileName = $"{defaultName}_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, planXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save plan: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void DownloadDeadlockXml_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not DeadlockProcessDetail row || string.IsNullOrEmpty(row.DeadlockGraphXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "XML files (*.xml)|*.xml|All files (*.*)|*.*",
            DefaultExt = ".xml",
            FileName = $"deadlock_{row.DeadlockTime:yyyyMMdd_HHmmss}.xml"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, row.DeadlockGraphXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save deadlock XML: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void DownloadBlockedProcessXml_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || btn.DataContext is not BlockedProcessReportRow row || string.IsNullOrEmpty(row.BlockedProcessReportXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "XML files (*.xml)|*.xml|All files (*.*)|*.*",
            DefaultExt = ".xml",
            FileName = $"blocked_process_{row.EventTime:yyyyMMdd_HHmmss}.xml"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, row.BlockedProcessReportXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save blocked process XML: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private static string CsvEscape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /* ========== Collection Health ========== */

    private void UpdateCollectorDurationChart(List<CollectionLogRow> data)
    {
        ClearChart(CollectorDurationChart);
        ApplyDarkTheme(CollectorDurationChart);

        if (data.Count == 0) { CollectorDurationChart.Refresh(); return; }

        /* Group by collector, plot each as a separate series */
        var groups = data
            .Where(d => d.DurationMs.HasValue && d.Status == "SUCCESS")
            .GroupBy(d => d.CollectorName)
            .OrderBy(g => g.Key)
            .ToList();

        int colorIdx = 0;
        foreach (var group in groups)
        {
            var points = group.OrderBy(d => d.CollectionTime).ToList();
            if (points.Count < 2) continue;

            var times = points.Select(d => d.CollectionTime.ToLocalTime().ToOADate()).ToArray();
            var durations = points.Select(d => (double)d.DurationMs!.Value).ToArray();

            var scatter = CollectorDurationChart.Plot.Add.Scatter(times, durations);
            scatter.LegendText = group.Key;
            scatter.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            scatter.LineWidth = 2;
            scatter.MarkerSize = 0;
            colorIdx++;
        }

        CollectorDurationChart.Plot.Axes.DateTimeTicksBottom();
        ReapplyAxisColors(CollectorDurationChart);
        CollectorDurationChart.Plot.YLabel("Duration (ms)");
        CollectorDurationChart.Plot.Axes.AutoScale();
        ShowChartLegend(CollectorDurationChart);
        CollectorDurationChart.Refresh();
    }

    private void OpenLogFile_Click(object sender, RoutedEventArgs e)
    {
        var logDir = System.IO.Path.Combine(AppContext.BaseDirectory, "logs");
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

    /* ========== Column Filtering ========== */

    private void InitializeFilterManagers()
    {
        _querySnapshotsFilterMgr = new DataGridFilterManager<QuerySnapshotRow>(QuerySnapshotsGrid);
        _queryStatsFilterMgr = new DataGridFilterManager<QueryStatsRow>(QueryStatsGrid);
        _procStatsFilterMgr = new DataGridFilterManager<ProcedureStatsRow>(ProcedureStatsGrid);
        _queryStoreFilterMgr = new DataGridFilterManager<QueryStoreRow>(QueryStoreGrid);
        _blockedProcessFilterMgr = new DataGridFilterManager<BlockedProcessReportRow>(BlockedProcessReportGrid);
        _deadlockFilterMgr = new DataGridFilterManager<DeadlockProcessDetail>(DeadlockGrid);
        _runningJobsFilterMgr = new DataGridFilterManager<RunningJobRow>(RunningJobsGrid);
        _serverConfigFilterMgr = new DataGridFilterManager<ServerConfigRow>(ServerConfigGrid);
        _databaseConfigFilterMgr = new DataGridFilterManager<DatabaseConfigRow>(DatabaseConfigGrid);
        _dbScopedConfigFilterMgr = new DataGridFilterManager<DatabaseScopedConfigRow>(DatabaseScopedConfigGrid);
        _traceFlagsFilterMgr = new DataGridFilterManager<TraceFlagRow>(TraceFlagsGrid);
        _collectionHealthFilterMgr = new DataGridFilterManager<CollectorHealthRow>(CollectionHealthGrid);
        _collectionLogFilterMgr = new DataGridFilterManager<CollectionLogRow>(CollectionLogGrid);

        _filterManagers[QuerySnapshotsGrid] = _querySnapshotsFilterMgr;
        _filterManagers[QueryStatsGrid] = _queryStatsFilterMgr;
        _filterManagers[ProcedureStatsGrid] = _procStatsFilterMgr;
        _filterManagers[QueryStoreGrid] = _queryStoreFilterMgr;
        _filterManagers[BlockedProcessReportGrid] = _blockedProcessFilterMgr;
        _filterManagers[DeadlockGrid] = _deadlockFilterMgr;
        _filterManagers[RunningJobsGrid] = _runningJobsFilterMgr;
        _filterManagers[ServerConfigGrid] = _serverConfigFilterMgr;
        _filterManagers[DatabaseConfigGrid] = _databaseConfigFilterMgr;
        _filterManagers[DatabaseScopedConfigGrid] = _dbScopedConfigFilterMgr;
        _filterManagers[TraceFlagsGrid] = _traceFlagsFilterMgr;
        _filterManagers[CollectionHealthGrid] = _collectionHealthFilterMgr;
        _filterManagers[CollectionLogGrid] = _collectionLogFilterMgr;
    }

    private void EnsureFilterPopup()
    {
        if (_filterPopup == null)
        {
            _filterPopupContent = new ColumnFilterPopup();
            _filterPopup = new Popup
            {
                Child = _filterPopupContent,
                StaysOpen = false,
                Placement = PlacementMode.Bottom,
                AllowsTransparency = true
            };
        }
    }

    private DataGrid? _currentFilterGrid;

    private void FilterButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;

        /* Walk up visual tree to find the parent DataGrid */
        var dataGrid = FindParentDataGridFromElement(button);
        if (dataGrid == null || !_filterManagers.TryGetValue(dataGrid, out var manager)) return;

        _currentFilterGrid = dataGrid;

        EnsureFilterPopup();

        /* Rewire events to the current grid */
        _filterPopupContent!.FilterApplied -= FilterPopup_FilterApplied;
        _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;
        _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
        _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

        /* Initialize with existing filter state */
        manager.Filters.TryGetValue(columnName, out var existingFilter);
        _filterPopupContent.Initialize(columnName, existingFilter);

        _filterPopup!.PlacementTarget = button;
        _filterPopup.IsOpen = true;
    }

    private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;

        if (_currentFilterGrid != null && _filterManagers.TryGetValue(_currentFilterGrid, out var manager))
        {
            manager.SetFilter(e.FilterState);
        }
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;
    }

    private static DataGrid? FindParentDataGridFromElement(DependencyObject element)
    {
        var current = element;
        while (current != null)
        {
            if (current is DataGrid dg)
                return dg;
            current = VisualTreeHelper.GetParent(current);
        }
        return null;
    }
}
