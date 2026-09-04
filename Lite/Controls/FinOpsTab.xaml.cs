/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitorLite.Controls;

public partial class FinOpsTab : UserControl
{
    private LocalDataService? _dataService;
    private ServerManager? _serverManager;
    private CredentialResolver? _credentialResolver;
    private List<ServerPropertyRow>? _serverInventoryCache;
    private DateTime _serverInventoryCacheTime;

    private readonly Dictionary<DataGrid, IDataGridFilterManager> _filterManagers = new();

    /* #2306: suppresses ServerSelector_SelectionChanged while RefreshServerList reselects the SAME
       logical server through a new instance — without it, any Manage Servers edit would clear filters
       for a server switch that never happened. Darling's FinOps tab carries the same flag. */
    private bool _populatingServers;

    private DataGridFilterManager<DatabaseResourceUsageRow>? _dbResourcesFilterMgr;
    private DataGridFilterManager<StorageGrowthRow>? _storageGrowthFilterMgr;
    private DataGridFilterManager<DatabaseSizeRow>? _dbSizesFilterMgr;
    private DataGridFilterManager<PvsStatsRow>? _pvsStatsFilterMgr;
    private DataGridFilterManager<IndexCleanupSummaryRow>? _indexSummaryFilterMgr;
    private DataGridFilterManager<IndexCleanupResultRow>? _indexDetailFilterMgr;
    private DataGridFilterManager<ApplicationConnectionRow>? _appConnectionsFilterMgr;
    private DataGridFilterManager<ServerPropertyRow>? _serverInventoryFilterMgr;
    private DataGridFilterManager<HighImpactQueryRow>? _highImpactFilterMgr;
    private DataGridFilterManager<IdleDatabaseRow>? _idleDbsFilterMgr;
    private DataGridFilterManager<TempdbSummaryRow>? _tempdbFilterMgr;
    private DataGridFilterManager<WaitCategorySummaryRow>? _waitCategoryFilterMgr;
    private DataGridFilterManager<ExpensiveQueryRow>? _expensiveQueriesFilterMgr;
    private DataGridFilterManager<MemoryGrantEfficiencyRow>? _memoryGrantFilterMgr;
    private DataGridFilterManager<IndexLockingRow>? _indexLockingFilterMgr;

    /* #2933: this tab has no load guard, and every grid above is shared across servers — one
       server switch during the fourteen-way fan-out in LoadPerServerDataAsync, or a per-grid Refresh
       during it, leaves two differently-scoped reads in flight for one grid and the LATER-STARTING
       one can land first. Each loader claims a generation for its own grid and drops its paint when a
       newer load for that grid has begun; keyed per grid, because a single-grid Refresh must not
       discard the other thirteen paints of a whole-tab load. Lite's own answer to this shape at three
       sites already (ServerTab.Pickers.cs' _waitStatsPickerGen / _memoryClerksPickerGen /
       _perfmonPickerGen); this is that idiom with the key spelled once. */
    private readonly ScopedLoadGenerations _loads = new();

    public FinOpsTab()
    {
        InitializeComponent();
        InitializeFilterManagers();
    }

    /// <summary>
    /// Initializes the control with required dependencies.
    /// </summary>
    public void Initialize(LocalDataService dataService, ServerManager serverManager)
    {
        _dataService = dataService;
        _serverManager = serverManager;
        _credentialResolver = serverManager.CredentialResolver;

        PopulateServerSelector();
        RefreshData();
    }

    /// <summary>
    /// Refreshes the server dropdown from the current server list.
    /// Called when servers are added or removed.
    /// </summary>
    public void RefreshServerList()
    {
        if (_serverManager == null) return;
        _serverInventoryCache = null; // Invalidate cache when server list changes

        var previousSelection = ServerSelector.SelectedItem as ServerConnection;
        var servers = _serverManager.GetAllServers();

        if (previousSelection != null
            && servers.FirstOrDefault(s => s.Id == previousSelection.Id) is { } match)
        {
            /* #2306 review catch (Darling's _populatingServers, ported): the same logical server
               reselected through a NEW instance (ServerManager replaces edited entries, and
               ComboBox compares by reference) still raises SelectionChanged. Without the guard, a
               tag or favorite edit in Manage Servers would wipe active column filters for a server
               switch that never happened. Nothing changed for this tab, so the handler — clear,
               drill reset, reload — is suppressed entirely. */
            _populatingServers = true;
            try
            {
                ServerSelector.ItemsSource = servers;
                ServerSelector.SelectedItem = match;
            }
            finally
            {
                _populatingServers = false;
            }

            return;
        }

        /* The previous selection is gone (or never existed): the selection genuinely moves, so the
           assignments below fire the handler on purpose — a real switch clears filters and reloads. */
        ServerSelector.ItemsSource = servers;
        if (servers.Count > 0)
            ServerSelector.SelectedIndex = 0;
    }

    private void PopulateServerSelector()
    {
        if (_serverManager == null) return;

        var servers = _serverManager.GetAllServers();
        ServerSelector.ItemsSource = servers;
        if (servers.Count > 0)
            ServerSelector.SelectedIndex = 0;
    }

    private int GetSelectedServerId()
    {
        if (ServerSelector.SelectedItem is ServerConnection server)
            return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        return 0;
    }

    // ── Plan navigation for the query-identifying FinOps grids ──
    // Lazy: executeActual reads the current selected-server connection string, and Window.GetWindow(this)
    // is only valid once the control is in the visual tree.
    private PlanNavigationController? _planActions;
    private PlanNavigationController PlanActions => _planActions ??= new PlanNavigationController(
        Window.GetWindow(this)!,
        (xml, label, qt) => Windows.PlanViewerWindow.ShowPlanAsync(Window.GetWindow(this)!, xml, label, qt),
        (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
            GetSelectedConnectionString() ?? "", db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct,
            productName: "SQL Server Performance Monitor Lite"),
        "the monitored server");

    private string? GetSelectedConnectionString()
        => ServerSelector.SelectedItem is ServerConnection s && _credentialResolver != null
            ? _credentialResolver.GetConnectionString(s)
            : null;

    private async System.Threading.Tasks.Task<string?> FetchFinOpsHighImpactPlanAsync(string queryHash)
    {
        if (string.IsNullOrEmpty(queryHash)) return null;
        var serverId = GetSelectedServerId();
        string? plan = null;
        if (serverId != 0 && _dataService != null)
        {
            try { plan = await System.Threading.Tasks.Task.Run(() => _dataService.GetCachedQueryPlanAsync(serverId, queryHash)); }
            catch { /* fall through to the live server */ }
        }
        if (string.IsNullOrEmpty(plan))
        {
            var connStr = GetSelectedConnectionString();
            if (!string.IsNullOrEmpty(connStr))
                plan = await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, queryHash);
        }
        return plan;
    }

    private async void FinOpsViewPlan_Click(object sender, RoutedEventArgs e)
    {
        if (GetFinOpsRow(sender) is HighImpactQueryRow row)
            await PlanActions.ViewPlanAsync(
                () => FetchFinOpsHighImpactPlanAsync(row.QueryHash),
                $"Est Plan - {row.QueryHash}", row.FullQueryText);
    }

    private async void FinOpsGetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        switch (GetFinOpsRow(sender))
        {
            case HighImpactQueryRow hi:
                await PlanActions.GetActualPlanAsync(hi.FullQueryText, hi.DatabaseName, $"Actual Plan - {hi.QueryHash}");
                break;
            case ExpensiveQueryRow ex:
                await PlanActions.GetActualPlanAsync(ex.FullQueryText, ex.DatabaseName, "Actual Plan - Expensive Query");
                break;
        }
    }

    private static object? GetFinOpsRow(object sender)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            if (contextMenu.PlacementTarget is DataGridRow row) return row.DataContext;
            if (contextMenu.PlacementTarget is DataGrid grid) return grid.CurrentCell.Item ?? grid.SelectedItem;
        }
        return null;
    }

    /// <summary>
    /// Refreshes all FinOps data.
    /// </summary>
    private decimal _currentServerMonthlyCost;

    public async void RefreshData()
    {
        await LoadServerInventoryAsync();
        await LoadPerServerDataAsync();
    }

    #region Data Loading

    private async System.Threading.Tasks.Task LoadPerServerDataAsync()
    {
        using var _profiler = Helpers.MethodProfiler.StartTiming("FinOps-PerServerData");
        var serverId = GetSelectedServerId();
        if (serverId == 0 || _dataService == null) return;

        // Re-read monthly cost from server manager in case user edited the server config
        if (ServerSelector.SelectedItem is Models.ServerConnection selectedServer && _serverManager != null)
        {
            var fresh = _serverManager.GetServerById(selectedServer.Id);
            _currentServerMonthlyCost = fresh?.MonthlyCostUsd ?? selectedServer.MonthlyCostUsd;
        }

        await System.Threading.Tasks.Task.WhenAll(
            LoadRecommendationsAsync(serverId),
            LoadUtilizationAsync(serverId),
            LoadDatabaseResourcesAsync(serverId),
            LoadApplicationConnectionsAsync(serverId),
            LoadDatabaseSizesAsync(serverId),
            LoadPvsStatsAsync(serverId),
            LoadStorageGrowthAsync(serverId),
            LoadIndexLockingAsync(serverId),
            LoadIdleDatabasesAsync(serverId),
            LoadTempdbSummaryAsync(serverId),
            LoadWaitCategorySummaryAsync(serverId),
            LoadExpensiveQueriesAsync(serverId),
            LoadMemoryGrantEfficiencyAsync(serverId),
            LoadHighImpactQueriesAsync(serverId)
        );
    }

    private async System.Threading.Tasks.Task LoadRecommendationsAsync(int serverId)
    {
        if (_dataService == null || _credentialResolver == null) return;

        var gen = _loads.Claim(nameof(LoadRecommendationsAsync));

        try
        {
            var selectedServer = ServerSelector.SelectedItem as Models.ServerConnection;
            var connectionString = selectedServer == null ? null : _credentialResolver.GetConnectionString(selectedServer);
            if (string.IsNullOrEmpty(connectionString)) return;

            var utilityConnectionString = _credentialResolver.GetUtilityConnectionString(selectedServer!);
            var data = await Task.Run(() => _dataService.GetRecommendationsAsync(serverId, connectionString, utilityConnectionString, _currentServerMonthlyCost));
            if (_loads.Superseded(nameof(LoadRecommendationsAsync), gen)) return;
            RecommendationsDataGrid.ItemsSource = data;
            RecommendationsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            RecommendationsCountIndicator.Text = data.Count > 0 ? $"{data.Count} recommendation(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load recommendations: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadUtilizationAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadUtilizationAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetUtilizationEfficiencyAsync(serverId));
            if (_loads.Superseded(nameof(LoadUtilizationAsync), gen)) return;

            if (data != null)
            {
                data.MonthlyCost = _currentServerMonthlyCost;

                // Compute free space % for health score from database sizes
                var dbSizes = await Task.Run(() => _dataService.GetDatabaseSizeLatestAsync(serverId));
                if (_loads.Superseded(nameof(LoadUtilizationAsync), gen)) return;
                var totalStorageMb = dbSizes.Sum(d => d.TotalSizeMb);
                var totalFreeMb = dbSizes.Sum(d => (d.FreeSpaceMb ?? 0m));
                data.FreeSpacePct = totalStorageMb > 0 ? totalFreeMb / totalStorageMb * 100m : 100m;
            }

            UpdateUtilizationSummary(data);
            NoUtilizationMessage.Visibility = data == null ? Visibility.Visible : Visibility.Collapsed;
            SummaryContent.Visibility = data == null ? Visibility.Collapsed : Visibility.Visible;

            /* Read into locals and paint all four together at the end, rather than assigning each ItemsSource
               straight from its own await. An assignment that IS the await statement paints the moment that
               read completes, so a check placed after the fourth one guarded nothing: three of the four could
               already have landed for a load a server switch had superseded. Painting together also removes
               the duplicated null-clearing branch — no data means four nulls, which is what the locals already hold. */
            System.Collections.IEnumerable? topTotal = null;
            System.Collections.IEnumerable? topAvg = null;
            System.Collections.IEnumerable? dbSizeSummary = null;
            System.Collections.IEnumerable? provisioningTrend = null;

            if (data != null)
            {
                topTotal = await Task.Run(() => _dataService.GetTopResourceConsumersByTotalAsync(serverId));
                if (_loads.Superseded(nameof(LoadUtilizationAsync), gen)) return;

                topAvg = await Task.Run(() => _dataService.GetTopResourceConsumersByAvgAsync(serverId));
                if (_loads.Superseded(nameof(LoadUtilizationAsync), gen)) return;

                dbSizeSummary = await Task.Run(() => _dataService.GetDatabaseSizeSummaryAsync(serverId));
                if (_loads.Superseded(nameof(LoadUtilizationAsync), gen)) return;

                provisioningTrend = await Task.Run(() => _dataService.GetProvisioningTrendAsync(serverId));
                if (_loads.Superseded(nameof(LoadUtilizationAsync), gen)) return;
            }

            TopTotalGrid.ItemsSource = topTotal;
            TopAvgGrid.ItemsSource = topAvg;
            DbSizeChart.ItemsSource = dbSizeSummary;
            ProvisioningTrendGrid.ItemsSource = provisioningTrend;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load utilization: {ex.Message}");
        }
    }

    private void UpdateUtilizationSummary(UtilizationEfficiencyRow? data)
    {
        if (data == null)
        {
            ProvisioningStatusText.Text = "No Data";
            ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
            AvgCpuText.Text = P95CpuText.Text = MaxCpuText.Text = CpuSamplesText.Text = "-";
            CpuCountText.Text = "-";
            WorkerThreadsText.Text = "-";
            AvgCpuBar.Width = P95CpuBar.Width = MaxCpuBar.Width = 0;
            MemoryUtilBar.Width = MemoryRatioBar.Width = 0;
            MemoryUtilText.Text = MemoryRatioText.Text = "-";
            PhysicalMemoryText.Text = TargetMemoryText.Text = TotalMemoryText.Text = BufferPoolText.Text = "-";
            ClassificationExplanation.Text = "";
            UtilizationContent.Visibility = Visibility.Collapsed;
            return;
        }

        UtilizationContent.Visibility = Visibility.Visible;

        ProvisioningStatusText.Text = data.ProvisioningStatus.Replace("_", " ");
        switch (data.ProvisioningStatus)
        {
            case "RIGHT_SIZED":
                ProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#27AE60"));
                ProvisioningStatusText.Foreground = Brushes.White;
                break;
            case "OVER_PROVISIONED":
                ProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#F39C12"));
                ProvisioningStatusText.Foreground = Brushes.Black;
                break;
            case "UNDER_PROVISIONED":
                ProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E74C3C"));
                ProvisioningStatusText.Foreground = Brushes.White;
                break;
            default:
                ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                ProvisioningStatusText.Foreground = Brushes.White;
                break;
        }

        /* CPU text + bars */
        AvgCpuText.Text = $"{data.AvgCpuPct:N2}%";
        P95CpuText.Text = $"{data.P95CpuPct:N2}%";
        MaxCpuText.Text = $"{data.MaxCpuPct}%";
        CpuSamplesText.Text = data.CpuSamples.ToString("N0");
        CpuCountText.Text = data.CpuCount.ToString("N0");
        WorkerThreadsText.Text = $"{data.CurrentWorkersCount:N0} / {data.MaxWorkersCount:N0}";

        SetBar(AvgCpuBar, AvgCpuFilled, AvgCpuEmpty, (double)data.AvgCpuPct);
        SetBar(P95CpuBar, P95CpuFilled, P95CpuEmpty, (double)data.P95CpuPct);
        SetBar(MaxCpuBar, MaxCpuFilled, MaxCpuEmpty, data.MaxCpuPct);

        /* Stolen Memory % = (Total Server Memory - Buffer Pool) / Total Server Memory */
        var stolenPct = data.TotalMemoryMb > 0
            ? (double)(data.TotalMemoryMb - data.BufferPoolMb) / data.TotalMemoryMb * 100.0
            : 0;
        MemoryUtilText.Text = $"{stolenPct:N0}%";
        SetBar(MemoryUtilBar, MemUtilFilled, MemUtilEmpty, stolenPct);

        /* Buffer Pool % = Buffer Pool / Physical Memory */
        var bpPct = data.PhysicalMemoryMb > 0
            ? (double)data.BufferPoolMb / data.PhysicalMemoryMb * 100.0
            : 0;
        MemoryRatioText.Text = $"{bpPct:N0}%";
        SetBar(MemoryRatioBar, MemRatioFilled, MemRatioEmpty, bpPct);

        PhysicalMemoryText.Text = $"{data.PhysicalMemoryMb:N0} MB";
        TargetMemoryText.Text = $"{data.TargetMemoryMb:N0} MB";
        TotalMemoryText.Text = $"{data.TotalMemoryMb:N0} MB";
        BufferPoolText.Text = $"{data.BufferPoolMb:N0} MB";

        /* Contextual explanation — one sentence describing WHY this classification */
        ClassificationExplanation.Text = data.ProvisioningStatus switch
        {
            "RIGHT_SIZED" => $"CPU is moderately loaded (avg {data.AvgCpuPct:N1}%, p95 {data.P95CpuPct:N1}%) and memory is well-utilized (buffer pool uses {bpPct:N0}% of physical RAM). No action needed.",
            "OVER_PROVISIONED" => $"CPU is lightly loaded (avg {data.AvgCpuPct:N1}%, max {data.MaxCpuPct}%) and buffer pool uses only {bpPct:N0}% of physical RAM. This server may have more resources than it needs.",
            /* The reason comes from the same place as the verdict. This branch used to read
               "P95CpuPct > 85 ? CPU : memory ratio is {x} (threshold: 0.95)", so a server flagged for grant
               pressure or worker saturation would have been explained as a memory ratio that no longer
               decides anything, citing a threshold the code does not check (#2246). */
            "UNDER_PROVISIONED" => ProvisioningVerdict.UnderProvisionedReason(
                data.P95CpuPct, data.MaxGrantWaiters, data.GrantTimeouts, data.ForcedGrants,
                data.MaxWorkersCount, data.CurrentWorkersCount),
            _ => ""
        };

        /* Cost summary cards — show if monthly cost is configured */
        if (data.MonthlyCost > 0)
        {
            AnnualComputeCostText.Text = $"${data.MonthlyCost:N0}/mo";
            AnnualTotalCostText.Text = $"${data.AnnualCost:N0}/yr";
            ComputeCostCard.Visibility = Visibility.Visible;
            TotalCostCard.Visibility = Visibility.Visible;
        }
        else
        {
            ComputeCostCard.Visibility = Visibility.Collapsed;
            TotalCostCard.Visibility = Visibility.Collapsed;
        }
        StorageCostCard.Visibility = Visibility.Collapsed;

        /* Health score */
        var bpRatio = data.PhysicalMemoryMb > 0 ? (decimal)data.BufferPoolMb / data.PhysicalMemoryMb : 0m;
        var cpuScore = FinOpsHealthCalculator.CpuScore(data.P95CpuPct);
        var memScore = FinOpsHealthCalculator.MemoryScore(bpRatio);
        var storScore = FinOpsHealthCalculator.StorageScore(data.FreeSpacePct);
        data.HealthScore = FinOpsHealthCalculator.Overall(cpuScore, memScore, storScore);
        HealthScoreText.Text = $"Health: {data.HealthScore}";
        HealthScoreBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString(data.HealthScoreColor));
        HealthScoreBorder.Visibility = Visibility.Visible;
    }

    private static void SetBar(Border bar, ColumnDefinition filled, ColumnDefinition empty, double pct)
    {
        var clamped = Math.Max(0, Math.Min(100, pct));

        /* Color thresholds: green < 60, orange 60-85, red > 85 */
        var color = clamped switch
        {
            > 85 => "#E74C3C",
            > 60 => "#F39C12",
            _ => "#27AE60"
        };
        bar.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString(color));

        /* Use star-width proportions — the layout engine handles sizing natively */
        filled.Width = new GridLength(Math.Max(clamped, 0.1), GridUnitType.Star);
        empty.Width = new GridLength(Math.Max(100 - clamped, 0.1), GridUnitType.Star);
    }

    private int HoursBackFromIndex(System.Windows.Controls.ComboBox combo) => combo.SelectedIndex switch { 0 => 1, 1 => 4, 2 => 12, 3 => 24, 4 => 168, _ => 24 };

    private async void ResourceUsageTimeRange_Changed(object sender, System.Windows.Controls.SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _dataService == null) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        await LoadDatabaseResourcesAsync(serverId);
    }

    private async System.Threading.Tasks.Task LoadDatabaseResourcesAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadDatabaseResourcesAsync));

        try
        {
            var hoursBack = HoursBackFromIndex(ResourceUsageTimeRangeCombo);
            var data = await Task.Run(() => _dataService.GetDatabaseResourceUsageAsync(serverId, hoursBack));
            if (_loads.Superseded(nameof(LoadDatabaseResourcesAsync), gen)) return;
            _dbResourcesFilterMgr!.UpdateData(data);
            NoDatabaseResourcesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            DbResourcesCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load database resources: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadApplicationConnectionsAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadApplicationConnectionsAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetApplicationConnectionsAsync(serverId));
            if (_loads.Superseded(nameof(LoadApplicationConnectionsAsync), gen)) return;
            _appConnectionsFilterMgr!.UpdateData(data);
            NoAppConnectionsMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            AppConnectionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} application(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load application connections: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadDatabaseSizesAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadDatabaseSizesAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetDatabaseSizeLatestAsync(serverId));
            if (_loads.Superseded(nameof(LoadDatabaseSizesAsync), gen)) return;

            // Compute proportional cost shares
            if (_currentServerMonthlyCost > 0 && data.Count > 0)
            {
                var totalMb = data.Sum(d => d.TotalSizeMb);
                if (totalMb > 0)
                {
                    foreach (var d in data)
                        d.MonthlyCostShare = (d.TotalSizeMb / totalMb) * _currentServerMonthlyCost;
                }
            }

            _dbSizesFilterMgr!.UpdateData(data);

            NoDbSizesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

            /* #2640: on Azure SQL DB this grid can only ever show ONE database, and saying so is the whole
               fix. The collector reads sys.database_files on the connected database — deliberately, because
               the enumeration it replaced went to master, which is the one database an Azure login reaching
               the server through a DATABASE-level firewall rule cannot open (#1631). So two rows named for
               whichever database the connection points at is CORRECT, and it reads exactly like a collector
               that only found master. A reporter connected to master saw "master data_0 / master log" and
               filed it as a bug, which is the reasonable reading of a grid headed "All Servers" that shows
               one database's files and explains nothing.

               The engine fact is read from the stored server properties, the same source and the same
               EngineEdition == 5 test the index-analysis path above already uses. */
            var scopeNote = string.Empty;

            if (data.Count > 0 && _dataService != null)
            {
                var properties = await _dataService.GetLatestServerPropertiesAsync(serverId);
                if (_loads.Superseded(nameof(LoadDatabaseSizesAsync), gen)) return;

                if (properties?.EngineEdition == 5)
                {
                    var only = data.Select(d => d.DatabaseName).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

                    scopeNote = only.Count == 1
                        ? $" — Azure SQL Database: only the CONNECTED database ('{only[0]}') is visible from "
                          + "this connection, so its siblings are not missing, they are unreachable. Add a server "
                          + "entry per database to see more."
                        : " — Azure SQL Database: each connection sees only its own database, so this grid "
                          + "covers the databases you have registered rather than every database on the server.";
                }
            }

            DbSizeCountIndicator.Text = data.Count > 0 ? $"{data.Count} file(s){scopeNote}" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load database sizes: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadPvsStatsAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadPvsStatsAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetPvsStatsLatestAsync(serverId));
            if (_loads.Superseded(nameof(LoadPvsStatsAsync), gen)) return;

            _pvsStatsFilterMgr!.UpdateData(data);

            NoPvsStatsMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            PvsCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";

            /* #1984 stage 2: the trend beside the grid — "when did it start growing" on the same
               time axis family as Storage Growth. Top-5 databases by current PVS size, 7 days. */
            var trend = await Task.Run(() => _dataService.GetPvsTrendAsync(serverId, DateTime.UtcNow.AddDays(-7)));
            if (_loads.Superseded(nameof(LoadPvsStatsAsync), gen)) return;
            RenderPvsTrendChart(trend);
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load PVS stats: {ex.Message}");
        }
    }

    /// <summary>
    /// One line per database, legend labels carrying each database's LATEST %-of-database (the two
    /// numbers #1984 asked for, on one chart rather than two stacked plots). Hidden entirely when
    /// there are no points — an ADR-less server gets no dead chart. Twin of the Darling viewer's
    /// RenderPvsTrendChart; series colours rotate the shared palette by index so redraws are stable.
    /// </summary>
    private void RenderPvsTrendChart(System.Collections.Generic.List<PvsTrendPoint> trend)
    {
        if (trend.Count == 0)
        {
            PvsTrendChart.Visibility = Visibility.Collapsed;
            return;
        }

        PvsTrendChart.Visibility = Visibility.Visible;
        PvsTrendChart.Plot.Clear();

        var seriesIndex = 0;
        foreach (var series in trend.GroupBy(t => t.DatabaseName).OrderByDescending(g => g.Max(t => t.PvsSizeMb)))
        {
            var points = series.OrderBy(t => t.CollectionTime).ToList();
            var times = points.Select(t => ServerTimeHelper.ToServerTime(t.CollectionTime).ToOADate()).ToArray();
            var values = points.Select(t => t.PvsSizeMb).ToArray();

            var line = PvsTrendChart.Plot.Add.TimeSeries(times, values);
            line.Color = ScottPlot.Color.FromHex(ChartPalette.CyclingColor(seriesIndex++));
            ChartStyle.StyleScatter(line);
            var latestPct = points[^1].PctOfDatabase;
            line.LegendText = latestPct is double pct
                ? $"{series.Key} ({pct:0.0}% of DB)"
                : series.Key;
        }

        PvsTrendChart.Plot.Legend.IsVisible = true;
        PvsTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        PvsTrendChart.Plot.Axes.AutoScale();
        PvsTrendChart.Plot.YLabel("PVS Off-Row MB");
        ChartStyle.ApplyThemeToChart(PvsTrendChart);
        PvsTrendChart.Refresh();
    }

    private async System.Threading.Tasks.Task LoadServerInventoryAsync(bool forceRefresh = false)
    {
        using var _profiler = Helpers.MethodProfiler.StartTiming("FinOps-ServerInventory");
        if (_dataService == null || _serverManager == null || _credentialResolver == null) return;

        var gen = _loads.Claim(nameof(LoadServerInventoryAsync));

        // Use cache if available and less than 5 minutes old
        if (!forceRefresh && _serverInventoryCache != null
            && (DateTime.Now - _serverInventoryCacheTime).TotalMinutes < 5)
        {
            _serverInventoryFilterMgr!.UpdateData(_serverInventoryCache);
            NoServerInventoryMessage.Visibility = _serverInventoryCache.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            ServerInventoryCountIndicator.Text = _serverInventoryCache.Count > 0 ? $"{_serverInventoryCache.Count} server(s)" : "";
            return;
        }

        try
        {
            var servers = _serverManager.GetAllServers();

            var tasks = servers.Select(async server =>
            {
                try
                {
                    var connStr = _credentialResolver.GetConnectionString(server);

                    // Step 1: Query live server properties
                    var item = await LocalDataService.GetServerPropertiesLiveAsync(connStr);
                    item.ServerName = server.DisplayName;
                    item.MonthlyCost = server.MonthlyCostUsd;

                    // Step 2: Get collected metrics from DuckDB
                    try
                    {
                        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
                        var (avgCpu, storageGb, idleDbs, status) = await Task.Run(() => _dataService!.GetServerMetricsAsync(serverId));
                        if (avgCpu.HasValue) item.AvgCpuPct = avgCpu;
                        if (storageGb.HasValue) item.StorageTotalGb = storageGb;
                        if (idleDbs.HasValue) item.IdleDbCount = idleDbs;
                        if (status != null) item.ProvisioningStatus = status;
                    }
                    catch
                    {
                        // DuckDB metrics may not exist yet — that's OK
                    }

                    return item;
                }
                catch (Exception ex)
                {
                    AppLogger.Error("FinOps", $"Failed to query {server.DisplayName}: {ex.Message}");
                    return (ServerPropertyRow?)null;
                }
            });

            var results = await System.Threading.Tasks.Task.WhenAll(tasks);
            if (_loads.Superseded(nameof(LoadServerInventoryAsync), gen)) return;
            var data = results.Where(r => r != null).Cast<ServerPropertyRow>().ToList();

            // Compute health scores for each server
            foreach (var item in data)
            {
                var cpuScore = FinOpsHealthCalculator.CpuScore(item.AvgCpuPct ?? 0m);
                var memScore = 80; // Default — we don't have buffer pool ratio in inventory
                var storScore = FinOpsHealthCalculator.StorageScore(50); // Default — no file-level free space in inventory
                item.HealthScore = FinOpsHealthCalculator.Overall(cpuScore, memScore, storScore);
            }

            _serverInventoryCache = data;
            _serverInventoryCacheTime = DateTime.Now;

            _serverInventoryFilterMgr!.UpdateData(data);
            NoServerInventoryMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            ServerInventoryCountIndicator.Text = data.Count > 0 ? $"{data.Count} server(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load server inventory: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadStorageGrowthAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadStorageGrowthAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetStorageGrowthAsync(serverId));
            if (_loads.Superseded(nameof(LoadStorageGrowthAsync), gen)) return;
            _storageGrowthFilterMgr!.UpdateData(data);
            NoStorageGrowthMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            StorageGrowthCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load storage growth: {ex.Message}");
        }
    }

    // ============================================
    // Object/Index stats (#1103) — the standalone Object Sizes & Index Usage loaders were removed in
    // #1138; that data is now the Storage Growth -> object -> index drill (FinOpsTab.ObjectHeatmap.cs).
    // The read methods GetObjectSizeGrowthAsync / GetIndexUsageAsync remain on LocalDataService for MCP.
    // ============================================

    // LoadIndexLockingAsync (the #1138 color-scaled grid + DB selector + index drill) lives in
    // FinOpsTab.Locking.cs.

    private async void RefreshIndexLocking_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadIndexLockingAsync(serverId);
    }

    private async System.Threading.Tasks.Task LoadIdleDatabasesAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadIdleDatabasesAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetIdleDatabasesAsync(serverId));
            if (_loads.Superseded(nameof(LoadIdleDatabasesAsync), gen)) return;
            _idleDbsFilterMgr!.UpdateData(data);
            IdleDatabasesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            IdleDatabasesCountIndicator.Text = data.Count > 0 ? $"{data.Count} idle database(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load idle databases: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadTempdbSummaryAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadTempdbSummaryAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetTempdbSummaryAsync(serverId));
            if (_loads.Superseded(nameof(LoadTempdbSummaryAsync), gen)) return;
            _tempdbFilterMgr!.UpdateData(data);
            TempdbPressureNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load tempdb summary: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadHighImpactQueriesAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadHighImpactQueriesAsync));

        try
        {
            var hoursBack = HoursBackFromIndex(HighImpactTimeRangeCombo);
            var data = await Task.Run(() => _dataService.GetHighImpactQueriesAsync(serverId, hoursBack));
            if (_loads.Superseded(nameof(LoadHighImpactQueriesAsync), gen)) return;
            _highImpactFilterMgr!.UpdateData(data);
            HighImpactNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            HighImpactCountIndicator.Text = data.Count > 0 ? $"{data.Count} high-impact query(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load high-impact queries: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadWaitCategorySummaryAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadWaitCategorySummaryAsync));

        try
        {
            var hoursBack = HoursBackFromIndex(WaitStatsTimeRangeCombo);
            var data = await Task.Run(() => _dataService.GetWaitCategorySummaryAsync(serverId, hoursBack));
            if (_loads.Superseded(nameof(LoadWaitCategorySummaryAsync), gen)) return;

            // Compute proportional cost shares — scaled to time window
            if (_currentServerMonthlyCost > 0 && data.Count > 0)
            {
                var windowBudget = _currentServerMonthlyCost * (hoursBack / 730.0m);
                var totalWait = data.Sum(w => w.TotalWaitTimeMs);
                if (totalWait > 0)
                {
                    foreach (var w in data)
                        w.MonthlyCostShare = (w.TotalWaitTimeMs / (decimal)totalWait) * windowBudget;
                }
            }

            _waitCategoryFilterMgr!.UpdateData(data);
            WaitCategorySummaryNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load wait category summary: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadExpensiveQueriesAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadExpensiveQueriesAsync));

        try
        {
            var hoursBack = HoursBackFromIndex(ExpensiveQueriesTimeRangeCombo);
            var data = await Task.Run(() => _dataService.GetExpensiveQueriesAsync(serverId, hoursBack));
            if (_loads.Superseded(nameof(LoadExpensiveQueriesAsync), gen)) return;

            // Compute proportional cost shares — scaled to time window
            if (_currentServerMonthlyCost > 0 && data.Count > 0)
            {
                var windowBudget = _currentServerMonthlyCost * (hoursBack / 730.0m);
                var totalCpu = data.Sum(q => q.TotalCpuMs);
                if (totalCpu > 0)
                {
                    foreach (var q in data)
                        q.MonthlyCostShare = (q.TotalCpuMs / (decimal)totalCpu) * windowBudget;
                }
            }

            _expensiveQueriesFilterMgr!.UpdateData(data);
            ExpensiveQueriesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            ExpensiveQueriesCountIndicator.Text = data.Count > 0 ? $"{data.Count} query(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load expensive queries: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadMemoryGrantEfficiencyAsync(int serverId)
    {
        if (_dataService == null) return;
        var gen = _loads.Claim(nameof(LoadMemoryGrantEfficiencyAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetMemoryGrantEfficiencyAsync(serverId));
            if (_loads.Superseded(nameof(LoadMemoryGrantEfficiencyAsync), gen)) return;
            _memoryGrantFilterMgr!.UpdateData(data);
            MemoryGrantEfficiencyNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load memory grant efficiency: {ex.Message}");
        }
    }

    #endregion

    #region Event Handlers

    private async void ServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_populatingServers) return; // same-server list repopulation, not a switch — see RefreshServerList

        ResetStorageDrill(); // a new server invalidates any open object/index drill

        /* #2306: column filters belong to the previous server too — same mechanism as Darling's FinOps
           tab: a filter set against server A silently zeroes server B's grid while the count indicators
           (computed from the unfiltered list) stay full, and Refresh cannot clear it. Cleared via the
           map every FinOps manager registers into, so a new grid inherits this without a second edit. */
        foreach (var manager in _filterManagers.Values)
        {
            manager.ClearFilters();
        }

        await LoadPerServerDataAsync();
    }

    private async void RefreshRecommendations_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadRecommendationsAsync(serverId);
    }

    private async void RefreshUtilization_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadUtilizationAsync(serverId);
    }

    private async void RefreshDatabaseResources_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadDatabaseResourcesAsync(serverId);
    }

    private async void RefreshApplicationConnections_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadApplicationConnectionsAsync(serverId);
    }

    private async void RefreshDatabaseSizes_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadDatabaseSizesAsync(serverId);
    }

    private async void RefreshPvsStats_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadPvsStatsAsync(serverId);
    }

    private async void RefreshServerInventory_Click(object sender, RoutedEventArgs e)
    {
        await LoadServerInventoryAsync(forceRefresh: true);
    }

    private async void RefreshStorageGrowth_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        // Refresh the view the user is actually looking at (#1138 drill), not just the parent grid.
        switch (_storageLevel)
        {
            case StorageDrillLevel.Objects when !string.IsNullOrEmpty(_objDrillDb):
                await LoadObjectGrowthAsync(serverId, _objDrillDb);
                break;
            case StorageDrillLevel.Indexes when !string.IsNullOrEmpty(_objDrillTable):
                await LoadObjectIndexDetailAsync(serverId, _objDrillDb, _objDrillSchema, _objDrillTable);
                break;
            default:
                await LoadStorageGrowthAsync(serverId);
                break;
        }
    }

    private async void WaitStatsTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _dataService == null) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        await LoadWaitCategorySummaryAsync(serverId);
    }

    private async void ExpensiveQueriesTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _dataService == null) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        await LoadExpensiveQueriesAsync(serverId);
    }

    private async void RefreshHighImpact_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadHighImpactQueriesAsync(serverId);
    }

    private async void HighImpactTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _dataService == null) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        await LoadHighImpactQueriesAsync(serverId);
    }

    private async void OptimizationRefresh_Click(object sender, RoutedEventArgs e)
    {
        using var _profiler = Helpers.MethodProfiler.StartTiming("FinOps-OptimizationRefresh");
        var serverId = GetSelectedServerId();
        if (serverId == 0 || _dataService == null) return;

        await System.Threading.Tasks.Task.WhenAll(
            LoadIdleDatabasesAsync(serverId),
            LoadTempdbSummaryAsync(serverId),
            LoadWaitCategorySummaryAsync(serverId),
            LoadExpensiveQueriesAsync(serverId),
            LoadMemoryGrantEfficiencyAsync(serverId)
        );
    }

    private async void RunIndexAnalysis_Click(object sender, RoutedEventArgs e)
    {
        using var _profiler = Helpers.MethodProfiler.StartTiming("FinOps-IndexAnalysis");
        if (_serverManager == null || _credentialResolver == null) return;

        var server = ServerSelector.SelectedItem as ServerConnection;
        if (server == null) return;

        var gen = _loads.Claim(nameof(RunIndexAnalysis_Click));

        try
        {
            var databaseNameEarly = IndexAnalysisDatabaseInput.Text?.Trim();
            var allDatabasesEarly = IndexAnalysisAllDatabases.IsChecked == true;

            /* #2407: Azure SQL Database has no cross-database execution, so the Utility DB idea — install
               sp_IndexCleanup once and point it at any database on the server — cannot work there. The proc
               runs INSIDE whichever database the connection opened, and @database_name asks it to read
               another one, which Azure refuses. Reported as "set Utility DB to db1, analysing db1 works,
               analysing db2 says no valid database" — the proc's own message, which reads like the database
               is missing rather than unreachable.

               So on Azure the connection targets the database being ANALYSED, not the utility database: the
               proc has to be installed in each database anyway (which is what the reporter found by
               experiment), and pointing at the target is the only shape that can work. */
            var properties = _dataService == null
                ? null
                : await _dataService.GetLatestServerPropertiesAsync(GetSelectedServerId());
            if (_loads.Superseded(nameof(RunIndexAnalysis_Click), gen)) return;
            var isAzureSqlDb = properties?.EngineEdition == 5;

            if (isAzureSqlDb && allDatabasesEarly)
            {
                /* Enumerating every database from one connection is the same cross-database read, so All
                   Databases cannot work on Azure either — and failing per-database would half-fill the grid
                   with whichever database the connection happened to open. */
                IndexAnalysisStatusText.Text =
                    "Azure SQL Database cannot analyse across databases — clear \u201CAll Databases\u201D and name one, "
                    + "with sp_IndexCleanup installed in it.";
                return;
            }

            var utilityConnectionString = isAzureSqlDb && !string.IsNullOrWhiteSpace(databaseNameEarly)
                ? _credentialResolver.GetConnectionStringForDatabase(server, databaseNameEarly!)
                : _credentialResolver.GetUtilityConnectionString(server);

            var exists = await LocalDataService.CheckSpIndexCleanupExistsAsync(utilityConnectionString);
            if (_loads.Superseded(nameof(RunIndexAnalysis_Click), gen)) return;
            if (!exists)
            {
                /* On Azure the proc must live in the target database, so name it — "not installed" against a
                   server with 50 databases is not actionable without saying which one was checked. */
                if (isAzureSqlDb && !string.IsNullOrWhiteSpace(databaseNameEarly))
                {
                    IndexAnalysisStatusText.Text =
                        $"sp_IndexCleanup is not installed in [{databaseNameEarly}]. Azure SQL Database cannot run it "
                        + "from another database, so it must be installed in each database you analyse.";
                }

                IndexAnalysisNotInstalledMessage.Visibility = Visibility.Visible;
                IndexAnalysisNoDataMessage.Visibility = Visibility.Collapsed;
                _indexSummaryFilterMgr!.UpdateData(new List<IndexCleanupSummaryRow>());
                _indexDetailFilterMgr!.UpdateData(new List<IndexCleanupResultRow>());
                return;
            }

            IndexAnalysisNotInstalledMessage.Visibility = Visibility.Collapsed;

            RunIndexAnalysisButton.IsEnabled = false;
            IndexAnalysisStatusText.Text = "Running analysis...";

            var databaseName = databaseNameEarly;
            var getAllDatabases = allDatabasesEarly;

            var (details, summaries) = await LocalDataService.RunIndexAnalysisAsync(
                utilityConnectionString,
                string.IsNullOrWhiteSpace(databaseName) ? null : databaseName,
                getAllDatabases);
            if (_loads.Superseded(nameof(RunIndexAnalysis_Click), gen)) return;

            _indexSummaryFilterMgr!.UpdateData(summaries);
            _indexDetailFilterMgr!.UpdateData(details);
            IndexAnalysisNoDataMessage.Visibility = details.Count == 0 && summaries.Count == 0
                ? Visibility.Visible : Visibility.Collapsed;
            IndexAnalysisStatusText.Text = details.Count > 0
                ? $"{details.Count} index(es) found"
                : "Analysis complete — no index issues found";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to run index analysis: {ex.Message}");
            if (_loads.Superseded(nameof(RunIndexAnalysis_Click), gen)) return;
            IndexAnalysisStatusText.Text = $"Error: {ex.Message}";
        }
        finally
        {
            RunIndexAnalysisButton.IsEnabled = true;
        }
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    /* #2645: all four mark items, one handler — the mark rides on the menu item's Tag. */
    private void MarkRow_Click(object sender, RoutedEventArgs e) => DataGridRowMarks.OnMarkMenuItemClicked(sender);

    /* Rows are recycled as the grid scrolls, so the paint has to happen as each one is realised rather
       than once after marking. DataGridRowMarks.Apply clears an unmarked row explicitly for the same
       reason: a recycled container still carries the previous row's brush. */
    private void MarkedGrid_LoadingRow(object sender, DataGridRowEventArgs e) => DataGridRowMarks.Apply(e.Row);

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        // FinOps shares one handler across several grids, so the file-name prefix is chosen
        // from the originating grid's name (resolved the same way the shared exporter does).
        var grid = sender is MenuItem menuItem ? FindParentDataGrid(menuItem) : null;
        var prefix = grid?.Name switch
        {
            nameof(DatabaseSizesDataGrid) => "database_sizes",
            nameof(PvsStatsDataGrid) => "pvs_stats",
            nameof(ServerInventoryDataGrid) => "server_inventory",
            nameof(DatabaseResourcesDataGrid) => "database_resources",
            nameof(ApplicationConnectionsDataGrid) => "application_connections",
            _ => "finops_export"
        };
        DataGridExport.ExportToCsv(sender, prefix, App.CsvSeparator);
    }

    #endregion

    #region Column Filtering

    private void InitializeFilterManagers()
    {
        _dbResourcesFilterMgr = new DataGridFilterManager<DatabaseResourceUsageRow>(DatabaseResourcesDataGrid);
        _storageGrowthFilterMgr = new DataGridFilterManager<StorageGrowthRow>(StorageGrowthDataGrid);
        _dbSizesFilterMgr = new DataGridFilterManager<DatabaseSizeRow>(DatabaseSizesDataGrid);
        _pvsStatsFilterMgr = new DataGridFilterManager<PvsStatsRow>(PvsStatsDataGrid);
        _indexSummaryFilterMgr = new DataGridFilterManager<IndexCleanupSummaryRow>(IndexAnalysisSummaryGrid);
        _indexDetailFilterMgr = new DataGridFilterManager<IndexCleanupResultRow>(IndexAnalysisDetailGrid);
        _appConnectionsFilterMgr = new DataGridFilterManager<ApplicationConnectionRow>(ApplicationConnectionsDataGrid);
        _serverInventoryFilterMgr = new DataGridFilterManager<ServerPropertyRow>(ServerInventoryDataGrid);
        _highImpactFilterMgr = new DataGridFilterManager<HighImpactQueryRow>(HighImpactDataGrid);
        _idleDbsFilterMgr = new DataGridFilterManager<IdleDatabaseRow>(IdleDatabasesDataGrid);
        _tempdbFilterMgr = new DataGridFilterManager<TempdbSummaryRow>(TempdbPressureDataGrid);
        _waitCategoryFilterMgr = new DataGridFilterManager<WaitCategorySummaryRow>(WaitCategorySummaryDataGrid);
        _expensiveQueriesFilterMgr = new DataGridFilterManager<ExpensiveQueryRow>(ExpensiveQueriesDataGrid);
        _memoryGrantFilterMgr = new DataGridFilterManager<MemoryGrantEfficiencyRow>(MemoryGrantEfficiencyDataGrid);
        _indexLockingFilterMgr = new DataGridFilterManager<IndexLockingRow>(IndexLockingDataGrid);

        _filterManagers[DatabaseResourcesDataGrid] = _dbResourcesFilterMgr;
        _filterManagers[StorageGrowthDataGrid] = _storageGrowthFilterMgr;
        _filterManagers[DatabaseSizesDataGrid] = _dbSizesFilterMgr;
        _filterManagers[PvsStatsDataGrid] = _pvsStatsFilterMgr;
        _filterManagers[IndexAnalysisSummaryGrid] = _indexSummaryFilterMgr;
        _filterManagers[IndexAnalysisDetailGrid] = _indexDetailFilterMgr;
        _filterManagers[ApplicationConnectionsDataGrid] = _appConnectionsFilterMgr;
        _filterManagers[ServerInventoryDataGrid] = _serverInventoryFilterMgr;
        _filterManagers[HighImpactDataGrid] = _highImpactFilterMgr;
        _filterManagers[IdleDatabasesDataGrid] = _idleDbsFilterMgr;
        _filterManagers[TempdbPressureDataGrid] = _tempdbFilterMgr;
        _filterManagers[WaitCategorySummaryDataGrid] = _waitCategoryFilterMgr;
        _filterManagers[ExpensiveQueriesDataGrid] = _expensiveQueriesFilterMgr;
        _filterManagers[MemoryGrantEfficiencyDataGrid] = _memoryGrantFilterMgr;
        _filterManagers[IndexLockingDataGrid] = _indexLockingFilterMgr;
    }

    /* Host/apply plumbing lives in the shared Ui controller. Lazy (a field initializer can't reference the
       instance field _filterManagers); the XAML-wired FilterButton_Click forwards to it. */
    private ColumnFilterPopupController? _filterPopupControllerField;
    private ColumnFilterPopupController FilterPopupController => _filterPopupControllerField ??= new ColumnFilterPopupController(_filterManagers);

    private void FilterButton_Click(object sender, RoutedEventArgs e) => FilterPopupController.HandleFilterButtonClick(sender);

    #endregion
}
