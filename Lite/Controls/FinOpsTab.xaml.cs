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
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class FinOpsTab : UserControl
{
    private LocalDataService? _dataService;
    private ServerManager? _serverManager;
    private CredentialService? _credentialService;
    private List<ServerPropertyRow>? _serverInventoryCache;
    private DateTime _serverInventoryCacheTime;

    public FinOpsTab()
    {
        InitializeComponent();
    }

    /// <summary>
    /// Initializes the control with required dependencies.
    /// </summary>
    public void Initialize(LocalDataService dataService, ServerManager serverManager)
    {
        _dataService = dataService;
        _serverManager = serverManager;
        _credentialService = serverManager.CredentialService;

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
        ServerSelector.ItemsSource = servers;

        if (previousSelection != null)
        {
            var match = servers.FirstOrDefault(s => s.Id == previousSelection.Id);
            if (match != null)
            {
                ServerSelector.SelectedItem = match;
                return;
            }
        }

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
            return RemoteCollectorService.GetDeterministicHashCode(server.ServerName);
        return 0;
    }

    /// <summary>
    /// Refreshes all FinOps data.
    /// </summary>
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

        await LoadUtilizationAsync(serverId);
        await LoadDatabaseResourcesAsync(serverId);
        await LoadApplicationConnectionsAsync(serverId);
        await LoadDatabaseSizesAsync(serverId);
    }

    private async System.Threading.Tasks.Task LoadUtilizationAsync(int serverId)
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetUtilizationEfficiencyAsync(serverId);
            UpdateUtilizationSummary(data);
            NoUtilizationMessage.Visibility = data == null ? Visibility.Visible : Visibility.Collapsed;
            SummaryContent.Visibility = data == null ? Visibility.Collapsed : Visibility.Visible;

            if (data != null)
            {
                TopTotalGrid.ItemsSource = await _dataService.GetTopResourceConsumersByTotalAsync(serverId);
                TopAvgGrid.ItemsSource = await _dataService.GetTopResourceConsumersByAvgAsync(serverId);
                DbSizeChart.ItemsSource = await _dataService.GetDatabaseSizeSummaryAsync(serverId);
                ProvisioningTrendGrid.ItemsSource = await _dataService.GetProvisioningTrendAsync(serverId);
            }
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
            "UNDER_PROVISIONED" => data.P95CpuPct > 85
                ? $"CPU p95 is {data.P95CpuPct:N1}% (threshold: 85%). This server may need more CPU capacity."
                : $"Buffer pool uses {bpPct:N0}% of physical RAM and memory ratio is {data.MemoryRatio:N2} (threshold: 0.95). Memory pressure is high.",
            _ => ""
        };
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

    private int GetResourceUsageHoursBack()
    {
        return ResourceUsageTimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 24
        };
    }

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

        try
        {
            var hoursBack = GetResourceUsageHoursBack();
            var data = await _dataService.GetDatabaseResourceUsageAsync(serverId, hoursBack);
            DatabaseResourcesDataGrid.ItemsSource = data;
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

        try
        {
            var data = await _dataService.GetApplicationConnectionsAsync(serverId);
            ApplicationConnectionsDataGrid.ItemsSource = data;
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

        try
        {
            var data = await _dataService.GetDatabaseSizeLatestAsync(serverId);
            DatabaseSizesDataGrid.ItemsSource = data;

            NoDbSizesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            DbSizeCountIndicator.Text = data.Count > 0 ? $"{data.Count} file(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load database sizes: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadServerInventoryAsync(bool forceRefresh = false)
    {
        using var _profiler = Helpers.MethodProfiler.StartTiming("FinOps-ServerInventory");
        if (_dataService == null || _serverManager == null || _credentialService == null) return;

        // Use cache if available and less than 5 minutes old
        if (!forceRefresh && _serverInventoryCache != null
            && (DateTime.Now - _serverInventoryCacheTime).TotalMinutes < 5)
        {
            ServerInventoryDataGrid.ItemsSource = _serverInventoryCache;
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
                    var connStr = server.GetConnectionString(_credentialService);

                    // Step 1: Query live server properties
                    var item = await LocalDataService.GetServerPropertiesLiveAsync(connStr);
                    item.ServerName = server.DisplayName;

                    // Step 2: Get collected metrics from DuckDB
                    try
                    {
                        var serverId = RemoteCollectorService.GetDeterministicHashCode(server.ServerName);
                        var (avgCpu, storageGb, idleDbs, status) = await _dataService!.GetServerMetricsAsync(serverId);
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
            var data = results.Where(r => r != null).Cast<ServerPropertyRow>().ToList();

            _serverInventoryCache = data;
            _serverInventoryCacheTime = DateTime.Now;

            ServerInventoryDataGrid.ItemsSource = data;
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

        try
        {
            var data = await _dataService.GetStorageGrowthAsync(serverId);
            StorageGrowthDataGrid.ItemsSource = data;
            NoStorageGrowthMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            StorageGrowthCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load storage growth: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadIdleDatabasesAsync(int serverId)
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetIdleDatabasesAsync(serverId);
            IdleDatabasesDataGrid.ItemsSource = data;
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

        try
        {
            var data = await _dataService.GetTempdbSummaryAsync(serverId);
            TempdbPressureDataGrid.ItemsSource = data;
            TempdbPressureNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load tempdb summary: {ex.Message}");
        }
    }

    private int GetWaitStatsHoursBack()
    {
        return WaitStatsTimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 24
        };
    }

    private int GetExpensiveQueriesHoursBack()
    {
        return ExpensiveQueriesTimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 24
        };
    }

    private async System.Threading.Tasks.Task LoadWaitCategorySummaryAsync(int serverId)
    {
        if (_dataService == null) return;

        try
        {
            var hoursBack = GetWaitStatsHoursBack();
            var data = await _dataService.GetWaitCategorySummaryAsync(serverId, hoursBack);
            WaitCategorySummaryDataGrid.ItemsSource = data;
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

        try
        {
            var hoursBack = GetExpensiveQueriesHoursBack();
            var data = await _dataService.GetExpensiveQueriesAsync(serverId, hoursBack);
            ExpensiveQueriesDataGrid.ItemsSource = data;
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

        try
        {
            var data = await _dataService.GetMemoryGrantEfficiencyAsync(serverId);
            MemoryGrantEfficiencyDataGrid.ItemsSource = data;
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
        await LoadPerServerDataAsync();
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

    private async void RefreshServerInventory_Click(object sender, RoutedEventArgs e)
    {
        await LoadServerInventoryAsync(forceRefresh: true);
    }

    private async void RefreshStorageGrowth_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadStorageGrowthAsync(serverId);
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
        if (_serverManager == null || _credentialService == null) return;

        var server = ServerSelector.SelectedItem as ServerConnection;
        if (server == null) return;

        try
        {
            var connectionString = server.GetConnectionString(_credentialService);

            var exists = await LocalDataService.CheckSpIndexCleanupExistsAsync(connectionString);
            if (!exists)
            {
                IndexAnalysisNotInstalledMessage.Visibility = Visibility.Visible;
                IndexAnalysisNoDataMessage.Visibility = Visibility.Collapsed;
                IndexAnalysisSummaryGrid.ItemsSource = null;
                IndexAnalysisDetailGrid.ItemsSource = null;
                return;
            }

            IndexAnalysisNotInstalledMessage.Visibility = Visibility.Collapsed;

            RunIndexAnalysisButton.IsEnabled = false;
            IndexAnalysisStatusText.Text = "Running analysis...";

            var databaseName = IndexAnalysisDatabaseInput.Text?.Trim();
            var getAllDatabases = IndexAnalysisAllDatabases.IsChecked == true;

            var (details, summaries) = await LocalDataService.RunIndexAnalysisAsync(
                connectionString,
                string.IsNullOrWhiteSpace(databaseName) ? null : databaseName,
                getAllDatabases);

            IndexAnalysisSummaryGrid.ItemsSource = summaries;
            IndexAnalysisDetailGrid.ItemsSource = details;
            IndexAnalysisNoDataMessage.Visibility = details.Count == 0 && summaries.Count == 0
                ? Visibility.Visible : Visibility.Collapsed;
            IndexAnalysisStatusText.Text = details.Count > 0
                ? $"{details.Count} index(es) found"
                : "Analysis complete — no index issues found";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to run index analysis: {ex.Message}");
            IndexAnalysisStatusText.Text = $"Error: {ex.Message}";
        }
        finally
        {
            RunIndexAnalysisButton.IsEnabled = true;
        }
    }

    #endregion

    #region Context Menu Handlers

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

        foreach (var col in grid.Columns)
        {
            sb.Append(col.Header?.ToString() ?? "");
            sb.Append('\t');
        }
        sb.AppendLine();

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

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null || grid.Items.Count == 0) return;

        var prefix = grid.Name switch
        {
            nameof(DatabaseSizesDataGrid) => "database_sizes",
            nameof(ServerInventoryDataGrid) => "server_inventory",
            nameof(DatabaseResourcesDataGrid) => "database_resources",
            nameof(ApplicationConnectionsDataGrid) => "application_connections",
            _ => "finops_export"
        };

        var dialog = new SaveFileDialog
        {
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            DefaultExt = ".csv",
            FileName = $"{prefix}_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (dialog.ShowDialog() != true) return;

        var sb = new StringBuilder();

        var headers = new List<string>();
        foreach (var col in grid.Columns)
            headers.Add(CsvEscape(col.Header?.ToString() ?? ""));
        sb.AppendLine(string.Join(",", headers));

        foreach (var item in grid.Items)
        {
            var values = new List<string>();
            foreach (var col in grid.Columns)
                values.Add(CsvEscape(GetCellValue(col, item)));
            sb.AppendLine(string.Join(",", values));
        }

        try
        {
            File.WriteAllText(dialog.FileName, sb.ToString(), Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to export: {ex.Message}", "Export Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    #endregion

    #region Helpers

    private static DataGrid? FindParentDataGrid(MenuItem menuItem)
    {
        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        while (target != null && target is not DataGrid)
            target = VisualTreeHelper.GetParent(target) as FrameworkElement;
        return target as DataGrid;
    }

    private static string GetCellValue(DataGridColumn col, object item)
    {
        if (col is DataGridBoundColumn boundCol && boundCol.Binding is Binding binding)
        {
            var prop = item.GetType().GetProperty(binding.Path.Path);
            return prop?.GetValue(item)?.ToString() ?? "";
        }
        return "";
    }

    private static string CsvEscape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    #endregion
}
