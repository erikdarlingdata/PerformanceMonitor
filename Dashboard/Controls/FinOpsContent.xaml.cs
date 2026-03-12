/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class FinOpsContent : UserControl
    {
        private DatabaseService? _databaseService;
        private ServerManager? _serverManager;
        private CredentialService? _credentialService;
        private List<FinOpsServerInventory>? _serverInventoryCache;
        private DateTime _serverInventoryCacheTime;

        public FinOpsContent()
        {
            InitializeComponent();
            Loaded += OnLoaded;
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(DatabaseResourcesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(DatabaseSizesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(ApplicationConnectionsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(ServerInventoryDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(TopTotalGrid);
            TabHelpers.AutoSizeColumnMinWidths(TopAvgGrid);
            TabHelpers.AutoSizeColumnMinWidths(StorageGrowthDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(IdleDatabasesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(TempdbPressureDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(WaitCategorySummaryDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(ExpensiveQueriesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(IndexAnalysisSummaryGrid);
            TabHelpers.AutoSizeColumnMinWidths(IndexAnalysisDetailGrid);

            TabHelpers.FreezeColumns(DatabaseResourcesDataGrid, 1);
            TabHelpers.FreezeColumns(DatabaseSizesDataGrid, 1);
            TabHelpers.FreezeColumns(ApplicationConnectionsDataGrid, 1);
            TabHelpers.FreezeColumns(ServerInventoryDataGrid, 1);
            TabHelpers.FreezeColumns(TopTotalGrid, 1);
            TabHelpers.FreezeColumns(TopAvgGrid, 1);
            TabHelpers.FreezeColumns(StorageGrowthDataGrid, 1);
            TabHelpers.FreezeColumns(IdleDatabasesDataGrid, 1);
            TabHelpers.FreezeColumns(WaitCategorySummaryDataGrid, 1);
            TabHelpers.FreezeColumns(ExpensiveQueriesDataGrid, 1);
            TabHelpers.FreezeColumns(IndexAnalysisDetailGrid, 1);
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(ServerManager serverManager, CredentialService credentialService)
        {
            _serverManager = serverManager ?? throw new ArgumentNullException(nameof(serverManager));
            _credentialService = credentialService ?? throw new ArgumentNullException(nameof(credentialService));

            var servers = _serverManager.GetAllServers();
            ServerSelector.ItemsSource = servers;
            if (servers.Count > 0)
                ServerSelector.SelectedIndex = 0;
        }

        private async void ServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (ServerSelector.SelectedItem is ServerConnection server && _credentialService != null)
            {
                var connectionString = server.GetConnectionString(_credentialService);
                _databaseService = new DatabaseService(connectionString);
                await RefreshDataAsync();
            }
        }

        /// <summary>
        /// Refreshes all FinOps data. Can be called from parent control.
        /// </summary>
        public async Task RefreshDataAsync()
        {
            try
            {
                await Task.WhenAll(
                    LoadUtilizationAsync(),
                    LoadDatabaseResourcesAsync(),
                    LoadDatabaseSizesAsync(),
                    LoadApplicationConnectionsAsync(),
                    LoadServerInventoryAsync(),
                    LoadStorageGrowthAsync(),
                    LoadIdleDatabasesAsync(),
                    LoadTempdbSummaryAsync(),
                    LoadWaitCategorySummaryAsync(),
                    LoadExpensiveQueriesAsync(),
                    LoadMemoryGrantEfficiencyAsync()
                );
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing FinOps data: {ex.Message}", ex);
            }
        }

        // ============================================
        // Utilization Tab
        // ============================================

        private async Task LoadUtilizationAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var efficiency = await _databaseService.GetFinOpsUtilizationEfficiencyAsync();
                UpdateUtilizationSummary(efficiency);
                NoUtilizationMessage.Visibility = efficiency == null ? Visibility.Visible : Visibility.Collapsed;
                SummaryContent.Visibility = efficiency == null ? Visibility.Collapsed : Visibility.Visible;

                if (efficiency != null)
                {
                    TopTotalGrid.ItemsSource = await _databaseService.GetFinOpsTopResourceConsumersByTotalAsync();
                    TopAvgGrid.ItemsSource = await _databaseService.GetFinOpsTopResourceConsumersByAvgAsync();
                    DbSizeChart.ItemsSource = await _databaseService.GetFinOpsDatabaseSizeSummaryAsync();
                    ProvisioningTrendGrid.ItemsSource = await _databaseService.GetFinOpsProvisioningTrendAsync();
                }
                else
                {
                    TopTotalGrid.ItemsSource = null;
                    TopAvgGrid.ItemsSource = null;
                    DbSizeChart.ItemsSource = null;
                    ProvisioningTrendGrid.ItemsSource = null;
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading utilization data: {ex.Message}", ex);
            }
        }

        private void UpdateUtilizationSummary(FinOpsUtilizationEfficiency? efficiency)
        {
            if (efficiency == null)
            {
                ProvisioningStatusText.Text = "No Data";
                ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                AvgCpuText.Text = P95CpuText.Text = MaxCpuText.Text = CpuSamplesText.Text = "-";
                AvgCpuBar.Width = P95CpuBar.Width = MaxCpuBar.Width = 0;
                MemoryUtilBar.Width = MemoryRatioBar.Width = 0;
                MemoryUtilText.Text = MemoryRatioText.Text = "-";
                PhysicalMemoryText.Text = TargetMemoryText.Text = TotalMemoryText.Text = BufferPoolText.Text = "-";
                WorkerThreadsText.Text = "-";
                CpuCountText.Text = "-";
                CpuSamplesText.Text = "-";
                ClassificationExplanation.Text = "";
                UtilizationContent.Visibility = Visibility.Collapsed;
                return;
            }

            UtilizationContent.Visibility = Visibility.Visible;

            // Provisioning status with color coding
            ProvisioningStatusText.Text = efficiency.ProvisioningStatus.Replace("_", " ");

            switch (efficiency.ProvisioningStatus)
            {
                case "RIGHT_SIZED":
                    ProvisioningStatusBorder.Background = (Brush)FindResource("SuccessBrush");
                    ProvisioningStatusText.Foreground = Brushes.White;
                    break;
                case "OVER_PROVISIONED":
                    ProvisioningStatusBorder.Background = (Brush)FindResource("WarningBrush");
                    ProvisioningStatusText.Foreground = Brushes.Black;
                    break;
                case "UNDER_PROVISIONED":
                    ProvisioningStatusBorder.Background = (Brush)FindResource("ErrorBrush");
                    ProvisioningStatusText.Foreground = Brushes.White;
                    break;
                default:
                    ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                    ProvisioningStatusText.Foreground = Brushes.White;
                    break;
            }

            /* CPU text + bars */
            AvgCpuText.Text = $"{efficiency.AvgCpuPct:N2}%";
            P95CpuText.Text = $"{efficiency.P95CpuPct:N2}%";
            MaxCpuText.Text = $"{efficiency.MaxCpuPct}%";
            CpuSamplesText.Text = efficiency.CpuSamples.ToString("N0");
            CpuCountText.Text = efficiency.CpuCount.ToString("N0");

            SetBar(AvgCpuBar, AvgCpuFilled, AvgCpuEmpty, (double)efficiency.AvgCpuPct);
            SetBar(P95CpuBar, P95CpuFilled, P95CpuEmpty, (double)efficiency.P95CpuPct);
            SetBar(MaxCpuBar, MaxCpuFilled, MaxCpuEmpty, efficiency.MaxCpuPct);

            /* Stolen Memory % = (Total Server Memory - Buffer Pool) / Total Server Memory
               Uses perfmon counter value (TotalServerMemoryMb) for parity with Lite */
            var tsm = efficiency.TotalServerMemoryMb > 0 ? efficiency.TotalServerMemoryMb : efficiency.TotalMemoryMb;
            var stolenPct = tsm > 0
                ? (double)(tsm - efficiency.BufferPoolMb) / tsm * 100.0
                : 0;
            MemoryUtilText.Text = $"{stolenPct:N0}%";
            SetBar(MemoryUtilBar, MemUtilFilled, MemUtilEmpty, stolenPct);

            /* Buffer Pool % = Buffer Pool / Physical Memory */
            var bpPct = efficiency.PhysicalMemoryMb > 0
                ? (double)efficiency.BufferPoolMb / efficiency.PhysicalMemoryMb * 100.0
                : 0;
            MemoryRatioText.Text = $"{bpPct:N0}%";
            SetBar(MemoryRatioBar, MemRatioFilled, MemRatioEmpty, bpPct);

            PhysicalMemoryText.Text = $"{efficiency.PhysicalMemoryMb:N0} MB";
            TargetMemoryText.Text = $"{efficiency.TargetMemoryMb:N0} MB";
            TotalMemoryText.Text = $"{tsm:N0} MB";
            BufferPoolText.Text = $"{efficiency.BufferPoolMb:N0} MB";
            WorkerThreadsText.Text = $"{efficiency.WorkerThreadsCurrent:N0} / {efficiency.WorkerThreadsMax:N0}";

            /* Contextual explanation — one sentence describing WHY this classification */
            ClassificationExplanation.Text = efficiency.ProvisioningStatus switch
            {
                "RIGHT_SIZED" => $"CPU is moderately loaded (avg {efficiency.AvgCpuPct:N1}%, p95 {efficiency.P95CpuPct:N1}%) and memory is well-utilized (buffer pool uses {bpPct:N0}% of physical RAM). No action needed.",
                "OVER_PROVISIONED" => $"CPU is lightly loaded (avg {efficiency.AvgCpuPct:N1}%, max {efficiency.MaxCpuPct}%) and buffer pool uses only {bpPct:N0}% of physical RAM. This server may have more resources than it needs.",
                "UNDER_PROVISIONED" => efficiency.P95CpuPct > 85
                    ? $"CPU p95 is {efficiency.P95CpuPct:N1}% (threshold: 85%). This server may need more CPU capacity."
                    : $"Buffer pool uses {bpPct:N0}% of physical RAM and memory ratio is {efficiency.MemoryRatio:N2} (threshold: 0.95). Memory pressure is high.",
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

        // ============================================
        // Database Resources Tab
        // ============================================

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

        private async void ResourceUsageTimeRange_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _databaseService == null) return;
            await LoadDatabaseResourcesAsync();
        }

        private async Task LoadDatabaseResourcesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var hoursBack = GetResourceUsageHoursBack();
                var data = await _databaseService.GetFinOpsDatabaseResourceUsageAsync(hoursBack);
                DatabaseResourcesDataGrid.ItemsSource = data;
                DatabaseResourcesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                DbResourcesCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database resources: {ex.Message}", ex);
            }
        }

        // ============================================
        // Storage Growth Tab
        // ============================================

        private async Task LoadStorageGrowthAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetFinOpsStorageGrowthAsync();
                StorageGrowthDataGrid.ItemsSource = data;
                StorageGrowthNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StorageGrowthCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading storage growth: {ex.Message}", ex);
            }
        }

        // ============================================
        // Optimization Tab — Idle Databases
        // ============================================

        private async Task LoadIdleDatabasesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetFinOpsIdleDatabasesAsync();
                IdleDatabasesDataGrid.ItemsSource = data;
                IdleDatabasesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                IdleDatabasesCountIndicator.Text = data.Count > 0 ? $"{data.Count} idle database(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading idle databases: {ex.Message}", ex);
            }
        }

        // ============================================
        // Optimization Tab — Tempdb Pressure
        // ============================================

        private async Task LoadTempdbSummaryAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetFinOpsTempdbSummaryAsync();
                TempdbPressureDataGrid.ItemsSource = data;
                TempdbPressureNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading tempdb summary: {ex.Message}", ex);
            }
        }

        // ============================================
        // Optimization Tab — Wait Stats Summary
        // ============================================

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

        private async Task LoadWaitCategorySummaryAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var hoursBack = GetWaitStatsHoursBack();
                var data = await _databaseService.GetFinOpsWaitCategorySummaryAsync(hoursBack);
                WaitCategorySummaryDataGrid.ItemsSource = data;
                WaitCategorySummaryNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading wait category summary: {ex.Message}", ex);
            }
        }

        // ============================================
        // Optimization Tab — Expensive Queries
        // ============================================

        private async Task LoadExpensiveQueriesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var hoursBack = GetExpensiveQueriesHoursBack();
                var data = await _databaseService.GetFinOpsExpensiveQueriesAsync(hoursBack);
                ExpensiveQueriesDataGrid.ItemsSource = data;
                ExpensiveQueriesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                ExpensiveQueriesCountIndicator.Text = data.Count > 0 ? $"{data.Count} query(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading expensive queries: {ex.Message}", ex);
            }
        }

        private async Task LoadMemoryGrantEfficiencyAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetFinOpsMemoryGrantEfficiencyAsync();
                MemoryGrantEfficiencyDataGrid.ItemsSource = data;
                MemoryGrantEfficiencyNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory grant efficiency: {ex.Message}", ex);
            }
        }

        // ============================================
        // Index Analysis Tab
        // ============================================

        private async void RunIndexAnalysis_Click(object sender, RoutedEventArgs e)
        {
            if (_databaseService == null) return;

            try
            {
                // Check if sp_IndexCleanup exists
                var exists = await _databaseService.CheckSpIndexCleanupExistsAsync();
                if (!exists)
                {
                    IndexAnalysisNotInstalledMessage.Visibility = Visibility.Visible;
                    IndexAnalysisNoDataMessage.Visibility = Visibility.Collapsed;
                    IndexAnalysisSummaryGrid.ItemsSource = null;
                    IndexAnalysisDetailGrid.ItemsSource = null;
                    return;
                }

                IndexAnalysisNotInstalledMessage.Visibility = Visibility.Collapsed;

                // Show busy state
                RunIndexAnalysisButton.IsEnabled = false;
                IndexAnalysisStatusText.Text = "Running analysis...";

                var databaseName = IndexAnalysisDatabaseInput.Text?.Trim();
                var getAllDatabases = IndexAnalysisAllDatabases.IsChecked == true;

                var (details, summaries) = await _databaseService.RunIndexAnalysisAsync(
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
                Logger.Error($"Error running index analysis: {ex.Message}", ex);
                IndexAnalysisStatusText.Text = $"Error: {ex.Message}";
            }
            finally
            {
                RunIndexAnalysisButton.IsEnabled = true;
            }
        }

        // ============================================
        // Database Sizes Tab
        // ============================================

        private async Task LoadDatabaseSizesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetFinOpsDatabaseSizeStatsAsync();
                DatabaseSizesDataGrid.ItemsSource = data;
                DatabaseSizesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                DbSizeCountIndicator.Text = data.Count > 0 ? $"{data.Count} file(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database sizes: {ex.Message}", ex);
            }
        }

        // ============================================
        // Application Connections Tab
        // ============================================

        private async Task LoadApplicationConnectionsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetFinOpsApplicationResourceUsageAsync();
                ApplicationConnectionsDataGrid.ItemsSource = data;
                ApplicationConnectionsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                AppConnectionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} application(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading application connections: {ex.Message}", ex);
            }
        }

        // ============================================
        // Server Inventory Tab
        // ============================================

        private async Task LoadServerInventoryAsync(bool forceRefresh = false)
        {
            if (_serverManager == null || _credentialService == null) return;

            // Use cache if available and less than 5 minutes old
            if (!forceRefresh && _serverInventoryCache != null
                && (DateTime.Now - _serverInventoryCacheTime).TotalMinutes < 5)
            {
                ServerInventoryDataGrid.ItemsSource = _serverInventoryCache;
                ServerInventoryNoDataMessage.Visibility = _serverInventoryCache.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
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

                        // Step 1: Query live server properties (works from any DB context)
                        var item = await DatabaseService.GetServerPropertiesLiveAsync(connStr);
                        item.ServerName = server.DisplayName;

                        // Step 2: Try to augment with collected metrics from PerformanceMonitor DB
                        try
                        {
                            var svc = new DatabaseService(connStr);
                            var (avgCpu, storageGb, idleDbs, status) = await svc.GetServerMetricsAsync();
                            if (avgCpu.HasValue) item.AvgCpuPct = avgCpu;
                            if (storageGb.HasValue) item.StorageTotalGb = storageGb;
                            if (idleDbs.HasValue) item.IdleDbCount = idleDbs;
                            if (status != null) item.ProvisioningStatus = status;
                        }
                        catch
                        {
                            // PerformanceMonitor DB may not exist or have no data — that's OK
                        }

                        return item;
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Error loading server inventory for {server.DisplayName}: {ex.Message}", ex);
                        return (FinOpsServerInventory?)null;
                    }
                });

                var results = await Task.WhenAll(tasks);
                var allItems = results.Where(r => r != null).Cast<FinOpsServerInventory>().ToList();

                _serverInventoryCache = allItems;
                _serverInventoryCacheTime = DateTime.Now;

                ServerInventoryDataGrid.ItemsSource = allItems;
                ServerInventoryNoDataMessage.Visibility = allItems.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                ServerInventoryCountIndicator.Text = allItems.Count > 0 ? $"{allItems.Count} server(s)" : "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server inventory: {ex.Message}", ex);
            }
        }

        // ============================================
        // Refresh Button Handlers
        // ============================================

        private async void UtilizationRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadUtilizationAsync();
        }

        private async void DatabaseResourcesRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDatabaseResourcesAsync();
        }

        private async void StorageGrowthRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadStorageGrowthAsync();
        }

        private async void WaitStatsTimeRange_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _databaseService == null) return;
            await LoadWaitCategorySummaryAsync();
        }

        private async void ExpensiveQueriesTimeRange_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _databaseService == null) return;
            await LoadExpensiveQueriesAsync();
        }

        private async void OptimizationRefresh_Click(object sender, RoutedEventArgs e)
        {
            await Task.WhenAll(
                LoadIdleDatabasesAsync(),
                LoadTempdbSummaryAsync(),
                LoadWaitCategorySummaryAsync(),
                LoadExpensiveQueriesAsync(),
                LoadMemoryGrantEfficiencyAsync()
            );
        }

        private async void DatabaseSizesRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDatabaseSizesAsync();
        }

        private async void ApplicationConnectionsRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadApplicationConnectionsAsync();
        }

        private async void ServerInventoryRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadServerInventoryAsync(forceRefresh: true);
        }

        // ============================================
        // Copy / Export Context Menu Handlers
        // ============================================

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.CurrentCell.Column != null)
                {
                    var cellContent = TabHelpers.GetCellContent(grid, grid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(grid, grid.SelectedItem);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(rowText, false);
                    }
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var sb = new StringBuilder();

                    // Header row
                    var headers = grid.Columns.Select(c => DataGridClipboardBehavior.GetHeaderText(c));
                    sb.AppendLine(string.Join("\t", headers));

                    // Data rows
                    foreach (var item in grid.Items)
                    {
                        var values = new List<string>();
                        foreach (var column in grid.Columns)
                        {
                            var binding = (column as DataGridBoundColumn)?.Binding as Binding;
                            if (binding != null)
                            {
                                var prop = item.GetType().GetProperty(binding.Path.Path);
                                var value = prop?.GetValue(item)?.ToString() ?? string.Empty;
                                values.Add(value);
                            }
                        }
                        sb.AppendLine(string.Join("\t", values));
                    }

                    if (sb.Length > 0)
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(sb.ToString(), false);
                    }
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var dialog = new SaveFileDialog
                    {
                        Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                        DefaultExt = ".csv",
                        FileName = $"FinOps_Export_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
                    };

                    if (dialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Header row
                            var sep = TabHelpers.CsvSeparator;
                            var headers = grid.Columns.Select(c => TabHelpers.EscapeCsvField(DataGridClipboardBehavior.GetHeaderText(c), sep));
                            sb.AppendLine(string.Join(sep, headers));

                            // Data rows
                            foreach (var item in grid.Items)
                            {
                                var values = new List<string>();
                                foreach (var column in grid.Columns)
                                {
                                    var binding = (column as DataGridBoundColumn)?.Binding as Binding;
                                    if (binding != null)
                                    {
                                        var prop = item.GetType().GetProperty(binding.Path.Path);
                                        values.Add(TabHelpers.EscapeCsvField(TabHelpers.FormatForExport(prop?.GetValue(item)), sep));
                                    }
                                }
                                sb.AppendLine(string.Join(sep, values));
                            }

                            File.WriteAllText(dialog.FileName, sb.ToString());
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Error exporting to CSV: {ex.Message}", ex);
                            MessageBox.Show($"Error exporting to CSV: {ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }
    }
}
