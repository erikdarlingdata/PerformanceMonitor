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
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class FinOpsContent : UserControl
    {
        private DatabaseService? _databaseService;
        private ServerManager? _serverManager;
        private CredentialService? _credentialService;
        private List<FinOpsServerInventory>? _serverInventoryCache;
        private DateTime _serverInventoryCacheTime;
        private decimal _currentServerMonthlyCost;

        private DataGridFilterManager<FinOpsDatabaseSizeStats>? _dbSizesFilterMgr;
        private Popup? _dbSizeFilterPopup;
        private ColumnFilterPopup? _dbSizeFilterPopupContent;

        public FinOpsContent()
        {
            InitializeComponent();
            // Create the Database Sizes filter manager here, NOT in OnLoaded: Initialize() sets
            // ServerSelector.SelectedIndex = 0, which fires RefreshDataAsync -> LoadDatabaseSizesAsync,
            // and that can run before the Loaded event. If the manager isn't set yet, the loader
            // fetches the data then silently discards it at its null-guard (Database Sizes stays
            // empty until a manual refresh). Creating it here removes that init-order race.
            _dbSizesFilterMgr = new DataGridFilterManager<FinOpsDatabaseSizeStats>(DatabaseSizesDataGrid);
            Loaded += OnLoaded;
        }

        // ── Plan navigation for the query-identifying FinOps grids ──
        // Lazy: the controller's executeActual reads the current _databaseService (set on server-select),
        // and Window.GetWindow(this) is only valid once the control is in the visual tree.
        private PlanNavigationController? _planActions;
        private PlanNavigationController PlanActions => _planActions ??= new PlanNavigationController(
            Window.GetWindow(this)!,
            (xml, label, qt) => PlanViewerWindow.ShowPlanAsync(Window.GetWindow(this)!, xml, label, qt),
            (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
                _databaseService!.ConnectionString, db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct),
            "the monitored server");

        private async void FinOpsViewPlan_Click(object sender, RoutedEventArgs e)
        {
            if (_databaseService == null) return;
            if (GetFinOpsRow(sender) is FinOpsHighImpactQuery row)
                await PlanActions.ViewPlanAsync(
                    () => _databaseService.GetQueryStatsPlanXmlAsync(row.DatabaseName, row.QueryHashDisplay),
                    $"Est Plan - {row.QueryHashDisplay}", row.FullQueryText);
        }

        private async void FinOpsGetActualPlan_Click(object sender, RoutedEventArgs e)
        {
            if (_databaseService == null) return;
            switch (GetFinOpsRow(sender))
            {
                case FinOpsHighImpactQuery hi:
                    await PlanActions.GetActualPlanAsync(hi.FullQueryText, hi.DatabaseName, $"Actual Plan - {hi.QueryHashDisplay}");
                    break;
                case FinOpsExpensiveQuery ex:
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

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(RecommendationsDataGrid);
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
            TabHelpers.AutoSizeColumnMinWidths(HighImpactDataGrid);

            TabHelpers.FreezeColumns(RecommendationsDataGrid, 1);
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
            TabHelpers.FreezeColumns(HighImpactDataGrid, 1);
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
                _currentServerMonthlyCost = server.MonthlyCostUsd;
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
                // Re-read monthly cost from server manager in case user edited the server config
                if (ServerSelector.SelectedItem is ServerConnection selectedServer && _serverManager != null)
                {
                    var fresh = _serverManager.GetServerById(selectedServer.Id);
                    _currentServerMonthlyCost = fresh?.MonthlyCostUsd ?? selectedServer.MonthlyCostUsd;
                }

                await Task.WhenAll(
                    LoadRecommendationsAsync(),
                    LoadUtilizationAsync(),
                    LoadDatabaseResourcesAsync(),
                    LoadDatabaseSizesAsync(),
                    LoadApplicationConnectionsAsync(),
                    LoadServerInventoryAsync(),
                    LoadStorageGrowthAsync(),
                    LoadObjectSizeGrowthAsync(),
                    LoadIndexUsageAsync(),
                    LoadIndexLockingAsync(),
                    LoadIdleDatabasesAsync(),
                    LoadTempdbSummaryAsync(),
                    LoadWaitCategorySummaryAsync(),
                    LoadExpensiveQueriesAsync(),
                    LoadMemoryGrantEfficiencyAsync(),
                    LoadHighImpactQueriesAsync()
                );
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing FinOps data: {ex.Message}", ex);
            }
        }

    }
}
