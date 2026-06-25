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

        // ============================================
        // Refresh Button Handlers
        // ============================================

        private async void RecommendationsRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadRecommendationsAsync();
        }

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

        private async void ObjectSizeGrowthRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadObjectSizeGrowthAsync();
        }

        private async void IndexUsageRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadIndexUsageAsync();
        }

        private async void IndexLockingRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadIndexLockingAsync();
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

        private async void HighImpactRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadHighImpactQueriesAsync();
        }

        private async void HighImpactTimeRange_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _databaseService == null) return;
            await LoadHighImpactQueriesAsync();
        }

        private async void ApplicationConnectionsRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadApplicationConnectionsAsync();
        }

        private async void ServerInventoryRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadServerInventoryAsync(forceRefresh: true);
        }

        private async void ResourceUsageTimeRange_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _databaseService == null) return;
            await LoadDatabaseResourcesAsync();
        }

    }
}
