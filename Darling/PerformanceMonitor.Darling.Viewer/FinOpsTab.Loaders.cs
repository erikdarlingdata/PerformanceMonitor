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
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The FinOps top-level aggregate tab — a COPY of Lite's cross-server FinOps tab
/// (<c>Controls/FinOpsTab.xaml.cs</c>), reads rewired to <see cref="ViewerDataService"/> Postgres. It restores
/// Lite's original shape: a top-level tab in the viewer shell (beside Overview / Recommendations / Alerts) with
/// its OWN server selector (independent of the sidebar, mirroring the Recommendations tab), NOT the earlier
/// per-server inner tab. All eleven sub-tabs are ported (Utilization, Database Resources, Storage Growth,
/// Locking &amp; Contention, Database Sizes, Optimization, High Impact, Application Connections, Server
/// Inventory, plus Index Analysis and Recommendations); Lite's two originally live-target sub-tabs (Index
/// Analysis / Recommendations) are reproduced MONITOR-SIDE over the collected store — Index Analysis (#1387)
/// reruns sp_IndexCleanup via the Common analyzer, and Recommendations runs its ~10 cost checks over collected
/// views — so neither touches a live target. The per-server sub-tabs scope to the selector's SELECTED server
/// (<c>_server.ServerId</c>); Server Inventory stays cross-server (it lists every registered server) and now
/// lives ONCE at the aggregate level instead of being duplicated inside every server's tab. FinOps COST
/// attribution is sourced from the registry (<c>servers.monthly_cost_usd</c> → <c>_server.MonthlyCostUsd</c>,
/// carried on the selected <see cref="DarlingServer"/>; 0 hides the cost affordances); the health score is
/// kept. This partial holds the sub-tab dispatch, the filter-manager wiring, and the simple grid loaders;
/// Locking lives in <c>FinOpsTab.Locking.cs</c>, the Storage Growth drill in <c>FinOpsTab.ObjectHeatmap.cs</c>,
/// Index Analysis in <c>FinOpsTab.IndexAnalysis.cs</c>, and Recommendations in
/// <c>FinOpsTab.Recommendations.cs</c>; the shell (server selector, filter plumbing, copy/export) lives in
/// <c>FinOpsTab.xaml.cs</c>.
/// </summary>
public partial class FinOpsTab
{
    /* FinOps sub-tab order (matches the task's port list; Lite's Recommendations + Index Analysis omitted). */
    private const int FinOpsUtilizationSubTabIndex = 0;
    private const int FinOpsDatabaseResourcesSubTabIndex = 1;
    private const int FinOpsStorageGrowthSubTabIndex = 2;
    private const int FinOpsLockingSubTabIndex = 3;
    private const int FinOpsDatabaseSizesSubTabIndex = 4;
    /* #1951 inserted Version Store (PVS) directly after Database Sizes, so every index below shifted by
       one. These are POSITIONAL — they must track the TabItem order in FinOpsTab.xaml, not the load order. */
    private const int FinOpsPvsStatsSubTabIndex = 5;
    private const int FinOpsOptimizationSubTabIndex = 6;
    private const int FinOpsHighImpactSubTabIndex = 7;
    private const int FinOpsApplicationConnectionsSubTabIndex = 8;
    private const int FinOpsServerInventorySubTabIndex = 9;
    private const int FinOpsIndexAnalysisSubTabIndex = 10;
    private const int FinOpsRecommendationsSubTabIndex = 11;

    private DataGridFilterManager<DatabaseResourceUsageRow>? _finopsDbResourcesFilterMgr;
    private DataGridFilterManager<StorageGrowthRow>? _finopsStorageGrowthFilterMgr;
    private DataGridFilterManager<DatabaseSizeRow>? _finopsDbSizesFilterMgr;
    private DataGridFilterManager<PvsStatsRow>? _finopsPvsStatsFilterMgr;
    private DataGridFilterManager<ApplicationConnectionRow>? _finopsAppConnectionsFilterMgr;
    private DataGridFilterManager<ServerPropertyRow>? _finopsServerInventoryFilterMgr;
    private DataGridFilterManager<HighImpactQueryRow>? _finopsHighImpactFilterMgr;
    private DataGridFilterManager<IdleDatabaseRow>? _finopsIdleDbsFilterMgr;
    private DataGridFilterManager<TempdbSummaryRow>? _finopsTempdbFilterMgr;
    private DataGridFilterManager<WaitCategorySummaryRow>? _finopsWaitCategoryFilterMgr;
    private DataGridFilterManager<ExpensiveQueryRow>? _finopsExpensiveQueriesFilterMgr;
    private DataGridFilterManager<MemoryGrantEfficiencyRow>? _finopsMemoryGrantFilterMgr;
    private DataGridFilterManager<IndexLockingRow>? _finopsIndexLockingFilterMgr;
    private DataGridFilterManager<IndexCleanupRollupRow>? _finopsIndexAnalysisRollupFilterMgr;
    private DataGridFilterManager<IndexCleanupRecommendationRow>? _finopsIndexAnalysisFilterMgr;
    private DataGridFilterManager<RecommendationRow>? _finopsRecommendationsFilterMgr;

    /// <summary>
    /// Registers the FinOps grids' column-filter managers into the shared <c>_filterManagers</c> map
    /// (defined in <c>ViewerServerTab.Filters.cs</c>). Called from the constructor after InitializeComponent,
    /// alongside the other Initialize* hooks, so the named grids exist.
    /// </summary>
    private void InitializeFinOpsTab()
    {
        _finopsDbResourcesFilterMgr = new DataGridFilterManager<DatabaseResourceUsageRow>(FinOpsDatabaseResourcesDataGrid);
        _finopsStorageGrowthFilterMgr = new DataGridFilterManager<StorageGrowthRow>(FinOpsStorageGrowthDataGrid);
        _finopsDbSizesFilterMgr = new DataGridFilterManager<DatabaseSizeRow>(FinOpsDatabaseSizesDataGrid);
        _finopsPvsStatsFilterMgr = new DataGridFilterManager<PvsStatsRow>(FinOpsPvsStatsDataGrid);
        _finopsAppConnectionsFilterMgr = new DataGridFilterManager<ApplicationConnectionRow>(FinOpsApplicationConnectionsDataGrid);
        _finopsServerInventoryFilterMgr = new DataGridFilterManager<ServerPropertyRow>(FinOpsServerInventoryDataGrid);
        _finopsHighImpactFilterMgr = new DataGridFilterManager<HighImpactQueryRow>(FinOpsHighImpactDataGrid);
        _finopsIdleDbsFilterMgr = new DataGridFilterManager<IdleDatabaseRow>(FinOpsIdleDatabasesDataGrid);
        _finopsTempdbFilterMgr = new DataGridFilterManager<TempdbSummaryRow>(FinOpsTempdbPressureDataGrid);
        _finopsWaitCategoryFilterMgr = new DataGridFilterManager<WaitCategorySummaryRow>(FinOpsWaitCategorySummaryDataGrid);
        _finopsExpensiveQueriesFilterMgr = new DataGridFilterManager<ExpensiveQueryRow>(FinOpsExpensiveQueriesDataGrid);
        _finopsMemoryGrantFilterMgr = new DataGridFilterManager<MemoryGrantEfficiencyRow>(FinOpsMemoryGrantEfficiencyDataGrid);
        _finopsIndexLockingFilterMgr = new DataGridFilterManager<IndexLockingRow>(FinOpsIndexLockingDataGrid);
        _finopsIndexAnalysisRollupFilterMgr = new DataGridFilterManager<IndexCleanupRollupRow>(FinOpsIndexAnalysisRollupGrid);
        _finopsIndexAnalysisFilterMgr = new DataGridFilterManager<IndexCleanupRecommendationRow>(FinOpsIndexAnalysisDetailGrid);
        _finopsRecommendationsFilterMgr = new DataGridFilterManager<RecommendationRow>(FinOpsRecommendationsDataGrid);

        _filterManagers[FinOpsDatabaseResourcesDataGrid] = _finopsDbResourcesFilterMgr;
        _filterManagers[FinOpsStorageGrowthDataGrid] = _finopsStorageGrowthFilterMgr;
        _filterManagers[FinOpsDatabaseSizesDataGrid] = _finopsDbSizesFilterMgr;
        _filterManagers[FinOpsPvsStatsDataGrid] = _finopsPvsStatsFilterMgr;
        _filterManagers[FinOpsApplicationConnectionsDataGrid] = _finopsAppConnectionsFilterMgr;
        _filterManagers[FinOpsServerInventoryDataGrid] = _finopsServerInventoryFilterMgr;
        _filterManagers[FinOpsHighImpactDataGrid] = _finopsHighImpactFilterMgr;
        _filterManagers[FinOpsIdleDatabasesDataGrid] = _finopsIdleDbsFilterMgr;
        _filterManagers[FinOpsTempdbPressureDataGrid] = _finopsTempdbFilterMgr;
        _filterManagers[FinOpsWaitCategorySummaryDataGrid] = _finopsWaitCategoryFilterMgr;
        _filterManagers[FinOpsExpensiveQueriesDataGrid] = _finopsExpensiveQueriesFilterMgr;
        _filterManagers[FinOpsMemoryGrantEfficiencyDataGrid] = _finopsMemoryGrantFilterMgr;
        _filterManagers[FinOpsIndexLockingDataGrid] = _finopsIndexLockingFilterMgr;
        _filterManagers[FinOpsIndexAnalysisRollupGrid] = _finopsIndexAnalysisRollupFilterMgr;
        _filterManagers[FinOpsIndexAnalysisDetailGrid] = _finopsIndexAnalysisFilterMgr;
        _filterManagers[FinOpsRecommendationsDataGrid] = _finopsRecommendationsFilterMgr;
    }

    /// <summary>
    /// A FinOps sub-tab switch reloads through this control's overlap-guarded
    /// <see cref="RefreshActiveSubTabAsync"/> (mirrors the per-server tab's Queries/File I/O sub-tab handlers).
    /// Gated on <see cref="FrameworkElement.IsLoaded"/> and the sub-TabControl's own selection so build-time and
    /// bubbled child selections are ignored.
    /// </summary>
    private async void FinOpsSubTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, FinOpsSubTabControl) || !IsLoaded)
        {
            return;
        }

        await RefreshActiveSubTabAsync();
    }

    /// <summary>
    /// Loads the FinOps tab's ACTIVE sub-tab only (Lite's visible-only rule). The shell's
    /// <see cref="LoadInnerTabAsync"/> owns the try/catch that surfaces failures on the status bar.
    /// </summary>
    private async Task LoadFinOpsAsync()
    {
        switch (FinOpsSubTabControl.SelectedIndex)
        {
            case FinOpsDatabaseResourcesSubTabIndex:
                await LoadFinOpsDatabaseResourcesAsync();
                break;
            case FinOpsStorageGrowthSubTabIndex:
                await LoadFinOpsStorageGrowthActiveAsync();
                break;
            case FinOpsLockingSubTabIndex:
                await LoadFinOpsIndexLockingAsync();
                break;
            case FinOpsDatabaseSizesSubTabIndex:
                await LoadFinOpsDatabaseSizesAsync();
                break;
            case FinOpsPvsStatsSubTabIndex:
                await LoadFinOpsPvsStatsAsync();
                break;
            case FinOpsOptimizationSubTabIndex:
                await LoadFinOpsOptimizationAsync();
                break;
            case FinOpsHighImpactSubTabIndex:
                await LoadFinOpsHighImpactAsync();
                break;
            case FinOpsApplicationConnectionsSubTabIndex:
                await LoadFinOpsApplicationConnectionsAsync();
                break;
            case FinOpsServerInventorySubTabIndex:
                await LoadFinOpsServerInventoryAsync();
                break;
            case FinOpsIndexAnalysisSubTabIndex:
                await LoadFinOpsIndexAnalysisAsync();
                break;
            case FinOpsRecommendationsSubTabIndex:
                await LoadFinOpsRecommendationsAsync();
                break;
            case FinOpsUtilizationSubTabIndex:
            default:
                await LoadFinOpsUtilizationAsync();
                break;
        }
    }

    // ── Utilization ──

    private async Task LoadFinOpsUtilizationAsync()
    {
        var data = await _dataService.GetUtilizationEfficiencyAsync(_server.ServerId);

        if (data != null)
        {
            /* Per-server FinOps budget (0 = hide the cost cards, like Lite). */
            data.MonthlyCost = _server.MonthlyCostUsd;

            /* Free space % for the storage health score, from the latest database sizes. */
            var dbSizes = await _dataService.GetDatabaseSizeLatestAsync(_server.ServerId);
            var totalStorageMb = dbSizes.Sum(d => d.TotalSizeMb);
            var totalFreeMb = dbSizes.Sum(d => d.FreeSpaceMb ?? 0m);
            data.FreeSpacePct = totalStorageMb > 0 ? totalFreeMb / totalStorageMb * 100m : 100m;
        }

        UpdateUtilizationSummary(data);
        FinOpsNoUtilizationMessage.Visibility = data == null ? Visibility.Visible : Visibility.Collapsed;
        FinOpsSummaryContent.Visibility = data == null ? Visibility.Collapsed : Visibility.Visible;

        if (data != null)
        {
            FinOpsTopTotalGrid.ItemsSource = await _dataService.GetTopResourceConsumersByTotalAsync(_server.ServerId);
            FinOpsTopAvgGrid.ItemsSource = await _dataService.GetTopResourceConsumersByAvgAsync(_server.ServerId);
            FinOpsDbSizeChart.ItemsSource = await _dataService.GetDatabaseSizeSummaryAsync(_server.ServerId);
            FinOpsProvisioningTrendGrid.ItemsSource = await _dataService.GetProvisioningTrendAsync(_server.ServerId);
        }
        else
        {
            FinOpsTopTotalGrid.ItemsSource = null;
            FinOpsTopAvgGrid.ItemsSource = null;
            FinOpsDbSizeChart.ItemsSource = null;
            FinOpsProvisioningTrendGrid.ItemsSource = null;
        }
    }

    private void UpdateUtilizationSummary(UtilizationEfficiencyRow? data)
    {
        if (data == null)
        {
            FinOpsProvisioningStatusText.Text = "No Data";
            FinOpsProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
            FinOpsAvgCpuText.Text = FinOpsP95CpuText.Text = FinOpsMaxCpuText.Text = FinOpsCpuSamplesText.Text = "-";
            FinOpsCpuCountText.Text = "-";
            FinOpsWorkerThreadsText.Text = "-";
            FinOpsAvgCpuBar.Width = FinOpsP95CpuBar.Width = FinOpsMaxCpuBar.Width = 0;
            FinOpsMemoryUtilBar.Width = FinOpsMemoryRatioBar.Width = 0;
            FinOpsMemoryUtilText.Text = FinOpsMemoryRatioText.Text = "-";
            FinOpsPhysicalMemoryText.Text = FinOpsTargetMemoryText.Text = FinOpsTotalMemoryText.Text = FinOpsBufferPoolText.Text = "-";
            FinOpsClassificationExplanation.Text = "";
            FinOpsUtilizationContent.Visibility = Visibility.Collapsed;
            FinOpsHealthScoreBorder.Visibility = Visibility.Collapsed;
            FinOpsComputeCostCard.Visibility = Visibility.Collapsed;
            FinOpsTotalCostCard.Visibility = Visibility.Collapsed;
            return;
        }

        FinOpsUtilizationContent.Visibility = Visibility.Visible;

        FinOpsProvisioningStatusText.Text = data.ProvisioningStatus.Replace("_", " ");
        switch (data.ProvisioningStatus)
        {
            case "RIGHT_SIZED":
                FinOpsProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#27AE60"));
                FinOpsProvisioningStatusText.Foreground = Brushes.White;
                break;
            case "OVER_PROVISIONED":
                FinOpsProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#F39C12"));
                FinOpsProvisioningStatusText.Foreground = Brushes.Black;
                break;
            case "UNDER_PROVISIONED":
                FinOpsProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E74C3C"));
                FinOpsProvisioningStatusText.Foreground = Brushes.White;
                break;
            default:
                FinOpsProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                FinOpsProvisioningStatusText.Foreground = Brushes.White;
                break;
        }

        /* CPU text + bars */
        FinOpsAvgCpuText.Text = $"{data.AvgCpuPct:N2}%";
        FinOpsP95CpuText.Text = $"{data.P95CpuPct:N2}%";
        FinOpsMaxCpuText.Text = $"{data.MaxCpuPct}%";
        FinOpsCpuSamplesText.Text = data.CpuSamples.ToString("N0");
        FinOpsCpuCountText.Text = data.CpuCount.ToString("N0");
        FinOpsWorkerThreadsText.Text = $"{data.CurrentWorkersCount:N0} / {data.MaxWorkersCount:N0}";

        SetBar(FinOpsAvgCpuBar, FinOpsAvgCpuFilled, FinOpsAvgCpuEmpty, (double)data.AvgCpuPct);
        SetBar(FinOpsP95CpuBar, FinOpsP95CpuFilled, FinOpsP95CpuEmpty, (double)data.P95CpuPct);
        SetBar(FinOpsMaxCpuBar, FinOpsMaxCpuFilled, FinOpsMaxCpuEmpty, data.MaxCpuPct);

        /* Stolen Memory % = (Total Server Memory - Buffer Pool) / Total Server Memory */
        var stolenPct = data.TotalMemoryMb > 0
            ? (double)(data.TotalMemoryMb - data.BufferPoolMb) / data.TotalMemoryMb * 100.0
            : 0;
        FinOpsMemoryUtilText.Text = $"{stolenPct:N0}%";
        SetBar(FinOpsMemoryUtilBar, FinOpsMemUtilFilled, FinOpsMemUtilEmpty, stolenPct);

        /* Buffer Pool % = Buffer Pool / Physical Memory */
        var bpPct = data.PhysicalMemoryMb > 0
            ? (double)data.BufferPoolMb / data.PhysicalMemoryMb * 100.0
            : 0;
        FinOpsMemoryRatioText.Text = $"{bpPct:N0}%";
        SetBar(FinOpsMemoryRatioBar, FinOpsMemRatioFilled, FinOpsMemRatioEmpty, bpPct);

        FinOpsPhysicalMemoryText.Text = $"{data.PhysicalMemoryMb:N0} MB";
        FinOpsTargetMemoryText.Text = $"{data.TargetMemoryMb:N0} MB";
        FinOpsTotalMemoryText.Text = $"{data.TotalMemoryMb:N0} MB";
        FinOpsBufferPoolText.Text = $"{data.BufferPoolMb:N0} MB";

        FinOpsClassificationExplanation.Text = data.ProvisioningStatus switch
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

        /* Cost summary cards — shown only when a monthly budget is configured (0 = hidden, like Lite). */
        if (data.MonthlyCost > 0)
        {
            FinOpsAnnualComputeCostText.Text = $"${data.MonthlyCost:N0}/mo";
            FinOpsAnnualTotalCostText.Text = $"${data.AnnualCost:N0}/yr";
            FinOpsComputeCostCard.Visibility = Visibility.Visible;
            FinOpsTotalCostCard.Visibility = Visibility.Visible;
        }
        else
        {
            FinOpsComputeCostCard.Visibility = Visibility.Collapsed;
            FinOpsTotalCostCard.Visibility = Visibility.Collapsed;
        }

        /* Health score */
        var bpRatio = data.PhysicalMemoryMb > 0 ? (decimal)data.BufferPoolMb / data.PhysicalMemoryMb : 0m;
        var cpuScore = FinOpsHealthCalculator.CpuScore(data.P95CpuPct);
        var memScore = FinOpsHealthCalculator.MemoryScore(bpRatio);
        var storScore = FinOpsHealthCalculator.StorageScore(data.FreeSpacePct);
        data.HealthScore = FinOpsHealthCalculator.Overall(cpuScore, memScore, storScore);
        FinOpsHealthScoreText.Text = $"Health: {data.HealthScore}";
        FinOpsHealthScoreBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString(data.HealthScoreColor));
        FinOpsHealthScoreBorder.Visibility = Visibility.Visible;
    }

    private static void SetBar(Border bar, ColumnDefinition filled, ColumnDefinition empty, double pct)
    {
        var clamped = Math.Max(0, Math.Min(100, pct));

        var color = clamped switch
        {
            > 85 => "#E74C3C",
            > 60 => "#F39C12",
            _ => "#27AE60"
        };
        bar.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString(color));

        filled.Width = new GridLength(Math.Max(clamped, 0.1), GridUnitType.Star);
        empty.Width = new GridLength(Math.Max(100 - clamped, 0.1), GridUnitType.Star);
    }

    private static int HoursBackFromIndex(ComboBox combo) => combo.SelectedIndex switch { 0 => 1, 1 => 4, 2 => 12, 3 => 24, 4 => 168, _ => 24 };

    // ── Database Resources ──

    private async Task LoadFinOpsDatabaseResourcesAsync()
    {
        var hoursBack = HoursBackFromIndex(FinOpsResourceUsageTimeRangeCombo);
        var data = await _dataService.GetDatabaseResourceUsageAsync(_server.ServerId, hoursBack);
        _finopsDbResourcesFilterMgr!.UpdateData(data);
        FinOpsNoDatabaseResourcesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsDbResourcesCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
    }

    // ── Database Sizes ──

    private async Task LoadFinOpsDatabaseSizesAsync()
    {
        var data = await _dataService.GetDatabaseSizeLatestAsync(_server.ServerId);

        /* Proportional cost share by size (mirrors Lite's LoadDatabaseSizesAsync). */
        if (_server.MonthlyCostUsd > 0 && data.Count > 0)
        {
            var totalMb = data.Sum(d => d.TotalSizeMb);
            if (totalMb > 0)
            {
                foreach (var d in data)
                    d.MonthlyCostShare = (d.TotalSizeMb / totalMb) * _server.MonthlyCostUsd;
            }
        }

        _finopsDbSizesFilterMgr!.UpdateData(data);
        FinOpsNoDbSizesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsDbSizeCountIndicator.Text = data.Count > 0 ? $"{data.Count} file(s)" : "";
    }

    // ── Version Store (PVS) ──

    private async Task LoadFinOpsPvsStatsAsync()
    {
        var data = await _dataService.GetPvsStatsLatestAsync(_server.ServerId);

        _finopsPvsStatsFilterMgr!.UpdateData(data);
        FinOpsNoPvsStatsMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsPvsCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";

        /* #1984 stage 2: the trend beside the grid — "when did it start growing" on the same axis
           family as Storage Growth. Top-5 databases by current PVS size, 7 days of hourly points. */
        var trend = await _dataService.GetPvsTrendAsync(_server.ServerId, DateTime.UtcNow.AddDays(-7));
        RenderPvsTrendChart(trend);
    }

    /// <summary>
    /// One line per database, legend labels carrying each database's LATEST %-of-database (the two
    /// numbers the proposal asked for on one chart rather than two stacked plots). Hidden entirely
    /// when there are no points — an ADR-less server gets no dead chart. Series colours rotate the
    /// shared chart palette by series index so a redraw is stable.
    /// </summary>
    private void RenderPvsTrendChart(List<PvsTrendPoint> trend)
    {
        if (trend.Count == 0)
        {
            FinOpsPvsTrendChart.Visibility = Visibility.Collapsed;
            return;
        }

        FinOpsPvsTrendChart.Visibility = Visibility.Visible;
        FinOpsPvsTrendChart.Plot.Clear();

        var seriesIndex = 0;
        foreach (var series in trend.GroupBy(t => t.DatabaseName).OrderByDescending(g => g.Max(t => t.PvsSizeMb)))
        {
            var points = series.OrderBy(t => t.CollectionTime).ToList();
            var times = points.Select(t => ViewerTimeHelper.ForDisplay(t.CollectionTime).ToOADate()).ToArray();
            var values = points.Select(t => t.PvsSizeMb).ToArray();

            var line = FinOpsPvsTrendChart.Plot.Add.TimeSeries(times, values);
            line.Color = ScottPlot.Color.FromHex(ChartPalette.CyclingColor(seriesIndex++));
            ChartStyle.StyleScatter(line);
            var latestPct = points[^1].PctOfDatabase;
            line.LegendText = latestPct is double pct
                ? $"{series.Key} ({pct:0.0}% of DB)"
                : series.Key;
        }

        FinOpsPvsTrendChart.Plot.Legend.IsVisible = true;
        FinOpsPvsTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        FinOpsPvsTrendChart.Plot.Axes.AutoScale();
        FinOpsPvsTrendChart.Plot.YLabel("PVS Off-Row MB");
        ChartStyle.ApplyThemeToChart(FinOpsPvsTrendChart);
        FinOpsPvsTrendChart.Refresh();
    }

    // ── Application Connections ──

    private async Task LoadFinOpsApplicationConnectionsAsync()
    {
        var data = await _dataService.GetApplicationConnectionsAsync(_server.ServerId);
        _finopsAppConnectionsFilterMgr!.UpdateData(data);
        FinOpsNoAppConnectionsMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsAppConnectionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} application(s)" : "";
    }

    // ── High Impact ──

    private async Task LoadFinOpsHighImpactAsync()
    {
        var hoursBack = HoursBackFromIndex(FinOpsHighImpactTimeRangeCombo);
        var data = await _dataService.GetHighImpactQueriesAsync(_server.ServerId, hoursBack);
        _finopsHighImpactFilterMgr!.UpdateData(data);
        FinOpsHighImpactNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsHighImpactCountIndicator.Text = data.Count > 0 ? $"{data.Count} high-impact query(s)" : "";
    }

    // ── Optimization (idle DBs + tempdb + wait categories + expensive queries + memory grants) ──

    private async Task LoadFinOpsOptimizationAsync()
    {
        using var readFanOut = ViewerReadFanOut.Of(5);

        await Task.WhenAll(
            LoadFinOpsIdleDatabasesAsync(),
            LoadFinOpsTempdbSummaryAsync(),
            LoadFinOpsWaitCategorySummaryAsync(),
            LoadFinOpsExpensiveQueriesAsync(),
            LoadFinOpsMemoryGrantEfficiencyAsync());
    }

    private async Task LoadFinOpsIdleDatabasesAsync()
    {
        var data = await _dataService.GetIdleDatabasesAsync(_server.ServerId);
        _finopsIdleDbsFilterMgr!.UpdateData(data);
        FinOpsIdleDatabasesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsIdleDatabasesCountIndicator.Text = data.Count > 0 ? $"{data.Count} idle database(s)" : "";
    }

    private async Task LoadFinOpsTempdbSummaryAsync()
    {
        var data = await _dataService.GetTempdbSummaryAsync(_server.ServerId);
        _finopsTempdbFilterMgr!.UpdateData(data);
        FinOpsTempdbPressureNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private async Task LoadFinOpsWaitCategorySummaryAsync()
    {
        var hoursBack = HoursBackFromIndex(FinOpsWaitStatsTimeRangeCombo);
        var data = await _dataService.GetWaitCategorySummaryAsync(_server.ServerId, hoursBack);

        /* Proportional cost share scaled to the window (mirrors Lite: budget * hoursBack/730). */
        if (_server.MonthlyCostUsd > 0 && data.Count > 0)
        {
            var windowBudget = _server.MonthlyCostUsd * (hoursBack / 730.0m);
            var totalWait = data.Sum(w => w.TotalWaitTimeMs);
            if (totalWait > 0)
            {
                foreach (var w in data)
                    w.MonthlyCostShare = (w.TotalWaitTimeMs / (decimal)totalWait) * windowBudget;
            }
        }

        _finopsWaitCategoryFilterMgr!.UpdateData(data);
        FinOpsWaitCategorySummaryNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private async Task LoadFinOpsExpensiveQueriesAsync()
    {
        var hoursBack = HoursBackFromIndex(FinOpsExpensiveQueriesTimeRangeCombo);
        var data = await _dataService.GetExpensiveQueriesAsync(_server.ServerId, hoursBack);

        /* Proportional cost share scaled to the window (mirrors Lite: budget * hoursBack/730). */
        if (_server.MonthlyCostUsd > 0 && data.Count > 0)
        {
            var windowBudget = _server.MonthlyCostUsd * (hoursBack / 730.0m);
            var totalCpu = data.Sum(q => q.TotalCpuMs);
            if (totalCpu > 0)
            {
                foreach (var q in data)
                    q.MonthlyCostShare = (q.TotalCpuMs / (decimal)totalCpu) * windowBudget;
            }
        }

        _finopsExpensiveQueriesFilterMgr!.UpdateData(data);
        FinOpsExpensiveQueriesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

        /* #1661: query text and plans live only in the raw tier, so this panel cannot answer a window wider than
           the raw horizon no matter what the picker offers. Say so rather than presenting a few days of data as
           though it were the month that was asked for — the aggregate FinOps panels DO reach further back via the
           rollups, so without this note the two would look inconsistent for no visible reason. */
        var now = DateTime.UtcNow;
        var (_, clamped) = RetentionTierRouter.ClampToTextHorizon(now, now.AddHours(-hoursBack));
        var coverageNote = clamped
            ? $" — showing the last {RetentionTierRouter.RawTextHorizon.TotalDays:N0} days; query text and plans are not retained beyond that"
            : "";

        FinOpsExpensiveQueriesCountIndicator.Text = data.Count > 0
            ? $"{data.Count} query(s){coverageNote}"
            : clamped ? coverageNote.TrimStart(' ', '—', ' ') : "";
    }

    private async Task LoadFinOpsMemoryGrantEfficiencyAsync()
    {
        var data = await _dataService.GetMemoryGrantEfficiencyAsync(_server.ServerId);
        _finopsMemoryGrantFilterMgr!.UpdateData(data);
        FinOpsMemoryGrantEfficiencyNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    // ── Server Inventory (cross-server) ──

    private async Task LoadFinOpsServerInventoryAsync()
    {
        var servers = await _dataService.GetServerInventoryAsync();

        /* Overlay each server's collected metrics + compute the health score (mirrors Lite's
           LoadServerInventoryAsync minus the live query). memScore/storScore use Lite's inventory-path
           defaults (buffer-pool ratio / file-level free space aren't in the inventory read). */
        /* Width is the inventory size, passed raw: the clamp to the pool ceiling belongs to
           ViewerReadFanOut, and a hand-capped guess here would be the copied-number mistake again. */
        using var readFanOut = ViewerReadFanOut.Of(servers.Count);

        await Task.WhenAll(servers.Select(async item =>
        {
            var (avgCpu, storageGb, idleDbs, status) = await _dataService.GetServerMetricsAsync(item.ServerId);
            if (avgCpu.HasValue) item.AvgCpuPct = avgCpu;
            if (storageGb.HasValue) item.StorageTotalGb = storageGb;
            if (idleDbs.HasValue) item.IdleDbCount = idleDbs;
            if (status != null) item.ProvisioningStatus = status;

            var cpuScore = FinOpsHealthCalculator.CpuScore(item.AvgCpuPct ?? 0m);
            var memScore = 80;
            var storScore = FinOpsHealthCalculator.StorageScore(50);
            item.HealthScore = FinOpsHealthCalculator.Overall(cpuScore, memScore, storScore);
        }));

        _finopsServerInventoryFilterMgr!.UpdateData(servers);
        FinOpsNoServerInventoryMessage.Visibility = servers.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsServerInventoryCountIndicator.Text = servers.Count > 0 ? $"{servers.Count} server(s)" : "";
    }

    // ── Refresh buttons + time-range combos (all route through the shell's overlap-guarded loop where they
    //    reload the active sub-tab, or call the specific loader directly for a non-active grid) ──

    private async void FinOpsRefreshUtilization_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsUtilizationAsync);
    private async void FinOpsRefreshDatabaseResources_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsDatabaseResourcesAsync);
    private async void FinOpsRefreshDatabaseSizes_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsDatabaseSizesAsync);

    private async void FinOpsRefreshPvsStats_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsPvsStatsAsync);
    private async void FinOpsRefreshApplicationConnections_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsApplicationConnectionsAsync);
    private async void FinOpsRefreshHighImpact_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsHighImpactAsync);
    private async void FinOpsRefreshServerInventory_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsServerInventoryAsync);
    private async void FinOpsOptimizationRefresh_Click(object sender, RoutedEventArgs e) => await RunFinOpsLoad(LoadFinOpsOptimizationAsync);

    private async void FinOpsResourceUsageTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        await RunFinOpsLoad(LoadFinOpsDatabaseResourcesAsync);
    }

    private async void FinOpsWaitStatsTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        await RunFinOpsLoad(LoadFinOpsWaitCategorySummaryAsync);
    }

    private async void FinOpsExpensiveQueriesTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        await RunFinOpsLoad(LoadFinOpsExpensiveQueriesAsync);
    }

    private async void FinOpsHighImpactTimeRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        await RunFinOpsLoad(LoadFinOpsHighImpactAsync);
    }

    /// <summary>
    /// "View Plan" for the FinOps High Impact + Expensive Queries grids: opens the stored
    /// <c>query_stats.query_plan_xml</c> the row already carries (Darling captures statement-level plans —
    /// CapturePlans defaults on) by raising <see cref="PlanRequested"/>, which the shell routes into the
    /// standalone Plan Viewer surface (this aggregate tab has no per-server plan host) — NO live SQL. Lite's
    /// "Get Actual Plan" (live execution) has no viewer equivalent and stays omitted everywhere, consistent
    /// with every other viewer grid.
    /// </summary>
    private void FinOpsViewPlan_Click(object sender, RoutedEventArgs e)
    {
        switch (FinOpsRowFromMenu(sender))
        {
            case HighImpactQueryRow hi when hi.HasQueryPlan:
                PlanRequested?.Invoke(hi.QueryPlanXml!, $"Plan - {hi.QueryHash}", hi.FullQueryText);
                break;
            case ExpensiveQueryRow ex when ex.HasQueryPlan:
                PlanRequested?.Invoke(ex.QueryPlanXml!, "Plan - Expensive Query", ex.FullQueryText);
                break;
            case HighImpactQueryRow:
            case ExpensiveQueryRow:
                MessageBox.Show(
                    "No execution plan was captured for this query. The plan may not have been collected yet, " +
                    "or it aged out of the store's retention window.",
                    "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
                break;
        }
    }

    /// <summary>
    /// "Get Actual Plan" for the FinOps High Impact grid (Dashboard parity) — RE-EXECUTES the row's query on the
    /// target (via the service's <c>execute_actual_plan</c> command) and routes the captured actual plan into the
    /// shell's Plan Viewer. Identifier-only: the service resolves the text from <c>query_stats</c> by (query_hash,
    /// database). ONLY High Impact rows carry a query_hash — the Expensive Queries grid groups by text, so its
    /// rows have no store key and the menu item stays disabled there (gated on <c>CanGetActualPlan</c>). The
    /// shared consent + data-modification flag + read-only guard + permission handling live in
    /// <see cref="ViewerActualPlanFlow"/>; the row's stored plan drives the modification detection.
    /// </summary>
    private async void FinOpsGetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        if (FinOpsRowFromMenu(sender) is not HighImpactQueryRow hi || string.IsNullOrEmpty(hi.QueryHash))
        {
            return;
        }

        var argsJson = ViewerDataService.BuildActualPlanArgs(hi.QueryHash, hi.DatabaseName);
        var label = $"Actual Plan - {hi.QueryHash}";

        var planXml = await ViewerActualPlanFlow.RequestActualPlanAsync(
            Window.GetWindow(this)!, _dataService, _server.ServerId, _server.DisplayName, hi.DatabaseName,
            hi.FullQueryText, hi.QueryPlanXml, argsJson,
            onStarted: () => System.Windows.Input.Mouse.OverrideCursor = System.Windows.Input.Cursors.Wait,
            onFinished: () => System.Windows.Input.Mouse.OverrideCursor = null,
            System.Threading.CancellationToken.None);

        if (planXml != null)
        {
            PlanRequested?.Invoke(planXml, label, hi.FullQueryText);
        }
    }

    /// <summary>Runs a FinOps loader with the same status-bar error surfacing the shell's <see cref="LoadInnerTabAsync"/> uses.</summary>
    private async Task RunFinOpsLoad(Func<Task> loader)
    {
        try
        {
            await loader();
            StatusChanged?.Invoke($"{_server.DisplayName} — refreshed {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"refresh failed: {ex.Message}");
        }
    }
}
