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
using Microsoft.Win32;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using ScottPlot;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitorLite.Windows;

public partial class QueryStoreHistoryWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private readonly string _databaseName;
    private readonly long _queryId;
    private readonly long _planId;
    private readonly int _hoursBack;
    private readonly string? _connectionString;
    private readonly string _queryText;
    private readonly PlanNavigationController _planActions;
    private List<QueryStoreHistoryRow> _historyData = new();
    private ChartHoverHelper? _chartHover;
    private ScottPlot.IPanel? _legendPanel;
    private DataGridFilterManager<QueryStoreHistoryRow>? _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;

    public QueryStoreHistoryWindow(LocalDataService dataService, int serverId, string databaseName, long queryId, long planId, string queryText, int hoursBack, string? connectionString = null)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _databaseName = databaseName;
        _queryId = queryId;
        _planId = planId;
        _hoursBack = hoursBack;
        _connectionString = connectionString;
        _queryText = queryText;

        _planActions = new PlanNavigationController(
            this,
            async (xml, label, qt) => await PlanViewerWindow.ShowPlanAsync(
                this, xml, label, qt, await _dataService.GetServerMetadataForPlanAnalysisAsync(_serverId)),
            (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
                _connectionString ?? "", db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct,
                productName: "SQL Server Performance Monitor Lite"),
            "the monitored server");

        _filterManager = new DataGridFilterManager<QueryStoreHistoryRow>(HistoryDataGrid);
        DataGridFilterColumns.AddFilterButtons(HistoryDataGrid, Filter_Click);
        _filterManager.UpdateFilterButtonStyles();

        var displayText = queryText.Length > 120 ? queryText[..120] + "..." : queryText;
        QueryIdentifierText.Text = $"Query Store History: Query {queryId} in [{databaseName}]";
        SummaryText.Text = displayText;
        Loaded += async (_, _) => await LoadHistoryAsync();
        ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (s, e) => ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private async System.Threading.Tasks.Task LoadHistoryAsync()
    {
        try
        {
            _historyData = await _dataService.GetQueryStoreHistoryAsync(_serverId, _databaseName, _queryId, _hoursBack);
            _filterManager!.UpdateData(_historyData);

            if (_historyData.Count > 0)
            {
                var totalExec = _historyData.Sum(r => r.ExecutionCount);
                var planCount = _historyData.Select(r => r.PlanId).Distinct().Count();
                var first = _historyData.First().CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes);
                var last = _historyData.Last().CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes);
                SummaryText.Text = $"{_historyData.Count} samples from {first:MM/dd HH:mm} to {last:MM/dd HH:mm} | " +
                                   $"Total Executions: {totalExec:N0} | " +
                                   (planCount > 1 ? $"{planCount} different plans" : "Single plan");
            }
            else
            {
                SummaryText.Text = "No history data found for this query in the selected time range.";
            }

            UpdateChart();
        }
        catch (Exception ex)
        {
            SummaryText.Text = $"Error loading history: {ex.Message}";
        }
    }

    private void UpdateChart()
    {
        if (_historyData == null || _historyData.Count == 0)
        {
            HistoryChart.Plot.Clear();
            HistoryChart.Refresh();
            return;
        }

        if (_legendPanel != null)
        {
            HistoryChart.Plot.Axes.Remove(_legendPanel);
            _legendPanel = null;
        }
        HistoryChart.Plot.Clear();

        var selected = MetricSelector.SelectedItem as ComboBoxItem;
        var tag = selected?.Tag?.ToString() ?? "AvgCpuTimeMs";
        var label = selected?.Content?.ToString() ?? "Avg CPU (ms)";

        var unit = tag.Contains("Ms") ? "ms" : "";
        if (_chartHover == null)
            _chartHover = new ChartHoverHelper(HistoryChart, unit);
        else
            _chartHover.Unit = unit;
        _chartHover.Clear();

        // One series per plan so plan switches/regressions are visible — same as the Dashboard drilldown.
        var planGroups = _historyData.GroupBy(r => r.PlanId).OrderBy(g => g.Key).ToList();
        var colors = ChartPalette.CyclingPalette.Select(ScottPlot.Color.FromHex).ToArray();

        int colorIndex = 0;
        foreach (var planGroup in planGroups)
        {
            var ordered = planGroup.OrderBy(r => r.CollectionTime).ToList();
            var xs = ordered.Select(r => r.CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes).ToOADate()).ToArray();
            var ys = ordered.Select(r => GetMetricValue(r, tag)).ToArray();

            var scatter = HistoryChart.Plot.Add.TimeSeries(xs, ys);
            scatter.Color = colors[colorIndex % colors.Length];
            ChartStyle.StyleScatter(scatter);
            var seriesLabel = planGroups.Count > 1 ? $"Plan {planGroup.Key}" : label;
            scatter.LegendText = seriesLabel;
            _chartHover.Add(scatter, seriesLabel);
            colorIndex++;
        }

        /* #1831: the DateChange variant routes labels through the shared formatter, which converts
           for the display mode — plain DateTimeTicksBottom() rendered raw server time. */
        HistoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
        if (planGroups.Count > 1)
        {
            _legendPanel = HistoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            HistoryChart.Plot.Legend.FontSize = 12;
        }
        ApplyTheme(HistoryChart);

        HistoryChart.Refresh();
    }

    private static double GetMetricValue(QueryStoreHistoryRow row, string tag) => tag switch
    {
        "AvgCpuTimeMs" => row.AvgCpuTimeMs,
        "AvgDurationMs" => row.AvgDurationMs,
        "AvgLogicalReads" => row.AvgLogicalReads,
        "AvgRowcount" => row.AvgRowcount,
        "ExecutionCount" => row.ExecutionCount,
        "TotalCpuMs" => row.TotalCpuMs,
        "TotalDurationMs" => row.TotalDurationMs,
        _ => row.AvgCpuTimeMs
    };

    private void MetricSelector_SelectionChanged(object sender, SelectionChangedEventArgs e) { if (IsLoaded) UpdateChart(); }

    private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn) return;
        // Rows can now span multiple plans — download the plan for THIS row, not the launching one.
        var rowPlanId = (btn.DataContext as QueryStoreHistoryRow)?.PlanId ?? _planId;
        if (string.IsNullOrEmpty(_connectionString) || string.IsNullOrEmpty(_databaseName) || rowPlanId == 0) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            var plan = await LocalDataService.FetchQueryStorePlanAsync(_connectionString, _databaseName, rowPlanId);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show("No query plan found in Query Store for this plan ID.", "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var dialog = new SaveFileDialog
            {
                Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
                DefaultExt = ".sqlplan",
                FileName = $"qs_plan_{_queryId}_{rowPlanId}_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
            };

            if (dialog.ShowDialog() != true) return;
            File.WriteAllText(dialog.FileName, plan, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            btn.Content = "Download Plan";
            btn.IsEnabled = true;
        }
    }

    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart) => ChartStyle.ApplyMinimalChartTheme(chart);

    private void OnThemeChanged(string _)
    {
        ApplyTheme(HistoryChart);
        HistoryChart.Refresh();
        _filterManager?.UpdateFilterButtonStyles();
    }

    #region Column Filter Popup

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

    private void Filter_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;
        if (_filterManager == null) return;

        EnsureFilterPopup();

        _filterPopupContent!.FilterApplied -= FilterPopup_FilterApplied;
        _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;
        _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
        _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

        _filterManager.Filters.TryGetValue(columnName, out var existingFilter);
        _filterPopupContent.Initialize(columnName, existingFilter);

        _filterPopup!.PlacementTarget = button;
        _filterPopup.IsOpen = true;
    }

    private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;
        _filterManager?.SetFilter(e.FilterState);
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;
    }

    #endregion

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "query_store_history", App.CsvSeparator);

    private long SelectedPlanId =>
        ((HistoryDataGrid.CurrentItem ?? HistoryDataGrid.SelectedItem) as QueryStoreHistoryRow)?.PlanId ?? _planId;

    private async System.Threading.Tasks.Task<string?> FetchPlanAsync(long planId)
    {
        if (string.IsNullOrEmpty(_connectionString) || planId == 0) return null;
        return await LocalDataService.FetchQueryStorePlanAsync(_connectionString, _databaseName, planId);
    }

    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        var planId = SelectedPlanId;
        await _planActions.ViewPlanAsync(() => FetchPlanAsync(planId), $"Est Plan - QS {_queryId}/{planId}", _queryText);
    }

    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
        => await _planActions.GetActualPlanAsync(_queryText, _databaseName, $"Actual Plan - QS {_queryId}");

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}

