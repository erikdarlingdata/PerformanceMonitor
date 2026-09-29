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

public partial class QueryStatsHistoryWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private readonly string _databaseName;
    private readonly string _queryHash;
    private readonly int _hoursBack;
    private readonly string? _connectionString;
    private readonly string? _queryText;
    private readonly PlanNavigationController _planActions;
    private List<QueryStatsHistoryRow> _historyData = new();
    private ChartHoverHelper? _chartHover;
    private DataGridFilterManager<QueryStatsHistoryRow>? _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;

    public QueryStatsHistoryWindow(LocalDataService dataService, int serverId, string databaseName, string queryHash, int hoursBack, string? queryText = null, string? connectionString = null)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _databaseName = databaseName;
        _queryHash = queryHash;
        _hoursBack = hoursBack;
        _queryText = queryText;
        _connectionString = connectionString;

        _planActions = new PlanNavigationController(
            this,
            async (xml, label, qt) => await PlanViewerWindow.ShowPlanAsync(
                this, xml, label, qt, await _dataService.GetServerMetadataForPlanAnalysisAsync(_serverId)),
            (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
                _connectionString ?? "", db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct,
                productName: "SQL Server Performance Monitor Lite"),
            "the monitored server");

        _filterManager = new DataGridFilterManager<QueryStatsHistoryRow>(HistoryDataGrid);
        DataGridFilterColumns.AddFilterButtons(HistoryDataGrid, Filter_Click);
        _filterManager.UpdateFilterButtonStyles();

        QueryIdentifierText.Text = $"Query Stats History: {queryHash} in [{databaseName}]";
        Loaded += async (_, _) => await LoadHistoryAsync();
        ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (s, e) => ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private async System.Threading.Tasks.Task LoadHistoryAsync()
    {
        try
        {
            _historyData = await _dataService.GetQueryStatsHistoryAsync(_serverId, _databaseName, _queryHash, _hoursBack);
            _filterManager!.UpdateData(_historyData);

            if (_historyData.Count > 0)
            {
                var totalExec = _historyData.Sum(r => r.DeltaExecutions);
                var totalCpu = _historyData.Sum(r => r.DeltaCpuMs);
                var first = Services.ServerTimeHelper.ToServerTime(_historyData.First().CollectionTime);
                var last = Services.ServerTimeHelper.ToServerTime(_historyData.Last().CollectionTime);
                SummaryText.Text = $"{_historyData.Count} samples from {first:MM/dd HH:mm} to {last:MM/dd HH:mm} | " +
                                   $"Total Executions: {totalExec:N0} | Total CPU: {totalCpu:N1} ms";
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

        HistoryChart.Plot.Clear();

        var selected = MetricSelector.SelectedItem as ComboBoxItem;
        var tag = selected?.Tag?.ToString() ?? "AvgCpuMs";
        var label = selected?.Content?.ToString() ?? "Avg CPU (ms)";

        var xs = _historyData.Select(r => Services.ServerTimeHelper.ToServerTime(r.CollectionTime).ToOADate()).ToArray();
        var ys = _historyData.Select(r => GetMetricValue(r, tag)).ToArray();

        var scatter = HistoryChart.Plot.Add.TimeSeries(xs, ys);
        scatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("MetricTrend"));
        ChartStyle.StyleScatter(scatter);
        scatter.LegendText = label;

        var unit = tag.Contains("Ms") ? "ms" : "";
        if (_chartHover == null)
            _chartHover = new ChartHoverHelper(HistoryChart, unit);
        else
            _chartHover.Unit = unit;
        _chartHover.Clear();
        _chartHover.Add(scatter, label);

        /* #1831: the DateChange variant routes labels through the shared formatter, which converts
           for the display mode — plain DateTimeTicksBottom() rendered raw server time. */
        HistoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ApplyTheme(HistoryChart);

        HistoryChart.Refresh();
    }

    private static double GetMetricValue(QueryStatsHistoryRow row, string tag) => tag switch
    {
        "AvgCpuMs" => row.AvgCpuMs,
        "AvgElapsedMs" => row.AvgElapsedMs,
        "AvgReads" => row.AvgReads,
        "DeltaExecutions" => row.DeltaExecutions,
        "DeltaCpuMs" => row.DeltaCpuMs,
        "DeltaLogicalReads" => row.DeltaLogicalReads,
        "DeltaSpills" => row.DeltaSpills,
        _ => row.AvgCpuMs
    };

    private void MetricSelector_SelectionChanged(object sender, SelectionChangedEventArgs e) { if (IsLoaded) UpdateChart(); }

    private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn) return;
        if (string.IsNullOrEmpty(_queryHash)) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            string? plan = null;
            var source = "collected data";

            // Try DuckDB first — plan may already be cached from collection
            try
            {
                plan = await _dataService.GetCachedQueryPlanAsync(_serverId, _queryHash);
            }
            catch
            {
                // DuckDB lookup failed, fall through to live server
            }

            // Fall back to live server if DuckDB didn't have it
            if (string.IsNullOrEmpty(plan) && !string.IsNullOrEmpty(_connectionString))
            {
                plan = await LocalDataService.FetchQueryPlanOnDemandAsync(_connectionString, _queryHash);
                source = "live server";
            }

            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show("No query plan found in collected data or the live plan cache for this query hash.", "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var dialog = new SaveFileDialog
            {
                Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
                DefaultExt = ".sqlplan",
                FileName = $"query_plan_{_queryHash}_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
            };

            if (dialog.ShowDialog() != true) return;
            File.WriteAllText(dialog.FileName, plan, Encoding.UTF8);
            btn.Content = $"Saved ({source})";
            return;
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            if (btn.Content is "...")
                btn.Content = "Download";
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
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "query_stats_history", App.CsvSeparator);

    private async System.Threading.Tasks.Task<string?> FetchPlanAsync()
    {
        if (string.IsNullOrEmpty(_queryHash)) return null;
        string? plan = null;
        try { plan = await _dataService.GetCachedQueryPlanAsync(_serverId, _queryHash); }
        catch { /* DuckDB lookup failed — fall through to the live server */ }
        if (string.IsNullOrEmpty(plan) && !string.IsNullOrEmpty(_connectionString))
            plan = await LocalDataService.FetchQueryPlanOnDemandAsync(_connectionString, _queryHash);
        return plan;
    }

    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
        => await _planActions.ViewPlanAsync(FetchPlanAsync, $"Est Plan - {_queryHash}", _queryText);

    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
        => await _planActions.GetActualPlanAsync(_queryText, _databaseName, $"Actual Plan - {_queryHash}");

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}

