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

public partial class ProcedureHistoryWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private readonly string _databaseName;
    private readonly string _schemaName;
    private readonly string _objectName;
    private readonly int _hoursBack;
    private readonly string? _connectionString;
    /// <summary>The zone the tab shows times in: the tick labels and the hover read it each time they draw, and the summary reads it when the history loads.</summary>
    private readonly Func<TimeZoneInfo> _displayZone;
    private readonly PlanNavigationController _planActions;
    private List<ProcedureStatsHistoryRow> _historyData = new();
    private ChartHoverHelper? _chartHover;
    private DataGridFilterManager<ProcedureStatsHistoryRow>? _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;

    public ProcedureHistoryWindow(LocalDataService dataService, int serverId, string databaseName, string schemaName, string objectName, int hoursBack, string? connectionString, Func<TimeZoneInfo> displayZone)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _databaseName = databaseName;
        _schemaName = schemaName;
        _objectName = objectName;
        _hoursBack = hoursBack;
        _connectionString = connectionString;
        _displayZone = displayZone;

        _planActions = new PlanNavigationController(
            this,
            async (xml, label, qt) => await PlanViewerWindow.ShowPlanAsync(
                this, xml, label, qt, await _dataService.GetServerMetadataForPlanAnalysisAsync(_serverId)),
            (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
                _connectionString ?? "", db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct,
                productName: "SQL Server Performance Monitor Lite"),
            "the monitored server");

        _filterManager = new DataGridFilterManager<ProcedureStatsHistoryRow>(HistoryDataGrid);
        DataGridFilterColumns.AddFilterButtons(HistoryDataGrid, Filter_Click);
        _filterManager.UpdateFilterButtonStyles();

        var fullName = string.IsNullOrEmpty(schemaName) ? objectName : $"{schemaName}.{objectName}";
        ProcIdentifierText.Text = $"Procedure History: {fullName} in [{databaseName}]";
        Loaded += async (_, _) => await LoadHistoryAsync();
        ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (s, e) => ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private async System.Threading.Tasks.Task LoadHistoryAsync()
    {
        try
        {
            _historyData = await _dataService.GetProcedureStatsHistoryAsync(_serverId, _databaseName, _schemaName, _objectName, _hoursBack);
            /* #4766: the grid words each row's times in this window's own zone, as the chart and the summary below do,
               not in whichever server's tab is selected when the row is drawn (this window stays open after another
               tab is selected). Set before the rows reach the grid. */
            foreach (var row in _historyData)
            {
                row.Zone = _displayZone;
            }

            _filterManager!.UpdateData(_historyData);

            if (_historyData.Count > 0)
            {
                var totalExec = _historyData.Sum(r => r.DeltaExecutions);
                var totalCpu = _historyData.Sum(r => r.DeltaCpuMs);
                var first = DisplayZone.ToDisplay(_historyData.First().CollectionTime, _displayZone());
                var last = DisplayZone.ToDisplay(_historyData.Last().CollectionTime, _displayZone());
                SummaryText.Text = $"{_historyData.Count} samples from {first:MM/dd HH:mm} to {last:MM/dd HH:mm} | " +
                                   $"Total Executions: {totalExec:N0} | Total CPU: {totalCpu:N1} ms";
            }
            else
            {
                SummaryText.Text = "No history data found for this procedure in the selected time range.";
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

        var xs = _historyData.Select(r => r.CollectionTime.ToOADate()).ToArray();
        var ys = _historyData.Select(r => GetMetricValue(r, tag)).ToArray();

        var scatter = HistoryChart.Plot.Add.TimeSeries(xs, ys);
        scatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("MetricTrend"));
        ChartStyle.StyleScatter(scatter);
        scatter.LegendText = label;

        var unit = tag.Contains("Ms") ? "ms" : "";
        if (_chartHover == null)
            _chartHover = new ChartHoverHelper(HistoryChart, unit, displayZone: _displayZone);
        else
            _chartHover.Unit = unit;
        _chartHover.Clear();
        _chartHover.Add(scatter, label);

        /* #4766: X is the collection time as stored, the naive-UTC instant. The display mode reaches only the
           text: the tick labels here, the hover, and the summary's first and last times. */
        HistoryChart.Plot.Axes.DateTimeTicksBottomUtc(_displayZone);
        ApplyTheme(HistoryChart);

        HistoryChart.Refresh();
    }

    private static double GetMetricValue(ProcedureStatsHistoryRow row, string tag) => tag switch
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

    private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn) return;
        if (string.IsNullOrEmpty(_connectionString) || string.IsNullOrEmpty(_objectName)) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            var plan = await LocalDataService.FetchProcedurePlanOnDemandAsync(_connectionString, _databaseName, _schemaName, _objectName);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show("No plan found in the plan cache for this procedure. The plan may have been evicted.", "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var dialog = new SaveFileDialog
            {
                Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
                DefaultExt = ".sqlplan",
                FileName = $"proc_plan_{_objectName}_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
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

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "procedure_history", App.CsvSeparator);

    // Procedures are View Plan only — re-executing a proc without parameter values is unsafe,
    // matching the main Procedure grid (no Get Actual case in its switch).
    private async System.Threading.Tasks.Task<string?> FetchPlanAsync()
    {
        if (string.IsNullOrEmpty(_connectionString) || string.IsNullOrEmpty(_objectName)) return null;
        return await LocalDataService.FetchProcedurePlanOnDemandAsync(_connectionString, _databaseName, _schemaName, _objectName);
    }

    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        var fullName = string.IsNullOrEmpty(_schemaName) ? _objectName : $"{_schemaName}.{_objectName}";
        await _planActions.ViewPlanAsync(FetchPlanAsync, $"Est Plan - {fullName}", queryText: null);
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}

