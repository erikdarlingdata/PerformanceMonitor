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
using Microsoft.Win32;
using PerformanceMonitorLite.Services;
using ScottPlot;

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
    private List<ProcedureStatsHistoryRow> _historyData = new();

    public ProcedureHistoryWindow(LocalDataService dataService, int serverId, string databaseName, string schemaName, string objectName, int hoursBack, string? connectionString = null)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _databaseName = databaseName;
        _schemaName = schemaName;
        _objectName = objectName;
        _hoursBack = hoursBack;
        _connectionString = connectionString;

        var fullName = string.IsNullOrEmpty(schemaName) ? objectName : $"{schemaName}.{objectName}";
        ProcIdentifierText.Text = $"Procedure History: {fullName} in [{databaseName}]";
        Loaded += async (_, _) => await LoadHistoryAsync();
    }

    private async System.Threading.Tasks.Task LoadHistoryAsync()
    {
        try
        {
            _historyData = await _dataService.GetProcedureStatsHistoryAsync(_serverId, _databaseName, _schemaName, _objectName, _hoursBack);
            HistoryDataGrid.ItemsSource = _historyData;

            if (_historyData.Count > 0)
            {
                var totalExec = _historyData.Sum(r => r.DeltaExecutions);
                var totalCpu = _historyData.Sum(r => r.DeltaCpuMs);
                var first = _historyData.First().CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes);
                var last = _historyData.Last().CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes);
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

        var xs = _historyData.Select(r => r.CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes).ToOADate()).ToArray();
        var ys = _historyData.Select(r => GetMetricValue(r, tag)).ToArray();

        var scatter = HistoryChart.Plot.Add.Scatter(xs, ys);
        scatter.LineWidth = 2;
        scatter.MarkerSize = 5;
        scatter.Color = ScottPlot.Color.FromHex("#4FC3F7");
        scatter.LegendText = label;

        HistoryChart.Plot.Axes.DateTimeTicksBottom();
        ApplyDarkTheme(HistoryChart);

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
        _ => row.AvgCpuMs
    };

    private void MetricSelector_SelectionChanged(object sender, SelectionChangedEventArgs e) { if (IsLoaded) UpdateChart(); }

    private static void ApplyDarkTheme(ScottPlot.WPF.WpfPlot chart)
    {
        var darkBg = ScottPlot.Color.FromHex("#22252b");
        var darkerBg = ScottPlot.Color.FromHex("#111217");
        var text = ScottPlot.Color.FromHex("#9DA5B4");
        var grid = ScottPlot.Colors.White.WithAlpha(20);

        chart.Plot.FigureBackground.Color = darkBg;
        chart.Plot.DataBackground.Color = darkerBg;
        chart.Plot.Axes.Color(text);
        chart.Plot.Grid.MajorLineColor = grid;

        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = text;
    }

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

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
