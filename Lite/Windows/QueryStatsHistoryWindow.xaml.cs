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

public partial class QueryStatsHistoryWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private readonly string _databaseName;
    private readonly string _queryHash;
    private readonly int _hoursBack;
    private readonly string? _connectionString;
    private List<QueryStatsHistoryRow> _historyData = new();

    public QueryStatsHistoryWindow(LocalDataService dataService, int serverId, string databaseName, string queryHash, int hoursBack, string? connectionString = null)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _databaseName = databaseName;
        _queryHash = queryHash;
        _hoursBack = hoursBack;
        _connectionString = connectionString;

        QueryIdentifierText.Text = $"Query Stats History: {queryHash} in [{databaseName}]";
        Loaded += async (_, _) => await LoadHistoryAsync();
        Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (s, e) => Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private async System.Threading.Tasks.Task LoadHistoryAsync()
    {
        try
        {
            _historyData = await _dataService.GetQueryStatsHistoryAsync(_serverId, _databaseName, _queryHash, _hoursBack);
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

        var xs = _historyData.Select(r => r.CollectionTime.AddMinutes(Services.ServerTimeHelper.UtcOffsetMinutes).ToOADate()).ToArray();
        var ys = _historyData.Select(r => GetMetricValue(r, tag)).ToArray();

        var scatter = HistoryChart.Plot.Add.Scatter(xs, ys);
        scatter.LineWidth = 2;
        scatter.MarkerSize = 5;
        scatter.Color = ScottPlot.Color.FromHex("#4FC3F7");
        scatter.LegendText = label;

        HistoryChart.Plot.Axes.DateTimeTicksBottom();
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
        "TotalSpills" => row.DeltaSpills,
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

    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart)
    {
        ScottPlot.Color figureBackground, dataBackground, textColor, gridColor;
        if (Helpers.ThemeManager.CurrentTheme == "CoolBreeze")
        {
            figureBackground = ScottPlot.Color.FromHex("#EEF4FA");
            dataBackground   = ScottPlot.Color.FromHex("#DAE6F0");
            textColor        = ScottPlot.Color.FromHex("#1A2A3A");
            gridColor        = ScottPlot.Colors.Black.WithAlpha(20);
        }
        else if (Helpers.ThemeManager.HasLightBackground)
        {
            figureBackground = ScottPlot.Color.FromHex("#FFFFFF");
            dataBackground   = ScottPlot.Color.FromHex("#F5F7FA");
            textColor        = ScottPlot.Color.FromHex("#4A5568");
            gridColor        = ScottPlot.Colors.Black.WithAlpha(20);
        }
        else
        {
            figureBackground = ScottPlot.Color.FromHex("#22252b");
            dataBackground   = ScottPlot.Color.FromHex("#111217");
            textColor        = ScottPlot.Color.FromHex("#9DA5B4");
            gridColor        = ScottPlot.Colors.White.WithAlpha(40);
        }
        chart.Plot.FigureBackground.Color = figureBackground;
        chart.Plot.DataBackground.Color = dataBackground;
        chart.Plot.Axes.Color(textColor);
        chart.Plot.Grid.MajorLineColor = gridColor;
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
    }

    private void OnThemeChanged(string _)
    {
        ApplyTheme(HistoryChart);
        HistoryChart.Refresh();
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.ExportToCsv(sender, "query_stats_history");

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}

