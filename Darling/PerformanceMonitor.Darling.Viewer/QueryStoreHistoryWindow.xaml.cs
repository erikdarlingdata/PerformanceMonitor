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
using Microsoft.Win32;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Query Store per-row double-click history window — ported from Lite's
/// <c>Windows/QueryStoreHistoryWindow.xaml.cs</c>. Shows the selected query's (by query_id, across every plan)
/// metric trend over the window — one chart series per plan_id, so plan switches / regressions are visible —
/// plus a filterable grid of its per-collection snapshots. Reads come from
/// <see cref="ViewerDataService.GetQueryStoreHistoryAsync"/> (Postgres, query-scoped). "View Plan" and the
/// per-row Download button surface the STORED Query Store plan for that row's plan_id
/// (<see cref="ViewerDataService.GetQueryStorePlanTextAsync"/>); "Get Actual Plan (re-run)" asks the service to RE-EXECUTE by QueryId for a runtime plan.
/// </summary>
public partial class QueryStoreHistoryWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly int _serverId;
    private readonly string _databaseName;
    private readonly long _queryId;
    private readonly long _planId;
    private readonly string _queryText;
    private readonly DateTime _startUtc;
    private readonly DateTime _endUtc;
    private List<ViewerQueryStoreHistoryRow> _historyData = new();
    private ChartHoverHelper? _chartHover;
    private ScottPlot.IPanel? _legendPanel;
    private readonly DataGridFilterManager<ViewerQueryStoreHistoryRow> _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private System.Threading.CancellationTokenSource? _actualPlanCts;

    public QueryStoreHistoryWindow(
        ViewerDataService dataService, int serverId, string databaseName, long queryId, long planId,
        string queryText, DateTime startUtc, DateTime endUtc)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _databaseName = databaseName;
        _queryId = queryId;
        _planId = planId;
        _queryText = queryText;
        _startUtc = startUtc;
        _endUtc = endUtc;

        _filterManager = new DataGridFilterManager<ViewerQueryStoreHistoryRow>(HistoryDataGrid);
        DataGridFilterColumns.AddFilterButtons(HistoryDataGrid, Filter_Click);
        _filterManager.UpdateFilterButtonStyles();

        var displayText = queryText.Length > 120 ? queryText[..120] + "..." : queryText;
        QueryIdentifierText.Text = $"Query Store History: Query {queryId} in [{databaseName}]";
        SummaryText.Text = displayText;
        Loaded += async (_, _) => await LoadHistoryAsync();
        ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (_, _) => { ThemeManager.ThemeChanged -= OnThemeChanged; _actualPlanCts?.Cancel(); };
    }

    private async Task LoadHistoryAsync()
    {
        try
        {
            _historyData = await _dataService.GetQueryStoreHistoryAsync(_serverId, _databaseName, _queryId, _startUtc, _endUtc);
            _filterManager.UpdateData(_historyData);
            ApplyDefaultDescendingByCollectionTime();

            if (_historyData.Count > 0)
            {
                var totalExec = _historyData.Sum(r => r.ExecutionCount);
                var planCount = _historyData.Select(r => r.PlanId).Distinct().Count();
                var first = ViewerTimeHelper.ForDisplay(_historyData.First().CollectionTime);
                var last = ViewerTimeHelper.ForDisplay(_historyData.Last().CollectionTime);
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

    /// <summary>
    /// Defaults the history grid to newest-first (Erik's descending-default grid convention). The SQL read stays
    /// ascending on purpose — the chart re-sorts per plan group and the summary reads <c>First()</c>/<c>Last()</c>
    /// — so this reorders ONLY the grid's view, mirroring the parent Queries grids'
    /// <c>SetDefaultSortIfNone(..., ListSortDirection.Descending)</c>. Guarded so a user's later sort survives.
    /// </summary>
    private void ApplyDefaultDescendingByCollectionTime()
    {
        if (HistoryDataGrid.Items.SortDescriptions.Count != 0)
            return;

        HistoryDataGrid.Items.SortDescriptions.Add(new System.ComponentModel.SortDescription(
            nameof(ViewerQueryStoreHistoryRow.CollectionTime), System.ComponentModel.ListSortDirection.Descending));
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

        // One series per plan so plan switches/regressions are visible — same as Lite / the Dashboard drilldown.
        var planGroups = _historyData.GroupBy(r => r.PlanId).OrderBy(g => g.Key).ToList();
        var colors = ChartPalette.CyclingPalette.Select(ScottPlot.Color.FromHex).ToArray();

        int colorIndex = 0;
        foreach (var planGroup in planGroups)
        {
            var ordered = planGroup.OrderBy(r => r.CollectionTime).ToList();
            var xs = ordered.Select(r => ViewerTimeHelper.ForDisplay(r.CollectionTime).ToOADate()).ToArray();
            var ys = ordered.Select(r => GetMetricValue(r, tag)).ToArray();

            var scatter = HistoryChart.Plot.Add.TimeSeries(xs, ys);
            scatter.Color = colors[colorIndex % colors.Length];
            ChartStyle.StyleScatter(scatter);
            var seriesLabel = planGroups.Count > 1 ? $"Plan {planGroup.Key}" : label;
            scatter.LegendText = seriesLabel;
            _chartHover.Add(scatter, seriesLabel);
            colorIndex++;
        }

        HistoryChart.Plot.Axes.DateTimeTicksBottom();
        if (planGroups.Count > 1)
        {
            _legendPanel = HistoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            HistoryChart.Plot.Legend.FontSize = 12;
        }
        ApplyTheme(HistoryChart);

        HistoryChart.Refresh();
    }

    /// <summary>Projects one history row to the metric the selector shows — the chart's Y value. Pure +
    /// static for unit testing (mirrors Lite's GetMetricValue switch).</summary>
    internal static double GetMetricValue(ViewerQueryStoreHistoryRow row, string tag) => tag switch
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

    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart) => ChartStyle.ApplyMinimalChartTheme(chart);

    private void OnThemeChanged(string _)
    {
        ApplyTheme(HistoryChart);
        HistoryChart.Refresh();
        _filterManager.UpdateFilterButtonStyles();
    }

    // ── Stored plan surface (viewer has no live server — Lite's live fetch becomes a stored read) ──

    private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button btn) return;
        // Rows can span multiple plans — download the plan for THIS row, not the launching one.
        var rowPlanId = (btn.DataContext as ViewerQueryStoreHistoryRow)?.PlanId ?? _planId;
        if (string.IsNullOrEmpty(_databaseName) || rowPlanId == 0) return;

        btn.IsEnabled = false;
        btn.Content = "...";
        try
        {
            var plan = await _dataService.GetQueryStorePlanTextAsync(_serverId, _databaseName, _queryId, rowPlanId);
            if (string.IsNullOrEmpty(plan))
            {
                MessageBox.Show(this, "No Query Store plan was captured in the collected data for this plan ID.",
                    "Plan Not Found", MessageBoxButton.OK, MessageBoxImage.Information);
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
            btn.Content = "Saved";
            return;
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, $"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            if (btn.Content is "...")
                btn.Content = "Download";
            btn.IsEnabled = true;
        }
    }

    private long SelectedPlanId =>
        ((HistoryDataGrid.CurrentItem ?? HistoryDataGrid.SelectedItem) as ViewerQueryStoreHistoryRow)?.PlanId ?? _planId;

    /// <summary>"View Plan" — shows the selected row's stored Query Store plan in a shared
    /// <see cref="PlanViewerControl"/> floated above this window. No live "Get Actual Plan".</summary>
    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        var planId = SelectedPlanId;
        string? planXml;
        try
        {
            planXml = await _dataService.GetQueryStorePlanTextAsync(_serverId, _databaseName, _queryId, planId);
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, $"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            return;
        }

        if (string.IsNullOrEmpty(planXml))
        {
            MessageBox.Show(this, "No Query Store plan was captured in the collected data for this plan ID.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var label = $"Est Plan - QS {_queryId}/{planId}";
        var viewer = new PlanViewerControl();
        try
        {
            viewer.ServerMetadata = await _dataService.GetPlanAnalysisServerMetadataAsync(_serverId);
            await viewer.LoadPlan(planXml, label, _queryText);
        }
        catch (Exception ex)
        {
            viewer.Cleanup();
            MessageBox.Show(this, $"Failed to load the execution plan:\n\n{ex.Message}",
                "Plan Load Error", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        var window = GraphViewerWindow.ShowGraph(this, viewer, label);
        window.Closed += (_, _) => viewer.Cleanup();
    }

    /// <summary>"Get Actual Plan" — asks the SERVICE to RE-EXECUTE this Query Store query (SET STATISTICS XML) and
    /// floats the captured actual plan. Identifier-only: the service resolves the text from query_store_stats by
    /// (query_id, database). Shared consent + data-modification flag + read-only guard via
    /// <see cref="ViewerActualPlanFlow"/>.</summary>
    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        if (_queryId == 0) return;

        var planId = SelectedPlanId;
        string? estimatedPlanXml = null;
        try { estimatedPlanXml = await _dataService.GetQueryStorePlanTextAsync(_serverId, _databaseName, _queryId, planId); }
        catch { /* detection degrades to the fail-safe uncertain path */ }

        var argsJson = ViewerDataService.BuildActualPlanArgsForQueryStore(_queryId, _databaseName);
        var label = $"Actual Plan - QS {_queryId}/{planId}";

        _actualPlanCts?.Dispose();
        _actualPlanCts = new System.Threading.CancellationTokenSource();

        var planXml = await ViewerActualPlanFlow.RequestActualPlanAsync(
            this, _dataService, _serverId, "the monitored server", _databaseName, _queryText, estimatedPlanXml, argsJson,
            onStarted: () => System.Windows.Input.Mouse.OverrideCursor = System.Windows.Input.Cursors.Wait,
            onFinished: () => System.Windows.Input.Mouse.OverrideCursor = null,
            _actualPlanCts.Token);

        if (planXml != null)
            await ViewerActualPlanFlow.OpenFloatingPlanAsync(this, _dataService, _serverId, planXml, label, _queryText);
    }

    // ── Column Filter Popup (mirrors WaitDrillDownWindow / ViewerServerTab.Filters.cs) ──

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
        if (_filterPopup != null) _filterPopup.IsOpen = false;
        _filterManager.SetFilter(e.FilterState);
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null) _filterPopup.IsOpen = false;
    }

    // ── Copy / Export / Close ──

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "query_store_history", ViewerExportSettings.CsvSeparator);

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
