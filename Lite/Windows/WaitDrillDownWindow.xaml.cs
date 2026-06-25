/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using static PerformanceMonitor.Ui.WaitDrillDownHelper;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Windows;

public partial class WaitDrillDownWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private readonly string _waitType;
    private readonly int _hoursBack;
    private readonly DateTime? _fromDate;
    private readonly DateTime? _toDate;
    private readonly string? _connectionString;
    private readonly PlanNavigationController _planActions;

    // Filter state
    private DataGridFilterManager<QuerySnapshotRow>? _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;

    public WaitDrillDownWindow(
        LocalDataService dataService,
        int serverId,
        string waitType,
        int hoursBack,
        DateTime? fromDate = null,
        DateTime? toDate = null,
        string? connectionString = null)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _waitType = waitType;
        _hoursBack = hoursBack;
        _fromDate = fromDate;
        _toDate = toDate;
        _connectionString = connectionString;

        _planActions = new PlanNavigationController(
            this,
            (xml, label, qt) => PlanViewerWindow.ShowPlanAsync(this, xml, label, qt),
            (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
                _connectionString ?? "", db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct),
            "the monitored server");

        _filterManager = new DataGridFilterManager<QuerySnapshotRow>(ResultsDataGrid);

        Title = $"Wait Drill-Down: {waitType}";

        var classification = Classify(waitType);
        HeaderText.Text = classification.Category == WaitCategory.Correlated
            ? $"Queries active during {waitType} spike"
            : $"Queries experiencing {waitType}";

        Loaded += async (_, _) => await LoadDataAsync();
        ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (_, _) => ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private async System.Threading.Tasks.Task LoadDataAsync()
    {
        SummaryText.Text = "Loading...";

        try
        {
            var classification = Classify(_waitType);
            SetWarningBanner(classification);

            if (classification.Category == WaitCategory.Chain)
            {
                await LoadChainDataAsync(classification);
            }
            else if (classification.Category == WaitCategory.Correlated || classification.Category == WaitCategory.Uncapturable)
            {
                await LoadCorrelatedDataAsync(classification);
            }
            else
            {
                await LoadDirectDataAsync(classification);
            }
        }
        catch (Exception ex)
        {
            SummaryText.Text = $"Error: {ex.Message}";
        }
    }

    private async System.Threading.Tasks.Task LoadDirectDataAsync(WaitClassification classification)
    {
        var data = await _dataService.GetQuerySnapshotsByWaitTypeAsync(
            _serverId, _waitType, _hoursBack, _fromDate, _toDate);

        if (data.Count == 0)
        {
            SummaryText.Text = $"No query-level data found for {_waitType} in the selected time range.";
            return;
        }

        // Sort by the classified column
        data = SortByProperty(data, classification.SortProperty);

        _filterManager!.UpdateData(data);

        var timeRange = GetTimeRangeDescription(data);
        var truncated = data.Count >= 500 ? " (limited to 500 rows)" : "";
        SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {timeRange}{truncated}";

        // Set initial sort on the DataGrid
        ApplyInitialSort(classification.SortProperty);
    }

    private async System.Threading.Tasks.Task LoadCorrelatedDataAsync(WaitClassification classification)
    {
        // Fetch ALL queries in the time range (no wait type filter)
        var data = await _dataService.GetAllQuerySnapshotsInRangeAsync(
            _serverId, _hoursBack, _fromDate, _toDate);

        if (data.Count == 0)
        {
            SummaryText.Text = $"No query snapshots found in the selected time range.";
            return;
        }

        data = SortByProperty(data, classification.SortProperty);
        _filterManager!.UpdateData(data);

        var timeRange = GetTimeRangeDescription(data);
        var truncated = data.Count >= 500 ? " (limited to 500 rows)" : "";
        SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {timeRange}{truncated}";

        ApplyInitialSort(classification.SortProperty);
    }

    private async System.Threading.Tasks.Task LoadChainDataAsync(WaitClassification classification)
    {
        // Get waiters with the target wait type
        var waiters = await _dataService.GetQuerySnapshotsByWaitTypeAsync(
            _serverId, _waitType, _hoursBack, _fromDate, _toDate);

        if (waiters.Count == 0)
        {
            SummaryText.Text = $"No query-level data found for {_waitType} in the selected time range.";
            return;
        }

        // Get all snapshots in range for chain walking
        var allSnapshots = await _dataService.GetAllQuerySnapshotsInRangeAsync(
            _serverId, _hoursBack, _fromDate, _toDate);

        // Map to SnapshotInfo for the chain walker
        var waiterInfos = waiters.Select(ToSnapshotInfo).ToList();
        var allInfos = allSnapshots.Select(ToSnapshotInfo).ToList();

        var headBlockerInfos = WalkBlockingChains(waiterInfos, allInfos);

        if (headBlockerInfos.Count == 0)
        {
            // No chain found — fall back to showing the waiters directly
            _filterManager!.UpdateData(waiters);
            var timeRange = GetTimeRangeDescription(waiters);
            SummaryText.Text = $"{waiters.Count} snapshot(s) | {classification.Description} | {timeRange} | No blocking chains found, showing waiters";
            return;
        }

        // Look up original full rows for each head blocker and set chain metadata
        var snapshotLookup = allSnapshots
            .GroupBy(s => (s.CollectionTime, s.SessionId))
            .ToDictionary(g => g.Key, g => g.First());

        var headBlockerRows = new List<QuerySnapshotRow>();
        foreach (var hb in headBlockerInfos)
        {
            if (snapshotLookup.TryGetValue((hb.CollectionTime, hb.SessionId), out var row))
            {
                row.ChainBlockedCount = hb.BlockedSessionCount;
                row.ChainBlockingPath = hb.BlockingPath;
                headBlockerRows.Add(row);
            }
        }

        if (headBlockerRows.Count == 0)
        {
            // Head blockers not found in snapshots — show waiters instead
            _filterManager!.UpdateData(waiters);
            var timeRange = GetTimeRangeDescription(waiters);
            SummaryText.Text = $"{waiters.Count} snapshot(s) | {classification.Description} | {timeRange} | Head blockers not in snapshots, showing waiters";
            return;
        }

        // Add chain columns to the existing XAML-defined columns
        InsertChainColumns();

        _filterManager!.UpdateData(headBlockerRows);

        var timeRangeDesc = GetTimeRangeDescription(headBlockerRows);
        SummaryText.Text = $"{headBlockerRows.Count} head blocker(s) from {waiters.Count} waiting session(s) | " +
                           $"{classification.Description} | {timeRangeDesc}";
    }

    private void InsertChainColumns()
    {
        // Insert "Blocked Sessions" and "Blocking Path" columns at the beginning of the grid
        var blockedCountCol = CreateFilterColumn("Blocked Sessions", "ChainBlockedCount", 105, isNumeric: true);
        var blockingPathCol = CreateFilterColumn("Blocking Path", "ChainBlockingPath", 250);

        ResultsDataGrid.Columns.Insert(0, blockedCountCol);
        ResultsDataGrid.Columns.Insert(1, blockingPathCol);
    }

    private DataGridTextColumn CreateFilterColumn(string headerText, string bindingPath, int width,
        bool isNumeric = false, string? stringFormat = null)
    {
        var filterButton = new Button { Tag = bindingPath, Margin = new Thickness(0, 0, 4, 0) };
        filterButton.SetResourceReference(StyleProperty, "ColumnFilterButtonStyle");
        filterButton.Click += Filter_Click;

        var header = new StackPanel { Orientation = Orientation.Horizontal };
        header.Children.Add(filterButton);
        header.Children.Add(new System.Windows.Controls.TextBlock
        {
            Text = headerText,
            FontWeight = FontWeights.Bold,
            VerticalAlignment = VerticalAlignment.Center
        });

        var binding = new System.Windows.Data.Binding(bindingPath);
        if (stringFormat != null) binding.StringFormat = stringFormat;

        var column = new DataGridTextColumn
        {
            Header = header,
            Binding = binding,
            Width = new DataGridLength(width)
        };

        if (isNumeric)
        {
            var numericStyle = (Style?)FindResource("NumericCell");
            if (numericStyle != null) column.ElementStyle = numericStyle;
        }

        return column;
    }

    private void SetWarningBanner(WaitClassification classification)
    {
        if (classification.Category == WaitCategory.Uncapturable)
        {
            WarningText.Text = $"Sessions experiencing {_waitType} waits may not be captured in query snapshots " +
                               "because they may lack assigned worker threads. Showing all queries in this time range.";
            WarningBanner.Visibility = Visibility.Visible;
        }
        else if (classification.Category == WaitCategory.Correlated)
        {
            WarningText.Text = $"{_waitType} waits are too brief to appear in query snapshots. " +
                               "Showing all queries active during this period, sorted by the most correlated metric.";
            WarningBanner.Visibility = Visibility.Visible;
            WarningBanner.Background = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromArgb(0x3D, 0x00, 0x33, 0x66));
            WarningBanner.BorderBrush = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromArgb(0x66, 0x00, 0x55, 0x99));
            WarningText.Foreground = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromRgb(0x66, 0xBB, 0xFF));
        }
        else if (classification.Category == WaitCategory.Chain)
        {
            WarningText.Text = $"Showing head blockers (the cause of {_waitType} waits), not the waiting sessions themselves.";
            WarningBanner.Visibility = Visibility.Visible;
            WarningBanner.Background = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromArgb(0x3D, 0x00, 0x33, 0x66));
            WarningBanner.BorderBrush = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromArgb(0x66, 0x00, 0x55, 0x99));
            WarningText.Foreground = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromRgb(0x66, 0xBB, 0xFF));
        }
    }

    private static SnapshotInfo ToSnapshotInfo(QuerySnapshotRow row) => new()
    {
        SessionId = row.SessionId,
        BlockingSessionId = row.BlockingSessionId,
        CollectionTime = row.CollectionTime,
        DatabaseName = row.DatabaseName,
        Status = row.Status,
        QueryText = row.QueryText,
        WaitType = row.WaitType,
        WaitTimeMs = row.WaitTimeMs,
        CpuTimeMs = row.CpuTimeMs,
        Reads = row.Reads,
        Writes = row.Writes,
        LogicalReads = row.LogicalReads
    };

    private static List<QuerySnapshotRow> SortByProperty(List<QuerySnapshotRow> data, string property) =>
        property switch
        {
            "CpuTimeMs" => data.OrderByDescending(r => r.CpuTimeMs).ToList(),
            "Reads" => data.OrderByDescending(r => r.Reads).ToList(),
            "Writes" => data.OrderByDescending(r => r.Writes).ToList(),
            "Dop" => data.OrderByDescending(r => r.Dop).ToList(),
            "GrantedQueryMemoryGb" => data.OrderByDescending(r => r.GrantedQueryMemoryGb).ToList(),
            "WaitTimeMs" => data.OrderByDescending(r => r.WaitTimeMs).ToList(),
            _ => data
        };

    private void ApplyInitialSort(string property)
    {
        var columnHeader = property switch
        {
            "CpuTimeMs" => "CPU (ms)",
            "Reads" => "Reads",
            "Writes" => "Writes",
            "Dop" => "DOP",
            "GrantedQueryMemoryGb" => "Memory (GB)",
            "WaitTimeMs" => "Wait (ms)",
            _ => null
        };

        if (columnHeader == null) return;

        foreach (var column in ResultsDataGrid.Columns)
        {
            if (column.Header is StackPanel sp)
            {
                var textBlock = sp.Children.OfType<System.Windows.Controls.TextBlock>().FirstOrDefault();
                if (textBlock?.Text == columnHeader)
                {
                    column.SortDirection = ListSortDirection.Descending;
                    break;
                }
            }
        }
    }

    private static string GetTimeRangeDescription(List<QuerySnapshotRow> data)
    {
        if (data.Count == 0) return "";
        var first = data.Min(r => r.CollectionTime);
        var last = data.Max(r => r.CollectionTime);
        return $"{ServerTimeHelper.FormatServerTime(first)} to {ServerTimeHelper.FormatServerTime(last)}";
    }


    private void OnThemeChanged(string _)
    {
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

        // Detach/reattach to avoid double-fire
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

    private void CopyCell_Click(object sender, RoutedEventArgs e) => ContextMenuHelper.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => ContextMenuHelper.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => ContextMenuHelper.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => ContextMenuHelper.ExportToCsv(sender, "wait_drill_down");

    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        if ((ResultsDataGrid.CurrentItem ?? ResultsDataGrid.SelectedItem) is not QuerySnapshotRow row) return;
        await _planActions.ViewPlanAsync(
            () => System.Threading.Tasks.Task.FromResult<string?>(row.LiveQueryPlan ?? row.QueryPlan),
            $"Est Plan - SPID {row.SessionId}", row.QueryText);
    }

    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        if ((ResultsDataGrid.CurrentItem ?? ResultsDataGrid.SelectedItem) is not QuerySnapshotRow row) return;
        await _planActions.GetActualPlanAsync(row.QueryText, row.DatabaseName ?? "",
            $"Actual Plan - SPID {row.SessionId}",
            estimatedPlanXml: row.LiveQueryPlan ?? row.QueryPlan,
            isolationLevel: row.TransactionIsolationLevel);
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
