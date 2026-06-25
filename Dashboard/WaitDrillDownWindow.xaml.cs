/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using static PerformanceMonitor.Ui.WaitDrillDownHelper;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard;

public partial class WaitDrillDownWindow : Window
{
    private readonly DatabaseService _databaseService;
    private readonly string _waitType;
    private readonly int _hoursBack;
    private readonly DateTime? _fromDate;
    private readonly DateTime? _toDate;

    // Filter state
    private Dictionary<string, ColumnFilterState> _filters = new();
    private List<QuerySnapshotItem>? _unfilteredData;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private readonly PlanNavigationController _planActions;

    public WaitDrillDownWindow(
        DatabaseService databaseService,
        string waitType,
        int hoursBack,
        DateTime? fromDate = null,
        DateTime? toDate = null)
    {
        InitializeComponent();
        _databaseService = databaseService;
        _waitType = waitType;
        _hoursBack = hoursBack;
        _fromDate = fromDate;
        _toDate = toDate;

        _planActions = new PlanNavigationController(
            this,
            (xml, label, qt) => PlanViewerWindow.ShowPlanAsync(this, xml, label, qt),
            (db, qt, est, iso, ct) => ActualPlanExecutor.ExecuteForActualPlanAsync(
                _databaseService.ConnectionString, db, qt, est, iso, isAzureSqlDb: false, timeoutSeconds: 0, ct),
            "the monitored server");

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

            List<QuerySnapshotItem> data;
            if (classification.Category == WaitCategory.Correlated || classification.Category == WaitCategory.Uncapturable)
            {
                // Fetch ALL queries in time range (no wait type filter)
                data = await _databaseService.GetQuerySnapshotsAsync(_hoursBack, _fromDate, _toDate);
            }
            else
            {
                data = await _databaseService.GetQuerySnapshotsByWaitTypeAsync(
                    _waitType, _hoursBack, _fromDate, _toDate);
            }

            if (data.Count == 0)
            {
                SummaryText.Text = classification.Category == WaitCategory.Correlated
                    ? "No query snapshots found in the selected time range."
                    : $"No query-level data found for {_waitType} in the selected time range.";
                return;
            }

            if (classification.Category == WaitCategory.Chain)
            {
                LoadChainData(data, classification);
            }
            else
            {
                LoadDirectData(data, classification);
            }
        }
        catch (Exception ex)
        {
            SummaryText.Text = $"Error: {ex.Message}";
        }
    }

    private void LoadDirectData(List<QuerySnapshotItem> data, WaitClassification classification)
    {
        data = SortByProperty(data, classification.SortProperty);
        _unfilteredData = data;
        _filters.Clear();
        ResultsDataGrid.ItemsSource = data;
        UpdateFilterButtonStyles();

        var timeRange = GetTimeRangeDescription(data);
        var truncated = data.Count >= 500 ? " (limited to 500 rows)" : "";
        SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {timeRange}{truncated}";

        ApplyInitialSort(classification.SortProperty);
    }

    private void LoadChainData(List<QuerySnapshotItem> data, WaitClassification classification)
    {
        // Map QuerySnapshotItem to SnapshotInfo for chain walker
        var allInfos = data.Select(ToSnapshotInfo).ToList();

        // For Dashboard, all returned rows already have the target wait type (filtered server-side)
        // so they're all "waiters" — use all of them for chain walking
        var headBlockerInfos = WalkBlockingChains(allInfos, allInfos);

        if (headBlockerInfos.Count == 0)
        {
            // No chain found — fall back to showing direct data
            _unfilteredData = data;
            _filters.Clear();
            ResultsDataGrid.ItemsSource = data;
            UpdateFilterButtonStyles();
            var timeRange = GetTimeRangeDescription(data);
            SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {timeRange} | No blocking chains found, showing waiters";
            return;
        }

        // Look up original full rows for each head blocker and set chain metadata
        var snapshotLookup = data
            .GroupBy(s => (s.CollectionTime, (int)s.SessionId))
            .ToDictionary(g => g.Key, g => g.First());

        var headBlockerRows = new List<QuerySnapshotItem>();
        foreach (var hb in headBlockerInfos)
        {
            if (snapshotLookup.TryGetValue((hb.CollectionTime, hb.SessionId), out var row))
            {
                row.ChainBlockingPath = hb.BlockingPath;
                // Overwrite BlockedSessionCount with chain walker's count
                row.BlockedSessionCount = (short)Math.Min(hb.BlockedSessionCount, short.MaxValue);
                headBlockerRows.Add(row);
            }
        }

        if (headBlockerRows.Count == 0)
        {
            // Head blockers not in data — show original data
            _unfilteredData = data;
            _filters.Clear();
            ResultsDataGrid.ItemsSource = data;
            UpdateFilterButtonStyles();
            var timeRange = GetTimeRangeDescription(data);
            SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {timeRange} | Head blockers not in snapshots, showing waiters";
            return;
        }

        // Insert chain-specific columns into the existing XAML columns
        InsertChainColumns();

        _unfilteredData = headBlockerRows;
        _filters.Clear();
        ResultsDataGrid.ItemsSource = headBlockerRows;
        UpdateFilterButtonStyles();

        var timeRangeDesc = GetTimeRangeDescription(headBlockerRows);
        SummaryText.Text = $"{headBlockerRows.Count} head blocker(s) from {data.Count} waiting session(s) | " +
                           $"{classification.Description} | {timeRangeDesc}";
    }

    private void InsertChainColumns()
    {
        // Insert "Blocking Path" column at the beginning — BlockedSessionCount already exists in the XAML columns
        var blockingPathCol = CreateFilterColumn("Blocking Path", "ChainBlockingPath", 250);
        ResultsDataGrid.Columns.Insert(0, blockingPathCol);
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

    private static SnapshotInfo ToSnapshotInfo(QuerySnapshotItem item) => new()
    {
        SessionId = item.SessionId,
        BlockingSessionId = item.BlockingSessionId ?? 0,
        CollectionTime = item.CollectionTime,
        DatabaseName = item.DatabaseName ?? "",
        Status = item.Status ?? "",
        QueryText = item.SqlText ?? "",
        WaitType = item.WaitInfo,
        WaitTimeMs = 0, // Dashboard wait_info is formatted text, no separate ms column
        CpuTimeMs = item.Cpu ?? 0,
        Reads = item.Reads ?? 0,
        Writes = item.Writes ?? 0,
        LogicalReads = 0 // Not available in Dashboard snapshot model
    };

    private static List<QuerySnapshotItem> SortByProperty(List<QuerySnapshotItem> data, string property) =>
        property switch
        {
            "CpuTimeMs" => data.OrderByDescending(r => r.Cpu ?? 0).ToList(),
            "Reads" => data.OrderByDescending(r => r.Reads ?? 0).ToList(),
            "Writes" => data.OrderByDescending(r => r.Writes ?? 0).ToList(),
            "Dop" => data, // Dashboard snapshots don't have a DOP column
            "GrantedQueryMemoryGb" => data.OrderByDescending(r => r.UsedMemoryMb ?? 0).ToList(),
            "WaitTimeMs" => data, // wait_info is text, can't sort numerically
            _ => data
        };

    private void ApplyInitialSort(string property)
    {
        var columnHeader = property switch
        {
            "CpuTimeMs" => "CPU (ms)",
            "Reads" => "Reads (pages)",
            "Writes" => "Writes (pages)",
            "GrantedQueryMemoryGb" => "Used Mem (MB)",
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

    private static string GetTimeRangeDescription(List<QuerySnapshotItem> data)
    {
        if (data.Count == 0) return "";
        var first = data.Min(r => r.CollectionTime);
        var last = data.Max(r => r.CollectionTime);
        return $"{ServerTimeHelper.ConvertForDisplay(first, ServerTimeHelper.CurrentDisplayMode):MM/dd HH:mm} to " +
               $"{ServerTimeHelper.ConvertForDisplay(last, ServerTimeHelper.CurrentDisplayMode):MM/dd HH:mm}";
    }

    private void OnThemeChanged(string _)
    {
        UpdateFilterButtonStyles();
    }

    #region Column Filter Popup

    private void Filter_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;

        if (_filterPopup == null)
        {
            _filterPopupContent = new ColumnFilterPopup();
            _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

            _filterPopup = new Popup
            {
                Child = _filterPopupContent,
                StaysOpen = false,
                Placement = PlacementMode.Bottom,
                AllowsTransparency = true
            };
        }

        _filters.TryGetValue(columnName, out var existingFilter);
        _filterPopupContent!.Initialize(columnName, existingFilter);

        _filterPopup.PlacementTarget = button;
        _filterPopup.IsOpen = true;
    }

    private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_filterPopup != null) _filterPopup.IsOpen = false;

        if (e.FilterState.IsActive)
            _filters[e.FilterState.ColumnName] = e.FilterState;
        else
            _filters.Remove(e.FilterState.ColumnName);

        ApplyFilters();
        UpdateFilterButtonStyles();
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null) _filterPopup.IsOpen = false;
    }

    private void ApplyFilters()
    {
        if (_unfilteredData == null) return;

        if (_filters.Count == 0)
        {
            ResultsDataGrid.ItemsSource = _unfilteredData;
            return;
        }

        var filtered = _unfilteredData.Where(item =>
        {
            foreach (var filter in _filters.Values)
            {
                if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    return false;
            }
            return true;
        }).ToList();

        ResultsDataGrid.ItemsSource = filtered;
    }

    private void UpdateFilterButtonStyles()
    {
        foreach (var column in ResultsDataGrid.Columns)
        {
            if (column.Header is StackPanel stackPanel)
            {
                var filterButton = stackPanel.Children.OfType<Button>().FirstOrDefault();
                if (filterButton?.Tag is string columnName)
                {
                    bool hasActive = _filters.TryGetValue(columnName, out var filter) && filter.IsActive;
                    filterButton.Content = new System.Windows.Controls.TextBlock
                    {
                        Text = "\uE71C",
                        FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                        Foreground = hasActive
                            ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00))
                            : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF))
                    };
                    filterButton.ToolTip = hasActive && filter != null
                        ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                        : "Click to filter";
                }
            }
        }
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            if (dataGrid != null && dataGrid.CurrentCell.Item != null)
            {
                var cellContent = TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                if (!string.IsNullOrEmpty(cellContent))
                    Clipboard.SetDataObject(cellContent, false);
            }
        }
    }

    private void CopyRow_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            if (dataGrid?.SelectedItem != null)
                Clipboard.SetDataObject(TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
        }
    }

    private void CopyAllRows_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            if (dataGrid != null && dataGrid.Items.Count > 0)
            {
                var sb = new StringBuilder();
                var headers = new List<string>();
                foreach (var column in dataGrid.Columns)
                {
                    if (column is DataGridBoundColumn)
                        headers.Add(DataGridClipboardBehavior.GetHeaderText(column));
                }
                sb.AppendLine(string.Join("\t", headers));
                foreach (var item in dataGrid.Items)
                    sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));
                Clipboard.SetDataObject(sb.ToString(), false);
            }
        }
    }

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            if (dataGrid != null && dataGrid.Items.Count > 0)
            {
                var dialog = new SaveFileDialog
                {
                    FileName = $"wait_drill_down_{_waitType}_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                    DefaultExt = ".csv",
                    Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                };

                if (dialog.ShowDialog() == true)
                {
                    try
                    {
                        var sb = new StringBuilder();
                        var headers = new List<string>();
                        foreach (var column in dataGrid.Columns)
                        {
                            if (column is DataGridBoundColumn)
                                headers.Add(TabHelpers.EscapeCsvField(DataGridClipboardBehavior.GetHeaderText(column)));
                        }
                        sb.AppendLine(string.Join(",", headers));
                        foreach (var item in dataGrid.Items)
                        {
                            var values = TabHelpers.GetRowValues(dataGrid, item);
                            sb.AppendLine(string.Join(",", values.Select(v => TabHelpers.EscapeCsvField(v))));
                        }
                        System.IO.File.WriteAllText(dialog.FileName, sb.ToString());
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show($"Export failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            }
        }
    }

    #endregion

    #region Plan Actions

    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        if (GetSnapshotItem(sender) is not { } item) return;
        await _planActions.ViewPlanAsync(
            () => _databaseService.GetQuerySnapshotPlanAsync(item.CollectionTime, item.SessionId),
            $"Est Plan - SPID {item.SessionId}", item.QueryText);
    }

    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        if (GetSnapshotItem(sender) is not { } item) return;
        await _planActions.GetActualPlanAsync(item.QueryText, item.DatabaseName ?? "",
            $"Actual Plan - SPID {item.SessionId}");
    }

    private static QuerySnapshotItem? GetSnapshotItem(object sender)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            return (dataGrid?.CurrentCell.Item ?? dataGrid?.SelectedItem) as QuerySnapshotItem;
        }
        return null;
    }

    #endregion

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
