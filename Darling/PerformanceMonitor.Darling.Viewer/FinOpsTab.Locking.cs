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
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Locking &amp; Contention sub-tab — a COPY of Lite's <c>FinOpsTab.Locking.cs</c> (#1138 Phase 2):
/// a color-scaled wait-type grid (a heatmap in grid form) with a database selector and an index-detail
/// drill. The four <c>*_wait_in_ms</c> columns are shaded per-column (log) by their wait category's hue via
/// <see cref="PerformanceMonitor.Ui.HeatIntensityToBrushConverter"/> (the shared .Ui converter); the numeric
/// values stay visible. Cumulative snapshot, no delta (§3B). Reads rewired to the viewer's Postgres reads;
/// the heat math (<see cref="FinOpsHeatmapBuilder.ColumnLogIntensities"/>) is the shared Common helper.
/// </summary>
public partial class FinOpsTab
{
    private enum FinOpsLockingLevel { Parent, Detail }

    private bool _suppressFinOpsLockingDbChange;

    /// <summary>Entry point (refresh / sub-tab activate): reset to the grid, repopulate the DB selector, load.</summary>
    private async Task LoadFinOpsIndexLockingAsync()
    {
        ShowFinOpsLockingView(FinOpsLockingLevel.Parent);
        await PopulateFinOpsLockingDbSelectorAsync();
        await LoadFinOpsIndexLockingGridAsync();
    }

    private async Task PopulateFinOpsLockingDbSelectorAsync()
    {
        var dbs = await _dataService.GetIndexLockingDatabasesAsync(_server.ServerId);
        var items = new List<string> { "All databases" };
        items.AddRange(dbs);

        _suppressFinOpsLockingDbChange = true;
        FinOpsLockingDbSelector.ItemsSource = items;
        FinOpsLockingDbSelector.SelectedIndex = 0;
        _suppressFinOpsLockingDbChange = false;
    }

    private string? SelectedFinOpsLockingDb()
        => FinOpsLockingDbSelector.SelectedIndex <= 0 ? null : FinOpsLockingDbSelector.SelectedItem as string;

    private async Task LoadFinOpsIndexLockingGridAsync()
    {
        var db = SelectedFinOpsLockingDb();
        var data = await _dataService.GetIndexLockingAsync(_server.ServerId, 200, db);
        ApplyFinOpsLockingHeat(data);
        _finopsIndexLockingFilterMgr!.UpdateData(data);
        FinOpsNoIndexLockingMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsIndexLockingCountIndicator.Text = data.Count > 0 ? $"{data.Count} index(es)" : "";

        /* Writers on an optimized-locking database wait on transaction-ID locks the grid's counters do not count. */
        var optimizedLockingNote = await _dataService.GetOptimizedLockingNoteAsync(_server.ServerId);
        FinOpsOptimizedLockingNote.Text = optimizedLockingNote ?? "";
        FinOpsOptimizedLockingNote.Visibility = optimizedLockingNote is null ? Visibility.Collapsed : Visibility.Visible;
    }

    /// <summary>Per-column log color-scale over the visible rows (#1138 §3B) — each wait column independent.</summary>
    private static void ApplyFinOpsLockingHeat(List<IndexLockingRow> rows)
    {
        var rowLock = FinOpsHeatmapBuilder.ColumnLogIntensities(rows.Select(r => r.RowLockWaitInMs).ToList());
        var pageLock = FinOpsHeatmapBuilder.ColumnLogIntensities(rows.Select(r => r.PageLockWaitInMs).ToList());
        var pageLatch = FinOpsHeatmapBuilder.ColumnLogIntensities(rows.Select(r => r.PageLatchWaitInMs).ToList());
        var pageIo = FinOpsHeatmapBuilder.ColumnLogIntensities(rows.Select(r => r.PageIoLatchWaitInMs).ToList());
        for (int i = 0; i < rows.Count; i++)
        {
            rows[i].RowLockHeat = rowLock[i];
            rows[i].PageLockHeat = pageLock[i];
            rows[i].PageLatchHeat = pageLatch[i];
            rows[i].PageIoLatchHeat = pageIo[i];
        }
    }

    private async void FinOpsLockingDbSelector_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressFinOpsLockingDbChange || !IsLoaded) return;
        ShowFinOpsLockingView(FinOpsLockingLevel.Parent);
        await RunFinOpsLoad(LoadFinOpsIndexLockingGridAsync);
    }

    private async void FinOpsRefreshIndexLocking_Click(object sender, RoutedEventArgs e)
        => await RunFinOpsLoad(LoadFinOpsIndexLockingAsync);

    private void FinOpsIndexLockingGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (FinOpsIndexLockingDataGrid.SelectedItem is IndexLockingRow row)
            ShowFinOpsLockingDetail(row);
    }

    private void FinOpsLockingBack_Click(object sender, RoutedEventArgs e)
        => ShowFinOpsLockingView(FinOpsLockingLevel.Parent);

    private void ShowFinOpsLockingView(FinOpsLockingLevel level)
    {
        FinOpsLockingParentView.Visibility = level == FinOpsLockingLevel.Parent ? Visibility.Visible : Visibility.Collapsed;
        FinOpsLockingDetailView.Visibility = level == FinOpsLockingLevel.Detail ? Visibility.Visible : Visibility.Collapsed;
        FinOpsLockingBackButton.Visibility = level == FinOpsLockingLevel.Detail ? Visibility.Visible : Visibility.Collapsed;
        var selectorVisible = level == FinOpsLockingLevel.Parent ? Visibility.Visible : Visibility.Collapsed;
        FinOpsLockingDbSelector.Visibility = selectorVisible;
        FinOpsLockingDbLabel.Visibility = selectorVisible;
        FinOpsLockingBreadcrumb.Text = level == FinOpsLockingLevel.Detail ? "Locking & Contention  ›  index detail" : "Locking & Contention";
    }

    private void ShowFinOpsLockingDetail(IndexLockingRow row)
    {
        FinOpsLockingDetailPanel.DataContext = row;

        FinOpsLockingDetailIdentity.Children.Clear();
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Database", row.DatabaseName);
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Schema", row.SchemaName);
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Table", row.TableName);
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Index", row.IndexName);
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Type", row.IndexTypeDesc);
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Reserved (MB)", row.ReservedMb.ToString("N1"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailIdentity, "Rows", row.TotalRows.ToString("N0"));

        FinOpsLockingDetailCounters.Children.Clear();
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Row Lock Count", row.RowLockCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Row Lock Wait Count", row.RowLockWaitCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Row Lock Wait (ms)", row.RowLockWaitInMs.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page Lock Count", row.PageLockCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page Lock Wait Count", row.PageLockWaitCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page Lock Wait (ms)", row.PageLockWaitInMs.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Lock Escalations", row.IndexLockPromotionCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page Latch Wait Count", row.PageLatchWaitCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page Latch Wait (ms)", row.PageLatchWaitInMs.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page IO Latch Wait Count", row.PageIoLatchWaitCount.ToString("N0"));
        AddFinOpsLockingDetailRow(FinOpsLockingDetailCounters, "Page IO Latch Wait (ms)", row.PageIoLatchWaitInMs.ToString("N0"));

        ShowFinOpsLockingView(FinOpsLockingLevel.Detail);
    }

    private void AddFinOpsLockingDetailRow(Panel panel, string label, string value)
    {
        var grid = new Grid { Margin = new Thickness(0, 2, 0, 2) };
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(170) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });

        var labelBlock = new TextBlock
        {
            Text = label,
            FontWeight = FontWeights.SemiBold,
            Foreground = (Brush)FindResource("ForegroundMutedBrush")
        };
        var valueBlock = new TextBlock
        {
            Text = value,
            Foreground = (Brush)FindResource("ForegroundBrush"),
            HorizontalAlignment = HorizontalAlignment.Right
        };
        Grid.SetColumn(labelBlock, 0);
        Grid.SetColumn(valueBlock, 1);
        grid.Children.Add(labelBlock);
        grid.Children.Add(valueBlock);
        panel.Children.Add(grid);
    }
}
