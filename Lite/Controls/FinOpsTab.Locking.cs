/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #1138 Phase 2 — the Locking &amp; Contention tab as a color-scaled wait-type grid (a heatmap in grid
/// form) with a database selector and an index-detail drill. The four *_wait_in_ms columns are shaded
/// per-column (log) by their wait category's hue via <see cref="HeatIntensityToBrushConverter"/>; the
/// numeric values stay visible. Cumulative snapshot, no delta (§3B), so this sidesteps the M1/M2 issues.
/// </summary>
public partial class FinOpsTab : UserControl
{
    private enum LockingLevel { Parent, Detail }

    private bool _suppressLockingDbChange;

    /// <summary>Entry point (refresh / server change): reset to the grid, repopulate the DB selector, load.</summary>
    private async Task LoadIndexLockingAsync(int serverId)
    {
        if (_dataService == null) return;
        ShowLockingView(LockingLevel.Parent);
        await PopulateLockingDbSelectorAsync(serverId);
        await LoadIndexLockingGridAsync(serverId);
    }

    private async Task PopulateLockingDbSelectorAsync(int serverId)
    {
        if (_dataService == null) return;

        var gen = _loads.Claim(nameof(PopulateLockingDbSelectorAsync));

        try
        {
            var dbs = await Task.Run(() => _dataService.GetIndexLockingDatabasesAsync(serverId));
            if (_loads.Superseded(nameof(PopulateLockingDbSelectorAsync), gen)) return;
            var items = new List<string> { "All databases" };
            items.AddRange(dbs);

            _suppressLockingDbChange = true;
            LockingDbSelector.ItemsSource = items;
            LockingDbSelector.SelectedIndex = 0;
            _suppressLockingDbChange = false;
        }
        catch (Exception ex)
        {
            _suppressLockingDbChange = false;
            AppLogger.Error("FinOps", $"Failed to load locking databases: {ex.Message}");
        }
    }

    private string? SelectedLockingDb()
        => LockingDbSelector.SelectedIndex <= 0 ? null : LockingDbSelector.SelectedItem as string;

    private async Task LoadIndexLockingGridAsync(int serverId)
    {
        if (_dataService == null) return;

        var gen = _loads.Claim(nameof(LoadIndexLockingGridAsync));

        try
        {
            var db = SelectedLockingDb();
            var data = await Task.Run(() => _dataService.GetIndexLockingAsync(serverId, 200, db));
            if (_loads.Superseded(nameof(LoadIndexLockingGridAsync), gen)) return;
            ApplyLockingHeat(data);
            _indexLockingFilterMgr!.UpdateData(data);
            NoIndexLockingMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            IndexLockingCountIndicator.Text = data.Count > 0 ? $"{data.Count} index(es)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load index locking: {ex.Message}");
        }
    }

    /// <summary>Per-column log color-scale over the visible rows (#1138 §3B) — each wait column independent.</summary>
    private static void ApplyLockingHeat(List<IndexLockingRow> rows)
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

    private async void LockingDbSelector_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressLockingDbChange || !IsLoaded || _dataService == null) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        ShowLockingView(LockingLevel.Parent);
        await LoadIndexLockingGridAsync(serverId);
    }

    private void IndexLockingGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (IndexLockingDataGrid.SelectedItem is IndexLockingRow row)
            ShowLockingDetail(row);
    }

    private void LockingBack_Click(object sender, RoutedEventArgs e)
        => ShowLockingView(LockingLevel.Parent);

    private void ShowLockingView(LockingLevel level)
    {
        LockingParentView.Visibility = level == LockingLevel.Parent ? Visibility.Visible : Visibility.Collapsed;
        LockingDetailView.Visibility = level == LockingLevel.Detail ? Visibility.Visible : Visibility.Collapsed;
        LockingBackButton.Visibility = level == LockingLevel.Detail ? Visibility.Visible : Visibility.Collapsed;
        var selectorVisible = level == LockingLevel.Parent ? Visibility.Visible : Visibility.Collapsed;
        LockingDbSelector.Visibility = selectorVisible;
        LockingDbLabel.Visibility = selectorVisible;
        LockingBreadcrumb.Text = level == LockingLevel.Detail ? "Locking & Contention  ›  index detail" : "Locking & Contention";
    }

    private void ShowLockingDetail(IndexLockingRow row)
    {
        LockingDetailPanel.DataContext = row;

        LockingDetailIdentity.Children.Clear();
        AddLockingDetailRow(LockingDetailIdentity, "Database", row.DatabaseName);
        AddLockingDetailRow(LockingDetailIdentity, "Schema", row.SchemaName);
        AddLockingDetailRow(LockingDetailIdentity, "Table", row.TableName);
        AddLockingDetailRow(LockingDetailIdentity, "Index", row.IndexName);
        AddLockingDetailRow(LockingDetailIdentity, "Type", row.IndexTypeDesc);
        AddLockingDetailRow(LockingDetailIdentity, "Reserved (MB)", row.ReservedMb.ToString("N1"));
        AddLockingDetailRow(LockingDetailIdentity, "Rows", row.TotalRows.ToString("N0"));

        LockingDetailCounters.Children.Clear();
        AddLockingDetailRow(LockingDetailCounters, "Row Lock Count", row.RowLockCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Row Lock Wait Count", row.RowLockWaitCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Row Lock Wait (ms)", row.RowLockWaitInMs.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page Lock Count", row.PageLockCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page Lock Wait Count", row.PageLockWaitCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page Lock Wait (ms)", row.PageLockWaitInMs.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Lock Escalations", row.IndexLockPromotionCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page Latch Wait Count", row.PageLatchWaitCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page Latch Wait (ms)", row.PageLatchWaitInMs.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page IO Latch Wait Count", row.PageIoLatchWaitCount.ToString("N0"));
        AddLockingDetailRow(LockingDetailCounters, "Page IO Latch Wait (ms)", row.PageIoLatchWaitInMs.ToString("N0"));

        ShowLockingView(LockingLevel.Detail);
    }

    private void AddLockingDetailRow(Panel panel, string label, string value)
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
