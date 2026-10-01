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
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Storage Growth sub-tab (parent → object → index drill) — a COPY of Lite's
/// <c>FinOpsTab.ObjectHeatmap.cs</c> (#1138). The parent grid (per-database growth) drills into a database's
/// object-growth heatmap (top-N objects by reserved-MB growth × daily buckets, colored by absolute reserved
/// MB) plus a companion grid, and an object drills to its per-index detail. Heatmap shaping/rendering flow
/// through the SHARED <see cref="FinOpsHeatmapBuilder"/> / <see cref="FinOpsHeatmapRenderer"/> so the viewer
/// renders identically to Dashboard/Lite; reads are rewired to the viewer's Postgres reads.
/// </summary>
public partial class FinOpsTab
{
    private enum FinOpsStorageDrillLevel { Parent, Objects, Indexes }

    private FinOpsStorageDrillLevel _finopsStorageLevel = FinOpsStorageDrillLevel.Parent;
    private string _finopsObjDrillDb = "";
    private string _finopsObjDrillSchema = "";
    private string _finopsObjDrillTable = "";

    private FinOpsHeatmapMatrix? _finopsObjHeatmapMatrix;
    private FinOpsHeatmapHandle? _finopsObjHeatmapHandle;
    private ScottPlot.Plottables.Heatmap? _finopsObjHeatmapPlottable;
    private Popup? _finopsObjHeatmapPopup;
    private TextBlock? _finopsObjHeatmapPopupText;
    private DateTime _finopsLastObjHeatmapHover;

    private int GetFinOpsObjectHeatmapDaysBack() => FinOpsObjectHeatmapWindowCombo?.SelectedIndex switch
    {
        0 => 7,
        1 => 30,
        2 => 90,
        _ => 30
    };

    /// <summary>Refresh-aware load: reloads whichever drill level is currently showing (server tab timer / Refresh).</summary>
    private async Task LoadFinOpsStorageGrowthActiveAsync()
    {
        switch (_finopsStorageLevel)
        {
            case FinOpsStorageDrillLevel.Objects when !string.IsNullOrEmpty(_finopsObjDrillDb):
                await LoadFinOpsObjectGrowthAsync(_finopsObjDrillDb);
                break;
            case FinOpsStorageDrillLevel.Indexes when !string.IsNullOrEmpty(_finopsObjDrillTable):
                await LoadFinOpsObjectIndexDetailAsync(_finopsObjDrillDb, _finopsObjDrillSchema, _finopsObjDrillTable);
                break;
            default:
                await LoadFinOpsStorageGrowthAsync();
                break;
        }
    }

    private async Task LoadFinOpsStorageGrowthAsync()
    {
        var data = await _dataService.GetStorageGrowthAsync(_server.ServerId);
        _finopsStorageGrowthFilterMgr!.UpdateData(data);
        FinOpsNoStorageGrowthMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsStorageGrowthCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";

        /* The Note column holds words only for a database whose size leaves its log out, so a server with none gets no
           empty column. */
        FinOpsStorageGrowthNoteColumn.Visibility = data.Any(r => r.Note != null) ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>Refresh button — reloads whichever drill level is showing (mirrors Lite's RefreshStorageGrowth_Click).</summary>
    private async void FinOpsRefreshStorageGrowth_Click(object sender, RoutedEventArgs e)
        => await RunFinOpsLoad(LoadFinOpsStorageGrowthActiveAsync);

    private void ShowFinOpsStorageView(FinOpsStorageDrillLevel level)
    {
        _finopsStorageLevel = level;
        FinOpsStorageParentView.Visibility = level == FinOpsStorageDrillLevel.Parent ? Visibility.Visible : Visibility.Collapsed;
        FinOpsStorageObjectView.Visibility = level == FinOpsStorageDrillLevel.Objects ? Visibility.Visible : Visibility.Collapsed;
        FinOpsStorageIndexView.Visibility = level == FinOpsStorageDrillLevel.Indexes ? Visibility.Visible : Visibility.Collapsed;

        FinOpsStorageBackButton.Visibility = level == FinOpsStorageDrillLevel.Parent ? Visibility.Collapsed : Visibility.Visible;
        var windowVisible = level == FinOpsStorageDrillLevel.Objects ? Visibility.Visible : Visibility.Collapsed;
        FinOpsObjectHeatmapWindowCombo.Visibility = windowVisible;
        FinOpsObjectHeatmapWindowLabel.Visibility = windowVisible;

        FinOpsStorageBreadcrumb.Text = level switch
        {
            FinOpsStorageDrillLevel.Objects => $"Storage Growth  ›  {_finopsObjDrillDb}",
            FinOpsStorageDrillLevel.Indexes => $"Storage Growth  ›  {_finopsObjDrillDb}  ›  {_finopsObjDrillSchema}.{_finopsObjDrillTable}",
            _ => "Storage Growth"
        };
    }

    private async void FinOpsStorageGrowthGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (FinOpsStorageGrowthDataGrid.SelectedItem is StorageGrowthRow row)
            await DrillFinOpsToObjectsAsync(row.DatabaseName);
    }

    private async void FinOpsStorageGrowthShowObjects_Click(object sender, RoutedEventArgs e)
    {
        if (FinOpsRowFromMenu(sender) is StorageGrowthRow row)
            await DrillFinOpsToObjectsAsync(row.DatabaseName);
    }

    private async void FinOpsObjectGrowthGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (FinOpsObjectGrowthDetailGrid.SelectedItem is ObjectSizeGrowthRow row)
            await DrillFinOpsToIndexesAsync(row.DatabaseName, row.SchemaName, row.TableName);
    }

    private void FinOpsStorageGrowthBack_Click(object sender, RoutedEventArgs e)
    {
        if (_finopsStorageLevel == FinOpsStorageDrillLevel.Indexes)
            ShowFinOpsStorageView(FinOpsStorageDrillLevel.Objects);
        else
            ShowFinOpsStorageView(FinOpsStorageDrillLevel.Parent);
    }

    private async void FinOpsObjectHeatmapWindow_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        if (_finopsStorageLevel != FinOpsStorageDrillLevel.Objects || string.IsNullOrEmpty(_finopsObjDrillDb)) return;
        await RunFinOpsLoad(() => LoadFinOpsObjectGrowthAsync(_finopsObjDrillDb));
    }

    private async Task DrillFinOpsToObjectsAsync(string databaseName)
    {
        if (string.IsNullOrEmpty(databaseName)) return;
        _finopsObjDrillDb = databaseName;
        ShowFinOpsStorageView(FinOpsStorageDrillLevel.Objects);
        await RunFinOpsLoad(() => LoadFinOpsObjectGrowthAsync(databaseName));
    }

    private async Task DrillFinOpsToIndexesAsync(string databaseName, string schemaName, string tableName)
    {
        if (string.IsNullOrEmpty(tableName)) return;
        _finopsObjDrillSchema = schemaName;
        _finopsObjDrillTable = tableName;
        ShowFinOpsStorageView(FinOpsStorageDrillLevel.Indexes);
        await RunFinOpsLoad(() => LoadFinOpsObjectIndexDetailAsync(databaseName, schemaName, tableName));
    }

    private async Task LoadFinOpsObjectGrowthAsync(string databaseName)
    {
        var days = GetFinOpsObjectHeatmapDaysBack();
        var (objects, samples) = await _dataService.GetObjectGrowthHeatmapDataAsync(_server.ServerId, databaseName, days);

        /* One canonical, deterministic top-of-chart-first ranking drives BOTH the companion grid and the
           heatmap rows, so they can never disagree (and stay identical across Dashboard/Lite). */
        var orderedKeys = FinOpsHeatmapBuilder.RankTopGrowers(
            objects.Select(o => ($"{o.SchemaName}.{o.TableName}", (double)(o.Growth30dMb ?? 0m))), objects.Count);
        var byKey = objects.ToDictionary(o => $"{o.SchemaName}.{o.TableName}");
        var orderedObjects = orderedKeys.Select(k => byKey[k]).ToList();

        FinOpsObjectGrowthDetailGrid.ItemsSource = orderedObjects;
        FinOpsStorageGrowthCountIndicator.Text = orderedObjects.Count > 0 ? $"{orderedObjects.Count} object(s)" : "";
        FinOpsObjectGrowthNoDataMessage.Visibility = orderedObjects.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

        /* Heatmap rows are bottom-to-top: the renderer flips vertically (row 0 = bottom), so reverse the
           top-first ranking to put the biggest grower at the TOP of the chart. */
        var rowKeysBottomToTop = Enumerable.Reverse(orderedKeys).ToList();
        _finopsObjHeatmapMatrix = FinOpsHeatmapBuilder.BuildMatrix(rowKeysBottomToTop, samples);
        _finopsObjHeatmapHandle = FinOpsHeatmapRenderer.Render(
            FinOpsObjectGrowthHeatmapChart,
            _finopsObjHeatmapMatrix,
            "Reserved (MB)",
            $"{databaseName} — Object Reserved Footprint",
            _finopsObjHeatmapHandle);
        _finopsObjHeatmapPlottable = _finopsObjHeatmapHandle.Plottable;
    }

    private async Task LoadFinOpsObjectIndexDetailAsync(string databaseName, string schemaName, string tableName)
    {
        var data = await _dataService.GetObjectIndexDetailAsync(_server.ServerId, databaseName, schemaName, tableName);
        FinOpsObjectIndexDetailGrid.ItemsSource = data;
        FinOpsStorageGrowthCountIndicator.Text = data.Count > 0 ? $"{data.Count} index(es)" : "";
        FinOpsObjectIndexNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>Finds the row a FinOps context-menu item was opened on (menu placed on the DataGridRow via its RowStyle).</summary>
    private static object? FinOpsRowFromMenu(object sender)
    {
        if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
        {
            if (contextMenu.PlacementTarget is DataGridRow row) return row.DataContext;
            if (contextMenu.PlacementTarget is DataGrid grid) return grid.CurrentCell.Item ?? grid.SelectedItem;
        }
        return null;
    }

    // ── Heatmap hover (copied from Lite; the popup shows object | day | reserved MB under the cursor) ──

    private void EnsureFinOpsObjHeatmapPopup()
    {
        if (_finopsObjHeatmapPopup != null) return;
        _finopsObjHeatmapPopupText = new TextBlock
        {
            Foreground = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0)),
            FontSize = 13,
            MaxWidth = 460,
            TextTrimming = TextTrimming.CharacterEllipsis
        };
        _finopsObjHeatmapPopup = new Popup
        {
            PlacementTarget = FinOpsObjectGrowthHeatmapChart,
            Placement = PlacementMode.Relative,
            IsHitTestVisible = false,
            AllowsTransparency = true,
            Child = new Border
            {
                Background = new SolidColorBrush(Color.FromRgb(0x33, 0x33, 0x33)),
                BorderBrush = new SolidColorBrush(Color.FromRgb(0x55, 0x55, 0x55)),
                BorderThickness = new Thickness(1),
                CornerRadius = new CornerRadius(3),
                Padding = new Thickness(8, 4, 8, 4),
                Child = _finopsObjHeatmapPopupText
            }
        };
    }

    private void FinOpsObjectHeatmapChart_MouseLeave(object sender, MouseEventArgs e)
    {
        if (_finopsObjHeatmapPopup != null) _finopsObjHeatmapPopup.IsOpen = false;
    }

    private void FinOpsObjectHeatmapChart_MouseMove(object sender, MouseEventArgs e)
    {
        EnsureFinOpsObjHeatmapPopup();
        if (_finopsObjHeatmapPopup == null || _finopsObjHeatmapPopupText == null || _finopsObjHeatmapPlottable == null) return;
        if (_finopsObjHeatmapMatrix == null || _finopsObjHeatmapMatrix.IsEmpty) return;

        var now = DateTime.UtcNow;
        if ((now - _finopsLastObjHeatmapHover).TotalMilliseconds < 50) return;
        _finopsLastObjHeatmapHover = now;

        var pos = e.GetPosition(FinOpsObjectGrowthHeatmapChart);
        var dpi = VisualTreeHelper.GetDpi(FinOpsObjectGrowthHeatmapChart);
        var pixel = new ScottPlot.Pixel((float)(pos.X * dpi.DpiScaleX), (float)(pos.Y * dpi.DpiScaleY));
        var coords = FinOpsObjectGrowthHeatmapChart.Plot.GetCoordinates(pixel);

        int numRows = _finopsObjHeatmapMatrix.Intensities.GetLength(0);
        int numCols = _finopsObjHeatmapMatrix.Intensities.GetLength(1);

        var (col, rowIdx) = _finopsObjHeatmapPlottable.GetIndexes(coords);
        int row = (numRows - 1) - rowIdx; // FlipVertically

        if (row < 0 || row >= numRows || col < 0 || col >= numCols)
        {
            _finopsObjHeatmapPopup.IsOpen = false;
            return;
        }

        double mb = _finopsObjHeatmapMatrix.Intensities[row, col];
        if (mb <= 0)
        {
            _finopsObjHeatmapPopup.IsOpen = false;
            return;
        }

        var label = row < _finopsObjHeatmapMatrix.RowLabels.Length ? _finopsObjHeatmapMatrix.RowLabels[row] : "?";
        var day = _finopsObjHeatmapMatrix.Days[col];
        _finopsObjHeatmapPopupText.Text = $"{label}  |  {day:M/d}  |  {mb:N1} MB reserved";

        _finopsObjHeatmapPopup.HorizontalOffset = pos.X + 15;
        _finopsObjHeatmapPopup.VerticalOffset = pos.Y + 15;
        _finopsObjHeatmapPopup.IsOpen = true;
    }
}
