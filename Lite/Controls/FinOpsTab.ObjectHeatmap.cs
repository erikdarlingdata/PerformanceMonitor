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
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #1138 — the Storage Growth tab's parent → object → index drill. The parent grid (per-database growth)
/// drills into a database's object-growth heatmap (top-N objects by reserved-MB growth × daily buckets,
/// colored by absolute reserved MB) plus a companion grid, and an object drills to its per-index detail.
/// Heatmap shaping is shared via <see cref="FinOpsHeatmapBuilder"/>/<see cref="FinOpsHeatmapRenderer"/> so
/// Dashboard and Lite render identically.
/// </summary>
public partial class FinOpsTab : UserControl
{
    private enum StorageDrillLevel { Parent, Objects, Indexes }

    private StorageDrillLevel _storageLevel = StorageDrillLevel.Parent;
    private string _objDrillDb = "";
    private string _objDrillSchema = "";
    private string _objDrillTable = "";

    private FinOpsHeatmapMatrix? _objHeatmapMatrix;
    private FinOpsHeatmapHandle? _objHeatmapHandle;
    private ScottPlot.Plottables.Heatmap? _objHeatmapPlottable;
    private Popup? _objHeatmapPopup;
    private TextBlock? _objHeatmapPopupText;
    private DateTime _lastObjHeatmapHover;

    private int GetObjectHeatmapDaysBack() => ObjectHeatmapWindowCombo?.SelectedIndex switch
    {
        0 => 7,
        1 => 30,
        2 => 90,
        _ => 30
    };

    /// <summary>Resets the Storage Growth drill back to the per-database parent view (server change / refresh).</summary>
    private void ResetStorageDrill()
    {
        _objDrillDb = _objDrillSchema = _objDrillTable = "";
        ShowStorageView(StorageDrillLevel.Parent);
    }

    private void ShowStorageView(StorageDrillLevel level)
    {
        _storageLevel = level;
        StorageParentView.Visibility = level == StorageDrillLevel.Parent ? Visibility.Visible : Visibility.Collapsed;
        StorageObjectView.Visibility = level == StorageDrillLevel.Objects ? Visibility.Visible : Visibility.Collapsed;
        StorageIndexView.Visibility = level == StorageDrillLevel.Indexes ? Visibility.Visible : Visibility.Collapsed;

        StorageBackButton.Visibility = level == StorageDrillLevel.Parent ? Visibility.Collapsed : Visibility.Visible;
        var windowVisible = level == StorageDrillLevel.Objects ? Visibility.Visible : Visibility.Collapsed;
        ObjectHeatmapWindowCombo.Visibility = windowVisible;
        ObjectHeatmapWindowLabel.Visibility = windowVisible;

        StorageBreadcrumb.Text = level switch
        {
            StorageDrillLevel.Objects => $"Storage Growth  ›  {_objDrillDb}",
            StorageDrillLevel.Indexes => $"Storage Growth  ›  {_objDrillDb}  ›  {_objDrillSchema}.{_objDrillTable}",
            _ => "Storage Growth"
        };
    }

    private async void StorageGrowthGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (StorageGrowthDataGrid.SelectedItem is StorageGrowthRow row)
            await DrillToObjectsAsync(row.DatabaseName);
    }

    private async void StorageGrowthShowObjects_Click(object sender, RoutedEventArgs e)
    {
        if (GetFinOpsRow(sender) is StorageGrowthRow row)
            await DrillToObjectsAsync(row.DatabaseName);
    }

    private async void ObjectGrowthGrid_DoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (ObjectGrowthDetailGrid.SelectedItem is ObjectSizeGrowthRow row)
            await DrillToIndexesAsync(row.DatabaseName, row.SchemaName, row.TableName);
    }

    private void StorageGrowthBack_Click(object sender, RoutedEventArgs e)
    {
        if (_storageLevel == StorageDrillLevel.Indexes)
            ShowStorageView(StorageDrillLevel.Objects);
        else
            ShowStorageView(StorageDrillLevel.Parent);
    }

    private async void ObjectHeatmapWindow_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _dataService == null) return;
        if (_storageLevel != StorageDrillLevel.Objects || string.IsNullOrEmpty(_objDrillDb)) return;
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadObjectGrowthAsync(serverId, _objDrillDb);
    }

    private async Task DrillToObjectsAsync(string databaseName)
    {
        if (string.IsNullOrEmpty(databaseName)) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        _objDrillDb = databaseName;
        ShowStorageView(StorageDrillLevel.Objects);
        await LoadObjectGrowthAsync(serverId, databaseName);
    }

    private async Task DrillToIndexesAsync(string databaseName, string schemaName, string tableName)
    {
        if (string.IsNullOrEmpty(tableName)) return;
        var serverId = GetSelectedServerId();
        if (serverId == 0) return;
        _objDrillSchema = schemaName;
        _objDrillTable = tableName;
        ShowStorageView(StorageDrillLevel.Indexes);
        await LoadObjectIndexDetailAsync(serverId, databaseName, schemaName, tableName);
    }

    private async Task LoadObjectGrowthAsync(int serverId, string databaseName)
    {
        if (_dataService == null) return;

        var gen = _loads.Claim(nameof(LoadObjectGrowthAsync));

        try
        {
            var days = GetObjectHeatmapDaysBack();
            var (objects, samples) = await Task.Run(() => _dataService.GetObjectGrowthHeatmapDataAsync(serverId, databaseName, days));
            if (_loads.Superseded(nameof(LoadObjectGrowthAsync), gen)) return;

            // One canonical, deterministic top-of-chart-first ranking drives BOTH the companion grid and
            // the heatmap rows, so they can never disagree (and stay identical across Dashboard/Lite).
            var orderedKeys = FinOpsHeatmapBuilder.RankTopGrowers(
                objects.Select(o => ($"{o.SchemaName}.{o.TableName}", (double)o.Growth30dMb)), objects.Count);
            var byKey = objects.ToDictionary(o => $"{o.SchemaName}.{o.TableName}");
            var orderedObjects = orderedKeys.Select(k => byKey[k]).ToList();

            ObjectGrowthDetailGrid.ItemsSource = orderedObjects;
            StorageGrowthCountIndicator.Text = orderedObjects.Count > 0 ? $"{orderedObjects.Count} object(s)" : "";
            ObjectGrowthNoDataMessage.Visibility = orderedObjects.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

            // Heatmap rows are bottom-to-top: the renderer flips vertically (row 0 = bottom), so reverse
            // the top-first ranking to put the biggest grower at the TOP of the chart.
            var rowKeysBottomToTop = Enumerable.Reverse(orderedKeys).ToList();
            _objHeatmapMatrix = FinOpsHeatmapBuilder.BuildMatrix(rowKeysBottomToTop, samples);
            _objHeatmapHandle = FinOpsHeatmapRenderer.Render(
                ObjectGrowthHeatmapChart,
                _objHeatmapMatrix,
                "Reserved (MB)",
                $"{databaseName} — Object Reserved Footprint",
                _objHeatmapHandle);
            _objHeatmapPlottable = _objHeatmapHandle.Plottable;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load object growth heatmap: {ex.Message}");
        }
    }

    private async Task LoadObjectIndexDetailAsync(int serverId, string databaseName, string schemaName, string tableName)
    {
        if (_dataService == null) return;

        var gen = _loads.Claim(nameof(LoadObjectIndexDetailAsync));

        try
        {
            var data = await Task.Run(() => _dataService.GetObjectIndexDetailAsync(serverId, databaseName, schemaName, tableName));
            if (_loads.Superseded(nameof(LoadObjectIndexDetailAsync), gen)) return;
            ObjectIndexDetailGrid.ItemsSource = data;
            StorageGrowthCountIndicator.Text = data.Count > 0 ? $"{data.Count} index(es)" : "";
            ObjectIndexNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load object index detail: {ex.Message}");
        }
    }

    // ── Heatmap hover ──

    private void EnsureObjHeatmapPopup()
    {
        if (_objHeatmapPopup != null) return;
        _objHeatmapPopupText = new TextBlock
        {
            Foreground = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0)),
            FontSize = 13,
            MaxWidth = 460,
            TextTrimming = TextTrimming.CharacterEllipsis
        };
        _objHeatmapPopup = new Popup
        {
            PlacementTarget = ObjectGrowthHeatmapChart,
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
                Child = _objHeatmapPopupText
            }
        };
    }

    private void ObjectHeatmapChart_MouseLeave(object sender, MouseEventArgs e)
    {
        if (_objHeatmapPopup != null) _objHeatmapPopup.IsOpen = false;
    }

    private void ObjectHeatmapChart_MouseMove(object sender, MouseEventArgs e)
    {
        EnsureObjHeatmapPopup();
        if (_objHeatmapPopup == null || _objHeatmapPopupText == null || _objHeatmapPlottable == null) return;
        if (_objHeatmapMatrix == null || _objHeatmapMatrix.IsEmpty) return;

        var now = DateTime.UtcNow;
        if ((now - _lastObjHeatmapHover).TotalMilliseconds < 50) return;
        _lastObjHeatmapHover = now;

        var pos = e.GetPosition(ObjectGrowthHeatmapChart);
        var dpi = VisualTreeHelper.GetDpi(ObjectGrowthHeatmapChart);
        var pixel = new ScottPlot.Pixel((float)(pos.X * dpi.DpiScaleX), (float)(pos.Y * dpi.DpiScaleY));
        var coords = ObjectGrowthHeatmapChart.Plot.GetCoordinates(pixel);

        int numRows = _objHeatmapMatrix.Intensities.GetLength(0);
        int numCols = _objHeatmapMatrix.Intensities.GetLength(1);

        var (col, rowIdx) = _objHeatmapPlottable.GetIndexes(coords);
        int row = (numRows - 1) - rowIdx; // FlipVertically

        if (row < 0 || row >= numRows || col < 0 || col >= numCols)
        {
            _objHeatmapPopup.IsOpen = false;
            return;
        }

        double mb = _objHeatmapMatrix.Intensities[row, col];
        if (mb <= 0)
        {
            _objHeatmapPopup.IsOpen = false;
            return;
        }

        var label = row < _objHeatmapMatrix.RowLabels.Length ? _objHeatmapMatrix.RowLabels[row] : "?";
        var day = _objHeatmapMatrix.Days[col];
        _objHeatmapPopupText.Text = $"{label}  |  {day:M/d}  |  {mb:N1} MB reserved";

        _objHeatmapPopup.HorizontalOffset = pos.X + 15;
        _objHeatmapPopup.VerticalOffset = pos.Y + 15;
        _objHeatmapPopup.IsOpen = true;
    }
}
