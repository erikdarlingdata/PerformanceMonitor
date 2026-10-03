/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Queries → Query Heatmap sub-tab (W1f-2): the metric combo + heatmap of query counts per
/// (5-minute bin × per-execution magnitude bucket), copied from Lite's <c>ServerTab.Charts.cs</c>
/// (<c>UpdateQueryHeatmapChart</c> + the hover, :1095-1252) with the read rewired to
/// <see cref="ViewerDataService.GetQueryHeatmapAsync"/> Postgres. The only render-body change is the
/// time axis (Lite's per-server <c>UtcOffsetMinutes</c> shift → <see cref="ViewerTimeHelper.ForDisplay"/> on each
/// bin's naive-UTC start; X is the column index, so those are text labels).
/// The right-click "Show Active Queries at This Time" drill-down IS wired here (the task calls for it, and
/// Active Queries is a sibling sub-tab of this one), built inline (Lite's <c>ContextMenuHelper</c> is
/// Lite-only and can't be referenced): ScottPlot's default right-click responses are removed
/// (<see cref="RemoveScottPlotContextMenuResponses"/>) and a single-item WPF menu takes over, dropping Lite's
/// save/export items (the viewer never ported chart save/export). The other charts' drill-downs share this
/// path — see <c>ViewerServerTab.DrillDown.cs</c>.
/// </summary>
public partial class ViewerServerTab
{
    private HeatmapResult? _lastHeatmapResult;
    private ScottPlot.Plottables.Heatmap? _heatmapPlottable;
    private Popup? _heatmapPopup;
    private TextBlock? _heatmapPopupText;
    private DateTime _lastHeatmapHoverUpdate;

    /// <summary>Themes the heatmap chart up front, builds the hover popup, and wires the drill-down
    /// context menu. Called from <see cref="InitializeQueriesTab"/> after InitializeComponent.</summary>
    private void InitializeQueryHeatmap()
    {
        ApplyTheme(QueryHeatmapChart);
        QueryHeatmapChart.Refresh();

        /* Query heatmap hover popup (copied from Lite's ServerTab.xaml.cs). */
        _heatmapPopupText = new TextBlock
        {
            Foreground = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0)),
            FontSize = 13,
            MaxWidth = 450,
            TextTrimming = TextTrimming.CharacterEllipsis,
        };
        _heatmapPopup = new Popup
        {
            PlacementTarget = QueryHeatmapChart,
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
                Child = _heatmapPopupText,
            },
        };

        WireHeatmapDrillDownMenu();
    }

    /// <summary>
    /// Builds the heatmap's right-click menu — Lite's heatmap drill-down (ServerTab.xaml.cs:285-315): the
    /// "Show Active Queries at This Time" item Inserted at the TOP of the chart's copy/save/export menu
    /// (<see cref="BuildChartContextMenu"/>), so it reads [drill-down] [separator] [Copy/Save/Export] like
    /// every other drill-down chart. BuildChartContextMenu already takes right-click over from ScottPlot; the
    /// Opened handler resolves the time bin under the cursor and the Click routes it to
    /// <see cref="OnHeatmapDrillDown"/>.
    /// </summary>
    private void WireHeatmapDrillDownMenu()
    {
        var menu = BuildChartContextMenu(QueryHeatmapChart, "Query_Heatmap");
        var drillItem = new MenuItem { Header = "Show _Active Queries at This Time" };
        menu.Items.Insert(0, drillItem);
        menu.Items.Insert(1, new Separator());

        menu.Opened += (_, _) =>
        {
            if (_lastHeatmapResult == null || _heatmapPlottable == null || _lastHeatmapResult.TimeBuckets.Length == 0)
            {
                drillItem.IsEnabled = false;
                return;
            }

            var pos = Mouse.GetPosition(QueryHeatmapChart);
            var dpi = VisualTreeHelper.GetDpi(QueryHeatmapChart);
            var pixel = new ScottPlot.Pixel((float)(pos.X * dpi.DpiScaleX), (float)(pos.Y * dpi.DpiScaleY));
            var coords = QueryHeatmapChart.Plot.GetCoordinates(pixel);
            var (col, _) = _heatmapPlottable.GetIndexes(coords);
            if (col >= 0 && col < _lastHeatmapResult.TimeBuckets.Length)
            {
                drillItem.Tag = _lastHeatmapResult.TimeBuckets[col];
                drillItem.IsEnabled = true;
            }
            else
            {
                drillItem.IsEnabled = false;
            }
        };

        drillItem.Click += (_, _) =>
        {
            if (drillItem.Tag is DateTime bucketTime)
                _ = OnHeatmapDrillDown(bucketTime);
        };
    }

    /// <summary>Loads the Query Heatmap sub-tab for the combo's current metric over the window.</summary>
    private async Task LoadQueryHeatmapAsync(DateTime startUtc, DateTime endUtc)
    {
        await ReadAndDrawQueryHeatmapAsync((HeatmapMetric)HeatmapMetricCombo.SelectedIndex, startUtc, endUtc);
    }

    /// <summary>
    /// Reads the heatmap for <paramref name="metric"/> over the window, draws it, and raises the "Showing since" banner (#4966):
    /// the one read of the heatmap, behind the range load and the metric change. The chart draws one column per 5-minute bin that
    /// holds a row, so a stretch with no row has no column and cannot tell a server that did not exist yet from a quiet one; the
    /// banner names the earlier of the coverage start and the first column drawn. The read has no row cap, so only that rule applies.
    /// </summary>
    private async Task ReadAndDrawQueryHeatmapAsync(HeatmapMetric metric, DateTime startUtc, DateTime endUtc)
    {
        var dataStartTask = _dataService.GetQueryHeatmapDataStartAsync(_server.ServerId, startUtc, endUtc);
        var result = await _dataService.GetQueryHeatmapAsync(_server.ServerId, metric, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        UpdateQueryHeatmapChart(result);
        await ShowEventDataStartAsync(QueryHeatmapTruncationBanner, dataStartTask, "Query Heatmap", startUtc, result.TimeBuckets.Select(t => (DateTime?)t));
    }

    private async void HeatmapMetric_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        try
        {
            var (startUtc, endUtc) = GetWindowUtc();
            var metric = (HeatmapMetric)HeatmapMetricCombo.SelectedIndex;
            await ReadAndDrawQueryHeatmapAsync(metric, startUtc, endUtc);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"heatmap metric change failed: {ex.Message}");
        }
    }

    private void UpdateQueryHeatmapChart(HeatmapResult result)
    {
        ClearChart(QueryHeatmapChart);
        ApplyTheme(QueryHeatmapChart);

        _lastHeatmapResult = result;

        if (result.TimeBuckets.Length == 0 || result.BucketLabels.Length == 0)
        {
            RefreshEmptyChart(QueryHeatmapChart, "Query Heatmap", "");
            return;
        }

        int numRows = result.Intensities.GetLength(0);
        int numCols = result.Intensities.GetLength(1);

        // Log1p scaling; NaN for empty cells so they render as background.
        var scaled = new double[numRows, numCols];
        for (int r = 0; r < numRows; r++)
        {
            for (int c = 0; c < numCols; c++)
            {
                scaled[r, c] = result.Intensities[r, c] > 0
                    ? Math.Log(1 + result.Intensities[r, c])
                    : double.NaN;
            }
        }

        var heatmap = QueryHeatmapChart.Plot.Add.Heatmap(scaled);
        _heatmapPlottable = heatmap;
        heatmap.FlipVertically = true; // row 0 ("0-1ms") at bottom, row 6 (">100s") at top
        heatmap.Colormap = new ScottPlot.Colormaps.Viridis();
        heatmap.NaNCellColor = QueryHeatmapChart.Plot.DataBackground.Color;

        // Let ScottPlot use default extent (0..numCols, 0..numRows).
        // Use manual tick labels for both axes instead.
        ReapplyAxisColors(QueryHeatmapChart);

        // X-axis: time labels at column positions
        var xTicks = new ScottPlot.TickGenerators.NumericManual();
        int xStep = Math.Max(1, numCols / 12); // ~12 labels max
        for (int i = 0; i < numCols; i += xStep)
        {
            var t = ViewerTimeHelper.ForDisplay(result.TimeBuckets[i]);
            xTicks.AddMajor(i, t.ToString("M/d\nHH:mm"));
        }
        QueryHeatmapChart.Plot.Axes.Bottom.TickGenerator = xTicks;
        QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Left.TickLabelStyle.ForeColor;

        // Y-axis: bucket labels
        var yTicks = new ScottPlot.TickGenerators.NumericManual();
        for (int i = 0; i < result.BucketLabels.Length; i++)
        {
            yTicks.AddMajor(i, result.BucketLabels[i]);
        }
        QueryHeatmapChart.Plot.Axes.Left.TickGenerator = yTicks;

        // Axis limits match default heatmap extent
        QueryHeatmapChart.Plot.Axes.SetLimitsX(-0.5, numCols - 0.5);
        QueryHeatmapChart.Plot.Axes.SetLimitsY(-0.5, numRows - 0.5);

        // Colorbar with real query counts (undo log1p for tick labels)
        var colorBar = new ScottPlot.Panels.ColorBar(heatmap, ScottPlot.Edge.Right);
        colorBar.Label = "Query Count";
        colorBar.LabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
        colorBar.Axis.TickLabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
        double maxRaw = 0;
        for (int r = 0; r < numRows; r++)
            for (int c = 0; c < numCols; c++)
                if (result.Intensities[r, c] > maxRaw) maxRaw = result.Intensities[r, c];
        var cbTicks = new ScottPlot.TickGenerators.NumericManual();
        cbTicks.AddMajor(0, "0");
        int[] niceValues = { 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000 };
        foreach (var n in niceValues)
        {
            if (n > maxRaw) break;
            cbTicks.AddMajor(Math.Log(1 + n), n.ToString("N0"));
        }
        cbTicks.AddMajor(Math.Log(1 + maxRaw), ((int)maxRaw).ToString("N0"));
        colorBar.Axis.TickGenerator = cbTicks;
        QueryHeatmapChart.Plot.Axes.AddPanel(colorBar);
        _chartHelper.SetLegendPanel(QueryHeatmapChart, colorBar);

        var metricName = ((ComboBoxItem)HeatmapMetricCombo.SelectedItem).Content?.ToString() ?? "Duration (ms)";
        QueryHeatmapChart.Plot.Title($"Query Distribution by {metricName}");
        QueryHeatmapChart.Plot.Axes.Title.Label.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;

        QueryHeatmapChart.Refresh();
    }

    private void HeatmapChart_MouseLeave(object sender, MouseEventArgs e)
    {
        if (_heatmapPopup != null) _heatmapPopup.IsOpen = false;
    }

    private void HeatmapChart_MouseMove(object sender, MouseEventArgs e)
    {
        if (_heatmapPopup == null || _heatmapPopupText == null || _heatmapPlottable == null) return;
        if (_lastHeatmapResult == null || _lastHeatmapResult.TimeBuckets.Length == 0) return;

        var now = DateTime.UtcNow;
        if ((now - _lastHeatmapHoverUpdate).TotalMilliseconds < 50) return;
        _lastHeatmapHoverUpdate = now;

        var pos = e.GetPosition(QueryHeatmapChart);
        var dpi = VisualTreeHelper.GetDpi(QueryHeatmapChart);
        var pixel = new ScottPlot.Pixel(
            (float)(pos.X * dpi.DpiScaleX),
            (float)(pos.Y * dpi.DpiScaleY));
        var coords = QueryHeatmapChart.Plot.GetCoordinates(pixel);

        int numRows = _lastHeatmapResult.Intensities.GetLength(0);
        int numCols = _lastHeatmapResult.Intensities.GetLength(1);

        // Default heatmap extent (no custom Position): cols = 0..numCols, rows = 0..numRows.
        // GetIndexes returns bitmap indices. With FlipVertically=true, flip row for data index.
        var (col, rowIdx) = _heatmapPlottable.GetIndexes(coords);
        int row = (numRows - 1) - rowIdx;

        if (row < 0 || row >= numRows || col < 0 || col >= numCols)
        {
            _heatmapPopup.IsOpen = false;
            return;
        }

        long count = (long)_lastHeatmapResult.Intensities[row, col];
        if (count == 0)
        {
            _heatmapPopup.IsOpen = false;
            return;
        }

        var cell = _lastHeatmapResult.CellDetails[row, col];
        /* Worded in the display zone with the UTC offset added in the repeated hour of an autumn change, so two
           bins an hour apart do not both read 01:30:00 (#4766; Lite's heatmap hover does the same). */
        var time = DisplayZone.Format(_lastHeatmapResult.TimeBuckets[col], ViewerTimeHelper.CurrentDisplayZone(), "HH:mm:ss");
        var bucketLabel = row < _lastHeatmapResult.BucketLabels.Length
            ? _lastHeatmapResult.BucketLabels[row]
            : "?";

        var tipText = $"{time}  |  {bucketLabel}  |  {count:N0} queries";
        if (cell != null && !string.IsNullOrEmpty(cell.TopQueryText))
        {
            // Single line, collapse whitespace, truncate
            var flat = System.Text.RegularExpressions.Regex.Replace(cell.TopQueryText, @"\s+", " ").Trim();
            if (flat.Length > 60) flat = flat[..60] + "...";
            tipText += $"\n{flat}";
        }
        _heatmapPopupText.Text = tipText;

        _heatmapPopup.HorizontalOffset = pos.X + 15;
        _heatmapPopup.VerticalOffset = pos.Y + 15;
        _heatmapPopup.IsOpen = true;
    }

    /// <summary>The store's naive-UTC window a heatmap drill opens: 5 minutes before the clicked bin to 10 minutes
    /// after it, so the bin and its neighbours are covered (pure, unit-tested).</summary>
    internal static (DateTime FromUtc, DateTime ToUtc) HeatmapDrillWindowUtc(DateTime bucketTimeUtc)
    {
        return (bucketTimeUtc.AddMinutes(-5), bucketTimeUtc.AddMinutes(10));
    }

    /// <summary>
    /// Heatmap → Active Queries drill-down: opens the Active Queries sub-tab filtered to a window around
    /// the clicked 5-minute bin (-5/+10 min, covering the bin and its neighbours — Lite's OnHeatmapDrillDown).
    /// The bin time is naive UTC (date_bin over collection_time), which the Active Queries read takes
    /// directly, so the window is built from it as is; only the indicator's words are in the display zone
    /// (<see cref="DrillDownIndicatorText"/>).
    /// </summary>
    private async Task OnHeatmapDrillDown(DateTime bucketTimeUtc)
    {
        var (fromUtc, toUtc) = HeatmapDrillWindowUtc(bucketTimeUtc);
        var indicator = DrillDownIndicatorText(fromUtc, toUtc, ViewerTimeHelper.CurrentDisplayZone());
        await NavigateToActiveQueriesForWindowAsync(fromUtc, toUtc, indicator);
    }
}
