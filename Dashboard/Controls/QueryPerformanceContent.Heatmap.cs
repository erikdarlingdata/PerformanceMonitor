/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class QueryPerformanceContent : UserControl
    {
        private async Task RefreshQueryHeatmapAsync()
        {
            if (_databaseService == null) return;
            try
            {
                var metric = (HeatmapMetric)HeatmapMetricCombo.SelectedIndex;
                var result = await _databaseService.GetQueryHeatmapAsync(metric, _heatmapHoursBack, _heatmapFromDate, _heatmapToDate);
                UpdateQueryHeatmapChart(result);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing heatmap: {ex.Message}", ex);
            }
        }

        private void UpdateQueryHeatmapChart(HeatmapResult result)
        {
            if (_legendPanels.TryGetValue(QueryHeatmapChart, out var existingPanel) && existingPanel != null)
            {
                QueryHeatmapChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[QueryHeatmapChart] = null;
            }
            QueryHeatmapChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(QueryHeatmapChart);

            _lastHeatmapResult = result;

            if (result.TimeBuckets.Length == 0 || result.BucketLabels.Length == 0)
            {
                QueryHeatmapChart.Plot.Title("Query Heatmap — No Data");
                QueryHeatmapChart.Refresh();
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
            heatmap.FlipVertically = true;
            heatmap.Colormap = new ScottPlot.Colormaps.Viridis();
            heatmap.NaNCellColor = QueryHeatmapChart.Plot.DataBackground.Color;

            // X-axis: time labels at column positions
            var xTicks = new ScottPlot.TickGenerators.NumericManual();
            int xStep = Math.Max(1, numCols / 12);
            for (int i = 0; i < numCols; i += xStep)
            {
                var t = result.TimeBuckets[i];
                xTicks.AddMajor(i, t.ToString("M/d\nh:mm tt"));
            }
            QueryHeatmapChart.Plot.Axes.Bottom.TickGenerator = xTicks;

            // Y-axis: bucket labels
            var yTicks = new ScottPlot.TickGenerators.NumericManual();
            for (int i = 0; i < result.BucketLabels.Length; i++)
            {
                yTicks.AddMajor(i, result.BucketLabels[i]);
            }
            QueryHeatmapChart.Plot.Axes.Left.TickGenerator = yTicks;

            QueryHeatmapChart.Plot.Axes.SetLimitsX(-0.5, numCols - 0.5);
            QueryHeatmapChart.Plot.Axes.SetLimitsY(-0.5, numRows - 0.5);

            TabHelpers.ReapplyAxisColors(QueryHeatmapChart);

            // Colorbar with whole-number ticks
            double maxRaw = 0;
            for (int r = 0; r < numRows; r++)
                for (int c = 0; c < numCols; c++)
                    if (result.Intensities[r, c] > maxRaw) maxRaw = result.Intensities[r, c];
            var colorBar = new ScottPlot.Panels.ColorBar(heatmap, ScottPlot.Edge.Right);
            colorBar.Label = "Query Count";
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
            colorBar.LabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
            colorBar.Axis.TickLabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
            QueryHeatmapChart.Plot.Axes.AddPanel(colorBar);
            _legendPanels[QueryHeatmapChart] = colorBar;

            var metricName = ((ComboBoxItem)HeatmapMetricCombo.SelectedItem).Content?.ToString() ?? "Duration (ms)";
            QueryHeatmapChart.Plot.Title($"Query Distribution by {metricName}");

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
            var dpi = System.Windows.Media.VisualTreeHelper.GetDpi(QueryHeatmapChart);
            var pixel = new ScottPlot.Pixel(
                (float)(pos.X * dpi.DpiScaleX),
                (float)(pos.Y * dpi.DpiScaleY));
            var coords = QueryHeatmapChart.Plot.GetCoordinates(pixel);

            int numRows = _lastHeatmapResult.Intensities.GetLength(0);
            int numCols = _lastHeatmapResult.Intensities.GetLength(1);

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
            var time = _lastHeatmapResult.TimeBuckets[col];
            var bucketLabel = row < _lastHeatmapResult.BucketLabels.Length
                ? _lastHeatmapResult.BucketLabels[row]
                : "?";

            var tipText = $"{time:HH:mm:ss}  |  {bucketLabel}  |  {count:N0} queries";
            if (cell != null && !string.IsNullOrEmpty(cell.TopQueryText))
            {
                var flat = System.Text.RegularExpressions.Regex.Replace(cell.TopQueryText, @"\s+", " ").Trim();
                if (flat.Length > 60) flat = flat[..60] + "...";
                tipText += $"\n{flat}";
            }
            _heatmapPopupText.Text = tipText;

            _heatmapPopup.HorizontalOffset = pos.X + 15;
            _heatmapPopup.VerticalOffset = pos.Y + 15;
            _heatmapPopup.IsOpen = true;
        }

        private async void HeatmapMetric_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _databaseService == null) return;
            await RefreshQueryHeatmapAsync();
        }
    }
}
