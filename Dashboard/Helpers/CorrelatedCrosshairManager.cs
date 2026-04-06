/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * SYNC WARNING: Lite has a matching copy at Lite/Helpers/CorrelatedCrosshairManager.cs.
 * Changes here must be mirrored there.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Helpers;

/// <summary>
/// Synchronizes vertical crosshair lines across multiple ScottPlot charts.
/// When the user hovers over any lane, all lanes show a VLine at the same X (time)
/// coordinate and value labels update to show each lane's value at that time.
/// </summary>
internal sealed class CorrelatedCrosshairManager : IDisposable
{
    private readonly List<LaneInfo> _lanes = new();
    private readonly Popup _tooltip;
    private readonly TextBlock _tooltipText;
    private DateTime _lastUpdate;
    private bool _isRefreshing;

    public CorrelatedCrosshairManager()
    {
        _tooltipText = new TextBlock
        {
            Foreground = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0)),
            FontSize = 13
        };

        _tooltip = new Popup
        {
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
                Child = _tooltipText
            }
        };
    }

    /// <summary>
    /// Registers a chart lane for crosshair synchronization.
    /// </summary>
    public void AddLane(ScottPlot.WPF.WpfPlot chart, string label, string unit, TextBlock valueLabel)
    {
        var lane = new LaneInfo
        {
            Chart = chart,
            Label = label,
            Unit = unit,
            ValueLabel = valueLabel
        };

        chart.MouseMove += (s, e) => OnMouseMove(lane, e);
        chart.MouseLeave += (s, e) => OnMouseLeave();

        _lanes.Add(lane);
    }

    /// <summary>
    /// Sets a single data series for a lane (most lanes have one series).
    /// </summary>
    public void SetLaneData(ScottPlot.WPF.WpfPlot chart, double[] times, double[] values,
        bool isEventBased = false)
    {
        var lane = _lanes.Find(l => l.Chart == chart);
        if (lane == null) return;

        lane.Series.Clear();
        lane.Series.Add(new DataSeries
        {
            Name = lane.Label,
            Times = times,
            Values = values,
            IsEventBased = isEventBased
        });
    }

    /// <summary>
    /// Adds a named data series to a lane (for lanes with multiple overlaid series).
    /// Call SetLaneData first to clear, then AddLaneSeries for additional series.
    /// </summary>
    public void AddLaneSeries(ScottPlot.WPF.WpfPlot chart, string name, string unit,
        double[] times, double[] values, bool isEventBased = false)
    {
        var lane = _lanes.Find(l => l.Chart == chart);
        if (lane == null) return;

        lane.Series.Add(new DataSeries
        {
            Name = name,
            Unit = unit,
            Times = times,
            Values = values,
            IsEventBased = isEventBased
        });
    }

    /// <summary>
    /// Clears data and VLines. Call before re-populating charts.
    /// </summary>
    public void PrepareForRefresh()
    {
        _isRefreshing = true;
        _tooltip.IsOpen = false;
        foreach (var lane in _lanes)
        {
            lane.Series.Clear();
            lane.VLine = null;
        }
    }

    /// <summary>
    /// Creates fresh VLine plottables on each lane's chart.
    /// Must be called AFTER chart data is populated.
    /// </summary>
    public void ReattachVLines()
    {
        foreach (var lane in _lanes)
        {
            var vline = lane.Chart.Plot.Add.VerticalLine(0);
            vline.Color = ScottPlot.Color.FromHex("#FFFFFF").WithAlpha(100);
            vline.LineWidth = 1;
            vline.LinePattern = ScottPlot.LinePattern.Dashed;
            vline.IsVisible = false;
            lane.VLine = vline;
        }
        _isRefreshing = false;
    }

    private void OnMouseMove(LaneInfo sourceLane, MouseEventArgs e)
    {
        if (_isRefreshing || sourceLane.VLine == null) return;

        var now = DateTime.UtcNow;
        if ((now - _lastUpdate).TotalMilliseconds < 16) return;
        _lastUpdate = now;

        var pos = e.GetPosition(sourceLane.Chart);
        var dpi = VisualTreeHelper.GetDpi(sourceLane.Chart);
        var pixel = new ScottPlot.Pixel(
            (float)(pos.X * dpi.DpiScaleX),
            (float)(pos.Y * dpi.DpiScaleY));
        var mouseCoords = sourceLane.Chart.Plot.GetCoordinates(pixel);
        double xValue = mouseCoords.X;

        var tooltipLines = new List<string>();
        var time = DateTime.FromOADate(xValue);
        var displayTime = ServerTimeHelper.ConvertForDisplay(time, ServerTimeHelper.CurrentDisplayMode);
        tooltipLines.Add(displayTime.ToString("yyyy-MM-dd HH:mm:ss"));

        foreach (var lane in _lanes)
        {
            if (lane.VLine == null) continue;

            lane.VLine.IsVisible = true;
            lane.VLine.X = xValue;

            if (lane.Series.Count == 1)
            {
                // Single series — use lane label and unit
                var series = lane.Series[0];
                double? value = FindNearestValue(series, xValue);

                if (value.HasValue)
                {
                    lane.ValueLabel.Text = $"{value.Value:N1} {lane.Unit}";
                    tooltipLines.Add($"{lane.Label}: {value.Value:N1} {lane.Unit}");
                }
                else
                {
                    lane.ValueLabel.Text = "";
                    tooltipLines.Add($"{lane.Label}: —");
                }
            }
            else if (lane.Series.Count > 1)
            {
                // Multiple series — show each with its own name
                var valueParts = new List<string>();
                foreach (var series in lane.Series)
                {
                    double? value = FindNearestValue(series, xValue);
                    string unit = series.Unit ?? lane.Unit;
                    if (value.HasValue)
                    {
                        valueParts.Add($"{value.Value:N0}");
                        tooltipLines.Add($"{series.Name}: {value.Value:N0} {unit}");
                    }
                    else
                    {
                        tooltipLines.Add($"{series.Name}: —");
                    }
                }
                lane.ValueLabel.Text = valueParts.Count > 0 ? string.Join("/", valueParts) : "";
            }
            else
            {
                lane.ValueLabel.Text = "";
                tooltipLines.Add($"{lane.Label}: —");
            }

            lane.Chart.Refresh();
        }

        _tooltipText.Text = string.Join("\n", tooltipLines);
        _tooltip.PlacementTarget = sourceLane.Chart;
        _tooltip.HorizontalOffset = pos.X + 15;
        _tooltip.VerticalOffset = pos.Y + 15;
        _tooltip.IsOpen = true;
    }

    private static double? FindNearestValue(DataSeries series, double targetX)
    {
        if (series.Times == null || series.Values == null || series.Times.Length == 0)
            return null;

        var times = series.Times;
        var values = series.Values;

        int lo = 0, hi = times.Length - 1;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (times[mid] < targetX)
                lo = mid + 1;
            else
                hi = mid;
        }

        int best = lo;
        if (lo > 0 && Math.Abs(times[lo - 1] - targetX) < Math.Abs(times[lo] - targetX))
            best = lo - 1;

        double val = values[best];
        if (double.IsNaN(val)) return null;

        if (series.IsEventBased)
        {
            double oneMinute = 1.0 / 1440.0;
            if (Math.Abs(times[best] - targetX) > oneMinute)
                return 0;
        }

        return val;
    }

    private void OnMouseLeave()
    {
        _tooltip.IsOpen = false;
        foreach (var lane in _lanes)
        {
            if (lane.VLine != null)
                lane.VLine.IsVisible = false;
            lane.ValueLabel.Text = "";
            lane.Chart.Refresh();
        }
    }

    public void Dispose()
    {
        _tooltip.IsOpen = false;
        foreach (var lane in _lanes)
        {
            lane.Series.Clear();
            lane.VLine = null;
        }
        _lanes.Clear();
    }

    private class DataSeries
    {
        public string Name { get; set; } = "";
        public string? Unit { get; set; }
        public double[]? Times { get; set; }
        public double[]? Values { get; set; }
        public bool IsEventBased { get; set; }
    }

    private class LaneInfo
    {
        public ScottPlot.WPF.WpfPlot Chart { get; set; } = null!;
        public string Label { get; set; } = "";
        public string Unit { get; set; } = "";
        public ScottPlot.Plottables.VerticalLine? VLine { get; set; }
        public TextBlock ValueLabel { get; set; } = null!;
        public List<DataSeries> Series { get; set; } = new();
    }
}
