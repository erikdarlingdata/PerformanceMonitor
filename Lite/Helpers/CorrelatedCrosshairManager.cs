/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * SYNC WARNING: Dashboard has a matching copy at Dashboard/Helpers/CorrelatedCrosshairManager.cs.
 * Changes here must be mirrored there.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

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
    public void AddLane(ScottPlot.WPF.WpfPlot chart, string label, string unit)
    {
        var lane = new LaneInfo
        {
            Chart = chart,
            Label = label,
            Unit = unit
        };

        chart.MouseMove += (s, e) => OnMouseMove(lane, e);
        chart.MouseLeave += (s, e) => OnMouseLeave();

        _lanes.Add(lane);
    }

    /// <summary>
    /// Sets the expected baseline range for a lane (upper/lower bounds).
    /// Values outside this range get ▲/▼ indicators in the tooltip.
    /// </summary>
    public void SetLaneBaseline(ScottPlot.WPF.WpfPlot chart, double lower, double upper,
        double minAnomalyValue = 0, bool isEventBased = false)
    {
        var lane = _lanes.Find(l => l.Chart == chart);
        if (lane == null) return;
        lane.BaselineLower = lower;
        lane.BaselineUpper = upper;
        lane.MinAnomalyValue = minAnomalyValue;
        lane.IsEventBased = isEventBased;
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
    /// Sets the label shown in the tooltip for comparison data (e.g., "yesterday").
    /// </summary>
    public void SetComparisonLabel(string label)
    {
        _comparisonLabel = label;
    }

    private string? _comparisonLabel;

    /// <summary>
    /// Clears data and VLines. Call before re-populating charts.
    /// The OnMouseMove guard relies on lane.VLine == null to detect "not ready",
    /// so this is self-healing: once ReattachVLines runs, crosshairs resume.
    /// </summary>
    public void PrepareForRefresh()
    {
        _tooltip.IsOpen = false;
        _comparisonLabel = null;
        foreach (var lane in _lanes)
        {
            lane.Series.Clear();
            lane.VLine = null;
            lane.BaselineUpper = null;
            lane.BaselineLower = null;
            lane.MinAnomalyValue = 0;
        }
    }

    /// <summary>
    /// Creates fresh VLine plottables on each lane's chart.
    /// Must be called AFTER chart data is populated. Safe to call in a finally
    /// block — if chart state is invalid, a failure on one lane won't prevent
    /// the others from recovering.
    /// </summary>
    public void ReattachVLines()
    {
        foreach (var lane in _lanes)
        {
            lane.VLine = CreateVLine(lane.Chart);
        }
    }

    /// <summary>
    /// Creates VLines only for lanes that don't already have one. Idempotent —
    /// safe to call from a finally block as a recovery path after an exception
    /// in the main refresh flow.
    /// </summary>
    public void EnsureVLinesAttached()
    {
        foreach (var lane in _lanes)
        {
            if (lane.VLine != null) continue;
            lane.VLine = CreateVLine(lane.Chart);
        }
    }

    private static ScottPlot.Plottables.VerticalLine? CreateVLine(ScottPlot.WPF.WpfPlot chart)
    {
        try
        {
            var vline = chart.Plot.Add.VerticalLine(0);
            vline.Color = ScottPlot.Color.FromHex("#FFFFFF").WithAlpha(100);
            vline.LineWidth = 1;
            vline.LinePattern = ScottPlot.LinePattern.Dashed;
            vline.IsVisible = false;
            return vline;
        }
        catch
        {
            /* If attach fails, return null so OnMouseMove skips this lane.
               Next refresh will try again. */
            return null;
        }
    }

    private void OnMouseMove(LaneInfo sourceLane, MouseEventArgs e)
    {
        if (sourceLane.VLine == null) return;

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

        _tooltipText.Inlines.Clear();
        var time = DateTime.FromOADate(xValue);
        var displayTime = ServerTimeHelper.ConvertForDisplay(time, ServerTimeHelper.CurrentDisplayMode);
        _tooltipText.Inlines.Add(new Run(displayTime.ToString("yyyy-MM-dd HH:mm:ss")));
        if (_comparisonLabel != null)
            _tooltipText.Inlines.Add(new Run($"  (dashed = {_comparisonLabel})") { Foreground = DimBrush });

        var defaultBrush = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0));

        foreach (var lane in _lanes)
        {
            if (lane.VLine == null) continue;

            lane.VLine.IsVisible = true;
            lane.VLine.X = xValue;

            if (lane.Series.Count == 1)
            {
                var series = lane.Series[0];
                double? value = FindNearestValue(series, xValue);

                if (value.HasValue)
                {
                    var indicator = GetBaselineIndicator(lane, value.Value);

                    // Tooltip: value + arrow + "30d avg" context
                    _tooltipText.Inlines.Add(new Run($"\n{lane.Label}: {value.Value:N1} {lane.Unit}") { Foreground = defaultBrush });
                    if (indicator != null)
                    {
                        _tooltipText.Inlines.Add(new Run($" {indicator.Value.Symbol}") { Foreground = indicator.Value.Brush });
                    }
                }
                else
                {
                    _tooltipText.Inlines.Add(new Run($"\n{lane.Label}: —") { Foreground = defaultBrush });
                }
            }
            else if (lane.Series.Count > 1)
            {
                foreach (var series in lane.Series)
                {
                    double? value = FindNearestValue(series, xValue);
                    string unit = series.Unit ?? lane.Unit;
                    if (value.HasValue)
                    {
                        _tooltipText.Inlines.Add(new Run($"\n{series.Name}: {value.Value:N0} {unit}") { Foreground = defaultBrush });
                        var indicator = GetBaselineIndicator(lane, value.Value);
                        if (indicator != null)
                            _tooltipText.Inlines.Add(new Run($" {indicator.Value.Symbol}") { Foreground = indicator.Value.Brush });
                    }
                    else
                        _tooltipText.Inlines.Add(new Run($"\n{series.Name}: —") { Foreground = defaultBrush });
                }
            }
            else
            {
                _tooltipText.Inlines.Add(new Run($"\n{lane.Label}: —") { Foreground = defaultBrush });
            }

            lane.Chart.Refresh();
        }
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

    private static readonly SolidColorBrush RedBrush = new(Color.FromRgb(0xFF, 0x52, 0x52));
    private static readonly SolidColorBrush GreenBrush = new(Color.FromRgb(0x69, 0xF0, 0x69));
    private static readonly SolidColorBrush DimBrush = new(Color.FromRgb(0x90, 0x96, 0xA0));

    private record struct BaselineIndicator(string Symbol, SolidColorBrush Brush);

    private static string? FormatBaselineContext(LaneInfo lane)
    {
        if (lane.BaselineUpper == null || lane.BaselineLower == null) return null;
        var mean = (lane.BaselineUpper.Value + lane.BaselineLower.Value) / 2.0;
        var formatted = mean >= 1000 ? $"{mean:N0}" : mean >= 10 ? $"{mean:N1}" : $"{mean:N2}";
        return $"30d avg: ~{formatted}";
    }

    private static BaselineIndicator? GetBaselineIndicator(LaneInfo lane, double value)
    {
        if (lane.BaselineUpper == null || lane.BaselineLower == null) return null;
        // For event-based metrics (blocking/deadlocks): value significantly above
        // the baseline mean is a spike, even if within the wide ± 2σ band.
        // Uses 3x mean as threshold — if you normally see ~5 events and now see 20, that's a spike.
        var mean = (lane.BaselineUpper.Value + lane.BaselineLower.Value) / 2.0;
        if (lane.IsEventBased && value >= 1.0 && (mean < 1.0 || value > mean * 3))
            return new BaselineIndicator("▲", RedBrush);
        // ▲ requires both: outside band AND above absolute minimum (prevents 1% CPU false alarms)
        if (value > lane.BaselineUpper.Value && value >= lane.MinAnomalyValue)
            return new BaselineIndicator("▲", RedBrush);
        // ▼ always shown when below band (drops are always interesting — tuning feedback)
        if (value < lane.BaselineLower.Value)
            return new BaselineIndicator("▼", GreenBrush);
        return null;
    }

    private void OnMouseLeave()
    {
        _tooltip.IsOpen = false;
        foreach (var lane in _lanes)
        {
            if (lane.VLine != null)
                lane.VLine.IsVisible = false;
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
        public List<DataSeries> Series { get; set; } = new();
        public double? BaselineUpper { get; set; }
        public double? BaselineLower { get; set; }
        public double MinAnomalyValue { get; set; }
        public bool IsEventBased { get; set; }
    }
}
