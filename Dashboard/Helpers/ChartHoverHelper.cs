using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitorDashboard.Helpers;

/// <summary>
/// Adds mouse-hover tooltips to a ScottPlot chart with multiple scatter series.
/// Shows the series name, value, and timestamp in a popup that follows the mouse.
/// Uses X-axis (time) proximity for reliable detection on time-series charts.
/// </summary>
internal sealed class ChartHoverHelper
{
    private readonly ScottPlot.WPF.WpfPlot _chart;
    private readonly List<(ScottPlot.Plottables.Scatter Scatter, string Label)> _scatters = new();
    private readonly List<(ScottPlot.Plottables.BarPlot BarPlot, string Label)> _barPlots = new();
    private readonly Popup _popup;
    private readonly TextBlock _text;
    private string _unit;
    private DateTime _lastUpdate;

    public ChartHoverHelper(ScottPlot.WPF.WpfPlot chart, string unit)
    {
        _chart = chart;
        _unit = unit;

        _text = new TextBlock
        {
            Foreground = new SolidColorBrush(Color.FromRgb(0xE0, 0xE0, 0xE0)),
            FontSize = 13
        };

        _popup = new Popup
        {
            PlacementTarget = chart,
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
                Child = _text
            }
        };

        chart.MouseMove += OnMouseMove;
        chart.MouseLeave += OnMouseLeave;
    }

    public string Unit { get => _unit; set => _unit = value; }

    public void Dispose()
    {
        _chart.MouseMove -= OnMouseMove;
        _chart.MouseLeave -= OnMouseLeave;
        _popup.IsOpen = false;
        _scatters.Clear();
        _barPlots.Clear();
    }

    public void Clear()
    {
        _scatters.Clear();
        _barPlots.Clear();
    }

    public void Add(ScottPlot.Plottables.Scatter scatter, string label) =>
        _scatters.Add((scatter, label));

    public void Add(ScottPlot.Plottables.BarPlot barPlot, string label) =>
        _barPlots.Add((barPlot, label));

    /// <summary>
    /// Returns the nearest series label and data-point time for the given mouse position,
    /// or null if no series is close enough.
    /// </summary>
    public (string Label, DateTime Time)? GetNearestSeries(Point mousePos)
    {
        if (_scatters.Count == 0 && _barPlots.Count == 0) return null;
        try
        {
            var dpi = VisualTreeHelper.GetDpi(_chart);
            var pixel = new ScottPlot.Pixel(
                (float)(mousePos.X * dpi.DpiScaleX),
                (float)(mousePos.Y * dpi.DpiScaleY));
            var mouseCoords = _chart.Plot.GetCoordinates(pixel);

            double bestYDistance = double.MaxValue;
            ScottPlot.DataPoint bestPoint = default;
            string bestLabel = "";
            bool found = false;

            foreach (var (scatter, label) in _scatters)
            {
                var nearest = scatter.Data.GetNearest(mouseCoords, _chart.Plot.LastRender);
                if (!nearest.IsReal) continue;
                var nearestPixel = _chart.Plot.GetPixel(
                    new ScottPlot.Coordinates(nearest.X, nearest.Y));
                double dx = Math.Abs(nearestPixel.X - pixel.X);
                double dy = Math.Abs(nearestPixel.Y - pixel.Y);
                if (dx < 80 && dy < bestYDistance)
                {
                    bestYDistance = dy;
                    bestPoint = nearest;
                    bestLabel = label;
                    found = true;
                }
            }

            FindNearestBar(pixel, ref bestYDistance, ref bestPoint, ref bestLabel, ref found);

            if (found)
                return (bestLabel, DateTime.FromOADate(bestPoint.X));
        }
        catch { }
        return null;
    }

    private void FindNearestBar(ScottPlot.Pixel pixel, ref double bestYDistance,
        ref ScottPlot.DataPoint bestPoint, ref string bestLabel, ref bool found)
    {
        foreach (var (barPlot, label) in _barPlots)
        {
            foreach (var bar in barPlot.Bars)
            {
                var topPixel = _chart.Plot.GetPixel(new ScottPlot.Coordinates(bar.Position, bar.Value));
                double halfWidthPx = Math.Abs(
                    _chart.Plot.GetPixel(new ScottPlot.Coordinates(bar.Position + bar.Size / 2, bar.Value)).X
                    - topPixel.X);
                double dx = Math.Abs(topPixel.X - pixel.X);
                if (dx > halfWidthPx + 4) continue;
                double dy = Math.Abs(topPixel.Y - pixel.Y);
                if (dy < bestYDistance)
                {
                    bestYDistance = dy;
                    // For stacked bars, report the segment height (Value - ValueBase), not the top coordinate
                    double segmentHeight = bar.Value - bar.ValueBase;
                    bestPoint = new ScottPlot.DataPoint(new ScottPlot.Coordinates(bar.Position, segmentHeight), 0);
                    bestLabel = label;
                    found = true;
                }
            }
        }
    }

    private void OnMouseMove(object sender, MouseEventArgs e)
    {
        if (_scatters.Count == 0 && _barPlots.Count == 0) return;
        var now = DateTime.UtcNow;
        if ((now - _lastUpdate).TotalMilliseconds < 30) return;
        _lastUpdate = now;

        try
        {
            var pos = e.GetPosition(_chart);
            var dpi = VisualTreeHelper.GetDpi(_chart);
            var pixel = new ScottPlot.Pixel(
                (float)(pos.X * dpi.DpiScaleX),
                (float)(pos.Y * dpi.DpiScaleY));
            var mouseCoords = _chart.Plot.GetCoordinates(pixel);

            /* Use X-axis (time) proximity as the primary filter, Y-axis distance
               as tiebreaker. This makes tooltips appear reliably when hovering at
               any Y position near a data point's time — standard for time-series. */
            double bestYDistance = double.MaxValue;
            ScottPlot.DataPoint bestPoint = default;
            string bestLabel = "";
            bool found = false;

            foreach (var (scatter, label) in _scatters)
            {
                var nearest = scatter.Data.GetNearest(mouseCoords, _chart.Plot.LastRender);
                if (!nearest.IsReal) continue;

                var nearestPixel = _chart.Plot.GetPixel(
                    new ScottPlot.Coordinates(nearest.X, nearest.Y));
                double dx = Math.Abs(nearestPixel.X - pixel.X);
                double dy = Math.Abs(nearestPixel.Y - pixel.Y);

                /* Must be within 80px horizontally (time axis). Among matches,
                   pick the series closest in Y (nearest line to cursor). */
                if (dx < 80 && dy < bestYDistance)
                {
                    bestYDistance = dy;
                    bestPoint = nearest;
                    bestLabel = label;
                    found = true;
                }
            }

            FindNearestBar(pixel, ref bestYDistance, ref bestPoint, ref bestLabel, ref found);

            if (found)
            {
                var time = ServerTimeHelper.ConvertForDisplay(DateTime.FromOADate(bestPoint.X), ServerTimeHelper.CurrentDisplayMode);
                string valueFormatted = (bestPoint.Y == Math.Floor(bestPoint.Y))
                    ? bestPoint.Y.ToString("N0")
                    : bestPoint.Y.ToString("N1");
                _text.Text = $"{bestLabel}\n{valueFormatted} {_unit}\n{time:HH:mm:ss}";
                _popup.HorizontalOffset = pos.X + 15;
                _popup.VerticalOffset = pos.Y + 15;
                _popup.IsOpen = true;
            }
            else
            {
                _popup.IsOpen = false;
            }
        }
        catch
        {
            _popup.IsOpen = false;
        }
    }

    private void OnMouseLeave(object sender, MouseEventArgs e)
    {
        _popup.IsOpen = false;
    }
}
