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

    public void Clear() => _scatters.Clear();

    public void Add(ScottPlot.Plottables.Scatter scatter, string label) =>
        _scatters.Add((scatter, label));

    private void OnMouseMove(object sender, MouseEventArgs e)
    {
        if (_scatters.Count == 0) return;
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

            if (found)
            {
                var time = DateTime.FromOADate(bestPoint.X);
                _text.Text = $"{bestLabel}\n{bestPoint.Y:N1} {_unit}\n{time:HH:mm:ss}";
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
