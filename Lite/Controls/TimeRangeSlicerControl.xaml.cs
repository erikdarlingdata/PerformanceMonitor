using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
// The DisplayZone property below hides the type's simple name inside this class.
using UiDisplayZone = PerformanceMonitor.Ui.DisplayZone;

namespace PerformanceMonitorLite.Controls;

public partial class TimeRangeSlicerControl : UserControl
{
    private List<TimeSliceBucket> _data = new();
    private DateTime? _requestedStartUtc;
    private DateTime? _requestedEndUtc;
    private string _metricLabel = "Sessions";
    private bool _isExpanded = true;

    // Overlay: per-item trend drawn on top of the aggregate area chart
    private List<(DateTime TimeUtc, double Value)>? _overlayData;
    private string? _overlayLabel;

    // Range as normalised [0..1] positions within the data span
    private double _rangeStart;
    private double _rangeEnd = 1.0;

    private const double HandleWidthPx = 8;
    private const double HandleGripWidthPx = 20;
    private const double MinRangeNorm = 0.02; // minimum ~2% of total span
    private const double ChartPaddingTop = 16;
    private const double ChartPaddingBottom = 20;

    private enum DragMode { None, MoveRange, DragStart, DragEnd }
    private DragMode _dragMode = DragMode.None;

    private enum HandleHover { None, Start, End }
    private HandleHover _hoveredHandle = HandleHover.None;
    private double _dragOriginX;
    private double _dragOriginRangeStart;
    private double _dragOriginRangeEnd;

    /// <summary>
    /// Fired when the user finishes adjusting the slicer handles.
    /// StartUtc/EndUtc are in UTC (matching DuckDB collection_time).
    /// </summary>
    public event EventHandler<SlicerRangeEventArgs>? RangeChanged;

    /// <summary>
    /// The zone the time axis labels and the range caption are worded in (#4766), read each time they draw. The data
    /// and the selection are naive-UTC instants and never move with it: the display mode reaches only the text.
    /// ServerTab sets it to its display zone; left null, it is the zone the display mode names for the selected
    /// server.
    /// </summary>
    public Func<TimeZoneInfo>? DisplayZone { get; set; }

    public TimeRangeSlicerControl()
    {
        InitializeComponent();
        SlicerBorder.SizeChanged += (_, _) => Redraw();
        IsVisibleChanged += (_, _) => { if (IsVisible) Redraw(); };
    }

    public bool IsExpanded
    {
        get => _isExpanded;
        set
        {
            _isExpanded = value;
            SlicerBorder.Visibility = _isExpanded ? Visibility.Visible : Visibility.Collapsed;
            ToggleIcon.Text = _isExpanded ? "▾" : "▸";
        }
    }

    /// <summary>
    /// Loads slicer data. All timestamps must be UTC.
    /// requestedStartUtc/requestedEndUtc define the full time window so the slicer
    /// shows the correct span even when data is sparse. Empty hours get zero-value buckets.
    /// Selection defaults to the full range (no filtering until user interacts).
    /// </summary>
    public void LoadData(List<TimeSliceBucket> data, string metricLabel,
        DateTime? requestedStartUtc = null, DateTime? requestedEndUtc = null)
    {
        // Preserve selection if we already have data (auto-refresh)
        DateTime? prevStart = null, prevEnd = null;
        if (_data.Count > 0 && (_rangeStart > 0 || _rangeEnd < 1.0))
        {
            prevStart = UtcAtNorm(_rangeStart);
            prevEnd = UtcAtNorm(_rangeEnd);
        }

        _requestedStartUtc = requestedStartUtc;
        _requestedEndUtc = requestedEndUtc;
        _data = FillEmptyBuckets(data, requestedStartUtc, requestedEndUtc);
        _metricLabel = metricLabel;

        if (prevStart.HasValue && prevEnd.HasValue && _data.Count >= 2)
        {
            _rangeStart = NormAtUtc(prevStart.Value);
            _rangeEnd = NormAtUtc(prevEnd.Value);
        }
        else
        {
            _rangeStart = 0;
            _rangeEnd = 1.0;
        }

        UpdateRangeLabel();
        Redraw();
    }

    /// <summary>
    /// Sets an overlay trend line on the slicer. Drawn on its own Y scale
    /// so it's visible regardless of the aggregate chart's magnitude.
    /// </summary>
    public void SetOverlay(List<(DateTime TimeUtc, double Value)> data, string label)
    {
        _overlayData = data.Count >= 1 ? data : null;
        _overlayLabel = label;
        Redraw();
    }

    /// <summary>Removes the overlay trend line.</summary>
    public void ClearOverlay()
    {
        if (_overlayData == null) return;
        _overlayData = null;
        _overlayLabel = null;
        Redraw();
    }

    /// <summary>Updates the metric label and redraws (when sort column changes).</summary>
    public void UpdateMetric(string metricLabel)
    {
        _metricLabel = metricLabel;
        Redraw();
    }

    public DateTime? SelectionStartUtc => _data.Count > 0 ? UtcAtNorm(_rangeStart) : null;
    public DateTime? SelectionEndUtc => _data.Count > 0 ? UtcAtNorm(_rangeEnd) : null;

    // ── Time mapping ──

    private DateTime DataStartUtc => _requestedStartUtc ?? _data[0].BucketTime;
    private DateTime DataEndUtc => _requestedEndUtc ?? _data[^1].BucketTime.AddHours(1);

    /// <summary>
    /// Fills in zero-value buckets for hours with no data so the slicer
    /// spans the full requested time range.
    /// </summary>
    private static List<TimeSliceBucket> FillEmptyBuckets(
        List<TimeSliceBucket> data, DateTime? requestedStart, DateTime? requestedEnd)
    {
        if (data.Count == 0 || !requestedStart.HasValue || !requestedEnd.HasValue)
            return data;

        var floorStart = FloorToHour(requestedStart.Value);
        // Floor the end too — we don't want a bucket for the current partial hour extending into the future
        var floorEnd = FloorToHour(requestedEnd.Value);

        var existing = new Dictionary<long, TimeSliceBucket>();
        foreach (var b in data)
            existing[FloorToHour(b.BucketTime).Ticks] = b;

        var result = new List<TimeSliceBucket>();
        for (var t = floorStart; t <= floorEnd; t = t.AddHours(1))
        {
            if (existing.TryGetValue(t.Ticks, out var bucket))
                result.Add(bucket);
            else
                result.Add(new TimeSliceBucket { BucketTime = t });
        }

        return result;
    }

    private DateTime UtcAtNorm(double norm)
    {
        var ticks = DataStartUtc.Ticks + (long)((DataEndUtc.Ticks - DataStartUtc.Ticks) * norm);
        return new DateTime(Math.Clamp(ticks, DataStartUtc.Ticks, DataEndUtc.Ticks), DateTimeKind.Utc);
    }

    private double NormAtUtc(DateTime utc)
    {
        var span = DataEndUtc.Ticks - DataStartUtc.Ticks;
        if (span <= 0) return 0;
        return Math.Clamp((double)(utc.Ticks - DataStartUtc.Ticks) / span, 0, 1);
    }

    // ── Display zone ──

    /// <summary>
    /// The zone the labels and the range caption are worded in: the one the tab set, else the zone the display mode
    /// names for the selected server.
    /// </summary>
    private TimeZoneInfo CurrentZone() =>
        DisplayZone?.Invoke()
        ?? ServerTab.PickerZone(ServerTimeHelper.CurrentDisplayMode, ServerTimeHelper.ActiveServerClock);

    /// <summary>
    /// The labels along the time axis (#4766): one at every whole wall-clock multiple of a step in
    /// <paramref name="zone"/> between the two instants (both ends inclusive), each as the instant it sits at and its
    /// wall time as "MM/dd HH:mm". The step is the finest one the chart axes use that leaves at most
    /// <paramref name="widthPx"/> / <paramref name="minSpacingPx"/> labels across the span (two at the least), so the
    /// labels sit on whole hours of the zone rather than at even fractions of a span that starts wherever the data
    /// does. A wall time that happens twice on the autumn change day gets a label at each occurrence, an hour apart in
    /// real time and reading the same; one the spring change skips gets none. Pure: it reads no control state.
    /// </summary>
    internal static IReadOnlyList<(DateTime Utc, string Text)> SlicerLabels(
        DateTime startUtc, DateTime endUtc, double widthPx, double minSpacingPx, TimeZoneInfo zone)
    {
        if (endUtc <= startUtc || widthPx <= 0 || minSpacingPx <= 0)
            return Array.Empty<(DateTime Utc, string Text)>();

        var target = Math.Max(2, (int)(widthPx / minSpacingPx));
        var step = DisplayZoneTickGenerator.ChooseStep(endUtc - startUtc, target);
        var ticks = UiDisplayZone.WallTicks(startUtc, endUtc, step, zone);
        var labels = new List<(DateTime Utc, string Text)>(ticks.Count);
        foreach (var (utc, wall) in ticks)
            labels.Add((utc, wall.ToString("MM/dd HH:mm", CultureInfo.InvariantCulture)));
        return labels;
    }

    // ── Drawing ──

    public void Redraw()
    {
        SlicerCanvas.Children.Clear();
        if (_data.Count < 1) return;

        var w = SlicerBorder.ActualWidth;
        var h = SlicerBorder.ActualHeight;
        if (w <= 0 || h <= 0) return;

        var values = _data.Select(d => d.Value).ToArray();
        var max = values.Max();
        if (max <= 0) max = 1;

        var chartTop = ChartPaddingTop;
        var chartBottom = h - ChartPaddingBottom;
        var chartHeight = chartBottom - chartTop;
        if (chartHeight <= 0) return;

        var n = values.Length;

        // Area chart — position points proportionally by time
        var linePoints = new List<Point>(n);
        for (int i = 0; i < n; i++)
        {
            var x = NormAtUtc(_data[i].BucketTime) * w;
            var y = chartBottom - (values[i] / max) * chartHeight;
            linePoints.Add(new Point(x, y));
        }

        var fillBrush = FindBrush("SlicerChartFillBrush", "#332EAEF1");
        var areaGeo = new StreamGeometry();
        using (var ctx = areaGeo.Open())
        {
            ctx.BeginFigure(new Point(linePoints[0].X, chartBottom), true, true);
            foreach (var pt in linePoints) ctx.LineTo(pt, true, false);
            ctx.LineTo(new Point(linePoints[^1].X, chartBottom), true, false);
        }
        SlicerCanvas.Children.Add(new Path { Data = areaGeo, Fill = fillBrush });

        var lineBrush = FindBrush("SlicerChartLineBrush", "#2EAEF1");
        var lineGeo = new StreamGeometry();
        using (var ctx = lineGeo.Open())
        {
            ctx.BeginFigure(linePoints[0], false, false);
            for (int i = 1; i < linePoints.Count; i++) ctx.LineTo(linePoints[i], true, false);
        }
        SlicerCanvas.Children.Add(new Path { Data = lineGeo, Stroke = lineBrush, StrokeThickness = 1.5 });

        // X-axis labels — whole wall-clock hours of the display zone (#4766), skip if too close
        var labelBrush = FindBrush("SlicerLabelBrush", "#E4E6EB");
        const double minLabelSpacingPx = 90;
        double lastLabelX = -minLabelSpacingPx;
        foreach (var (tickUtc, text) in SlicerLabels(DataStartUtc, DataEndUtc, w, minLabelSpacingPx, CurrentZone()))
        {
            var x = NormAtUtc(tickUtc) * w;
            if (x - lastLabelX < minLabelSpacingPx) continue;
            if (x < 10 || x > w - 40) continue;
            var tb = new TextBlock { Text = text, FontSize = 9, Foreground = labelBrush };
            Canvas.SetLeft(tb, x - 25);
            Canvas.SetTop(tb, chartBottom + 2);
            SlicerCanvas.Children.Add(tb);
            lastLabelX = x;
        }

        // Metric label top-right
        var metricBrush = FindBrush("SlicerToggleBrush", "#E4E6EB");
        var metricTb = new TextBlock { Text = _metricLabel, FontSize = 13, FontWeight = FontWeights.SemiBold, Foreground = metricBrush };
        Canvas.SetLeft(metricTb, w - 120);
        Canvas.SetTop(metricTb, 2);
        SlicerCanvas.Children.Add(metricTb);

        // Selection overlays
        var overlayBrush = FindBrush("SlicerOverlayBrush", "#99000000");
        var selectedBrush = FindBrush("SlicerSelectedBrush", "#22FFFFFF");
        var handleBrush = FindBrush("SlicerHandleBrush", "#E4E6EB");
        var handleHoverBrush = FindBrush("SlicerHandleHoverBrush", "#2EAEF1");

        var selLeft = _rangeStart * w;
        var selRight = _rangeEnd * w;

        if (selLeft > 0) AddRect(0, 0, selLeft, h, overlayBrush);
        if (selRight < w) AddRect(selRight, 0, w - selRight, h, overlayBrush);
        AddRect(selLeft, 0, Math.Max(0, selRight - selLeft), h, selectedBrush);

        DrawHandle(selLeft, h, _hoveredHandle == HandleHover.Start ? handleHoverBrush : handleBrush);
        DrawHandle(selRight - HandleWidthPx, h, _hoveredHandle == HandleHover.End ? handleHoverBrush : handleBrush);

        AddLine(selLeft, 0, selRight, 0, handleBrush, 0.5);
        AddLine(selLeft, h, selRight, h, handleBrush, 0.5);

        // Overlay dots (per-item highlight from grid selection)
        if (_overlayData != null && _overlayData.Count >= 1)
        {
            var overlayMax = _overlayData.Max(d => d.Value);
            if (overlayMax <= 0) overlayMax = 1;

            var dotBrush = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#FF6F00"));
            var firstBucket = _data[0].BucketTime;
            var lastBucket = _data[^1].BucketTime;
            foreach (var pt in _overlayData)
            {
                if (pt.TimeUtc < firstBucket || pt.TimeUtc > lastBucket) continue;
                var norm = NormAtUtc(pt.TimeUtc);
                var ox = norm * w;
                var oy = Math.Clamp(chartBottom - (pt.Value / overlayMax) * chartHeight, chartTop, chartBottom);
                var dot = new Ellipse { Width = 5, Height = 5, Fill = dotBrush };
                Canvas.SetLeft(dot, ox - 2.5);
                Canvas.SetTop(dot, oy - 2.5);
                SlicerCanvas.Children.Add(dot);
            }

            if (!string.IsNullOrEmpty(_overlayLabel))
            {
                var overlayLabelTb = new TextBlock
                {
                    Text = _overlayLabel,
                    FontSize = 11,
                    FontWeight = FontWeights.SemiBold,
                    Foreground = dotBrush
                };
                Canvas.SetLeft(overlayLabelTb, 8);
                Canvas.SetTop(overlayLabelTb, 2);
                SlicerCanvas.Children.Add(overlayLabelTb);
            }
        }
    }

    private void AddRect(double x, double y, double width, double height, Brush fill)
    {
        var rect = new Rectangle { Width = width, Height = height, Fill = fill };
        Canvas.SetLeft(rect, x); Canvas.SetTop(rect, y);
        SlicerCanvas.Children.Add(rect);
    }

    private void AddLine(double x1, double y1, double x2, double y2, Brush stroke, double opacity)
    {
        SlicerCanvas.Children.Add(new Line
        {
            X1 = x1, Y1 = y1, X2 = x2, Y2 = y2,
            Stroke = stroke, StrokeThickness = 1, Opacity = opacity
        });
    }

    private void DrawHandle(double x, double canvasHeight, Brush brush)
    {
        AddRect(x, 0, HandleWidthPx, canvasHeight, brush);
        ((Rectangle)SlicerCanvas.Children[^1]).Opacity = 0.7;
        var midY = canvasHeight / 2;
        for (int i = -1; i <= 1; i++)
        {
            SlicerCanvas.Children.Add(new Line
            {
                X1 = x + 2, Y1 = midY + i * 5, X2 = x + HandleWidthPx - 2, Y2 = midY + i * 5,
                Stroke = Brushes.Black, StrokeThickness = 1, Opacity = 0.6
            });
        }
    }

    private Brush FindBrush(string key, string fallbackHex)
    {
        if (TryFindResource(key) is Brush b) return b;
        return new SolidColorBrush((Color)ColorConverter.ConvertFromString(fallbackHex));
    }

    // ── Range label ──

    private void UpdateRangeLabel()
    {
        if (_data.Count == 0) { RangeLabel.Text = ""; return; }
        var zone = CurrentZone();
        var startDisplay = UiDisplayZone.Format(UtcAtNorm(_rangeStart), zone, "yyyy-MM-dd HH:mm");
        var endDisplay = UiDisplayZone.Format(UtcAtNorm(_rangeEnd), zone, "yyyy-MM-dd HH:mm");
        var spanHours = (UtcAtNorm(_rangeEnd) - UtcAtNorm(_rangeStart)).TotalHours;
        var spanLabel = spanHours >= 1 ? $"{spanHours:F0}h" : $"{spanHours * 60:F0}m";
        RangeLabel.Text = $"{startDisplay} \u2192 {endDisplay}  ({spanLabel})";
    }

    // ── Mouse interaction ──

    private void Toggle_Click(object sender, RoutedEventArgs e) => IsExpanded = !IsExpanded;

    /// <summary>
    /// Quick-range chip: selects the last N minutes of the loaded window (Tag = minutes), or the
    /// full window when Tag is 0 ("All"). A duration longer than the window clamps to the start.
    /// </summary>
    private void QuickRange_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button b || _data.Count < 2) return;
        if (b.Tag is not string tag || !int.TryParse(tag, out int minutes)) return;

        if (minutes <= 0)
        {
            _rangeStart = 0;
            _rangeEnd = 1.0;
        }
        else
        {
            var start = DataEndUtc.AddMinutes(-minutes);
            if (start < DataStartUtc) start = DataStartUtc;
            _rangeStart = NormAtUtc(start);
            _rangeEnd = 1.0;
        }

        UpdateRangeLabel();
        Redraw();
        FireRangeChanged();
    }

    private void Canvas_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        if (_data.Count < 2) return;
        var w = SlicerBorder.ActualWidth;
        if (w <= 0) return;
        var pos = e.GetPosition(SlicerCanvas);

        var selLeft = _rangeStart * w;
        var selRight = _rangeEnd * w;

        _dragOriginX = pos.X;
        _dragOriginRangeStart = _rangeStart;
        _dragOriginRangeEnd = _rangeEnd;

        if (Math.Abs(pos.X - selLeft) <= HandleGripWidthPx)
        { _dragMode = DragMode.DragStart; SlicerCanvas.CaptureMouse(); e.Handled = true; return; }
        if (Math.Abs(pos.X - selRight) <= HandleGripWidthPx)
        { _dragMode = DragMode.DragEnd; SlicerCanvas.CaptureMouse(); e.Handled = true; return; }
        if (pos.X >= selLeft && pos.X <= selRight)
        { _dragMode = DragMode.MoveRange; SlicerCanvas.CaptureMouse(); e.Handled = true; }
    }

    private void Canvas_MouseMove(object sender, MouseEventArgs e)
    {
        if (_data.Count < 2) return;
        var w = SlicerBorder.ActualWidth;
        if (w <= 0) return;
        var pos = e.GetPosition(SlicerCanvas);

        if (_dragMode == DragMode.None)
        {
            var selLeft = _rangeStart * w;
            var selRight = _rangeEnd * w;
            HandleHover newHover = HandleHover.None;
            if (Math.Abs(pos.X - selLeft) <= HandleGripWidthPx)
            { SlicerCanvas.Cursor = Cursors.SizeWE; newHover = HandleHover.Start; }
            else if (Math.Abs(pos.X - selRight) <= HandleGripWidthPx)
            { SlicerCanvas.Cursor = Cursors.SizeWE; newHover = HandleHover.End; }
            else if (pos.X >= selLeft && pos.X <= selRight)
                SlicerCanvas.Cursor = Cursors.SizeAll;
            else
                SlicerCanvas.Cursor = Cursors.Arrow;
            if (newHover != _hoveredHandle) { _hoveredHandle = newHover; Redraw(); }
            return;
        }

        var deltaNorm = (pos.X - _dragOriginX) / w;

        switch (_dragMode)
        {
            case DragMode.DragStart:
                _rangeStart = Math.Clamp(_dragOriginRangeStart + deltaNorm, 0, _rangeEnd - MinRangeNorm);
                break;
            case DragMode.DragEnd:
                _rangeEnd = Math.Clamp(_dragOriginRangeEnd + deltaNorm, _rangeStart + MinRangeNorm, 1);
                break;
            case DragMode.MoveRange:
                var span = _dragOriginRangeEnd - _dragOriginRangeStart;
                var newStart = _dragOriginRangeStart + deltaNorm;
                if (newStart < 0) newStart = 0;
                if (newStart + span > 1) newStart = 1 - span;
                _rangeStart = newStart;
                _rangeEnd = newStart + span;
                break;
        }

        UpdateRangeLabel();
        Redraw();
        e.Handled = true;
    }

    private void Canvas_MouseLeave(object sender, MouseEventArgs e)
    {
        if (_dragMode == DragMode.None && _hoveredHandle != HandleHover.None)
        {
            _hoveredHandle = HandleHover.None;
            Redraw();
        }
    }

    private void Canvas_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        if (_dragMode != DragMode.None)
        {
            _dragMode = DragMode.None;
            SlicerCanvas.ReleaseMouseCapture();
            FireRangeChanged();
            e.Handled = true;
        }
    }

    private void Canvas_MouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (_data.Count < 2) return;
        if (!Keyboard.Modifiers.HasFlag(ModifierKeys.Control)) return;

        var w = SlicerBorder.ActualWidth;
        if (w <= 0) return;

        var pos = e.GetPosition(SlicerCanvas);
        var pivot = Math.Clamp(pos.X / w, 0, 1);
        var span = _rangeEnd - _rangeStart;

        var zoomFactor = e.Delta > 0 ? 0.85 : 1.0 / 0.85;
        var newSpan = Math.Clamp(span * zoomFactor, MinRangeNorm, 1.0);

        var pivotInRange = (pivot - _rangeStart) / span;
        var newStart = pivot - pivotInRange * newSpan;
        var newEnd = newStart + newSpan;

        if (newStart < 0) { newStart = 0; newEnd = newSpan; }
        if (newEnd > 1) { newEnd = 1; newStart = 1 - newSpan; }

        _rangeStart = Math.Max(0, newStart);
        _rangeEnd = Math.Min(1, newEnd);

        UpdateRangeLabel();
        Redraw();
        FireRangeChanged();
        e.Handled = true;
    }

    private void FireRangeChanged()
    {
        if (_data.Count == 0) return;
        // Snap to hour boundaries so slider positions align with the hourly buckets shown in the graph
        var startUtc = FloorToHour(UtcAtNorm(_rangeStart));
        var endUtc = CeilToHour(UtcAtNorm(_rangeEnd));
        RangeChanged?.Invoke(this, new SlicerRangeEventArgs(startUtc, endUtc));
    }

    private static DateTime FloorToHour(DateTime dt) =>
        new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, dt.Kind);

    private static DateTime CeilToHour(DateTime dt)
    {
        var floored = FloorToHour(dt);
        return floored == dt ? dt : floored.AddHours(1);
    }
}

public class SlicerRangeEventArgs : EventArgs
{
    public DateTime StartUtc { get; }
    public DateTime EndUtc { get; }
    public SlicerRangeEventArgs(DateTime startUtc, DateTime endUtc) { StartUtc = startUtc; EndUtc = endUtc; }
}
