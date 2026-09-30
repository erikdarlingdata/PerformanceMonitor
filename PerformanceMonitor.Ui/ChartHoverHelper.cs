using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Adds mouse-hover tooltips to a ScottPlot chart with multiple scatter series.
/// Shows the series name, value, and timestamp in a popup that follows the mouse.
/// Uses X-axis (time) proximity for reliable detection on time-series charts.
///
/// Also owns the shared <b>click-to-isolate</b> mechanic: clicking a series' built-in legend key
/// (or its line) dims every other series and auto-fits the Y axis to the clicked one, so a series
/// that is flat/hidden under the big lines becomes readable; clicking it again (or an empty area)
/// restores the full view. A re-render (<see cref="Clear"/>) resets isolate. The mechanic lives
/// here because every dynamic-legend chart in both apps already funnels its series through this
/// helper, so all charts get it from one change.
/// </summary>
internal sealed class ChartHoverHelper
{
    /// <summary>A registered series: the scatter, its full (untruncated) label, and the unmutated
    /// identity color captured at registration (see <see cref="Add"/>).</summary>
    internal readonly record struct SeriesEntry(
        ScottPlot.Plottables.Scatter Scatter, string Label, ScottPlot.Color Identity,
        ScottPlot.Color OrigLineColor, float OrigLineWidth, float OrigMarkerSize, bool OrigFillY);

    private readonly ScottPlot.WPF.WpfPlot _chart;
    private readonly List<SeriesEntry> _series = new();
    private readonly List<(ScottPlot.Plottables.BarPlot BarPlot, string Label)> _barPlots = new();
    private readonly Popup _popup;
    private readonly TextBlock _text;
    private string _unit;
    private DateTime _lastUpdate;
    private bool _needsReanchor = true;

    // ── Click-to-isolate state ─────────────────────────────────────────────────────────────────
    private readonly bool _enableClickIsolate;
    private string? _isolatedLabel;                              // null = nothing isolated
    private bool _leftPressed;
    private Point _pressPos;
    private bool _suppressNextLeftUp;                          // set on double-click so the 2nd up can't re-isolate

    /// <summary>The faint line/marker alpha applied to non-isolated series while isolated.</summary>
    internal const byte DimAlpha = 40;

    /// <summary>How far (device-independent px) the mouse may move between press and release and
    /// still count as a click rather than a pan-drag.</summary>
    private const double ClickDragThresholdPx = 5.0;

    /// <summary>Chart → helper lookup so the per-app autoscale handlers (which are static and hold
    /// no helper reference) can clear an active isolate before rescaling. Weak-keyed: entries
    /// disappear when the chart is collected.</summary>
    private static readonly ConditionalWeakTable<ScottPlot.WPF.WpfPlot, ChartHoverHelper> _registry = new();

    /// <summary>
    /// The display zone when the chart's X is the UTC instant (#4766), else <c>null</c>: X is then the server-time
    /// value the app plotted and the text goes through <see cref="UiTimeContext.ConvertForDisplay"/> as before.
    /// </summary>
    private readonly Func<TimeZoneInfo>? _displayZone;

    /// <summary>
    /// Says whether a plotted X is one whose stored wall time names two instants (#4766): the server's repeated
    /// hour, where the instant the X sits at is only the first of the two and cannot say which pass the data was.
    /// For such an X the hover words the time without the repeated-hour UTC offset, which would name a pass the data
    /// never recorded. An X whose stored wall time happens once is an exact instant and keeps its offset. The
    /// argument is the plotted X as <see cref="DateTime.FromOADate"/> reads it. Only used with a display zone;
    /// <see cref="Clear"/> sets it back to <c>null</c>, so a predicate never outlives the data it was built for.
    /// </summary>
    public Func<DateTime, bool>? PlainTimeAt { get; set; }

    /// <param name="chart">The chart to attach to.</param>
    /// <param name="unit">The unit printed after a value.</param>
    /// <param name="enableClickIsolate">Whether a click isolates the series under the cursor.</param>
    /// <param name="displayZone">
    /// Set when the chart plots the UTC instant as X: the hover time is then that instant in the zone, with the
    /// UTC offset added when the wall time is ambiguous (<see cref="DisplayZone.Format"/>). Read on every hover,
    /// so a display-mode switch shows on the next move. Leave it out and the hover behaves exactly as it always has.
    /// </param>
    public ChartHoverHelper(ScottPlot.WPF.WpfPlot chart, string unit, bool enableClickIsolate = true,
        Func<TimeZoneInfo>? displayZone = null)
    {
        _chart = chart;
        _unit = unit;
        _enableClickIsolate = enableClickIsolate;
        _displayZone = displayZone;

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

        /* Tab switching can leave the popup wedged: WPF unloads the parent TabItem
           without firing MouseLeave, so IsOpen stays true with a stale anchor.
           When the chart becomes visible again, OnMouseMove sets IsOpen = true
           but it is already true, so the popup never re-anchors and never shows.
           Force-close on every visibility/load transition so the next mouse move
           re-opens cleanly. */
        chart.IsVisibleChanged += OnChartVisibilityChanged;
        chart.Unloaded += OnChartUnloaded;
        chart.Loaded += OnChartLoaded;

        if (_enableClickIsolate)
        {
            // Preview-down records the press point before ScottPlot's pan logic; bubbling Up decides
            // click-vs-drag. We never set e.Handled, so pan/zoom/right-click keep working.
            chart.PreviewMouseLeftButtonDown += OnPreviewLeftButtonDown;
            chart.MouseLeftButtonUp += OnLeftButtonUp;
            // Double-click is the autoscale/restore gesture (the per-app handler restores). Flag it so
            // the second mouse-up can't re-isolate — deterministic, vs. relying on e.ClickCount on the up.
            chart.MouseDoubleClick += OnDoubleClick;
        }

        _registry.AddOrUpdate(chart, this);
    }

    public string Unit { get => _unit; set => _unit = value; }

    public void Dispose()
    {
        _chart.MouseMove -= OnMouseMove;
        _chart.MouseLeave -= OnMouseLeave;
        _chart.IsVisibleChanged -= OnChartVisibilityChanged;
        _chart.Unloaded -= OnChartUnloaded;
        _chart.Loaded -= OnChartLoaded;
        _chart.PreviewMouseLeftButtonDown -= OnPreviewLeftButtonDown;
        _chart.MouseLeftButtonUp -= OnLeftButtonUp;
        _chart.MouseDoubleClick -= OnDoubleClick;
        _registry.Remove(_chart);
        _popup.IsOpen = false;
        _series.Clear();
        _barPlots.Clear();
    }

    private void OnChartVisibilityChanged(object sender, DependencyPropertyChangedEventArgs e) => ForceReanchor();
    private void OnChartUnloaded(object sender, RoutedEventArgs e) => ForceReanchor();
    private void OnChartLoaded(object sender, RoutedEventArgs e) => ForceReanchor();

    /* A tab visibility/load transition can wedge the popup open with a stale anchor; flag a
       re-anchor so the next mouse move toggles it once instead of toggling on every move. */
    private void ForceReanchor()
    {
        _popup.IsOpen = false;
        _needsReanchor = true;
    }

    public void Clear()
    {
        // A re-render resets isolate.
        _isolatedLabel = null;
        _series.Clear();
        _barPlots.Clear();
        PlainTimeAt = null;
    }

    public void Add(ScottPlot.Plottables.Scatter scatter, string label) =>
        /* Capture the IDENTITY color from the marker fill, NOT scatter.Color: Add runs after
           ChartStyle.StyleScatter, which has already mutated the line color to identity.WithAlpha(215)
           but never touches the marker fill — so MarkerStyle.FillColor still holds the pure identity
           used to dim/restore this series. Also snapshot the full visual state as the chart left it, so
           restore is faithful for line-only charts (CollectorDuration / trend charts use MarkerSize 0,
           no fill, and never call StyleScatter) as well as the StyleScatter'd fill charts. */
        _series.Add(new SeriesEntry(
            scatter, label, scatter.MarkerStyle.FillColor,
            scatter.LineColor, scatter.LineWidth, scatter.MarkerSize, scatter.FillY));

    public void Add(ScottPlot.Plottables.BarPlot barPlot, string label) =>
        _barPlots.Add((barPlot, label));

    /// <summary>
    /// Returns the nearest series label and data-point time for the given mouse position,
    /// or null if no series is close enough.
    /// </summary>
    public (string Label, DateTime Time)? GetNearestSeries(Point mousePos)
    {
        if (_series.Count == 0 && _barPlots.Count == 0) return null;
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

            foreach (var entry in _series)
            {
                var nearest = entry.Scatter.Data.GetNearest(mouseCoords, _chart.Plot.LastRender);
                if (!nearest.IsReal) continue;
                var nearestPixel = _chart.Plot.GetPixel(
                    new ScottPlot.Coordinates(nearest.X, nearest.Y));
                double dx = Math.Abs(nearestPixel.X - pixel.X);
                double dy = Math.Abs(nearestPixel.Y - pixel.Y);
                if (dx < 80 && dy < bestYDistance)
                {
                    bestYDistance = dy;
                    bestPoint = nearest;
                    bestLabel = entry.Label;
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
            /* Bar width in pixels is the same for every bar on a linear axis, so compute it once
               per plot (lazily on the first bar) instead of two GetPixel calls per bar. */
            double? halfWidthPx = null;
            foreach (var bar in barPlot.Bars)
            {
                halfWidthPx ??= Math.Abs(
                    _chart.Plot.GetPixel(new ScottPlot.Coordinates(bar.Size / 2, 0)).X
                    - _chart.Plot.GetPixel(new ScottPlot.Coordinates(0, 0)).X);
                var topPixel = _chart.Plot.GetPixel(new ScottPlot.Coordinates(bar.Position, bar.Value));
                double dx = Math.Abs(topPixel.X - pixel.X);
                if (dx > halfWidthPx.Value + 4) continue;
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
        if (_series.Count == 0 && _barPlots.Count == 0) return;
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

            foreach (var entry in _series)
            {
                var nearest = entry.Scatter.Data.GetNearest(mouseCoords, _chart.Plot.LastRender);
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
                    bestLabel = entry.Label;
                    found = true;
                }
            }

            FindNearestBar(pixel, ref bestYDistance, ref bestPoint, ref bestLabel, ref found);

            if (found)
            {
                var plottedX = DateTime.FromOADate(bestPoint.X);
                var time = FormatHoverTime(plottedX, _displayZone, PlainTimeAt?.Invoke(plottedX) == true);
                string valueFormatted = (bestPoint.Y == Math.Floor(bestPoint.Y))
                    ? bestPoint.Y.ToString("N0")
                    : bestPoint.Y.ToString("N1");
                _text.Text = $"{bestLabel}\n{valueFormatted} {_unit}\n{time}";
                _popup.HorizontalOffset = pos.X + 15;
                _popup.VerticalOffset = pos.Y + 15;
                /* Updating the offsets above moves an already-open popup, so only toggle IsOpen
                   when a re-anchor is actually needed (a tab visibility/load transition wedged it).
                   Toggling every move tore down and recreated the popup's native window each frame. */
                if (_needsReanchor)
                {
                    if (_popup.IsOpen) _popup.IsOpen = false;
                    _popup.IsOpen = true;
                    _needsReanchor = false;
                }
                else if (!_popup.IsOpen)
                {
                    _popup.IsOpen = true;
                }
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

    /// <summary>
    /// The time line of a hover for the plotted X <paramref name="plottedX"/>. With no zone, X is the app's
    /// server-time value and the line is the display-mode conversion of it, as it has always been. With a zone, X
    /// is the UTC instant: the line is that instant in the zone, and the zone's offset follows it when the wall
    /// time happens twice. With a zone and <paramref name="plain"/> (the X's stored wall time names two instants,
    /// <see cref="PlainTimeAt"/>) the line is the same wall time without that offset; without a zone
    /// <paramref name="plain"/> changes nothing.
    /// </summary>
    internal static string FormatHoverTime(DateTime plottedX, Func<TimeZoneInfo>? displayZone, bool plain = false)
        => displayZone is null
            ? UiTimeContext.ConvertForDisplay(plottedX).ToString("HH:mm:ss")
            : plain
                ? DisplayZone.ToDisplay(plottedX, displayZone()).ToString("HH:mm:ss", CultureInfo.InvariantCulture)
                : DisplayZone.Format(plottedX, displayZone(), "HH:mm:ss");

    // ── Click-to-isolate ───────────────────────────────────────────────────────────────────────

    /// <summary>Finds the live helper wrapping a chart, if one is registered. Used by the per-app
    /// autoscale handlers to clear an active isolate before rescaling.</summary>
    internal static bool TryGetForChart(ScottPlot.WPF.WpfPlot chart, out ChartHoverHelper helper)
        => _registry.TryGetValue(chart, out helper!);

    private void OnPreviewLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        _leftPressed = true;
        _pressPos = e.GetPosition(_chart);
    }

    // Fires on the 2nd down of a double-click, before the terminal up — so the up below is suppressed.
    private void OnDoubleClick(object sender, MouseButtonEventArgs e) => _suppressNextLeftUp = true;

    private void OnLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        // The 2nd-down's PreviewMouseLeftButtonDown is marked Handled by Control.HandleDoubleClick, so our
        // press handler is skipped and _leftPressed is already false on a double-click's terminal up.
        // Consume the suppress flag (set by OnDoubleClick) BEFORE the _leftPressed gate, or it sticks and
        // swallows the next genuine click. This flag is the deterministic re-isolate guard; double-click
        // is the autoscale/restore gesture (SetupChartContextMenu calls Restore()) and must not re-isolate.
        if (_suppressNextLeftUp) { _suppressNextLeftUp = false; _leftPressed = false; return; }
        if (!_leftPressed) return;
        _leftPressed = false;

        if (e.ClickCount != 1) return;   // cheap secondary guard for a double-click's terminal up

        var pos = e.GetPosition(_chart);
        double dx = pos.X - _pressPos.X;
        double dy = pos.Y - _pressPos.Y;
        // A drag is a pan, not a click — ignore it (don't isolate).
        if ((dx * dx + dy * dy) > (ClickDragThresholdPx * ClickDragThresholdPx)) return;

        try { HandleLeftClick(pos); }
        catch { /* never break the input pipeline on a hit-test edge case */ }
    }

    private void HandleLeftClick(Point pos)
    {
        var dpi = VisualTreeHelper.GetDpi(_chart);
        var clickPixel = new ScottPlot.Pixel(
            (float)(pos.X * dpi.DpiScaleX),
            (float)(pos.Y * dpi.DpiScaleY));

        // Branch ONCE on legend-panel containment. Inside the panel → legend hit-test only (never
        // fall through to the line hit-test); else → line hit-test.
        if (TryLegendHitTest(clickPixel, out var plottable))
        {
            if (plottable is not null)
            {
                // A real legend key. Map to a registered series by reference (labels are truncated
                // in the legend, so never match by string). A key that maps to no scatter — e.g. a
                // bar plot — is a deliberate no-op (bars are deferred).
                var label = LabelForPlottable(plottable);
                if (label is not null) ToggleIsolate(label);
            }
            else if (_isolatedLabel is not null)
            {
                // Click landed in the legend padding band (no key) — treat like an empty-area click.
                Restore();
            }
            return;
        }

        // Outside the legend panel: hit-test the lines.
        var hit = GetNearestSeries(pos);
        if (hit is not null) ToggleIsolate(hit.Value.Label);
        else if (_isolatedLabel is not null) Restore();
    }

    /// <summary>
    /// Hit-tests the click against the bottom legend's per-item label/symbol rectangles.
    /// Returns true when the click is anywhere inside the legend panel (the caller must then NOT
    /// fall through to the line hit-test); <paramref name="plottable"/> is the matched item's
    /// plottable (possibly null for a manual legend item, or when the click was in the padding band
    /// between keys). Returns false when there is no bottom legend or the click is outside it.
    /// Recipe mirrors ScottPlot's own LegendPanel.Render path at 5.1.58.
    /// </summary>
    private bool TryLegendHitTest(ScottPlot.Pixel clickPixel, out ScottPlot.IPlottable? plottable)
    {
        plottable = null;

        var lr = _chart.Plot.LastRender;
        if (lr.Count == 0) return false;                                  // not yet rendered
        var layout = lr.Layout;

        var panel = layout.PanelSizes.Keys.OfType<ScottPlot.Panels.LegendPanel>().FirstOrDefault();
        if (panel is null) return false;                                  // no bottom legend on this chart

        using var paint = ScottPlot.Paint.NewDisposablePaint();
        var rect = panel.GetPanelRect(layout.DataRect, layout.PanelSizes[panel], layout.PanelOffsets[panel], paint);
        if (!rect.Contains(clickPixel)) return false;                     // click is not in the legend panel

        // Inside the legend panel from here on. The tight layout is anchored at origin (0,0); align
        // it inside the panel rect exactly as LegendPanel.Render does, then offset each item rect once.
        var tight = _chart.Plot.Legend.GetLayout(rect.Size, paint);
        var placed = tight.LegendRect.AlignedInside(rect, panel.Alignment);
        var off = new ScottPlot.PixelOffset(placed.Left, placed.Top);

        for (int i = 0; i < tight.LegendItems.Length; i++)
        {
            if (tight.LabelRects[i].WithOffset(off).Contains(clickPixel) ||
                tight.SymbolRects[i].WithOffset(off).Contains(clickPixel))
            {
                plottable = tight.LegendItems[i].Plottable;               // may be null (manual item)
                return true;
            }
        }
        return true;                                                      // in panel, but on no key
    }

    private string? LabelForPlottable(ScottPlot.IPlottable plottable)
    {
        foreach (var entry in _series)
            if (ReferenceEquals(entry.Scatter, plottable))
                return entry.Label;
        return null;
    }

    private void ToggleIsolate(string label)
    {
        if (NextIsolate(_isolatedLabel, label) is null)
            Restore();
        else
            Isolate(label);
    }

    private void Isolate(string label)
    {
        if (_series.Count == 0) return;

        // Dim every other series; leave the isolated one at its true original look. The Y axis is left
        // untouched — isolate is a focus aid, not a zoom. (To inspect a buried series, deselect the big
        // ones in the picker, which re-renders + autoscales.)
        foreach (var entry in _series)
        {
            var visual = ResolveSeriesVisual(label, entry.Label);
            if (visual.Dim)
            {
                // Faint line + marker, and drop the gradient fill ribbon so it doesn't stay vivid.
                entry.Scatter.Color = entry.Identity.WithAlpha(visual.LineAlpha);
                entry.Scatter.FillY = visual.FillRibbon;   // false while dimmed
            }
            else
            {
                // The isolated series back at its true original look (faithful for line-only charts too).
                RestoreSeriesVisual(entry);
            }
        }

        _isolatedLabel = label;
        _chart.Refresh();
    }

    /// <summary>Un-dims every series back to the full multi-series view. A no-op when nothing is isolated
    /// (so the per-app autoscale hook can call it unconditionally).</summary>
    internal void Restore()
    {
        if (_isolatedLabel is null) return;

        foreach (var entry in _series)
            RestoreSeriesVisual(entry);                                   // faithful for fill + line-only charts
        _isolatedLabel = null;
        _chart.Refresh();
    }

    /// <summary>Returns one series to its captured original look. Fill charts (OrigFillY true) are
    /// rebuilt by StyleScatter — it regenerates the gradient from the unchanged data. Line-only charts
    /// (no fill: CollectorDuration / trend charts use MarkerSize 0 and never call StyleScatter) get their
    /// captured line + marker values written back directly; running StyleScatter on those would wrongly
    /// add density markers and a fill ribbon they never had.</summary>
    internal static void RestoreSeriesVisual(in SeriesEntry e)
    {
        if (e.OrigFillY)
        {
            e.Scatter.Color = e.Identity;
            ChartStyle.StyleScatter(e.Scatter);
        }
        else
        {
            // Color sets line + marker to the opaque identity; then put the captured line look back.
            e.Scatter.Color = e.Identity;
            e.Scatter.LineColor = e.OrigLineColor;
            e.Scatter.LineWidth = e.OrigLineWidth;
            e.Scatter.MarkerSize = e.OrigMarkerSize;
            e.Scatter.FillY = false;
        }
    }

    // ── Pure helpers (unit tested; no live WpfPlot needed) ──────────────────────────────────────

    /// <summary>The next isolate target given the current one and a freshly clicked label: clicking
    /// the already-isolated series toggles OFF (null); clicking any other series isolates it.</summary>
    internal static string? NextIsolate(string? current, string clicked)
        => string.Equals(current, clicked, StringComparison.Ordinal) ? null : clicked;

    /// <summary>How a series should look under a given isolate state: the target (or every series
    /// when nothing is isolated) renders full with its fill ribbon; all others dim with no fill.</summary>
    internal static IsolateVisual ResolveSeriesVisual(string? isolatedLabel, string seriesLabel)
        => (isolatedLabel is not null && !string.Equals(isolatedLabel, seriesLabel, StringComparison.Ordinal))
            ? IsolateVisual.Dimmed
            : IsolateVisual.Full;

    /// <summary>The visual decision for one series under isolate. <see cref="Dim"/> false means the
    /// series keeps its identity color and fill; true means line/marker drop to <see cref="DimAlpha"/>
    /// and the fill ribbon is removed.</summary>
    internal readonly record struct IsolateVisual(bool Dim, byte LineAlpha, bool FillRibbon)
    {
        public static readonly IsolateVisual Dimmed = new(true, DimAlpha, false);
        public static readonly IsolateVisual Full = new(false, 255, true);
    }

}
