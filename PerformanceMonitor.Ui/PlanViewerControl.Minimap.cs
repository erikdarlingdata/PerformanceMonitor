/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using System.Windows.Threading;
using PerformanceMonitor.PlanAnalysis;

using WpfPath = System.Windows.Shapes.Path;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The plan viewer's minimap: a scaled-down overview of the whole plan in a corner panel, with a
/// viewport box that tracks the main scroll viewer's scroll/zoom, click-to-center, double-click to
/// zoom and select a node, a resizable panel whose size is remembered across plans, and edges
/// colored by the same actual/estimated accuracy ratio as the main canvas. The math (scale,
/// node/subtree rectangles, viewport box, click-to-offset, resize clamp, zoom-to-node) lives in
/// <see cref="MinimapLayout"/>; this file only builds and positions the WPF shapes from it.
/// </summary>
public partial class PlanViewerControl
{
    private readonly Dictionary<Border, PlanNode> _minimapNodeMap = new();
    private Border? _minimapViewportBox;
    private bool _minimapVisible;

    // Remembered across plans (and control instances) — matches PerformanceStudio's static fields,
    // so reopening the minimap on a different plan keeps the size the user last dragged it to.
    private static double _minimapWidth = 220;
    private static double _minimapHeight = 220;

    private bool _minimapResizing;
    private Point _minimapResizeStart;
    private double _minimapResizeStartWidth;
    private double _minimapResizeStartHeight;

    // #4622: set while a zero-size-canvas retry is queued via RenderMinimap's DispatcherPriority.Loaded
    // deferral, so a canvas that is still unsized when the retry runs gives up instead of re-queuing
    // itself forever (which would starve input). Cleared ONLY inside that retry's own posted callback --
    // CloseMinimapPanel must never clear it too (#4643): a close, then a reopen, both landing before a
    // pending retry runs, would clear the flag here while that retry is still queued, so its callback
    // would find the flag false and post a second retry, whose callback would find it false again and
    // post a third, chaining forever at Loaded priority (above Input) for as long as the canvas stays
    // unsized. A retry that finds the panel closed, or the canvas sized, by the time it runs just
    // returns and clears the flag itself, so the next real trigger (open, resize, statement render)
    // always gets a fresh attempt.
    private bool _minimapRenderDeferred;

    private void MinimapToggle_Click(object sender, RoutedEventArgs e)
    {
        if (_minimapVisible)
            CloseMinimapPanel();
        else
            OpenMinimapPanel();
    }

    private void MinimapClose_Click(object sender, RoutedEventArgs e)
    {
        CloseMinimapPanel();
    }

    private void OpenMinimapPanel()
    {
        _minimapVisible = true;
        MinimapPanel.Width = _minimapWidth;
        MinimapPanel.Height = _minimapHeight;
        MinimapPanel.Visibility = Visibility.Visible;
        RenderMinimap();
    }

    private void CloseMinimapPanel()
    {
        _minimapVisible = false;
        _minimapResizing = false;
        MinimapPanel.Visibility = Visibility.Collapsed;
    }

    /// <summary>Re-renders the minimap. Called after a statement render and after scroll/zoom changes while open.</summary>
    private void RenderMinimap()
    {
        MinimapCanvas.Children.Clear();
        _minimapNodeMap.Clear();
        _minimapViewportBox = null;

        if (!_minimapVisible) return;
        if (_currentStatement?.RootNode == null || PlanCanvas.Width <= 0 || PlanCanvas.Height <= 0) return;

        var canvasW = MinimapCanvas.ActualWidth;
        var canvasH = MinimapCanvas.ActualHeight;
        if (canvasW <= 0 || canvasH <= 0)
        {
            // First open: MinimapPanel starts Collapsed, so this first call lands before WPF has
            // measured MinimapCanvas, and ActualWidth/ActualHeight are still 0 -- a bare return here
            // left the panel empty until the user dragged the resize grip or closed/reopened it,
            // because nothing else re-renders it (#4622). Re-post one retry at DispatcherPriority.Loaded,
            // after the pending layout pass has sized the canvas, matching PerformanceStudio's Avalonia
            // port. The flag caps it at a single retry: if the canvas is still unsized when that runs
            // (e.g. the tab is hidden), give up instead of re-queuing forever, which would starve input.
            if (!_minimapRenderDeferred)
            {
                _minimapRenderDeferred = true;
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    RenderMinimap();
                    _minimapRenderDeferred = false;
                }), DispatcherPriority.Loaded);
            }
            return;
        }

        var scale = MinimapLayout.GetScale(canvasW, canvasH, PlanCanvas.Width, PlanCanvas.Height);

        var divergenceLimit = Math.Max(PlanEdgeColour.MinDivergenceLimit, AccuracyRatioDivergenceLimit);

        RenderMinimapBranches(_currentStatement.RootNode, scale);
        RenderMinimapEdges(_currentStatement.RootNode, scale, divergenceLimit);
        RenderMinimapNodes(_currentStatement.RootNode, scale);
        RenderMinimapViewportBox(scale);
    }

    private void RenderMinimapBranches(PlanNode root, double scale)
    {
        for (int i = 0; i < root.Children.Count; i++)
        {
            var child = root.Children[i];
            var (a, r, g, b) = MinimapLayout.BranchColorFor(i);
            var (x, y, width, height) = MinimapLayout.GetSubtreeRect(child, scale);

            var rect = new Rectangle
            {
                Width = width,
                Height = height,
                Fill = new SolidColorBrush(Color.FromArgb(a, r, g, b)),
                RadiusX = 2,
                RadiusY = 2
            };
            Canvas.SetLeft(rect, x);
            Canvas.SetTop(rect, y);
            MinimapCanvas.Children.Add(rect);
        }
    }

    private void RenderMinimapEdges(PlanNode node, double scale, double divergenceLimit)
    {
        foreach (var child in node.Children)
        {
            var parentRight = (node.X + PlanLayoutEngine.NodeWidth) * scale;
            var parentCenterY = (node.Y + PlanLayoutEngine.GetNodeHeight(node) / 2) * scale;
            var childLeft = child.X * scale;
            var childCenterY = (child.Y + PlanLayoutEngine.GetNodeHeight(child) / 2) * scale;
            var midX = (parentRight + childLeft) / 2;

            var rows = child.HasActualStats ? child.ActualRows : child.EstimateRows;
            var thickness = MinimapLayout.GetEdgeThickness(rows, scale);

            var geometry = new PathGeometry();
            var figure = new PathFigure { StartPoint = new Point(parentRight, parentCenterY), IsClosed = false };
            figure.Segments.Add(new LineSegment(new Point(midX, parentCenterY), true));
            figure.Segments.Add(new LineSegment(new Point(midX, childCenterY), true));
            figure.Segments.Add(new LineSegment(new Point(childLeft, childCenterY), true));
            geometry.Figures.Add(figure);

            var path = new WpfPath
            {
                Data = geometry,
                Stroke = GetLinkColorBrush(child, divergenceLimit),
                StrokeThickness = thickness,
                StrokeLineJoin = PenLineJoin.Round
            };
            MinimapCanvas.Children.Add(path);

            RenderMinimapEdges(child, scale, divergenceLimit);
        }
    }

    /// <summary>
    /// The minimap's own edge-color lookup, so it can pass a pre-clamped divergence limit down the
    /// recursion instead of re-clamping per edge. Matches <see cref="GetLinkColorBrush(PlanNode)"/>'s
    /// tier-to-brush mapping.
    /// </summary>
    private SolidColorBrush GetLinkColorBrush(PlanNode child, double clampedDivergenceLimit)
    {
        var key = PlanEdgeColour.ForChild(child, clampedDivergenceLimit);
        return key switch
        {
            PlanEdgeColourKey.LightOrange => EdgeLightOrangeBrush,
            PlanEdgeColourKey.FluoOrange => EdgeFluoOrangeBrush,
            PlanEdgeColourKey.FluoRed => EdgeFluoRedBrush,
            PlanEdgeColourKey.Blue => EdgeBlueBrush,
            PlanEdgeColourKey.LightBlue => EdgeLightBlueBrush,
            PlanEdgeColourKey.FluoBlue => EdgeFluoBlueBrush,
            _ => EdgeBrush,
        };
    }

    private void RenderMinimapNodes(PlanNode node, double scale)
    {
        var (x, y, width, height) = MinimapLayout.GetNodeRect(node, scale);

        var border = new Border
        {
            Width = width,
            Height = height,
            Background = node.IsExpensive
                ? new SolidColorBrush(Color.FromArgb(0x60, 0xE5, 0x73, 0x73))
                : (Brush)FindResource("BackgroundLightBrush"),
            BorderBrush = node.IsExpensive ? Brushes.OrangeRed : (Brush)FindResource("BorderBrush"),
            BorderThickness = new Thickness(0.5),
            CornerRadius = new CornerRadius(1)
        };

        var icon = PlanIconMapper.GetIcon(node.IconName);
        if (icon != null)
        {
            var iconSize = System.Math.Min(System.Math.Min(width * 0.7, height * 0.7), 16);
            if (iconSize >= 6)
            {
                border.Child = new Image
                {
                    Source = icon,
                    Width = iconSize,
                    Height = iconSize,
                    HorizontalAlignment = HorizontalAlignment.Center,
                    VerticalAlignment = VerticalAlignment.Center
                };
            }
        }

        Canvas.SetLeft(border, x);
        Canvas.SetTop(border, y);
        MinimapCanvas.Children.Add(border);

        _minimapNodeMap[border] = node;

        foreach (var child in node.Children)
            RenderMinimapNodes(child, scale);
    }

    private void RenderMinimapViewportBox(double scale)
    {
        var viewW = PlanScrollViewer.ActualWidth;
        var viewH = PlanScrollViewer.ActualHeight;
        if (viewW <= 0 || viewH <= 0) return;

        var (x, y, width, height) = MinimapLayout.GetViewportRect(
            scale, _zoomLevel, PlanCanvas.Width, PlanCanvas.Height,
            viewW, viewH, PlanScrollViewer.HorizontalOffset, PlanScrollViewer.VerticalOffset);

        var accentColor = (TryFindResource("AccentBrush") as SolidColorBrush)?.Color
            ?? Color.FromRgb(0x2E, 0xAE, 0xF1);
        var fillBrush = new SolidColorBrush(Color.FromArgb(0x40, accentColor.R, accentColor.G, accentColor.B));
        var borderBrush = new SolidColorBrush(Color.FromArgb(0xB0, accentColor.R, accentColor.G, accentColor.B));

        _minimapViewportBox = new Border
        {
            Width = width,
            Height = height,
            Background = fillBrush,
            BorderBrush = borderBrush,
            BorderThickness = new Thickness(1.5),
            CornerRadius = new CornerRadius(1),
            IsHitTestVisible = false
        };
        Canvas.SetLeft(_minimapViewportBox, x);
        Canvas.SetTop(_minimapViewportBox, y);
        MinimapCanvas.Children.Add(_minimapViewportBox);
    }

    /// <summary>
    /// Repositions/resizes the viewport box without a full re-render. Called from the main
    /// ScrollViewer's ScrollChanged and from SetZoom, so scrolling/zooming while the minimap is
    /// open tracks live instead of only updating on the next statement render.
    /// </summary>
    private void UpdateMinimapViewportBox()
    {
        if (!_minimapVisible || _minimapViewportBox == null || _currentStatement?.RootNode == null) return;
        if (PlanCanvas.Width <= 0 || PlanCanvas.Height <= 0) return;

        var canvasW = MinimapCanvas.ActualWidth;
        var canvasH = MinimapCanvas.ActualHeight;
        if (canvasW <= 0 || canvasH <= 0) return;

        var scale = MinimapLayout.GetScale(canvasW, canvasH, PlanCanvas.Width, PlanCanvas.Height);
        var viewW = PlanScrollViewer.ActualWidth;
        var viewH = PlanScrollViewer.ActualHeight;
        if (viewW <= 0 || viewH <= 0) return;

        var (x, y, width, height) = MinimapLayout.GetViewportRect(
            scale, _zoomLevel, PlanCanvas.Width, PlanCanvas.Height,
            viewW, viewH, PlanScrollViewer.HorizontalOffset, PlanScrollViewer.VerticalOffset);

        _minimapViewportBox.Width = width;
        _minimapViewportBox.Height = height;
        Canvas.SetLeft(_minimapViewportBox, x);
        Canvas.SetTop(_minimapViewportBox, y);
    }

    private void MinimapCanvas_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        if (_currentStatement?.RootNode == null || PlanCanvas.Width <= 0 || PlanCanvas.Height <= 0) return;

        var canvasW = MinimapCanvas.ActualWidth;
        var canvasH = MinimapCanvas.ActualHeight;
        if (canvasW <= 0 || canvasH <= 0) return;

        var scale = MinimapLayout.GetScale(canvasW, canvasH, PlanCanvas.Width, PlanCanvas.Height);
        var pos = e.GetPosition(MinimapCanvas);
        var viewW = PlanScrollViewer.ActualWidth;
        var viewH = PlanScrollViewer.ActualHeight;

        // Double-click on a node zooms to it and selects it in the main canvas, matching
        // PerformanceStudio. A single click anywhere (node or not) centers the viewport there.
        if (e.ClickCount == 2)
        {
            var node = MinimapLayout.FindNodeAt(_currentStatement.RootNode, pos.X, pos.Y, scale);
            if (node != null)
            {
                ZoomToMinimapNode(node, viewW, viewH);
                e.Handled = true;
                return;
            }
        }

        var (offsetX, offsetY) = MinimapLayout.ClickToScrollOffset(pos.X, pos.Y, scale, _zoomLevel, viewW, viewH);
        PlanScrollViewer.ScrollToHorizontalOffset(offsetX);
        PlanScrollViewer.ScrollToVerticalOffset(offsetY);
        e.Handled = true;
    }

    /// <summary>
    /// Double-click-to-zoom: sets the main canvas' zoom so the node takes about a third of the
    /// viewport, selects it (same as clicking the node directly on the main canvas), then centers
    /// it, matching PerformanceStudio's <c>ZoomToNode</c>.
    /// </summary>
    private void ZoomToMinimapNode(PlanNode node, double viewportWidth, double viewportHeight)
    {
        var fitZoom = MinimapLayout.GetZoomToNodeLevel(
            PlanLayoutEngine.NodeWidth, PlanLayoutEngine.GetNodeHeight(node), viewportWidth, viewportHeight, MinZoom, MaxZoom);
        SetZoom(fitZoom);

        // Select before centering, not after: SelectNode opens the properties panel when it wasn't
        // already open, and that panel takes ~400px from the right of PlanScrollViewer. Centering
        // against viewportWidth/viewportHeight (the size BEFORE that panel opens) put the node 201px
        // right of center when the panel was closed, and left a smaller residual even when it was
        // already open, because a zoom change alone can toggle a scrollbar and shift ViewportWidth by
        // a few more pixels (#4622). Deferring to DispatcherPriority.Loaded and reading
        // PlanScrollViewer.ViewportWidth/Height there, after SelectNode's layout has settled, centers
        // on the viewport the user actually ends up looking at either way.
        foreach (var child in PlanCanvas.Children)
        {
            if (child is Border b && b.Tag is PlanNode n && n == node)
            {
                SelectNode(b, n);
                break;
            }
        }

        Dispatcher.BeginInvoke(new Action(() =>
        {
            var (offsetX, offsetY) = MinimapLayout.GetNodeCenterOffset(
                node.X, node.Y, PlanLayoutEngine.NodeWidth, PlanLayoutEngine.GetNodeHeight(node),
                _zoomLevel, PlanScrollViewer.ViewportWidth, PlanScrollViewer.ViewportHeight);
            PlanScrollViewer.ScrollToHorizontalOffset(offsetX);
            PlanScrollViewer.ScrollToVerticalOffset(offsetY);
        }), DispatcherPriority.Loaded);
    }

    private void PlanScrollViewer_ScrollChanged(object sender, ScrollChangedEventArgs e)
    {
        UpdateMinimapViewportBox();
    }

    /* Resize drags are measured against this control (the UserControl), NOT against MinimapPanel.
       The panel is pinned to the bottom-right, so growing it moves its own top-left corner — and
       the grip lives in that corner. Measured in the panel's own coordinates the grip would
       therefore sit still while the pointer moved, and the drag would fight itself. This control
       does not move, so deltas taken from it mean what they say. */
    private void MinimapResizeGrip_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        _minimapResizing = true;
        _minimapResizeStart = e.GetPosition(this);
        _minimapResizeStartWidth = MinimapPanel.Width;
        _minimapResizeStartHeight = MinimapPanel.Height;
        ((UIElement)sender).CaptureMouse();
        e.Handled = true;
    }

    private void MinimapResizeGrip_MouseMove(object sender, MouseEventArgs e)
    {
        if (!_minimapResizing) return;

        if (e.LeftButton != MouseButtonState.Pressed)
        {
            _minimapResizing = false;
            return;
        }

        var current = e.GetPosition(this);
        var deltaX = current.X - _minimapResizeStart.X;
        var deltaY = current.Y - _minimapResizeStart.Y;
        var (newWidth, newHeight) = MinimapLayout.ResizeFromDrag(_minimapResizeStartWidth, _minimapResizeStartHeight, deltaX, deltaY);

        MinimapPanel.Width = newWidth;
        MinimapPanel.Height = newHeight;
        _minimapWidth = newWidth;
        _minimapHeight = newHeight;
        e.Handled = true;

        RenderMinimap();
    }

    private void MinimapResizeGrip_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        if (!_minimapResizing) return;
        _minimapResizing = false;
        ((UIElement)sender).ReleaseMouseCapture();
        e.Handled = true;
        RenderMinimap();
    }
}
