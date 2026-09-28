/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitor.PlanAnalysis;

using WpfPath = System.Windows.Shapes.Path;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The plan viewer's minimap: a scaled-down overview of the whole plan in a corner panel, with a
/// viewport box that tracks the main scroll viewer's scroll/zoom, and click-to-center. The math
/// (scale, node/subtree rectangles, viewport box, click-to-offset) lives in
/// <see cref="MinimapLayout"/>; this file only builds and positions the WPF shapes from it.
///
/// <para>Resize/persist, double-click zoom+select, and accuracy-colored minimap edges are not part
/// of this pass — the minimap edges use a fixed neutral color until the main canvas' accuracy
/// coloring is ported.</para>
/// </summary>
public partial class PlanViewerControl
{
    private readonly Dictionary<Border, PlanNode> _minimapNodeMap = new();
    private Border? _minimapViewportBox;
    private bool _minimapVisible;

    private void MinimapToggle_Click(object sender, RoutedEventArgs e)
    {
        if (_minimapVisible)
            CloseMinimapPanel();
        else
            OpenMinimapPanel();
    }

    private void OpenMinimapPanel()
    {
        _minimapVisible = true;
        MinimapPanel.Visibility = Visibility.Visible;
        RenderMinimap();
    }

    private void CloseMinimapPanel()
    {
        _minimapVisible = false;
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
        if (canvasW <= 0 || canvasH <= 0) return;

        var scale = MinimapLayout.GetScale(canvasW, canvasH, PlanCanvas.Width, PlanCanvas.Height);

        RenderMinimapBranches(_currentStatement.RootNode, scale);
        RenderMinimapEdges(_currentStatement.RootNode, scale);
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

    private void RenderMinimapEdges(PlanNode node, double scale)
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

            // Neutral color for now: accuracy-ratio coloring on the minimap lands in a later pass,
            // once the main canvas' own accuracy-colored edges (tracked separately) are in.
            var path = new WpfPath
            {
                Data = geometry,
                Stroke = EdgeBrush,
                StrokeThickness = thickness,
                StrokeLineJoin = PenLineJoin.Round
            };
            MinimapCanvas.Children.Add(path);

            RenderMinimapEdges(child, scale);
        }
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

        var (offsetX, offsetY) = MinimapLayout.ClickToScrollOffset(pos.X, pos.Y, scale, _zoomLevel, viewW, viewH);
        PlanScrollViewer.ScrollToHorizontalOffset(offsetX);
        PlanScrollViewer.ScrollToVerticalOffset(offsetY);
        e.Handled = true;
    }

    private void PlanScrollViewer_ScrollChanged(object sender, ScrollChangedEventArgs e)
    {
        UpdateMinimapViewportBox();
    }
}
