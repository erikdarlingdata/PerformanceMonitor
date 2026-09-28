/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Pure geometry for the plan viewer's minimap: the scale from the minimap canvas and full plan
/// sizes, node and subtree rectangles in minimap space, the viewport box from the main scroll
/// viewer's offset/zoom/size, and the click-to-center scroll offset. No UI types: the WPF
/// control converts these numbers into shapes.
/// </summary>
public static class MinimapLayout
{
    /// <summary>The 8-color cycle used for translucent per-root-child subtree backgrounds, as ARGB bytes.</summary>
    public static readonly (byte A, byte R, byte G, byte B)[] BranchColors =
    {
        (0x30, 0x4F, 0xA3, 0xFF), // blue
        (0x30, 0x7B, 0xCF, 0x7B), // green
        (0x30, 0xFF, 0xB3, 0x47), // orange
        (0x30, 0xE5, 0x73, 0x73), // red
        (0x30, 0xCF, 0x7B, 0xCF), // purple
        (0x30, 0x7B, 0xCF, 0xCF), // teal
        (0x30, 0xFF, 0xE0, 0x4F), // yellow
        (0x30, 0xFF, 0x7B, 0xA5), // pink
    };

    /// <summary>The background tint for an "expensive" node's minimap rectangle, as ARGB bytes.</summary>
    public static readonly (byte A, byte R, byte G, byte B) ExpensiveNodeBackground = (0x60, 0xE5, 0x73, 0x73);

    /// <summary>The branch color for the i-th root child, cycling through <see cref="BranchColors"/>.</summary>
    public static (byte A, byte R, byte G, byte B) BranchColorFor(int rootChildIndex)
        => BranchColors[rootChildIndex % BranchColors.Length];

    /// <summary>
    /// The minimap scale: the canvas is shrunk to fit inside the minimap panel while preserving
    /// aspect ratio, so the binding dimension (width or height) sets the scale for both axes.
    /// Returns 1 when either the canvas or the plan has no usable size.
    /// </summary>
    public static double GetScale(double minimapCanvasWidth, double minimapCanvasHeight, double planWidth, double planHeight)
    {
        if (planWidth <= 0 || planHeight <= 0) return 1;
        if (minimapCanvasWidth <= 0 || minimapCanvasHeight <= 0) return 1;

        var scaleX = minimapCanvasWidth / planWidth;
        var scaleY = minimapCanvasHeight / planHeight;
        return Math.Min(scaleX, scaleY);
    }

    /// <summary>
    /// Collects the bounding box of a subtree in plan coordinates: min/max of each node's X, and
    /// min Y down to the deepest node's bottom edge (Y + its rendered height). Matches the main
    /// canvas' node placement, so the box lines up with the full-size subtree.
    /// </summary>
    public static void CollectSubtreeBounds(PlanNode node, ref double minX, ref double minY, ref double maxX, ref double maxY)
    {
        if (node.X < minX) minX = node.X;
        if (node.Y < minY) minY = node.Y;
        if (node.X > maxX) maxX = node.X;

        var bottom = node.Y + PlanLayoutEngine.GetNodeHeight(node);
        if (bottom > maxY) maxY = bottom;

        foreach (var child in node.Children)
            CollectSubtreeBounds(child, ref minX, ref minY, ref maxX, ref maxY);
    }

    /// <summary>
    /// The minimap-space rectangle for a root child's whole subtree (the translucent branch
    /// background). A 2px margin on each side (4px total on width/height) keeps the rectangle
    /// from hugging the node edges exactly.
    /// </summary>
    public static (double X, double Y, double Width, double Height) GetSubtreeRect(PlanNode rootChild, double scale)
    {
        double minX = double.MaxValue, minY = double.MaxValue;
        double maxX = double.MinValue, maxY = double.MinValue;
        CollectSubtreeBounds(rootChild, ref minX, ref minY, ref maxX, ref maxY);

        var width = (maxX - minX + PlanLayoutEngine.NodeWidth) * scale + 4;
        var height = (maxY - minY + PlanLayoutEngine.GetNodeHeight(rootChild)) * scale + 4;
        var x = minX * scale - 2;
        var y = minY * scale - 2;
        return (x, y, width, height);
    }

    /// <summary>
    /// The minimap-space rectangle for one node's small representation. Never smaller than 4px on
    /// either axis, so a node is still clickable/visible at a heavily shrunk scale.
    /// </summary>
    public static (double X, double Y, double Width, double Height) GetNodeRect(PlanNode node, double scale)
    {
        var x = node.X * scale;
        var y = node.Y * scale;
        var width = Math.Max(4, PlanLayoutEngine.NodeWidth * scale);
        var height = Math.Max(4, PlanLayoutEngine.GetNodeHeight(node) * scale);
        return (x, y, width, height);
    }

    /// <summary>
    /// The minimap elbow-connector thickness: the same logarithmic row-count rule the main
    /// canvas uses for its own connector thickness, scaled down, floored at 0.5px so a thin
    /// edge still shows.
    /// </summary>
    public static double GetEdgeThickness(double rows, double scale)
    {
        var fullThickness = Math.Max(2, Math.Min(Math.Floor(Math.Log(Math.Max(1, rows))), 12));
        return Math.Max(0.5, fullThickness * scale);
    }

    /// <summary>
    /// The minimap viewport box: the visible portion of the plan canvas, in minimap coordinates,
    /// from the main scroll viewer's offset, zoom level and viewport size. Never smaller than 4px
    /// on either axis. Returns a zero-size box at the origin if the viewport or plan has no usable
    /// size (nothing to show yet).
    /// </summary>
    public static (double X, double Y, double Width, double Height) GetViewportRect(
        double scale,
        double zoomLevel,
        double planWidth,
        double planHeight,
        double viewportWidth,
        double viewportHeight,
        double scrollOffsetX,
        double scrollOffsetY)
    {
        if (viewportWidth <= 0 || viewportHeight <= 0 || planWidth <= 0 || planHeight <= 0 || zoomLevel <= 0)
            return (0, 0, 0, 0);

        var contentWidth = planWidth * zoomLevel;
        var contentHeight = planHeight * zoomLevel;

        var width = Math.Max(4, Math.Min(viewportWidth / contentWidth, 1.0) * planWidth * scale);
        var height = Math.Max(4, Math.Min(viewportHeight / contentHeight, 1.0) * planHeight * scale);
        var x = (scrollOffsetX / zoomLevel) * scale;
        var y = (scrollOffsetY / zoomLevel) * scale;
        return (x, y, width, height);
    }

    /// <summary>
    /// The main scroll viewer's target offset for a click at a minimap point: convert the click
    /// back to plan content coordinates, then center the viewport on that point. Clamped to zero
    /// so a click near the top-left edge never asks for a negative offset.
    /// </summary>
    public static (double OffsetX, double OffsetY) ClickToScrollOffset(
        double minimapClickX,
        double minimapClickY,
        double scale,
        double zoomLevel,
        double viewportWidth,
        double viewportHeight)
    {
        if (scale <= 0) return (0, 0);

        var contentX = minimapClickX / scale;
        var contentY = minimapClickY / scale;

        var offsetX = Math.Max(0, contentX * zoomLevel - viewportWidth / 2);
        var offsetY = Math.Max(0, contentY * zoomLevel - viewportHeight / 2);
        return (offsetX, offsetY);
    }
}
