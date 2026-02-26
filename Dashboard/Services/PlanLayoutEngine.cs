using System;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services;

public static class PlanLayoutEngine
{
    public const double NodeWidth = 150;
    public const double HorizontalSpacing = 180;
    public const double VerticalSpacing = 24;
    public const double Padding = 40;

    /* Per-line heights matching CreateNodeVisual font sizes + WPF line spacing */
    private const double IconRowHeight = 36;  /* 32px icon + 2px margin + 2px spacing */
    private const double Line10 = 17;         /* FontSize 10 text block */
    private const double Line9 = 15;          /* FontSize 9 text block */
    private const double NodePadding = 12;    /* Border padding (4+4) + border thickness */

    /// <summary>
    /// Minimum node height (estimated plans without actual stats or object name).
    /// </summary>
    public const double NodeHeightMin = 90;

    public static void Layout(PlanStatement statement)
    {
        if (statement.RootNode == null) return;

        // Phase 1: X positions by tree depth (root at left, children to the right)
        SetXPositions(statement.RootNode, 0);

        // Phase 2: Y positions with overlap prevention
        double nextY = Padding;
        SetYPositions(statement.RootNode, ref nextY);
    }

    public static (double width, double height) GetExtents(PlanNode root)
    {
        double maxX = 0, maxBottom = 0;
        CollectExtents(root, ref maxX, ref maxBottom);
        return (maxX + NodeWidth + Padding, maxBottom + Padding);
    }

    /// <summary>
    /// Calculates the expected rendered height of a node based on its content.
    /// Matches the lines emitted by CreateNodeVisual in PlanViewerControl.
    /// </summary>
    public static double GetNodeHeight(PlanNode node)
    {
        double h = IconRowHeight + Line10 + Line10 + NodePadding; /* icon + name + cost + padding */

        if (node.HasActualStats)
        {
            h += Line10;  /* elapsed time */
            h += Line9;   /* CPU time */
            h += Line9;   /* actual/estimated rows */
        }

        if (!string.IsNullOrEmpty(node.ObjectName))
        {
            h += Line9;   /* object name */
        }

        return Math.Max(h, NodeHeightMin);
    }

    private static void SetXPositions(PlanNode node, int depth)
    {
        node.X = Padding + depth * HorizontalSpacing;

        foreach (var child in node.Children)
            SetXPositions(child, depth + 1);
    }

    private static void SetYPositions(PlanNode node, ref double nextY)
    {
        if (node.Children.Count == 0)
        {
            // Leaf node: place at the next available Y position
            node.Y = nextY;
            nextY += GetNodeHeight(node) + VerticalSpacing;
            return;
        }

        // Process children first (post-order)
        foreach (var child in node.Children)
            SetYPositions(child, ref nextY);

        // SSMS-style: parent aligns with first child (creates horizontal spine)
        node.Y = node.Children[0].Y;
    }

    private static void CollectExtents(PlanNode node, ref double maxX, ref double maxBottom)
    {
        if (node.X > maxX) maxX = node.X;

        var bottom = node.Y + GetNodeHeight(node);
        if (bottom > maxBottom) maxBottom = bottom;

        foreach (var child in node.Children)
            CollectExtents(child, ref maxX, ref maxBottom);
    }
}
