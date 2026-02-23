using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public static class PlanLayoutEngine
{
    public const double NodeWidth = 150;
    public const double NodeHeight = 90;
    public const double HorizontalSpacing = 180;
    public const double VerticalSpacing = 24;
    public const double Padding = 40;

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
        double maxX = 0, maxY = 0;
        CollectExtents(root, ref maxX, ref maxY);
        return (maxX + NodeWidth + Padding, maxY + NodeHeight + Padding);
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
            nextY += NodeHeight + VerticalSpacing;
            return;
        }

        // Process children first (post-order)
        foreach (var child in node.Children)
            SetYPositions(child, ref nextY);

        // SSMS-style: parent aligns with first child (creates horizontal spine)
        node.Y = node.Children[0].Y;
    }

    private static void CollectExtents(PlanNode node, ref double maxX, ref double maxY)
    {
        if (node.X > maxX) maxX = node.X;
        if (node.Y > maxY) maxY = node.Y;

        foreach (var child in node.Children)
            CollectExtents(child, ref maxX, ref maxY);
    }
}
