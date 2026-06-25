/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// Left-to-right tree/forest layout for blocking chains — apex at the left, victims flowing right.
    /// The algorithm mirrors the query-plan layout's SSMS-style "parent aligns to its first child"
    /// approach, but is reimplemented against <see cref="BlockingChainNode"/> (Common cannot reference
    /// the plan-analysis layout engine without a dependency cycle). Pure; node heights come from a caller
    /// measure callback so this stays WPF-free and unit-testable.
    /// </summary>
    public static class BlockingChainLayout
    {
        public const double NodeWidth = 260;          // wide enough for login / host / app identity lines
        public const double HorizontalSpacing = 330;  // NodeWidth + horizontal gap between depths
        public const double VerticalSpacing = 22;
        public const double Padding = 40;
        public const double ForestGap = 36; // extra vertical gap between separate apex trees

        /// <summary>
        /// Assigns X/Y to every node and returns the total canvas extent. Roots stack vertically into a
        /// forest. Deterministic for a deterministic tree + measure.
        /// </summary>
        public static (double Width, double Height) Layout(
            IReadOnlyList<BlockingChainNode> roots,
            Func<BlockingChainNode, double> measureHeight)
        {
            if (roots == null) throw new ArgumentNullException(nameof(roots));
            if (measureHeight == null) throw new ArgumentNullException(nameof(measureHeight));

            double nextY = Padding;
            foreach (var root in roots)
            {
                SetXPositions(root, 0);
                SetYPositions(root, measureHeight, ref nextY);
                nextY += ForestGap;
            }

            double maxX = 0, maxBottom = 0;
            foreach (var root in roots)
                CollectExtents(root, measureHeight, ref maxX, ref maxBottom);

            return (maxX + NodeWidth + Padding, maxBottom + Padding);
        }

        private static void SetXPositions(BlockingChainNode node, int depth)
        {
            node.X = Padding + depth * HorizontalSpacing;
            foreach (var child in node.Children)
                SetXPositions(child, depth + 1);
        }

        private static void SetYPositions(BlockingChainNode node, Func<BlockingChainNode, double> measureHeight, ref double nextY)
        {
            if (node.Children.Count == 0)
            {
                node.Y = nextY;
                nextY += measureHeight(node) + VerticalSpacing;
                return;
            }

            foreach (var child in node.Children)
                SetYPositions(child, measureHeight, ref nextY);

            // Align the parent with its first child (SSMS-style horizontal spine).
            node.Y = node.Children[0].Y;
        }

        private static void CollectExtents(BlockingChainNode node, Func<BlockingChainNode, double> measureHeight, ref double maxX, ref double maxBottom)
        {
            if (node.X > maxX) maxX = node.X;
            var bottom = node.Y + measureHeight(node);
            if (bottom > maxBottom) maxBottom = bottom;

            foreach (var child in node.Children)
                CollectExtents(child, measureHeight, ref maxX, ref maxBottom);
        }
    }
}
