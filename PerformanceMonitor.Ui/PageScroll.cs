/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// A page that scrolls as one (#8226: the FinOps Utilization and Optimization pages; #6133: the PostgreSQL Vacuum and Overview
/// pages) and holds grids of its own. A grid's own scroll viewer marks the mouse wheel handled even when it has nothing left to
/// scroll, so over a grid the page would not move: <see cref="ForwardWheel"/>, on the page's <c>PreviewMouseWheel</c>, passes the
/// wheel to the page unless the grid under the pointer can still scroll that way itself. <see cref="GridMaxHeight"/> is the most
/// height a grid takes on such a page: a short grid still sizes to its rows, and a long one scrolls inside, keeping its row
/// virtualization instead of stretching the page by its whole length.
/// </summary>
public static class PageScroll
{
    /// <summary>The most height a grid takes inside a page that scrolls as one.</summary>
    public const double GridMaxHeight = 400;

    /// <summary>The gap between a chart and the grid under it on a page that stacks the two.</summary>
    public const double ChartOverGridGap = 8;

    /// <summary>
    /// #8307: the least height of a page that stacks one chart over one grid: the chart keeps <see cref="ChartLayout.MinPlotHeight"/>
    /// over the grid at the height its rows take (<paramref name="gridHeight"/>, at most <see cref="GridMaxHeight"/>) and the gap
    /// between them. Below this the page scrolls, instead of the grid shrinking to a few rows with a scroll bar of its own beside the
    /// page's; at or above it the page fits with no scroll bar. The page sets it again whenever the grid's size changes.
    /// </summary>
    public static double ChartOverGridMinHeight(double gridHeight) =>
        ChartLayout.MinPlotHeight + Math.Clamp(gridHeight, 0, GridMaxHeight) + ChartOverGridGap;

    /// <summary>
    /// For a page <see cref="ScrollViewer"/>'s <c>PreviewMouseWheel</c>: scrolls the page by the wheel and marks the event handled,
    /// unless the scroll viewer under the pointer (a grid's own) can still move in the wheel's direction, in which case the wheel is
    /// left to it.
    /// </summary>
    public static void ForwardWheel(object sender, MouseWheelEventArgs e)
    {
        if (sender is not ScrollViewer page || e.Handled)
        {
            return;
        }

        ScrollViewer? inner = null;
        for (DependencyObject? d = e.OriginalSource as DependencyObject;
             d != null && !ReferenceEquals(d, page);
             d = VisualTreeHelper.GetParent(d))
        {
            if (d is ScrollViewer sv)
            {
                inner = sv;
                break;
            }
        }

        if (inner != null && inner.ScrollableHeight > 0)
        {
            bool atTop = inner.VerticalOffset <= 0;
            bool atBottom = inner.VerticalOffset >= inner.ScrollableHeight;
            if ((e.Delta > 0 && !atTop) || (e.Delta < 0 && !atBottom))
            {
                return;
            }
        }

        page.ScrollToVerticalOffset(page.VerticalOffset - e.Delta);
        e.Handled = true;
    }
}
