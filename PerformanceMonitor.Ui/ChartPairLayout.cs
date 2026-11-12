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

namespace PerformanceMonitor.Ui;

/// <summary>
/// #8219: two charts that stack when the tab is tall enough for both and sit side by side when it is not (Current Waits at
/// Lite's 900x600 minimum, where the tab has about 377 high and two stacked charts need about 460).
/// </summary>
/// <remarks>
/// The tab's Grid has two columns, a banner row (0), a first-chart row (1), a second-chart row (2) and a note row (3), each
/// card spanning both columns when stacked. The Grid's XAML <see cref="FrameworkElement.MinHeight"/> is the height one chart row
/// needs with the banner and note; the stacked layout needs the second row's own minimum
/// (<see cref="ChartLayout.MinPlotHeight"/>) on top of it. The mode is chosen from the scroll viewer's own height on every
/// resize. Neither mode changes that height (only the content is rearranged, and the vertical scroll bar takes width, not
/// height), so the choice cannot feed back into itself.
/// </remarks>
public static class ChartPairLayout
{
    /// <summary>
    /// Wires the switch. <paramref name="secondRow"/> is the Grid row the second card uses when stacked, and is collapsed while
    /// the cards sit side by side.
    /// </summary>
    public static void Attach(ScrollViewer viewport, Grid grid, FrameworkElement first, FrameworkElement second, RowDefinition secondRow)
    {
        var stackedMin = secondRow.MinHeight;
        var oneRowMin = grid.MinHeight;
        var stackedNeed = oneRowMin + stackedMin;
        var firstMargin = first.Margin;
        var secondMargin = second.Margin;
        var secondHeight = secondRow.Height;
        var columns = Math.Max(1, grid.ColumnDefinitions.Count);
        bool? sideBySide = null;

        void Apply()
        {
            // A tab that is not shown reports no size; keep the last mode rather than flip on it.
            if (viewport.ActualHeight <= 0) return;
            var wanted = viewport.ActualHeight < stackedNeed;
            if (sideBySide == wanted) return;
            sideBySide = wanted;
            if (wanted)
            {
                // The second card moves up beside the first; its old row collapses so it adds nothing to the tab's height.
                secondRow.MinHeight = 0;
                secondRow.Height = GridLength.Auto;
                Grid.SetRow(second, Grid.GetRow(first));
                Grid.SetColumn(first, 0);
                Grid.SetColumnSpan(first, 1);
                Grid.SetColumn(second, 1);
                Grid.SetColumnSpan(second, 1);
                first.Margin = new Thickness(firstMargin.Left, 0, 4, 0);
                second.Margin = new Thickness(4, 0, secondMargin.Right, 0);
                grid.MinHeight = oneRowMin;
            }
            else
            {
                // Restore the row first, then move the card into it, so a layout between the two never sees the card in a collapsed row.
                secondRow.MinHeight = stackedMin;
                secondRow.Height = secondHeight;
                Grid.SetRow(second, Grid.GetRow(first) + 1);
                Grid.SetColumn(first, 0);
                Grid.SetColumnSpan(first, columns);
                Grid.SetColumn(second, 0);
                Grid.SetColumnSpan(second, columns);
                first.Margin = firstMargin;
                second.Margin = secondMargin;
            }
        }

        viewport.SizeChanged += (_, _) => Apply();
        viewport.Loaded += (_, _) => Apply();
    }
}
