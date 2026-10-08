/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Gives every column of every grid under a root at least the width its own header needs, so a header is never cut mid-word
/// ("Current Size M", "CPU 9") whatever fixed width the column was given. The header's unit stays in the header; the column grows to show it.
/// </summary>
internal static class DataGridHeaderFit
{
    /// <summary>Room beyond the header text for the cell padding and the sort glyph.</summary>
    internal const double Padding = 30;

    /// <summary>The width a header of <paramref name="textWidth"/> of text needs.</summary>
    internal static double NeededWidth(double textWidth) => Math.Ceiling(textWidth + Padding);

    /// <summary>Raises each column's <see cref="DataGridColumn.MinWidth"/> to its header's width, for every grid under <paramref name="root"/>.</summary>
    internal static void Apply(DependencyObject root)
    {
        if (root is DataGrid grid)
        {
            foreach (var column in grid.Columns)
            {
                var text = MeasureHeader(column.Header, grid);
                if (text > 0)
                {
                    column.MinWidth = Math.Max(column.MinWidth, NeededWidth(text));
                }
            }
        }

        foreach (var child in LogicalTreeHelper.GetChildren(root))
        {
            if (child is DependencyObject next)
            {
                Apply(next);
            }
        }
    }

    private static double MeasureHeader(object? header, DataGrid grid)
    {
        switch (header)
        {
            case string text when text.Length > 0:
                var formatted = new FormattedText(
                    text, CultureInfo.CurrentUICulture, FlowDirection.LeftToRight,
                    new Typeface(grid.FontFamily, FontStyles.Normal, FontWeights.Bold, FontStretches.Normal),
                    grid.FontSize, Brushes.Black, VisualTreeHelper.GetDpi(grid).PixelsPerDip);
                return formatted.WidthIncludingTrailingWhitespace;
            case UIElement element:
                element.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
                return element.DesiredSize.Width;
            default:
                return 0;
        }
    }
}
