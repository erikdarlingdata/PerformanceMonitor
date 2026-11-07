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

namespace PerformanceMonitor.Ui;

/// <summary>
/// Gives every column of every grid under a root at least the width its own header needs, so a header is never cut mid-word
/// ("Current Size M", "CPU 9") whatever fixed width the column was given. The header's unit stays in the header; the column grows to show it.
/// </summary>
public static class DataGridHeaderFit
{
    /// <summary>
    /// #8197: set by the DataGrid style in each theme, so every grid of both apps gets the fit when it loads, with no per-tab call. Before it only the
    /// Job History, Alert History and FinOps tabs called <see cref="Apply"/>, and the Queries grids (a 100 px "Avg CPU (ms)" column, a 120 px "Avg Duration (ms)" one)
    /// cut their headers mid-word, rows or no rows.
    /// </summary>
    public static readonly DependencyProperty EnabledProperty =
        DependencyProperty.RegisterAttached(
            "Enabled",
            typeof(bool),
            typeof(DataGridHeaderFit),
            new PropertyMetadata(false, OnEnabledChanged));

    public static bool GetEnabled(DependencyObject obj) => (bool)obj.GetValue(EnabledProperty);
    public static void SetEnabled(DependencyObject obj, bool value) => obj.SetValue(EnabledProperty, value);

    private static void OnEnabledChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not DataGrid grid)
        {
            return;
        }

        /* Idempotent: detach first so re-applying the style does not double-subscribe. */
        grid.Loaded -= OnLoaded;
        if ((bool)e.NewValue)
        {
            grid.Loaded += OnLoaded;
            if (grid.IsLoaded)
            {
                FitColumns(grid);
            }
        }
    }

    private static void OnLoaded(object sender, RoutedEventArgs e) => FitColumns((DataGrid)sender);

    /// <summary>Room beyond the header text for the cell padding and the sort glyph.</summary>
    internal const double Padding = 30;

    /// <summary>The width a header of <paramref name="textWidth"/> of text needs.</summary>
    internal static double NeededWidth(double textWidth) => Math.Ceiling(textWidth + Padding);

    /// <summary>Raises each column's <see cref="DataGridColumn.MinWidth"/> to its header's width, for every grid under <paramref name="root"/>.</summary>
    internal static void Apply(DependencyObject root)
    {
        if (root is DataGrid grid)
        {
            FitColumns(grid);
        }

        foreach (var child in LogicalTreeHelper.GetChildren(root))
        {
            if (child is DependencyObject next)
            {
                Apply(next);
            }
        }
    }

    /// <summary>Raises each column of <paramref name="grid"/> to the width its own header needs; a column is never made narrower.</summary>
    private static void FitColumns(DataGrid grid)
    {
        foreach (var column in grid.Columns)
        {
            var text = MeasureHeader(column.Header, grid);
            if (text > 0)
            {
                column.MinWidth = Math.Max(column.MinWidth, NeededWidth(text));
                /* A column already laid out keeps its fixed width when only MinWidth rises (a header whose title grows after the tab opens). */
                if (column.Width.IsAbsolute && column.Width.Value < column.MinWidth)
                {
                    column.Width = new DataGridLength(column.MinWidth);
                }
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
                /* Invalidated first: a title changed since the last layout pass would otherwise be skipped as already measured. */
                element.InvalidateMeasure();
                element.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
                return element.DesiredSize.Width;
            default:
                return 0;
        }
    }
}
