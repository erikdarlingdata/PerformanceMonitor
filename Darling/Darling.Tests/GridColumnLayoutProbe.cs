/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;

namespace Darling.Tests;

/// <summary>
/// #8099: measures a server-tab grid as it renders. The grid is taken out of its tab and shown on its own in a
/// window as wide as a maximized window on a 3840 px display at 250 % scaling (<see cref="Viewport"/>), its rows are
/// bound, and the realized cells are read: how tall a Query Text cell is, how it trims, what its tooltip holds, and
/// whether a column's cells end inside the grid's viewport. Shared by Lite.Tests (by <c>Compile Include</c>, like
/// <see cref="ClippedTextProbe"/>) and Darling.Tests, because the two server tabs carry the same grids.
/// </summary>
internal static class GridColumnLayoutProbe
{
    /// <summary>A maximized window on a 3840 px display at 250 % scaling.</summary>
    internal const int Viewport = 1536;

    /// <summary>
    /// A statement long enough to wrap in a 500 px cell and to run past 320 px on one line, with the line breaks and
    /// indentation a statement carries in the plan cache.
    /// </summary>
    internal const string LongQueryText =
        "SELECT\n    o.order_id,\n    SUM(l.quantity * l.unit_price) AS order_total\nFROM dbo.orders AS o\n"
        + "JOIN dbo.order_lines AS l\n  ON l.order_id = o.order_id\nWHERE o.placed_at >= DATEADD(DAY, -7, SYSDATETIME())\n"
        + "GROUP BY o.order_id\nORDER BY order_total DESC\nOPTION (RECOMPILE);";

    /// <summary>
    /// The grid named <paramref name="name"/> on <paramref name="tab"/>, taken out of the tab so it can be shown on its
    /// own. Selecting its tab instead would start that tab's load against a server that is not there. The grid keeps
    /// its columns, cell templates, styles and handlers; only its parent changes.
    /// </summary>
    internal static DataGrid Detach(FrameworkElement tab, string name)
    {
        var grid = tab.FindName(name) as DataGrid ?? throw new InvalidOperationException($"{name} is not a DataGrid on the tab");
        switch (grid.Parent)
        {
            case ContentControl content:
                content.Content = null;
                break;
            case Decorator decorator:
                decorator.Child = null;
                break;
            case Panel panel:
                panel.Children.Remove(grid);
                break;
            default:
                throw new InvalidOperationException($"{name} sits in a {grid.Parent?.GetType().Name ?? "null"}, which this probe cannot detach from");
        }

        return grid;
    }

    /// <summary>
    /// Shows <paramref name="grid"/> alone in a <see cref="Viewport"/> px wide window, binds <paramref name="rows"/>,
    /// lets the layout settle, and hands the realized grid to <paramref name="measure"/>. Returns what it found.
    /// </summary>
    internal static List<string> Shown(DataGrid grid, IList rows, Func<DataGrid, List<string>> measure)
    {
        var window = new Window { Content = grid, Width = Viewport, Height = 800, Left = 0, Top = 0 };
        window.Show();
        try
        {
            ClippedTextProbe.Settle(window);
            grid.ItemsSource = rows;
            ClippedTextProbe.Settle(window);
            return measure(grid);
        }
        finally
        {
            window.Close();
        }
    }

    /// <summary>
    /// The Query Text cells of <paramref name="rows"/> are one line each, trimmed with an ellipsis, with the full text
    /// (line breaks and all) in the tooltip, and the cells of each column in <paramref name="columnsInside"/> end inside
    /// the grid's viewport. Each finding is one line naming the row and the column.
    /// </summary>
    internal static List<string> MeasureQueryText(DataGrid grid, IList rows, string queryText, params string[] columnsInside)
    {
        var found = new List<string>();
        var queryTextColumn = Column(grid, "Query Text");
        foreach (var row in rows)
        {
            /* A template column's cell content is the presenter that hosts the template; the TextBlock is inside it. */
            var content = queryTextColumn.GetCellContent(row);
            var cell = content as TextBlock ?? (content is null ? null : ClippedTextProbe.Descendants(content).OfType<TextBlock>().FirstOrDefault());
            if (cell is null)
            {
                found.Add($"row {rows.IndexOf(row)}: Query Text has no realized TextBlock");
                continue;
            }

            /* One line of the grid's font is about 1.3 times its size; a wrapped cell is several times taller. */
            if (cell.ActualHeight > cell.FontSize * 2)
            {
                found.Add($"row {rows.IndexOf(row)}: Query Text is {cell.ActualHeight:F0} px tall at a {cell.FontSize:F0} px font, more than one line");
            }

            if (cell.TextTrimming != TextTrimming.CharacterEllipsis)
            {
                found.Add($"row {rows.IndexOf(row)}: Query Text trims with {cell.TextTrimming}, not an ellipsis");
            }

            if (cell.Text.Contains('\n'))
            {
                found.Add($"row {rows.IndexOf(row)}: Query Text still carries the statement's line breaks");
            }

            if (!Equals(cell.ToolTip, queryText))
            {
                found.Add($"row {rows.IndexOf(row)}: the Query Text tooltip is not the full statement");
            }
        }

        found.AddRange(Outside(grid, rows, columnsInside));
        return found;
    }

    /// <summary>
    /// The cells of each column in <paramref name="columnsInside"/> end inside the grid's viewport for every row;
    /// <paramref name="blankColumn"/> (a column whose cells are all blank for these rows) is no wider than its header
    /// needs, and <paramref name="oneWordColumn"/> is at most <paramref name="oneWordWidth"/> px.
    /// </summary>
    internal static List<string> MeasureSettings(DataGrid grid, IList rows, string blankColumn, string oneWordColumn, double oneWordWidth, params string[] columnsInside)
    {
        var found = Outside(grid, rows, columnsInside);

        var blank = Column(grid, blankColumn);
        var header = ClippedTextProbe.Descendants(grid).OfType<DataGridColumnHeader>().FirstOrDefault(h => ReferenceEquals(h.Column, blank));
        if (header is null)
        {
            found.Add($"{blankColumn} has no realized header");
        }
        /* #8197: the header floor (DataGridHeaderFit) also keeps room for the sort glyph, which an unsorted header has not drawn yet (8 px + its 6 px margin). */
        else if (blank.ActualWidth > header.DesiredSize.Width + 14.5)
        {
            found.Add($"{blankColumn} is {blank.ActualWidth:F0} px wide for blank cells under a {header.DesiredSize.Width:F0} px header");
        }

        var oneWord = Column(grid, oneWordColumn);
        if (oneWord.ActualWidth > oneWordWidth + 0.5)
        {
            found.Add($"{oneWordColumn} is {oneWord.ActualWidth:F0} px wide for a one-word value, more than {oneWordWidth:F0}");
        }

        return found;
    }

    private static List<string> Outside(DataGrid grid, IList rows, string[] headers)
    {
        var found = new List<string>();
        var viewport = ClippedTextProbe.Descendants(grid).OfType<ScrollContentPresenter>().First();
        var viewportRight = viewport.TransformToAncestor(grid).TransformBounds(new Rect(viewport.RenderSize)).Right;
        foreach (var headerText in headers)
        {
            var column = Column(grid, headerText);
            foreach (var row in rows)
            {
                if (column.GetCellContent(row) is not FrameworkElement content)
                {
                    found.Add($"row {rows.IndexOf(row)}: {headerText} has no realized cell");
                    continue;
                }

                var cell = content.Parent as FrameworkElement ?? content;
                var bounds = cell.TransformToAncestor(grid).TransformBounds(new Rect(cell.RenderSize));
                if (bounds.Right > viewportRight + 0.5)
                {
                    found.Add($"row {rows.IndexOf(row)}: {headerText} spans {bounds.Left:F0}..{bounds.Right:F0}, past the viewport's right edge at {viewportRight:F0}");
                }
            }
        }

        return found;
    }

    /// <summary>The column whose header reads <paramref name="text"/>: a plain string header, or the first TextBlock in a header element.</summary>
    internal static DataGridColumn Column(DataGrid grid, string text)
    {
        foreach (var column in grid.Columns)
        {
            var header = column.Header switch
            {
                string s => s,
                DependencyObject element => LogicalTreeHelper.GetChildren(element).OfType<DependencyObject>().Prepend(element)
                    .SelectMany(e => e is TextBlock ? new[] { e } : LogicalTreeHelper.GetChildren(e).OfType<DependencyObject>())
                    .OfType<TextBlock>().FirstOrDefault()?.Text,
                _ => null,
            };
            if (header == text)
            {
                return column;
            }
        }

        throw new InvalidOperationException($"no column headed \"{text}\" on {grid.Name}: " + string.Join(", ", grid.Columns.Select(c => c.Header is string s ? s : c.Header?.GetType().Name ?? "null")));
    }
}
