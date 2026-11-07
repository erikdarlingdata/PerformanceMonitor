/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #8223: puts a number column's header on the same edge as its values. A column whose cells are right-aligned (its element style sets
/// <c>TextAlignment</c> or <c>HorizontalAlignment</c> to Right, as the themes' <c>NumericCell</c> does) got a left-aligned header, which sat
/// a column's width to the left of the figures it names: "Total Duration (ms)" about 100 px left of its values in a wide column.
/// Called once by a tab after its markup loads, for every grid under it. A column that sets its own header style, and a column whose
/// cells are not right-aligned, are left as they are. A template column, which has no element style to read, says it is a number column
/// with <see cref="IsNumberColumnProperty"/>.
/// </summary>
public static class DataGridNumberHeaders
{
    public static readonly DependencyProperty IsNumberColumnProperty =
        DependencyProperty.RegisterAttached("IsNumberColumn", typeof(bool), typeof(DataGridNumberHeaders), new PropertyMetadata(false));

    public static bool GetIsNumberColumn(DependencyObject obj) => (bool)obj.GetValue(IsNumberColumnProperty);
    public static void SetIsNumberColumn(DependencyObject obj, bool value) => obj.SetValue(IsNumberColumnProperty, value);

    /// <summary>Right-aligns the header of each number column of every grid under <paramref name="root"/>.</summary>
    public static void Apply(DependencyObject root)
    {
        if (root is DataGrid grid)
        {
            Align(grid);
        }

        foreach (var child in LogicalTreeHelper.GetChildren(root))
        {
            if (child is DependencyObject next)
            {
                Apply(next);
            }
        }
    }

    private static void Align(DataGrid grid)
    {
        Style? right = null;
        foreach (var column in grid.Columns)
        {
            /* HeaderStyle reads the grid's ColumnHeaderStyle when the column sets none (the Viewer's DarkGrid sets one), so only a value set on the column itself counts as its own style. */
            if (column.ReadLocalValue(DataGridColumn.HeaderStyleProperty) != DependencyProperty.UnsetValue || !IsNumber(column))
            {
                continue;
            }

            right ??= RightHeaderStyle(grid);
            column.HeaderStyle = right;
        }
    }

    private static Style RightHeaderStyle(DataGrid grid)
    {
        var style = new Style(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader), grid.TryFindResource(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader)) as Style);
        style.Setters.Add(new Setter(Control.HorizontalContentAlignmentProperty, HorizontalAlignment.Right));
        return style;
    }

    private static bool IsNumber(DataGridColumn column)
    {
        if (GetIsNumberColumn(column))
        {
            return true;
        }

        for (var style = (column as DataGridBoundColumn)?.ElementStyle; style is not null; style = style.BasedOn)
        {
            foreach (var setter in style.Setters.OfType<Setter>())
            {
                if ((setter.Property == TextBlock.TextAlignmentProperty && setter.Value is TextAlignment.Right)
                    || (setter.Property == FrameworkElement.HorizontalAlignmentProperty && setter.Value is HorizontalAlignment.Right))
                {
                    return true;
                }
            }
        }

        return false;
    }
}
