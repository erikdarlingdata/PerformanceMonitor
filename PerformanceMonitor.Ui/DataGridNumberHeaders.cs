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
/// with <see cref="IsNumberColumnProperty"/>. The header's style is based on the one the grid's text headers use (its <c>ColumnHeaderStyle</c>, or the
/// theme's header style) and changes only the alignment, and it is built again after a live theme switch.
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
            if (grid.GetValue(ThemeFollowerProperty) is null)
            {
                grid.SetValue(ThemeFollowerProperty, new ThemeFollower(grid));
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

    /// <summary>On a column: the header style this class gave it, so a later pass can tell it from a style the column set itself.</summary>
    private static readonly DependencyProperty OwnHeaderStyleProperty =
        DependencyProperty.RegisterAttached("OwnHeaderStyle", typeof(Style), typeof(DataGridNumberHeaders), new PropertyMetadata(null));

    /// <summary>On a grid: the object that keeps its number headers on the current theme, so a second <see cref="Apply"/> does not add another.</summary>
    private static readonly DependencyProperty ThemeFollowerProperty =
        DependencyProperty.RegisterAttached("ThemeFollower", typeof(ThemeFollower), typeof(DataGridNumberHeaders), new PropertyMetadata(null));

    private static void Align(DataGrid grid)
    {
        Style? right = null;
        foreach (var column in grid.Columns)
        {
            /* HeaderStyle reads the grid's ColumnHeaderStyle when the column sets none (the Viewer's DarkGrid sets one), so only a value set on the column itself counts as its own style. */
            var own = column.GetValue(OwnHeaderStyleProperty) as Style;
            var local = column.ReadLocalValue(DataGridColumn.HeaderStyleProperty);
            if ((local != DependencyProperty.UnsetValue && !ReferenceEquals(local, own)) || !IsNumber(column))
            {
                continue;
            }

            /* The header style the grid's text headers use: a style built on it before a theme switch still points at the old theme's brushes, so it is built again when the base is another object. */
            var basedOn = TextHeaderStyle(grid) ?? grid.ColumnHeaderStyle ?? grid.TryFindResource(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader)) as Style;
            if (own is not null && ReferenceEquals(own.BasedOn, basedOn))
            {
                continue;
            }

            right ??= RightHeaderStyle(basedOn);
            column.SetValue(OwnHeaderStyleProperty, right);
            column.HeaderStyle = right;
        }
    }

    /// <summary>
    /// The style a realized text header of the grid is using now, so a number header matches its neighbours whichever of the grid's
    /// <c>ColumnHeaderStyle</c> and the theme's header style they resolved to (WPF picks one or the other as the headers are first built and after a
    /// theme switch); null while no text header has been built.
    /// </summary>
    private static Style? TextHeaderStyle(DependencyObject parent)
    {
        for (var i = 0; i < System.Windows.Media.VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = System.Windows.Media.VisualTreeHelper.GetChild(parent, i);
            if (child is System.Windows.Controls.Primitives.DataGridColumnHeader { Column: { } column } header)
            {
                if (column.ReadLocalValue(DataGridColumn.HeaderStyleProperty) == DependencyProperty.UnsetValue && !IsNumber(column))
                {
                    return header.Style;
                }

                continue;
            }

            if (TextHeaderStyle(child) is { } found)
            {
                return found;
            }
        }

        return null;
    }

    private static Style RightHeaderStyle(Style? basedOn)
    {
        var style = new Style(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader), basedOn);
        style.Setters.Add(new Setter(Control.HorizontalContentAlignmentProperty, HorizontalAlignment.Right));
        return style;
    }

    /// <summary>
    /// Builds a grid's number headers again when a theme switch swaps the header style they are based on. Subscribes to
    /// <see cref="ThemeManager.ThemeChanged"/> only while the grid is loaded (a grid on a hidden tab is unloaded, and is aligned again when it is shown),
    /// so a closed tab is not kept alive by the static event.
    /// </summary>
    private sealed class ThemeFollower
    {
        private readonly DataGrid _grid;
        private bool _subscribed;

        internal ThemeFollower(DataGrid grid)
        {
            _grid = grid;
            grid.Loaded += (_, _) => { Align(_grid); Subscribe(); };
            grid.Unloaded += (_, _) => Unsubscribe();
            if (grid.IsLoaded)
            {
                Subscribe();
            }
        }

        private void Subscribe()
        {
            if (!_subscribed)
            {
                _subscribed = true;
                ThemeManager.ThemeChanged += OnThemeChanged;
            }
        }

        private void Unsubscribe()
        {
            _subscribed = false;
            ThemeManager.ThemeChanged -= OnThemeChanged;
        }

        private void OnThemeChanged(string _) => Align(_grid);
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
