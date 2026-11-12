/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Specialized;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Ends a narrow text cell's text with an ellipsis instead of cutting it at the cell's edge. A text column with no element style of its
/// own draws its text with <see cref="DataGridTextColumn.DefaultElementStyle"/>, a style the themes cannot reach: it is set on the text
/// block explicitly, so no implicit theme style applies to it. Set by the DataGrid style in each theme, this gives every such column a
/// style based on that default, with the trimming the themes' <c>DataGridCellText</c> carries. A column with its own element style keeps it.
/// </summary>
public static class DataGridCellTrimming
{
    public static readonly DependencyProperty EnabledProperty =
        DependencyProperty.RegisterAttached(
            "Enabled",
            typeof(bool),
            typeof(DataGridCellTrimming),
            new PropertyMetadata(false, OnEnabledChanged));

    public static bool GetEnabled(DependencyObject obj) => (bool)obj.GetValue(EnabledProperty);
    public static void SetEnabled(DependencyObject obj, bool value) => obj.SetValue(EnabledProperty, value);

    private static Style? _trimmedDefault;

    /// <summary>The default text column style plus trimming; sealed once, shared by every column it is set on.</summary>
    internal static Style TrimmedDefaultElementStyle
    {
        get
        {
            if (_trimmedDefault is null)
            {
                var style = new Style(typeof(TextBlock), DataGridTextColumn.DefaultElementStyle);
                style.Setters.Add(new Setter(TextBlock.TextTrimmingProperty, TextTrimming.CharacterEllipsis));
                style.Seal();
                _trimmedDefault = style;
            }

            return _trimmedDefault;
        }
    }

    private static void OnEnabledChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not DataGrid grid)
        {
            return;
        }

        /* Idempotent: detach first so re-applying the style does not double-subscribe. */
        ((INotifyCollectionChanged)grid.Columns).CollectionChanged -= OnColumnsChanged;
        if ((bool)e.NewValue)
        {
            ((INotifyCollectionChanged)grid.Columns).CollectionChanged += OnColumnsChanged;
            Apply(grid);
        }
    }

    /// <summary>A column added after the grid was styled (a grid whose columns are built in code) gets the same treatment.</summary>
    private static void OnColumnsChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        if (e.NewItems is null)
        {
            return;
        }

        foreach (var item in e.NewItems)
        {
            if (item is DataGridTextColumn column)
            {
                Apply(column);
            }
        }
    }

    /// <summary>Gives every text column of <paramref name="grid"/> with no element style of its own the trimmed default.</summary>
    internal static void Apply(DataGrid grid)
    {
        foreach (var column in grid.Columns)
        {
            if (column is DataGridTextColumn text)
            {
                Apply(text);
            }
        }
    }

    private static void Apply(DataGridTextColumn column)
    {
        /* A local element style (NumericCell, FreeTextCell, DataGridCellText and the rest) is the column's own choice and stays. */
        if (column.ReadLocalValue(DataGridBoundColumn.ElementStyleProperty) == DependencyProperty.UnsetValue)
        {
            column.ElementStyle = TrimmedDefaultElementStyle;
        }
    }
}
