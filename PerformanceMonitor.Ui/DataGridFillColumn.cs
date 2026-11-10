/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #7383: keeps a grid's fixed columns at the width they were given when the grid also has a fill (<c>Width="*"</c>) column.
/// WPF's DataGrid, once its columns no longer fit, takes the missing width back from every column, fixed ones included, down to
/// their <see cref="DataGridColumn.MinWidth"/> (20 by default): a 26-column grid in a window narrower than its columns showed a 20 px PID
/// column and headers reading "Bacl" and "Us", and never a horizontal scrollbar. With this on, a fill column is held at an absolute width
/// that is whatever the viewport has left after the fixed columns, never below a floor, so the fixed columns keep their width and the grid
/// scrolls sideways when the columns do not fit. A wider window still grows the fill column.
/// Applied app-wide by the implicit DataGrid style in the theme dictionaries and by the keyed grid styles, as
/// <see cref="DataGridSortBehavior.SortDescendingFirst"/> is. A fixed column the user drags narrower or wider stays where it is dropped and
/// the fill column takes up the difference. The fill column itself cannot be dragged: it always takes what the others leave.
/// </summary>
public static class DataGridFillColumn
{
    /// <summary>The narrowest a fill column is made when the others leave less than this: a fill column is free text, and a sliver of it reads as nothing.</summary>
    public const double MinFillWidth = 150;

    public static readonly DependencyProperty EnabledProperty =
        DependencyProperty.RegisterAttached(
            "Enabled",
            typeof(bool),
            typeof(DataGridFillColumn),
            new PropertyMetadata(false, OnEnabledChanged));

    public static bool GetEnabled(DependencyObject obj) => (bool)obj.GetValue(EnabledProperty);
    public static void SetEnabled(DependencyObject obj, bool value) => obj.SetValue(EnabledProperty, value);

    /// <summary>The star weight each fill column was declared with: once a column is held at an absolute width, this is all that remembers it was a fill column.</summary>
    private static readonly ConditionalWeakTable<DataGridColumn, StarWeight> s_weights = new();

    private sealed class StarWeight
    {
        internal double Value;
    }

    /// <summary>Grids being fitted right now, so the width change a fit makes does not start another fit.</summary>
    private static readonly HashSet<DataGrid> s_fitting = new();

    /// <summary>The handler each grid listens for row changes with, kept so turning the fit off (or applying the style again) detaches the same one.</summary>
    private static readonly ConditionalWeakTable<DataGrid, NotifyCollectionChangedEventHandler> s_itemsChanged = new();

    /// <summary>
    /// Scroll viewers whose content scrolls by pixel because their grid is empty (see <see cref="KeepEmptyGridScrollable"/>), each with what its
    /// <see cref="ScrollViewer.CanContentScroll"/> held before: the binding to its grid that WPF gives a template's scroll viewer, a local value, or
    /// <see cref="DependencyProperty.UnsetValue"/> for neither.
    /// </summary>
    private static readonly ConditionalWeakTable<ScrollViewer, object> s_emptyScroll = new();

    private static void OnEnabledChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not DataGrid grid)
        {
            return;
        }

        /* Idempotent: detach first so re-applying the style does not double-subscribe. */
        grid.Loaded -= OnLoaded;
        grid.RemoveHandler(ScrollViewer.ScrollChangedEvent, (ScrollChangedEventHandler)OnScrollChanged);
        grid.RemoveHandler(Thumb.DragCompletedEvent, (DragCompletedEventHandler)OnDragCompleted);
        if (s_itemsChanged.TryGetValue(grid, out var attached))
        {
            ((INotifyCollectionChanged)grid.Items).CollectionChanged -= attached;
            s_itemsChanged.Remove(grid);
        }

        if ((bool)e.NewValue)
        {
            grid.Loaded += OnLoaded;
            grid.AddHandler(ScrollViewer.ScrollChangedEvent, (ScrollChangedEventHandler)OnScrollChanged);
            grid.AddHandler(Thumb.DragCompletedEvent, (DragCompletedEventHandler)OnDragCompleted);
            /* #8223: rows arriving at or leaving an empty grid change whether its scroll viewer needs the empty-grid width (see KeepEmptyGridScrollable). */
            NotifyCollectionChangedEventHandler onItemsChanged = (_, _) => Fit(grid);
            s_itemsChanged.Add(grid, onItemsChanged);
            ((INotifyCollectionChanged)grid.Items).CollectionChanged += onItemsChanged;
            if (grid.IsLoaded)
            {
                Fit(grid);
            }
        }
    }

    private static void OnLoaded(object sender, RoutedEventArgs e) => Fit((DataGrid)sender);

    /// <summary>The viewport (a resized window, a vertical scrollbar appearing) or the columns' extent (a column added later) changed.</summary>
    private static void OnScrollChanged(object sender, ScrollChangedEventArgs e)
    {
        if (e.ViewportWidthChange != 0 || e.ExtentWidthChange != 0)
        {
            Fit((DataGrid)sender);
        }
    }

    /// <summary>A column was resized by hand: the fill column takes up the difference.</summary>
    private static void OnDragCompleted(object sender, DragCompletedEventArgs e) => Fit((DataGrid)sender);

    /// <summary>Gives each fill column of <paramref name="grid"/> the viewport width the fixed columns leave, not below the floor. Public for a test that changes a column and fits again.</summary>
    public static void Fit(DataGrid grid)
    {
        if (s_fitting.Contains(grid))
        {
            return;
        }

        /* #8534: before the loaded check. Rows can arrive at a grid that is not loaded (its tab was left before the read landed), and the
           grid is laid out again before Loaded is raised, so the scroll viewer has to scroll by row by then. */
        ScrollByRowOnceRowsArrive(grid);
        if (!grid.IsLoaded)
        {
            return;
        }

        var scroll = grid.Template?.FindName("DG_ScrollViewer", grid) as ScrollViewer;
        if (scroll is null || scroll.ViewportWidth <= 0 || double.IsInfinity(scroll.ViewportWidth))
        {
            return;
        }

        KeepEmptyGridScrollable(grid, scroll);

        var fill = new List<(DataGridColumn Column, double Weight)>();
        var fixedWidth = 0.0;
        foreach (var column in grid.Columns)
        {
            if (column.Visibility != Visibility.Visible)
            {
                continue;
            }

            if (column.Width.IsStar)
            {
                s_weights.GetOrCreateValue(column).Value = column.Width.Value;
            }

            if (s_weights.TryGetValue(column, out var weight))
            {
                fill.Add((column, Math.Max(weight.Value, double.Epsilon)));
            }
            else
            {
                /* Absolute columns are what they were given (never less than their own floor); Auto and size-to-cells columns are what they measured. */
                fixedWidth += column.Width.IsAbsolute ? Math.Max(column.Width.Value, column.MinWidth) : column.ActualWidth;
            }
        }

        if (fill.Count == 0)
        {
            return;
        }

        var widths = Share(scroll.ViewportWidth - fixedWidth, fill);
        s_fitting.Add(grid);
        try
        {
            for (var i = 0; i < fill.Count; i++)
            {
                var column = fill[i].Column;
                if (!column.Width.IsAbsolute || Math.Abs(column.Width.Value - widths[i]) >= 0.5)
                {
                    column.Width = new DataGridLength(widths[i]);
                }
            }
        }
        finally
        {
            s_fitting.Remove(grid);
        }
    }

    /// <summary>
    /// #8223: an empty grid never scrolls sideways. WPF takes the scroll extent from the rows, so with none the extent is 0: columns that do not fit
    /// (the health grids, all fixed columns, empty most of the time) were cut at the edge, the last one mid-header, with no way to reach the rest. While the
    /// grid is empty and its columns are wider than the viewport, the scroll viewer scrolls by pixel and its content is held as wide as the columns, so
    /// the horizontal scroll bar shows and the headers scroll with it. Rows arriving put it back to scrolling by row.
    /// <para>#8534: it goes back by restoring the scroll viewer's binding to its grid, which takes effect at once. Clearing the value would leave the
    /// property at its default, scrolling by pixel, until the scroll viewer is next measured, and the rows that just arrived are measured before
    /// it: a rows presenter with no scroll owner is given unbounded height and builds a container for every row it was handed, thousands where a
    /// dozen are in view. With the binding back, the presenter has its scroll owner before the layout pass and builds only the rows in view.</para>
    /// </summary>
    private static void KeepEmptyGridScrollable(DataGrid grid, ScrollViewer scroll)
    {
        if (scroll.Content is not FrameworkElement presenter)
        {
            return;
        }

        /* The width each column is about to have, as the fit below counts it: a column given an absolute width is laid out that wide even before the pass that says so. */
        var total = grid.Columns.Where(c => c.Visibility == Visibility.Visible)
            .Sum(c => c.Width.IsAbsolute ? Math.Max(c.Width.Value, c.MinWidth) : c.ActualWidth);
        if (grid.Items.Count == 0 && total > scroll.ViewportWidth + 0.5)
        {
            if (!s_emptyScroll.TryGetValue(scroll, out _))
            {
                s_emptyScroll.Add(scroll, (object?)BindingOperations.GetBindingBase(scroll, ScrollViewer.CanContentScrollProperty)
                    ?? scroll.ReadLocalValue(ScrollViewer.CanContentScrollProperty));
                scroll.CanContentScroll = false;
            }

            if (Math.Abs(presenter.MinWidth - total) >= 0.5)
            {
                presenter.MinWidth = total;
            }
        }
        else
        {
            ScrollAsBefore(scroll);
        }
    }

    /// <summary>
    /// A grid that scrolled by pixel while empty has rows: its scroll viewer goes back to what it held. This needs only the grid's template,
    /// not a loaded grid, so it also runs for rows that arrive while the grid's tab is not selected.
    /// </summary>
    private static void ScrollByRowOnceRowsArrive(DataGrid grid)
    {
        if (grid.Items.Count > 0 && grid.Template?.FindName("DG_ScrollViewer", grid) is ScrollViewer scroll)
        {
            ScrollAsBefore(scroll);
        }
    }

    /// <summary>Puts back what <paramref name="scroll"/> held before it was made to scroll by pixel, and lets its content be as narrow as it likes again. Nothing to do for a scroll viewer that was not.</summary>
    private static void ScrollAsBefore(ScrollViewer scroll)
    {
        if (!s_emptyScroll.TryGetValue(scroll, out var before))
        {
            return;
        }

        s_emptyScroll.Remove(scroll);
        if (before is BindingBase binding)
        {
            BindingOperations.SetBinding(scroll, ScrollViewer.CanContentScrollProperty, binding);
        }
        else if (before == DependencyProperty.UnsetValue)
        {
            scroll.ClearValue(ScrollViewer.CanContentScrollProperty);
        }
        else
        {
            scroll.SetValue(ScrollViewer.CanContentScrollProperty, before);
        }

        (scroll.Content as FrameworkElement)?.ClearValue(FrameworkElement.MinWidthProperty);
    }

    /// <summary>
    /// Splits <paramref name="room"/> among the fill columns by weight; a column whose share is under its floor takes the floor,
    /// and the others share what is left.
    /// </summary>
    private static double[] Share(double room, List<(DataGridColumn Column, double Weight)> fill)
    {
        var floors = fill.Select(f => Math.Max(MinFillWidth, f.Column.MinWidth)).ToArray();
        var widths = new double[fill.Count];
        var atFloor = new bool[fill.Count];
        while (true)
        {
            var open = Enumerable.Range(0, fill.Count).Where(i => !atFloor[i]).ToList();
            var remaining = room - Enumerable.Range(0, fill.Count).Where(i => atFloor[i]).Sum(i => floors[i]);
            var weight = open.Sum(i => fill[i].Weight);
            var changed = false;
            foreach (var i in open)
            {
                widths[i] = remaining * fill[i].Weight / weight;
                if (widths[i] < floors[i])
                {
                    atFloor[i] = true;
                    changed = true;
                }
            }

            if (!changed || open.Count == 0)
            {
                break;
            }
        }

        for (var i = 0; i < widths.Length; i++)
        {
            widths[i] = atFloor[i] ? floors[i] : Math.Floor(widths[i]);
        }

        return widths;
    }
}
