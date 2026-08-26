/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Windows.Controls;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// What a marked row means. The words are the operator's, not ours (#2645): "it is done", "to do",
/// "do not do".
/// </summary>
public enum DataGridRowMark
{
    None = 0,
    Done,
    ToDo,
    DoNot,
}

/// <summary>
/// Session-scoped row marking for every grid at once (#2645).
///
/// <para>
/// Requested for the Index Analysis grid — run the analysis, then mark rows so you can remember which
/// indexes you have dealt with and which you have decided against. It generalises for free: every grid in
/// Lite is wrapped by <see cref="DataGridFilterManager{T}"/>, so the marks live in one place and every
/// grid gets them.
/// </para>
///
/// <para>
/// <b>Marks are held against the row OBJECT, and that is a deliberate limit rather than an oversight.</b>
/// <c>UpdateData</c> replaces the row objects on every refresh, and the live tabs refresh on a
/// 60-second timer — so on those grids a mark lasts until the next refresh, and the feature is honest
/// about lasting exactly as long as the thing it is attached to. On the run-on-demand grids, which is
/// where this was asked for, the rows persist until the analysis is run again and so do the marks.
/// </para>
///
/// <para>
/// The alternative — keying a mark on the row's CONTENT so it survives a refresh — needs a key function
/// per grid, fifty-two of them, and a wrong key silently moves somebody's "do not do" onto a different
/// index. For a note whose whole value is being right about which row it is on, being wrong is worse than
/// being brief. Content keys are worth adding to individual grids on request; they are not worth writing
/// on spec.
/// </para>
///
/// <para>
/// A <see cref="ConditionalWeakTable{TKey,TValue}"/> holds them, so a mark never keeps a discarded result
/// set alive: when the grid drops the old rows the marks go with them, with no bookkeeping and no leak.
/// </para>
/// </summary>
public static class DataGridRowMarks
{
    /* Deliberately translucent. These sit UNDER the selection highlight and the alert-severity row
       triggers, both of which the operator still needs to read on a marked row — an opaque fill would
       win the argument with every one of them. */
    private static readonly Brush s_done = Frozen(Color.FromArgb(0x33, 0x22, 0xC5, 0x5E));
    private static readonly Brush s_toDo = Frozen(Color.FromArgb(0x33, 0xD9, 0x77, 0x06));
    private static readonly Brush s_doNot = Frozen(Color.FromArgb(0x33, 0xDC, 0x26, 0x26));

    private static readonly ConditionalWeakTable<object, object> s_marks = new();

    /// <summary>Marks (or with <see cref="DataGridRowMark.None"/>, unmarks) one row object.</summary>
    public static void Set(object? row, DataGridRowMark mark)
    {
        if (row is null)
        {
            return;
        }

        s_marks.Remove(row);

        if (mark != DataGridRowMark.None)
        {
            /* Boxed: ConditionalWeakTable's value must be a reference type, and the enum is not. */
            s_marks.Add(row, mark);
        }
    }

    /// <summary>The mark on a row, or <see cref="DataGridRowMark.None"/>.</summary>
    public static DataGridRowMark Get(object? row)
        => row is not null && s_marks.TryGetValue(row, out var mark) && mark is DataGridRowMark value
            ? value
            : DataGridRowMark.None;

    /// <summary>
    /// Applies the mark to a realised row. Call from <c>DataGrid.LoadingRow</c>.
    ///
    /// <para>That event is the reason no row model needs a new property and no XAML needs a trigger — and
    /// it is also why the paint must handle the UNMARKED case explicitly. WPF RECYCLES row containers as
    /// you scroll, so a container that carried a mark a moment ago arrives holding that brush for a
    /// different row; clearing it is what stops a mark smearing down the grid.</para>
    /// </summary>
    public static void Apply(DataGridRow? row)
    {
        if (row is null)
        {
            return;
        }

        var brush = Get(row.Item) switch
        {
            DataGridRowMark.Done => s_done,
            DataGridRowMark.ToDo => s_toDo,
            DataGridRowMark.DoNot => s_doNot,
            _ => null,
        };

        if (brush is null)
        {
            row.ClearValue(DataGridRow.BackgroundProperty);
        }
        else
        {
            row.Background = brush;
        }
    }

    /// <summary>
    /// Marks every selected row of a grid and repaints, so a multi-row selection is one action rather
    /// than one per row — which is how the request describes using it ("mark some of indexes rows").
    /// </summary>
    public static void SetSelection(DataGrid? grid, DataGridRowMark mark)
    {
        if (grid is null)
        {
            return;
        }

        foreach (var item in Selected(grid))
        {
            Set(item, mark);
        }

        Repaint(grid);
    }

    /// <summary>Re-applies marks to the rows currently realised. Cheap: only what is on screen exists.</summary>
    public static void Repaint(DataGrid? grid)
    {
        if (grid is null)
        {
            return;
        }

        foreach (var item in grid.Items)
        {
            if (grid.ItemContainerGenerator.ContainerFromItem(item) is DataGridRow row)
            {
                Apply(row);
            }
        }
    }

    /// <summary>
    /// The ONE context-menu handler every grid shares. The mark is taken from the menu item's
    /// <c>Tag</c>, so the four items differ only in their XAML and no host needs four handlers.
    ///
    /// <para>The grid is resolved the same way the copy and export items already resolve it, through
    /// <see cref="DataGridHelpers.FindParentDataGrid"/> — one placement rule for the whole shared menu
    /// rather than a second one that could disagree with it.</para>
    /// </summary>
    public static void OnMarkMenuItemClicked(object sender)
    {
        if (sender is not MenuItem item)
        {
            return;
        }

        var mark = item.Tag as string switch
        {
            nameof(DataGridRowMark.Done) => DataGridRowMark.Done,
            nameof(DataGridRowMark.ToDo) => DataGridRowMark.ToDo,
            nameof(DataGridRowMark.DoNot) => DataGridRowMark.DoNot,
            _ => DataGridRowMark.None,
        };

        SetSelection(DataGridHelpers.FindParentDataGrid(sender), mark);
    }

    private static IEnumerable<object> Selected(DataGrid grid)
    {
        /* SelectedItems is empty for a single-select grid, where SelectedItem carries it instead. Both
           are checked so this works on every grid rather than on the ones that happen to be Extended. */
        if (grid.SelectedItems.Count > 0)
        {
            foreach (var item in grid.SelectedItems)
            {
                if (item is not null)
                {
                    yield return item;
                }
            }
        }
        else if (grid.SelectedItem is { } single)
        {
            yield return single;
        }
    }

    private static Brush Frozen(Color color)
    {
        var brush = new SolidColorBrush(color);
        brush.Freeze();
        return brush;
    }
}
