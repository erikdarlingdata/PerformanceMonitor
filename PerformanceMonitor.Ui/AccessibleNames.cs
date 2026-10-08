/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Automation;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Gives every tab, grid column header, grid row and empty grid cell an accessible name taken from what is on screen
/// (Lite click-through F17).
///
/// <para>A tab whose header is a panel (the server tabs: a name, a badge and a close button) and a grid column whose header
/// is a panel (the filter button beside the title) have no <c>AutomationProperties.Name</c>, so UI Automation names them by
/// the type name of the header ("System.Windows.Controls.StackPanel"). A grid row is named by its data item's type name
/// ("...ViewerJobHistoryRow"). A screen reader or a UI test then cannot say which tab, column or row it is on. A string header is named correctly by WPF already, and a name set by hand always wins.</para>
///
/// <para>It is a class handler on the control types, not a style or a template: every theme and every control that
/// declares its own template gets it, and no header has to be edited one at a time.</para>
///
/// <para>Cost: the handlers run on every <c>SizeChanged</c> of every header, tab, row and cell. After an element has been
/// looked at once, that path is a property read and a return: no allocation and no tree walk. The tree is walked, and text
/// watchers are added, only when the element is first seen, when its content or text changes, and when a virtualized row or cell is
/// reused for another item.</para>
/// </summary>
public static class AccessibleNames
{
    private static bool s_registered;

    /// <summary>
    /// Text watchers this thread has attached and not yet removed. Diagnostic only: a test reads it to show the count follows the
    /// cells and rows on screen, not the rows in the data, when a virtualized grid is scrolled.
    /// </summary>
    [ThreadStatic]
    private static int s_watcherCount;

    internal static int WatcherCount => s_watcherCount;

    /// <summary>Registers the handlers once for the process. Call it from the application's startup.</summary>
    public static void Register()
    {
        if (s_registered)
        {
            return;
        }

        s_registered = true;
        /* SizeChanged, not Loaded: WPF raises Loaded straight to an instance's own handlers and never runs class handlers for it,
           so a class handler on LoadedEvent never ran and the headers kept the type name of their panel (release walk). Every
           element gets its first SizeChanged when it is first laid out, before a screen reader can ask for its name. */
        EventManager.RegisterClassHandler(typeof(TabItem), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnTabItemSized));
        EventManager.RegisterClassHandler(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnColumnHeaderSized));
        EventManager.RegisterClassHandler(typeof(DataGridCell), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnCellSized));
        EventManager.RegisterClassHandler(typeof(DataGridRow), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnRowSized));
    }

    /// <summary>The name this class set itself, so a later change of the header text can replace it while a name set by hand never is.</summary>
    private static readonly DependencyProperty AutoNameProperty = DependencyProperty.RegisterAttached(
        "AutoName", typeof(string), typeof(AccessibleNames), new PropertyMetadata(null));

    /// <summary>The text block a name was read from, and the handler that re-reads the name when its text changes.</summary>
    private sealed class Watcher
    {
        public TextBlock Source = null!;
        public EventHandler Handler = null!;
    }

    private static readonly DependencyProperty WatcherProperty = DependencyProperty.RegisterAttached(
        "Watcher", typeof(Watcher), typeof(AccessibleNames), new PropertyMetadata(null));

    /// <summary>
    /// The header object a tab or column header was last looked at with. The per-event path compares it by reference with the
    /// current header and returns: a header with no text in it (nothing to watch) is not walked again on every resize.
    /// </summary>
    private static readonly DependencyProperty CheckedHeaderProperty = DependencyProperty.RegisterAttached(
        "CheckedHeader", typeof(object), typeof(AccessibleNames), new PropertyMetadata(null));

    internal static void OnTabItemSized(object sender, RoutedEventArgs e)
    {
        if (sender is TabItem tab && !ReferenceEquals(tab.GetValue(CheckedHeaderProperty), tab.Header))
        {
            Name(tab, tab.Header);
        }
    }

    internal static void OnColumnHeaderSized(object sender, RoutedEventArgs e)
    {
        /* SizeChanged fires on every resize: a header already looked at with this content is left alone. */
        if (sender is System.Windows.Controls.Primitives.DataGridColumnHeader header && !ReferenceEquals(header.GetValue(CheckedHeaderProperty), header.Content))
        {
            Name(header, header.Content);
        }
    }

    // ---- cells and rows: names from the visible text, kept current by watchers on the text blocks they were read from ----

    /// <summary>
    /// What is attached to one grid cell or row: the text blocks its name was read from, the one handler that re-reads the
    /// name when any of them changes, and the flags that keep the per-event path to a single check.
    /// </summary>
    private sealed class TextWatch
    {
        public bool Active;
        public bool Hooked;
        public bool ContentWatched;
        public bool RefreshPending;
        public readonly List<TextBlock> Sources = new();
        public EventHandler Changed = null!;
    }

    private static readonly DependencyProperty CellWatchProperty = DependencyProperty.RegisterAttached(
        "CellWatch", typeof(TextWatch), typeof(AccessibleNames), new PropertyMetadata(null));

    private static readonly DependencyProperty RowWatchProperty = DependencyProperty.RegisterAttached(
        "RowWatch", typeof(TextWatch), typeof(AccessibleNames), new PropertyMetadata(null));

    /// <summary>
    /// A grid cell with nothing visible in it (a Retries column on a job that never retried) is named by UI Automation from the
    /// row's type name: "Item: PerformanceMonitor...ViewerJobHistoryRow, Column Display Index: 7". The cell is named
    /// "&lt;column&gt;: blank" instead, and the name is dropped again if its text fills in. A text cell and a cell built from a template
    /// are judged the same way: it is blank when it holds only empty text blocks and plain layout panels. Anything else (a check box,
    /// a button, an image, a coloured box) counts as content and the cell is left alone, as is a cell with text or a name set by hand.
    /// </summary>
    internal static void OnCellSized(object sender, RoutedEventArgs e)
    {
        if (sender is DataGridCell cell && cell.GetValue(CellWatchProperty) is not TextWatch { Active: true })
        {
            ActivateCell(cell);
        }
    }

    private static void ActivateCell(DataGridCell cell)
    {
        var watch = cell.GetValue(CellWatchProperty) as TextWatch;

        if (watch is null)
        {
            watch = new TextWatch();
            watch.Changed = (_, _) => RefreshCell(cell, watch);
            cell.SetValue(CellWatchProperty, watch);
        }

        watch.Active = true;

        if (!watch.Hooked)
        {
            watch.Hooked = true;
            cell.Unloaded += OnCellUnloaded;
            cell.Loaded += OnCellLoaded;
        }

        if (!watch.ContentWatched)
        {
            watch.ContentWatched = true;
            s_watcherCount++;
            DependencyPropertyDescriptor.FromProperty(ContentControl.ContentProperty, typeof(DataGridCell)).AddValueChanged(cell, watch.Changed);
        }

        RefreshCell(cell, watch);
    }

    private static void OnCellLoaded(object sender, RoutedEventArgs e)
    {
        /* A cell that was unloaded and put back may not be resized, so its first SizeChanged would not run again. */
        if (sender is DataGridCell cell && cell.GetValue(CellWatchProperty) is TextWatch { Active: false })
        {
            ActivateCell(cell);
        }
    }

    private static void OnCellUnloaded(object sender, RoutedEventArgs e)
    {
        if (sender is DataGridCell cell && cell.GetValue(CellWatchProperty) is TextWatch watch)
        {
            Deactivate(cell, watch, ContentControl.ContentProperty, typeof(DataGridCell));
        }
    }

    private static void RefreshCell(DataGridCell cell, TextWatch watch)
    {
        var content = cell.Content;

        if (content is null)
        {
            /* Not generated yet: the Content watcher calls this again when it is. */
            return;
        }

        var leaves = new List<TextBlock>();
        var blank = content is string s ? string.IsNullOrWhiteSpace(s) : content is DependencyObject d && IsBlank(d, leaves);
        Retarget(watch, leaves);

        if (blank)
        {
            var title = cell.Column is null ? null : VisibleText(cell.Column.Header);

            if (!string.IsNullOrWhiteSpace(title))
            {
                SetAuto(cell, title + ": blank");
                return;
            }
        }

        ClearAuto(cell);
    }

    /// <summary>
    /// A grid row is named by UI Automation from its data item's type name. It is named from the first two visible columns that
    /// have text instead: "Time 06:01:12, Server example-sql-01". The same rule for every grid, templated cells included.
    /// </summary>
    internal static void OnRowSized(object sender, RoutedEventArgs e)
    {
        if (sender is DataGridRow row && row.GetValue(RowWatchProperty) is not TextWatch { Active: true })
        {
            ActivateRow(row);
        }
    }

    private static void ActivateRow(DataGridRow row)
    {
        var watch = row.GetValue(RowWatchProperty) as TextWatch;

        if (watch is null)
        {
            watch = new TextWatch();
            watch.Changed = (_, _) => RefreshRow(row, watch);
            row.SetValue(RowWatchProperty, watch);
        }

        watch.Active = true;

        if (!watch.Hooked)
        {
            watch.Hooked = true;
            row.Unloaded += OnRowUnloaded;
            row.Loaded += OnRowLoaded;
            row.DataContextChanged += OnRowDataContextChanged;
        }

        RefreshRow(row, watch);
    }

    private static void OnRowLoaded(object sender, RoutedEventArgs e)
    {
        if (sender is DataGridRow row && row.GetValue(RowWatchProperty) is TextWatch { Active: false })
        {
            ActivateRow(row);
        }
    }

    private static void OnRowUnloaded(object sender, RoutedEventArgs e)
    {
        if (sender is DataGridRow row && row.GetValue(RowWatchProperty) is TextWatch watch)
        {
            Deactivate(row, watch, null, null);
        }
    }

    /// <summary>
    /// A virtualized grid reuses a row for another item by changing its DataContext. The row's cells take the new item's values
    /// afterwards, so the name is re-read once at Loaded priority, when they have.
    /// </summary>
    private static void OnRowDataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
    {
        if (sender is not DataGridRow row || row.GetValue(RowWatchProperty) is not TextWatch { Active: true } watch || watch.RefreshPending)
        {
            return;
        }

        watch.RefreshPending = true;
        row.Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() =>
        {
            watch.RefreshPending = false;

            if (watch.Active)
            {
                RefreshRow(row, watch);
            }
        }));
    }

    private static void RefreshRow(DataGridRow row, TextWatch watch)
    {
        var grid = ItemsControl.ItemsControlFromItemContainer(row) as DataGrid;

        if (grid is null)
        {
            return;
        }

        var leaves = new List<TextBlock>();
        var parts = new List<string>(2);

        foreach (var column in grid.Columns.Where(c => c.Visibility == Visibility.Visible).OrderBy(c => c.DisplayIndex))
        {
            var content = column.GetCellContent(row);

            if (content is null)
            {
                continue;
            }

            var own = new List<TextBlock>();
            IsBlank(content, own);
            leaves.AddRange(own);
            var text = own.Select(t => t.Text?.Trim()).FirstOrDefault(t => !string.IsNullOrEmpty(t));

            if (string.IsNullOrEmpty(text))
            {
                continue;
            }

            var title = VisibleText(column.Header);
            parts.Add(string.IsNullOrWhiteSpace(title) ? text : title + " " + text);

            if (parts.Count == 2)
            {
                break;
            }
        }

        Retarget(watch, leaves);

        if (parts.Count > 0)
        {
            SetAuto(row, string.Join(", ", parts));
        }
        else
        {
            ClearAuto(row);
        }
    }

    /// <summary>
    /// True when <paramref name="node"/> shows nothing: only empty text blocks inside layout panels and plain borders. Every text
    /// block met is added to <paramref name="leaves"/> (empty or not) so the caller can watch them. Anything else, including a
    /// panel or border with a background, is content.
    /// </summary>
    private static bool IsBlank(DependencyObject node, List<TextBlock> leaves)
    {
        var blank = true;
        Collect(node, leaves, ref blank);

        foreach (var t in leaves)
        {
            if (!string.IsNullOrWhiteSpace(t.Text))
            {
                return false;
            }
        }

        return blank;
    }

    private static void Collect(DependencyObject node, List<TextBlock> leaves, ref bool blank)
    {
        switch (node)
        {
            case TextBlock tb:
                leaves.Add(tb);
                return;
            case Panel { Background: not null }:
            case Border { Background: not null }:
            case Border { BorderBrush: not null }:
                blank = false;
                return;
            case Panel:
            case Decorator:
            case ContentPresenter:
                for (var i = 0; i < VisualTreeHelper.GetChildrenCount(node); i++)
                {
                    Collect(VisualTreeHelper.GetChild(node, i), leaves, ref blank);
                }

                return;
            default:
                /* A check box, a button, an image, a shape: visible content that is not text. */
                blank = false;
                return;
        }
    }

    /// <summary>Points the watch at exactly <paramref name="sources"/>: removes handlers from text blocks no longer read, adds them to new ones.</summary>
    private static void Retarget(TextWatch watch, List<TextBlock> sources)
    {
        var descriptor = DependencyPropertyDescriptor.FromProperty(TextBlock.TextProperty, typeof(TextBlock));

        for (var i = watch.Sources.Count - 1; i >= 0; i--)
        {
            if (!sources.Contains(watch.Sources[i]))
            {
                descriptor.RemoveValueChanged(watch.Sources[i], watch.Changed);
                watch.Sources.RemoveAt(i);
                s_watcherCount--;
            }
        }

        foreach (var source in sources)
        {
            if (!watch.Sources.Contains(source))
            {
                descriptor.AddValueChanged(source, watch.Changed);
                watch.Sources.Add(source);
                s_watcherCount++;
            }
        }
    }

    private static void Deactivate(DependencyObject element, TextWatch watch, DependencyProperty? contentProperty, Type? owner)
    {
        Retarget(watch, new List<TextBlock>());

        if (watch.ContentWatched && contentProperty is not null && owner is not null)
        {
            DependencyPropertyDescriptor.FromProperty(contentProperty, owner).RemoveValueChanged(element, watch.Changed);
            watch.ContentWatched = false;
            s_watcherCount--;
        }

        watch.Active = false;
    }

    private static void ClearAuto(DependencyObject element)
    {
        if (element.GetValue(AutoNameProperty) is string auto)
        {
            if (AutomationProperties.GetName(element) == auto)
            {
                element.ClearValue(AutomationProperties.NameProperty);
            }

            element.ClearValue(AutoNameProperty);
        }
    }

    private static void OnUnloaded(object sender, RoutedEventArgs e)
    {
        if (sender is DependencyObject element)
        {
            Unwatch(element);
        }
    }

    /// <summary>
    /// Names <paramref name="element"/> from <paramref name="header"/> unless a hand-set name is there or the header is plain text.
    /// A name this class set before is replaced when the header text has changed since (an edited server tab, a bound column
    /// title), and a changed text is picked up from then on without another call.
    /// </summary>
    public static void Name(DependencyObject element, object? header)
    {
        if (header is null || header is string)
        {
            element.SetValue(CheckedHeaderProperty, header);
            return;
        }

        var text = VisibleText(header, out var source);

        if (!string.IsNullOrWhiteSpace(text) && SetAuto(element, text))
        {
            if (element is System.Windows.Controls.Primitives.DataGridColumnHeader)
            {
                NameFilterButtons(header, text);
            }
        }

        Watch(element, source);
        /* After Watch: attaching a new watcher goes through Unwatch, which forgets what was checked. */
        element.SetValue(CheckedHeaderProperty, header);
    }

    /// <summary>Sets the name unless one was set by hand; true when the element carries this class's name afterwards.</summary>
    private static bool SetAuto(DependencyObject element, string text)
    {
        var current = AutomationProperties.GetName(element);
        var auto = (string?)element.GetValue(AutoNameProperty);

        if (!string.IsNullOrEmpty(current) && !string.Equals(current, auto, StringComparison.Ordinal))
        {
            return false;
        }

        if (!string.Equals(current, text, StringComparison.Ordinal))
        {
            AutomationProperties.SetName(element, text);
        }

        element.SetValue(AutoNameProperty, text);
        return true;
    }

    /// <summary>The filter button beside a column title has only a private-use glyph: a screen reader reads it as "Filter &lt;column&gt;".</summary>
    private static void NameFilterButtons(object header, string columnTitle)
    {
        if (header is System.Windows.Controls.Primitives.ButtonBase button)
        {
            SetAuto(button, "Filter " + columnTitle);
        }
        else if (header is DependencyObject d)
        {
            foreach (var child in LogicalTreeHelperChildren(d))
            {
                NameFilterButtons(child, columnTitle);
            }
        }
    }

    private static void Watch(DependencyObject element, TextBlock? source)
    {
        var watcher = (Watcher?)element.GetValue(WatcherProperty);

        if (watcher is not null && ReferenceEquals(watcher.Source, source))
        {
            return;
        }

        Unwatch(element);

        if (source is null || element is not FrameworkElement fe)
        {
            return;
        }

        var descriptor = DependencyPropertyDescriptor.FromProperty(TextBlock.TextProperty, typeof(TextBlock));
        EventHandler handler = (_, _) =>
        {
            object? current = element switch
            {
                TabItem t => t.Header,
                System.Windows.Controls.Primitives.DataGridColumnHeader h => h.Content,
                _ => null,
            };
            Name(element, current);
        };
        descriptor.AddValueChanged(source, handler);
        s_watcherCount++;
        element.SetValue(WatcherProperty, new Watcher { Source = source, Handler = handler });
        fe.Unloaded -= OnUnloaded;
        fe.Unloaded += OnUnloaded;
    }

    private static void Unwatch(DependencyObject element)
    {
        /* Forgetting what was checked lets the next SizeChanged look again after a header is unloaded and put back. */
        element.ClearValue(CheckedHeaderProperty);

        if (element.GetValue(WatcherProperty) is Watcher watcher)
        {
            DependencyPropertyDescriptor.FromProperty(TextBlock.TextProperty, typeof(TextBlock)).RemoveValueChanged(watcher.Source, watcher.Handler);
            s_watcherCount--;
            element.ClearValue(WatcherProperty);
        }
    }

    /// <summary>
    /// The first piece of visible text in a header: the string itself, a text block's text, or the first non-empty text
    /// block found inside a panel or content control, in the order the user reads them. Null when there is none.
    /// </summary>
    public static string? VisibleText(object? header) => VisibleText(header, out _);

    private static string? VisibleText(object? header, out TextBlock? source)
    {
        source = null;

        switch (header)
        {
            case null:
                return null;
            case string s:
                return s.Trim();
            case System.Windows.Controls.Primitives.ButtonBase:
                /* The filter button beside a column title: its glyph is not the title. */
                return null;
            case TextBlock tb:
                source = tb;
                return tb.Text?.Trim();
            case DependencyObject d:
                foreach (var child in LogicalTreeHelperChildren(d))
                {
                    var found = VisibleText(child, out var childSource);

                    if (!string.IsNullOrWhiteSpace(found))
                    {
                        source = childSource;
                        return found;
                    }
                }

                return null;
            default:
                return header.ToString();
        }
    }

    private static System.Collections.IEnumerable LogicalTreeHelperChildren(DependencyObject d)
    {
        if (d is ContentControl control && control.Content is not null)
        {
            return new[] { control.Content };
        }

        return LogicalTreeHelper.GetChildren(d);
    }
}
