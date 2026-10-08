/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Windows;
using System.Windows.Automation;
using System.Windows.Controls;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Gives every tab and every grid column header an accessible name equal to its visible text (Lite click-through F17).
///
/// <para>A tab whose header is a panel (the server tabs: a name, a badge and a close button) and a grid column whose header
/// is a panel (the filter button beside the title) have no <c>AutomationProperties.Name</c>, so UI Automation names them by
/// the type name of the header ("System.Windows.Controls.StackPanel"). A screen reader or a UI test then cannot say which tab or
/// column it is on. A string header is named correctly by WPF already, and a name set by hand always wins.</para>
///
/// <para>It is a class handler on the control types, not a style or a template: every theme and every control that
/// declares its own template gets it, and no header has to be edited one at a time.</para>
/// </summary>
public static class AccessibleNames
{
    private static bool s_registered;

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
        EventManager.RegisterClassHandler(typeof(TabItem), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnTabItemLoaded));
        EventManager.RegisterClassHandler(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnColumnHeaderLoaded));
        EventManager.RegisterClassHandler(typeof(DataGridCell), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnCellSized));
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

    private static void OnTabItemLoaded(object sender, RoutedEventArgs e)
    {
        if (sender is TabItem tab && tab.GetValue(WatcherProperty) is null)
        {
            Name(tab, tab.Header);
        }
    }

    private static void OnColumnHeaderLoaded(object sender, RoutedEventArgs e)
    {
        /* SizeChanged fires on every resize: an element a watcher already keeps current is left alone. */
        if (sender is System.Windows.Controls.Primitives.DataGridColumnHeader header && header.GetValue(WatcherProperty) is null)
        {
            Name(header, header.Content);
        }
    }

    /// <summary>
    /// A grid cell whose text is empty (a Retries column on a job that never retried) is named by UI Automation from the
    /// row's type name: "Item: PerformanceMonitor...ViewerJobHistoryRow, Column Display Index: 7". The cell is named
    /// "&lt;column&gt;: blank" instead, and the name is dropped again if the cell's text fills in. A cell that has text, or
    /// a name set by hand (a cell style), is left alone.
    /// </summary>
    private static void OnCellSized(object sender, RoutedEventArgs e)
    {
        if (sender is not DataGridCell cell || cell.Content is not TextBlock text)
        {
            return;
        }

        if (!string.IsNullOrEmpty(text.Text))
        {
            /* The text filled in while no watcher was attached (the cell was unloaded): drop the blank name. */
            if (cell.GetValue(AutoNameProperty) is string stale && AutomationProperties.GetName(cell) == stale)
            {
                cell.ClearValue(AutomationProperties.NameProperty);
                cell.ClearValue(AutoNameProperty);
            }

            return;
        }

        var title = cell.Column is null ? null : VisibleText(cell.Column.Header);

        if (string.IsNullOrWhiteSpace(title) || !SetAuto(cell, title + ": blank"))
        {
            return;
        }

        if (cell.GetValue(WatcherProperty) is null)
        {
            var descriptor = DependencyPropertyDescriptor.FromProperty(TextBlock.TextProperty, typeof(TextBlock));
            EventHandler handler = (_, _) =>
            {
                if (!string.IsNullOrEmpty(text.Text) && cell.GetValue(AutoNameProperty) is string auto && AutomationProperties.GetName(cell) == auto)
                {
                    cell.ClearValue(AutomationProperties.NameProperty);
                    cell.ClearValue(AutoNameProperty);
                }
            };
            descriptor.AddValueChanged(text, handler);
            cell.SetValue(WatcherProperty, new Watcher { Source = text, Handler = handler });
            cell.Unloaded -= OnUnloaded;
            cell.Unloaded += OnUnloaded;
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
        element.SetValue(WatcherProperty, new Watcher { Source = source, Handler = handler });
        fe.Unloaded -= OnUnloaded;
        fe.Unloaded += OnUnloaded;
    }

    private static void Unwatch(DependencyObject element)
    {
        if (element.GetValue(WatcherProperty) is Watcher watcher)
        {
            DependencyPropertyDescriptor.FromProperty(TextBlock.TextProperty, typeof(TextBlock)).RemoveValueChanged(watcher.Source, watcher.Handler);
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
