/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
        EventManager.RegisterClassHandler(typeof(TabItem), FrameworkElement.LoadedEvent, new RoutedEventHandler(OnTabItemLoaded));
        EventManager.RegisterClassHandler(typeof(System.Windows.Controls.Primitives.DataGridColumnHeader), FrameworkElement.LoadedEvent, new RoutedEventHandler(OnColumnHeaderLoaded));
    }

    private static void OnTabItemLoaded(object sender, RoutedEventArgs e)
    {
        if (sender is TabItem tab)
        {
            Name(tab, tab.Header);
        }
    }

    private static void OnColumnHeaderLoaded(object sender, RoutedEventArgs e)
    {
        if (sender is System.Windows.Controls.Primitives.DataGridColumnHeader header)
        {
            Name(header, header.Content);
        }
    }

    /// <summary>Names <paramref name="element"/> from <paramref name="header"/> unless it already has a name or the header is plain text.</summary>
    public static void Name(DependencyObject element, object? header)
    {
        if (header is null || header is string || !string.IsNullOrEmpty(AutomationProperties.GetName(element)))
        {
            return;
        }

        var text = VisibleText(header);

        if (!string.IsNullOrWhiteSpace(text))
        {
            AutomationProperties.SetName(element, text);
        }
    }

    /// <summary>
    /// The first piece of visible text in a header: the string itself, a text block's text, or the first non-empty text
    /// block found inside a panel or content control, in the order the user reads them. Null when there is none.
    /// </summary>
    public static string? VisibleText(object? header)
    {
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
                return tb.Text?.Trim();
            case DependencyObject d:
                foreach (var child in LogicalTreeHelperChildren(d))
                {
                    var found = VisibleText(child);

                    if (!string.IsNullOrWhiteSpace(found))
                    {
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
