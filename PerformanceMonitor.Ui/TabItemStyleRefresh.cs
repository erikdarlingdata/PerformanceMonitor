/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #8221: a TabControl's ItemContainerStyle reaches a TabItem written in the XAML only once, when the item is prepared. When the
    /// control's style comes from a theme style's setter (Lite's server tab strip), a run-time theme switch gives the control the new
    /// theme's item style but leaves its tabs on the old one, so the selected tab keeps the old theme's ink and underline on a Light or
    /// Cool Breeze page. <see cref="ThemeManager.Apply"/> calls <see cref="Capture"/> before it swaps the theme dictionary and
    /// <see cref="Reapply"/> after, for every open window in both apps.
    /// </summary>
    internal static class TabItemStyleRefresh
    {
        /// <summary>
        /// The tabs, in every open window, whose Style is the very style their control hands out. A tab that carries a style of its own
        /// (a different object) is not in the list, so it is never overwritten.
        /// </summary>
        internal static List<(TabControl Tabs, TabItem Item)> Capture()
        {
            var found = new List<(TabControl Tabs, TabItem Item)>();
            /* Windows can be read only on the thread that owns the Application. */
            if (Application.Current is { } app && app.Dispatcher.CheckAccess())
            {
                foreach (var window in app.Windows.OfType<Window>())
                {
                    Walk(window, found);
                }
            }

            return found;
        }

        /// <summary>Hands each captured tab its control's current item style, when the control's style has changed under it.</summary>
        internal static void Reapply(List<(TabControl Tabs, TabItem Item)> captured)
        {
            foreach (var (tabs, item) in captured)
            {
                if (tabs.ItemContainerStyle is { } style && !ReferenceEquals(item.Style, style))
                {
                    item.Style = style;
                }
            }
        }

        private static void Walk(DependencyObject parent, List<(TabControl Tabs, TabItem Item)> found)
        {
            if (parent is TabControl { ItemContainerStyle: { } style } tabs)
            {
                foreach (var item in tabs.Items.OfType<TabItem>())
                {
                    if (ReferenceEquals(item.Style, style))
                    {
                        found.Add((tabs, item));
                    }
                }
            }

            foreach (var child in LogicalTreeHelper.GetChildren(parent).OfType<DependencyObject>())
            {
                Walk(child, found);
            }
        }
    }
}
