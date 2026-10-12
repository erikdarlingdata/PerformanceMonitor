/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Media3D;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Feeds the time range picker's "Data starts ..." note from a "Showing since" banner site (#5562 R7). Every banner on the
/// server tab, and Job History's, goes through <see cref="ViewerServerTab.UpdateTruncationBanner"/> (the event surfaces through
/// <see cref="ViewerServerTab.ShowEventDataStartAsync"/>, which ends in it), so that one method hands its floor here: the answer
/// of the data-start probe the site already awaited, with no query of its own. The floor reaches the picker of the tab the
/// banner sits in, and only while the banner is on screen: the content of a tab that is not selected has no parent in the
/// visual tree, so a banner of a background tab never moves the note of the tab in front.
/// </summary>
internal static class ViewerDataStartNote
{
    /// <summary>Hands <paramref name="floor"/> to the picker of the tab that holds <paramref name="banner"/>, if the banner is on
    /// screen in one. A banner outside any tab (a test's bare control, a detached tab) feeds nothing.</summary>
    internal static void Feed(DependencyObject banner, DateTime? floor)
    {
        for (var node = ParentOf(banner); node is not null; node = ParentOf(node))
        {
            switch (node)
            {
                case ViewerServerTab serverTab:
                    serverTab.RecordDataStart(floor);
                    return;
                case JobHistoryTab jobHistory:
                    jobHistory.RecordDataStart(floor);
                    return;
            }
        }
    }

    /// <summary>The first ancestor of <paramref name="start"/> that is a <typeparamref name="T"/>, by the visual tree (the logical
    /// tree for a node that is not a visual), or null. The walk <see cref="Feed"/> makes, generic so a test can run it on plain
    /// panels.</summary>
    internal static T? FindAncestor<T>(DependencyObject start) where T : DependencyObject
    {
        for (var node = ParentOf(start); node is not null; node = ParentOf(node))
        {
            if (node is T found)
            {
                return found;
            }
        }

        return null;
    }

    /* The visual parent, so the content of a tab that is not selected (not connected to the visual tree) ends the walk: its logical
       parent is its TabItem, which would feed a background tab's floor to the tab in front. */
    private static DependencyObject? ParentOf(DependencyObject node) =>
        node is Visual or Visual3D ? VisualTreeHelper.GetParent(node) : LogicalTreeHelper.GetParent(node);
}
