/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Ui;

/// <summary>
/// What the Databases filter stores for the boxes ticked in its popup (Lite and the Darling Viewer share the rule).
/// An empty selection means "All" (no filter), so a database collected later is included. Ticking every box, by hand
/// or with Select All, is also "All": storing the twelve names would be a real twelve-database filter that left out
/// the thirteenth and read "12 of 12" on the button.
/// </summary>
public static class DatabaseFilterSelection
{
    /// <summary>
    /// The names to store: the ticked names, or none when every COLLECTED database is ticked. The comparison is against
    /// the collected names the read returned, not the list on screen: that list also holds the sticky names of a filter
    /// for databases not collected yet, and when the read failed (<paramref name="collectedNames"/> is null or empty) the
    /// list is nothing but those sticky names, so "every box ticked" would widen a narrow filter to All (#5554).
    /// </summary>
    public static List<string> Stored(IReadOnlyList<(string Name, bool IsSelected)> items, IReadOnlyCollection<string>? collectedNames)
    {
        var ticked = new List<string>();
        foreach (var (name, isSelected) in items)
        {
            if (isSelected)
            {
                ticked.Add(name);
            }
        }

        if (collectedNames == null || collectedNames.Count == 0)
        {
            return ticked;
        }

        var tickedSet = new HashSet<string>(ticked, System.StringComparer.OrdinalIgnoreCase);
        foreach (var name in collectedNames)
        {
            if (!tickedSet.Contains(name))
            {
                return ticked;
            }
        }

        return new List<string>();
    }
}
