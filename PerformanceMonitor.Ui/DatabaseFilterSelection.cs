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
    /// The names to store: the ticked names, or none when every listed database is ticked (or none is).
    /// </summary>
    public static List<string> Stored(IReadOnlyList<(string Name, bool IsSelected)> items)
    {
        var ticked = new List<string>();
        foreach (var (name, isSelected) in items)
        {
            if (isSelected)
            {
                ticked.Add(name);
            }
        }

        return items.Count > 0 && ticked.Count == items.Count ? new List<string>() : ticked;
    }
}
