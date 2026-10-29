/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The rule behind the Collector Schedules window's search box and "Disabled only" box (#6768): a row is shown when its collector
/// name contains the typed text (any letter case) AND, with "Disabled only" on, the collector is turned off. Empty text and the box
/// cleared show every row. The window applies this to the grid's view only, never to the list that Save writes.
/// </summary>
public static class CollectorScheduleFilter
{
    public static bool Matches(string name, bool enabled, string? text, bool disabledOnly)
    {
        if (disabledOnly && enabled)
        {
            return false;
        }

        var needle = text?.Trim();
        return string.IsNullOrEmpty(needle) || name.Contains(needle, StringComparison.OrdinalIgnoreCase);
    }
}
