/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The selection a time-range slicer carries across a reload of its data. A reload (an auto-refresh, a filter change) rebuilds the
/// buckets over a new span, and the host has loaded its grid for the whole window, so a narrowed selection that survives the reload
/// has to reach the grid again or the highlight and the grid disagree. The carried selection is the same two instants on the new
/// span, each clamped to it. A full-window selection is not carried, and a selection the new span no longer holds is dropped; in
/// both cases the slicer shows the whole window, as the grid does.
/// </summary>
public static class SlicerReloadSelection
{
    /// <summary>
    /// The previous selection as positions on the new span, in [0, 1], or null when there is nothing to carry: no previous
    /// selection, a selection that covers the whole span, or one the span no longer holds any of.
    /// </summary>
    public static (double Start, double End)? Carry(
        DateTime? previousStartUtc, DateTime? previousEndUtc, DateTime spanStartUtc, DateTime spanEndUtc)
    {
        if (!previousStartUtc.HasValue || !previousEndUtc.HasValue)
            return null;

        var span = spanEndUtc.Ticks - spanStartUtc.Ticks;
        if (span <= 0)
            return null;

        var start = Math.Clamp((double)(previousStartUtc.Value.Ticks - spanStartUtc.Ticks) / span, 0, 1);
        var end = Math.Clamp((double)(previousEndUtc.Value.Ticks - spanStartUtc.Ticks) / span, 0, 1);

        if (end <= start)
            return null;
        if (start <= 0 && end >= 1)
            return null;

        return (start, end);
    }
}
