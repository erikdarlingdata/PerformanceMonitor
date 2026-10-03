/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>
    /// Raises or hides the "Showing since" banner of an EVENT surface (#4966): a grid that filters on the event's own
    /// time (blocked process reports, deadlocks), where a server's first collection stores the server's history, so a
    /// row can carry an event time from before the coverage the probe found. The banner names the earlier of the
    /// probe's answer and the earliest event the grid shows (<see cref="ViewerEventDataStart.Of"/>), never a time later
    /// than a row on screen. The probe is awaited through <see cref="DataStartOrNullAsync"/>, so a probe that throws
    /// costs this banner and nothing after it.
    /// </summary>
    /// <param name="banner">The surface's banner.</param>
    /// <param name="probe">The surface's data-start probe, started beside its read.</param>
    /// <param name="surface">The surface's name, for the log line a failed probe writes.</param>
    /// <param name="startUtc">The start of the window the grid just drew.</param>
    /// <param name="shownEventTimesUtc">The event time of each row the grid shows (naive UTC; null when a row has none).</param>
    /// <param name="rowCap">The newest-first row cap of the grid's read, or null for a read with none. A read that returned that
    /// many rows names its oldest row, whatever the store covers (<see cref="ViewerEventDataStart.ReadHitCap"/>).</param>
    internal static async Task ShowEventDataStartAsync(
        TextBlock banner, Task<DateTime?> probe, string surface, DateTime startUtc, IEnumerable<DateTime?> shownEventTimesUtc, int? rowCap = null)
    {
        var coverageStart = await DataStartOrNullAsync(probe, surface);
        var shown = shownEventTimesUtc.ToList();
        UpdateTruncationBanner(banner,
            ViewerEventDataStart.Of(coverageStart, ViewerEventDataStart.EarliestOf(shown), ViewerEventDataStart.ReadHitCap(shown.Count, rowCap)), startUtc);
    }
}
