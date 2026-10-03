/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Where an EVENT surface says its data starts (#4966). Blocked process reports, deadlocks and the other grids that
/// list events filter on the event's own time, and a server's first collection stores the server's event history, so
/// a row can carry an event time from before the server was added. The shared probe
/// (<see cref="PerformanceMonitor.Darling.Storage.DataWindowFloor"/>) answers where the collector's COVERAGE starts:
/// the later of the server's first collection and the table's retention edge. A notice that named only that would name
/// a time later than the earliest row the grid shows, and read as a cut where history reaches further back.
///
/// <para>So the notice names the earlier of the coverage start and the earliest event the grid shows. A grid whose
/// history reaches the range's start then names a time at or before it, and the banner stays hidden; a quiet start
/// (the store covered the range, but the first event came late) still names the coverage, which is at or before the
/// range's start, and stays hidden too.</para>
/// </summary>
public static class ViewerEventDataStart
{
    /// <summary>
    /// The instant an event surface's "Showing since" notice names: <paramref name="coverageStartUtc"/>, or the earliest
    /// event the grid shows (<paramref name="earliestShownUtc"/>) when that is earlier. Null when the probe has no
    /// answer (<paramref name="coverageStartUtc"/> null: nothing in the range, or the probe failed), so a surface never
    /// names a start the store could not vouch for.
    ///
    /// <para>A read that hit its row cap (<paramref name="readHitCap"/>, see <see cref="ReadHitCap"/>) returns the newest
    /// rows and stops, so the grid does not reach back further than its oldest row, whatever the store covers: the
    /// notice names that row. It comes from the rows themselves, so it needs no answer from the probe. A read that
    /// stayed under its cap keeps the rule above.</para>
    /// </summary>
    public static DateTime? Of(DateTime? coverageStartUtc, DateTime? earliestShownUtc, bool readHitCap = false) =>
        readHitCap && earliestShownUtc is DateTime oldest
            ? oldest
            : coverageStartUtc is not DateTime coverage
                ? null
                : earliestShownUtc is DateTime shown && shown < coverage ? shown : coverage;

    /// <summary>
    /// True when a read that keeps its newest <paramref name="rowCap"/> rows returned that many: it may have left older
    /// rows out, so the grid's oldest row is where its data starts. A read with no cap (<paramref name="rowCap"/> null)
    /// never hits one.
    /// </summary>
    public static bool ReadHitCap(int shownRowCount, int? rowCap) => rowCap is int cap && shownRowCount >= cap;

    /// <summary>The earliest of <paramref name="eventTimesUtc"/>, ignoring the rows that carry no event time; null when none does.</summary>
    public static DateTime? EarliestOf(IEnumerable<DateTime?> eventTimesUtc)
    {
        ArgumentNullException.ThrowIfNull(eventTimesUtc);

        DateTime? earliest = null;
        foreach (var time in eventTimesUtc)
        {
            if (time is DateTime t && (earliest is null || t < earliest))
            {
                earliest = t;
            }
        }

        return earliest;
    }
}
