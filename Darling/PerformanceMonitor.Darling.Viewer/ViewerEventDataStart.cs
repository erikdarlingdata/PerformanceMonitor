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
    /// event the grid shows (<paramref name="earliestShownUtc"/>) when that is earlier.
    ///
    /// <para>The probe answers one of three ways, and they are not the same. It found coverage
    /// (<paramref name="coverageStartUtc"/> set): the rule above. It found NONE in the window
    /// (<paramref name="coverageStartUtc"/> null: the range ends before the server's first collection, or the collector never
    /// ran in it), yet the grid can still list history, because an event carries its own time and a server's first collection
    /// stores the events that came before it: the notice names the earliest event shown, and the notice's own check
    /// (<see cref="PerformanceMonitor.Darling.Storage.RawWindowFloor.IsTruncated"/>) raises it only when that comes later than
    /// the range's start by more than the slack. Or it FAILED (<paramref name="probeFailed"/>): the store could not vouch
    /// for anything, so the notice names nothing, and the grid's own first row does not stand in for the coverage.</para>
    ///
    /// <para>A read that hit its row cap (<paramref name="readHitCap"/>, see <see cref="ReadHitCap"/>) returns the newest
    /// rows and stops, so the grid does not reach back further than its oldest row, whatever the store covers: the
    /// notice names that row. It comes from the rows themselves, so it needs no answer from the probe. A grid fed by
    /// several reads can hold fewer rows than its cap while one of its reads filled its own: that read's oldest row
    /// (<paramref name="cappedSourceOldestUtc"/>, see <see cref="CappedSourceStart"/>) bounds the grid the same way,
    /// and when both name one the later wins, since the grid is complete only from there. A read that stayed under its
    /// cap keeps the rule above.</para>
    /// </summary>
    public static DateTime? Of(
        DateTime? coverageStartUtc, DateTime? earliestShownUtc, bool readHitCap = false, bool probeFailed = false, DateTime? cappedSourceOldestUtc = null)
    {
        var capStart = readHitCap && earliestShownUtc is DateTime oldest ? oldest : (DateTime?)null;
        if (cappedSourceOldestUtc is DateTime sourceOldest && (capStart is not DateTime named || sourceOldest > named))
        {
            capStart = sourceOldest;
        }

        if (capStart is not null)
        {
            return capStart;
        }

        if (probeFailed)
        {
            return null;
        }

        return coverageStartUtc is not DateTime coverage
            ? earliestShownUtc
            : earliestShownUtc is DateTime shown && shown < coverage ? shown : coverage;
    }

    /// <summary>
    /// Where a grid fed by several newest-first reads, each with its own <paramref name="rowCap"/>, is complete from: the
    /// later of the oldest rows of the reads that filled their cap, or null when none did. A merge can leave fewer rows
    /// than the cap while one read stopped at its own (the rows another read already holds, or its own repeats within a
    /// minute, drop out), so the merged count never shows it; the older rows of that read are missing all the same. Rows with
    /// no event time never name a start.
    /// </summary>
    public static DateTime? CappedSourceStart(int rowCap, params IReadOnlyCollection<DateTime?>[] sourceEventTimesUtc)
    {
        ArgumentNullException.ThrowIfNull(sourceEventTimesUtc);

        DateTime? start = null;
        foreach (var source in sourceEventTimesUtc)
        {
            if (ReadHitCap(source.Count, rowCap) && EarliestOf(source) is DateTime oldest && (start is null || oldest > start))
            {
                start = oldest;
            }
        }

        return start;
    }

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
