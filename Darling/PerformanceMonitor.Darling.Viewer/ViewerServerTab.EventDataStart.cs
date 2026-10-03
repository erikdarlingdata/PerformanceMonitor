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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>
    /// Raises or hides the "Showing since" banner of an EVENT surface (#4966): a grid that filters on the event's own
    /// time (blocked process reports, deadlocks), where a server's first collection stores the server's history, so a
    /// row can carry an event time from before the coverage the probe found. The banner names the earlier of the
    /// probe's answer and the earliest event the grid shows (<see cref="ViewerEventDataStart.Of"/>), never a time later
    /// than a row on screen, including when the probe found no coverage in the window (a range that ends before the server's
    /// first collection lists the history that collection stored). The probe is awaited through
    /// <see cref="DataStartAnswerAsync"/>, so a probe that throws costs this banner and nothing after it.
    /// </summary>
    /// <param name="banner">The surface's banner.</param>
    /// <param name="probe">The surface's data-start probe, started beside its read.</param>
    /// <param name="surface">The surface's name, for the log line a failed probe writes.</param>
    /// <param name="startUtc">The start of the window the grid just drew.</param>
    /// <param name="shownEventTimesUtc">The event time of each row the grid shows (naive UTC; null when a row has none).</param>
    /// <param name="rowCap">The newest-first row cap of the grid's read, or null for a read with none. A read that returned that
    /// many rows names its oldest row, whatever the store covers (<see cref="ViewerEventDataStart.ReadHitCap"/>).</param>
    /// <param name="cappedSourceOldestUtc">For a grid merged from several reads, the oldest row of the one that filled its own
    /// cap (<see cref="ViewerEventDataStart.CappedSourceStart"/>), or null when none did: the merge can hold fewer rows than
    /// <paramref name="rowCap"/> while that read left older rows out. It bounds the grid like the cap above, with no slack.</param>
    internal static async Task ShowEventDataStartAsync(
        TextBlock banner, Task<DateTime?> probe, string surface, DateTime startUtc, IEnumerable<DateTime?> shownEventTimesUtc, int? rowCap = null,
        DateTime? cappedSourceOldestUtc = null)
    {
        /* A probe that threw is hidden (Failed); one that found no coverage in the window (Start null) is not, because the grid can
           still list history from before the server's first collection and the notice then names its earliest event. */
        var (probeFailed, coverageStart) = await DataStartAnswerAsync(probe, surface);
        var shown = shownEventTimesUtc.ToList();
        var earliestShown = ViewerEventDataStart.EarliestOf(shown);
        var readHitCap = ViewerEventDataStart.ReadHitCap(shown.Count, rowCap);

        /* The slack (RawWindowFloor.IsTruncated, 90 minutes) belongs to the coverage probe alone: it absorbs a first collection that
           lands a little after the window starts. A read that filled its cap dropped rows for certain, so its verdict has no slack:
           the banner shows whenever the oldest row shown is later than the window's start, which on a range of an hour or less is
           the only way it can show at all. The compared start moves back by the slack so the shared check lands on that bare
           comparison, and the banner still names the oldest row. */
        var capped = (readHitCap && earliestShown is not null) || cappedSourceOldestUtc is not null;
        var comparedStartUtc = capped ? startUtc - DurationTrendRouting.TruncationSlack : startUtc;
        UpdateTruncationBanner(
            banner, ViewerEventDataStart.Of(coverageStart, earliestShown, readHitCap, probeFailed, cappedSourceOldestUtc), comparedStartUtc);
    }
}
