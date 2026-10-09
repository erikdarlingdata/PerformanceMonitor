/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// Where the Blocking charts' blocked-process-report data starts once the server's <c>blocked process threshold (s)</c> is
/// taken into account (#5098). The report collector can run, and log runs, for days before the threshold is switched on; the
/// XE session then captures nothing, so zeros drawn for that stretch are not coverage. The threshold's history comes from the
/// daily <c>server_config</c> snapshots, so the answer has about a day of resolution: it names the first snapshot that read on.
/// One pure rule shared by Lite and the desktop viewer so the two cannot drift.
/// </summary>
public static class BlockingThresholdCoverage
{
    /// <summary>
    /// Combines the collector's coverage with the threshold's history.
    /// </summary>
    /// <param name="collectorCoverage">The report collector's coverage start (its first logged run in the window, or the window's start when a run precedes it), or null when it has none.</param>
    /// <param name="earliestReport">The earliest report in the window, read with the chart's own predicate; null if none.</param>
    /// <param name="onAtWindowStart">True when the newest snapshot at or before the window's start read above zero.</param>
    /// <param name="firstOnInWindow">The first snapshot in the window (after its start, up to its end) that read above zero, or null.</param>
    /// <param name="sawZeroSnapshot">True when a snapshot read zero before the threshold was seen on: any zero at or before the window's start, or any zero in the window dated before <paramref name="firstOnInWindow"/> (every zero in the window when there is none).</param>
    /// <returns>
    /// With the threshold on at the start, or with no threshold snapshots to go by, the earlier of the coverage and the report
    /// (today's answer), and so does a window whose snapshots never read zero. Otherwise the earlier of the report and the later of
    /// the coverage and the threshold's start, where the threshold's start is the first snapshot that read on, or the earliest report
    /// when every snapshot read zero. Only the start can move: a zero must be seen, before the first nonzero, for the answer to change.
    /// </returns>
    public static DateTime? Combine(
        DateTime? collectorCoverage, DateTime? earliestReport, bool onAtWindowStart, DateTime? firstOnInWindow, bool sawZeroSnapshot)
    {
        DateTime? thresholdStart = null;
        if (!onAtWindowStart)
        {
            /* The start moves only when a zero was seen. A nonzero in-window snapshot with no zero before it says nothing about when the
               threshold went on: the earlier snapshots may simply have been purged, or never taken. */
            thresholdStart = sawZeroSnapshot ? (firstOnInWindow ?? earliestReport) : null;
        }

        var covered = Later(collectorCoverage, thresholdStart);
        return Earlier(earliestReport, covered);
    }

    private static DateTime? Later(DateTime? a, DateTime? b) =>
        a is null ? b : b is null ? a : a > b ? a : b;

    private static DateTime? Earlier(DateTime? a, DateTime? b) =>
        a is null ? b : b is null ? a : a < b ? a : b;
}
