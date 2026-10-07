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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #5449: the procedure_stats collector stores no row for a procedure in a cycle where it did no work. An older store kept
/// one (deltas 0), so its per-procedure history chart drew a 0 for the quiet minute. A newer store has a hole there, and a
/// line drawn across the hole shows the busy minutes either side as if the procedure ran all the time. The chart adds the
/// collector's runs that stored no row for this procedure back as 0 points, so both kinds of store draw the same.
/// The history GRID is not changed: it lists the minutes with work. Lite's twin is <c>ProcedureHistoryIdleRuns</c>.
/// </summary>
internal static class ViewerProcedureHistoryIdleRuns
{
    /// <summary>One SUCCESS run of the collector: its log time (<c>collection_log.collection_time</c>) and its logged duration.</summary>
    internal readonly record struct Run(DateTime Time, long DurationMs);

    /// <summary>
    /// The collector runs that left no row for the procedure, between its first and last stored row, as the time to plot each
    /// 0 at. Darling stamps a run's rows when the run starts and writes its log row after the run, so a run's rows sit just
    /// before its log time: run k owns the rows in (the previous run's log time, its own log time]. A run is a candidate when
    /// the previous run's log time is at or after the first row and its own is before the last (the procedure was in the cache
    /// for the whole of its span), and idle when no row of the procedure falls in what it owns. The 0 goes at the log time minus
    /// the logged duration, so it never lands before the run's real start. The duration is only the placement: it covers
    /// the run's query and store time, not its wall clock, so it never decides ownership. Runs before the first row or after
    /// the last are not returned: the procedure was not in the cache yet, or had left it, and a 0 there would be invented.
    /// Both lists ascending.
    /// </summary>
    internal static List<DateTime> IdleRunTimes(IReadOnlyList<DateTime> rowTimes, IReadOnlyList<Run> runs)
    {
        var idle = new List<DateTime>();
        if (rowTimes.Count == 0)
        {
            return idle;
        }

        var first = rowTimes[0];
        var last = rowTimes[rowTimes.Count - 1];
        var r = 0;
        for (var i = 1; i < runs.Count; i++)
        {
            var previous = runs[i - 1].Time;
            var logged = runs[i].Time;
            if (previous < first || logged >= last)
            {
                continue;
            }

            while (r < rowTimes.Count && rowTimes[r] <= previous)
            {
                r++;
            }

            if (r < rowTimes.Count && rowTimes[r] <= logged)
            {
                continue;
            }

            idle.Add(logged.AddMilliseconds(-runs[i].DurationMs));
        }

        return idle;
    }

    /// <summary>The history rows with an all-zero row added at each idle run, in time order, for the chart (not the grid).</summary>
    internal static List<ViewerProcedureStatsHistoryRow> ChartRows(
        IReadOnlyList<ViewerProcedureStatsHistoryRow> history, IReadOnlyList<DateTime> idleRunTimes)
    {
        if (idleRunTimes.Count == 0)
        {
            return history.ToList();
        }

        return history
            .Concat(idleRunTimes.Select(t => new ViewerProcedureStatsHistoryRow { CollectionTime = t }))
            .OrderBy(r => r.CollectionTime)
            .ToList();
    }
}
