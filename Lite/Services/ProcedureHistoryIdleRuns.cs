/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #5449: the procedure_stats collector stores no row for a procedure in a cycle where it did no work. An older store kept
/// one (deltas 0), so its per-procedure history chart drew a 0 for the quiet minute. A newer store has a hole there, and a
/// line drawn across the hole shows the busy minutes either side as if the procedure ran all the time. The chart adds the
/// collector's runs that stored no row for this procedure back as 0 points, so both kinds of store draw the same.
/// The history GRID is not changed: it lists the minutes with work.
/// </summary>
internal static class ProcedureHistoryIdleRuns
{
    /// <summary>
    /// The collector runs that left no row for the procedure, between its first and last stored row. A run owns the rows in
    /// [its time, the next run's time), so a run is idle when none falls there (this holds whether the log stamps a run at the
    /// same instant as its rows or a moment before them). Runs before the first row or after the last are not returned: the
    /// procedure was not in the cache yet, or had left it, and a 0 there would be invented. Both lists ascending.
    /// </summary>
    internal static List<DateTime> IdleRunTimes(IReadOnlyList<DateTime> rowTimes, IReadOnlyList<DateTime> runTimes)
    {
        var idle = new List<DateTime>();
        if (rowTimes.Count == 0)
        {
            return idle;
        }

        var first = rowTimes[0];
        var last = rowTimes[rowTimes.Count - 1];
        var r = 0;
        for (var i = 0; i < runTimes.Count; i++)
        {
            var t = runTimes[i];
            if (t <= first || t >= last)
            {
                continue;
            }

            var next = i + 1 < runTimes.Count ? runTimes[i + 1] : (DateTime?)null;
            while (r < rowTimes.Count && rowTimes[r] < t)
            {
                r++;
            }

            if (r < rowTimes.Count && (next is null || rowTimes[r] < next.Value))
            {
                continue;
            }

            idle.Add(t);
        }

        return idle;
    }

    /// <summary>The history rows with an all-zero row added at each idle run, in time order, for the chart (not the grid).</summary>
    internal static List<ProcedureStatsHistoryRow> ChartRows(IReadOnlyList<ProcedureStatsHistoryRow> history, IReadOnlyList<DateTime> idleRunTimes)
    {
        if (idleRunTimes.Count == 0)
        {
            return history.ToList();
        }

        return history
            .Concat(idleRunTimes.Select(t => new ProcedureStatsHistoryRow { CollectionTime = t }))
            .OrderBy(r => r.CollectionTime)
            .ToList();
    }
}
