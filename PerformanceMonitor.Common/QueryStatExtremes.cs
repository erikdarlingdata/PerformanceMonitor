/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// #2235: the min/max CPU and elapsed columns on the top-queries/top-procedures reads are LIFETIME
/// extremes for the plan's time in cache — <c>sys.dm_exec_query_stats.max_worker_time</c> is a
/// high-water mark the engine never lowers, so the collector snapshots it as-is (a max cannot be
/// delta'd) and a windowed <c>MAX()</c> over snapshots still returns the lifetime value. Same
/// semantics as <c>max_dop</c>, whose tool description has always warned about this; these columns
/// didn't, and the field report showed why that matters: 8 of 20 rows on one box had
/// <c>max_cpu_ms</c> EXCEEDING the whole window's <c>total_cpu_ms</c>, inviting a reader to quote
/// an extreme from an arbitrary earlier period as if it happened this week.
///
/// <para>Windowing the max for real is not derivable from what's collected once rows group across
/// plans (a per-series "did the high-water mark advance in-window" test drowns in false positives
/// when MIN/MAX mix different plans' marks), so the honest fix is the one shipped here: label the
/// semantics, and surface the self-evident tell — max exceeding the windowed total PROVES the
/// extreme predates the window.</para>
/// </summary>
public static class QueryStatExtremes
{
    /// <summary>
    /// The conditional annotation for one result row, or null when nothing needs saying. Fires only
    /// on the provable case (an extreme larger than the whole window's total); a lifetime max that
    /// happens to sit inside the window's total is indistinguishable from a windowed one and gets no
    /// note. Unit-agnostic: compare like with like (both ms or both µs).
    /// </summary>
    public static string? LifetimeExtremeNote(
        double totalCpu, double maxCpu, double totalElapsed, double maxElapsed)
    {
        var cpuExceeds = maxCpu > totalCpu;
        var elapsedExceeds = maxElapsed > totalElapsed;
        if (!cpuExceeds && !elapsedExceeds)
        {
            return null;
        }

        var which = (cpuExceeds, elapsedExceeds) switch
        {
            (true, true) => "max_cpu_ms and max_elapsed_ms exceed",
            (true, false) => "max_cpu_ms exceeds",
            _ => "max_elapsed_ms exceeds",
        };

        return $"{which} this window's total — min/max (like max_dop) are lifetime extremes for the plan's time in cache, and this extreme predates the window; use total and avg for what happened in the window";
    }
}
