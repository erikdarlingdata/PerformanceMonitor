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
/// The thresholds of the Query Store regressions read (get_query_store_regressions), shared by Darling's read
/// (<c>QueryStoreRegressionBaseline</c>) and Lite's (<c>LocalDataService.GetQueryStoreRegressionsAsync</c>) so the two products
/// flag the same rows (#5506, #6268). A query is listed for COST when its Regular CPU per execution is more than
/// <see cref="CostGatePercent"/>% above its baseline and the rise in its CPU per execution, times its recent runs, adds at least
/// <see cref="CostMinAddedCpuMs"/> ms of CPU over the recent window (#6781, <see cref="CostTestSql"/>), and for TIMEOUTS (Aborted runs) or ERRORS (Exception runs) when that
/// group's per-hour rate in the recent window is at least <see cref="FailureRateMultiple"/> times the baseline's, or any at all
/// where the baseline had none, with at least <see cref="FailureMinRecentRuns"/> such runs in the recent window.
/// </summary>
public static class QueryStoreRegressionRules
{
    /// <summary>A query qualifies for <c>cost</c> when its Regular CPU per execution is more than this percent above its baseline.</summary>
    public const int CostGatePercent = 25;

    /// <summary>
    /// #6781: a query qualifies for <c>cost</c> only when it also added at least this many milliseconds of CPU over the recent
    /// window: its recent Regular runs times the rise in its CPU per run. CPU, not duration, because CPU is the test's own metric:
    /// a query whose CPU per run rose while its elapsed time fell (a plan that went parallel) is a real CPU regression and adds no
    /// duration. A fixed amount over the window, not a rate per hour. Chosen from one production store with 42 servers (three
    /// servers, 24 and 168 hours): about 80 to 96% of the rows the CPU test alone passed added under 1 s of CPU, most of them a
    /// few runs of a cheap query, while today's first page is unchanged wherever at least a page of rows clears it. A once-a-day
    /// job whose CPU per run rose by minutes adds minutes in one run and still passes, which a minimum run count would not allow.
    /// A rate per hour drops far more at 168 hours than at 24 for the same query, so a job that runs once a week would fall out
    /// of the long window first, the window where it is most likely to be seen. Spelled "1 s" in the tools' text and in the
    /// Viewer's, by hand, as <see cref="CostGatePercent"/> is.
    /// </summary>
    public const int CostMinAddedCpuMs = 1_000;

    /// <summary>
    /// #6781: the cost test, as one SQL boolean over a row with <c>base_cpu_ms</c>, <c>recent_cpu_ms</c> (ms per run) and
    /// <c>recent_execs</c> (a run count) under <paramref name="alias"/>. One text for
    /// Darling's read (the MCP tool and the Viewer, every tier) and Lite's, PostgreSQL and DuckDB alike. Regular CPU per run more
    /// than <see cref="CostGatePercent"/>% above the baseline AND at least <see cref="CostMinAddedCpuMs"/> ms of added CPU over
    /// the window. A baseline with no CPU (no ratio) is false, as the CPU test alone was.
    /// </summary>
    public static string CostTestSql(string alias) =>
        $"COALESCE(({alias}.recent_cpu_ms - {alias}.base_cpu_ms) * 100.0 / NULLIF({alias}.base_cpu_ms, 0) > {CostGatePercent}"
        + $" AND ({alias}.recent_cpu_ms - {alias}.base_cpu_ms) * {alias}.recent_execs >= {CostMinAddedCpuMs}, false)";

    /// <summary>The closing sentence of the regressions read's <c>truncation_note</c> when the window starts at <c>baseline_start</c>
    /// and the store holds only part of the baseline. One definition for Darling's and Lite's reads, and the text the web page's
    /// <c>plain-text.js</c> rewrites, so a reword here breaks the web test instead of leaving a field name on the page (#6649).</summary>
    public const string BaselineWindowTail =
        "Here the window starts at baseline_start, so the baseline holds only the part from effective_start to baseline_end.";

    /// <summary>The closing sentence of <c>truncation_note</c> when the store starts at or after <c>baseline_end</c> (#6750): the
    /// baseline holds no hours, so <see cref="BaselineWindowTail"/> ("holds only the part from effective_start to baseline_end")
    /// would name an empty or backwards span. The sentence says what can still be flagged: timeouts and errors on recent runs alone, and
    /// cost only from an interval that ran before <c>baseline_end</c> and was collected after it. Darling's baseline keeps rows on their last
    /// run time, so such an interval can enter it, and a per-execution average needs no hours. Lite's baseline rows are filtered on
    /// collection time, so no such interval enters there and the sentence holds for both. Chosen by <see cref="TailFor"/>, and rewritten by the web page's
    /// <c>plain-text.js</c> as <see cref="BaselineWindowTail"/> is.</summary>
    public const string EmptyBaselineTail =
        "Here effective_start is at or after baseline_end, so the baseline holds no hours: timeouts and errors are flagged on recent runs alone, and a cost flag can come only from an interval that ran before baseline_end and was collected after it.";

    /// <summary>The sentence that follows <see cref="EmptyBaselineTail"/> when the store starts after <c>baseline_end</c>, the
    /// recent window's start (#6750): the recent window is cut as well. It names only keys both
    /// payloads carry (Darling's also names that instant <c>recent_window_start</c>, Lite's does not).</summary>
    public const string RecentCutTail =
        "effective_start is also after baseline_end, so the recent window holds only the part from effective_start.";

    /// <summary>The sentence that follows <see cref="RecentCutTail"/> (#6750): both products divide each recent per-hour rate by the
    /// hours the recent window holds, from the instant the store's data starts, on every read path.</summary>
    public const string RecentHoursHeldTail =
        "Its per-hour rates divide by the hours it holds.";

    /// <summary>
    /// The closing sentence(s) of the regressions read's <c>truncation_note</c> for a store whose data starts at
    /// <paramref name="effectiveStart"/> (#6750), by one comparison for Darling and Lite. <paramref name="baselineEnd"/> is the
    /// recent window's start, where the baseline ends. Before it, the baseline holds part of its span
    /// (<see cref="BaselineWindowTail"/>). At or after it the baseline is empty (<see cref="EmptyBaselineTail"/>), and strictly
    /// after it the recent window is cut too (<see cref="RecentCutTail"/>, then <see cref="RecentHoursHeldTail"/>).
    /// </summary>
    public static string TailFor(DateTime effectiveStart, DateTime baselineEnd)
    {
        if (effectiveStart < baselineEnd)
        {
            return BaselineWindowTail;
        }

        if (effectiveStart == baselineEnd)
        {
            return EmptyBaselineTail;
        }

        return EmptyBaselineTail + " " + RecentCutTail + " " + RecentHoursHeldTail;
    }

    /// <summary>A rise in timeouts or errors qualifies when the recent per-hour rate is at least this many times the baseline's.
    /// A nonzero recent rate where the baseline had none qualifies whatever the multiple.</summary>
    public const int FailureRateMultiple = 2;

    /// <summary>A rise in timeouts or errors needs at least this many such runs in the recent window, so one stray timeout on a
    /// quiet query does not flag it.</summary>
    public const int FailureMinRecentRuns = 3;

    /// <summary>A timeouts-only or errors-only row is HIGH when its rate is at least this many times the baseline's (or the baseline
    /// had none), MEDIUM otherwise. Cost rows keep the duration bands; a failure row never gets CRITICAL.</summary>
    public const int FailureHighMultiple = 5;

    /// <summary>Rows with no cost rise (timeouts or errors only) may take at most this share of the row cap, as
    /// <c>cap / FailureShareDivisor</c>, so a flood of error rows cannot push every cost row past the cap.</summary>
    public const int FailureShareDivisor = 3;
}
