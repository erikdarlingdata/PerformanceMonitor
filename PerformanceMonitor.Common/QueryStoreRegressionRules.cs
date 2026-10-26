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
/// <see cref="CostGatePercent"/>% above its baseline, and for TIMEOUTS (Aborted runs) or ERRORS (Exception runs) when that
/// group's per-hour rate in the recent window is at least <see cref="FailureRateMultiple"/> times the baseline's, or any at all
/// where the baseline had none, with at least <see cref="FailureMinRecentRuns"/> such runs in the recent window.
/// </summary>
public static class QueryStoreRegressionRules
{
    /// <summary>A query qualifies for <c>cost</c> when its Regular CPU per execution is more than this percent above its baseline.</summary>
    public const int CostGatePercent = 25;

    /// <summary>The closing sentence of the regressions read's <c>truncation_note</c> when the window starts at <c>baseline_start</c>
    /// and the store holds only part of the baseline. One definition for Darling's and Lite's reads, and the text the web page's
    /// <c>plain-text.js</c> rewrites, so a reword here breaks the web test instead of leaving a field name on the page (#6649).</summary>
    public const string BaselineWindowTail =
        "Here the window starts at baseline_start, so the baseline holds only the part from effective_start to baseline_end.";

    /// <summary>The closing sentence of <c>truncation_note</c> when the store starts at or after <c>baseline_end</c> (#6750): the
    /// baseline holds nothing, so <see cref="BaselineWindowTail"/> ("holds only the part from effective_start to baseline_end")
    /// would name an empty or backwards span. Chosen by <see cref="TailFor"/>, and rewritten by the web page's
    /// <c>plain-text.js</c> as <see cref="BaselineWindowTail"/> is.</summary>
    public const string EmptyBaselineTail =
        "Here effective_start is at or after baseline_end, so the baseline is empty: no query can be flagged for cost, and only timeouts and errors were tested.";

    /// <summary>The sentence that follows <see cref="EmptyBaselineTail"/> when the store starts after <c>recent_window_start</c>
    /// too (#6750): the recent window is cut as well. True for both products.</summary>
    public const string RecentCutTail =
        "effective_start is also after recent_window_start, so the recent window holds only the part from effective_start.";

    /// <summary>The sentence Darling adds after <see cref="RecentCutTail"/> (#6750): its read divides each recent per-hour rate by
    /// the hours the recent window holds. Lite's read still divides by the window it was asked for, so Lite does not say this.</summary>
    public const string RecentHoursHeldTail =
        "Its per-hour rates divide by the hours it holds.";

    /// <summary>
    /// The closing sentence(s) of the regressions read's <c>truncation_note</c> for a store whose data starts at
    /// <paramref name="effectiveStart"/> (#6750), by one comparison for Darling and Lite. <paramref name="baselineEnd"/> is the
    /// recent window's start, where the baseline ends. Before it, the baseline holds part of its span
    /// (<see cref="BaselineWindowTail"/>). At or after it the baseline is empty (<see cref="EmptyBaselineTail"/>), and strictly
    /// after it the recent window is cut too (<see cref="RecentCutTail"/>, then <see cref="RecentHoursHeldTail"/> when
    /// <paramref name="ratesUseHoursHeld"/>).
    /// </summary>
    public static string TailFor(DateTime effectiveStart, DateTime baselineEnd, bool ratesUseHoursHeld)
    {
        if (effectiveStart < baselineEnd)
        {
            return BaselineWindowTail;
        }

        if (effectiveStart == baselineEnd)
        {
            return EmptyBaselineTail;
        }

        return EmptyBaselineTail + " " + RecentCutTail + (ratesUseHoursHeld ? " " + RecentHoursHeldTail : "");
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
