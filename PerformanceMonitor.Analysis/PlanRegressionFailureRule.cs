/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The failure half of the PLAN_REGRESSION fact (#6267), shared by Lite's and Darling's collectors: a query whose Aborted
/// (timeout) or Exception (error) runs rose. The cost read counts Regular runs only, so a plan that regresses into timeouts
/// or errors was invisible to it, and its successful runs can even look cheaper because the slow runs that hit the timeout
/// drop out (Erik, 2026-10-21: "not counting failures and not showing timeout metrics misses very important query
/// characteristics and i would call it downright misleading").
///
/// <para>The rule is #6260's, for the regressions read. A group rises when its per-hour rate in the recent window is at
/// least <see cref="RateMultiple"/> times the baseline's (or any at all where the baseline had none) with at least
/// <see cref="MinRecentRuns"/> runs in the recent window. The recent window is the last <see cref="RecentHours"/> hours of
/// the pass, the baseline the rest of the 14-day window. Darling's <c>QueryStoreRegressionBaseline</c> takes its three gate
/// constants from here, so the regressions read and the fact cannot drift.</para>
/// </summary>
public static class PlanRegressionFailureRule
{
    /// <summary>A rise in timeouts or errors needs at least this many such runs in the recent window, so one stray timeout on a
    /// quiet query does not flag it.</summary>
    public const int MinRecentRuns = 3;

    /// <summary>A rise qualifies when the recent per-hour rate is at least this many times the baseline's. A nonzero recent rate
    /// where the baseline had none qualifies whatever the multiple.</summary>
    public const int RateMultiple = 2;

    /// <summary>A rise is HIGH when its rate is at least this many times the baseline's (or the baseline had none), MEDIUM
    /// otherwise.</summary>
    public const int HighMultiple = 5;

    /// <summary>The recent window, in hours.</summary>
    public const int RecentHours = 24;

    /// <summary>How many failing queries one read returns.</summary>
    public const int ReadLimit = 100;

    /// <summary>The fact's <see cref="Fact.Value"/> when only failures rose and at least one group is HIGH. The cost scale
    /// (concerning 2, critical 10) scores it 0.75, so a failure-only fact is never CRITICAL, as #6260's rows are not.</summary>
    public const double ValueHigh = 6.0;

    /// <summary>The fact's <see cref="Fact.Value"/> when only failures rose and none is HIGH: scores 0.5625.</summary>
    public const double ValueMedium = 3.0;

    /// <summary>One query whose timeouts or errors rose.</summary>
    public sealed record Failure(
        string DatabaseName,
        long QueryId,
        long RecentTimeouts,
        long BaselineTimeouts,
        long RecentErrors,
        long BaselineErrors,
        bool TimeoutsRise,
        bool ErrorsRise,
        bool TimeoutsHigh,
        bool ErrorsHigh);

    /// <summary>
    /// Writes the failure figures onto the PLAN_REGRESSION fact's metadata and returns the value the failures alone would
    /// score (0 when none): <see cref="ValueHigh"/> when any rise is HIGH, else <see cref="ValueMedium"/>. The fact keeps the
    /// larger of that and the cost factor.
    /// </summary>
    public static double AddMetadata(Dictionary<string, double> metadata, IReadOnlyList<Failure> failures)
    {
        if (failures.Count == 0)
        {
            return 0.0;
        }

        var timeouts = failures.Where(f => f.TimeoutsRise).OrderByDescending(f => f.RecentTimeouts).ToList();
        var errors = failures.Where(f => f.ErrorsRise).OrderByDescending(f => f.RecentErrors).ToList();
        metadata["failure_offender_count"] = failures.Count;
        metadata["timeout_offender_count"] = timeouts.Count;
        metadata["error_offender_count"] = errors.Count;
        metadata["failure_recent_hours"] = RecentHours;
        if (timeouts.Count > 0)
        {
            metadata["worst_timeout_query_id"] = timeouts[0].QueryId;
            metadata["worst_timeout_recent_runs"] = timeouts[0].RecentTimeouts;
            metadata["worst_timeout_baseline_runs"] = timeouts[0].BaselineTimeouts;
        }

        if (errors.Count > 0)
        {
            metadata["worst_error_query_id"] = errors[0].QueryId;
            metadata["worst_error_recent_runs"] = errors[0].RecentErrors;
            metadata["worst_error_baseline_runs"] = errors[0].BaselineErrors;
        }

        return failures.Any(f => f.TimeoutsHigh || f.ErrorsHigh) ? ValueHigh : ValueMedium;
    }
}
