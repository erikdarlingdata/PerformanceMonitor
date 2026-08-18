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
/// #2320: the denominator the top-CPU rankings never handed the caller. A ranking that explains 10%
/// of the box reads exactly like one that explains 90% unless the response says which — pre-#2290
/// the reads explained ~10% of one production instance and nothing said so, and the Datadog
/// disagreement on #2235 died the moment someone divided its worker_time sum by the box's available
/// CPU-seconds and got 137%. This computes that division for our own output, from series both
/// stores already collect (cpu_utilization sampling and server_properties.cpu_count).
/// </summary>
public static class CpuAttribution
{
    /// <summary>
    /// The minimum fraction of the WINDOW the utilization samples must span before the ratio is
    /// worth reporting. Span, deliberately not a count-per-minute expectation (the review catch):
    /// the collector's cadence is user-configurable per server, so any assumed frequency would
    /// permanently disqualify a legitimately slowed server. A series whose first and last samples
    /// bracket at least half the window supports an average at ANY cadence; below that the
    /// denominator extrapolates from a fragment, and the contract is to OMIT rather than fabricate
    /// (a missing ratio is honest; a wrong one sends someone chasing 3x the CPU that existed).
    /// </summary>
    public const double MinSpanCoverage = 0.5;

    /// <summary>
    /// And a floor on the sample count itself: a span can be bracketed by two lonely points. Three
    /// is the least that starts to look like a series.
    /// </summary>
    public const int MinSamples = 3;

    /// <summary>One computed window, ready for the wire.</summary>
    public readonly record struct CpuWindow(
        double MeasuredSqlCpuSeconds, double AttributedCpuSeconds, double AttributedRatio, string? Note);

    /// <summary>
    /// Computes the attribution window, or null when the denominator cannot be trusted: fewer than
    /// <see cref="MinSamples"/> samples, a sampled span under <see cref="MinSpanCoverage"/> of the
    /// window, an unknown core count, or a degenerate window. Null means "omit the field", never
    /// "zero". <paramref name="observedSpanHours"/> is last-sample minus first-sample, in hours —
    /// cadence-agnostic on purpose.
    /// </summary>
    public static CpuWindow? Compute(
        double attributedCpuMs,
        double? avgSqlCpuPercent,
        int samplesInWindow,
        double observedSpanHours,
        int? cpuCount,
        double windowHours)
    {
        if (avgSqlCpuPercent is not double avgPct
            || samplesInWindow < MinSamples
            || cpuCount is not int cores
            || cores <= 0
            || windowHours <= 0)
        {
            return null;
        }

        if (observedSpanHours < windowHours * MinSpanCoverage)
        {
            return null;
        }

        var measuredSeconds = avgPct / 100.0 * cores * windowHours * 3600.0;
        if (measuredSeconds <= 0)
        {
            /* A box whose SQL CPU averaged a hard zero across a covered window: the ratio would
               divide by zero, and "you used none and attributed some" is better said by omission
               plus the raw numbers the caller already has. */
            return null;
        }

        var attributedSeconds = attributedCpuMs / 1000.0;
        var ratio = attributedSeconds / measuredSeconds;

        var note = ratio switch
        {
            < 0.5 => FormattableString.Invariant(
                $"the returned rows explain {ratio * 100:F0}% of the SQL CPU this window actually burned — the remainder lives below the ranking cut, in plans evicted between snapshots, and in uncached work"),
            > 1.1 => "attributed CPU exceeds the measured total — the two series disagree at window edges (sampling skew); treat as ~100%",
            _ => null,
        };

        return new CpuWindow(measuredSeconds, attributedSeconds, ratio, note);
    }
}
