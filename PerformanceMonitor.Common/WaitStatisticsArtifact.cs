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
/// The one shared test for an isolated single-sample artifact in a <c>SQLServer:Wait Statistics</c> gauge
/// counter (#4476). Field evidence from a production fleet (3 days, 43 servers): 141 rows of
/// <c>Waits started per second</c> exceeded 10,000,000 (max 274,700,202) against a typical median of 76-504,
/// each one ISOLATED — one server, one sample, with normal neighbours on both sides (318 → 214,396,451 →
/// 1,021; 612 → 90,893,408 → 1,021; 40 → 32,713,827 → 878; 816 → 127,703,309 → 267). Every counter carries
/// <c>cntr_type</c> 65792 (a gauge) on every affected server — the collector stores exactly what the DMV
/// reported, so this is not a collector typing bug. The spike magnitude matches the counter's CUMULATIVE
/// count since the instance started (~250/s over ~10 days is ~2x10^8), so the DMV intermittently hands back
/// the lifetime cumulative count in a rate-family instance for one sample. The stored row is never rewritten;
/// this predicate exists for the READ path to recognise and set the point aside, visibly.
/// </summary>
public static class WaitStatisticsArtifact
{
    /// <summary>The <c>object_name</c> suffix every SQL Server perfmon row from the Wait Statistics object
    /// carries, with or without the <c>SQLServer:</c>/named-instance (<c>MSSQL$&lt;INSTANCE&gt;:</c>) prefix
    /// the DMV puts in front of every object name — the same "match by suffix" idiom the rest of the perfmon
    /// family uses to tolerate a named instance without spelling out every prefix shape.</summary>
    public const string ObjectNameSuffix = ":Wait Statistics";

    /// <summary>The floor below which a value is never judged an artifact, however large the ratio to its
    /// neighbours — the measured fleet's normal medians (76-504) are nowhere near this, and a real burst in
    /// these counters has stayed under it too.</summary>
    public const long MinValue = 1_000_000;

    /// <summary>How far above both neighbours a value must sit to be judged an artifact rather than a real
    /// burst. The measured spike ratios were 10^4-10^6 to their neighbours; a real burst in these instances
    /// has stayed under 10^3 of its neighbours, so 1,000 is the floor that separates the two populations
    /// without being tuned to the exact field numbers.</summary>
    public const long NeighborRatio = 1_000;

    /// <summary>
    /// True when <paramref name="value"/> is an isolated single-sample artifact in a Wait Statistics gauge
    /// series: both neighbours are present (a first or last point in a window is never judged — it has only
    /// one neighbour, or none), the row's object is Wait Statistics (matched by <see cref="ObjectNameSuffix"/>,
    /// tolerating the SQLServer:/named-instance prefix), the row's stored type is a gauge (<see
    /// cref="PerfmonCounterKind.Gauge"/> — a rate row's own cumulative count is not itself evidence of this
    /// artifact; the field evidence is specifically a gauge instance carrying a cumulative-shaped number),
    /// <paramref name="value"/> is at least <see cref="MinValue"/>, and it exceeds <see cref="NeighborRatio"/>
    /// times the LARGER of the two neighbours.
    /// </summary>
    public static bool IsIsolatedSingleSampleArtifact(
        string? objectName, PerfmonCounterKind kind, long? previousValue, long value, long? nextValue)
    {
        if (kind != PerfmonCounterKind.Gauge)
        {
            return false;
        }

        if (objectName is null || !objectName.EndsWith(ObjectNameSuffix, StringComparison.Ordinal))
        {
            return false;
        }

        if (previousValue is not long prev || nextValue is not long next)
        {
            return false;
        }

        if (value < MinValue)
        {
            return false;
        }

        var neighborMax = Math.Max(prev, next);
        return value > NeighborRatio * neighborMax;
    }
}
