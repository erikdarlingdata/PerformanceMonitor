/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// How far a nominal growth window's baseline snapshot may sit from the span its name promises. get_table_index_sizes
/// reads each baseline as the newest index snapshot at or before the window's cutoff, and the snapshots are daily at
/// best: a collector that stalled, or a server that was out of scope for a while, leaves a gap, and the newest snapshot
/// before a cutoff can then be weeks older than the window. A 7-day figure measured from a 30-day-old snapshot is the
/// 30-day figure under another name, and the two read identical on every steadily growing table. A figure therefore
/// stands only when the span it covers, from its baseline snapshot to the latest snapshot, is within <see cref="Tolerance"/>
/// of the window: a 7-day figure spans 6 to 8 days, a 30-day figure 29 to 31. The tolerance is the one get_storage_growth
/// already judges its database baselines by, so a table's "7 days ago" and a database's "7 days ago" are the same span,
/// the two ranges are disjoint, so the two figures can never be one measurement, and the span's lower bound keeps a stalled
/// collector, whose latest snapshot is itself days old, from labelling two days of growth as seven. Both products' rows
/// judge their baselines here, so the rule is one rule.
/// </summary>
public static class GrowthBaselineReach
{
    /// <summary>How far from its window a baseline snapshot may sit: one day either side, the storage-growth reader's rule.</summary>
    public static readonly TimeSpan Tolerance = TimeSpan.FromDays(1);

    /// <summary>The shorter nominal window, in days.</summary>
    public const int ShortWindowDays = 7;

    /// <summary>The longer nominal window, in days.</summary>
    public const int LongWindowDays = 30;

    /// <summary>The shortest span a figure for <paramref name="windowDays"/> may cover.</summary>
    public static TimeSpan ShortestSpan(int windowDays) => TimeSpan.FromDays(windowDays) - Tolerance;

    /// <summary>The longest span a figure for <paramref name="windowDays"/> may cover.</summary>
    public static TimeSpan LongestSpan(int windowDays) => TimeSpan.FromDays(windowDays) + Tolerance;

    /// <summary>
    /// True when a baseline snapshot exists and the span from it to the latest snapshot is within reach of the window:
    /// the figure measured from it may carry the window's name.
    /// </summary>
    public static bool Covers(DateTime? baselineSnapshot, DateTime latestSnapshot, int windowDays)
    {
        if (baselineSnapshot is not { } baseline) return false;
        var span = latestSnapshot - baseline;
        return span >= ShortestSpan(windowDays) && span <= LongestSpan(windowDays);
    }

    /// <summary>The range a figure for <paramref name="windowDays"/> may span, for a sentence: "6 to 8 days".</summary>
    public static string DescribeRange(int windowDays) =>
        string.Create(CultureInfo.InvariantCulture, $"{ShortestSpan(windowDays).TotalDays:0.#} to {LongestSpan(windowDays).TotalDays:0.#} days");

    /// <summary>
    /// Why a baseline snapshot that exists is out of reach of its window, for the history note: the snapshot, how far it
    /// sits from the latest snapshot, and the range the figure may span. Null when the baseline covers the window, or
    /// when there is no snapshot at all (that is a different sentence).
    /// </summary>
    public static string? OutOfReachClause(DateTime? baselineSnapshot, DateTime latestSnapshot, int windowDays)
    {
        if (baselineSnapshot is not { } baseline || Covers(baseline, latestSnapshot, windowDays)) return null;
        var spanDays = (latestSnapshot - baseline).TotalDays;
        return string.Create(CultureInfo.InvariantCulture,
            $"the newest snapshot at or before {windowDays} days ago is from {baseline:o}, {spanDays:0.#} days before the latest snapshot and outside the {DescribeRange(windowDays)} a {windowDays}-day figure may span");
    }
}
