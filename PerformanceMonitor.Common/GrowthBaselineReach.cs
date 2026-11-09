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
/// reads each baseline as the index snapshot nearest the window's cutoff within <see cref="Tolerance"/> either side, the
/// rule get_storage_growth applies to database baselines, so a table's "7 days ago" and a database's are one span and a
/// daily collector that missed its run at the mark resolves to the snapshot a day inside it rather than to nothing or to
/// one a day outside. A store with a gap wider than the tolerance on both sides of a cutoff has no baseline there.
/// The figure measured from a baseline then stands only when the span from it to the latest snapshot, judged to the
/// nearest whole day, is within the tolerance of the window: 6, 7 or 8 days for a 7-day figure, 29 to 31 for a 30-day one.
/// Snapshots are daily, so spans are whole days plus the seconds a run started early or late, and the rounding keeps that
/// drift from flipping the guard at the 6- and 8-day edges a missed run lands on. The guard refuses a collector whose latest
/// snapshot is itself days old, whose "7-day" baseline is then only a day or two before it, and the two windows' ranges are
/// disjoint, so the two figures can never be one measurement. Both products' rows judge their baselines here, so the rule
/// is one rule.
/// </summary>
public static class GrowthBaselineReach
{
    /// <summary>How far from its cutoff a baseline snapshot may sit: one day either side, the storage-growth reader's rule.</summary>
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
    /// True when a baseline snapshot exists and the span from it to the latest snapshot, to the nearest whole day, is within
    /// reach of the window: the figure measured from it may carry the window's name.
    /// </summary>
    public static bool Covers(DateTime? baselineSnapshot, DateTime latestSnapshot, int windowDays)
    {
        if (baselineSnapshot is not { } baseline) return false;
        var wholeDays = Math.Round((latestSnapshot - baseline).TotalDays, MidpointRounding.AwayFromZero);
        return wholeDays >= ShortestSpan(windowDays).TotalDays && wholeDays <= LongestSpan(windowDays).TotalDays;
    }

    /// <summary>
    /// True when the store's span (whole calendar days between its earliest and latest snapshots) is too short to hold any
    /// snapshot within the tolerance of the window's cutoff: no baseline exists because nothing is that old, not because of
    /// a gap.
    /// </summary>
    public static bool StoreYoungerThan(int daysOfData, int windowDays) => daysOfData < ShortestSpan(windowDays).TotalDays;

    /// <summary>The range a figure for <paramref name="windowDays"/> may span, for a sentence: "6 to 8 days".</summary>
    public static string DescribeRange(int windowDays) =>
        string.Create(CultureInfo.InvariantCulture, $"{ShortestSpan(windowDays).TotalDays:0.#} to {LongestSpan(windowDays).TotalDays:0.#} days");

    /// <summary>The tolerance, for a sentence: "a day".</summary>
    public static string ToleranceWords =>
        Tolerance == TimeSpan.FromDays(1) ? "a day" : string.Create(CultureInfo.InvariantCulture, $"{Tolerance.TotalDays:0.#} days");

    /// <summary>
    /// Why a store old enough to hold a baseline has none for the window: no snapshot lies within the tolerance of the cutoff
    /// on either side (a gap in the snapshots). "no snapshot lies within a day of 7 days ago".
    /// </summary>
    public static string NoneWithinToleranceClause(int windowDays) =>
        string.Create(CultureInfo.InvariantCulture, $"no snapshot lies within {ToleranceWords} of {windowDays} days ago");

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
            $"the snapshot nearest {windowDays} days ago is from {baseline:o}, {spanDays:0.#} days before the latest snapshot and outside the {DescribeRange(windowDays)} a {windowDays}-day figure may span");
    }
}
