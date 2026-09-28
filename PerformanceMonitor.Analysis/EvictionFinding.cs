/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The decision behind <see cref="PgTargetFactKeys.ConfigStatStatementsEviction"/>: pg_stat_statements evicting
/// statements is a finding when the eviction passes were seen in at least <see cref="MinEvictingHours"/> of the
/// last <see cref="WindowHours"/> hourly windows and none of those windows held a statements-epoch change (a reset
/// restarts the counter, so a count taken across one says nothing about capacity).
///
/// <para>An hour with no <c>statements_dealloc=</c> measure is UNKNOWN, not zero: it counts toward neither side.
/// The rule counts only known evicting hours, so three known evicting hours are enough even when the other hours
/// are unknown, and a window with no known hour never fires.</para>
/// </summary>
public static class EvictionFinding
{
    /// <summary>The hourly windows examined, ending at the analysis window's end.</summary>
    public const int WindowHours = 6;

    /// <summary>Evicting hours needed to fire.</summary>
    public const int MinEvictingHours = 3;

    /// <summary>The fact metadata key carrying <c>pg_stat_statements.max</c>.</summary>
    public const string MaxEntriesKey = "max_entries";

    /// <summary>One hourly window: the counter was observed, eviction passes were &gt; 0, a statements-epoch change was recorded.</summary>
    public readonly record struct Hour(bool Known, bool Evicted, bool EpochChanged);

    /// <summary>Whether to fire, and the number of known evicting hours.</summary>
    public static (bool Fire, int EvictingHours) Decide(IReadOnlyList<Hour> hours)
    {
        var evicting = 0;
        foreach (var hour in hours)
        {
            if (hour.EpochChanged) return (false, 0);
            if (hour.Known && hour.Evicted) evicting++;
        }

        return (evicting >= MinEvictingHours, evicting);
    }
}
