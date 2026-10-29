/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// A wall-clock ceiling for one whole per-database pass, and the rotation that lets the next pass pick up
/// where this one stopped (#6875).
///
/// <para><see cref="ICollectorSchemaInfo.PerItemWallClockBudget"/> bounds ONE database. On a server with
/// hundreds of databases that still leaves the sum unbounded, so a collector that declares
/// <see cref="ICollectorSchemaInfo.PassWallClockBudget"/> also stops starting new databases once the pass
/// has spent it. Databases the pass did not reach are not read and write nothing, so a collector that
/// stores a "scanned, nothing found" marker per database stores none for them. The pass remembers the last
/// database it started (<see cref="ResumeOrder"/> consumes it), so the next pass starts at the database after
/// that one and wraps, instead of re-reading the same first databases every time and never reaching the
/// last ones.</para>
///
/// <para>The check runs BEFORE a database starts, never inside one: a database that has started finishes or
/// fails on its own per-item budget, and a flush that has started is never abandoned. At least one database
/// always runs, so a budget smaller than one database's read still moves the rotation forward.</para>
/// </summary>
public sealed class PassBudget
{
    private readonly TimeSpan? _budget;
    private readonly TimeProvider _clock;
    private readonly long _start;

    /// <param name="budget">Null (a collector that declares none) never spends.</param>
    /// <param name="clock">The clock the budget is measured on. Tests pass a fake that advances by hand.</param>
    public PassBudget(TimeSpan? budget, TimeProvider? clock = null)
    {
        _budget = budget;
        _clock = clock ?? TimeProvider.System;
        _start = _clock.GetTimestamp();
    }

    /// <summary>True once the pass has used its whole budget.</summary>
    public bool Spent => _budget is { } budget && _clock.GetElapsedTime(_start) >= budget;

    /// <summary>The ceiling in whole seconds, for the note.</summary>
    public int BudgetSeconds => (int)(_budget?.TotalSeconds ?? 0);

    /// <summary>
    /// The databases in the order this pass reads them: the pass's own sorted order, rotated so it starts at
    /// the first name after <paramref name="resumeAfter"/> (the last database the previous pass started) and
    /// wraps to the names before it. Null <paramref name="resumeAfter"/>, or one past every name, starts at
    /// the first. Compared by ordinal value, not by position, so a database dropped or added since the last pass
    /// moves the start by at most one place and never loses the rotation.
    /// </summary>
    public static List<string> ResumeOrder(IReadOnlyList<string> items, string? resumeAfter)
    {
        ArgumentNullException.ThrowIfNull(items);

        var sorted = new List<string>(items);
        sorted.Sort(StringComparer.Ordinal);
        if (resumeAfter is null)
        {
            return sorted;
        }

        var start = sorted.FindIndex(name => string.CompareOrdinal(name, resumeAfter) > 0);
        if (start <= 0)
        {
            return sorted;
        }

        var rotated = new List<string>(sorted.Count);
        rotated.AddRange(sorted.GetRange(start, sorted.Count - start));
        rotated.AddRange(sorted.GetRange(0, start));
        return rotated;
    }

    /// <summary>The collection_log note for a pass that stopped on its budget.</summary>
    public static string StopNote(int budgetSeconds, int reached, int total) =>
        string.Format(
            CultureInfo.InvariantCulture,
            "pass budget ({0} s) spent after {1} of {2} database(s); the rest were not read and have no marker, the next pass resumes after the last one read",
            budgetSeconds, reached, total);
}
