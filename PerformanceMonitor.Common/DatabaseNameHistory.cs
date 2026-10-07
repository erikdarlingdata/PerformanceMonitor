/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// Turns a severe error's <c>database_id</c> into the database name that id carried AT THE ERROR'S TIME (#5373).
/// Shared by Lite, the Darling service's MCP tool and the Darling viewer, so all three say the same thing.
///
/// <para>SQL Server reuses the id of a dropped database. If database A had id 7, was dropped, and database B
/// later got id 7, the server's LATEST id-to-name map names B for an error that happened in A. So each id keeps its
/// history here, as the points where its name changed, read from the collected database-size snapshots:</para>
/// <list type="number">
/// <item>the newest snapshot at or before the error's time that has the id decides the name (a snapshot taken at
/// the same instant counts as before);</item>
/// <item>if no snapshot at or before the error has the id, the OLDEST snapshot after the error that has it;</item>
/// <item>if the id was never seen, <c>"database_id N"</c>, the raw id, rather than a silent blank.</item>
/// </list>
/// A null or 0 id means "no database context" (error_reported often carries database_id 0; <c>DB_NAME(0)</c> is NULL
/// server-side too) and resolves to an empty name. Times compare in UTC; an Unspecified kind is taken as UTC, which
/// is how both stores hold them.
/// </summary>
public sealed class DatabaseNameHistory
{
    /// <summary>One id's name from <see cref="StartedAt"/> on: the first snapshot that carried the id under this name.</summary>
    public readonly record struct Change(int DatabaseId, string DatabaseName, DateTime StartedAt);

    /// <summary>How far each history read looks past the errors' range (#5373): back from the floor snapshot, forward
    /// from the last error, and back from now for an error with no time. The only index is
    /// <c>(server_id, collection_time)</c>, so an id that is never collected would otherwise make every refresh scan the
    /// server's whole history. A database offline longer than this before its error shows <c>database_id N</c>.</summary>
    public const int LookbackDays = 14;

    /// <summary><see cref="LookbackDays"/> as a span.</summary>
    public static TimeSpan LookbackWindow { get; } = TimeSpan.FromDays(LookbackDays);

    private readonly Dictionary<int, Change[]> _byId;

    /// <summary>A history with no snapshots: every real id resolves to its raw id.</summary>
    public static DatabaseNameHistory Empty { get; } = new(Array.Empty<Change>());

    /// <summary>
    /// Builds the history from name-change points, or from raw snapshot rows (a row that repeats the previous
    /// name of the same id changes nothing). Rows may arrive in any order.
    /// </summary>
    public DatabaseNameHistory(IEnumerable<Change> changes)
    {
        _byId = changes
            .GroupBy(c => c.DatabaseId)
            .ToDictionary(
                g => g.Key,
                g => g.Select(c => c with { StartedAt = ToUtc(c.StartedAt) }).OrderBy(c => c.StartedAt).ThenBy(c => c.DatabaseName, StringComparer.Ordinal).ToArray());
    }

    /// <summary>The name <paramref name="databaseId"/> carried at <paramref name="eventTime"/>; see the class remarks.
    /// A null event time (an event with no timestamp) falls back to the id's newest known name.</summary>
    public string Resolve(int? databaseId, DateTime? eventTime)
    {
        if (databaseId is not { } id || id == 0)
            return string.Empty;
        if (!_byId.TryGetValue(id, out var changes) || changes.Length == 0)
            return $"database_id {id}";
        if (eventTime is not { } at)
            return changes[^1].DatabaseName;

        var t = ToUtc(at);
        /* The last change that started at or before t: its name is the newest snapshot at or before t. */
        var found = -1;
        for (var i = 0; i < changes.Length && changes[i].StartedAt <= t; i++)
            found = i;
        /* No snapshot at or before t has the id: the oldest one after it. Two names at that one instant take the larger
           name in ordinal order, the same tie rule as the SQL arms and as a time at or after that instant. */
        if (found < 0)
        {
            found = 0;
            while (found + 1 < changes.Length && changes[found + 1].StartedAt == changes[0].StartedAt)
                found++;
        }

        return changes[found].DatabaseName;
    }

    /// <summary>What a history read has to cover for a set of errors (#5373): the real database ids, the range of the
    /// times of the errors that carry one, and whether any of those errors has no time (it resolves to its id's
    /// newest name, so the newest row per id is read too).</summary>
    public readonly record struct ReadPlan(int[] Ids, (DateTime Min, DateTime Max)? Range, bool NeedsNewest);

    /// <summary>Plans the read for the errors' (database id, time) pairs. An error with no database context (null or
    /// 0 id) needs nothing, whatever its time.</summary>
    public static ReadPlan Plan(IEnumerable<(int? DatabaseId, DateTime? EventTime)> errors)
    {
        var withId = errors.Where(e => e.DatabaseId is { } id && id != 0).ToList();
        return new ReadPlan(
            withId.Select(e => e.DatabaseId!.Value).Distinct().ToArray(),
            RangeOf(withId.Select(e => e.EventTime)),
            withId.Any(e => e.EventTime is null));
    }

    /// <summary>The earliest and latest of the given times, or null when none has a value. The history read
    /// is limited to this range.</summary>
    public static (DateTime Min, DateTime Max)? RangeOf(IEnumerable<DateTime?> times)
    {
        DateTime? min = null, max = null;
        foreach (var time in times)
        {
            if (time is not { } t)
                continue;
            var utc = ToUtc(t);
            if (min is null || utc < min)
                min = utc;
            if (max is null || utc > max)
                max = utc;
        }

        return min is { } lo && max is { } hi ? (lo, hi) : null;
    }

    private static DateTime ToUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
    };
}
