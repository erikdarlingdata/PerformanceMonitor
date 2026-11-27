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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Input checks for the alert-side excluded-databases list (#6098 follow-up): at most <see cref="MaxEntries"/> names of at
/// most <see cref="MaxNameLength"/> characters (the SQL Server limit for a database name), none holding a control character.
/// A comma IS allowed, since SQL Server database names may contain one. Names are otherwise kept exactly as written:
/// the list has always been stored raw, and this adds bounds only.
/// An entry may hold <c>*</c> as a wildcard (#9050, <see cref="PerformanceMonitor.Common.DatabaseNamePattern"/>). One made
/// only of <c>*</c> would skip every database, master included, so the excluded-databases lists refuse it.
/// </summary>
public static class ExcludedDatabaseNames
{
    /// <summary>Most names the list keeps.</summary>
    public const int MaxEntries = 256;

    /// <summary>Longest name, in characters.</summary>
    public const int MaxNameLength = 128;

    /// <summary>True when one name is within the length bound and holds no control character (NUL included).</summary>
    public static bool IsAcceptableName(string? name) =>
        name is not null && name.Length <= MaxNameLength && !name.Any(char.IsControl);

    /// <summary>The refusal text for a write that arrives as a whole (the MCP write), or null when the list is
    /// acceptable. The caller learns instead of the list being truncated or a name silently lost.
    /// <paramref name="patterns"/> is true for a list whose entries may be patterns (the excluded-databases lists):
    /// an entry of only <c>*</c> is then refused. A list of literal names (the Extended Events session databases)
    /// passes false and keeps a <c>*</c> as a character.</summary>
    public static string? Refusal(IEnumerable<string>? raw, string field, bool patterns = true)
    {
        if (raw is null)
        {
            return null;
        }

        var count = 0;
        foreach (var name in raw)
        {
            if (++count > MaxEntries)
            {
                return $"'{field}' holds more than {MaxEntries} entries.";
            }

            if (!IsAcceptableName(name))
            {
                return $"'{field}' entries must not contain a control character and must be at most {MaxNameLength} characters.";
            }

            if (patterns && DatabaseNamePattern.IsMatchAll(name))
            {
                return $"'{field}' entries must not be only * characters, because that would skip every database. Use a pattern such as archive_*.";
            }
        }

        return null;
    }

    /// <summary>The list with every name <see cref="IsAcceptableName"/> rejects dropped, and every entry made only of
    /// <c>*</c>, cut off at <see cref="MaxEntries"/> names. Order and spelling are untouched.</summary>
    public static IReadOnlyList<string> Normalize(IEnumerable<string>? raw)
    {
        if (raw is null)
        {
            return Array.Empty<string>();
        }

        return raw.Where(n => IsAcceptableName(n) && !DatabaseNamePattern.IsMatchAll(n)).Take(MaxEntries).ToList();
    }
}
