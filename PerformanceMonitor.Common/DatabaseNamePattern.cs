/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one meaning of an entry in an excluded-databases list (#9050). <c>*</c> matches any run of characters,
/// including none. Nothing else is a wildcard: <c>?</c>, <c>%</c>, <c>_</c> and <c>[</c> are literal characters,
/// and an entry without <c>*</c> is an exact name, as it always was (database names often hold <c>_</c>, so
/// LIKE syntax would have changed existing lists). The C# match (<see cref="Matches"/>) and the SQL spelling
/// (<see cref="ToLikePattern"/>) live side by side so a collector's SQL and an alert's C# filter cannot disagree
/// about which names an entry covers. Case follows the caller: C# folds case (as every filter of the list always
/// has), SQL follows the column's collation.
/// </summary>
public static class DatabaseNamePattern
{
    /// <summary>The one wildcard character.</summary>
    public const char Wildcard = '*';

    /// <summary>The escape character <see cref="ToLikePattern"/> writes, for <c>LIKE ... ESCAPE '\'</c>.</summary>
    public const char LikeEscape = '\\';

    /// <summary>True when the entry holds a wildcard and so is a pattern, not an exact name.</summary>
    public static bool IsPattern(string? entry) => entry is not null && entry.Contains(Wildcard);

    /// <summary>True when the entry is one or more <c>*</c> and nothing else: it would match every database.</summary>
    public static bool IsMatchAll(string? entry)
    {
        if (string.IsNullOrEmpty(entry))
        {
            return false;
        }

        foreach (var c in entry)
        {
            if (c != Wildcard)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// True when <paramref name="name"/> is covered by <paramref name="entry"/>, ignoring case. A match-all entry covers nothing. An entry without
    /// <c>*</c> must equal the name; with <c>*</c>, each run of characters between stars must appear in order, the
    /// first at the start and the last at the end.
    /// </summary>
    public static bool Matches(string? entry, string? name)
    {
        if (entry is null || name is null)
        {
            return false;
        }

        /* A match-all entry is ignored (#9050): a list that held a bare * before wildcards existed did nothing,
           and it must not start dropping every database now. The Viewer and the MCP tool refuse to write one. */
        if (IsMatchAll(entry))
        {
            return false;
        }

        if (!IsPattern(entry))
        {
            return string.Equals(entry, name, StringComparison.OrdinalIgnoreCase);
        }

        var parts = entry.Split(Wildcard);
        var first = parts[0];
        var last = parts[^1];
        if (name.Length < first.Length + last.Length
            || !name.StartsWith(first, StringComparison.OrdinalIgnoreCase)
            || !name.EndsWith(last, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var position = first.Length;
        var end = name.Length - last.Length;
        for (var i = 1; i < parts.Length - 1; i++)
        {
            if (parts[i].Length == 0)
            {
                continue;
            }

            var found = name.IndexOf(parts[i], position, end - position, StringComparison.OrdinalIgnoreCase);
            if (found < 0)
            {
                return false;
            }

            position = found + parts[i].Length;
        }

        return true;
    }

    /// <summary>True when any entry covers <paramref name="name"/>. Entries are used as written.</summary>
    public static bool MatchesAny(IEnumerable<string?>? entries, string? name)
    {
        if (entries is null || name is null)
        {
            return false;
        }

        foreach (var entry in entries)
        {
            if (Matches(entry, name))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>True when any entry, trimmed first, covers <paramref name="name"/>. For the filters that have always trimmed.</summary>
    public static bool MatchesAnyTrimmed(IEnumerable<string?>? entries, string? name)
    {
        if (entries is null || name is null)
        {
            return false;
        }

        foreach (var entry in entries)
        {
            if (Matches(entry?.Trim(), name))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The entry as a <c>LIKE</c> operand for <c>ESCAPE '\'</c>: <c>*</c> becomes <c>%</c>, and the escape
    /// character, <c>%</c> and <c>_</c> are escaped so they stay literal. <paramref name="escapeBracket"/> also
    /// escapes <c>[</c>, which SQL Server's LIKE reads as the start of a character class and PostgreSQL's does not.
    /// Escaping it is harmless on PostgreSQL (a backslash makes the next character literal), so one spelling serves
    /// both there. DuckDB's ILIKE refuses an escape before a character that is not special, so its callers pass false.
    /// </summary>
    public static string ToLikePattern(string entry, bool escapeBracket)
    {
        var sb = new StringBuilder(entry.Length + 8);
        foreach (var c in entry)
        {
            switch (c)
            {
                case Wildcard:
                    sb.Append('%');
                    break;
                case LikeEscape:
                case '%':
                case '_':
                    sb.Append(LikeEscape).Append(c);
                    break;
                case '[' when escapeBracket:
                    sb.Append(LikeEscape).Append(c);
                    break;
                default:
                    sb.Append(c);
                    break;
            }
        }

        return sb.ToString();
    }
}
