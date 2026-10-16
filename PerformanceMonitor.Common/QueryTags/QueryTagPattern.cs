/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common.QueryTags;

/// <summary>
/// The one pattern matcher both stores use (#5979). A pattern ignores case and treats <c>*</c> as the only
/// wildcard: it matches any run of characters, including none. Every other character, <c>.</c>, <c>(</c> and
/// <c>[</c> included, matches itself. The pattern is never handed to a regular expression engine; it is split
/// on <c>*</c> and the pieces are searched for with ordinal, case-insensitive string comparison.
/// </summary>
public static class QueryTagPattern
{
    private const StringComparison Cmp = StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// True when <paramref name="pattern"/> matches the whole of <paramref name="value"/>. A null value never
    /// matches, and neither does a null or empty pattern.
    /// </summary>
    public static bool MatchesWhole(string? pattern, string? value) => Matches(pattern, value, false);

    /// <summary>
    /// True when <paramref name="pattern"/> matches somewhere inside <paramref name="value"/>, as if it were
    /// wrapped in <c>*</c> on both sides. A null value never matches, and neither does a null or empty pattern.
    /// </summary>
    public static bool MatchesAnywhere(string? pattern, string? value) => Matches(pattern, value, true);

    private static bool Matches(string? pattern, string? value, bool anywhere)
    {
        if (string.IsNullOrEmpty(pattern) || value is null)
        {
            return false;
        }

        string[] pieces = pattern.Split('*');
        // A piece before the first star is anchored to the start, a piece after the last star to the end,
        // unless the caller asked for "anywhere", which frees both ends.
        bool anchoredStart = !anywhere && pieces[0].Length > 0;
        bool anchoredEnd = !anywhere && pieces[^1].Length > 0;

        if (pieces.Length == 1)
        {
            // No wildcard at all.
            return anywhere
                ? value.Contains(pieces[0], Cmp)
                : string.Equals(value, pieces[0], Cmp);
        }

        int position = 0;
        int last = pieces.Length - 1;
        for (int i = 0; i <= last; i++)
        {
            string piece = pieces[i];
            if (piece.Length == 0)
            {
                continue;
            }

            if (i == 0 && anchoredStart)
            {
                if (!value.StartsWith(piece, Cmp))
                {
                    return false;
                }

                position = piece.Length;
                continue;
            }

            if (i == last && anchoredEnd)
            {
                // The tail must sit at the very end and must not reach back into what earlier pieces used.
                return value.Length - piece.Length >= position && value.EndsWith(piece, Cmp);
            }

            int found = value.IndexOf(piece, position, Cmp);
            if (found < 0)
            {
                return false;
            }

            position = found + piece.Length;
        }

        return true;
    }

    /// <summary>
    /// Reads every <c>/* tag:NAME */</c> or <c>-- tag:NAME</c> comment in <paramref name="text"/>, in the
    /// order they appear. NAME ends at whitespace or at <c>*/</c>. Hand-parsed, no regular expression.
    /// </summary>
    public static IEnumerable<string> TagCommentNames(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            yield break;
        }

        int from = 0;
        while (from < text.Length)
        {
            int at = text.IndexOf("tag:", from, Cmp);
            if (at < 0)
            {
                yield break;
            }

            from = at + 4;

            // Walk back over whitespace; the comment opener must sit right there.
            int before = at - 1;
            while (before >= 0 && (text[before] == ' ' || text[before] == '\t'))
            {
                before--;
            }

            bool opened = before >= 1
                && ((text[before] == '*' && text[before - 1] == '/')
                    || (text[before] == '-' && text[before - 1] == '-'));
            if (!opened)
            {
                continue;
            }

            int end = from;
            while (end < text.Length
                   && !char.IsWhiteSpace(text[end])
                   && !(text[end] == '*' && end + 1 < text.Length && text[end + 1] == '/'))
            {
                end++;
            }

            if (end > from)
            {
                yield return text.Substring(from, end - from);
            }

            from = end;
        }
    }
}
