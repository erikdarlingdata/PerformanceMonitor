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
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// #4384: an exact, per-file census of every place production C# source builds a string literal that
/// mentions BOTH <c>plan_force_actions</c> and the whole word <c>detail</c> — the replacement for the
/// #4346/#4376 raw-<c>detail</c>-reader scan, which attributed sites to a computed enclosing-method name
/// (fragile: a nested type or a field-held SQL constant could steal or dodge that attribution). This census
/// makes no attempt to name a caller; it counts LITERALS, joined the way the C# compiler would treat
/// adjacent pieces built with <c>+</c> as a single logical string.
/// </summary>
internal static class PlanForceActionDetailCensus
{
    /// <summary>A raw literal span found in source, with the source range it occupied (used only to check
    /// the gap between two consecutive spans when deciding whether to join them).</summary>
    private readonly record struct LiteralSpan(int Start, int End, string Text);

    /// <summary>
    /// Tokenizes <paramref name="source"/> into its string literals — regular, verbatim (<c>@"..."</c>),
    /// interpolated (<c>$"..."</c>, <c>$@"..."</c>/<c>@$"..."</c>), and raw (<c>"""..."""</c>, including
    /// interpolated raw) — skipping <c>//</c> and <c>/* */</c> comments and <c>'...'</c> char literals, and
    /// JOINS consecutive literals separated only by whitespace, comments, and <c>+</c> into one logical
    /// literal, the way the compiler folds a compile-time constant concatenation. Each interpolation hole
    /// (<c>{...}</c>, brace-balanced; <c>{{</c>/<c>}}</c> are literal escapes, not holes) is replaced with a
    /// single space in the returned text, so a hole never accidentally supplies either half of a two-word
    /// match.
    /// </summary>
    public static IReadOnlyList<string> JoinedStringLiterals(string source)
    {
        var spans = FindLiteralSpans(source);
        var joined = new List<string>();
        var current = new StringBuilder();
        var haveCurrent = false;
        var previousEnd = -1;

        foreach (var span in spans)
        {
            var canJoin = haveCurrent && OnlyWhitespaceCommentsAndPlus(source, previousEnd, span.Start);

            if (!canJoin)
            {
                if (haveCurrent)
                {
                    joined.Add(current.ToString());
                }

                current = new StringBuilder();
                haveCurrent = true;
            }

            current.Append(span.Text);
            previousEnd = span.End;
        }

        if (haveCurrent)
        {
            joined.Add(current.ToString());
        }

        return joined;
    }

    /// <summary>True when <paramref name="source"/>[<paramref name="start"/>..<paramref name="end"/>)
    /// consists only of whitespace, <c>//</c>/<c>/* */</c> comments, and <c>+</c> tokens.</summary>
    private static bool OnlyWhitespaceCommentsAndPlus(string source, int start, int end)
    {
        var i = start;
        while (i < end)
        {
            var c = source[i];

            if (char.IsWhiteSpace(c) || c == '+')
            {
                i++;
                continue;
            }

            if (c == '/' && i + 1 < end && source[i + 1] == '/')
            {
                var nl = source.IndexOf('\n', i, end - i);
                i = nl < 0 ? end : nl + 1;
                continue;
            }

            if (c == '/' && i + 1 < end && source[i + 1] == '*')
            {
                var close = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = (close < 0 || close + 2 > end) ? end : close + 2;
                continue;
            }

            return false;
        }

        return true;
    }

    /// <summary>The number of JOINED literals (see <see cref="JoinedStringLiterals"/>) whose text contains
    /// both <c>plan_force_actions</c> and the whole word <c>detail</c>, case-insensitive. Deliberately
    /// simple — no attribution, no caller name — so a new site anywhere in the joined text is caught by
    /// the exact counts this census pins.
    /// </summary>
    public static int CountDetailSites(string source)
    {
        var count = 0;
        foreach (var literal in JoinedStringLiterals(source))
        {
            if (literal.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase)
                && Regex.IsMatch(literal, @"\bdetail\b", RegexOptions.IgnoreCase))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// The number of JOINED literals (see <see cref="JoinedStringLiterals"/>) that name the table
    /// <c>plan_force_actions</c> at all, with or without <c>detail</c> alongside it. This exists because
    /// <see cref="CountDetailSites"/> only counts co-occurrence: a table name held in its own <c>const</c>,
    /// a bare <c>SELECT *</c> against the table, or a <c>detail</c> reference that lands in a separate
    /// interpolation hole from the table name all escape that co-occurrence count, but every one of them
    /// still joins to a literal that names the table — which is exactly what this method pins.
    /// </summary>
    public static int CountTableMentions(string source)
    {
        var count = 0;
        foreach (var literal in JoinedStringLiterals(source))
        {
            if (literal.Contains("plan_force_actions", StringComparison.OrdinalIgnoreCase))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Whole-word occurrences of <paramref name="identifier"/> in <paramref name="source"/> once comments
    /// AND string/char literal bodies are removed. Counts identifier USES in code: a declaration's own
    /// name token counts exactly like a reference to it.
    /// </summary>
    public static int CountIdentifierUses(string source, string identifier)
    {
        var masked = MaskCommentsAndLiterals(source);
        return Regex.Matches(masked, $@"\b{Regex.Escape(identifier)}\b").Count;
    }

    /// <summary>Every literal span found in <paramref name="source"/>, in source order.</summary>
    private static List<LiteralSpan> FindLiteralSpans(string source)
    {
        var spans = new List<LiteralSpan>();
        var i = 0;
        var n = source.Length;

        while (i < n)
        {
            var c = source[i];

            if (c == '/' && i + 1 < n && source[i + 1] == '/')
            {
                var nl = source.IndexOf('\n', i);
                i = nl < 0 ? n : nl + 1;
                continue;
            }

            if (c == '/' && i + 1 < n && source[i + 1] == '*')
            {
                var close = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? n : close + 2;
                continue;
            }

            if (c == '\'')
            {
                i++;
                while (i < n)
                {
                    if (source[i] == '\\' && i + 1 < n)
                    {
                        i += 2;
                        continue;
                    }

                    if (source[i] == '\'')
                    {
                        i++;
                        break;
                    }

                    i++;
                }

                continue;
            }

            var prefixLength = LiteralPrefixLength(source, i);
            if (prefixLength >= 0)
            {
                var start = i;
                var (text, end) = ReadStringLiteral(source, i, prefixLength);
                spans.Add(new LiteralSpan(start, end, text));
                i = end;
                continue;
            }

            i++;
        }

        return spans;
    }

    /// <summary>Returns <paramref name="source"/> with every comment and string/char literal body replaced
    /// by spaces (newlines preserved so line-based callers still line up).</summary>
    private static string MaskCommentsAndLiterals(string source)
    {
        var builder = new StringBuilder(source.Length);
        var i = 0;
        var n = source.Length;

        while (i < n)
        {
            var c = source[i];

            if (c == '/' && i + 1 < n && source[i + 1] == '/')
            {
                var nl = source.IndexOf('\n', i);
                var end = nl < 0 ? n : nl;
                AppendMasked(builder, source, i, end);
                i = end;
                continue;
            }

            if (c == '/' && i + 1 < n && source[i + 1] == '*')
            {
                var close = source.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var end = close < 0 ? n : close + 2;
                AppendMasked(builder, source, i, end);
                i = end;
                continue;
            }

            if (c == '\'')
            {
                var start = i;
                i++;
                while (i < n)
                {
                    if (source[i] == '\\' && i + 1 < n)
                    {
                        i += 2;
                        continue;
                    }

                    if (source[i] == '\'')
                    {
                        i++;
                        break;
                    }

                    i++;
                }

                AppendMasked(builder, source, start, i);
                continue;
            }

            var prefixLength = LiteralPrefixLength(source, i);
            if (prefixLength >= 0)
            {
                var start = i;
                var (_, end) = ReadStringLiteral(source, i, prefixLength);
                AppendMasked(builder, source, start, end);
                i = end;
                continue;
            }

            builder.Append(c);
            i++;
        }

        return builder.ToString();
    }

    private static void AppendMasked(StringBuilder builder, string source, int start, int end)
    {
        for (var k = start; k < end; k++)
        {
            builder.Append(source[k] == '\n' ? '\n' : ' ');
        }
    }

    /// <summary>Length of the literal's prefix (<c>@</c>, <c>$</c>, <c>$@</c>, <c>@$</c>, or none) if
    /// <paramref name="position"/> begins a string literal, else -1.</summary>
    private static int LiteralPrefixLength(string source, int position)
    {
        var n = source.Length;
        if (position >= n)
        {
            return -1;
        }

        if (source[position] == '"')
        {
            return 0;
        }

        if (source[position] == '@' && position + 1 < n)
        {
            if (source[position + 1] == '"')
            {
                return 1;
            }

            if (source[position + 1] == '$' && position + 2 < n && source[position + 2] == '"')
            {
                return 2;
            }
        }

        if (source[position] == '$' && position + 1 < n)
        {
            if (source[position + 1] == '"')
            {
                return 1;
            }

            if (source[position + 1] == '@' && position + 2 < n && source[position + 2] == '"')
            {
                return 2;
            }
        }

        return -1;
    }

    /// <summary>Reads one string literal beginning at <paramref name="start"/> (whose prefix is
    /// <paramref name="prefixLength"/> characters, before the first quote), returning its full text
    /// (interpolation holes replaced with a single space each) and the index just past its end.</summary>
    private static (string Text, int End) ReadStringLiteral(string source, int start, int prefixLength)
    {
        var n = source.Length;
        var prefix = source[start..(start + prefixLength)];
        var isVerbatim = prefix.Contains('@');
        var isInterpolated = prefix.Contains('$');

        var quoteStart = start + prefixLength;
        var quoteRunLength = 0;
        while (quoteStart + quoteRunLength < n && source[quoteStart + quoteRunLength] == '"')
        {
            quoteRunLength++;
        }

        if (quoteRunLength >= 3)
        {
            return ReadRawString(source, start, quoteStart, quoteRunLength, isInterpolated);
        }

        var builder = new StringBuilder();
        builder.Append(prefix).Append('"');
        var i = quoteStart + 1;

        while (i < n)
        {
            var c = source[i];

            if (isVerbatim)
            {
                if (c == '"')
                {
                    if (i + 1 < n && source[i + 1] == '"')
                    {
                        builder.Append("\"\"");
                        i += 2;
                        continue;
                    }

                    builder.Append('"');
                    i++;
                    break;
                }
            }
            else
            {
                if (c == '\\' && i + 1 < n)
                {
                    builder.Append(c).Append(source[i + 1]);
                    i += 2;
                    continue;
                }

                if (c == '"')
                {
                    builder.Append('"');
                    i++;
                    break;
                }

                if (c == '\n')
                {
                    /* Unterminated regular string literal — bail rather than run to EOF. */
                    break;
                }
            }

            if (isInterpolated && c == '{')
            {
                if (i + 1 < n && source[i + 1] == '{')
                {
                    builder.Append("{{");
                    i += 2;
                    continue;
                }

                i = SkipInterpolationHole(source, i, builder);
                continue;
            }

            if (isInterpolated && c == '}' && i + 1 < n && source[i + 1] == '}')
            {
                builder.Append("}}");
                i += 2;
                continue;
            }

            builder.Append(c);
            i++;
        }

        return (builder.ToString(), i);
    }

    /// <summary>Reads a raw string literal (<c>"""..."""</c>, N&gt;=3 quotes, optionally interpolated),
    /// replacing each hole with a single space.</summary>
    private static (string Text, int End) ReadRawString(
        string source, int literalStart, int quoteStart, int quoteRunLength, bool isInterpolated)
    {
        var n = source.Length;
        var delimiter = new string('"', quoteRunLength);
        var bodyStart = quoteStart + quoteRunLength;
        var builder = new StringBuilder();
        builder.Append(source, literalStart, bodyStart - literalStart);

        var i = bodyStart;
        while (i < n)
        {
            if (isInterpolated && source[i] == '{')
            {
                if (i + 1 < n && source[i + 1] == '{')
                {
                    builder.Append("{{");
                    i += 2;
                    continue;
                }

                i = SkipInterpolationHole(source, i, builder);
                continue;
            }

            if (source[i] == '"'
                && i + quoteRunLength <= n
                && string.CompareOrdinal(source, i, delimiter, 0, quoteRunLength) == 0
                && (i + quoteRunLength >= n || source[i + quoteRunLength] != '"'))
            {
                builder.Append(delimiter);
                i += quoteRunLength;
                break;
            }

            builder.Append(source[i]);
            i++;
        }

        return (builder.ToString(), i);
    }

    /// <summary>Skips a brace-balanced <c>{...}</c> interpolation hole starting at the <c>{</c> at
    /// <paramref name="openBraceIndex"/>, appending a single space to <paramref name="builder"/> in its
    /// place, and returns the index just past the matching <c>}</c>.</summary>
    private static int SkipInterpolationHole(string source, int openBraceIndex, StringBuilder builder)
    {
        var n = source.Length;
        var depth = 0;
        var i = openBraceIndex;

        while (i < n)
        {
            var c = source[i];

            if (c == '{')
            {
                depth++;
                i++;
                continue;
            }

            if (c == '}')
            {
                depth--;
                i++;
                if (depth == 0)
                {
                    break;
                }

                continue;
            }

            if (c == '"')
            {
                /* A string literal inside the hole (e.g. a ternary's branch) — skip it whole so a brace
                   inside it never desynchronises the depth count. */
                var innerPrefixLength = LiteralPrefixLength(source, i);
                var (_, end) = ReadStringLiteral(source, i, Math.Max(innerPrefixLength, 0));
                i = end;
                continue;
            }

            i++;
        }

        builder.Append(' ');
        return i;
    }
}
