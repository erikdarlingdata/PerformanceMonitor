/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// <para>Decides whether the command constructed at a given offset had its deadline chosen on purpose (#2874).
/// Five command-timeout pins ask that question - <c>.Storage</c>, <c>.Viewer</c>, <c>.Analysis</c>,
/// <c>PgFactCollector</c> and the alert pass - and they used to ask it five identical ways. This is the one
/// implementation they share, the same treatment <see cref="CSharpSourceWalker"/> gave the five copies of the
/// source walk in #2913 and <c>IlCallSiteScanner</c> gave the five copies of the IL walk in #2898. The walk
/// copies had already drifted by the time they were consolidated, and only one of them carried the hardening
/// the others needed; a judgement duplicated five ways would drift the same way.</para>
///
/// <para><b>The question is asked in two halves, because there are two places a deadline can legitimately be
/// written and each has a different neighbour problem.</b> An initializer belongs to the construction it is
/// attached to, so it is read from the CONSTRUCTION span; read from the wider statement span it would accept
/// the FOLLOWING construction's initializer, and an untimed command directly ahead of a timed one would pass.
/// An assignment names its target, so it cannot be mistaken for a construction's own, and it is read from the
/// statement span - which is where the <c>CreateCommand</c> shape has to put it, a method result being unable
/// to take an initializer.</para>
///
/// <para><b>Both halves are asked of STRIPPED source.</b> A deadline merely SPELLED in a comment is not a
/// deadline, and this codebase quotes code in its prose constantly: judged raw, a note explaining where the
/// deadline used to be stands in for the deadline itself. Callers pass
/// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output, which is character-aligned with its
/// input, so an offset taken from either means the same thing in both.</para>
/// </summary>
internal static class CommandDeadlineScanner
{
    /// <summary>
    /// A deadline in a construction's own object initializer. Read against the CONSTRUCTION span only.
    /// </summary>
    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Whether the command constructed at <paramref name="index"/> in already-STRIPPED
    /// <paramref name="code"/> sets an explicit deadline.
    /// </summary>
    internal static bool SetsAnExplicitDeadline(string code, int index)
    {
        ArgumentNullException.ThrowIfNull(code);

        if (s_setsTimeout.IsMatch(CSharpSourceWalker.ConstructionSpanFrom(code, index)))
        {
            return true;
        }

        var bound = BoundName(code, index);

        if (bound is null)
        {
            /* Nothing was bound to the construction - it is returned, or passed straight into a call - so the
               initializer was the only place a deadline could have been written. */
            return false;
        }

        return Assigns(bound).IsMatch(CSharpSourceWalker.StatementSpanFrom(code, index, statements: 2));
    }

    /// <summary>
    /// <para>An assignment to <c>CommandTimeout</c> through <paramref name="bound"/> specifically.</para>
    ///
    /// <para><b>The name is what makes this the site's own deadline rather than a neighbour's.</b> An
    /// unqualified <c>\.CommandTimeout\s*=</c> over the statement span accepts a SIBLING's assignment: an
    /// untimed <c>using (...)</c> header whose block opens with <c>using var sibling = conn.CreateCommand();</c>
    /// and <c>sibling.CommandTimeout = 10;</c> spends both counted statements on the sibling, and the outer
    /// command reads as timed. Every fixture in this family used to exercise that sibling through an object
    /// INITIALIZER, which has no leading dot and so never reached this regex - while the assignment spelling
    /// it missed is the dominant one, 112 of the MCP surface's 119 sites. Found in review, not by the
    /// fixtures.</para>
    /// </summary>
    private static Regex Assigns(string bound) => new(
        @"(?<![A-Za-z0-9_])" + Regex.Escape(bound) + @"\s*[!?]?\s*\.\s*CommandTimeout\s*=",
        RegexOptions.CultureInvariant);

    /// <summary>
    /// <para>The identifier the construction at <paramref name="index"/> is bound to, or null when it is not
    /// bound to one.</para>
    ///
    /// <para>Taken as the identifier before the FIRST <c>=</c> of the statement the construction sits in,
    /// which is the binder in every shape here and in none of the shapes that have no binder: a declaration
    /// (<c>using var c = ...</c>), a header declaration (<c>using (var c = ...)</c>) and a field assignment
    /// all put it there, while <c>return new ...</c> and <c>Wrap(conn.CreateCommand())</c> have no <c>=</c> at
    /// all. FIRST rather than last so a conditional whose arms are each a construction still resolves to the
    /// one variable they share - the shape in <c>ViewerDataService.FinOps.Locking.cs</c> - and comparison and
    /// lambda operators are skipped so <c>x == null ? ... : ...</c> does not look like a binder.</para>
    /// </summary>
    private static string? BoundName(string code, int index)
    {
        var from = index;

        while (from > 0 && code[from - 1] is not (';' or '{' or '}'))
        {
            from--;
        }

        for (var i = from; i < index; i++)
        {
            if (code[i] != '=')
            {
                continue;
            }

            if (i + 1 < code.Length && code[i + 1] is '=' or '>')
            {
                i++;
                continue;
            }

            if (i > from && code[i - 1] is '=' or '!' or '<' or '>' or '+' or '-' or '*' or '/' or '|' or '&' or '^' or '%')
            {
                continue;
            }

            var end = i - 1;

            while (end >= from && char.IsWhiteSpace(code[end]))
            {
                end--;
            }

            var start = end;

            while (start >= from && (char.IsLetterOrDigit(code[start]) || code[start] == '_'))
            {
                start--;
            }

            return end > start ? code[(start + 1)..(end + 1)] : null;
        }

        return null;
    }
}
