/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #4058: whether a log entry that LOOKS like a deadlock report or an <c>auto_explain</c> plan capture was
/// actually written by one of those subsystems, rather than by a client's own PL/pgSQL <c>RAISE</c> built to
/// imitate one. A database client controls its own error message, detail and SQLSTATE completely — a RAISE
/// can copy <c>ERROR:  deadlock detected</c> and a DETAIL shaped like a wait-for graph, or a LOG line shaped
/// like <c>auto_explain</c>'s <c>duration: N ms  plan:</c> text, verbatim. Nothing about the PRIMARY line or
/// the companion fields a family parser reads distinguishes the forgery from the real thing; what does is
/// WHO wrote it, and PostgreSQL records that in two places this checks.
///
/// <para><b>Two independent signals, because either alone can miss.</b> <see cref="RaisedByPlpgsql"/> reads
/// <c>Context</c>, which PL/pgSQL always appends when a RAISE fires inside a function — its first line ends
/// " at RAISE" — but a genuine deadlock raised BY a PL/pgSQL function's own statement also carries a
/// <c>Context</c> line, ending " SQL statement" instead, never " at RAISE": that is the client's control
/// surface stopping one frame short of PostgreSQL's own report. <see cref="ReportedByOther"/> reads
/// <c>Location</c> — csvlog's <c>location</c> column or jsonlog's <c>func_name</c>, filled only under
/// <c>log_error_verbosity = verbose</c> — which names the C function that actually wrote the entry:
/// <c>DeadLockReport</c> for a real deadlock, <c>explain_ExecutorEnd</c> for a real capture, and
/// <c>exec_stmt_raise</c> for any RAISE, genuine or forged. Verified live against a PostgreSQL 18 target.
/// </para>
///
/// <para><b>This check is PARTIAL, and deliberately says so rather than promising more.</b> A RAISE issued
/// from PL/Perl, PL/Tcl or a C-language function carries none of PL/pgSQL's own <c>Context</c> line, so
/// <see cref="RaisedByPlpgsql"/> is blind to it — <see cref="ReportedByOther"/>'s <c>Location</c> check is
/// what still catches that shape, but only under verbose logging. A client cannot set
/// <c>log_error_verbosity</c> for the server, but a target running at <c>terse</c> or <c>default</c> gets
/// no <see cref="ReportedByOther"/> coverage at all: <c>Location</c> is empty on every other verbosity and
/// on the stderr transport, which carries no such field. And <c>errhidecontext</c> can blank
/// <c>Context</c> for a message issued at a severity below ERROR, though a forged deadlock or plan capture
/// has to run at ERROR or LOG respectively to be believed, where <c>errhidecontext</c> does not apply.
/// Neither signal is a promise that every RAISE-shaped record is caught; together they are the check that
/// exists rather than none at all.</para>
/// </summary>
internal static class PgLogEntryProvenance
{
    private const string FunctionFramePrefix = "PL/pgSQL function ";
    private const string InlineCodeBlockLiteral = "inline_code_block";
    private const string LineToken = " line ";
    private const string AtToken = " at ";
    private const string RaiseKind = "RAISE";

    /// <summary>
    /// True when the INNERMOST frame of <paramref name="e"/>'s <c>Context</c> is a PL/pgSQL function or DO
    /// block whose statement kind is exactly <c>RAISE</c> — the frame PL/pgSQL always appends when a
    /// <c>RAISE</c> statement fires, naming the function (or <c>inline_code_block</c> for a DO block) and
    /// the line the RAISE itself sits on. False when <c>Context</c> is null or empty (no PL/pgSQL frame at
    /// all — the stderr transport's own genuine deadlock and plan-capture reports carry none), and false for
    /// a genuine deadlock raised BY a statement inside a PL/pgSQL function, whose innermost frame is an SPI
    /// frame instead (<c>SQL statement ".."</c> or <c>SQL expression ".."</c>) or a lock-wait frame with no
    /// PL/pgSQL frame at the top at all: that is PostgreSQL's own report, reached through the function
    /// rather than authored by it.
    ///
    /// <para>This parses the function signature itself — walking it character by character with a quote
    /// toggle rather than trusting the first '\n' as a line boundary — because a quoted function name can
    /// contain a literal newline, or text shaped like " line 1 at SQL statement", entirely inside the
    /// quotes PostgreSQL puts around it: splitting on the first physical line before parsing would read a
    /// fragment of the quoted name as the whole frame and miss the real " line N at RAISE" that follows the
    /// signature. A quoted type name can likewise contain '(' or ')'; the parenthesis walk here tracks quote
    /// state so only an UNQUOTED paren changes depth.</para>
    ///
    /// <para>Anything that does not parse — no leading "PL/pgSQL function ", an unterminated signature, no
    /// " line N at " following it — returns false, the same fail-open this replaces: a RAISE issued from
    /// PL/Perl, PL/Tcl or a C-language function carries none of PL/pgSQL's own frame at all, so this is
    /// blind to it (<see cref="ReportedByOther"/>'s <c>Location</c> check is what still catches that shape,
    /// but only under verbose logging), and <c>errhidecontext</c> can blank <c>Context</c> entirely for a
    /// message issued below ERROR.</para>
    /// </summary>
    internal static bool RaisedByPlpgsql(PgLogEntry e)
    {
        if (string.IsNullOrEmpty(e.Context))
        {
            return false;
        }

        var context = e.Context;

        if (!context.StartsWith(FunctionFramePrefix, StringComparison.Ordinal))
        {
            return false;
        }

        int afterSignature;

        var rest = context.AsSpan(FunctionFramePrefix.Length);

        /* A DO block prints no argument list: the frame reads inline_code_block, then " line N at KIND". The
           literal plus the line token together, not the bare word, because a client can CREATE FUNCTION
           inline_code_block(...) and its frame must still be parsed as a signature, or its RAISE would fail open. */
        if (rest.StartsWith(InlineCodeBlockLiteral + LineToken))
        {
            /* A DO block: PostgreSQL names it "inline_code_block" rather than a signature, and no
               parenthesized argument list follows it. */
            afterSignature = FunctionFramePrefix.Length + InlineCodeBlockLiteral.Length;
        }
        else
        {
            var openParen = IndexOfUnquotedChar(context, FunctionFramePrefix.Length, '(');

            if (openParen < 0)
            {
                return false;
            }

            var closeParen = MatchingCloseParen(context, openParen);

            if (closeParen < 0)
            {
                return false;
            }

            afterSignature = closeParen + 1;
        }

        if (!context.AsSpan(afterSignature).StartsWith(LineToken, StringComparison.Ordinal))
        {
            return false;
        }

        var digitsStart = afterSignature + LineToken.Length;
        var i = digitsStart;

        while (i < context.Length && char.IsAsciiDigit(context[i]))
        {
            i++;
        }

        if (i == digitsStart)
        {
            /* " line " with no digits after it isn't the shape a real frame ever has. */
            return false;
        }

        if (!context.AsSpan(i).StartsWith(AtToken, StringComparison.Ordinal))
        {
            return false;
        }

        var kindStart = i + AtToken.Length;
        var newline = context.IndexOf('\n', kindStart);
        var kindEnd = newline < 0 ? context.Length : newline;
        var kind = context[kindStart..kindEnd].TrimEnd('\r');

        return string.Equals(kind, RaiseKind, StringComparison.Ordinal);
    }

    /// <summary>
    /// The index of the first <paramref name="target"/> in <paramref name="s"/> at or after
    /// <paramref name="start"/> that sits OUTSIDE a double-quoted run, or -1 if none does. A double quote
    /// toggles the in-quotes state; a doubled <c>""</c> (PostgreSQL's escape for a literal quote inside a
    /// quoted identifier) toggles it twice, netting no change, which is the correct outcome.
    /// </summary>
    private static int IndexOfUnquotedChar(string s, int start, char target)
    {
        var inQuotes = false;

        for (var i = start; i < s.Length; i++)
        {
            var c = s[i];

            if (c == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (!inQuotes && c == target)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// The index of the <c>)</c> matching the <c>(</c> at <paramref name="openParen"/>, tracking both
    /// double-quote state (so a quoted type name's own parens never change depth) and paren depth (so a
    /// type modifier like <c>numeric(10,2)</c> nested inside the argument list is walked over correctly),
    /// or -1 if the signature never closes.
    /// </summary>
    private static int MatchingCloseParen(string s, int openParen)
    {
        var inQuotes = false;
        var depth = 0;

        for (var i = openParen; i < s.Length; i++)
        {
            var c = s[i];

            if (c == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (inQuotes)
            {
                continue;
            }

            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;

                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    /// <summary>
    /// True when <paramref name="e"/>'s <c>Location</c> is filled (verbose logging only) and does NOT start
    /// with <paramref name="expectedFunction"/> — the C function that actually wrote the entry is not the
    /// one the caller is trusting the entry to have come from. False when <c>Location</c> is empty: under
    /// any verbosity other than verbose, and on the stderr transport, <c>Location</c> carries nothing at
    /// all, and an empty result here is not a clearance — it means this signal has nothing to say and the
    /// caller falls back to <see cref="RaisedByPlpgsql"/>.
    /// </summary>
    internal static bool ReportedByOther(PgLogEntry e, string expectedFunction)
    {
        ArgumentException.ThrowIfNullOrEmpty(expectedFunction);

        if (string.IsNullOrEmpty(e.Location))
        {
            return false;
        }

        return !e.Location.StartsWith(expectedFunction, StringComparison.Ordinal);
    }
}
