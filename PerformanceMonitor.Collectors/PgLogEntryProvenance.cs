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
    private const string RaiseContextSuffix = " at RAISE";

    /// <summary>
    /// True when the first line of <paramref name="e"/>'s <c>Context</c> ends with " at RAISE" — the line
    /// PL/pgSQL always appends when a <c>RAISE</c> statement fires inside a function, naming the function
    /// and line the RAISE itself sits on. False when <c>Context</c> is null or empty (no PL/pgSQL frame at
    /// all — the stderr transport's own genuine deadlock and plan-capture reports carry none), and false for
    /// a genuine deadlock raised BY a statement inside a PL/pgSQL function, whose first <c>Context</c> line
    /// ends " SQL statement" instead: that is PostgreSQL's own report, reached through the function rather
    /// than authored by it.
    /// </summary>
    internal static bool RaisedByPlpgsql(PgLogEntry e)
    {
        if (string.IsNullOrEmpty(e.Context))
        {
            return false;
        }

        var newline = e.Context.IndexOf('\n');
        var firstLine = newline < 0 ? e.Context : e.Context[..newline];
        firstLine = firstLine.TrimEnd('\r');

        return firstLine.EndsWith(RaiseContextSuffix, StringComparison.Ordinal);
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
