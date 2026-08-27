/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using Npgsql;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Why an analysis pass stopped early, when it did (#2430).
/// </summary>
public enum AnalysisAbandonKind
{
    /// <summary>Not an abandonment at all — a genuine fault, which keeps its ERROR.</summary>
    None,

    /// <summary>The host is stopping. Expected, costs one Information line, and the next start recomputes.</summary>
    Shutdown,

    /// <summary>
    /// The pass outran its per-run budget on a service that is otherwise fine. Expected in the sense
    /// that we raised it, NOT in the sense that it is healthy — a pass that cannot finish inside its
    /// budget is losing that server a cycle of findings, so it is a warning and not an Information.
    /// </summary>
    Timeout
}

/// <summary>
/// Classifies whether an analysis-pass failure is the residue of an abandonment we asked for, so the
/// catch sites can tell <i>unfinished because we called it off</i> from <i>unfinished because
/// something broke</i> (#2299) — and, since #2430, which of the two ways we called it off.
///
/// <para>Before #2299, a clean <c>Stop-Service</c> logged seven ERRORs from
/// work still in flight after "collection loop stopped" — the loop's data source is disposed at
/// method scope exit and the managed postmaster is then <c>pg_ctl stop -m fast</c>-ed, so the
/// abandoned pass's next store read throws <see cref="ObjectDisposedException"/> (or the server
/// kills its open connection with 57P01), and those seven lines were 7 of the day's 9 ERRORs,
/// burying the two that meant something.</para>
/// </summary>
public static class AnalysisShutdown
{
    /// <summary>
    /// True when this failure should be ABANDONED quietly rather than logged as a fault: the pass's
    /// token has fired AND the exception is a shape an abandonment produces. Both halves are
    /// load-bearing — the same exceptions with the token NOT signalled mean a data source was
    /// disposed (or a connection administratively killed) while the pass was meant to be running,
    /// which is a real bug whose only evidence is exactly this text, so it must stay an ERROR.
    /// Component catch sites use this in a <c>when</c> filter so the residue PROPAGATES — unwinding
    /// the pass to the one line <see cref="Classify"/> chooses — instead of being swallowed
    /// per-metric.
    ///
    /// <para>This was <c>IsShutdownAbandon</c> until #2430, and the rename is the point. The token it
    /// is handed is now the pass's armed budget, so "the token fired" no longer means "we are
    /// stopping": at this level the REASON does not matter, only that nobody should see nine ERRORs
    /// for work we ourselves called off. Deciding which reason is <see cref="Classify"/>'s job, once,
    /// where both tokens are in scope.</para>
    /// </summary>
    public static bool IsExpectedAbandon(Exception ex, CancellationToken passToken) =>
        passToken.IsCancellationRequested && IsShutdownResidue(ex);

    /// <summary>
    /// Which kind of abandonment this failure is, or <see cref="AnalysisAbandonKind.None"/> if it is a
    /// genuine fault (#2430). Asked ONCE, at the top of the pass, because it is the only place both
    /// tokens are in scope — and because the answer decides a log LEVEL and a sentence, both of which
    /// are read by someone deciding whether to investigate.
    ///
    /// <para>Shutdown is tested first and wins a tie: a stop arriving during an already-overrunning
    /// pass is a stop, and reporting it as a timeout would invent an incident out of a clean
    /// <c>Stop-Service</c>.</para>
    ///
    /// <para>The timeout arm is deliberately narrower than the shutdown arm. A budget expiring produces
    /// exactly one shape — the token observed properly — whereas a disposed data source or a 57P0x
    /// means the store went away, which a timeout on a running service does not cause. Widening this
    /// arm to the full residue set would quietly relabel the very bug #2299 kept an ERROR, for the
    /// whole window after any pass overruns. Those fall through to <see cref="AnalysisAbandonKind.None"/>
    /// and stay faults.</para>
    /// </summary>
    public static AnalysisAbandonKind Classify(
        Exception ex, CancellationToken shutdownToken, CancellationToken passToken)
    {
        if (shutdownToken.IsCancellationRequested && IsShutdownResidue(ex))
        {
            return AnalysisAbandonKind.Shutdown;
        }

        if (passToken.IsCancellationRequested && ex is OperationCanceledException)
        {
            return AnalysisAbandonKind.Timeout;
        }

        return AnalysisAbandonKind.None;
    }

    /// <summary>
    /// The exception shapes a stop produces, detected structurally (the
    /// <see cref="PgBaselineProvider.IsCommandTimeout"/> discipline — never message matching):
    /// <see cref="OperationCanceledException"/> is the token observed properly;
    /// <see cref="ObjectDisposedException"/> (bare or wrapped one level, Npgsql surfaces both) is
    /// the loop's data source disposed underneath an in-flight read; SQLSTATE 57P01/57P02/57P03
    /// are the postmaster going away server-side — the same trio
    /// <c>PostgresTargetProvider</c> classifies as connection-fatal. A
    /// <see cref="TimeoutException"/> is deliberately NOT residue: a command timeout coinciding
    /// with shutdown still means the query outgrew its deadline, and relabelling it would hide
    /// the growth signal #2294 made visible.
    /// </summary>
    internal static bool IsShutdownResidue(Exception ex) =>
        ex is OperationCanceledException
        || ex is ObjectDisposedException
        || ex.InnerException is ObjectDisposedException
        || ex is PostgresException { SqlState: "57P01" or "57P02" or "57P03" };
}
