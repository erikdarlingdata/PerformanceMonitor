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
/// Classifies whether an analysis-pass failure is the residue of the host shutting down, so the
/// catch sites can tell <i>unfinished because we asked it to stop</i> from <i>unfinished because
/// something broke</i> (#2299). Before this, a clean <c>Stop-Service</c> logged seven ERRORs from
/// work still in flight after "collection loop stopped" — the loop's data source is disposed at
/// method scope exit and the managed postmaster is then <c>pg_ctl stop -m fast</c>-ed, so the
/// abandoned pass's next store read throws <see cref="ObjectDisposedException"/> (or the server
/// kills its open connection with 57P01), and those seven lines were 7 of the day's 9 ERRORs,
/// burying the two that meant something.
/// </summary>
public static class AnalysisShutdown
{
    /// <summary>
    /// True when this failure should be ABANDONED quietly because the host is stopping: the
    /// stopping token has fired AND the exception is a shape shutdown produces. Both halves are
    /// load-bearing — the same exceptions with the token NOT signalled mean a data source was
    /// disposed (or a connection administratively killed) while the service was meant to be
    /// running, which is a real bug whose only evidence is exactly this text, so it must stay
    /// an ERROR. Catch sites use this in a <c>when</c> filter so shutdown residue propagates
    /// (unwinding the pass to one Information line) instead of being swallowed per-metric.
    /// </summary>
    public static bool IsShutdownAbandon(Exception ex, CancellationToken stoppingToken) =>
        stoppingToken.IsCancellationRequested && IsShutdownResidue(ex);

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
