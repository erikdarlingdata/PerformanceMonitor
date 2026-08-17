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
/// Distinguishes "unfinished because we asked it to stop" from "unfinished because something broke" (#2299).
///
/// <para>A clean service stop used to log seven ERRORs in 800 ms, all AFTER "collection loop stopped": the
/// analysis pass was still computing baselines and filtering findings when the managed postmaster went down
/// (57P01) and the NpgsqlDataSource was disposed underneath it (ObjectDisposedException). Seven of the day's
/// nine ERROR lines were that stop — a 4.5× inflated ERROR count whose dominant cause was "someone restarted
/// the service", burying the two lines that meant something.</para>
///
/// <para>The rule is deliberately TWO-factor. The exception shape alone is not enough — a data source
/// disposed while the service is meant to be running is a genuine bug, and these log lines would be its only
/// evidence, so <see cref="ShouldAbandon"/> demands the stopping token be signalled as well. With the token
/// signalled, the three named shapes (plus the postmaster's admin-shutdown family) are the expected outcome
/// of a stop; without it, every one of them keeps its ERROR.</para>
///
/// <para>The shape list stays CLOSED rather than "any Npgsql error while stopping": a genuine store fault
/// that happens to coincide with a stop should still log as one, so only the shapes a stop itself produces
/// are classified. <c>57014</c> (query_canceled) is deliberately absent — that is the command-timeout
/// signature <c>PgBaselineProvider.IsCommandTimeout</c> reports specially, and it stays an ERROR.</para>
/// </summary>
internal static class ShutdownResidue
{
    /// <summary>
    /// True when <paramref name="ex"/> (or its inner chain) is a shape a service stop produces:
    /// cancellation, a disposed data source, or the managed postmaster going down
    /// (57P01 admin_shutdown / 57P02 crash_shutdown / 57P03 cannot_connect_now — the server_shutdown
    /// class, all three of which a stopping bundled server can surface depending on timing).
    /// </summary>
    internal static bool IsShutdownShaped(Exception? ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is OperationCanceledException or ObjectDisposedException)
            {
                return true;
            }

            if (current is PostgresException { SqlState: "57P01" or "57P02" or "57P03" })
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The two-factor gate: only a shutdown-shaped failure while the stopping token is signalled is
    /// abandoned. Everything else keeps its ERROR — including these exact shapes while the service is
    /// meant to be running, which would be a real bug this log line is the only evidence of.
    /// </summary>
    internal static bool ShouldAbandon(Exception ex, CancellationToken stoppingToken) =>
        stoppingToken.IsCancellationRequested && IsShutdownShaped(ex);

    /// <summary>
    /// Converts the residue into the cancellation the caller already handles, carrying the original
    /// failure as the inner exception — so one INFO line at the top of the pass replaces a burst of
    /// per-component ERRORs, and the real cause is still attached if anyone asks.
    /// </summary>
    internal static OperationCanceledException Abandon(Exception residue, CancellationToken stoppingToken) =>
        new("Analysis abandoned at shutdown.", residue, stoppingToken);
}
