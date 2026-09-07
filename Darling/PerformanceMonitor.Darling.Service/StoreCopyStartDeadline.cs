/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The deadline on a store COPY's START phase — <c>BeginBinaryImportAsync</c>, which sends the
/// <c>COPY</c> and awaits the server's <c>CopyInResponse</c> before any row is written.
///
/// <para><b>Why the start phase needs a deadline of its own.</b> <c>NpgsqlBinaryImporter.Timeout</c>
/// bounds <c>StartRowAsync</c> / <c>Write</c> / <c>CompleteAsync</c> and nothing above them, so it cannot
/// reach the call that returns the importer. The connection's <c>CommandTimeout</c> is what governs that
/// await, and Npgsql 10.0.3 exposes it read-only with no setter and gives
/// <c>BeginBinaryImportAsync</c> exactly one overload — <c>(string, CancellationToken)</c>. The
/// token is therefore the only per-call bound the API offers, and this type is that bound.</para>
///
/// <para><b>The phase this bounds is the one a lock wait stalls in.</b> The backend takes its table locks
/// while executing the <c>COPY</c> statement and sends <c>CopyInResponse</c> only afterwards, so a
/// conflicting lock holds the start phase and not the row loop. Measured against Npgsql 10.0.3 and
/// PostgreSQL 17 with a conflicting <c>ACCESS EXCLUSIVE</c> held: unbounded, the start phase was still
/// blocked past six seconds; with a two-second token deadline it failed at 2.01 s.</para>
///
/// <para><b>Why a token deadline rather than a data source carrying its own
/// <c>CommandTimeout</c>.</b> A connection-string <c>CommandTimeout</c> would bound this await too, but it
/// bounds it the way the defect did — as a default inherited by every command on a borrowed connection,
/// chosen by nobody at the site that runs. It would also add a second pool against the bundled store, its
/// own lifetime, and a decision about which of the sweep body's writes belong to it. The deadline is
/// chosen here instead, one line from the row loop's own, and both come from the same constant.</para>
///
/// <para><b>A breach is re-raised as a <see cref="TimeoutException"/>, and that is the point of
/// <see cref="Breach"/>.</b> Npgsql surfaces a cancelled <c>Begin</c> as an
/// <see cref="OperationCanceledException"/>, which on this path means the service is stopping: the COPY's
/// fault arm excludes it from the phase stamp for that reason, and
/// <see cref="StoreWriteReattempt.RunAsync"/> refuses to re-attempt through one. A deadline breach left in
/// that shape would be bounded and invisible — a stall reported as an orderly stop. A
/// <see cref="TimeoutException"/> is the shape a client-side deadline already has here
/// (<see cref="PostgresTransportFault.IsTransportFault"/> reads it as a transport fault), it is not an
/// <see cref="NpgsqlException"/> so it does not reach <c>DarlingWorker</c>'s PostgreSQL-target timeout arm
/// and its read-specific remedy, and it carries no SQLSTATE so it does not reach the
/// <c>PostgresException</c> arm either. It lands in the general arm, which renders the stamped phase into
/// both the app log and the <c>collection_log</c> row.</para>
///
/// <para>Because it is not a cancellation, the COPY's existing arm stamps it with whatever phase was live
/// — <see cref="StoreCopyPhase.Start"/>, since a breach can only be raised before the transition — and
/// <see cref="StoreWriteReattempt.IsSafeToReattempt"/> then accepts it. That is sound for the same two
/// properties the phase always carried: no row was started, so a <c>COPY ... FROM STDIN</c> cannot have
/// committed, and <c>WritePayload</c> never ran, so no delta baseline moved. It also shortens the worst
/// case rather than lengthening it: the pair of attempts was two inherited 30 s defaults and is now two of
/// this deadline.</para>
/// </summary>
internal sealed class StoreCopyStartDeadline : IDisposable
{
    /// <summary>
    /// The start phase's budget. The SAME constant the row loop's <c>NpgsqlBinaryImporter.Timeout</c>
    /// takes, because it is the same regime — one sweep permit and one borrowed store connection held for
    /// the duration, nothing enclosing it, and a watchdog that only logs. Two phases of one write do not
    /// get two budgets.
    /// </summary>
    internal static readonly TimeSpan Deadline =
        TimeSpan.FromSeconds(ServiceCommandDeadlines.CollectionSweepSeconds);

    /// <summary>
    /// What a breached start phase says, with the deadline it breached. <c>{0}</c> = the deadline in
    /// seconds.
    ///
    /// <para>It names the awaited protocol message and the mechanism, because a reader holding this row
    /// needs to know that the COPY never began rather than that "something timed out" — the two phases of
    /// one COPY have different remedies and, left to Npgsql, the identical
    /// <c>Exception while reading from stream</c> text.</para>
    /// </summary>
    internal const string BreachMessageFormat =
        "the store-write COPY was not acknowledged within {0}s: BeginBinaryImportAsync sends the COPY and "
        + "awaits CopyInResponse, which the collection-sweep deadline bounds through its cancellation token "
        + "because Npgsql exposes the connection's CommandTimeout read-only and offers no per-call "
        + "overload. No rows were sent.";

    /// <summary><see cref="BreachMessageFormat"/> parsed once (CA1863).</summary>
    private static readonly CompositeFormat s_breachMessage = CompositeFormat.Parse(BreachMessageFormat);

    /// <summary>
    /// <see cref="BreachMessageFormat"/> rendered with <see cref="Deadline"/>. Rendered from the deadline
    /// this type actually applies rather than from the constant directly, so the sentence cannot name a
    /// number the code does not use.
    /// </summary>
    internal static readonly string BreachMessage = string.Format(
        CultureInfo.InvariantCulture, s_breachMessage, Deadline.TotalSeconds);

    private readonly CancellationTokenSource _deadline;

    /// <summary>
    /// The caller's own token, kept so <see cref="Breached"/> can tell this deadline's cancellation from
    /// the service stopping. Captured at construction rather than passed back in, so a call site cannot
    /// hand the predicate a token that is not the one this deadline was linked to.
    /// </summary>
    private readonly CancellationToken _caller;

    private StoreCopyStartDeadline(TimeSpan deadline, CancellationToken cancellationToken)
    {
        _caller = cancellationToken;
        _deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _deadline.CancelAfter(deadline);
    }

    /// <summary>
    /// Starts the clock on one COPY's start phase, linked to <paramref name="cancellationToken"/> so a
    /// stopping service still cancels the await.
    ///
    /// <para><paramref name="deadline"/> exists so a test can construct one that has already elapsed;
    /// every production call omits it and takes <see cref="Deadline"/>. A COPY site passing its own value
    /// would decouple the busiest write in the process from the number this regime derived, so no site
    /// does, and a pin asserts that none does.</para>
    /// </summary>
    internal static StoreCopyStartDeadline Start(
        CancellationToken cancellationToken, TimeSpan? deadline = null) =>
        new(deadline ?? Deadline, cancellationToken);

    /// <summary>
    /// The token to hand <c>BeginBinaryImportAsync</c>. It governs the start phase ONLY: Npgsql does not
    /// retain it, so the row loop below runs on the caller's token under the importer's own
    /// <c>Timeout</c> — verified against Npgsql 10.0.3, where a COPY completed normally both after this
    /// object was disposed and after its deadline had elapsed mid-loop.
    /// </summary>
    internal CancellationToken Token => _deadline.Token;

    /// <summary>
    /// True when THIS deadline cancelled the await rather than the service stopping.
    ///
    /// <para>It requires the caller's token to be un-cancelled, so a stop that arrives alongside an
    /// elapsed deadline reads as a stop. That is the direction that costs least: a real stop reported as a
    /// stop loses nothing, whereas a stop re-raised as a store timeout would put a fabricated ERROR row
    /// and a re-attempt against a store the same process is shutting down.</para>
    /// </summary>
    internal bool Breached() =>
        _deadline.IsCancellationRequested && !_caller.IsCancellationRequested;

    /// <summary>
    /// The fault a breached start phase is re-raised as — see the type summary for why it is a
    /// <see cref="TimeoutException"/> and not the <see cref="OperationCanceledException"/> Npgsql threw.
    /// The cancellation is kept as the inner exception: nothing on this path walks the chain for one, and
    /// it is what says which await ran out.
    /// </summary>
    internal static TimeoutException Breach(OperationCanceledException cancellation) =>
        new(BreachMessage, cancellation);

    public void Dispose() => _deadline.Dispose();
}
