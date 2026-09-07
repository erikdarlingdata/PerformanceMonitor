/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="StoreCopyStartDeadline"/> — the bound on a store COPY's start phase, and the three decisions
/// it owns: the value, whether a cancellation was ITS deadline or the service stopping, and what shape a
/// breach is raised in.
///
/// <para><b>These are pins on the real object, not on source text.</b> The optional deadline parameter
/// exists so a test can construct one that has already elapsed, which is what makes the discrimination
/// table below behavioural rather than a regex over a predicate. It is asserted unused at every production
/// site by <c>CollectionSweepCommandTimeoutTests.EveryCopyWriter_BoundsTheStartPhaseWithTheSharedDeadline</c>.</para>
///
/// <para><b>Cancellation is awaited through a registration rather than slept on.</b>
/// <c>CancelAfter(TimeSpan.Zero)</c> is not synchronous — it schedules — so a test that read
/// <c>Breached()</c> immediately would be racing. A registration fires immediately when the token is
/// already cancelled and on cancellation otherwise, so <see cref="ElapsedAsync"/> is deterministic without
/// naming a duration, and a regression cannot show up as a flake.</para>
///
/// <para>The live end-to-end half — that a real lock wait on a real store really does end at this deadline,
/// with this fault, on the shipped COPY body — is
/// <c>StoreCopyPhaseLivePostgresTests.TheStartPhaseIsBoundedByTheSweepDeadlineNotTheConnectionsCommandTimeout</c>.
/// This class deliberately owns nothing about Npgsql's behaviour.</para>
/// </summary>
public class StoreCopyStartDeadlineTests
{
    /// <summary>
    /// The deadline is the sweep constant, read off the <see cref="TimeSpan"/> that is actually applied.
    ///
    /// <para>The constructed value rather than the digits in the source, because
    /// <c>TimeSpan.FromMilliseconds</c> where <c>FromSeconds</c> was meant is a one-word edit that leaves
    /// every source scan matching while cutting the bound by a factor of a thousand — and a bound that
    /// small would make every store write fail rather than bound a stall.</para>
    /// </summary>
    [Fact]
    public void TheDeadlineIsTheCollectionSweepConstant()
    {
        Assert.Equal(
            (double)ServiceCommandDeadlines.CollectionSweepSeconds,
            StoreCopyStartDeadline.Deadline.TotalSeconds);
    }

    /// <summary>
    /// An elapsed deadline on an un-cancelled caller is a breach — the only case that authorises the
    /// translation.
    /// </summary>
    [Fact]
    public async Task AnElapsedDeadlineOnAnUncancelledCallerIsABreach()
    {
        using var deadline = StoreCopyStartDeadline.Start(CancellationToken.None, TimeSpan.Zero);
        await ElapsedAsync(deadline);

        Assert.True(deadline.Breached());
    }

    /// <summary>
    /// A deadline that has NOT elapsed is not a breach, whatever else is true. The floor case, and without
    /// it a <c>Breached()</c> hard-wired to <c>true</c> would satisfy every other test here.
    /// </summary>
    [Fact]
    public void AnUnelapsedDeadlineIsNotABreach()
    {
        using var deadline = StoreCopyStartDeadline.Start(CancellationToken.None);

        Assert.False(deadline.Breached());
    }

    /// <summary>
    /// A cancelled CALLER is not a breach even though the linked token has fired — and this is the case the
    /// discrimination exists for.
    ///
    /// <para>A linked source cancels when its parent does, so "my token is cancelled" cannot tell a
    /// deadline from a stop on its own. Reading it that way would re-raise an orderly shutdown as a store
    /// timeout: a fabricated ERROR row, and a re-attempt against a store the same process is bringing
    /// down.</para>
    /// </summary>
    [Fact]
    public async Task ACancelledCallerIsNotABreach()
    {
        using var caller = new CancellationTokenSource();
        using var deadline = StoreCopyStartDeadline.Start(caller.Token);

        await caller.CancelAsync();
        await ElapsedAsync(deadline);

        /* The premise: the linked token really did fire, so this is not passing because nothing happened. */
        Assert.True(deadline.Token.IsCancellationRequested);
        Assert.False(deadline.Breached());
    }

    /// <summary>
    /// Both at once reads as a stop, and that is the direction chosen rather than the one that fell out.
    ///
    /// <para>A stop arriving alongside an elapsed deadline is a real race on a shutting-down service. Read
    /// as a stop, a genuine store stall loses its ERROR row during a shutdown that was about to discard the
    /// cycle anyway; read as a breach, a shutdown gains a fabricated store timeout and a re-attempt. The
    /// first costs a sample nobody was going to keep.</para>
    /// </summary>
    [Fact]
    public async Task ADeadlineAndAStopTogetherReadAsAStop()
    {
        using var caller = new CancellationTokenSource();
        using var deadline = StoreCopyStartDeadline.Start(caller.Token, TimeSpan.Zero);

        await ElapsedAsync(deadline);
        await caller.CancelAsync();

        Assert.False(deadline.Breached());
    }

    /// <summary>
    /// The breach fault's shape, and every consequence the COPY path draws from it — asserted together,
    /// because the shape is only interesting for what it causes.
    ///
    /// <para><b>Not an <see cref="OperationCanceledException"/></b> is the load-bearing one. The COPY's
    /// general arm excludes cancellation from the phase stamp and
    /// <see cref="StoreWriteReattempt.RunAsync"/> refuses to re-attempt through one, so a breach left in
    /// Npgsql's own shape would be bounded and invisible — a stall reported as an orderly stop. It is also
    /// not an <see cref="NpgsqlException"/>, which keeps it out of <c>DarlingWorker</c>'s
    /// PostgreSQL-target timeout arm and its read-specific remedy.</para>
    /// </summary>
    [Fact]
    public void ABreachIsATimeoutFaultThatTheReattemptGateAccepts()
    {
        var cancellation = new OperationCanceledException("Query was cancelled");
        var breach = StoreCopyStartDeadline.Breach(cancellation);

        Assert.IsType<TimeoutException>(breach);
        Assert.IsNotAssignableFrom<OperationCanceledException>(breach);
        Assert.IsNotAssignableFrom<NpgsqlException>(breach);

        /* The cancellation is kept as the cause: nothing on this path walks the chain for one, and it is
           what says which await ran out. */
        Assert.Same(cancellation, breach.InnerException);

        /* The message names the deadline it breached, rendered from the TimeSpan the type applies — so it
           cannot go on quoting a number the code stopped using — and says no rows were sent, which is what
           tells a reader why this failure is the recoverable one. */
        Assert.Contains(
            $"within {ServiceCommandDeadlines.CollectionSweepSeconds}s",
            breach.Message,
            StringComparison.Ordinal);
        Assert.Contains("No rows were sent.", breach.Message, StringComparison.Ordinal);

        /* The consequences. Transport, because a client-side deadline is one; and start-phase once the
           COPY's arm stamps the live value, which together are exactly IsSafeToReattempt's conjunction. A
           re-attempt is then exactly-once and delta-safe: no row was started, so a COPY ... FROM STDIN
           cannot have committed, and WritePayload never ran, so no baseline moved. */
        Assert.True(PostgresTransportFault.IsTransportFault(breach));

        CollectorFaultCopyPhase.Stamp(breach, StoreCopyPhase.Start);
        Assert.True(StoreWriteReattempt.IsSafeToReattempt(breach));
    }

    /// <summary>
    /// The token is the deadline's, not the caller's — so handing it to <c>BeginBinaryImportAsync</c>
    /// bounds the await. Distinct tokens is the whole claim: returning the caller's would satisfy every
    /// compile-time check and bound nothing.
    /// </summary>
    [Fact]
    public async Task TheTokenIsTheDeadlinesAndNotTheCallers()
    {
        using var caller = new CancellationTokenSource();
        using var deadline = StoreCopyStartDeadline.Start(caller.Token, TimeSpan.Zero);

        await ElapsedAsync(deadline);

        Assert.True(deadline.Token.IsCancellationRequested);
        Assert.False(caller.IsCancellationRequested);
    }

    /// <summary>
    /// Waits for the deadline's token to be cancelled, through a registration rather than a delay — see the
    /// class summary. A registration on an already-cancelled token runs immediately, so this returns
    /// without yielding in that case rather than waiting for a scheduler.
    ///
    /// <para><b>The wait is BOUNDED, and that is the difference between a pin and a hang.</b> Left
    /// unbounded, the one mutation that stops the deadline being applied at all — dropping
    /// <c>CancelAfter</c> from the constructor — makes every test here wait forever. Measured: the suite
    /// produced no pass, no failure and no message, and never exited, which in CI is a job timeout with
    /// nothing naming the cause. A bounded wait turns that same mutation into a named assertion failure.
    /// The budget is enormous against a timer that should already have fired, so it cannot flake on a
    /// loaded runner.</para>
    /// </summary>
    private static async Task ElapsedAsync(StoreCopyStartDeadline deadline)
    {
        var elapsed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = deadline.Token.Register(() => elapsed.TrySetResult());

        var settled = await Task.WhenAny(elapsed.Task, Task.Delay(TimeSpan.FromSeconds(30)));

        Assert.True(
            settled == elapsed.Task,
            "the start-phase deadline never cancelled its token, so nothing bounds a COPY's start phase — "
            + "the deadline is not reaching the linked source at all. Asserted rather than awaited without "
            + "a limit, because an unbounded await reports this as a hang carrying no message.");
    }
}
