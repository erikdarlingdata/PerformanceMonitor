/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3848: the alert pass retries a store read ONCE, two seconds later, before counting it failed — and the
/// discrimination that makes that safe.
///
/// <para><b>What the retry is for.</b> Every alert-read kill on the largest production store over the week
/// that produced the issue was a sparse transient inside one of the store's own write bands — raw-hypertable
/// compression, the daily-tier materialization's WAL storm, the hourly successor refreshes, timed-checkpoint
/// fsync tails of 8–25 s. Each crossed
/// <see cref="DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds"/>, was recorded by
/// <see cref="AlertReadFailureCounter"/> (#3013), and skipped its condition for that pass — then the next
/// pass 30 s later ran into the same band. The monitoring seat's own retry-once discipline lost no read over
/// the same minutes, which is the measurement this design is taken from.</para>
///
/// <para><b>Why the discrimination is the risky part and gets most of this file.</b> The seam must retry a
/// client-side deadline and nothing else. Three neighbouring shapes must NOT be retried: the store's own
/// <c>statement_timeout</c> (a considered answer — an identical second attempt gets an identical reply, and
/// retrying past a SQLSTATE turns a legible error into a doubled one), the pass's stopping token (a retry
/// that outlives an orderly stop holds a sweep permit against a store the same process is shutting down),
/// and an ordinary server error. The exception TEXT cannot separate the first from a dropped connection —
/// Npgsql renders both as "Exception while reading from stream" with no SQLSTATE, the misdiagnosis #2826
/// exists to prevent — so the shapes are matched on TYPE through the wrapper chain, and the fixtures below
/// are the exact shapes measured off Npgsql 10.0.3 rather than shapes assumed from its documentation.</para>
///
/// <para><b>The measurement, recorded here because the fixtures are only as good as their provenance.</b>
/// Probed on postgres:17-alpine with <c>SELECT pg_sleep(30)</c> at <c>CommandTimeout = 2</c>:</para>
/// <list type="bullet">
/// <item>a client deadline gives <c>NpgsqlException("Exception while reading from stream")</c> over
/// <c>TimeoutException("Timeout during reading attempt")</c>, with no SQLSTATE — identically from
/// <c>ExecuteScalarAsync</c>, from <c>ExecuteReaderAsync</c>, and from <c>ReadAsync</c> mid-stream after
/// rows have already been consumed;</item>
/// <item><c>SET statement_timeout = 1500</c> under <c>CommandTimeout = 60</c> gives a bare
/// <c>PostgresException</c> at SQLSTATE <c>57014</c> with no <c>TimeoutException</c> in the chain at
/// all;</item>
/// <item>a caller-token cancel gives <c>OperationCanceledException</c> wrapping
/// <c>PostgresException</c> 57014 ("canceling statement due to user request"), and a pre-cancelled token a
/// bare <c>OperationCanceledException</c>;</item>
/// <item>a connect timeout gives <c>NpgsqlException("Failed to connect to …")</c> over
/// <c>TimeoutException("Timeout during connection attempt")</c> — inside the predicate, deliberately;</item>
/// <item>and the connection is left <c>State = Open</c> and immediately reusable after every one of the
/// deadline cases, with the backend's statement genuinely cancelled (<c>pg_stat_activity</c> held zero
/// surviving <c>pg_sleep</c> backends one second later) — which is what makes a second attempt on the same
/// pooled connection sound rather than a read against a poisoned socket.</item>
/// </list>
///
/// <para>The fixtures are constructed rather than provoked, because provoking them needs a store that
/// stalls on demand: a pin that requires one gets skipped in CI and stops being evidence. What keeps them
/// honest is that each carries the measured type chain, and
/// <see cref="TheSeamIsFedTheShapesTheRigMeasured"/> asserts the chains are the ones documented above
/// rather than whatever a later edit finds convenient.</para>
/// </summary>
public sealed class AlertReadRetrySeamTests
{
    /// <summary>
    /// A client-side command-deadline expiry, as Npgsql actually renders one: the transport exception over a
    /// <see cref="TimeoutException"/>, no SQLSTATE anywhere.
    /// </summary>
    private static Exception CommandDeadline() =>
        new NpgsqlException(
            "Exception while reading from stream",
            new TimeoutException("Timeout during reading attempt"));

    /// <summary>
    /// The store's OWN cancellation. A <see cref="PostgresException"/> at 57014 with nothing timeout-shaped
    /// in the chain — the backend answering, which the seam must never re-ask.
    /// </summary>
    private static Exception ServerStatementTimeout() =>
        new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");

    private static readonly TimeSpan ExpectedWait =
        TimeSpan.FromSeconds(DarlingAlertReadAdapter.AlertPassRetryDelaySeconds);

    /// <summary>
    /// An adapter over a data source that is never opened, plus the delay recorder the pins assert on.
    ///
    /// <para>The <see cref="NpgsqlDataSource"/> is constructed and never connected to, which is legal: every
    /// pin here drives <see cref="DarlingAlertReadAdapter.ExecuteWithOneRetryAsync"/> with its own fake
    /// read, so no code path touches the store. That is the point of the seam taking the whole read as a
    /// delegate — the retry DECISION is testable without a store, where a decision buried inside fourteen
    /// read bodies would need one that stalls on demand.</para>
    /// </summary>
    private static (DarlingAlertReadAdapter Adapter, AlertReadFailureCounter Counter, List<TimeSpan> Waits)
        Harness(Action? duringWait = null)
    {
        var waits = new List<TimeSpan>();
        var counter = new AlertReadFailureCounter();

        /* The counter is this test's OWN instance, never AlertReadFailureCounter.Shared: the write side is
           injected precisely so a pin cannot pollute the process-global one (the class says so). */
        var adapter = new DarlingAlertReadAdapter(
            NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d"),
            readFailures: counter,
            delay: (wait, token) =>
            {
                waits.Add(wait);

                /* The hook is what lets a pin act DURING the wait — the window the real Task.Delay owns
                   and the one a service stop actually lands in. */
                duringWait?.Invoke();

                /* The token is HONOURED rather than ignored. A fake delay that swallowed cancellation
                   would make the cancelled-during-the-wait pin pass for the wrong reason, and
                   ThrowIfCancellationRequested raises the same OperationCanceledException that the real
                   Task.Delay raises as TaskCanceledException (measured on the rig). */
                token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            });

        return (adapter, counter, waits);
    }

    /// <summary>
    /// (a) A read that crosses the deadline once and then answers: ONE result, counted as a retry, NOT as a
    /// failure — and the seam waited the two seconds before re-asking.
    ///
    /// <para>This is the whole point of the change in one assertion. Before it, this read's first attempt
    /// was the end of the story: the condition was skipped for that pass and the failure count rose. Now the
    /// condition is judged, and the band's cost is a count instead of a blind alert.</para>
    ///
    /// <para>The wait is asserted as a VALUE through the injected delay rather than by sleeping, for the
    /// reason the seam takes the delay at all: a pin that spent two real seconds would be the slowest test
    /// in the project and would still only prove that two seconds passed, not that the seam asked for
    /// them.</para>
    /// </summary>
    [Fact]
    public async Task ARetriedRead_ReturnsItsAnswer_CountsTheRetry_AndWaitsTwoSecondsFirst()
    {
        var (adapter, counter, waits) = Harness();
        var attempts = 0;

        var result = await adapter.ExecuteWithOneRetryAsync(
            _ =>
            {
                attempts++;
                return attempts == 1 ? throw CommandDeadline() : Task.FromResult(41);
            },
            "101",
            "deadlocks",
            CancellationToken.None);

        Assert.Equal(41, result);
        Assert.Equal(2, attempts);

        /* ONE wait, of exactly the constant — so a future edit that retried immediately, or twice, or with
           a different pause, reds here rather than shipping a different policy under the same name. */
        Assert.Equal(new[] { ExpectedWait }, waits.ToArray());

        var reading = counter.ReadFor("101");
        Assert.Equal(1, reading.ServerRetriedReads);
        Assert.Equal(1, reading.InstanceRetriedReads);

        /* And NOT a failure: nothing went blind, so nothing is counted blind — including the trio, which
           must stay null or the surface would date an episode that did not happen. */
        Assert.Equal(0, reading.ServerReadFailures);
        Assert.Equal(0, reading.InstanceReadFailures);
        Assert.Null(reading.LastFailureAtUtc);
        Assert.Null(reading.LastFailureRead);
    }

    /// <summary>
    /// (b) A read that crosses the deadline TWICE: the second failure propagates, and the retry is counted
    /// even though it did not work.
    ///
    /// <para><b>The decision this pin records, because it could defensibly go either way.</b>
    /// <c>RetriedReads</c> counts the attempt, not the success: a read that stalled twice increments the
    /// retry count AND — through the caller's own catch arm, which is where it always lived — today's
    /// failure count. The alternative (count retries only when they succeed) makes the retry figure a
    /// success rate, and it understates the write bands' cost by exactly the episodes where the band was
    /// worst, which is the confident-zero shape #3013 exists to remove. So: both counts move, and the two
    /// together say what happened — a read was re-asked, and twelve seconds later the band was still
    /// on.</para>
    ///
    /// <para>The failure itself is NOT recorded by the seam. It propagates, and the engine's existing
    /// per-condition catch arm records it with its own clock and name exactly as it did before this change
    /// — so one stalled-twice read is one failure, never two, and the elapsed still measures the attempt
    /// that faulted rather than both attempts plus the wait.</para>
    /// </summary>
    [Fact]
    public async Task AReadThatStallsTwice_Propagates_AndStillCountsTheRetryItSpent()
    {
        var (adapter, counter, waits) = Harness();
        var attempts = 0;

        var thrown = await Assert.ThrowsAsync<NpgsqlException>(() =>
            adapter.ExecuteWithOneRetryAsync<int>(
                _ =>
                {
                    attempts++;
                    throw CommandDeadline();
                },
                "101",
                "forced-plan failures",
                CancellationToken.None));

        Assert.Equal(2, attempts);
        Assert.Equal(new[] { ExpectedWait }, waits.ToArray());

        /* The SECOND attempt's exception, not the first's, and not one chained onto the other: two
           different faults in one message column is worse than the one that actually ended the read
           (StoreWriteReattempt's reasoning, verbatim). */
        Assert.IsType<TimeoutException>(thrown.InnerException);

        var reading = counter.ReadFor("101");
        Assert.Equal(1, reading.ServerRetriedReads);

        /* The seam counts no failure — that is the caller's arm, which this pin deliberately does not
           stand in for. A seam that recorded it too would double-count every stalled-twice read. */
        Assert.Equal(0, reading.ServerReadFailures);
    }

    /// <summary>
    /// (c) Every fault that is NOT a client-side deadline propagates on the FIRST attempt, unretried and
    /// unwaited — the half of the design that keeps the retry narrow.
    ///
    /// <para>Theory rather than one case, because each excluded shape is excluded for its own reason and a
    /// single fixture would prove the gate closed for one of them. The store's <c>statement_timeout</c> and
    /// an ordinary server error are the backend ANSWERING (retrying past a SQLSTATE doubles a legible error
    /// into an illegible one). A socket reset and a torn stream are connection faults rather than a read
    /// taking too long: they are inside <see cref="PostgresTransportFault"/>'s predicate and deliberately
    /// outside this one, because the population this retry is sized for is a store that is UP and slow
    /// inside a write band — and a broken connection during an alert pass is a different condition the
    /// count should keep saying out loud. A bare <see cref="NpgsqlException"/> with nothing timeout-shaped
    /// inside it is the control for that boundary: the transport predicate ends in "any NpgsqlException" and
    /// this one must not, or the gate drifts into retrying everything the first time Npgsql adds a
    /// wrapper.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(NonRetryableFaults))]
    public async Task ANonDeadlineFault_PropagatesUnretried(string _, Exception fault)
    {
        var (adapter, counter, waits) = Harness();
        var attempts = 0;

        await Assert.ThrowsAnyAsync<Exception>(() =>
            adapter.ExecuteWithOneRetryAsync<int>(
                _ =>
                {
                    attempts++;
                    throw fault;
                },
                "101",
                "database state",
                CancellationToken.None));

        Assert.Equal(1, attempts);
        Assert.Empty(waits);

        var reading = counter.ReadFor("101");
        Assert.Equal(0, reading.ServerRetriedReads);
        Assert.Equal(0, reading.InstanceRetriedReads);
    }

    public static TheoryData<string, Exception> NonRetryableFaults() => new()
    {
        { "the store's own statement_timeout at 57014", ServerStatementTimeout() },
        {
            "an ordinary server error",
            new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01")
        },
        {
            /* The measured shape of a server reply wrapped in transport machinery: the PostgresException
               test comes first at EVERY level of the chain walk, so this answers false rather than matching
               on the wrapper. */
            "a server reply wrapped in a transport exception",
            new NpgsqlException("Exception while reading from stream", ServerStatementTimeout())
        },
        {
            "a reset socket",
            new NpgsqlException("Exception while reading from stream", new SocketException(104))
        },
        {
            "a torn stream",
            new NpgsqlException("Exception while reading from stream", new IOException("connection lost"))
        },
        { "a bare transport exception with no timeout inside it", new NpgsqlException("broken") },
        { "an unrelated fault", new InvalidOperationException("not a store fault at all") },
    };

    /// <summary>
    /// (d) The pass's stopping token cancelled DURING the two-second wait: an
    /// <see cref="OperationCanceledException"/>, and no second attempt.
    ///
    /// <para>The case the wait creates and the reason it honours the token. A service stopping mid-band
    /// would otherwise spend two seconds per in-flight read waiting to re-ask a store the same process is
    /// shutting down, while holding a fleet-sweep permit — <see cref="StoreWriteReattempt"/>'s reasoning,
    /// and the one case where a second attempt is guaranteed useless.</para>
    ///
    /// <para>The retry is still COUNTED, because it was still spent: the read crossed the deadline and the
    /// seam committed to re-asking before the stop arrived. Counting it only on the far side of the wait
    /// would silently drop every retry a shutdown interrupts, which is the population an operator reading
    /// a restart's counts most wants to see.</para>
    /// </summary>
    [Fact]
    public async Task ATokenCancelledDuringTheWait_Cancels_WithNoSecondAttempt()
    {
        using var stopping = new CancellationTokenSource();

        /* The stop lands strictly INSIDE the wait — after the first attempt's deadline fault has already
           committed the seam to re-asking, and before the second attempt runs. That window is the one the
           real Task.Delay owns and the only one this behaviour is about: a stop arriving BEFORE the first
           attempt faults is the separate case the filter's own passToken guard refuses, pinned by
           ACancelledFirstAttempt_IsNeverRetried_InEitherMeasuredShape. */
        var (adapter, counter, waits) = Harness(duringWait: stopping.Cancel);
        var attempts = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            adapter.ExecuteWithOneRetryAsync<int>(
                _ =>
                {
                    attempts++;
                    throw CommandDeadline();
                },
                "101",
                "poison waits",
                stopping.Token));

        Assert.Equal(1, attempts);

        /* The wait was ASKED FOR and then refused by the token, which is the distinction: a seam that
           checked the token before requesting the delay would also pass "no second attempt" while skipping
           the honouring this pin exists for. */
        Assert.Equal(new[] { ExpectedWait }, waits.ToArray());
        Assert.Equal(1, counter.ReadFor("101").ServerRetriedReads);
    }

    /// <summary>
    /// A read cancelled through the pass token on its FIRST attempt is never a retry candidate at all — the
    /// arm that sits ahead of the filter rather than relying on it to answer false.
    ///
    /// <para>Both measured cancellation shapes, because they differ: a token cancel mid-statement arrives
    /// as <see cref="OperationCanceledException"/> wrapping <see cref="PostgresException"/> 57014, and a
    /// pre-cancelled token as a bare one. The wrapping case is the load-bearing one — its inner chain holds
    /// a SQLSTATE, so a seam that reasoned about the chain first instead of catching the cancellation first
    /// would classify it by its inner exception.</para>
    /// </summary>
    [Fact]
    public async Task ACancelledFirstAttempt_IsNeverRetried_InEitherMeasuredShape()
    {
        foreach (var fault in new Exception[]
        {
            new OperationCanceledException("Query was cancelled", ServerStatementTimeout()),
            new OperationCanceledException(),
        })
        {
            var (adapter, counter, waits) = Harness();
            var attempts = 0;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                adapter.ExecuteWithOneRetryAsync<int>(
                    _ =>
                    {
                        attempts++;
                        throw fault;
                    },
                    "101",
                    "blocking",
                    CancellationToken.None));

            Assert.Equal(1, attempts);
            Assert.Empty(waits);
            Assert.Equal(0, counter.ReadFor("101").ServerRetriedReads);
        }
    }

    /// <summary>
    /// The predicate itself, against the exact type chains the rig measured — so the fixtures above cannot
    /// drift from Npgsql's real behaviour without something saying so.
    ///
    /// <para>Asserted through <see cref="DarlingAlertReadAdapter.IsCommandTimeout"/> directly rather than
    /// only through the seam: the seam's behaviour is what matters, and the predicate is where a careless
    /// edit lands. Both directions, and the connect-timeout case is deliberately on the TRUE side — its
    /// chain is indistinguishable from a command deadline except by message text, which is what #3013
    /// refuses to key on, and a store unreachable inside its connect timeout is the same transient
    /// population worth one more ask.</para>
    /// </summary>
    [Fact]
    public void TheSeamIsFedTheShapesTheRigMeasured()
    {
        /* TRUE: the three measured deadline shapes. */
        Assert.True(DarlingAlertReadAdapter.IsCommandTimeout(CommandDeadline()));
        Assert.True(DarlingAlertReadAdapter.IsCommandTimeout(
            new NpgsqlException(
                "Failed to connect to 10.255.255.1:5432",
                new TimeoutException("Timeout during connection attempt"))));
        Assert.True(DarlingAlertReadAdapter.IsCommandTimeout(
            new TimeoutException("Timeout during reading attempt")));

        /* FALSE: a reply at any depth, a connection fault, and a bare wrapper. */
        Assert.False(DarlingAlertReadAdapter.IsCommandTimeout(ServerStatementTimeout()));
        Assert.False(DarlingAlertReadAdapter.IsCommandTimeout(
            new NpgsqlException("Exception while reading from stream", ServerStatementTimeout())));
        Assert.False(DarlingAlertReadAdapter.IsCommandTimeout(
            new NpgsqlException("Exception while reading from stream", new SocketException(104))));
        Assert.False(DarlingAlertReadAdapter.IsCommandTimeout(new NpgsqlException("broken")));

        /* The ordering property, stated as its own case: a PostgresException OUTSIDE a TimeoutException
           still answers false. Npgsql does not produce this chain today, and that is exactly why it is
           pinned — the walk's correctness must not depend on which order the wrapping happens to take. */
        Assert.False(DarlingAlertReadAdapter.IsCommandTimeout(
            new PostgresException(
                "canceling statement due to statement timeout", "ERROR", "ERROR", "57014")));

        /* And the boundary against the WRITE side's predicate, which is a different question: a reset
           socket is a transport fault and is not a deadline. Asserted as a disagreement rather than
           described, so a future consolidation of the two predicates has to decide this on purpose. */
        var socketReset = new NpgsqlException("Exception while reading from stream", new SocketException(104));
        Assert.True(PostgresTransportFault.IsTransportFault(socketReset));
        Assert.False(DarlingAlertReadAdapter.IsCommandTimeout(socketReset));
    }

    /// <summary>
    /// Every read this adapter serves goes out through the seam, derived from SOURCE rather than listed.
    ///
    /// <para>The claim a behavioural pin cannot make: that the retry covers the WHOLE family rather than
    /// the reads someone remembered to route. Each public <c>Get…Async</c> must be a thin forwarder whose
    /// body calls <c>ExecuteWithOneRetryAsync</c>, with the read body moved to a private
    /// <c>…CoreAsync</c> sibling — so a thirteenth read added in the old shape (public, <c>async</c>,
    /// opening its own connection) fails here on the day it is written rather than shipping unretried and
    /// silently blind, which is how this whole class of gap survives.</para>
    ///
    /// <para>Counted in both directions. Downward catches an extractor that stopped matching — the way a
    /// source-walking guard starts reporting clean on code it no longer reads. Upward puts a person in
    /// front of a new read to decide whether it belongs on this budget.</para>
    /// </summary>
    [Fact]
    public void EveryPublicReadOnTheAdapter_GoesOutThroughTheSeam()
    {
        var source = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs");
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);

        /* Matched on the forwarder shape: `public Task<T> GetXAsync(` — deliberately NOT `public async
           Task<T>`, because the old un-routed shape is exactly that, so the absence of `async` on a public
           read IS the property being asserted. */
        /* The return type is matched permissively rather than as `<[^>]+>`, which was this pin's own first
           bug: every read on this adapter returns a NESTED generic (`Task<List<DeadlockAlertRow>>`), and a
           character class that stops at the first `>` matched only the four whose payload is not itself
           generic — reporting four routed reads out of twelve. A guard that silently sees a third of its
           population is the failure mode this file exists to assert against, found by the count floor
           below doing its job. */
        var forwarders = Regex.Matches(stripped, @"public\s+Task<.+?>\s+(?<name>Get\w+Async)\s*\(")
            .Select(m => m.Groups["name"].Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        /* Twelve public reads across the adapter's nineteen commands: the multi-command reads (database
           state's four maintenance statements ahead of its deviation read, anomalous jobs' freshness probe
           ahead of its page) are ONE read each and retry as a unit, which is sound because every statement
           in them is idempotent by construction. */
        Assert.Equal(12, forwarders.Count);

        /* No public read is left in the old self-executing shape. The control below proves this pattern
           can match, so its silence here is a real absence. */
        var unrouted = Regex.Matches(stripped, @"public\s+async\s+Task<.+?>\s+(?<name>Get\w+Async)\s*\(")
            .Select(m => m.Groups["name"].Value)
            .ToList();

        Assert.True(
            unrouted.Count == 0,
            $"{unrouted.Count} public read(s) on the alert-read adapter execute their own commands instead "
            + "of going out through the #3848 retry seam, so a write-band stall blinds them for a pass: "
            + string.Join(", ", unrouted));

        /* Each forwarder really does call the seam, and really does have a Core sibling holding the read.
           Asserted per read rather than as one count, so a forwarder that was gutted into calling the seam
           with an inline lambda that opens its own connection still reds. */
        foreach (var name in forwarders)
        {
            var core = name[..^5] + "CoreAsync";
            var body = BodyOf(stripped, "public Task<", name);

            Assert.Contains("ExecuteWithOneRetryAsync(", body, StringComparison.Ordinal);
            Assert.Contains($"{core}(", body, StringComparison.Ordinal);

            /* And the Core sibling exists as a PRIVATE async declaration holding the read. Without this
               the forwarder could call a seam wrapped around an inline body that opens its own
               connection, which is the routing this pin claims but would not have checked. */
            Assert.Matches(new Regex(@"private\s+async\s+Task<.+?>\s+" + Regex.Escape(core) + @"\s*\("), stripped);
        }

        /* The read NAMES the seam is given must be the ones the engine's own catch arms record failures
           under, or one read carries two names across the two counts and last_failure_read stops being
           attributable. Derived from both sides rather than listed. */
        var seamNames = Regex.Matches(
                source,
                @"ExecuteWithOneRetryAsync\(\s*ct\s*=>[^;]*?,\s*serverKey,\s*""(?<name>[^""]+)""",
                RegexOptions.Singleline)
            .Select(m => m.Groups["name"].Value)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(12, seamNames.Count);
        Assert.Equal(seamNames.Count, seamNames.Distinct(StringComparer.Ordinal).Count());

        var engine = RepoFile.ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs");
        var engineNames = Regex.Matches(engine, @"RecordReadFailure\([^,]+,\s*""(?<name>[^""]+)""")
            .Select(m => m.Groups["name"].Value)
            .ToHashSet(StringComparer.Ordinal);

        foreach (var name in seamNames)
        {
            Assert.True(
                engineNames.Contains(name),
                $"the retry seam records \"{name}\" but no AlertEngine catch arm records a failure under "
                + "that name, so one read would carry two names across the retry and failure counts");
        }
    }

    /// <summary>The brace-balanced body of the declaration whose signature starts with <paramref name="prefix"/>
    /// and contains <paramref name="name"/> — enough to assert what a forwarder calls.</summary>
    private static string BodyOf(string stripped, string prefix, string name)
    {
        var at = stripped.IndexOf(prefix, StringComparison.Ordinal);
        while (at >= 0)
        {
            var open = stripped.IndexOf('{', at);
            var signature = open > at ? stripped[at..open] : string.Empty;

            if (signature.Contains(name, StringComparison.Ordinal) && open > at)
            {
                return CSharpSourceWalker.BraceBalanced(stripped, open);
            }

            at = stripped.IndexOf(prefix, at + prefix.Length, StringComparison.Ordinal);
        }

        Assert.Fail($"no declaration matching '{prefix}…{name}' in the adapter");
        return string.Empty;
    }

    /// <summary>
    /// The retry pause and the deadline it rides on are both inside the band the pass's arithmetic assumes.
    ///
    /// <para>The sibling of <c>AlertPassCommandTimeoutTests.TheAlertPassDeadline_StaysInsideItsJustifiedBand</c>,
    /// for the number that change introduced. Bounded BELOW because a zero-or-near-zero pause is not a
    /// retry — it re-asks while the write band is still on, spends a second deadline and buys the same
    /// answer, turning one blind condition into a 20-second permit hold. Bounded ABOVE by the arithmetic
    /// that justifies the whole design: two attempts plus the pause must still fit inside the 30 s
    /// <c>s_alertSweepInterval</c> that restarts this pass, or a single stalled read outlives its own
    /// cadence and the "one cycle of delay" cost the deadline's asymmetry is argued from stops being
    /// true.</para>
    /// </summary>
    [Fact]
    public void TheRetryPause_StaysInsideThePassesOwnArithmetic()
    {
        var pause = DarlingAlertReadAdapter.AlertPassRetryDelaySeconds;
        var deadline = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

        Assert.True(
            pause >= 1,
            $"a {pause}s pause re-asks the store while the write band that killed the first attempt is "
            + "still on, which spends a second deadline for the same answer");

        Assert.True(
            (2 * deadline) + pause < 30,
            $"two {deadline}s attempts plus a {pause}s pause is {(2 * deadline) + pause}s, at or past the "
            + "30s alert sweep interval — one stalled read would outlive the cadence that restarts the "
            + "pass, and the deadline's err-short asymmetry is argued from a cost of ONE cycle of delay");
    }
}
