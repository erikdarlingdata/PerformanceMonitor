/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins that a collector store-write fault raised in the COPY's START phase is reported distinguishably
/// from one raised in its DATA phase (#3095).
///
/// <para><b>The defect.</b> <c>DarlingCollectorRunner.WriteBatchAsync</c> bounds the data phase with
/// <c>ServiceCommandDeadlines.CollectionSweepSeconds</c> on <c>NpgsqlBinaryImporter.Timeout</c>, and that
/// property does not reach <c>BeginBinaryImportAsync</c> above it — Begin awaits <c>CopyInResponse</c>
/// under the connection's <c>CommandTimeout</c>, which Npgsql 10.0.3 exposes read-only with no per-call
/// overload, so the start phase runs on the undocumented 30 s default. Both phases surface as
/// <c>Exception while reading from stream</c>, so neither the message nor the outermost type separated
/// them, and the start phase is where a lock wait stalls.</para>
///
/// <para><b>What makes this more than a log improvement.</b> A start-phase fault has started no row, so a
/// <c>COPY ... FROM STDIN</c> cannot have committed and no <c>CollectorDeltaCalculator</c> baseline has
/// moved — <c>WritePayload</c> runs in the row loop below Begin. Data-phase faults have neither property.
/// So the phase is what lets a consumer scope a re-attempt to the population where one is safe, and it has
/// to be readable programmatically rather than only out of a sentence.</para>
/// </summary>
public class StoreCopyPhaseTests
{
    /// <summary>
    /// The message BOTH phases produce. Every discrimination test here uses it for both exceptions, so a
    /// test that passes cannot be passing on a difference in text.
    /// </summary>
    private const string SharedMessage = "Exception while reading from stream";

    /// <summary>A start-phase fault as it really arrives: an Npgsql exception wrapping a client-side deadline.</summary>
    private static NpgsqlException StartFault()
    {
        var fault = new NpgsqlException(SharedMessage, new TimeoutException());
        CollectorFaultCopyPhase.Stamp(fault, StoreCopyPhase.Start);
        return fault;
    }

    /// <summary>A data-phase fault, textually identical to <see cref="StartFault"/>.</summary>
    private static NpgsqlException DataFault()
    {
        var fault = new NpgsqlException(SharedMessage, new TimeoutException());
        CollectorFaultCopyPhase.Stamp(fault, StoreCopyPhase.Data);
        return fault;
    }

    /// <summary>
    /// The headline claim, and the assertion this change exists to make true: two faults whose messages
    /// are byte-identical are told apart, by a caller and by a reader.
    ///
    /// <para>The unstamped pair is the control. It establishes that the two exceptions really are
    /// indistinguishable without the stamp — so the discrimination above is coming from the phase and not
    /// from anything incidental to how the fixtures were built. Without that control this test would pass
    /// just as happily against two exceptions that differed for some other reason.</para>
    /// </summary>
    [Fact]
    public void AStartPhaseFaultIsDistinguishableFromADataPhaseFaultWithTheSameMessage()
    {
        var start = StartFault();
        var data = DataFault();

        /* The premise: nothing about the exceptions themselves separates them. */
        Assert.Equal(start.Message, data.Message);
        Assert.Equal(start.GetType(), data.GetType());

        /* Programmatically — this is the half a consumer gates on. */
        Assert.Equal(StoreCopyPhase.Start, CollectorFaultCopyPhase.For(start));
        Assert.Equal(StoreCopyPhase.Data, CollectorFaultCopyPhase.For(data));
        Assert.NotEqual(CollectorFaultCopyPhase.For(start), CollectorFaultCopyPhase.For(data));

        /* And in the reported text — the half an operator reads, in the app log and in collection_log. */
        Assert.NotEqual(CollectorFaultCopyPhase.Describe(start), CollectorFaultCopyPhase.Describe(data));
        Assert.Contains(CollectorFaultCopyPhase.StartPhaseLabel, CollectorFaultCopyPhase.Describe(start), StringComparison.Ordinal);
        Assert.Contains(CollectorFaultCopyPhase.DataPhaseLabel, CollectorFaultCopyPhase.Describe(data), StringComparison.Ordinal);

        /* Neither loses the original message, which is still the thing that says what went wrong. */
        Assert.Contains(SharedMessage, CollectorFaultCopyPhase.Describe(start), StringComparison.Ordinal);
        Assert.Contains(SharedMessage, CollectorFaultCopyPhase.Describe(data), StringComparison.Ordinal);

        /* THE CONTROL: unstamped, the same two exceptions are indistinguishable by every route above. */
        var unstampedStart = new NpgsqlException(SharedMessage, new TimeoutException());
        var unstampedData = new NpgsqlException(SharedMessage, new TimeoutException());

        Assert.Equal(
            CollectorFaultCopyPhase.For(unstampedStart), CollectorFaultCopyPhase.For(unstampedData));
        Assert.Equal(
            CollectorFaultCopyPhase.Describe(unstampedStart), CollectorFaultCopyPhase.Describe(unstampedData));
        Assert.Equal(SharedMessage, CollectorFaultCopyPhase.Describe(unstampedStart));
    }

    /// <summary>
    /// The absence of a stamp is <see cref="StoreCopyPhase.Unknown"/> and must never read as
    /// <see cref="StoreCopyPhase.Start"/>.
    ///
    /// <para>This is the direction that matters, because <c>Start</c> is the value that authorises
    /// something: a consumer scoping a re-attempt to it relies on "no rows sent", and a fault whose phase
    /// was never recorded establishes nothing of the kind. Reading total is what keeps a missing stamp —
    /// a read-only <c>Data</c> bag, a fault from a path with no stamping arm, the post-COPY dimension
    /// flush — costing a diagnostic rather than granting a guarantee.</para>
    /// </summary>
    [Fact]
    public void AnUnstampedFaultReadsAsUnknownAndNeverAsStart()
    {
        Assert.Equal(StoreCopyPhase.Unknown, CollectorFaultCopyPhase.For(new InvalidOperationException()));
        Assert.Equal(StoreCopyPhase.Unknown, CollectorFaultCopyPhase.For(null));

        /* A payload of the wrong type is not a phase. */
        var wrongType = new InvalidOperationException(SharedMessage);
        wrongType.Data[CollectorFaultCopyPhase.DataKey] = "Start";
        Assert.Equal(StoreCopyPhase.Unknown, CollectorFaultCopyPhase.For(wrongType));

        /* Stamping Unknown records nothing rather than recording "Unknown" as a finding. */
        var unknownStamp = new InvalidOperationException(SharedMessage);
        CollectorFaultCopyPhase.Stamp(unknownStamp, StoreCopyPhase.Unknown);
        Assert.Equal(StoreCopyPhase.Unknown, CollectorFaultCopyPhase.For(unknownStamp));

        /* And the message of an unphased fault is returned unchanged — the same instance, so the
           OutOfMemoryException landing pad this feeds allocates nothing on that path. */
        var plain = new InvalidOperationException(SharedMessage);
        Assert.Same(plain.Message, CollectorFaultCopyPhase.Describe(plain));
        Assert.Equal(string.Empty, CollectorFaultCopyPhase.Describe(null));

        /* Zero, so the default of the enum is the one that grants nothing. */
        Assert.Equal(StoreCopyPhase.Unknown, default(StoreCopyPhase));
    }

    /// <summary>
    /// A stamp already present is kept. The innermost frame to catch a fault is the one that knows which
    /// phase raised it, so a wider frame must not be able to relabel a <c>Data</c> fault as <c>Start</c> —
    /// which is the single relabel that would turn an unsafe re-attempt into an authorised one.
    /// </summary>
    [Fact]
    public void AnExistingPhaseIsNotOverwrittenByAWiderFrame()
    {
        var fault = DataFault();

        CollectorFaultCopyPhase.Stamp(fault, StoreCopyPhase.Start);

        Assert.Equal(StoreCopyPhase.Data, CollectorFaultCopyPhase.For(fault));
    }

    /// <summary>
    /// The phase rides on <see cref="Exception.Data"/> and therefore cannot change what the classifier
    /// sees — the same reason <see cref="CollectorFaultDatabase"/> is not a wrapper either.
    ///
    /// <para>Load-bearing beyond tidiness: fault classification on this path keys on the exception's TYPE
    /// and its inner chain, and so does any predicate asking whether a fault is a retryable transport
    /// fault. Wrapping would re-route every one of those in order to improve a sentence. The phase has to
    /// be an INDEPENDENT axis, so that "is this a transport fault" and "was it the start phase" can be
    /// asked of the same exception without either disturbing the other.</para>
    /// </summary>
    [Fact]
    public void StampingThePhaseDoesNotChangeHowTheFaultClassifies()
    {
        var fault = new NpgsqlException(SharedMessage, new TimeoutException());
        var before = PostgresTargetProvider.Instance.Classify(fault, yieldsOnLockTimeout: false);

        CollectorFaultCopyPhase.Stamp(fault, StoreCopyPhase.Start);

        Assert.Equal(before, PostgresTargetProvider.Instance.Classify(fault, yieldsOnLockTimeout: false));
        Assert.Equal(CollectorTargetFault.CommandTimeout, before);
        Assert.IsType<NpgsqlException>(fault);

        /* The type, message and inner chain are all untouched, which is what the arms upstream read. */
        Assert.Equal(SharedMessage, fault.Message);
        Assert.IsType<TimeoutException>(fault.InnerException);
    }

    /// <summary>
    /// The wiring, pinned in source: no pure test can reach <c>WriteBatchAsync</c>'s COPY, because it
    /// needs a live store connection, a definition and a server runtime. What can go wrong here is which
    /// value is live at which point, and every one of those mistakes compiles and runs.
    ///
    /// <para>The ordering assertion is the one that matters. <c>Start</c> must mean strictly "the importer
    /// never came back", so the transition to <c>Data</c> has to sit INSIDE the importer block and BEFORE
    /// the row loop: outside it, a fault that had already sent rows would be reported as the phase a
    /// consumer treats as safe to re-attempt.</para>
    /// </summary>
    [Fact]
    public void TheCopyWriteStampsTheStartPhaseUntilBeginReturns()
    {
        var runner = ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        /* Start is the value in force before the COPY is opened. */
        Assert.Matches(
            new Regex(@"var copyPhase = StoreCopyPhase\.Start;[\s\S]{0,600}?BeginBinaryImportAsync\("),
            runner);

        /* The transition sits between Begin and the first row, and nothing sends a row before it. The
           bounded window admits the deadline assignment and the writer hand-off, and would not admit a
           transition moved below the loop. */
        Assert.Matches(
            new Regex(@"BeginBinaryImportAsync\([\s\S]{0,200}?\{\s*copyPhase = StoreCopyPhase\.Data;"),
            runner);

        /* Exactly one transition, and it is to Data. Two would mean a second, unreviewed opinion about
           which phase is in force. */
        Assert.Equal(1, Regex.Matches(runner, @"copyPhase = StoreCopyPhase\.Data;").Count);

        /* The fault arm stamps whatever phase was live and rethrows BARE — no wrapping, so the type,
           message, stack and inner chain reaching the handlers upstream are unchanged. */
        Assert.Matches(
            new Regex(@"catch \(Exception ex\) when \(ex is not OperationCanceledException\)\s*\{\s*"
                      + @"CollectorFaultCopyPhase\.Stamp\(ex, copyPhase\);\s*throw;\s*\}"),
            runner);

        /* A cancelled sweep is not a COPY phase: it says the service is stopping, not which protocol
           exchange was in flight, and it must not be readable as a recoverable start-phase stall. */
        Assert.DoesNotContain(
            "CollectorFaultCopyPhase.Stamp(ex, StoreCopyPhase.Start)", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reporting site, pinned in source for the same reason: the general fault arm is where a store
    /// COPY fault actually lands, and it must name the phase in BOTH places it reports.
    ///
    /// <para>Which arm it lands in is not obvious and is why this is pinned rather than assumed. The store
    /// connection is Npgsql whatever the monitored target's engine is, so a store-write fault is not a
    /// <c>PostgresException</c> and does not reach the SQLSTATE arm, and it does not satisfy the
    /// PostgreSQL-target timeout arm's engine term either. It falls all the way through to the general
    /// arm, whose message was a bare <c>ex.Message</c>.</para>
    ///
    /// <para>Both call sites are asserted because the <c>collection_log</c> row is the instrument any
    /// measurement of this population reads. Naming the phase only in the app log would leave the stored
    /// rows exactly as ambiguous as they are without this change, which is the half doing the damage.</para>
    /// </summary>
    [Fact]
    public void TheGeneralFaultArmNamesTheCopyPhaseInBothTheLogAndTheStoredRow()
    {
        var worker = ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        /* Composed once, from the helper, inside the general arm. */
        Assert.Equal(1, Regex.Matches(
            worker, @"var message = CollectorFaultCopyPhase\.Describe\(ex\);").Count);

        /* The app log line takes it. */
        Assert.Matches(
            new Regex(@"var message = CollectorFaultCopyPhase\.Describe\(ex\);[\s\S]{0,400}?"
                      + @"=> ERROR: \{Message\}"",\s*\n\s*server\.Config\.DisplayName, collectorName, message\);"),
            worker);

        /* And so does the collection_log row this arm writes — exactly one such row. */
        Assert.Equal(1, Regex.Matches(
            worker,
            @"collectorName, ""ERROR"", 0, runClock\.ElapsedMilliseconds, 0, message, fanout: null").Count);

        /* Neither reverts to the bare message. These are the two expressions this arm carried before, and
           they are what a revert would restore — a revert that compiles, runs, and reports both phases
           identically.

           BOTH detectors are anchored to this arm's own ERROR status rather than to an argument list.
           Several arms in this method pass a bare `ex.Message` legitimately — the XE-capture-down warning
           logs one and its SESSION_MISSING row stores one — so a bare substring would fail on a
           neighbour and read as a defect here. */
        Assert.Equal(0, Regex.Matches(
            worker,
            @"=> ERROR: \{Message\}"",\s*\n\s*server\.Config\.DisplayName, collectorName, ex\.Message\);").Count);
        Assert.Equal(0, Regex.Matches(
            worker,
            @"collectorName, ""ERROR"", 0, runClock\.ElapsedMilliseconds, 0, ex\.Message, fanout: null").Count);
    }

    /// <summary>
    /// The source comment at the COPY's deadline points at the OPEN issue for the residual it describes.
    ///
    /// <para>#2874 closed <c>completed</c> on 2026-09-05 having narrowed this regime rather than closed
    /// it, and the comment recording the residual was the only tracker it had — so the closure left the
    /// residual with no open home, which is why #3095 was filed. A reader who finds the comment first must
    /// not be sent to a closed issue and conclude the start phase was fixed.</para>
    /// </summary>
    [Fact]
    public void TheCopyDeadlineCommentPointsAtTheOpenIssueForTheStartPhase()
    {
        var runner = ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");

        Assert.Contains("The unbounded start phase is tracked on #3095.", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("Tracked as a residual on #2874.", runner, StringComparison.Ordinal);
    }
}
