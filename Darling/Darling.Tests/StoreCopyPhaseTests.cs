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
            new Regex(@"var copyPhase\s*=\s*StoreCopyPhase\.Start;[\s\S]{0,600}?BeginBinaryImportAsync\("),
            runner);

        /* The transition sits between Begin and the first row, and nothing sends a row before it. The
           bounded window admits the deadline assignment and the writer hand-off, and would not admit a
           transition moved below the loop. */
        Assert.Matches(
            new Regex(@"BeginBinaryImportAsync\([\s\S]{0,200}?\{\s*copyPhase\s*=\s*StoreCopyPhase\.Data;"),
            runner);

        /* Exactly one transition, and it is to Data. Two would mean a second, unreviewed opinion about
           which phase is in force. */
        Assert.Equal(1, Regex.Matches(runner, @"copyPhase\s*=\s*StoreCopyPhase\.Data;").Count);

        /* And exactly one assignment of Start — the declaration itself, which this pattern matches as a
           substring of `var copyPhase = ...`. SYMMETRIC with the Data count above, and it closes the one
           construction the other four assertions here are all blind to: a bare
           `copyPhase = StoreCopyPhase.Start;` added anywhere below the row loop. That mutation keeps the
           Data count at one, does not move the transition, and is not a Stamp call, so it passes every
           other assertion in this test — while making a post-row-loop fault carry Start.

           Which is not merely a wrong label. StoreWriteReattempt.IsSafeToReattempt re-runs a collector's
           batch on Start, so a fault that had already written rows would be re-attempted: the batch is
           duplicated into aggregates that cannot separate the copies, and every delta is re-derived
           against an already-advanced CollectorDeltaCalculator baseline and committed as a zero. The
           realistic route in is a reset for a second COPY in this method, which is exactly the edit that
           would not touch anything else here. */
        /* Both counts, and the two positional patterns above, spell the assignment `\s*=\s*` rather
           than with literal single spaces: `copyPhase=StoreCopyPhase.Start;` is valid C# and evaded every
           literal form. Low probability — a formatter normalises it and this repo's style is consistent —
           but the cost of closing it is four characters, and a pin that a reformat can slip past is not a
           pin. */
        Assert.Equal(1, Regex.Matches(runner, @"copyPhase\s*=\s*StoreCopyPhase\.Start;").Count);

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

    /* ══ #3111: the axis covers EVERY store COPY, and one consumer routes on that ══════════════════════ */

    /// <summary>
    /// Every binary COPY the service performs stamps the phase — DISCOVERED, not listed (#3111).
    ///
    /// <para><b>Why a scan rather than three more file names.</b> The axis landed on
    /// <c>DarlingCollectorRunner</c> alone while three structurally identical COPYs carried the same
    /// residual untouched, and a pin naming the four would have exactly that failure mode one COPY later:
    /// a fifth site would be absent from the list, absent from the axis, and absent from anything that
    /// reds. So the population is derived from the source — every file whose CODE calls
    /// <c>BeginBinaryImportAsync</c> — and the file list below is compared against what the scan found
    /// rather than trusted as its input.</para>
    ///
    /// <para><b>The set equality is load-bearing for something specific, and it runs BOTH ways.</b>
    /// <see cref="CollectorFaultCopyPhase.IsProvenStoreWrite"/> reads "carries a phase" as "is a store
    /// write", and that is a biconditional: every COPY must stamp, and only a COPY may stamp. A new COPY
    /// site enlarges the population the predicate speaks for; a stamping site OUTSIDE a COPY breaks its
    /// premise outright and hands a target read the store-write label. Both are asserted, against the
    /// discovered sets rather than against a list — see the two set assertions, which is where the second
    /// direction lives, because the per-file loop can only speak about files it visited.</para>
    ///
    /// <para><b>What the stamping detector can and cannot see, stated rather than implied.</b> It matches a
    /// QUALIFIED <c>CollectorFaultCopyPhase.Stamp(</c> in stripped code. Two things are therefore out of its
    /// reach, and only one of them is closed here. A <c>using static</c> on the axis would make
    /// <c>Stamp(</c> legal unqualified: that route is swept for and asserted empty, so the scan reports the
    /// blind spot instead of reading clean past it. An unqualified call from INSIDE
    /// <c>CollectorFaultCopyPhase.cs</c> itself is not covered and is deliberately out of scope — that file
    /// is the axis's own definition rather than a fault path, and it holds the declaration the whole
    /// pattern is named for, so it cannot be distinguished from a call by a text scan. Reflection is not
    /// covered either, and nothing in this repository reaches an internal static that way.</para>
    ///
    /// <para>Read STRIPPED: this family names <c>BeginBinaryImportAsync</c> and both phase values
    /// repeatedly in prose, and <see cref="CollectorFaultCopyPhase"/> and <see cref="StoreWriteReattempt"/>
    /// discuss the call without making it. Read raw, those two would be swept as COPY sites and the
    /// structural assertions below would fail against files that perform no COPY at all.</para>
    /// </summary>
    [Fact]
    public void EveryStoreCopyInTheServiceStampsTheCopyPhase()
    {
        var serviceDirectory = Path.Combine(
            Root, "Darling", "PerformanceMonitor.Darling.Service");

        var sources = Directory
            .EnumerateFiles(serviceDirectory, "*.cs", SearchOption.AllDirectories)
            .Where(f =>
                !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) &&
                !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        /* The floor that stops every assertion below passing on an empty sweep — a wrong directory returns
           no files, and no files satisfy "every COPY site is instrumented" perfectly. */
        Assert.True(
            sources.Count >= 100,
            $"the sweep found only {sources.Count} .cs files under {serviceDirectory} — it is not reading "
            + "the service project, so nothing below is asserting anything about the COPY sites");

        var copySites = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var stampSites = new SortedSet<string>(StringComparer.Ordinal);
        var aliasedAxis = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var file in sources)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(
                File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal));

            if (code.Contains("BeginBinaryImportAsync(", StringComparison.Ordinal))
            {
                copySites.Add(Path.GetFileName(file), code);
            }

            if (code.Contains("CollectorFaultCopyPhase.Stamp(", StringComparison.Ordinal))
            {
                stampSites.Add(Path.GetFileName(file));
            }

            /* The one route the qualified-call test above cannot see: a `using static` on the axis makes
               `Stamp(` legal unqualified, and the fault it stamps would be indistinguishable from a COPY's.
               Collected here so the scan reports its own blind spot rather than reading clean past it. */
            if (Regex.IsMatch(code, @"using\s+static\s+[\w.]*\bCollectorFaultCopyPhase\s*;"))
            {
                aliasedAxis.Add(Path.GetFileName(file));
            }
        }

        Assert.Equal(
            new[]
            {
                "DarlingCollectorRunner.cs",
                "RdsCpuIngestor.cs",
                "RdsDeadlockIngestor.cs",
                "RdsPlanIngestor.cs",
            },
            copySites.Keys.ToArray());

        /* THE CONVERSE, and the half the per-file loop below structurally cannot reach: no file OUTSIDE
           that set stamps a phase at all.

           `IsProvenStoreWrite` reads "carries a phase" as "is a store write", and its soundness is a
           BICONDITIONAL — every COPY stamps, AND only a COPY stamps. The loop below establishes the first
           by visiting the discovered files; nothing in it can say anything about a file it never visited.
           Add `CollectorFaultCopyPhase.Stamp(` to any target-read path and that fault carries a phase, so
           `IsProvenStoreWrite` answers true, so DarlingWorker's PostgreSQL-target arm excludes it — and a
           genuine target read loses the sentence that correctly describes it and is reported as a store
           write. That is the confident-wrong direction that predicate's own remarks argue is the expensive
           one, arrived at from the other side.

           Equality rather than a subset, in one statement rather than two: a COPY site that stopped
           stamping is also a defect, and the per-file assertions below already name it precisely. */
        Assert.Equal(copySites.Keys.ToArray(), stampSites.ToArray());

        /* And the scan's own blind spot, asserted empty rather than assumed so. */
        Assert.True(
            aliasedAxis.Count == 0,
            "these files carry `using static ... CollectorFaultCopyPhase`, which lets Stamp( be called "
            + "unqualified and makes the stamping-site scan above blind to it: "
            + string.Join(", ", aliasedAxis)
            + ". Either drop the using or widen the detector — an unqualified stamp on a target-read path "
            + "is exactly what IsProvenStoreWrite cannot survive.");

        /* Every assertion below carries the FILE NAME in its message. A bare Assert.Matches inside a
           foreach over a discovered population reports which pattern failed and not which member failed
           it, which on a four-file sweep is the difference between a diagnosis and a re-run. */
        foreach (var (name, code) in copySites)
        {
            /* Start is the value in force before the COPY is opened. The comment region between the two is
               whitespace in stripped text, so `\s*` spans it exactly — rather than a character budget a
               longer comment would silently outgrow. */
            Assert.True(
                Regex.IsMatch(code, @"var copyPhase\s*=\s*StoreCopyPhase\.Start;\s*try\s*\{\s*using \(var importer"),
                $"{name}: Start must be the phase in force before the COPY is opened, declared immediately "
                + "ahead of the try that guards it");

            /* The transition sits between Begin and the first row. Bounded so it would not admit one moved
               below the row loop. */
            Assert.True(
                Regex.IsMatch(code, @"BeginBinaryImportAsync\([\s\S]{0,200}?\{\s*copyPhase\s*=\s*StoreCopyPhase\.Data;"),
                $"{name}: the transition to Data must sit inside the importer block and before the first "
                + "row, or a fault that had already sent rows would report the phase a re-attempt trusts");

            /* Exactly one of each, symmetric, for TheCopyWriteStampsTheStartPhaseUntilBeginReturns' reason:
               a bare `copyPhase = StoreCopyPhase.Start;` below the row loop passes every positional
               assertion here while making a fault that had already sent rows read as re-attemptable. */
            Assert.True(
                Regex.Matches(code, @"copyPhase\s*=\s*StoreCopyPhase\.Start;").Count == 1,
                $"{name}: exactly one assignment of Start — the declaration — is allowed; a second one "
                + "anywhere below the row loop is the mutation every other assertion here is blind to");
            Assert.True(
                Regex.Matches(code, @"copyPhase\s*=\s*StoreCopyPhase\.Data;").Count == 1,
                $"{name}: exactly one transition to Data is allowed; two would be a second, unreviewed "
                + "opinion about which phase is in force");

            /* The fault arm stamps whatever phase was live and rethrows BARE, so the type, message and
               inner chain reaching every classification arm upstream are unchanged. */
            Assert.True(
                Regex.IsMatch(
                    code,
                    @"catch \(Exception ex\) when \(ex is not OperationCanceledException\)\s*\{\s*"
                    + @"CollectorFaultCopyPhase\.Stamp\(ex, copyPhase\);\s*throw;\s*\}"),
                $"{name}: the COPY needs the stamp-and-rethrow-bare arm, excluding cancellation — a "
                + "stopping token says the service is shutting down, not which exchange was in flight");

            /* And no site hard-stamps Start. It can only arise from the variable's initial state, which is
               what makes "Start means the importer never came back" a property of the control flow rather
               than of somebody's judgement at a catch site. */
            Assert.DoesNotContain(
                "CollectorFaultCopyPhase.Stamp(ex, StoreCopyPhase.Start)", code, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A proven store write is EXACTLY a fault carrying a phase, and the predicate declines in the
    /// direction that costs least (#3111).
    ///
    /// <para>The <see cref="StoreCopyPhase.Unknown"/> case is the one that matters and it is an
    /// under-claim on purpose: the dimension flush and the transaction commit after the COPY (#1767) are
    /// genuine store writes that carry no stamp, and they read false here. The predicate therefore
    /// separates a population it is CERTAIN about from everything else — which is the direction that leaves
    /// an unproven store write with a vaguer message rather than handing a target-read remedy to something
    /// that never touched the target.</para>
    /// </summary>
    [Fact]
    public void AProvenStoreWriteIsExactlyAFaultCarryingACopyPhase()
    {
        Assert.True(CollectorFaultCopyPhase.IsProvenStoreWrite(StartFault()));
        Assert.True(CollectorFaultCopyPhase.IsProvenStoreWrite(DataFault()));

        /* Unstamped: the post-COPY flush and commit, and every target read. */
        Assert.False(CollectorFaultCopyPhase.IsProvenStoreWrite(
            new NpgsqlException(SharedMessage, new TimeoutException())));
        Assert.False(CollectorFaultCopyPhase.IsProvenStoreWrite(new InvalidOperationException()));
        Assert.False(CollectorFaultCopyPhase.IsProvenStoreWrite(null));

        /* A payload of the wrong type is not a phase, so it cannot buy the routing decision either. */
        var wrongType = new InvalidOperationException(SharedMessage);
        wrongType.Data[CollectorFaultCopyPhase.DataKey] = "Start";
        Assert.False(CollectorFaultCopyPhase.IsProvenStoreWrite(wrongType));
    }

    /// <summary>
    /// The two sentences a PostgreSQL-target <c>CommandTimeout</c> can now receive are DIFFERENT, and the
    /// store-write one carries none of the target-read remedy (#3111).
    ///
    /// <para><b>The defect this asserts against.</b> A store write is Npgsql whatever the monitored
    /// target's engine is, so a COPY timeout on a PostgreSQL target satisfied every term of the
    /// target-timeout arm's filter and received its client-side sentence — whose remedy is
    /// read-specific: <c>Npgsql cancelled the read mid-stream ... shrinking the work is the right one</c>.
    /// Shrinking a read is not the remedy for a COPY blocked on a store-side lock, and an operator acts on
    /// a confident instruction.</para>
    ///
    /// <para>Both halves are asserted, because only the pair is evidence. That the store-write message
    /// omits the read remedy proves nothing on its own — an empty string would satisfy it — so the
    /// target-read sentence is asserted to CONTAIN the phrases the other must not, which is what
    /// establishes that the two populations genuinely diverge rather than that one of them is missing.</para>
    /// </summary>
    [Fact]
    public void AStoreWriteTimeoutIsDescribedAsAStoreWriteAndCarriesNoTargetReadRemedy()
    {
        var storeWrite = CollectorFaultCopyPhase.Describe(StartFault());

        /* Which SIDE failed, first — the thing an operator needs before the rest is actionable. */
        Assert.Contains("store-write", storeWrite, StringComparison.Ordinal);
        Assert.Contains("no rows sent", storeWrite, StringComparison.Ordinal);

        /* And none of the target-read remedy. */
        Assert.DoesNotContain("shrinking the work", storeWrite, StringComparison.Ordinal);
        Assert.DoesNotContain("cancelled the read", storeWrite, StringComparison.Ordinal);

        /* The control: those phrases are real and they are what this fault no longer receives. Without
           this half the assertions above would pass against any string that happened not to say them. */
        var targetRead = DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", "appdb", elapsedMs: 30_000,
            origin: new CollectorFaultCancelOrigin(PostgresCancelSource.OurCommandDeadline, null));

        Assert.Contains("shrinking the work", targetRead, StringComparison.Ordinal);
        Assert.Contains("cancelled the read", targetRead, StringComparison.Ordinal);
        Assert.DoesNotContain("store-write", targetRead, StringComparison.Ordinal);
    }

    /// <summary>
    /// The routing itself, pinned in source: the exclusion is a term of the arm's FILTER, not a branch
    /// inside it (#3111).
    ///
    /// <para>No pure test can reach this — it is a <c>when</c> clause on one <c>catch</c> in a sweep body
    /// that needs a runtime, a store and a live collector — and the distinction it pins is invisible to a
    /// test that could. Inside the arm, a store write would already have been claimed by it: the fault
    /// would not reach the general arm below, so it would get neither the phase named in its
    /// <c>collection_log</c> row nor the reprobe decision that arm makes. Excluding it in the filter is
    /// what makes the fall-through happen, and "the general arm handles it" is the whole design.</para>
    ///
    /// <para>The ordering assertion is separate and cheap: the exclusion has to sit ABOVE the
    /// <c>Classify</c> call, so the arm answers "is this even a target read" before it spends a
    /// classification on it.</para>
    /// </summary>
    [Fact]
    public void TheTargetTimeoutArmExcludesAProvenStoreWriteInItsFilter()
    {
        var worker = ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        /* Exactly one exclusion, so a second arm cannot grow its own opinion about the same question. */
        Assert.Equal(1, Regex.Matches(
            worker, @"&& !CollectorFaultCopyPhase\.IsProvenStoreWrite\(ex\)").Count);

        /* A filter term of the engine-gated arm, above the Classify call, and reached before the arm's
           body — the window admits the intervening comment and would not admit a term moved after the
           classification or a check moved inside the braces. */
        Assert.Matches(
            new Regex(@"Target\.Engine == CollectorTargetEngine\.PostgreSql[\s\S]{0,4000}?"
                      + @"&& !CollectorFaultCopyPhase\.IsProvenStoreWrite\(ex\)[\s\S]{0,200}?"
                      + @"&& PostgresTargetProvider\.Instance\.Classify\("),
            worker);

        /* And it is NOT expressed as a branch inside the arm, which would keep the fault out of the
           general arm and cost it both the phase in its stored row and that arm's reprobe decision. */
        Assert.DoesNotContain(
            "if (CollectorFaultCopyPhase.IsProvenStoreWrite(ex))", worker, StringComparison.Ordinal);
    }
}
