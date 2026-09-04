/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every store read and write the COLLECTION SWEEP performs must carry an explicit command deadline
/// (#2874) — including the binary COPY that is the collector's actual store write, which no pin in this
/// sweep could previously see.
///
/// <para><b>The fourth construction shape.</b> The three shapes the landed pins match are
/// <c>new NpgsqlCommand(</c>, <c>.CreateCommand(</c> and a bare <c>.CreateCommand</c> method group. A
/// binary COPY is none of them: <c>NpgsqlConnection.BeginBinaryImportAsync</c> returns an
/// <see cref="Npgsql.NpgsqlBinaryImporter"/> whose deadline is its own <c>Timeout</c> property — a
/// <c>TimeSpan</c>, not the <c>int</c> seconds of <c>NpgsqlCommand.CommandTimeout</c> — initialised from
/// the connection's <c>CommandTimeout</c> and therefore inheriting the same undocumented 30 s. There is
/// no <c>NpgsqlCommand</c> and no <c>CreateCommand</c> anywhere in the expression, so every regex in
/// #2810, #2871, #2882, #2888, #2901 and #2905 reported this family clean while all four sites sat on
/// the default. <c>.Service</c> is the only project that has any (Storage 0, Viewer 0, Analysis 0), and
/// one of them — <c>DarlingCollectorRunner.WriteBatchAsync</c> — is the store write of every collector,
/// on every server, on every cycle. <see cref="EveryCopyWriter_SetsTheImporterDeadline"/> is the shape
/// guard, and it is deliberately PROJECT-WIDE rather than scoped to the four known files.</para>
///
/// <para><b>The fifth shape: a QUALIFIED type name.</b> <c>new NpgsqlCommand(</c> does not match
/// <c>new Npgsql.NpgsqlCommand(</c>, so the census regex was blind to a construction carrying its
/// namespace, and a site written that way was counted zero times and had its deadline looked at never.
/// The form occurs exactly once in the repo — <c>DarlingWorker.ReadPgStatementTextAsync</c>'s
/// <c>pg_stat_statements</c> text fetch — and that site is a MONITORED-TARGET read rather than a store
/// one, so it is deliberately NOT enlisted below and the widening moved the census by ZERO: the twelve
/// members were re-scanned under both patterns and every one returned the same count. The shape is closed
/// anyway, because declining to close a shape a census demonstrably cannot see is this issue's own
/// mistake in miniature — and closing it is what lets
/// <see cref="EverySweepBodyCommand_IsBuiltAgainstTheStore_NotAMonitoredTarget"/> see that site at all if
/// an edit ever moves it inside a sweep member.</para>
///
/// <para><b>Store and target are separate regimes, and this pin now says which one every site is.</b>
/// <c>.Storage</c> and <c>.Viewer</c> never needed that distinction — those projects only ever talk to
/// the store. <c>.Service</c> talks to monitored servers too, and a target command stamped with
/// <see cref="ServiceCommandDeadlines.CollectionSweepSeconds"/> would look deliberate while bounding the
/// wrong hop: that 10 s is derived from the STORE's measured write latency and the sweep's own connection
/// permit, and a <c>pg_stat_statements</c> read against a monitored PostgreSQL server shares neither
/// input. So the receiver is checked rather than assumed, following the allowlist in
/// <see cref="McpReadCommandTimeoutTests"/>.</para>
///
/// <para><b>Why the command sweep below is scoped by MEMBER, not by file.</b> Every earlier pin in this
/// sweep could glob a directory, because <c>.Storage</c> and <c>.Viewer</c> are Npgsql-store-only
/// projects whose sites all share one budget. <c>.Service</c> is neither. The collection sweep's sites
/// sit in files that ALSO hold other regimes' sites — <c>DarlingObservability</c> carries the per-cycle
/// <c>collection_log</c> insert next to the daily purge's retention row and the analysis pass's state
/// write; <c>DarlingWorker</c> carries the per-collector statement-text refresh next to two command-plane
/// handlers and an alert-pass read. A file-scoped guard here would either fail on sites this group
/// deliberately left alone or, worse, be relaxed until it guarded nothing. Members are the smallest unit
/// that expresses "runs inside the sweep body".</para>
///
/// <para>The VALUE is pinned as a band below, and derived in
/// <see cref="ServiceCommandDeadlines.CollectionSweepSeconds"/> from this regime's own callers: no
/// enclosing budget, a 15 s launch tick, one of <c>max_concurrent_sweeps</c> permits plus one borrowed
/// store connection held for the body's duration, and a 60 s watchdog that only logs. It is derived at
/// the width the knob is MOVING to (8), not the seeded 4.</para>
/// </summary>
public sealed class CollectionSweepCommandTimeoutTests
{
    /// <summary>
    /// The sweep's store-touching members, as (file, member) pairs. Named rather than globbed for the
    /// reason in the class comment, and each one is reachable from <c>ProcessServerSweepAsync</c> on the
    /// sweep's own cadence.
    ///
    /// <para><b>The site that looked like another regime and was not.</b>
    /// <c>DarlingObservability.WriteAnalysisStateAsync</c> was the seventeenth site and it is IN scope,
    /// after review caught it: it looks like the analysis pass's state write and is not one. The pass's
    /// budget is <c>passCts.CancelAfter(s_analysisTimeout)</c>, whose token is threaded ONLY into
    /// <c>analysisService.AnalyzeAsync</c> — every <c>WriteAnalysisStateAsync</c> call takes the plain
    /// <c>stoppingToken</c> instead, and one of them (<c>DarlingWorker.cs:1877</c>, the PostgreSQL-target
    /// tombstone) is called straight from <c>ProcessServerSweepAsync</c> with no pass around it at all.
    /// Being lexically inside a budgeted method is not the same as being under its budget, and that is
    /// the distinction this group's member scoping exists to make rather than to blur.</para>
    ///
    /// <para><b>What is deliberately absent, and why each is a different budget.</b>
    /// <c>DarlingWorker.ReadLatestCpuAsync</c> is reached from <c>EvaluateAlertsAsync</c> and belongs to
    /// #2882's alert pass (10 s, bounded by a 30 s sweep interval) — it is a site that pin's file-scoped
    /// list missed, not a member of this regime. <c>ReadStoreSizeBytesAsync</c> runs on the disk-check
    /// cadence. <c>RunTestHypotheticalIndexAsync</c> / <c>RunExecuteActualPlanAsync</c> and
    /// <c>DarlingCommandExecutor</c> are the command plane, with a 5-minute claim lease and no heartbeat.
    /// <c>DarlingDeltaCalculator</c>'s four seeds and <c>StoreConfigProvider</c>'s seven seeding sites run
    /// ONCE at startup. <c>StoreConfigProvider.ReadConfigVersionAsync</c> is the 15 s reload beacon and is
    /// the closest call of all — it runs on the sweep's own tick — but it runs on the SERIAL loop thread
    /// ahead of every launch, so its blast radius is the whole fleet and its floor is a single-row lookup
    /// rather than a batch write. Different bounds on both sides; left with the startup group.</para>
    ///
    /// <para><b>The target read inside an enlisted member's own call graph.</b>
    /// <c>DarlingWorker.ReadPgStatementTextAsync</c> is called by <c>TryRefreshPgStatementTextAsync</c>,
    /// which IS enlisted, on the sweep's own cadence — and it is still out of scope, because scope here is
    /// the STORE budget and that method opens an <c>NpgsqlConnection</c> on
    /// <c>runtime.ConnectionString</c>: a monitored PostgreSQL target. Its 60 s is not a bespoke bound
    /// either. <c>MonitoredServerConnection.BuildConnectionString</c> puts <c>CommandTimeout = 60</c> on
    /// every monitored-PostgreSQL connection string, and Npgsql initialises a command's deadline from its
    /// connection's, so the site RESTATES the target-wide value it would have inherited. Nor is it a lone
    /// literal: <c>CommandTimeout = 60</c> appears 26 times in this project — 23 target commands in
    /// <c>DarlingXeSessions</c>, both of <c>MonitoredServerConnection</c>'s connection-string builders,
    /// and this site. The target commands that do NOT take 60 took a different number for a reason of
    /// their own (<c>DarlingServerConnector</c>'s probes, at 15 s and 30 s), which is what a per-site
    /// target bound looks like here when one is actually warranted.
    /// Enlisting it would replace that with a 10 s store bound: a 6x tightening of a read whose cost is the
    /// monitored server's, argued from latency measured on the store. Giving it a target-side constant of
    /// its own would invent a one-member regime while the number's actual home is the connection-string
    /// builder. It is left exactly as it is, and
    /// <see cref="EverySweepBodyCommand_IsBuiltAgainstTheStore_NotAMonitoredTarget"/> is what stops it — or
    /// anything shaped like it — from arriving in this budget by adjacency.</para>
    /// </summary>
    private static readonly (string File, string Member)[] s_sweepMembers =
    {
        ("DarlingCollectorRunner.cs", "GetCollectorStateAsync"),
        ("DarlingCollectorRunner.cs", "SaveCollectorStateAsync"),
        ("DarlingCollectorRunner.cs", "DeleteCollectorStateKeyAsync"),
        ("DarlingCollectorRunner.cs", "PruneOrphanedQueryStoreDatabaseStateAsync"),
        ("DarlingCollectorRunner.cs", "PruneForeignQueryStoreDatabaseStateAsync"),
        ("DarlingCollectorRunner.cs", "HasPriorCollectorSuccessAsync"),
        ("DarlingWorker.cs", "TryRefreshPgStatementTextAsync"),
        ("DarlingWorker.cs", "ReadCollectorWatermarksAsync"),
        ("DarlingObservability.cs", "UpsertServerAsync"),
        ("DarlingObservability.cs", "LogCollectionAsync"),
        ("DarlingObservability.cs", "WriteAnalysisStateAsync"),
        ("Targets/RdsCpuIngestor.cs", "GetWatermarkAsync"),
    };

    /// <summary>
    /// The sweep's command sites, counted so a member that stops creating commands fails loudly.
    ///
    /// <para>RE-DERIVED rather than adjusted when <see cref="s_commandCtor"/> grew the qualified shape.
    /// Each of the twelve members was re-scanned under the old pattern and the new one and every member
    /// returned the same count, so the widening moved this number by zero — the qualified construction the
    /// widening exists for is not inside any of them. Ten sites are
    /// <c>new NpgsqlCommand(sql, connection)</c> on a connection borrowed from the store pool; three are
    /// <c>_postgres.CreateCommand(</c> on the data source itself.</para>
    /// </summary>
    private const int ExpectedSweepCommandSites = 13;

    /// <summary>The four COPY writers, counted for the same reason.</summary>
    private const int ExpectedCopyWriterSites = 4;

    /// <summary>
    /// Every command shape the landed pins know about, with the type name allowed to carry its namespace:
    /// <c>new NpgsqlCommand(</c> or <c>new Npgsql.NpgsqlCommand(</c>, and the factory either called
    /// (<c>.CreateCommand(</c>) or handed over as a bare method group. The qualified alternative is the
    /// widening #2938 adopted for the storage and viewer pins.
    ///
    /// <para>The BARE METHOD GROUP is kept rather than split out, even though #2895's census found ZERO of
    /// it in this project and the one apparent hit was the phrase in running prose inside a <c>/* */</c>
    /// block — "absent today" is not a guard. It stays folded into the census, rather than getting the
    /// separate must-be-zero assertion <see cref="McpReadCommandTimeoutTests"/> gives it, because a handoff
    /// appearing inside one of these twelve members has to fail LOUDLY and folded in it fails twice over:
    /// the deadline scan cannot find a <c>CommandTimeout</c> on a delegate nobody has invoked yet, and the
    /// count moves. Splitting it would change what the census counts, which is the one thing to avoid doing
    /// to a counted census casually.</para>
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new (?:Npgsql\.)?NpgsqlCommand\s*\(|\.CreateCommand\s*\(|\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The deadline, judged over STRIPPED source like the construction scan above it — the two halves of
    /// one question should not disagree about what counts as code.
    ///
    /// <para>Stripping the VALUE span is not unconditionally right for every pin in this family: one whose
    /// value regex has to see inside a string LITERAL would be broken by it. It is right for this one
    /// because the pattern targets an assignment in code, and the failure it closes is a false CLEAN — a
    /// comment in the two-statement window spelling <c>command.CommandTimeout =</c> would certify an
    /// untimed site, at a line where no edit could ever fix it.</para>
    /// </summary>
    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The store's command FACTORY, as the receiver reads immediately behind a <c>CreateCommand</c>:
    /// <c>_postgres</c> is the worker's and the ingestor's field, <c>postgres</c> the
    /// <c>NpgsqlDataSource</c> the static helpers take as a parameter. The optional <c>!</c> is the
    /// null-forgiving operator the worker writes on its own field.
    ///
    /// <para><b>Word-boundaried, and the boundary guards the direction that matters.</b> Without the
    /// lookbehind, <c>sourcepostgres.CreateCommand(</c> matches <c>postgres\.CreateCommand</c> as a
    /// substring, so a monitored-target command on any variable whose name ENDS in the receiver's name
    /// would be waved through as a store command — a false ACCEPT in exactly the guard that exists to stop
    /// one. The mirror case is safe by construction: an unrecognised receiver fails asking for a
    /// decision.</para>
    ///
    /// <para><b>Anchored at the END</b> of the line-to-match window, so a store receiver earlier on the
    /// same line cannot vouch for a target construction after it — the two-construction fixture in
    /// <see cref="TheReceiverClassifier_TellsAStoreCommandFromAMonitoredTargetOne"/> is that control.</para>
    /// </summary>
    private static readonly Regex s_storeCommandFactory = new(
        @"(?<![A-Za-z0-9_])_?postgres!?\s*\.CreateCommand\s*[(,);]\z",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A command constructed against a <c>connection</c> — which is a STORE connection only when the
    /// member also borrowed one from the store pool (<see cref="s_storeConnectionOpen"/>), because the
    /// name is equally what a monitored target's connection is called.
    ///
    /// <para>The pre-comma span admits no parenthesis and no semicolon on purpose. Over stripped source
    /// the embedded SQL contributes neither, so this cannot match across arguments or across statements —
    /// and a future <c>new NpgsqlCommand(BuildSql(kind), connection)</c> would be REJECTED and have to be
    /// looked at, which is the false-positive direction and the one to err in.</para>
    /// </summary>
    private static readonly Regex s_storeConnectionArgument = new(
        @"new (?:Npgsql\.)?NpgsqlCommand\s*\([^;()]*,\s*(?<![A-Za-z0-9_])connection\s*\)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The member borrowing a connection from the store pool. Asked per MEMBER rather than per file —
    /// which the member scoping makes possible and the reference pin could not do — so a store open
    /// elsewhere in the same file cannot vouch for a target command here. That is not hypothetical:
    /// <c>DarlingWorker</c> holds both kinds.
    /// </summary>
    private static readonly Regex s_storeConnectionOpen = new(
        @"(?<![A-Za-z0-9_])connection\s*=\s*await\s+_?postgres!?\s*\.OpenConnectionAsync\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The fourth shape. No <c>NpgsqlCommand</c>, no <c>CreateCommand</c>, so nothing above sees it.</summary>
    private static readonly Regex s_copyWriter = new(
        @"BeginBinaryImportAsync\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// <c>NpgsqlBinaryImporter.Timeout</c>, matched on the property rather than on a variable name so a
    /// site that calls its importer something other than <c>importer</c> still counts.
    ///
    /// <para>Judged over STRIPPED source, for the reason on <see cref="s_setsTimeout"/> and with the same
    /// justification: it targets code, not a literal. The leading dot is what made this one's
    /// comment-immunity fixture pass for the wrong reason until the fixture was corrected — see
    /// <see cref="TheCopyScanner_SeesTheImporterDeadlineWithoutBorrowingANeighbours"/>.</para>
    /// </summary>
    private static readonly Regex s_setsImporterTimeout = new(
        @"\.Timeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EverySweepBodyCommand_SetsTheCollectionSweepDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var (file, member) in s_sweepMembers)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var body = MemberBody(text, member, path);

            /* ONE stripped body, for the shape scan AND the value scan. Matching the construction over
               stripped source while judging its deadline over RAW source is an asymmetry, and it fails in
               the direction that matters: a comment inside the window that happens to spell
               `command.CommandTimeout =` makes an untimed site read clean, and no edit at that line could
               ever fix it. Stripping is length-preserving, so the offsets stay character-aligned. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(body);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                total++;

                /* Two statements, for the reason #2810's pin records: the CreateCommand shape's method
                   result cannot take an object initializer, so its deadline is the statement AFTER the
                   construction. The span is walked literal- and comment-aware, which is load-bearing
                   rather than defensive here — these members embed verbatim SQL carrying both semicolons
                   and quote characters. */
                if (!SetsTheSweepDeadline(code, ctor.Index))
                {
                    offenders.Add($"{file} {member} +{LineOf(code, ctor.Index)}");
                }
            }
        }

        /* Offenders BEFORE the census, so a real defect reports as a defect. Reversed, an added-and-untimed
           site would fail on the count and say nothing about the deadline it is missing. */
        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} collection-sweep command(s) inherit Npgsql's 30s default instead of setting "
            + $"{nameof(ServiceCommandDeadlines)}.{nameof(ServiceCommandDeadlines.CollectionSweepSeconds)}: "
            + string.Join(", ", offenders));

        /* The census is a tripwire in BOTH directions. Downward it catches a member that stopped creating
           commands, or an extractor that silently returned an empty body — the way a source-walking guard
           starts reporting clean on code it no longer reads. Upward it forces a human to decide whether a
           newly added site really belongs to this budget, rather than inheriting the answer by adjacency. */
        Assert.Equal(ExpectedSweepCommandSites, total);
    }

    /// <summary>
    /// Every command the census counts is built against the STORE, so the store-derived
    /// <see cref="ServiceCommandDeadlines.CollectionSweepSeconds"/> is the right bound for all of them.
    ///
    /// <para>This is the assertion <c>.Storage</c>'s and <c>.Viewer</c>'s pins had no need for and this
    /// group does: those projects only ever talk to the store, and <c>.Service</c> talks to monitored
    /// servers from the same files. The reference is
    /// <c>McpReadCommandTimeoutTests.EveryCommandInScope_IsBuiltAgainstTheStore_NotAMonitoredTarget</c>, and
    /// the classification is the RECEIVER — checked rather than assumed, so a target command that arrives
    /// in one of these twelve members fails asking for a decision instead of inheriting a store bound that
    /// would look deliberate while bounding the wrong hop.</para>
    ///
    /// <para><b>The store count is asserted as well</b>, so this cannot pass by classifying nothing. A
    /// negative assertion over a scan that read an empty body is satisfied vacuously — the failure
    /// direction every pin in this sweep has had to be defended against — and a store count of zero
    /// against thirteen expected sites is what catches it.</para>
    /// </summary>
    [Fact]
    public void EverySweepBodyCommand_IsBuiltAgainstTheStore_NotAMonitoredTarget()
    {
        var offenders = new List<string>();
        var store = 0;

        foreach (var (file, member) in s_sweepMembers)
        {
            var path = SourcePath(file);
            var body = MemberBody(File.ReadAllText(path), member, path);
            var code = CSharpSourceWalker.StripCommentsAndStrings(body);
            var borrowsFromStore = s_storeConnectionOpen.IsMatch(code);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                if (IsStoreCommand(code, ctor, borrowsFromStore))
                {
                    store++;
                    continue;
                }

                offenders.Add($"{file} {member} +{LineOf(code, ctor.Index)}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} collection-sweep command(s) have an unrecognised receiver, so it is not "
            + "established whether they address the STORE or a MONITORED TARGET — "
            + $"{nameof(ServiceCommandDeadlines)}.{nameof(ServiceCommandDeadlines.CollectionSweepSeconds)} "
            + "is derived from the store's measured write latency and this sweep's connection permit, and a "
            + "target command carrying it would bound the wrong hop (the collector runner threads a target "
            + "timeout as a parameter; the pg_stat_statements text fetch takes the 60s its monitored-target "
            + "connection string already carries): "
            + string.Join(", ", offenders));

        Assert.Equal(ExpectedSweepCommandSites, store);
    }

    /// <summary>
    /// The fourth shape, swept over the WHOLE project rather than the four files that have it today.
    ///
    /// <para>Scoped that way deliberately: the defect this pin exists for is that a COPY writer is
    /// invisible to a command-shaped regex, so the failure mode to guard is a FIFTH one being added
    /// somewhere else and inheriting the default in silence. A list of four filenames would not catch
    /// that, which is the #2786 shape — a guard that names the arm it was written for.</para>
    /// </summary>
    [Fact]
    public void EveryCopyWriter_SetsTheImporterDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in ServiceSources())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

            foreach (Match copy in s_copyWriter.Matches(code))
            {
                total++;

                var span = CSharpSourceWalker.StatementSpanFrom(code, copy.Index, statements: 2);

                if (!s_setsImporterTimeout.IsMatch(span))
                {
                    offenders.Add($"{Path.GetFileName(path)}:{LineOf(code, copy.Index)}");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} binary COPY writer(s) leave NpgsqlBinaryImporter.Timeout unset, so the COPY "
            + "inherits the connection's 30s CommandTimeout — a deadline nobody chose, surfacing as "
            + $"\"Exception while reading from stream\": {string.Join(", ", offenders)}");

        /* A FIFTH COPY writer fails here even when it is correctly timed, and that is the point: this shape
           was invisible to six landed pins, so a new one must be looked at by a person deciding whether it
           shares this budget — not waved through because it happened to set the property. */
        Assert.Equal(ExpectedCopyWriterSites, total);
    }

    /// <summary>
    /// The COPY writers take the SAME constant as the commands beside them, because they are the same
    /// regime — and pinned RELATIONALLY rather than by value so the two can never drift apart. A bare
    /// <c>TimeSpan.FromSeconds(10)</c> at a COPY site would satisfy the shape guard above while quietly
    /// decoupling the busiest write in the process from the number this group derived.
    /// </summary>
    [Fact]
    public void TheCopyWriters_TakeTheSweepConstantRatherThanALiteral()
    {
        var relational = new Regex(
            @"\.Timeout\s*=\s*TimeSpan\.FromSeconds\(\s*ServiceCommandDeadlines\.CollectionSweepSeconds\s*\)",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        var bound = ServiceSources()
            .Select(p => CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(p)))
            .Sum(t => relational.Matches(t).Count);

        Assert.Equal(ExpectedCopyWriterSites, bound);
    }

    /// <summary>
    /// The value, bounded on both sides — see
    /// <see cref="ServiceCommandDeadlines.CollectionSweepSeconds"/> for the derivation. A band rather
    /// than an equality, following the precedent .Storage and .Viewer set: freezing the exact number
    /// makes every future re-derivation a test edit, and the claim being defended is that it sits between
    /// two measured bounds.
    /// </summary>
    [Fact]
    public void TheCollectionSweepDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.CollectionSweepSeconds;

        Assert.True(
            seconds >= 5,
            $"collection-sweep deadline {seconds}s leaves no headroom over the measured worst case — a "
            + "1.53s store write on the busiest server, projected to ~3.1s at max_concurrent_sweeps=8, "
            + "plus 673-893ms of store connection acquisition (#2819)");

        Assert.True(
            seconds < 30,
            $"collection-sweep deadline {seconds}s is at or above Npgsql's inherited 30s default, so it "
            + "buys nothing: nothing encloses the sweep body, and the 60s watchdog only logs");

        Assert.True(
            seconds < DarlingWorker.SweepWatchdogSeconds / 4,
            $"collection-sweep deadline {seconds}s is too close to the {DarlingWorker.SweepWatchdogSeconds}s "
            + "watchdog: real cycles run 13-48s, so one blown deadline on a heavy cycle would cross the "
            + "threshold and report a merely-slow body as a hang — the #1581/#2170 warning herd");
    }

    /// <summary>
    /// The scanners are what sixteen sites' correctness is asserted through, so their own edges are
    /// pinned: a false positive fails a green build on correct code, and a false NEGATIVE reports success
    /// on the defect.
    ///
    /// <para>Every fixture is a shape that occurs. The <c>using (...)</c> block form is how all four COPY
    /// writers are actually written, and its brace arithmetic is the non-obvious case — the span walker
    /// enters at the importer call already inside the <c>using</c>'s parenthesis, so the depth counter
    /// goes NEGATIVE closing it before the block's brace brings it back. The last fixture is the one that
    /// matters most: an untimed COPY whose method is followed by a timed one must NOT borrow its
    /// neighbour's deadline.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "using (var importer = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    importer.Timeout = TimeSpan.FromSeconds(10);\n"
        + "    writer.Importer = importer;\n"
        + "}\n",
        true)]
    [InlineData(
        "await using var importer = await conn.BeginBinaryImportAsync(Copy, ct);\n"
        + "importer.Timeout = TimeSpan.FromSeconds(10);\n",
        true)]
    [InlineData(
        "using (var importer = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    /* no deadline here: importer.Timeout = TimeSpan.FromSeconds(n) is set by the caller. */\n"
        + "    writer.Importer = importer;\n"
        + "}\n",
        false)]
    [InlineData(
        "using (var importer = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    writer.Importer = importer;\n"
        + "}\n"
        + "await Other();\n"
        + "using (var second = await conn.BeginBinaryImportAsync(Copy, ct))\n"
        + "{\n"
        + "    second.Timeout = TimeSpan.FromSeconds(10);\n"
        + "}\n",
        false)]
    public void TheCopyScanner_SeesTheImporterDeadlineWithoutBorrowingANeighbours(string source, bool expectedTimed)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var copy = s_copyWriter.Match(code);
        Assert.True(copy.Success, "the fixture did not contain a COPY writer");

        var span = CSharpSourceWalker.StatementSpanFrom(code, copy.Index, statements: 2);

        Assert.Equal(expectedTimed, s_setsImporterTimeout.IsMatch(span));
    }

    /// <summary>
    /// The deadline scan reads CODE, not prose — the positive control for judging the value span over
    /// stripped source, and the command half of the fixture
    /// <see cref="TheCopyScanner_SeesTheImporterDeadlineWithoutBorrowingANeighbours"/> is for the COPY
    /// half. Without it the fix to <see cref="s_setsTimeout"/> would have no witness at all.
    ///
    /// <para>The first two fixtures are the failure being closed, and the comment is written the way
    /// someone documenting the absence of a deadline actually writes it — quoting the assignment. Over raw
    /// source both read TIMED, certifying an untimed site at a line no edit could fix. The last fixture is
    /// the literal direction: SQL is not code either.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection);\n"
        + "/* no deadline: command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds is set\n"
        + "   by the caller instead. */\n"
        + "command.Parameters.AddWithValue(id);\n",
        false)]
    [InlineData(
        "// command.CommandTimeout = 10 was removed here.\n"
        + "await using var command = _postgres!.CreateCommand(Sql);\n"
        + "command.Parameters.AddWithValue(id);\n",
        false)]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection);\n"
        + "command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;\n",
        true)]
    [InlineData(
        "await using var command = new Npgsql.NpgsqlCommand(Sql, connection) { CommandTimeout = 60 };\n",
        true)]
    [InlineData(
        "using var command = new NpgsqlCommand(\"SELECT 1 -- CommandTimeout = 10\", connection);\n"
        + "command.Parameters.AddWithValue(id);\n",
        false)]
    public void TheDeadlineScanner_ReadsCodeNotProse(string source, bool expectedTimed)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var ctor = s_commandCtor.Match(code);

        Assert.True(ctor.Success, "the fixture did not contain a command construction");
        Assert.Equal(expectedTimed, SetsTheSweepDeadline(code, ctor.Index));
    }

    /// <summary>
    /// Whether the construction at <paramref name="at"/> carries a deadline, over the TWO statements from
    /// it — the <c>CreateCommand</c> shape's result cannot take an object initializer, so its deadline is
    /// the statement after the construction. <paramref name="code"/> is stripped source, so a
    /// <c>CommandTimeout =</c> written in a comment or inside the member's verbatim SQL cannot certify an
    /// untimed site.
    /// </summary>
    private static bool SetsTheSweepDeadline(string code, int at)
        => s_setsTimeout.IsMatch(CSharpSourceWalker.StatementSpanFrom(code, at, statements: 2));

    /// <summary>
    /// The member extractor, pinned separately. It is the piece with no precedent in this sweep — every
    /// earlier pin globbed a directory — and a body that resolves to the WRONG span, or to an empty one,
    /// would report clean on whatever it failed to read.
    /// </summary>
    [Fact]
    public void TheMemberExtractor_ReturnsOnlyTheNamedMembersBody()
    {
        const string Source =
            "class C\n"
            + "{\n"
            + "    void Before() { var a = new NpgsqlCommand(X); }\n"
            + "    async Task TargetAsync(int id)\n"
            + "    {\n"
            + "        var command = new NpgsqlCommand(Y);\n"
            + "        if (id > 0) { command.CommandTimeout = 10; }\n"
            + "    }\n"
            + "    void After() { var b = new NpgsqlCommand(Z); }\n"
            + "}\n";

        var body = MemberBody(Source, "TargetAsync", "fixture");

        Assert.Contains("new NpgsqlCommand(Y)", body);
        Assert.DoesNotContain("new NpgsqlCommand(X)", body);
        Assert.DoesNotContain("new NpgsqlCommand(Z)", body);
        Assert.Single(s_commandCtor.Matches(body));
    }

    /// <summary>
    /// The census regex per SHAPE — the positive control for the widening, and for the decision to read
    /// stripped source rather than raw.
    ///
    /// <para>The qualified fixture is the one that fails on the pre-widening pattern: a scan for
    /// <c>new NpgsqlCommand(</c> does not match <c>new Npgsql.NpgsqlCommand(</c>, so a construction written
    /// that way was invisible to the census and to the deadline check built on it. The last two fixtures
    /// are the other direction — <c>NpgsqlConnection</c> is not a command, and a construction quoted in
    /// PROSE is not a site. This repo's comments quote code constantly, and a pin that scanned raw text
    /// would report a phantom offender at a line no edit could fix.</para>
    /// </summary>
    [Theory]
    [InlineData("        using var command = new NpgsqlCommand(Sql, connection);\n", 1)]
    [InlineData("        await using var command = new Npgsql.NpgsqlCommand(Sql, connection);\n", 1)]
    [InlineData("        await using var command = _postgres!.CreateCommand(Sql);\n", 1)]
    [InlineData("        Append(postgres.CreateCommand, rows);\n", 1)]
    [InlineData("        await using var connection = new NpgsqlConnection(runtime.ConnectionString);\n", 0)]
    [InlineData("        /* a comment naming new Npgsql.NpgsqlCommand( is not a construction. */\n", 0)]
    public void TheCensusRegex_SeesEveryConstructionShape_TheQualifiedTypeNameIncluded(string source, int expected)
        => Assert.Equal(expected, s_commandCtor.Matches(CSharpSourceWalker.StripCommentsAndStrings(source)).Count);

    /// <summary>
    /// The receiver classifier, positively and negatively controlled through the identical code path the
    /// sweep uses — <see cref="IsStoreCommand"/> — so neither direction can pass by matching nothing.
    ///
    /// <para>The negatives are what a missing word boundary or an unanchored window would turn into false
    /// ACCEPTS, which is the dangerous direction here: <c>sourcepostgres</c> and <c>targetConnection</c>
    /// each end in an allowlisted name. The sixth fixture is the REAL shape of the target read this group
    /// deliberately left out of <see cref="s_sweepMembers"/> — a connection built straight from a monitored
    /// server's connection string, with nothing borrowed from the store pool — so if that method is ever
    /// enlisted, it fails here rather than silently acquiring a store bound.</para>
    ///
    /// <para>The LAST construction in each fixture is the one classified. That is what pins the receiver to
    /// the site ITSELF: the two-construction fixture passes on any scan that lets a store receiver earlier
    /// on the line vouch for a target one after it.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "        await using var connection = await _postgres.OpenConnectionAsync(ct);\n"
        + "        using var command = new NpgsqlCommand(Sql, connection);\n",
        true)]
    [InlineData(
        "        await using var connection = await postgres.OpenConnectionAsync(ct);\n"
        + "        using var command = new Npgsql.NpgsqlCommand(Sql, connection);\n",
        true)]
    [InlineData("        await using var command = _postgres!.CreateCommand(Sql);\n", true)]
    [InlineData("        Append(postgres.CreateCommand, rows);\n", true)]
    [InlineData(
        "        await using var connection = await _postgres.OpenConnectionAsync(ct);\n"
        + "        using var command = new NpgsqlCommand(Sql, targetConnection);\n",
        false)]
    [InlineData(
        "        await using var connection = new NpgsqlConnection(runtime.ConnectionString);\n"
        + "        await using var command = new Npgsql.NpgsqlCommand(Sql, connection);\n",
        false)]
    [InlineData("        using var command = sourcepostgres.CreateCommand(Sql);\n", false)]
    [InlineData("        using var command = targetPostgres.CreateCommand(Sql);\n", false)]
    [InlineData("        using var a = postgres.CreateCommand(X); using var b = target.CreateCommand(Y);\n", false)]
    public void TheReceiverClassifier_TellsAStoreCommandFromAMonitoredTargetOne(string source, bool expectedStore)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var matches = s_commandCtor.Matches(code);

        Assert.True(matches.Count > 0, "the fixture did not contain a command construction");

        Assert.Equal(
            expectedStore,
            IsStoreCommand(code, matches[^1], s_storeConnectionOpen.IsMatch(code)));
    }

    /// <summary>
    /// Whether the construction at <paramref name="ctor"/> addresses the STORE.
    ///
    /// <para>The receiver sits BEHIND a <c>CreateCommand</c> and AHEAD of a <c>new NpgsqlCommand</c> — as
    /// its connection argument — so both directions are read, and each is bounded STRUCTURALLY rather than
    /// by a character count: behind, by the construction's own line; ahead, by its own statement. The
    /// reference pin can afford a fixed 200-character window and this one cannot, because these members
    /// embed multi-line verbatim SQL that no character count clears.</para>
    ///
    /// <para>Both directions are read over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s
    /// output, which is what makes the argument shape safe to match with a plain regex: the embedded SQL
    /// contributes no comma, parenthesis or semicolon of its own.</para>
    /// </summary>
    private static bool IsStoreCommand(string code, Match ctor, bool borrowsFromStore)
    {
        var behind = code[(code.LastIndexOf('\n', ctor.Index) + 1)..(ctor.Index + ctor.Length)];

        if (s_storeCommandFactory.IsMatch(behind))
        {
            return true;
        }

        return borrowsFromStore
            && s_storeConnectionArgument.IsMatch(
                CSharpSourceWalker.StatementSpanFrom(code, ctor.Index, statements: 1));
    }

    /// <summary>
    /// The text of one member's body, from its signature to the matching close brace.
    ///
    /// <para>Brace-matched over <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output rather
    /// than the raw text, because a brace inside verbatim SQL or a comment would otherwise close the body
    /// early — and these members are full of both. The offsets are then applied to the ORIGINAL text, so
    /// the value-bearing regexes above see what is actually written. That is the same
    /// stripped-walk/raw-span split the shared walker uses for statement spans.</para>
    /// </summary>
    private static string MemberBody(string text, string member, string path)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(text);

        /* The member NAME followed by an optional generic argument list and then its parameter list.
           The generic hole is not decoration: WriteBatchAsync<TRow>( is real in this project, and a
           pattern of `name\s*\(` would miss it — the #2874 trap where a grep encoded the wrong shape
           and read zero hits as clean. */
        var signature = new Regex(
            @"\b" + Regex.Escape(member) + @"\s*(?:<[^<>()]*>)?\s*\(",
            RegexOptions.CultureInvariant);

        var declarations = signature.Matches(stripped)
            .Where(m => BodyStart(stripped, m.Index) >= 0)
            .ToArray();

        Assert.True(
            declarations.Length == 1,
            $"expected exactly one declaration of {member} in {path}, found {declarations.Length} — "
            + "an overload or a rename has moved this member out from under the guard");

        var open = BodyStart(stripped, declarations[0].Index);
        var depth = 0;

        for (var i = open; i < stripped.Length; i++)
        {
            if (stripped[i] == '{')
            {
                depth++;
            }
            else if (stripped[i] == '}' && --depth == 0)
            {
                return text[declarations[0].Index..(i + 1)];
            }
        }

        Assert.Fail($"{member} in {path} has an unbalanced body");
        return string.Empty;
    }

    /// <summary>
    /// Index of the <c>{</c> that opens the body of the declaration starting at <paramref name="at"/>,
    /// or -1 when what follows the parameter list is not a block — which is how a CALL to the member, or
    /// an expression-bodied delegation, is told apart from its declaration. That distinction is load
    /// bearing: <c>DarlingCollectorRunner</c> has two expression-bodied <c>CreateCommand</c> members that
    /// #2874's census counted as offenders and which are not store commands at all.
    /// </summary>
    private static int BodyStart(string stripped, int at)
    {
        var depth = 0;

        for (var i = stripped.IndexOf('(', at); i >= 0 && i < stripped.Length; i++)
        {
            if (stripped[i] == '(')
            {
                depth++;
            }
            else if (stripped[i] == ')' && --depth == 0)
            {
                for (var j = i + 1; j < stripped.Length; j++)
                {
                    if (char.IsWhiteSpace(stripped[j]))
                    {
                        continue;
                    }

                    return stripped[j] == '{' ? j : -1;
                }

                return -1;
            }
        }

        return -1;
    }

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string SourcePath(string relative)
    {
        var path = Path.Combine(
            RepoRoot(),
            "Darling",
            "PerformanceMonitor.Darling.Service",
            relative.Replace('/', Path.DirectorySeparatorChar));

        /* A moved or renamed sweep source must fail loudly rather than silently shrinking the scan to
           the files that still resolve — an empty sweep is how a guard starts reporting clean. */
        Assert.True(File.Exists(path), $"collection-sweep source not found: {path}");

        return path;
    }

    private static IEnumerable<string> ServiceSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        Assert.True(Directory.Exists(dir), $"service project directory not found: {dir}");

        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(dir, p))
            .OrderBy(p => p, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 150, $"the service sweep found only {paths.Length} files — the project has moved");

        return paths;
    }

    private static bool IsBuildOutput(string projectDir, string path)
    {
        var relative = Path.GetRelativePath(projectDir, path);
        var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return segments.Any(s =>
            string.Equals(s, "bin", System.StringComparison.OrdinalIgnoreCase)
            || string.Equals(s, "obj", System.StringComparison.OrdinalIgnoreCase));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
