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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store&lt;-&gt;service COMMAND plane, the two command handlers that resolve from the store, the Query
/// Store backfill worker's two reads, and the control-plane reload beacon each carry an EXPLICIT command
/// deadline, and each carries its OWN regime's deadline rather than a neighbour's (#2874).
///
/// <para><b>Four constants for nine sites, because two of them have an enclosing budget that does not
/// bound them.</b> <c>DarlingWorker.BackfillSliceDeadline</c> is 300 s over the backfill slice, but
/// <c>AbandonableStep</c> ABANDONS rather than cancels — it races the work against a <c>Task.Delay</c> and
/// returns without signalling anything — so the enclosing budget stops WAITING while the statement keeps
/// running on the server. Measured against a live store: a 20 s statement with <c>CommandTimeout = 0</c>
/// under a 3 s deadline returned <c>Abandoned</c> at 3.0 s with <c>pg_stat_activity</c> still reporting
/// that backend <c>state = 'active'</c>; with <c>CommandTimeout = 5</c> it faulted at 5.0 s and the backend
/// was gone. The command deadline is the only instrument. And the command plane's own budget is a 5-minute
/// claim lease with no heartbeat, which the commands it leases legitimately outlast — so there too the
/// per-command deadline is what has to be derived rather than inherited.</para>
///
/// <para><b>Scoped by MEMBER, and by (member, constant) pairs rather than by "has a deadline".</b>
/// #2928 established member scoping for this project: <c>.Service</c>'s budget regimes do not respect file
/// boundaries, and both of that group's own errors were boundary errors — one attributing sites to a regime
/// by file, one excluding a site because of the method it appeared inside. This pin goes one step further
/// and asserts WHICH constant each site takes, because four of this group's nine sites live in files that
/// hold other regimes' sites and two of them sit in the same method as each other's neighbours. A guard
/// that only asked "is there a <c>CommandTimeout =</c> nearby" would pass on a site cross-wired to the
/// wrong regime's number, which for the actual-plan resolver would mean a 5 s bound on a read whose floor
/// is measured in tens of seconds.</para>
///
/// <para><b>Premises that live in private fields are asserted from SOURCE, not by reference.</b> Three of
/// the bounds below are derived from <c>private static readonly</c> members —
/// <c>DarlingWorker.s_commandPollInterval</c>, <c>s_sweepInterval</c> and <c>BackfillSliceDeadline</c> — so
/// there is no symbol to compare against without widening visibility for a test's convenience. Reading the
/// value out of the source instead keeps the derivation falsifiable: raise the sweep interval and the
/// beacon's band assertion fails and forces a re-derivation, which a hardcoded 15 in a test comment would
/// not. The two <c>.Viewer</c> premises — <c>ViewerDataService.DefaultCommandTimeout</c> and
/// <c>ImperativeCommandTimeout</c> — are read the same way, for a different reason: they are public, but
/// that project is <c>net10.0-windows</c> WPF, and a source read is verifiable on a non-Windows host where
/// a project reference is not. The one premise that is both reachable and cheap to reference,
/// <c>DarlingWorker.ActualPlanCaptureTimeoutSeconds</c>, is a real compile-time reference. Each source
/// read fails loudly on a rename rather than returning a default, which is what a vacuous band assertion
/// would do.</para>
///
/// <para><b>Known duplication, recorded rather than consolidated.</b> The member-signature extractor below
/// is the second copy in this suite; <c>CollectionSweepCommandTimeoutTests</c> has the first. Both build on
/// <see cref="CSharpSourceWalker"/> for the literal-aware walk and for <c>BraceBalanced</c>, which is the
/// part #2925 and #2927 consolidated and which is deliberately NOT re-implemented here. Extracting the
/// signature match belongs in one change after the remaining #2874 groups land, not in one of them —
/// groups C and E are writing pins in this directory concurrently, and a shared extraction now would
/// collide with whichever of them reached it second.</para>
/// </summary>
public sealed class CommandPlaneCommandTimeoutTests
{
    /// <summary>
    /// Every store-touching member in this group, with the regime constant it must take. Named rather
    /// than globbed, and paired with its constant rather than merely listed.
    ///
    /// <para><b>Why the executor's four and the hypothetical-index lookup share one number.</b> Each is a
    /// single row on a keyed, non-hypertable relation — <c>config.config_command</c> by its identity PK,
    /// <c>config.config_service</c> by <c>id = 1</c>, <c>config.config_monitored_servers</c> by
    /// <c>server_id</c>, <c>collect.pg_statement_text</c> by its <c>(server_id, queryid)</c> PK — and each
    /// runs on the same serial 5 s command loop under the same claim.</para>
    ///
    /// <para><b>Why the actual-plan resolve does not.</b> Same token, same loop, nothing re-runs either —
    /// so by the token test they are one regime — but none of its three resolvers predicates on
    /// <c>collection_time</c>, the partitioning column, so each is a <c>LIMIT 1</c> over every chunk in
    /// retention on a table measured at 62.5 GB across 19 chunks (#2795). Its FLOOR is larger than the
    /// plane's entire value, which is the split this sweep's own rule demands: a wrong bound is worse than
    /// no bound.</para>
    ///
    /// <para><b>What is deliberately absent, and why each is a different budget.</b>
    /// <c>DarlingWorker.ReadLatestCpuAsync</c> belongs to #2882's alert pass (10 s off a 30 s cadence) — a
    /// site that pin's file-scoped list missed, not a member of this regime. <c>ReadStoreSizeBytesAsync</c>
    /// runs on the 5-minute disk-check cadence. <c>StoreConfigProvider</c>'s other twelve sites seed and
    /// reconcile ONCE per process start, unlike the beacon below which the sweep re-runs every 15 s for the
    /// life of the process. <c>DarlingDeltaCalculator</c>'s four are startup seeds.
    /// <c>DarlingCommandExecutor</c>'s <c>test_connect</c>, <c>snapshot_now</c>, <c>analyze_now</c>,
    /// <c>purge_now</c>, <c>fetch_plan</c> and <c>fetch_active_queries</c> branches touch the store through
    /// members other groups own or not at all.</para>
    /// </summary>
    private static readonly (string File, string Member, string Constant)[] s_sites =
    {
        ("DarlingCommandExecutor.cs", "ReclaimStaleCommandsAsync", nameof(ServiceCommandDeadlines.CommandPlaneSeconds)),
        ("DarlingCommandExecutor.cs", "ClaimNextAsync", nameof(ServiceCommandDeadlines.CommandPlaneSeconds)),
        ("DarlingCommandExecutor.cs", "ExecuteStoreWriteAsync", nameof(ServiceCommandDeadlines.CommandPlaneSeconds)),
        ("DarlingCommandExecutor.cs", "ReportAsync", nameof(ServiceCommandDeadlines.CommandPlaneSeconds)),
        ("DarlingWorker.cs", "RunTestHypotheticalIndexAsync", nameof(ServiceCommandDeadlines.CommandPlaneSeconds)),
        ("DarlingWorker.cs", "RunExecuteActualPlanAsync", nameof(ServiceCommandDeadlines.ActualPlanResolveSeconds)),
        ("QueryStoreBackfill.cs", "GetCandidateDatabasesAsync", nameof(ServiceCommandDeadlines.QueryStoreBackfillReadSeconds)),
        ("QueryStoreBackfill.cs", "GetStoredFloorAsync", nameof(ServiceCommandDeadlines.QueryStoreBackfillReadSeconds)),
        ("StoreConfigProvider.cs", "ReadConfigVersionAsync", nameof(ServiceCommandDeadlines.ConfigReloadBeaconSeconds)),
    };

    /// <summary>This group's command sites, counted so a member that stops creating commands fails loudly.</summary>
    private const int ExpectedSiteCount = 9;

    /// <summary>
    /// All FIVE construction shapes this sweep now knows about. The bare method group is absent from this
    /// project and the qualified type name occurs once (already timed, and a monitored-target command), but
    /// "absent today" is not a guard — the qualified shape was found only because a red-first variant came
    /// back green against a scanner built on four.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\s*\(|new\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)+NpgsqlCommand\s*\(|\.CreateCommand\s*\(|\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The two-statement window from a construction, with comments and literal TEXT blanked.
    ///
    /// <para><b>Blanked, unlike the landed pins, and a fixture below is why.</b>
    /// <see cref="CSharpSourceWalker.StatementSpanFrom"/> cuts from the ORIGINAL text on purpose — its
    /// remarks say callers want to see what is actually written — and every pin in this sweep then matches
    /// its value regex over that raw span. So a COMMENT inside the window that happens to spell the
    /// deadline satisfies the value regex, and an untimed site reads as clean. That is the dangerous
    /// direction, and it is not hypothetical for a codebase whose style actively encourages an explanatory
    /// comment in exactly that gap; #2928's own equivalent fixture escapes it only because
    /// <c>Timeout = </c> written in prose lacks the leading dot its regex requires. The walk is already
    /// literal-aware, and the stripper preserves length, so taking the same offsets out of the stripped
    /// text costs nothing and closes it. A deadline can never legitimately live inside a string
    /// literal.</para>
    /// </summary>
    private static string StatementSpan(string text, int index) =>
        CSharpSourceWalker.StatementSpanFrom(CSharpSourceWalker.StripCommentsAndStrings(text), index, statements: 2);

    [Fact]
    public void EveryCommandPlaneSite_TakesItsOwnRegimesDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var (file, member, constant) in s_sites)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var body = MemberBody(text, member, path);

            /* The construction scan runs over STRIPPED text so a mention in prose cannot become a phantom
               offender reported at a line no edit can fix, and so a `.CreateCommand` written inside a
               comment cannot inflate the census. That stripper is load-bearing rather than defensive in
               this project: an earlier count of this same sweep reported a bare method group in .Service
               that was the phrase in running prose inside a block comment. */
            var relational = new Regex(
                @"CommandTimeout\s*=\s*ServiceCommandDeadlines\." + Regex.Escape(constant) + @"\b",
                RegexOptions.CultureInvariant);

            var strippedBody = CSharpSourceWalker.StripCommentsAndStrings(body);

            foreach (Match ctor in s_commandCtor.Matches(strippedBody))
            {
                total++;

                /* Two statements, for the reason #2810's pin records: the CreateCommand shape's method
                   result cannot take an object initializer, so its deadline is the statement AFTER the
                   construction. RunTestHypotheticalIndexAsync is written as `using (...) { ...; }`, where
                   the window is the block's first two statements — the deadline has to be one of them. */
                var span = StatementSpan(body, ctor.Index);

                if (!relational.IsMatch(span))
                {
                    offenders.Add($"{file} {member} +{LineOf(body, ctor.Index)} (wants {constant})");
                }
            }
        }

        /* Offenders BEFORE the census, so a real defect reports as a defect rather than as an arithmetic
           mismatch that says nothing about the deadline it is missing. */
        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command-plane / backfill / beacon command(s) do not take their regime's "
            + $"{nameof(ServiceCommandDeadlines)} constant, so they inherit Npgsql's undocumented 30s "
            + $"default or a neighbouring regime's number: {string.Join(", ", offenders)}");

        /* A tripwire in both directions. Downward it catches a member that stopped creating commands, or an
           extractor that silently returned an empty body — the way a source-walking guard starts reporting
           clean on code it no longer reads. Upward it forces a human to decide whether a new site really
           belongs to this budget rather than inheriting the answer by adjacency. */
        Assert.Equal(ExpectedSiteCount, total);
    }

    /// <summary>
    /// The same nine sites, judged a SECOND way — through the shared
    /// <see cref="CommandDeadlineScanner"/> that #2938 extracted and five pins route through.
    ///
    /// <para><b>Additive rather than a replacement, because the two ask different questions.</b> The
    /// shared scanner answers "does this site set an explicit deadline at all", and it does that better
    /// than a bare regex can: it reads the construction's own initializer separately from the statement
    /// span, and it qualifies the assignment by the NAME bound to the construction, so a sibling's
    /// deadline inside the same two-statement window cannot be borrowed. What it cannot express is which
    /// REGIME's constant a site must take — and cross-wiring the actual-plan resolve to the command
    /// plane's 5 s would put a tens-of-seconds read on a millisecond bound while satisfying every
    /// deadline-shaped check. So the relational assertion above stays as the primary guard and this runs
    /// beside it. If the two ever disagree, one of them is wrong and the build says so.</para>
    /// </summary>
    [Fact]
    public void EveryCommandPlaneSite_AlsoPassesTheSharedDeadlineScanner()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var (file, member, _) in s_sites)
        {
            var path = SourcePath(file);
            var body = MemberBody(File.ReadAllText(path), member, path);

            /* The scanner's contract is already-STRIPPED source, which is the standard this pin's own
               fixture argued for before the extraction existed. */
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(body);

            foreach (Match ctor in s_commandCtor.Matches(stripped))
            {
                total++;

                if (!CommandDeadlineScanner.SetsAnExplicitDeadline(stripped, ctor.Index))
                {
                    offenders.Add($"{file} {member} +{LineOf(body, ctor.Index)}");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command-plane site(s) fail the SHARED deadline scanner even though the "
            + "relational check above passed — the two guards disagree, so one of them is reading the "
            + $"wrong span: {string.Join(", ", offenders)}");

        Assert.Equal(ExpectedSiteCount, total);
    }

    /// <summary>
    /// The command plane's own bound, on both sides — derived in
    /// <see cref="ServiceCommandDeadlines.CommandPlaneSeconds"/>. A band rather than an equality, following
    /// the precedent .Storage and .Viewer set: freezing the number makes every re-derivation a test edit,
    /// and the claim being defended is that it sits between two bounds.
    /// </summary>
    [Fact]
    public void TheCommandPlaneDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.CommandPlaneSeconds;
        var pollTick = SecondsOfPrivateTimeSpan("DarlingWorker.cs", "s_commandPollInterval");

        Assert.Equal(5, pollTick);

        Assert.True(
            seconds >= 1,
            $"command-plane deadline {seconds}s leaves no headroom over the measured worst case — 3.9ms cold "
            + "on the same single-row config_command shape (#2901) — and the report write is what ENDS the "
            + "claim lease, so it must not be the statement that gives up early");

        Assert.True(
            seconds <= pollTick,
            $"command-plane deadline {seconds}s exceeds the plane's own {pollTick}s poll cadence. The loop is "
            + "single-threaded and the stale-command reaper runs FIRST on every tick, so one stalled "
            + "statement is added tick period for the whole plane");

        var chain = 4 * seconds;
        var stated = SecondsOfViewerTimeSpan("ViewerDataService.Commands.cs", "DefaultCommandTimeout");

        Assert.Equal(45, stated);

        Assert.True(
            chain < stated,
            $"the executor's four statements chain to {chain}s, at or past the {stated}s the viewer STATES "
            + "for pause/resume (ViewerDataService.DefaultCommandTimeout) — the overshoot #2901 fixed on "
            + "the other end of this same plane");
    }

    /// <summary>
    /// The actual-plan resolve's bound. Floored by the plane's own value — its cost is larger, which is why
    /// it is a separate regime at all — and capped by what the command's budget has left after the part
    /// that is already bounded.
    /// </summary>
    [Fact]
    public void TheActualPlanResolveDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.ActualPlanResolveSeconds;

        Assert.True(
            seconds > ServiceCommandDeadlines.CommandPlaneSeconds,
            $"actual-plan resolve deadline {seconds}s is not above the command plane's "
            + $"{ServiceCommandDeadlines.CommandPlaneSeconds}s. It reads query_store_stats with no "
            + "collection_time predicate — a LIMIT 1 over every chunk in retention on a 62.5GB / 19-chunk "
            + "table whose reads the store's own log shows cancelled at the 30s default 2,092 and 631 times "
            + "in one day (#2795) — so the plane's number would fail a button that works today");

        /* The resolve is not the last thing the command does. What follows it inside the same claim is the
           re-execution, already bounded; what waits on the whole thing is the viewer's imperative poll
           budget. The remainder, less one poll tick to claim and one plane deadline to report, is the room
           this deadline may occupy — and landing inside it is what makes a timeout surface as a legible
           "store error" rather than as a viewer poll miss. */
        var viewerBudget = SecondsOfViewerTimeSpan("ViewerDataService.ControlCommands.cs", "ImperativeCommandTimeout");
        var reExecution = DarlingWorker.ActualPlanCaptureTimeoutSeconds;

        Assert.Equal(180, viewerBudget);
        var claimAndReport = SecondsOfPrivateTimeSpan("DarlingWorker.cs", "s_commandPollInterval")
            + ServiceCommandDeadlines.CommandPlaneSeconds;
        var remainder = viewerBudget - reExecution - claimAndReport;

        Assert.True(
            remainder > 0,
            $"the {viewerBudget}s viewer budget no longer has room for the {reExecution}s re-execution plus "
            + $"{claimAndReport}s of claim and report — this deadline's derivation needs redoing");

        Assert.True(
            seconds <= remainder,
            $"actual-plan resolve deadline {seconds}s exceeds the {remainder}s the command's own budget has "
            + $"left ({viewerBudget}s viewer poll - {reExecution}s re-execution - {claimAndReport}s claim and "
            + "report), so a slow resolve would surface as a poll miss instead of a timeout outcome");
    }

    /// <summary>
    /// The backfill reads' bound. This is the one regime in this group where the derived value is ABOVE
    /// Npgsql's default rather than below it, because the shipped 30 s is measurably beneath what these two
    /// reads cost cold on the largest store — and the enclosing budget abandons rather than cancels, so
    /// nothing else would ever kill the statement.
    /// </summary>
    [Fact]
    public void TheBackfillReadDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.QueryStoreBackfillReadSeconds;
        var sliceDeadline = SecondsOfPrivateTimeSpan("DarlingWorker.cs", "BackfillSliceDeadline");
        var tick = SecondsOfPrivateTimeSpan("DarlingWorker.cs", "s_queryStoreBackfillInterval");

        Assert.Equal(300, sliceDeadline);
        Assert.Equal(300, tick);

        Assert.True(
            seconds > 51,
            $"backfill read deadline {seconds}s does not clear the 40,743-50,560ms cold cost #2795 measured "
            + "for the unbounded shape-twin of these two reads on the 62.5GB / 19-chunk query_store_stats. "
            + "Below that, a cold read fails EVERY time and the failure is swallowed at LogDebug as 'no "
            + "backfill work'");

        Assert.True(
            seconds < sliceDeadline,
            $"backfill read deadline {seconds}s is at or past the {sliceDeadline}s BackfillSliceDeadline. "
            + "AbandonableStep abandons rather than cancels — measured: Abandoned returned at 3.0s with the "
            + "backend still state='active' — so at or above that the loop walks away from a statement that "
            + "keeps burning a pooled store connection, and the step's in-flight guard quarantines that "
            + "server's backfill until the task truly ends");

        Assert.True(
            seconds < tick,
            $"backfill read deadline {seconds}s is at or past the {tick}s backfill tick, so one read could "
            + "consume a whole slice's cycle");
    }

    /// <summary>
    /// The reload beacon's bound. Tighter than the command plane's despite the same millisecond floor,
    /// because the read sits on the SERIAL sweep thread ahead of every server's launch: its blast radius is
    /// the whole fleet and it fails open.
    /// </summary>
    [Fact]
    public void TheConfigReloadBeaconDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.ConfigReloadBeaconSeconds;
        var sweepTick = SecondsOfPrivateTimeSpan("DarlingWorker.cs", "s_sweepInterval");

        Assert.Equal(15, sweepTick);

        Assert.True(
            seconds >= 1,
            $"reload-beacon deadline {seconds}s leaves no headroom over a single-row config_service read "
            + "(3.9ms cold, #2901) — and a beacon that fails open stops applying config changes");

        Assert.True(
            seconds * 5 <= sweepTick,
            $"reload-beacon deadline {seconds}s is not a fifth or less of the {sweepTick}s sweep tick. The "
            + "read is awaited at the top of the sweep loop on the serial thread, ahead of the launch of "
            + "every server, so a full overshoot must still leave the sweep inside two ticks");

        Assert.True(
            seconds < ServiceCommandDeadlines.CommandPlaneSeconds,
            $"reload-beacon deadline {seconds}s is not tighter than the command plane's "
            + $"{ServiceCommandDeadlines.CommandPlaneSeconds}s. Same millisecond floor, more lopsided "
            + "asymmetry: the beacon's overrun costs the whole fleet's collection latency while its failure "
            + "costs one tick of config-change delay");

        /* The one invariant this file states in prose and nothing enforced. SerialLoopSeconds bounds the
           reload BODY, which runs only once the beacon has already seen a version change; the beacon runs
           on every tick and is a single-row lookup, so it is the tightest thing on that thread. Its own
           doc comment says it "must stay" looser than this one — pinned rather than trusted, because the
           two constants now sit ten lines apart and converging them for tidiness would compile. */
        Assert.True(
            seconds < ServiceCommandDeadlines.SerialLoopSeconds,
            $"reload-beacon deadline {seconds}s is not tighter than the serial loop's "
            + $"{ServiceCommandDeadlines.SerialLoopSeconds}s, which that constant's own derivation "
            + "requires: the beacon fires on every 15s tick while the reload body it gates runs only on a "
            + "version change, and every command in that body is a heavier read than this one");
    }

    /// <summary>
    /// The four regimes stay DISTINCT. Two of them landing on the same number would mean one of the four
    /// derivations was not doing any work, and the cheapest way for this file to rot is for a later edit to
    /// converge them for tidiness.
    /// </summary>
    [Fact]
    public void TheFourRegimes_AreFourDifferentNumbers()
    {
        var values = new[]
        {
            ServiceCommandDeadlines.CommandPlaneSeconds,
            ServiceCommandDeadlines.ActualPlanResolveSeconds,
            ServiceCommandDeadlines.QueryStoreBackfillReadSeconds,
            ServiceCommandDeadlines.ConfigReloadBeaconSeconds,
        };

        Assert.Equal(values.Length, values.Distinct().Count());
    }

    /// <summary>
    /// The scanner's own edges, because nine sites' correctness is asserted through it: a false positive
    /// fails a green build on correct code, and a false NEGATIVE reports success on the defect.
    ///
    /// <para>Every fixture is a shape that occurs in this group. The <c>using (...) { ...; }</c> form is how
    /// <c>RunTestHypotheticalIndexAsync</c> is actually written, and its brace arithmetic is the non-obvious
    /// case — the walk enters at the construction already inside the <c>using</c>'s parenthesis, so the
    /// depth counter goes NEGATIVE closing it before the block's brace brings it back. The last two
    /// fixtures are the ones that matter: a site must not borrow a NEIGHBOUR's deadline, and a site must
    /// not pass on the WRONG regime's constant.</para>
    /// </summary>
    [Theory]
    [InlineData(
        "using (var lookup = _postgres.CreateCommand(Sql))\n"
        + "{\n"
        + "    lookup.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;\n"
        + "    lookup.Parameters.AddWithValue(id);\n"
        + "}\n",
        true)]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection);\n"
        + "command.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;\n",
        true)]
    [InlineData(
        "using var command = new Npgsql.NpgsqlCommand(Sql, connection);\n"
        + "command.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;\n",
        true)]
    [InlineData(
        "using (var lookup = _postgres.CreateCommand(Sql))\n"
        + "{\n"
        + "    /* a comment mentioning CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds is not one. */\n"
        + "    lookup.Parameters.AddWithValue(id);\n"
        + "}\n",
        false)]
    [InlineData(
        "using (var first = _postgres.CreateCommand(Sql))\n"
        + "{\n"
        + "    first.Parameters.AddWithValue(id);\n"
        + "}\n"
        + "using var second = new NpgsqlCommand(Sql, connection);\n"
        + "second.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;\n",
        false)]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection);\n"
        + "command.CommandTimeout = ServiceCommandDeadlines.QueryStoreBackfillReadSeconds;\n",
        false)]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection);\n"
        + "command.CommandTimeout = 5;\n",
        false)]
    public void TheScanner_SeesTheRegimesOwnDeadlineWithoutBorrowingANeighbours(string source, bool expected)
    {
        var relational = new Regex(
            @"CommandTimeout\s*=\s*ServiceCommandDeadlines\."
            + nameof(ServiceCommandDeadlines.CommandPlaneSeconds) + @"\b",
            RegexOptions.CultureInvariant);

        var ctor = s_commandCtor.Match(CSharpSourceWalker.StripCommentsAndStrings(source));
        Assert.True(ctor.Success, "the fixture did not contain a command construction");

        Assert.Equal(expected, relational.IsMatch(StatementSpan(source, ctor.Index)));

        /* The positive control on the negative: the SAME fixture read the way every landed pin reads it —
           value regex over the RAW span — so a case that only this pin gets right is visibly one, rather
           than a green that proves nothing. The comment fixture is the pair that differs. */
        var rawSpan = CSharpSourceWalker.StatementSpanFrom(source, ctor.Index, statements: 2);

        Assert.True(
            expected || !relational.IsMatch(rawSpan) || source.Contains("/*", System.StringComparison.Ordinal),
            "an untimed fixture read as timed over the raw span for a reason other than a comment — the "
            + "stripped-span fix above is narrower than the gap it was written for");
    }

    /// <summary>
    /// The member extractor, pinned separately: a body that resolves to the WRONG span, or to an empty one,
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
            + "        if (id > 0) { command.CommandTimeout = 3; }\n"
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
    /// The private-field reader, pinned for the same reason: it is what three band assertions' PREMISES are
    /// read through, and a reader that matched nothing would make those assertions vacuous. It fails rather
    /// than returning a default, so a renamed field is loud.
    /// </summary>
    [Fact]
    public void ThePrivateTimeSpanReader_ReadsBothUnitFormsAndFailsOnAMiss()
    {
        const string Source =
            "private static readonly TimeSpan s_five = TimeSpan.FromSeconds(5);\n"
            + "private static readonly TimeSpan s_threeMinutes = TimeSpan.FromMinutes(3);\n";

        Assert.Equal(5, SecondsOfTimeSpanIn(Source, "s_five"));
        Assert.Equal(180, SecondsOfTimeSpanIn(Source, "s_threeMinutes"));
        Assert.Null(SecondsOfTimeSpanIn(Source, "s_absent"));
    }

    /// <summary>
    /// Seconds of a <c>TimeSpan</c> field declared in one of the viewer's sources — the other end of this
    /// same command plane, whose stated waits are two of this pin's premises.
    /// </summary>
    private static int SecondsOfViewerTimeSpan(string file, string field) =>
        SecondsOfTimeSpanFile(ProjectPath("PerformanceMonitor.Darling.Viewer", file), field);

    /// <summary>
    /// Seconds of a <c>TimeSpan</c> field declared in one of this project's sources, read from the source
    /// because the field is private. Fails the test rather than returning a default when the field is gone.
    /// </summary>
    private static int SecondsOfPrivateTimeSpan(string file, string field) =>
        SecondsOfTimeSpanFile(SourcePath(file), field);

    /// <summary>Seconds of a <c>TimeSpan</c> field in a named source file, failing loudly on a miss.</summary>
    private static int SecondsOfTimeSpanFile(string path, string field)
    {
        var text = File.ReadAllText(path);
        var seconds = SecondsOfTimeSpanIn(CSharpSourceWalker.StripCommentsAndStrings(text), field);

        Assert.True(
            seconds is not null,
            $"could not read {field} out of {path} — this pin's band assertions are derived from it, so a "
            + "rename has to be noticed rather than silently making them vacuous");

        return seconds!.Value;
    }

    /// <summary>Seconds of a <c>TimeSpan.FromSeconds</c>/<c>FromMinutes</c> field in the given text, or null.</summary>
    private static int? SecondsOfTimeSpanIn(string text, string field)
    {
        var match = new Regex(
            @"\b" + Regex.Escape(field) + @"\s*=\s*TimeSpan\.From(Seconds|Minutes)\s*\(\s*(\d+)\s*\)",
            RegexOptions.CultureInvariant).Match(text);

        if (!match.Success)
        {
            return null;
        }

        var value = int.Parse(match.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture);

        return match.Groups[1].Value == "Minutes" ? value * 60 : value;
    }

    /// <summary>
    /// The text of one member's body, from its signature to the matching close brace. Brace-matched over
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output through
    /// <see cref="CSharpSourceWalker.BraceBalanced"/>, because a brace inside verbatim SQL or a comment
    /// would otherwise close the body early and these members are full of both. The offsets are then
    /// applied to the ORIGINAL text, so the value-bearing regexes see what is actually written.
    /// </summary>
    private static string MemberBody(string text, string member, string path)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(text);

        /* The member NAME followed by an optional generic argument list and then its parameter list. The
           generic hole is not decoration: this project declares WriteBatchAsync<TRow>(, and a pattern of
           `name\s*\(` would miss it — the #2874 trap where a scan encoded the wrong shape and read zero
           hits as clean. */
        var signature = new Regex(
            @"\b" + Regex.Escape(member) + @"\s*(?:<[^<>()]*>)?\s*\(",
            RegexOptions.CultureInvariant);

        var declarations = signature.Matches(stripped)
            .Where(m => BodyStart(stripped, m.Index) >= 0)
            .ToArray();

        Assert.True(
            declarations.Length == 1,
            $"expected exactly one declaration of {member} in {path}, found {declarations.Length} — an "
            + "overload or a rename has moved this member out from under the guard");

        var open = BodyStart(stripped, declarations[0].Index);
        var block = CSharpSourceWalker.BraceBalanced(stripped, open);

        Assert.EndsWith("}", block);

        return text[declarations[0].Index..(open + block.Length)];
    }

    /// <summary>
    /// Index of the <c>{</c> that opens the body of the declaration starting at <paramref name="at"/>, or
    /// -1 when what follows the parameter list is not a block — which is how a CALL to the member, or an
    /// expression-bodied delegation, is told apart from its declaration. That distinction is load bearing:
    /// <c>DarlingCollectorRunner</c> has two expression-bodied <c>CreateCommand</c> members that #2874's
    /// census counted as offenders and which are not store commands at all.
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

    private static string SourcePath(string relative) => ProjectPath("PerformanceMonitor.Darling.Service", relative);

    private static string ProjectPath(string project, string relative)
    {
        var path = Path.Combine(
            RepoRoot(),
            "Darling",
            project,
            relative.Replace('/', Path.DirectorySeparatorChar));

        /* A moved or renamed source must fail loudly rather than silently shrinking the scan to the files
           that still resolve — an empty sweep is how a guard starts reporting clean. */
        Assert.True(File.Exists(path), $"command-plane source not found: {path}");

        return path;
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(System.AppContext.BaseDirectory);

        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        Assert.True(directory is not null, "could not locate the repository root from the test output directory");

        return directory!.FullName;
    }
}
