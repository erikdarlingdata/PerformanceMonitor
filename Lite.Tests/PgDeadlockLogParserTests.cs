// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The deadlock log parser (#2661). Every fixture here is REAL output captured from a PostgreSQL 17.11
/// target, not a hand-written approximation of one — the two traps below are exactly the kind that a
/// plausible-looking fake would not have.
/// </summary>
public sealed class PgDeadlockLogParserTests
{
    /* Captured verbatim, including the %Q query id running straight into ERROR: with no separator. */
    private const string RealBlock =
        "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151ERROR:  deadlock detected\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
        + "\tProcess 1549: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n"
        + "\tProcess 1556: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151HINT:  See server log for query details.\n";

    /// <summary>
    /// The captured report with its prefix zone SUBSTITUTED, and the zone is the only thing changed.
    ///
    /// <para>Synthesised on purpose, and it is what makes the #2993 assertions mean anything: every target
    /// on the fleet runs <c>log_timezone = UTC</c>, so a fixture carrying the captured zone passes
    /// identically whether the check exists or not. A prefix the fleet does not produce is the only
    /// evidence available for the path that matters.</para>
    /// </summary>
    private static string WithLogZone(string zone) =>
        RealBlock.Replace(" UTC [", $" {zone} [", StringComparison.Ordinal);

    [Fact]
    public void ParsesARealReport_WholeGraphAndBothStatements()
    {
        var found = PgDeadlockLogParser.Extract(RealBlock);

        var deadlock = Assert.Single(found);

        Assert.Equal(new DateTime(2026, 8, 26, 22, 25, 24, 100, DateTimeKind.Utc), deadlock.OccurredAtUtc);
        Assert.Equal(1549, deadlock.VictimPid);
        Assert.Equal(2, deadlock.ParticipantCount);
        Assert.Equal("ShareLock", deadlock.LockModes);
        Assert.Equal("transaction 808, transaction 809", deadlock.Resources);

        /* The victim's statement, not merely SOME statement: the report names both, and attributing the
           wrong one to the cancelled session would point an investigation at the surviving query. */
        Assert.StartsWith("BEGIN; UPDATE dl SET v=v+1 WHERE id=1;", deadlock.VictimStatement, StringComparison.Ordinal);

        var statements = PgDeadlockLogParser.ParseStatements(deadlock.GraphText);
        Assert.Equal(2, statements.Count);
        Assert.Contains("WHERE id=1", statements[1549], StringComparison.Ordinal);
        Assert.Contains("WHERE id=2", statements[1556], StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>%Q</c> writes the query id with NO separator before the severity — the captured line really does
    /// read <c>[1549] 322048460535975151ERROR:</c>. A pattern requiring whitespace there matches nothing,
    /// and matches nothing in the way that looks like "this server has no deadlocks", which is the worst
    /// possible failure for this collector.
    /// </summary>
    [Fact]
    public void ParsesWithAndWithoutTheQueryIdInThePrefix()
    {
        Assert.Single(PgDeadlockLogParser.Extract(RealBlock));

        var noQueryId = RealBlock.Replace("322048460535975151", string.Empty, StringComparison.Ordinal);
        Assert.Single(PgDeadlockLogParser.Extract(noQueryId));
    }

    /// <summary>
    /// A participant's statement is arbitrary user SQL and can span lines, each arriving tab-indented under
    /// the DETAIL block. Taking one line per participant would truncate every multi-line statement to its
    /// first — silently, because the row still looks fine.
    /// </summary>
    [Fact]
    public void KeepsAMultiLineStatementWhole()
    {
        var multiline =
            "2026-08-26 22:25:24.100 UTC [1549] ERROR:  deadlock detected\n"
            + "2026-08-26 22:25:24.100 UTC [1549] DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
            + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
            + "\tProcess 1549: \n"
            + "\tUPDATE orders\n"
            + "\t   SET status = 'shipped'\n"
            + "\t WHERE id = 1;\n"
            + "\tProcess 1556: \n"
            + "\tUPDATE orders SET status = 'held' WHERE id = 2;\n"
            + "2026-08-26 22:25:24.100 UTC [1549] HINT:  See server log for query details.\n";

        var deadlock = Assert.Single(PgDeadlockLogParser.Extract(multiline));

        Assert.Contains("SET status = 'shipped'", deadlock.VictimStatement, StringComparison.Ordinal);
        Assert.Contains("WHERE id = 1;", deadlock.VictimStatement, StringComparison.Ordinal);
    }

    /// <summary>
    /// Participants are counted from the wait EDGES, not from the <c>Process N:</c> statement headers: the
    /// server omits a header when it could not recover the text, and a participant with no statement is
    /// still in the cycle. Counting headers would under-report it.
    /// </summary>
    [Fact]
    public void CountsParticipantsFromEdges_NotFromStatementHeaders()
    {
        var noStatements =
            "2026-08-26 22:25:24.100 UTC [1549] ERROR:  deadlock detected\n"
            + "2026-08-26 22:25:24.100 UTC [1549] DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
            + "\tProcess 1556 waits for ShareLock on transaction 810; blocked by process 1601.\n"
            + "\tProcess 1601 waits for ShareLock on transaction 808; blocked by process 1549.\n"
            + "2026-08-26 22:25:24.100 UTC [1549] HINT:  See server log for query details.\n";

        var deadlock = Assert.Single(PgDeadlockLogParser.Extract(noStatements));

        Assert.Equal(3, deadlock.ParticipantCount);
        Assert.Null(deadlock.VictimStatement);
    }

    /// <summary>
    /// The resource is captured WHOLE rather than decomposed. It is <c>transaction N</c> in the common case
    /// but also <c>tuple (b,o) of relation N</c>, <c>relation N of database N</c> and advisory locks, and an
    /// enumeration would silently drop the shapes it did not anticipate.
    /// </summary>
    [Fact]
    public void CarriesLockResourcesItWasNotWrittenAgainst()
    {
        var tupleLock =
            "2026-08-26 22:25:24.100 UTC [1549] ERROR:  deadlock detected\n"
            + "2026-08-26 22:25:24.100 UTC [1549] DETAIL:  Process 1549 waits for ExclusiveLock on tuple (0,2) of relation 16385 of database 16384; blocked by process 1556.\n"
            + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
            + "2026-08-26 22:25:24.100 UTC [1549] HINT:  See server log for query details.\n";

        var deadlock = Assert.Single(PgDeadlockLogParser.Extract(tupleLock));

        Assert.Contains("tuple (0,2) of relation 16385 of database 16384", deadlock.Resources, StringComparison.Ordinal);
        Assert.Contains("ExclusiveLock", deadlock.LockModes, StringComparison.Ordinal);
        Assert.Contains("ShareLock", deadlock.LockModes, StringComparison.Ordinal);
    }

    /// <summary>
    /// The hash is identity across repeated reads: on the <c>pg_read_file</c> route the same report is
    /// seen every cycle while it stays in the log tail, and on the consume-once RDS route it comes back
    /// after a restart or a write that did not land. Either way it is stored once. Two DIFFERENT deadlocks
    /// must not collide — verified on the rig, where a repeat of the same query pair produced a different
    /// hash because the process IDs differed.
    /// </summary>
    [Fact]
    public void HashIsStableForOneReportAndDistinctBetweenReports()
    {
        var first = Assert.Single(PgDeadlockLogParser.Extract(RealBlock));
        var again = Assert.Single(PgDeadlockLogParser.Extract(RealBlock));

        Assert.Equal(first.DeadlockHash, again.DeadlockHash);

        var otherPids = RealBlock.Replace("1549", "1827", StringComparison.Ordinal)
                                 .Replace("1556", "1830", StringComparison.Ordinal);
        var other = Assert.Single(PgDeadlockLogParser.Extract(otherPids));

        Assert.NotEqual(first.DeadlockHash, other.DeadlockHash);
    }

    /// <summary>
    /// A block with no wait edge is not a deadlock report, and a half-read one is ordinary rather than a
    /// fault: both transports read a bounded window, so a report cut at the edge happens on either.
    /// Skipped, never thrown.
    ///
    /// <para>Recovered on only one of them. The <c>pg_read_file</c> route re-reads an overlapping tail, so
    /// the cut report is whole on the next pass; the RDS log-API route is consume-once, so it is not for
    /// the life of its resume marker (#3009). The two pins below carry what the skip costs there, and they
    /// are separate tests because the two cut positions fail in opposite directions.</para>
    /// </summary>
    [Fact]
    public void SkipsWhatItCannotParse_RatherThanThrowing()
    {
        Assert.Empty(PgDeadlockLogParser.Extract(null));
        Assert.Empty(PgDeadlockLogParser.Extract(string.Empty));
        Assert.Empty(PgDeadlockLogParser.Extract("nothing to see here"));

        var truncated = RealBlock[..RealBlock.IndexOf("blocked by process", StringComparison.Ordinal)];
        Assert.Empty(PgDeadlockLogParser.Extract(truncated));
    }

    /// <summary>
    /// #3009, first shape: a chunk boundary landing BEFORE <c>DETAIL:</c> loses the report whole.
    ///
    /// <para>The <c>ERROR:  deadlock detected</c> line is present and complete, and nothing comes out —
    /// the pattern requires the DETAIL group, so there is no partial match to salvage. On the
    /// <c>pg_read_file</c> route the next overlapping read carries the report whole and this costs
    /// nothing. On the RDS route the resume marker has already advanced past those bytes, so nothing
    /// re-requests them while that marker lives, and the deadlock is gone in the shape that reads as a
    /// server which had none.</para>
    ///
    /// <para>Asserted with the uncut slab beside it rather than alone: an <c>Assert.Empty</c> on a fixture
    /// that never parsed in the first place would pass for the wrong reason, and that is the failure mode
    /// this whole pin exists to describe.</para>
    /// </summary>
    [Fact]
    public void AChunkCutBeforeTheDetailLineLosesTheReportWhole()
    {
        var cutBeforeDetail = RealBlock[..RealBlock.IndexOf("DETAIL:", StringComparison.Ordinal)];

        /* The cut kept the whole ERROR line, so the slab really does hold the start of a deadlock
           report — the loss below is the cut and not an absent fixture. */
        Assert.Contains("ERROR:  deadlock detected\n", cutBeforeDetail, StringComparison.Ordinal);
        Assert.DoesNotContain("DETAIL:", cutBeforeDetail, StringComparison.Ordinal);
        Assert.Single(PgDeadlockLogParser.Extract(RealBlock));

        Assert.Empty(PgDeadlockLogParser.Extract(cutBeforeDetail));
    }

    /// <summary>
    /// #3009, second shape and the dangerous one: a chunk boundary landing INSIDE the DETAIL block still
    /// parses, and stores a row that under-reports the deadlock.
    ///
    /// <para>Cut immediately after the first wait edge, which is where a boundary lands most cheaply — the
    /// line is complete and newline-terminated, so the pattern's <c>(?:\t[^\n]*\n)*</c> continuation
    /// group is satisfied by taking none of them. The block MATCHES, <c>FromBlock</c> finds an edge, and a
    /// row lands naming one of the two locked resources and neither participant's SQL. Absence would at
    /// least be absence; this is a stored deadlock that is quietly smaller than the one that happened.</para>
    ///
    /// <para><b>Why no consistency check on the row can catch it.</b> <c>ParticipantCount</c> is counted
    /// from the same edges <c>GraphText</c> carries, so a fragment lowers both together and the two never
    /// disagree — which is why comparing them, as the issue first proposed, measures nothing. The signal
    /// that survives is the CYCLE: a whole report writes one edge per participant, so a participant count
    /// exceeding the edge count cannot come from a deadlock PostgreSQL actually reported. This pin
    /// asserts that arithmetic on both slabs so the two cases are told apart by shape rather than by
    /// magnitude.</para>
    /// </summary>
    [Fact]
    public void AChunkCutMidDetailStoresAnUnderReportingRow_RatherThanNothing()
    {
        const string FirstEdgeEnd = "blocked by process 1556.\n";

        var cutMidDetail = RealBlock[..(RealBlock.IndexOf(FirstEdgeEnd, StringComparison.Ordinal)
            + FirstEdgeEnd.Length)];

        var whole = Assert.Single(PgDeadlockLogParser.Extract(RealBlock));

        /* IT PARSED. Everything after this describes a row that reached the store. */
        var partial = Assert.Single(PgDeadlockLogParser.Extract(cutMidDetail));

        /* Indistinguishable from the whole report on the columns a reader would look at first. */
        Assert.Equal(whole.OccurredAtUtc, partial.OccurredAtUtc);
        Assert.Equal(whole.VictimPid, partial.VictimPid);
        Assert.Equal(whole.ParticipantCount, partial.ParticipantCount);
        Assert.Equal(whole.LockModes, partial.LockModes);

        /* And under-reporting on the ones carrying the evidence. */
        Assert.Equal("transaction 808, transaction 809", whole.Resources);
        Assert.Equal("transaction 809", partial.Resources);
        Assert.NotNull(whole.VictimStatement);
        Assert.Null(partial.VictimStatement);
        Assert.Equal(2, PgDeadlockLogParser.ParseStatements(whole.GraphText).Count);
        Assert.Empty(PgDeadlockLogParser.ParseStatements(partial.GraphText));

        /* The cycle arithmetic, which is the only thing in the row that gives the fragment away. */
        var wholeEdges = s_edgeCount.Matches(whole.GraphText).Count;
        var partialEdges = s_edgeCount.Matches(partial.GraphText).Count;

        /* A whole report writes one edge per participant. */
        Assert.Equal(whole.ParticipantCount, wholeEdges);
        Assert.Equal(2, wholeEdges);

        /* A fragment cannot: participants come from the pids NAMED in the edges, so one edge yields two
           participants and the cycle it claims is short an edge. This inequality is the detection, and
           the magnitudes are pinned beside it so a change to either side has to be deliberate. */
        Assert.True(
            partial.ParticipantCount > partialEdges,
            $"a fragment must carry more participants than edges; got {partial.ParticipantCount} "
            + $"participant(s) and {partialEdges} edge(s)");
        Assert.Equal(2, partial.ParticipantCount);
        Assert.Equal(1, partialEdges);

        /* Nothing merges the fragment with the whole report either, so a later complete read stores a
           SECOND row beside this one rather than correcting it. */
        Assert.NotEqual(whole.DeadlockHash, partial.DeadlockHash);
    }

    /* One wait edge, matched the way the parser's own s_edge ends one. A whole report carries one per
       participant, so this is what makes "participants exceed edges" an arithmetic that a genuine report
       cannot satisfy. */
    private static readonly System.Text.RegularExpressions.Regex s_edgeCount =
        new(@"; blocked by process \d+\.");

    /// <summary>
    /// The collector declares what it writes. A mismatch here is the #2622 class of defect: the runtime
    /// check only fires when the collector RUNS, and this one only runs where the log is readable.
    /// </summary>
    [Fact]
    public void CollectorDeclaresEveryColumnItWrites()
    {
        var writer = new RecordingCollectorRowWriter();

        PgDeadlocksCollector.Instance.WritePayload(
            new PgDeadlocksCollector.Row(
                new DateTime(2026, 8, 26, 22, 25, 24, DateTimeKind.Utc),
                1549, 2, "ABC123", "ShareLock", "transaction 808", "UPDATE dl ...", "graph"),
            writer,
            MakeContext());

        Assert.Equal(PgDeadlocksCollector.Instance.PayloadColumns.Count, writer.Values.Count);
    }

    /// <summary>
    /// #2993: a log stamped in a non-UTC zone is REFUSED, naming the setting.
    ///
    /// <para>Before this, the prefix zone was captured by the pattern and read by nothing, and the stamp
    /// went through <c>TryParse</c> with <c>AssumeUniversal</c> — so this exact fixture parsed, cleanly,
    /// into a deadlock at 22:25:24 UTC that actually happened at 03:25:24 the next day. Nothing errored,
    /// nothing was empty, and the row was five hours wrong in a trend bucket. That is why the refusal is
    /// the assertion here: there is no output to check, because the whole point is that no output is
    /// produced.</para>
    /// </summary>
    [Fact]
    public void RefusesAReportWhoseLogPrefixIsNotUtc_NamingTheSetting()
    {
        var ex = Assert.Throws<PgLogTimezoneUnsupportedException>(
            () => PgDeadlockLogParser.Extract(WithLogZone("EST")));

        Assert.Equal("EST", ex.ObservedZone);
        Assert.Contains("log_timezone", ex.Message, StringComparison.Ordinal);
        Assert.Contains("EST", ex.Message, StringComparison.Ordinal);

        /* The sentence that stops the row being read as an empty log. A refusal whose message does not say
           so is the same silent nothing one layer over. */
        Assert.Contains("NOT 'no deadlocks were detected'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A zone with no abbreviation renders as a NUMERIC offset, and that shape has to be refused too.
    ///
    /// <para>It is the case that would have escaped the check by never reaching it: the pattern matched the
    /// zone as <c>\w+</c>, which matches neither the sign nor the colon, so a <c>+07</c> prefix matched no
    /// block at all and the collector reported a server with no deadlocks. Silent in a different direction
    /// from the shifted stamp, and the same category — so it is refused rather than skipped, which is what
    /// this asserts: an exception, not an empty list.</para>
    /// </summary>
    [Theory]
    [InlineData("+07")]
    [InlineData("-03:30")]
    [InlineData("+0530")]
    public void RefusesANumericNonZeroOffsetPrefix_RatherThanMatchingNothing(string zone)
    {
        var ex = Assert.Throws<PgLogTimezoneUnsupportedException>(
            () => PgDeadlockLogParser.Extract(WithLogZone(zone)));

        Assert.Equal(zone, ex.ObservedZone);
    }

    /// <summary>
    /// Every spelling of zero offset still parses, through the whole pattern rather than the predicate
    /// alone. A check that refused a UTC server would be worse than no check: it would take deadlock
    /// capture away from the entire fleet.
    /// </summary>
    [Theory]
    [InlineData("UTC")]
    [InlineData("GMT")]
    [InlineData("UCT")]
    [InlineData("+00")]
    [InlineData("-00")]
    [InlineData("+00:00")]
    [InlineData("-0000")]
    public void AZeroOffsetPrefixStillParses(string zone)
    {
        var deadlock = Assert.Single(PgDeadlockLogParser.Extract(WithLogZone(zone)));

        Assert.Equal(new DateTime(2026, 8, 26, 22, 25, 24, 100, DateTimeKind.Utc), deadlock.OccurredAtUtc);
        Assert.Equal(1549, deadlock.VictimPid);
    }

    /// <summary>
    /// The predicate on its own, because the allowlist is the load-bearing part and an over-long one is
    /// the same silent-wrong defect as no check at all, arriving one entry at a time. <c>BST</c> and
    /// <c>CST</c> are here to be REFUSED: they are the ambiguity that makes conversion impossible —
    /// <c>CST</c> is three zones — and refusing them is the whole trade this fix makes.
    /// </summary>
    [Theory]
    [InlineData("UTC", true)]
    [InlineData("utc", true)]
    [InlineData("GMT", true)]
    [InlineData("UCT", true)]
    [InlineData("+00", true)]
    [InlineData("-00:00", true)]
    [InlineData("EST", false)]
    [InlineData("CST", false)]
    [InlineData("BST", false)]
    [InlineData("IST", false)]
    [InlineData("AEST", false)]
    [InlineData("+01", false)]
    [InlineData("+0030", false)]
    [InlineData("-05:30", false)]
    [InlineData("+", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsZeroOffsetLogZone_AnswersDetectionRatherThanConversion(string? zone, bool expected)
        => Assert.Equal(expected, PgDeadlockLogParser.IsZeroOffsetLogZone(zone));

    /// <summary>
    /// The WIRING, not the pattern: the collector's four result columns reach the parser in the right
    /// order, driven through the real <c>ReadAsync</c>.
    ///
    /// <para>An <c>Assert.Contains</c> on the query text cannot see this. The zone arrived as a new column
    /// in the middle of the list, so a mis-ordinal would hand the parser the zone where it expects the
    /// victim pid — which parses as nothing, returns null, and drops every deadlock silently. This is the
    /// one seam where that is possible.</para>
    /// </summary>
    [Fact]
    public async Task TheCollectorHandsTheParserItsFourColumnsInOrder()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "2026-08-26 22:25:24.100",
                "UTC",
                "1549",
                "Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
                + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n",
            });

        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(1549, row.VictimPid);
        Assert.Equal(2, row.ParticipantCount);
        Assert.Equal(new DateTime(2026, 8, 26, 22, 25, 24, 100, DateTimeKind.Utc), row.OccurredAtUtc);
    }

    /// <summary>
    /// And the same seam refuses: the zone column is READ rather than carried past, so a non-UTC target
    /// gets a classified refusal out of the collector instead of rows stamped in local time.
    /// </summary>
    [Fact]
    public async Task TheCollectorRefusesWhenItsZoneColumnIsNotUtc()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "2026-08-26 22:25:24.100",
                "EST",
                "1549",
                "Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
                + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n",
            });

        var ex = await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(
            async () => await PgDeadlocksCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None));

        Assert.Equal("EST", ex.ObservedZone);
    }

    /// <summary>
    /// A window holding a non-UTC block and a readable UTC one refuses the WHOLE read, siblings included.
    ///
    /// <para>Raised in review on the PR rather than predicted: the throw is per block and unwinds
    /// <c>Extract</c>'s loop, so a straddled window loses deadlocks that were perfectly storable. It is
    /// the accepted trade and this pins it as a DECISION rather than leaving it an accident - storing the
    /// UTC half would leave a partial history from a target just declared unreadable, with nothing in the
    /// data marking what is missing, and a reader could not then tell a quiet server from a
    /// half-collected one. The straddle only arises while a <c>log_timezone</c> change crosses the read.
    /// What it costs differs by transport, which <see cref="PgDeadlockLogParser.Extract"/>'s remarks
    /// state: the <c>pg_read_file</c> route re-reads an overlapping tail, while the RDS log-API route is
    /// consume-once and loses the readable siblings for the life of its resume marker.</para>
    /// </summary>
    [Fact]
    public void AStraddledWindowRefusesTheWholeRead_NotJustTheOffendingBlock()
    {
        /* Ordered so the readable block comes FIRST: if the loop returned what it had instead of
           unwinding, this would come back with one deadlock and no exception. */
        var straddled = WithLogZone("UTC")
            + RealBlock.Replace(" UTC [", " EST [", StringComparison.Ordinal)
                       .Replace("1549", "1827", StringComparison.Ordinal)
                       .Replace("1556", "1830", StringComparison.Ordinal);

        Assert.Equal(2, s_blockCount.Matches(straddled).Count);

        var ex = Assert.Throws<PgLogTimezoneUnsupportedException>(
            () => PgDeadlockLogParser.Extract(straddled));

        Assert.Equal("EST", ex.ObservedZone);
    }

    /* The fixture's own positive control: without it a straddled slab that had somehow stopped holding
       two recognisable blocks would satisfy the refusal above for the wrong reason. */
    private static readonly System.Text.RegularExpressions.Regex s_blockCount =
        new(@"ERROR:  deadlock detected");

    private static CollectorContext MakeContext() => new()
    {
        ServerId = 42,
        ServerName = "pg-target",
        CollectionTime = new DateTime(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = 17,
        },
        ExcludedDatabases = Array.Empty<string>(),
    };

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }
}
