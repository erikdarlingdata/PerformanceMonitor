// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Linq;
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
    /// The hash is identity across the overlapping reads the collector makes on purpose: the same report is
    /// seen every cycle while it stays in the log tail, and must be stored once. Two DIFFERENT deadlocks
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
    /// fault: both transports read a bounded window, so a report cut at the edge is whole on the next
    /// overlapping pass. Skipped, never thrown.
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
