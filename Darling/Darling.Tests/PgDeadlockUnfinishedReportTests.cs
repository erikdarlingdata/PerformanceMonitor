// #4735 item 4: a deadlock report that a read ends inside (its HINT line not yet written or not yet read) hashes
// differently from the report the next read gets whole, so storing the fragment stores the report twice, and on the
// RDS log API, which never offers the rest again, stores only the fragment. The self-hosted route skips the
// unfinished report because the next read covers those lines again; the RDS route holds it for the next chunk.
using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

public sealed class PgDeadlockUnfinishedReportTests
{
    private const string Prefix = "2026-08-26 22:25:24.100 UTC [1549] ";

    private const string Header = Prefix + "ERROR:  deadlock detected\n";

    private const string DetailFirst =
        Prefix + "DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n";

    private const string DetailRest =
        "\tProcess 1549: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n"
        + "\tProcess 1556: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n";

    private const string Detail = DetailFirst + DetailRest;

    private const string Hint = Prefix + "HINT:  See server log for query details.\n";

    /// <summary>Header, DETAIL and HINT: a report the read got whole.</summary>
    private const string Whole = Header + Detail + Hint;

    private const string Other = "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n";

    private static string SecondReport() =>
        Whole.Replace("22:25:24.100", "22:26:24.100", StringComparison.Ordinal).Replace("[1549]", "[1777]", StringComparison.Ordinal);

    private static string SecondReportWithoutHint()
    {
        var second = SecondReport();
        return second[..second.IndexOf("2026-08-26 22:26:24.100 UTC [1777] HINT:", StringComparison.Ordinal)];
    }

    // ---- the parser's two checks ----------------------------------------------------------------------------

    [Theory]
    [InlineData(null, false)]
    [InlineData("", false)]
    public void IsUnfinished_NothingIsNotUnfinished(string? candidate, bool expected)
        => Assert.Equal(expected, PgDeadlockLogParser.IsUnfinished(candidate));

    [Fact]
    public void IsUnfinished_ACandidateWithNoLineAfterItsDetailIsUnfinished()
        => Assert.True(PgDeadlockLogParser.IsUnfinished(Header + Detail));

    [Fact]
    public void IsUnfinished_ACandidateWithItsHintIsNot()
        => Assert.False(PgDeadlockLogParser.IsUnfinished(Whole));

    [Fact]
    public void IsUnfinished_ACandidateWhoseNextLineIsNotAHintIsNot_BecauseNoHintWillComeForIt()
        => Assert.False(PgDeadlockLogParser.IsUnfinished(Header + Detail + Other));

    [Theory]
    [InlineData("quiet", -1)]
    [InlineData("whole", -1)]
    [InlineData("cutBeforeTheHint", 0)]
    [InlineData("followedByOtherLines", -1)]
    [InlineData("headerOnly", 0)]
    [InlineData("headerWithoutItsNewline", 0)]
    [InlineData("detailLineCutShort", 0)]
    [InlineData("continuationLineCutShort", 0)]
    [InlineData("hintCutShort", 0)]
    [InlineData("headerThenOtherLines", -1)]
    public void UnfinishedTailStart_FindsTheReportTheTextEndsInside(string shape, int reportAt)
    {
        var lead = Other + SecondReport();
        var text = shape switch
        {
            "quiet" => lead + Other,
            "whole" => lead + Whole,
            "cutBeforeTheHint" => lead + Header + Detail,
            "followedByOtherLines" => lead + Header + Detail + Other,
            "headerOnly" => lead + Header,
            "headerWithoutItsNewline" => lead + Header.TrimEnd('\n'),
            "detailLineCutShort" => lead + Header + DetailFirst[..60],
            "continuationLineCutShort" => lead + Header + Detail[..^12],
            "hintCutShort" => lead + Header + Detail + Hint[..25],
            _ => lead + Header + Other,
        };

        Assert.Equal(reportAt < 0 ? -1 : lead.Length, PgDeadlockLogParser.UnfinishedTailStart(text));
    }

    [Fact]
    public void UnfinishedTailStart_AReportThatAnotherReportFollowsIsNotTheOneCut()
    {
        /* The first has no HINT, and the second's header follows it: other log lines follow, so it is complete as it
           is. Only the last report of the text can be cut. */
        var text = Header + Detail + SecondReport();
        Assert.Equal(-1, PgDeadlockLogParser.UnfinishedTailStart(text));
        Assert.True(PgDeadlockLogParser.IsUnfinished(Header + Detail));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void UnfinishedTailStart_NothingHasNoTail(string? text)
        => Assert.Equal(-1, PgDeadlockLogParser.UnfinishedTailStart(text));

    // ---- the self-hosted route ------------------------------------------------------------------------------

    private static CollectorContext Context() => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 29, 1, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = TestLogHashKeys.Fixed,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
    };

    /// <summary>The candidate rows of one read, newest first as the query orders them.</summary>
    private static async Task<System.Collections.Generic.List<PgDeadlocksCollector.Row>> ReadAsync(params string[] candidatesNewestFirst)
    {
        var table = new DataTable();
        table.Columns.Add("report_text", typeof(string));
        table.Columns.Add("log_timezone", typeof(string));

        foreach (var candidate in candidatesNewestFirst)
        {
            table.Rows.Add(candidate, "UTC");
        }

        using var reader = new DataTableReader(table);
        return await PgDeadlocksCollector.Instance.ReadAsync(reader, Context(), CancellationToken.None);
    }

    [Fact]
    public async Task ACandidateCutAtTheEndOfTheRead_IsNotStored_AndTheNextReadStoresItOnce()
    {
        var first = await ReadAsync(Header + Detail);
        Assert.Empty(first);

        /* The next read covers those lines again, and this time the HINT is there. */
        var second = await ReadAsync(Whole);
        var stored = Assert.Single(second);
        Assert.Equal(1549, stored.VictimPid);

        /* Stored once across the two reads, and under the hash the whole report has. */
        Assert.Equal(PgDeadlockLogParser.Extract(Whole).Single().DeadlockHash, stored.DeadlockHash);
    }

    [Fact]
    public async Task ACandidateThatOtherLinesFollow_IsStoredAtOnce()
    {
        /* The newest candidate has the line after its DETAIL, though that line is not a HINT. */
        var rows = await ReadAsync(Header + Detail + Other);
        Assert.Single(rows);
    }

    [Fact]
    public async Task OnlyTheNewestCandidateCanBeTheOneTheReadCut()
    {
        /* Newest first: the second report is whole, and the older one has no line after its DETAIL only because the
           second report's header follows it. It is complete as it is. */
        var rows = await ReadAsync(SecondReport(), Header + Detail);
        Assert.Equal(2, rows.Count);

        /* The newest one cut, the older one not: one stored now, the cut one on the next read. */
        var cutNewest = await ReadAsync(SecondReportWithoutHint(), Header + Detail);
        Assert.Equal(1549, Assert.Single(cutNewest).VictimPid);
    }

    /// <summary>
    /// The self-hosted route skips an unfinished report because the next read covers its lines again. The next read
    /// starts at the first line inside the last <see cref="PgServerLogTail.ResumeOverlapBytes"/> of what this read
    /// returned (<see cref="PgServerLogTail.NextResumeOffset"/>, the C# twin of the resume CTE's rule), so a report
    /// cut at the end of a read, which is a few hundred bytes long, starts after that offset; and a read shorter than
    /// the overlap starts again where it began.
    /// </summary>
    [Fact]
    public void TheNextReadStartsBeforeAnUnfinishedReportAtTheEndOfARead()
    {
        const long readFrom = 1_000_000;
        var filler = string.Concat(Enumerable.Repeat(Other, (2 * PgServerLogTail.ResumeOverlapBytes / Other.Length) + 1));
        var reportStart = readFrom + Encoding.UTF8.GetByteCount(filler);

        var next = PgServerLogTail.NextResumeOffset(Encoding.UTF8.GetBytes(filler + Header + Detail), readFrom);

        Assert.True(next > readFrom, "a long read moves the marker forward");
        Assert.True(next <= reportStart, "and not past the start of the report the read ended inside");
        Assert.True(reportStart - next <= PgServerLogTail.ResumeOverlapBytes, "which lies inside the last megabyte the next read covers");

        Assert.Equal(readFrom, PgServerLogTail.NextResumeOffset(Encoding.UTF8.GetBytes(Header + Detail), readFrom));
    }

    // ---- the RDS route --------------------------------------------------------------------------------------

    [Fact]
    public void ABlockSplitAcrossTwoChunks_IsStoredOnce_Whole()
    {
        var chunk1 = Other + Header + Detail;
        var chunk2 = Hint + Other;

        /* What the first chunk alone would have stored: a report the HINT had not arrived for. */
        var step1 = RdsDeadlockCarry.Step(string.Empty, chunk1, moreCanArrive: true);
        Assert.Equal(Other, step1.Text);
        Assert.Equal(Header + Detail, step1.Next);
        Assert.False(step1.StoredUnfinished);
        Assert.Empty(PgDeadlockLogParser.Extract(step1.Text));

        var step2 = RdsDeadlockCarry.Step(step1.Next, chunk2, moreCanArrive: true);
        Assert.Equal(Header + Detail + Hint + Other, step2.Text);
        Assert.Equal(string.Empty, step2.Next);
        Assert.False(step2.StoredUnfinished);

        var stored = Assert.Single(PgDeadlockLogParser.Extract(step2.Text));
        Assert.Equal(PgDeadlockLogParser.Extract(Whole).Single().DeadlockHash, stored.DeadlockHash);
    }

    [Fact]
    public void AFragmentCutMidDetail_WouldHaveBeenStoredUnderAnotherHash()
    {
        /* The defect, for the record: a chunk that ends after the first two DETAIL lines stores a smaller deadlock. */
        var fragment = Assert.Single(PgDeadlockLogParser.Extract(Header + DetailFirst));
        var whole = Assert.Single(PgDeadlockLogParser.Extract(Whole));
        Assert.NotEqual(whole.DeadlockHash, fragment.DeadlockHash);

        /* Held instead, it is stored once, as the whole. */
        var step1 = RdsDeadlockCarry.Step(string.Empty, Header + DetailFirst, moreCanArrive: true);
        Assert.Empty(PgDeadlockLogParser.Extract(step1.Text));
        var step2 = RdsDeadlockCarry.Step(step1.Next, DetailRest + Hint, moreCanArrive: true);
        Assert.Equal(whole.DeadlockHash, Assert.Single(PgDeadlockLogParser.Extract(step2.Text)).DeadlockHash);
    }

    [Fact]
    public void ABlockThatOtherLinesFollow_IsStoredAtOnceAndNothingIsHeld()
    {
        var step = RdsDeadlockCarry.Step(string.Empty, Header + Detail + Other, moreCanArrive: true);

        Assert.Equal(Header + Detail + Other, step.Text);
        Assert.Equal(string.Empty, step.Next);
        Assert.False(step.StoredUnfinished);
        Assert.Single(PgDeadlockLogParser.Extract(step.Text));
    }

    [Fact]
    public void ABlockStillUnfinishedAfterOneExtraChunk_IsStoredAsItIs()
    {
        var step1 = RdsDeadlockCarry.Step(string.Empty, Header + DetailFirst, moreCanArrive: true);
        Assert.Equal(Header + DetailFirst, step1.Next);

        /* One full extra chunk arrived and the HINT is still not in it: not held again. */
        var step2 = RdsDeadlockCarry.Step(step1.Next, DetailRest, moreCanArrive: true);
        Assert.Equal(Header + Detail, step2.Text);
        Assert.Equal(string.Empty, step2.Next);
        Assert.True(step2.StoredUnfinished);
        Assert.Single(PgDeadlockLogParser.Extract(step2.Text));
    }

    [Fact]
    public void ABlockThatTheNextChunkFinishes_LetsANewUnfinishedBlockBeHeldAfresh()
    {
        var step1 = RdsDeadlockCarry.Step(string.Empty, Header + Detail, moreCanArrive: true);

        /* Chunk two ends the first report and begins a second, cut: the first is stored, the second is held. */
        var step2 = RdsDeadlockCarry.Step(step1.Next, Hint + SecondReportWithoutHint(), moreCanArrive: true);

        Assert.Equal(Header + Detail + Hint, step2.Text);
        Assert.StartsWith("2026-08-26 22:26:24.100", step2.Next, StringComparison.Ordinal);
        Assert.False(step2.StoredUnfinished);
    }

    [Fact]
    public void TheLastChunkOfAFile_StoresTheUnfinishedBlock_BecauseNothingMoreCanArrive()
    {
        var step = RdsDeadlockCarry.Step(string.Empty, Header + Detail, moreCanArrive: false);

        Assert.Equal(Header + Detail, step.Text);
        Assert.Equal(string.Empty, step.Next);
        Assert.True(step.StoredUnfinished);
    }

    [Fact]
    public void ABlockLargerThanTheCarryBound_IsStoredAsItIs()
    {
        var huge = Header + Prefix + "DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
            + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
            + "\tProcess 1549: " + new string('x', RdsDeadlockCarry.MaxCarryLength) + "\n";

        var step = RdsDeadlockCarry.Step(string.Empty, huge, moreCanArrive: true);

        Assert.Equal(string.Empty, step.Next);
        Assert.True(step.StoredUnfinished);
    }

    [Fact]
    public void AnEmptyChunk_LeavesTheHeldBlockAloneAndAgesNothing()
    {
        var step = RdsDeadlockCarry.Step(Header + Detail, string.Empty, moreCanArrive: true);

        Assert.Equal(string.Empty, step.Text);
        Assert.Equal(Header + Detail, step.Next);
        Assert.False(step.StoredUnfinished);
    }

    [Fact]
    public void TheBookHoldsOneBlockPerInstanceAndFile_AndARotationStartsFresh()
    {
        var book = new RdsDeadlockCarryBook();

        var (held0, key, file) = book.CarryFor("inst-a|error/postgresql.log.2026-08-26-22");
        Assert.Equal(string.Empty, held0);
        Assert.Equal("inst-a", key);

        book.Commit(key, file, Header + Detail);

        Assert.Equal(Header + Detail, book.CarryFor("inst-a|error/postgresql.log.2026-08-26-22").Held);

        /* Another instance, and the next file of the same instance, hold nothing of it. */
        Assert.Equal(string.Empty, book.CarryFor("inst-b|error/postgresql.log.2026-08-26-22").Held);
        Assert.Equal(string.Empty, book.CarryFor("inst-a|error/postgresql.log.2026-08-26-23").Held);

        book.Commit(key, file, string.Empty);
        Assert.Equal(string.Empty, book.CarryFor("inst-a|error/postgresql.log.2026-08-26-22").Held);
    }
}
