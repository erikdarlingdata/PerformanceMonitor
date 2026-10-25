using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A blocking chain is graded on how long it has held people up, not only how wide it is, and a chain still being
/// sampled when the window ends says so. An idle session holding key locks for 13 minutes blocked only two others;
/// graded on structure alone it ranked below days-old plan regressions and its advice read as a chain that had cleared.
/// </summary>
public class BlockingChainDurationTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public BlockingChainDurationTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static Fact ChainFact(double depth, double victims, double maxWaitMs)
    {
        var fact = new Fact
        {
            Source = "blocking",
            Key = "BLOCKING_CHAIN",
            Value = depth,
            Metadata = new Dictionary<string, double>
            {
                ["worst_chain_depth"] = depth,
                ["worst_chain_victim_count"] = victims,
                ["worst_chain_max_wait_ms"] = maxWaitMs
            }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        return fact;
    }

    /* ── scoring ── */

    [Fact]
    public void Score_TwoVictimsWaitingThirteenMinutes_ReadsCriticalAndOutranksAFiveFoldPlanRegression()
    {
        var chain = ChainFact(depth: 1, victims: 2, maxWaitMs: 780_000);

        var fiveFoldPlanRegression = FactScorer.ApplyThresholdFormula(5, 2, 10);
        Assert.Equal(1.0, chain.BaseSeverity, precision: 3);
        Assert.True(chain.BaseSeverity > fiveFoldPlanRegression);
    }

    [Fact]
    public void Score_TwoVictimsWaitingFiveSeconds_IsGradedOnStructureAsBefore()
    {
        // depth 1 -> 0.167, two victims -> 0.2, a 5 s wait -> 0.083: the victims lead, as they always did.
        Assert.Equal(0.2, ChainFact(depth: 1, victims: 2, maxWaitMs: 5_000).BaseSeverity, precision: 3);
    }

    [Theory]
    [InlineData(30_000, 0.5)]    // the concerning mark
    [InlineData(165_000, 0.75)]  // midway between 30 s and 300 s
    [InlineData(300_000, 1.0)]   // the critical mark
    [InlineData(3_600_000, 1.0)] // past it stays at 1.0
    public void Score_DurationAlone_FollowsTheConcerningAndCriticalMarks(double maxWaitMs, double expected)
    {
        Assert.Equal(expected, ChainFact(depth: 1, victims: 1, maxWaitMs: maxWaitMs).BaseSeverity, precision: 3);
    }

    /* ── the fact builder ── */

    private static BlockingPairRow Pair(DateTime at, int blocked, int blocking, long waitMs, DateTime? blockingTran = null) => new()
    {
        EventTime = at,
        BlockedSpid = blocked,
        BlockingSpid = blocking,
        WaitTimeMs = waitMs,
        BlockingTranStarted = blockingTran,
        BlockingStatus = "suspended"
    };

    private static Fact Build(List<BlockingPairRow> rows, DateTime windowEnd, int intervalMinutes = 1)
    {
        var reconstruction = BlockingChainReconstructor.Reconstruct(rows, 50, 5000, 100_000, scopeByMonitorLoop: false);
        return BlockingChainFactBuilder.Build(
            new AnalysisContext { ServerId = 7, TimeRangeStart = windowEnd.AddHours(-4), TimeRangeEnd = windowEnd },
            reconstruction, rows, intervalMinutes);
    }

    [Fact]
    public void Build_PicksTheChainWithTheHighestGrade_NotTheWidestOne()
    {
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow>
        {
            // a wide pile-up (one head, six victims) that lasted seconds ...
            Pair(end.AddMinutes(-90), 301, 300, 4_000), Pair(end.AddMinutes(-90), 302, 300, 4_000),
            Pair(end.AddMinutes(-90), 303, 300, 4_000), Pair(end.AddMinutes(-90), 304, 300, 4_000),
            Pair(end.AddMinutes(-90), 305, 300, 4_000), Pair(end.AddMinutes(-90), 306, 300, 4_000),
            // ... and a narrow one a session held for 13 minutes
            Pair(end.AddMinutes(-1), 501, 500, 780_000), Pair(end.AddMinutes(-1), 502, 500, 780_000)
        };

        var fact = Build(rows, end);

        Assert.Equal(500, fact.Metadata["worst_apex_spid"]);
        Assert.Equal(780_000, fact.Metadata["worst_chain_max_wait_ms"]);
        Assert.Equal(2, fact.Metadata["total_reconstructed_chains"]);
    }

    [Fact]
    public void Build_NewestSampleWithinTwoIntervalsOfTheWindowEnd_IsOpen()
    {
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow> { Pair(end.AddSeconds(-110), 501, 500, 60_000) };

        var fact = Build(rows, end, intervalMinutes: 1);

        Assert.Equal(1, fact.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(110_000, fact.Metadata["worst_chain_last_seen_age_ms"]);
    }

    [Fact]
    public void Build_NewestSampleOlderThanTwoIntervals_IsClosed_AndASlowerCadenceWidensTheBound()
    {
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow> { Pair(end.AddMinutes(-4), 501, 500, 60_000) };

        Assert.Equal(0, Build(rows, end, intervalMinutes: 1).Metadata["worst_chain_open_at_end"]);
        Assert.Equal(1, Build(rows, end, intervalMinutes: 5).Metadata["worst_chain_open_at_end"]);
    }

    [Fact]
    public void Build_UsesTheChainsOwnNewestSample_NotAnotherChainsOrAnEarlierLongerWaitRow()
    {
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow>
        {
            // the worst chain was last seen an hour ago; an unrelated chain is live now
            Pair(end.AddMinutes(-60), 501, 500, 780_000),
            Pair(end.AddMinutes(-30), 601, 600, 5_000)
        };

        Assert.Equal(0, Build(rows, end).Metadata["worst_chain_open_at_end"]);
    }

    [Fact]
    public void Build_CarriesTheHeadsTransactionStartOnlyWhenTheReportHadOne()
    {
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var started = new DateTime(2026, 10, 25, 11, 47, 0);

        var withTran = Build(new List<BlockingPairRow> { Pair(end.AddMinutes(-1), 501, 500, 60_000, started) }, end);
        var withoutTran = Build(new List<BlockingPairRow> { Pair(end.AddMinutes(-1), 501, 500, 60_000) }, end);

        Assert.Equal((started - DateTime.UnixEpoch).TotalSeconds, withTran.Metadata["worst_apex_tran_started_clock_s"]);
        Assert.DoesNotContain("worst_apex_tran_started_clock_s", withoutTran.Metadata.Keys);
    }

    /* ── advice ── */

    private static AdviceBlock Advice(bool? open, double? tranClock = null)
    {
        var fact = new Fact
        {
            Key = "BLOCKING_CHAIN",
            Metadata = new Dictionary<string, double>
            {
                ["worst_chain_depth"] = 2,
                ["worst_chain_victim_count"] = 2,
                ["worst_apex_spid"] = 57,
                ["worst_apex_sleeping"] = 0,
                ["worst_chain_max_wait_ms"] = 780_000
            }
        };
        if (open is bool o) fact.Metadata["worst_chain_open_at_end"] = o ? 1 : 0;
        if (tranClock is double c) fact.Metadata["worst_apex_tran_started_clock_s"] = c;
        return FactAdvice.Compose("BLOCKING_CHAIN", new[] { fact }.ToFactLookup())!;
    }

    [Fact]
    public void Advice_ChainOpenAtTheWindowEnd_SaysItWasStillBlocking_AndNamesTheHeadWaitAndTransaction()
    {
        var started = new DateTime(2026, 10, 25, 11, 47, 5);
        var advice = Advice(open: true, tranClock: (started - DateTime.UnixEpoch).TotalSeconds);

        Assert.Contains("still blocking at the end of the window", advice.Headline);
        Assert.Contains("session 57 was at the head", advice.Investigation);
        Assert.Contains("13 min", advice.Investigation);
        Assert.Contains("2026-10-25 11:47:05", advice.Investigation);
        Assert.DoesNotContain("cleared long ago", advice.Remediation);
        Assert.Contains("still blocking at the end of the window", advice.Remediation);
    }

    [Fact]
    public void Advice_OpenChainWithoutATransactionStart_OmitsIt()
    {
        var advice = Advice(open: true);

        Assert.Contains("session 57 was at the head", advice.Investigation);
        Assert.DoesNotContain("transaction open since", advice.Investigation);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(null)]
    public void Advice_ChainThatEndedEarlier_KeepsTheClearedText(bool? open)
    {
        var advice = Advice(open);

        Assert.Contains("cleared long ago", advice.Remediation);
        Assert.DoesNotContain("still blocking at the end of the window", advice.Headline + advice.Investigation + advice.Remediation);
    }

    /* ── collected from Lite's DuckDB store ── */

    private async Task<Fact> CollectChainAsync(DateTime lastSeen, int minutes)
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();
        await seeder.SeedLiveBlockingChainAsync(lastSeen, minutes);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(TestDataSeeder.CreateTestContext());
        new FactScorer().ScoreAll(facts);
        return Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN");
    }

    [Fact]
    public async Task Collect_ChainSampledUpToTheWindowEnd_IsOpen_AndGradesCriticalOnItsThirteenMinuteWait()
    {
        var chain = await CollectChainAsync(TestDataSeeder.TestPeriodEnd.AddSeconds(-30), minutes: 13);

        Assert.Equal(1, chain.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(780_000, chain.Metadata["worst_chain_max_wait_ms"]);
        Assert.Equal(500, chain.Metadata["worst_apex_spid"]);
        Assert.Equal(2, chain.Metadata["worst_chain_victim_count"]);
        Assert.Equal(1.0, chain.BaseSeverity, precision: 3);
        Assert.True(chain.Metadata.ContainsKey("worst_apex_tran_started_clock_s"));
    }

    [Fact]
    public async Task Collect_ChainThatEndedHoursBeforeTheWindowEnd_IsClosed()
    {
        var chain = await CollectChainAsync(TestDataSeeder.TestPeriodEnd.AddHours(-3), minutes: 13);

        Assert.Equal(0, chain.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(780_000, chain.Metadata["worst_chain_max_wait_ms"]);
    }
}
