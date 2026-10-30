using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
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

    private static BlockingPairRow Pair(DateTime at, int blocked, int blocking, long waitMs, DateTime? blockingTran = null,
        int blockedEcid = 0, int blockingEcid = 0) => new()
    {
        EventTime = at,
        BlockedSpid = blocked,
        BlockedEcid = blockedEcid,
        BlockingSpid = blocking,
        BlockingEcid = blockingEcid,
        WaitTimeMs = waitMs,
        BlockingTranStarted = blockingTran,
        BlockingStatus = "suspended"
    };

    private static Fact Build(List<BlockingPairRow> rows, DateTime windowEnd, int intervalMinutes = 1, DateTime? asOfUtc = null)
    {
        var reconstruction = BlockingChainReconstructor.Reconstruct(rows, 50, 5000, 100_000, scopeByMonitorLoop: true);
        return BlockingChainFactBuilder.Build(
            new AnalysisContext { ServerId = 7, TimeRangeStart = windowEnd.AddHours(-4), TimeRangeEnd = windowEnd, AsOfUtc = asOfUtc },
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
    public void Build_DmvOnlyServerOnAFiveMinuteCollector_OneLongBlockIsOneEpisode_AndStillOpen()
    {
        // No blocked-process report (an RDS Web or Express server): the DMV snapshot is the only source, sampled every
        // 5 minutes. One lead blocker held for 30 minutes is seven samples. A fixed 2-minute episode gap counted
        // seven episodes and looked for the newest sample only 2 minutes past the worst scan, so a live block read
        // as closed (#6931).
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow>();
        for (var i = 0; i < 7; i++)
            rows.Add(Pair(end.AddMinutes(-33 + 5 * i), 501, 500, 60_000 * (i + 1)));

        var fact = Build(rows, end, intervalMinutes: 5);

        Assert.Equal(1, fact.Metadata["total_reconstructed_chains"]);
        Assert.Equal(1, fact.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(TimeSpan.FromMinutes(3).TotalMilliseconds, fact.Metadata["worst_chain_last_seen_age_ms"]);
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

    [Fact]
    public void Build_AnIntervalAboveTheCeiling_DoesNotCallAnyChainOpen()
    {
        // frequency_minutes = 0 resolves to the daily on-load recapture (1440): two intervals reach back two days,
        // so a chain last seen four hours before the window end would read as open. The fact says nothing instead.
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow> { Pair(end.AddHours(-4), 501, 500, 780_000), Pair(end.AddHours(-4), 502, 500, 780_000) };

        var daily = Build(rows, end, intervalMinutes: CollectorScheduleDefaults.OnLoadRecaptureMinutes);
        var atCeiling = Build(rows, end, intervalMinutes: BlockingChainFactBuilder.MaxOpenAtEndIntervalMinutes);

        Assert.DoesNotContain("worst_chain_open_at_end", daily.Metadata.Keys);
        Assert.Equal(TimeSpan.FromHours(4).TotalMilliseconds, daily.Metadata["worst_chain_last_seen_age_ms"]);
        Assert.Equal(0, atCeiling.Metadata["worst_chain_open_at_end"]);
    }

    [Fact]
    public void BlockingInterval_IsTheSlowerCollectorsEffectiveSchedule_AndTheDefaultUsesTheSameExpression()
    {
        Assert.Equal(5, CollectorScheduleDefaults.EffectiveBlockingIntervalMinutes(name => name == "blocked_process_report" ? 5 : 1));
        Assert.Equal(CollectorScheduleDefaults.OnLoadRecaptureMinutes,
            CollectorScheduleDefaults.EffectiveBlockingIntervalMinutes(name => name == "dmv_blocking_snapshot" ? 0 : 1));
        Assert.Equal(
            CollectorScheduleDefaults.EffectiveBlockingIntervalMinutes(name => CollectorScheduleDefaults.All[name].FrequencyMinutes),
            CollectorScheduleDefaults.DefaultBlockingIntervalMinutes);
    }

    [Fact]
    public void Build_TheSameSpidUnderAnotherEcid_IsAnotherSession_AndDoesNotKeepTheChainOpen()
    {
        // 500/0 blocked 501/0 for 13 minutes, last seen an hour before the window end. A separate parallel pair, 500/1
        // blocking 501/1 a few seconds before the end, shares both spids but not the sessions.
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow>
        {
            Pair(end.AddMinutes(-60), 501, 500, 780_000),
            Pair(end.AddSeconds(-5), 501, 500, 1_000, blockedEcid: 1, blockingEcid: 1)
        };

        var fact = Build(rows, end);

        Assert.Equal(2, fact.Metadata["total_reconstructed_chains"]);
        Assert.Equal(780_000, fact.Metadata["worst_chain_max_wait_ms"]);
        Assert.Equal(0, fact.Metadata["worst_chain_open_at_end"]);
        Assert.Equal(TimeSpan.FromMinutes(60).TotalMilliseconds, fact.Metadata["worst_chain_last_seen_age_ms"]);
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

    [Fact]
    public void Advice_OpenChainInAnAnchoredWindow_StatesItAtThatWindowsEnd_WithoutTheKillAdvice()
    {
        // as_of reads a window that ended in the past: "may be holding locks right now ... KILL" would be false.
        var end = new DateTime(2026, 10, 20, 12, 0, 0);
        var rows = new List<BlockingPairRow> { Pair(end.AddSeconds(-20), 501, 500, 780_000), Pair(end.AddSeconds(-20), 502, 500, 780_000) };

        var anchored = Build(rows, end, asOfUtc: end);
        var live = Build(rows, end);
        var anchoredAdvice = FactAdvice.Compose("BLOCKING_CHAIN", new[] { anchored }.ToFactLookup())!;
        var liveAdvice = FactAdvice.Compose("BLOCKING_CHAIN", new[] { live }.ToFactLookup())!;

        Assert.Equal(1, anchored.Metadata["worst_chain_open_at_end"]);
        Assert.Contains("still blocking at the end of the window", anchoredAdvice.Headline);
        Assert.Contains("still blocking at the end of", anchoredAdvice.Remediation);
        Assert.DoesNotContain("KILL", anchoredAdvice.Remediation);
        Assert.DoesNotContain("right now", anchoredAdvice.Remediation);
        Assert.Contains("KILL", liveAdvice.Remediation);
        Assert.Contains("right now", liveAdvice.Remediation);
    }

    [Fact]
    public void Advice_ChainThatEndedEarlier_KeepsTheClearedText()
    {
        var advice = Advice(open: false);

        Assert.Contains("cleared long ago", advice.Remediation);
        Assert.DoesNotContain("still blocking at the end of the window", advice.Headline + advice.Investigation + advice.Remediation);
    }

    [Fact]
    public void Advice_ChainWithAnUnknownEnd_SaysTheCollectorIsTooCoarse_NotClearedAndNotKill()
    {
        // A live window whose collector interval is above the ceiling carries no open-at-end key: the data cannot
        // say the chain cleared, nor that it is still open.
        var end = new DateTime(2026, 10, 25, 12, 0, 0);
        var rows = new List<BlockingPairRow> { Pair(end.AddHours(-4), 501, 500, 780_000), Pair(end.AddHours(-4), 502, 500, 780_000) };
        var daily = Build(rows, end, intervalMinutes: CollectorScheduleDefaults.OnLoadRecaptureMinutes);
        var advice = FactAdvice.Compose("BLOCKING_CHAIN", new[] { daily }.ToFactLookup())!;

        Assert.DoesNotContain("worst_chain_open_at_end", daily.Metadata.Keys);
        Assert.Contains("too seldom to tell whether this chain is still open", advice.Remediation);
        Assert.Contains("current blocking", advice.Remediation);
        Assert.DoesNotContain("cleared long ago", advice.Remediation);
        Assert.DoesNotContain("KILL", advice.Remediation);
        Assert.DoesNotContain("still blocking at the end of the window", advice.Headline + advice.Investigation);
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

    /* ── the drill-down lists the chain the fact describes ── */

    private long _nextReportId = -7_000;

    private async Task InsertReportAsync(DateTime at, int blocked, int blocking, long waitMs, int monitorLoop = 7)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
    blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid, monitor_loop)
VALUES ($1, $2, $3, $4, $2, 'RankDb', $5, $6, $7, 'X', 'SELECT 1', 'SELECT 2', 0, 0, $8)";
        foreach (var value in new object[] { _nextReportId--, at, TestDataSeeder.TestServerId, TestDataSeeder.TestServerName, blocked, blocking, waitMs, monitorLoop })
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task DrillDown_ListsTheChainTheFactNames_WhenThreeWiderPileUpsOutrankItOnStructure()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();
        var context = TestDataSeeder.CreateTestContext();
        var end = TestDataSeeder.TestPeriodEnd;

        // Three pile-ups, six to eight victims behind one head, each over in seconds ...
        foreach (var (head, victims) in new[] { (300, 6), (310, 7), (320, 8) })
            for (var v = 1; v <= victims; v++)
                await InsertReportAsync(end.AddMinutes(-90), head + v, head, 4_000);
        // ... and one head that held two sessions up for thirteen minutes.
        await InsertReportAsync(end.AddMinutes(-1), 501, 500, 780_000);
        await InsertReportAsync(end.AddMinutes(-1), 502, 500, 780_000);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        var fact = Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN");
        Assert.Equal(4, fact.Metadata["total_reconstructed_chains"]);
        Assert.Equal(500, fact.Metadata["worst_apex_spid"]);

        var finding = new AnalysisFinding { RootFactKey = "BLOCKING_CHAIN", StoryPath = "BLOCKING_CHAIN", PathKeys = ["BLOCKING_CHAIN"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        var shown = System.Text.Json.JsonSerializer.SerializeToElement(finding.DrillDown!["reconstructed_blocking_chains"]);
        var apexes = shown.EnumerateArray().Select(c => c.GetProperty("apex_spid").GetInt32()).ToList();
        Assert.Equal(3, apexes.Count);
        Assert.Contains(500, apexes);
    }

    /* ── #6931: sessions are scoped by scan, so a reused spid never links two unrelated moments ── */

    private static readonly DateTime SpidReuseEnd = TestDataSeeder.TestPeriodEnd;

    /// <summary>The issue's window: spid 132 holds a lock for 142 s early on (re-fired every 5 s), an unrelated
    /// session on spid 133 later blocks a session on spid 132, and a new session on 132 blocks one on 133 an hour after that.</summary>
    private async Task SeedReusedSpidWindowAsync()
    {
        var first = SpidReuseEnd.AddHours(-3.6);
        for (var i = 0; i < 29; i++)
            await InsertReportAsync(first.AddSeconds(5 * i), 133, 132, 5_000 * (i + 1) - 3_000, monitorLoop: 100 + i);
        await InsertReportAsync(SpidReuseEnd.AddHours(-2), 132, 133, 20_000, monitorLoop: 300);
        await InsertReportAsync(SpidReuseEnd.AddHours(-1), 133, 132, 30_000, monitorLoop: 500);
    }

    [Fact]
    public async Task Collect_ReusedSpidsAcrossScans_ReportNoCycle_AndTheRealDepthVictimsAndWait()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();
        await SeedReusedSpidWindowAsync();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(TestDataSeeder.CreateTestContext());
        var fact = Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN");

        Assert.Equal(0, fact.Metadata["cycle_detected"]);
        Assert.Equal(0, fact.Metadata["traversal_truncated"]);
        Assert.Equal(1, fact.Metadata["worst_chain_depth"]);
        Assert.Equal(1, fact.Metadata["worst_chain_victim_count"]);
        Assert.Equal(142_000, fact.Metadata["worst_chain_max_wait_ms"]);
        Assert.Equal(3, fact.Metadata["total_reconstructed_chains"]);
    }

    [Fact]
    public async Task DrillDown_ReusedSpidsAcrossScans_ListsEachEpisodeOnce_WithNoZeroVictimChain()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();
        await SeedReusedSpidWindowAsync();

        var finding = new AnalysisFinding { RootFactKey = "BLOCKING_CHAIN", StoryPath = "BLOCKING_CHAIN", PathKeys = ["BLOCKING_CHAIN"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], TestDataSeeder.CreateTestContext());

        var shown = System.Text.Json.JsonSerializer.SerializeToElement(finding.DrillDown!["reconstructed_blocking_chains"])
            .EnumerateArray().ToList();
        Assert.Equal(3, shown.Count);
        Assert.All(shown, c => Assert.Equal(1, c.GetProperty("victim_count").GetInt32()));
        Assert.Equal(142_000, shown.Max(c => c.GetProperty("max_wait_ms").GetInt64()));
    }

    [Fact]
    public async Task Collect_ARealThreeLevelChainInsideOneScan_StillReportsDepthThree()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();
        var at = SpidReuseEnd.AddHours(-1);
        await InsertReportAsync(at, 11, 10, 9_000, monitorLoop: 5);
        await InsertReportAsync(at, 12, 11, 8_000, monitorLoop: 5);
        await InsertReportAsync(at, 13, 12, 7_000, monitorLoop: 5);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(TestDataSeeder.CreateTestContext());
        var fact = Assert.Single(facts, f => f.Key == "BLOCKING_CHAIN");

        Assert.Equal(3, fact.Metadata["worst_chain_depth"]);
        Assert.Equal(3, fact.Metadata["worst_chain_victim_count"]);
        Assert.Equal(0, fact.Metadata["cycle_detected"]);
    }

    [Fact]
    public void Build_OneEpisodeReFiredEveryFiveSeconds_CountsAsOneEpisode()
    {
        var end = new DateTime(2026, 10, 27, 17, 0, 0);
        var start = end.AddHours(-3.6);
        var rows = Enumerable.Range(0, 29).Select(i => Pair(start.AddSeconds(5 * i), 133, 132, 5_000 * (i + 1) - 3_000)).ToList();

        var fact = Build(rows, end);

        Assert.Equal(1, fact.Metadata["total_reconstructed_chains"]);
        Assert.Equal(142_000, fact.Metadata["worst_chain_max_wait_ms"]);
    }

    [Fact]
    public void Build_TheWorstChainIsNotKeptOpenByAnUnrelatedEpisodeThatReusesItsSpids()
    {
        // #6931: the 13:23 block on 132/133 ended at 13:25 and is the worst chain. An unrelated block on the same
        // spids at 16:08 is another episode: it is not a later sighting of the worst chain.
        var end = new DateTime(2026, 10, 27, 17, 0, 0);
        var at = new DateTime(2026, 10, 27, 13, 23, 0);
        var rows = Enumerable.Range(0, 29).Select(i => Pair(at.AddSeconds(5 * i), 133, 132, 5_000 * (i + 1) - 3_000)).ToList();
        rows.Add(Pair(new DateTime(2026, 10, 27, 16, 8, 0), 133, 132, 20_000));

        var fact = Build(rows, end);

        Assert.Equal(142_000, fact.Metadata["worst_chain_max_wait_ms"]);
        Assert.Equal(0, fact.Metadata["worst_chain_open_at_end"]);
        Assert.Equal((end - at.AddSeconds(140)).TotalMilliseconds, fact.Metadata["worst_chain_last_seen_age_ms"]);
    }

    [Fact]
    public void Build_TheSameApexSpidAnHourLater_CountsAsASecondEpisode()
    {
        // The issue's spid 132: it heads one block early on and an unrelated one much later.
        var end = new DateTime(2026, 10, 27, 17, 0, 0);
        var rows = new List<BlockingPairRow>
        {
            Pair(end.AddHours(-3.6), 133, 132, 142_000),
            Pair(end.AddHours(-3.6).AddSeconds(5), 133, 132, 142_000),
            Pair(end.AddHours(-0.9), 133, 132, 20_000)
        };

        Assert.Equal(2, Build(rows, end).Metadata["total_reconstructed_chains"]);
    }
}
