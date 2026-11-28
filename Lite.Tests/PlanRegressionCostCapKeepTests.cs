/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9888: the plan regression cost read prices at most a cap of candidates per call and names the rows it left out, and it keeps its
/// rows for a server and window so a repeat read does not run the statement again. Counts through test hooks, never timing.
/// </summary>
public sealed class PlanRegressionCostCapKeepTests : IDisposable
{
    private const int ServerId = 9888;
    private const int Candidates = 60;
    private static readonly DateTime Now = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_9888_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public PlanRegressionCostCapKeepTests()
    {
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        Seed(Candidates);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best effort */ }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private string Row(long query, long plan, string hash, long interval, DateTime collected, DateTime lastExec, long execs, double cpu) =>
        $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id, interval_start_time_utc, avg_rowcount)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db1', {query}, {plan}, 'Regular', {Ts(lastExec.AddHours(-1))}, {Ts(lastExec)},
     'SELECT {query} FROM dbo.t{query}', '0xQ{query}', {execs}, {cpu}, {cpu + 20000}, '{hash}', false, 0, {interval}, {Ts(lastExec.AddHours(-1))}, 10)";

    /// <summary>Queries with a cheap older plan and a costly newest plan, each costlier than the one before, so the ranking is unambiguous.</summary>
    private void Seed(int queries)
    {
        var statements = new List<string>();
        for (var q = 1; q <= queries; q++)
        {
            var cheap = q * 10 + 1;
            var costly = q * 10 + 2;
            for (var interval = 0; interval < 2; interval++)
            {
                var coldLast = Now.AddDays(-5).AddHours(interval);
                var hotLast = Now.AddHours(-1).AddMinutes(interval * 10);
                statements.Add(Row(q, cheap, "0xCHEAP" + cheap, 1 + interval, coldLast.AddMinutes(1), coldLast, 30, 20_000));
                statements.Add(Row(q, cheap, "0xCHEAP" + cheap, 1 + interval, coldLast.AddMinutes(2), coldLast, 60, 20_000));
                statements.Add(Row(q, costly, "0xCOSTLY" + costly, 11 + interval, hotLast.AddMinutes(1), hotLast, 30, 200_000 + q * 1_000));
                statements.Add(Row(q, costly, "0xCOSTLY" + costly, 11 + interval, hotLast.AddMinutes(2), hotLast, 60, 200_000 + q * 1_000));
            }
        }

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }

    private static AnalysisContext Context(TimeSpan? back = null, int serverId = ServerId) => new()
    {
        ServerId = serverId,
        ServerName = "TestSrv",
        TimeRangeStart = Now.AddHours(-(back ?? TimeSpan.FromHours(4)).TotalHours),
        TimeRangeEnd = Now,
    };

    private async Task<(List<DuckDbFactCollector.PlanRegressionCandidate> Rows, DuckDbFactCollector.PlanRegressionStatementTime Time)> ReadCostAsync(
        DuckDbFactCollector collector, AnalysisContext context)
    {
        var token = TestContext.Current.CancellationToken;
        var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14), token);
        return await collector.ReadPlanRegressionCostAsync(context, plan);
    }

    [Fact]
    public async Task WithTheCapBelowTheCandidates_TheReadPricesExactlyTheCap_AndCountsTheRest()
    {
        var collector = new DuckDbFactCollector(_duckDb) { PlanRegressionCostCapForTests = 25 };

        var (rows, time) = await ReadCostAsync(collector, Context());

        Assert.Equal(25, rows.Count);
        Assert.Equal(Candidates - 25, time.LeftOut);
        /* The cut keeps the worst: the costliest newest plans lost the most CPU. */
        Assert.Equal(Enumerable.Range(Candidates - 24, 25).Select(q => (long)q).OrderBy(q => q), rows.Select(r => r.QueryId).OrderBy(q => q));
    }

    [Fact]
    public async Task TheFactNamesHowManyCandidatesItPricedAndHowManyItLeftOut()
    {
        var collector = new DuckDbFactCollector(_duckDb) { PlanRegressionCostCapForTests = 25 };
        var facts = new List<Fact>();

        await collector.CollectPlanRegressionFactsAsync(Context(), facts);

        var fact = Assert.Single(facts);
        Assert.Equal(25, fact.Metadata["cost_candidates_priced"]);
        Assert.Equal(Candidates - 25, fact.Metadata["cost_candidates_left_out"]);
    }

    [Fact]
    public async Task ARepeatReadInsideTheKeepWindow_DoesNotRunTheStatementAgain_AndAChangedWindowStartDoes()
    {
        var collector = new DuckDbFactCollector(_duckDb);

        await ReadCostAsync(collector, Context());
        Assert.Equal(1, collector.PlanRegressionCostStatementRunsForTests);

        var (_, repeat) = await ReadCostAsync(collector, Context());
        Assert.Equal(1, collector.PlanRegressionCostStatementRunsForTests);
        Assert.True(repeat.FromKeep);

        var (_, moved) = await ReadCostAsync(collector, Context(TimeSpan.FromHours(5)));
        Assert.Equal(2, collector.PlanRegressionCostStatementRunsForTests);
        Assert.False(moved.FromKeep);
    }

    [Fact]
    public async Task TheFactsWithTheKeepAreTheFactsWithout()
    {
        var kept = new DuckDbFactCollector(_duckDb);
        var factsFirst = new List<Fact>();
        var factsFromKeep = new List<Fact>();
        await kept.CollectPlanRegressionFactsAsync(Context(), factsFirst);
        await kept.CollectPlanRegressionFactsAsync(Context(), factsFromKeep);
        var factsFresh = new List<Fact>();
        await new DuckDbFactCollector(_duckDb) { CostKeep = new PlanRegressionCostKeep() }.CollectPlanRegressionFactsAsync(Context(), factsFresh);

        Assert.Equal(1, kept.PlanRegressionCostStatementRunsForTests);
        var fresh = Assert.Single(factsFresh);
        foreach (var fact in new[] { Assert.Single(factsFirst), Assert.Single(factsFromKeep) })
        {
            Assert.Equal(fresh.Value, fact.Value);
            Assert.Equal(fresh.Metadata.OrderBy(m => m.Key), fact.Metadata.OrderBy(m => m.Key));
        }
    }

    [Fact]
    public async Task ACostReadThatFindsNothing_NamesItsScanInTheReplyState_OnceAndNotWhenKeptOrWhenItPriced()
    {
        const int quietServer = ServerId + 1;
        var collector = new DuckDbFactCollector(_duckDb);

        var first = Context(serverId: quietServer);
        var facts = new List<Fact>();
        await collector.CollectPlanRegressionFactsAsync(first, facts);

        Assert.Empty(facts);
        Assert.Matches(@"^The plan regression cost read found no candidate and its scan of the archive window took \d+ ms", first.PlanRegressionCostNote);
        Assert.Equal(first.PlanRegressionCostNote, CollectionCaveatState.From(first).PlanRegressionCostNote);

        /* A repeat inside the keep window ran no statement, so there is no scan to name. */
        var repeat = Context(serverId: quietServer);
        await collector.CollectPlanRegressionFactsAsync(repeat, new List<Fact>());
        Assert.Null(repeat.PlanRegressionCostNote);

        /* A read that priced candidates carries no floor note. */
        var priced = Context();
        await collector.CollectPlanRegressionFactsAsync(priced, new List<Fact>());
        Assert.Null(priced.PlanRegressionCostNote);
        Assert.Null(CollectionCaveatState.From(priced).PlanRegressionCostNote);
    }

    /* ── #9957: the keep answers reads whose end is the clock ─────────────────────────────────────────────────────────────────────── */

    private sealed class ManualTime : TimeProvider
    {
        private long _ticks;
        public override long GetTimestamp() => _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public void Advance(TimeSpan by) => _ticks += by.Ticks;
    }

    /// <summary>A window ending at <paramref name="end"/>: <paramref name="clock"/> true is a read with no as_of, false an anchored one.</summary>
    private static AnalysisContext At(DateTime end, bool clock, TimeSpan? back = null) => new()
    {
        ServerId = ServerId,
        ServerName = "TestSrv",
        TimeRangeStart = end - (back ?? TimeSpan.FromHours(4)),
        TimeRangeEnd = end,
        WindowEndsAtClock = clock,
    };

    /// <summary>The cost read as the pass makes it: the window resolved first, the archive plan taken for its start.</summary>
    private async Task<(List<DuckDbFactCollector.PlanRegressionCandidate> Rows, DuckDbFactCollector.PlanRegressionStatementTime Time)> ReadClockCostAsync(
        DuckDbFactCollector collector, AnalysisContext context)
    {
        var window = await collector.ResolvePlanRegressionCostWindowAsync(context);
        var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", window.ScanStart, TestContext.Current.CancellationToken);
        return await collector.ReadPlanRegressionCostAsync(context, plan, window);
    }

    private void InsertRow(DateTime collected, DateTime lastExec)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = Row(1, 19_001, "0xLATE", 99, collected, lastExec, 30, 20_000);
        cmd.ExecuteNonQuery();
    }

    private static void AssertSameFacts(Fact expected, Fact actual, string? except = null)
    {
        Assert.Equal(expected.Value, actual.Value);
        Assert.Equal(
            expected.Metadata.Where(m => m.Key != except).OrderBy(m => m.Key),
            actual.Metadata.Where(m => m.Key != except).OrderBy(m => m.Key));
    }

    [Fact]
    public async Task TwoUnanchoredReadsASecondApart_RunTheStatementOnce_AndReturnTheFactsOfReadsWithoutTheKeep()
    {
        var time = new ManualTime();
        var keep = new PlanRegressionCostKeep(time);
        var pass = new DuckDbFactCollector(_duckDb) { CostKeep = keep };
        var read = new DuckDbFactCollector(_duckDb) { CostKeep = keep };

        var first = new List<Fact>();
        await pass.CollectPlanRegressionFactsAsync(At(Now.AddSeconds(10), clock: true), first);
        time.Advance(TimeSpan.FromSeconds(1));
        var second = new List<Fact>();
        await read.CollectPlanRegressionFactsAsync(At(Now.AddSeconds(11), clock: true), second);

        Assert.Equal(1, keep.StatementRuns);
        Assert.Equal(1, keep.Hits);
        Assert.Equal(1, pass.PlanRegressionCostStatementRunsForTests);
        Assert.Equal(0, read.PlanRegressionCostStatementRunsForTests);

        /* Without the keep: the same window with a keep of its own, and the window the read had before it ended at the newest row. */
        var without = new List<Fact>();
        await new DuckDbFactCollector(_duckDb) { CostKeep = new PlanRegressionCostKeep() }.CollectPlanRegressionFactsAsync(At(Now.AddSeconds(11), clock: true), without);
        var exactWindow = new List<Fact>();
        await new DuckDbFactCollector(_duckDb) { CostKeep = new PlanRegressionCostKeep() }.CollectPlanRegressionFactsAsync(At(Now.AddSeconds(11), clock: false), exactWindow);
        /* The read answered from the keep carries the facts of the read without it, for its own end, and the facts of the window the read had before it ended at the newest row. */
        AssertSameFacts(Assert.Single(without), Assert.Single(second));
        AssertSameFacts(Assert.Single(exactWindow), Assert.Single(second));
        /* The first read ended a second earlier, and the age of the best plan is measured to the read's own end: that figure alone moves. */
        AssertSameFacts(Assert.Single(without), Assert.Single(first), except: "best_plan_age_days");
    }

    [Fact]
    public async Task ThePassAndAReadAfterIt_RunTheStatementOnce_AndAReadAfterANewCollectionRunsItAgain()
    {
        var time = new ManualTime();
        var keep = new PlanRegressionCostKeep(time);
        var pass = new DuckDbFactCollector(_duckDb) { CostKeep = keep };
        var read = new DuckDbFactCollector(_duckDb) { CostKeep = keep };

        await pass.CollectPlanRegressionFactsAsync(At(Now.AddSeconds(5), clock: true), new List<Fact>());
        time.Advance(TimeSpan.FromSeconds(30));
        var (_, afterPass) = await ReadClockCostAsync(read, At(Now.AddSeconds(35), clock: true));
        Assert.True(afterPass.FromKeep);
        Assert.Equal(1, keep.StatementRuns);

        /* A collection lands: its rows carry a collection_time after the newest one the keep was made for. */
        InsertRow(collected: Now.AddMinutes(-30), lastExec: Now.AddMinutes(-31));
        time.Advance(TimeSpan.FromSeconds(30));
        var (_, afterCollection) = await ReadClockCostAsync(read, At(Now.AddSeconds(65), clock: true));
        Assert.False(afterCollection.FromKeep);
        Assert.Equal(2, keep.StatementRuns);
        Assert.Equal(1, afterCollection.KeepHitsSincePreviousStatement);

        var (_, repeat) = await ReadClockCostAsync(pass, At(Now.AddSeconds(66), clock: true));
        Assert.True(repeat.FromKeep);
        Assert.Equal(2, keep.StatementRuns);
    }

    [Fact]
    public async Task AKeepOlderThanItsLifetime_RunsTheStatementAgain()
    {
        var time = new ManualTime();
        var keep = new PlanRegressionCostKeep(time);
        var collector = new DuckDbFactCollector(_duckDb) { CostKeep = keep };

        await ReadClockCostAsync(collector, At(Now.AddSeconds(5), clock: true));
        time.Advance(PlanRegressionCostKeep.KeepFor + TimeSpan.FromSeconds(1));
        var (_, expired) = await ReadClockCostAsync(collector, At(Now.AddSeconds(6), clock: true));

        Assert.False(expired.FromKeep);
        Assert.Equal(2, keep.StatementRuns);
    }

    [Fact]
    public async Task AnAnchoredRead_KeepsItsExactKey_AndAChangedAsOfRunsTheStatementAgain()
    {
        var keep = new PlanRegressionCostKeep(new ManualTime());
        var collector = new DuckDbFactCollector(_duckDb) { CostKeep = keep };

        await ReadCostAsync(collector, At(Now, clock: false));
        Assert.Equal(1, keep.StatementRuns);

        /* A new collection does not move an anchored window: the statement cannot see a row collected after the as_of. */
        InsertRow(collected: Now.AddMinutes(-30), lastExec: Now.AddMinutes(-31));
        var (_, repeat) = await ReadCostAsync(collector, At(Now, clock: false));
        Assert.True(repeat.FromKeep);
        Assert.Equal(1, keep.StatementRuns);

        var (_, moved) = await ReadCostAsync(collector, At(Now.AddSeconds(1), clock: false));
        Assert.False(moved.FromKeep);
        Assert.Equal(2, keep.StatementRuns);
    }

    [Fact]
    public async Task ARepeatedCompare_SharesTheScanOfBothWindows_WhileEachEndHasARowFromTheLastDay()
    {
        var time = new ManualTime();
        var keep = new PlanRegressionCostKeep(time);

        async Task<(bool Comparison, bool Baseline)> CompareAsync(DuckDbFactCollector collector, DateTime end)
        {
            var comparison = await ReadClockCostAsync(collector, At(end, clock: true));
            /* compare_analysis: the baseline window ends before the comparison window. The seeded store holds old rows
               about five days back, so this baseline ends two hours after them, as a baseline the newest rows of which are a day old or less. */
            var baseline = await ReadClockCostAsync(collector, At(end.AddDays(-5).AddHours(2), clock: true));
            return (comparison.Time.FromKeep, baseline.Time.FromKeep);
        }

        var first = await CompareAsync(new DuckDbFactCollector(_duckDb) { CostKeep = keep }, Now.AddSeconds(10));
        Assert.Equal((false, false), first);
        Assert.Equal(2, keep.StatementRuns);

        time.Advance(TimeSpan.FromSeconds(1));
        var repeat = await CompareAsync(new DuckDbFactCollector(_duckDb) { CostKeep = keep }, Now.AddSeconds(11));
        Assert.Equal((true, true), repeat);
        Assert.Equal(2, keep.StatementRuns);
        Assert.Equal(2, keep.Hits);
    }

    [Fact]
    public async Task ReadsThroughTwoAnalysisServices_ShareOneScan_AsThePassAndAnMcpCallDo()
    {
        var keep = PlanRegressionCostKeep.For(_duckDb);

        /* One AnalysisService per read, as the MCP host and the scheduled pass build them, each with its own clock a second apart. */
        var first = new AnalysisService(_duckDb) { UtcNowForTests = () => DateTime.SpecifyKind(Now.AddSeconds(10), DateTimeKind.Utc) };
        var second = new AnalysisService(_duckDb) { UtcNowForTests = () => DateTime.SpecifyKind(Now.AddSeconds(11), DateTimeKind.Utc) };
        await first.CollectAndScoreFactsAsync(ServerId, "TestSrv", hoursBack: 4, cancellationToken: TestContext.Current.CancellationToken);
        var runsAfterFirst = keep.StatementRuns;
        await second.CollectAndScoreFactsAsync(ServerId, "TestSrv", hoursBack: 4, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, runsAfterFirst);
        Assert.Equal(1, keep.StatementRuns);
        Assert.Equal(1, keep.Hits);
    }
}
