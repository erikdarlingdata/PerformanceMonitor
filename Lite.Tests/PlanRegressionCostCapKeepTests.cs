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
        await new DuckDbFactCollector(_duckDb).CollectPlanRegressionFactsAsync(Context(), factsFresh);

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
}
