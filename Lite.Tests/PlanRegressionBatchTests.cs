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
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9353: the plan regression reads take their databases in batches sized by how long the last batch held the store's read lock. Whatever
/// the batches, the rows and their order are the one statement's, and a writer gets the lock between them. Six databases of unequal
/// size, with query ids repeated across them so rows tie on the CPU lost and the factor.
/// </summary>
public sealed class PlanRegressionBatchTests : IDisposable
{
    private const int ServerId = 9353;
    private static readonly DateTime Now = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
    private const int Hours = 96;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;

    public PlanRegressionBatchTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = "TestSrv",
        TimeRangeStart = Now.AddHours(-4),
        TimeRangeEnd = Now,
    };

    /* Synchronous on purpose: the database lock is thread-affine (#2463), so no await may sit between taking it and releasing it. */
    private void Execute(params string[] statements)
    {
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

    /// <summary>
    /// The databases and how many queries each holds: 12, 9, 6, 3, 3 and 1. Each query ran its cheap plan until two days before the end of
    /// the window and its costly plan after, hourly, so most of them regress by a factor the query number sets. Query numbers repeat across
    /// the databases, so a row in one database ties with one in another on everything but the database and the order is the key's.
    /// </summary>
    private static readonly (string Name, int Queries)[] Databases = [("Db0", 12), ("Db1", 9), ("Db2", 6), ("Db3", 3), ("Db4", 3), ("Db5", 1)];

    private static string SeedSql(string databaseColumn)
    {
        var start = Now.AddHours(-Hours);
        var values = string.Join(", ", Databases.Select((d, i) => $"('{d.Name}', {i}, {d.Queries})"));
        return $@"
SELECT h::BIGINT * 100000 + d.i * 1000 + r.q AS collection_id, {Ts(start)} + to_hours(h) + to_minutes(5) AS collection_time, {ServerId} AS server_id,
     'TestSrv' AS server_name, {databaseColumn} AS database_name, r.q::BIGINT AS query_id,
     CASE WHEN h < {Hours - 48} THEN r.q::BIGINT * 10 + 1 ELSE r.q::BIGINT * 10 + 2 END AS plan_id,
     'Regular' AS execution_type_desc, {Ts(start)} + to_hours(h) - to_hours(1) AS first_execution_time, {Ts(start)} + to_hours(h) + to_minutes(2) AS last_execution_time,
     'SELECT ' || r.q || ' FROM dbo.t' AS query_text, NULL::BLOB AS query_text_digest, '0xQ' || r.q AS query_hash, 60::BIGINT AS execution_count,
     CASE WHEN h < {Hours - 48} THEN 20000.0 ELSE 20000.0 * (1 + (r.q % 7) * 1.5) END AS avg_cpu_time_us, 40000.0 AS avg_duration_us,
     CASE WHEN h < {Hours - 48} THEN '0xCHEAP' || r.q ELSE '0xCOSTLY' || r.q END AS query_plan_hash, false AS is_forced_plan, 0::BIGINT AS force_failure_count,
     h::BIGINT + 1 AS runtime_stats_interval_id, {Ts(start)} + to_hours(h) AS interval_start_time_utc, NULL::VARCHAR AS replica_role, 10.0 AS avg_rowcount
FROM range(0, {Hours}) AS t(h), (VALUES {values}) AS d(name, i, n), range(1, 13) AS r(q)
WHERE r.q <= d.n";
    }

    private const string Columns = @"collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_text_digest, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id, interval_start_time_utc, replica_role, avg_rowcount";

    private void Seed() => Execute($"INSERT INTO query_store_stats ({Columns}) {SeedSql("d.name")}");

    private Task<ArchiveReadPlan> PlanAsync(AnalysisContext context) =>
        _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14), TestContext.Current.CancellationToken);

    private async Task<List<(string, long)>> OneStatementAsync()
    {
        var context = Context();
        var plan = await PlanAsync(context);
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return (await DuckDbFactCollector.ReadPlanRegressionCandidatesAsync(connection, context, plan, DuckDbFactCollector.PlanRegressionCostSql))
            .Select(c => (c.DatabaseName, c.QueryId)).ToList();
    }

    /* Zero: no batch is ever allowed a second database. An hour: the batch after the first takes every database that is left. */
    public static TheoryData<int, int> Targets => new() { { 0, 6 }, { 3600, 2 } };

    [Theory]
    [MemberData(nameof(Targets))]
    public async Task TheCostRead_InBatches_GivesTheOneStatementsRowsInItsOrder(int targetSeconds, int expectedReads)
    {
        Seed();
        var oneStatement = await OneStatementAsync();
        Assert.Equal(Databases.Length, oneStatement.Select(r => r.Item1).Distinct().Count());
        var reads = 1;
        var collector = new DuckDbFactCollector(_duckDb)
        {
            CostKeep = new PlanRegressionCostKeep(),
            PlanRegressionHoldTargetForTests = TimeSpan.FromSeconds(targetSeconds),
            AfterPlanRegressionDatabaseReadForTests = () => reads++,
        };
        var context = Context();

        var (inBatches, _) = await collector.ReadPlanRegressionCostAsync(context, await PlanAsync(context));

        Assert.Equal(oneStatement, inBatches.Select(c => (c.DatabaseName, c.QueryId)).ToList());
        Assert.Equal(expectedReads, reads);
    }

    [Theory]
    [MemberData(nameof(Targets))]
    public async Task TheDrillDown_InBatches_GivesTheOneStatementsFive(int targetSeconds, int expectedReads)
    {
        Seed();
        var context = Context();
        var plan = await _duckDb.PlanArchiveReadAsync(
            System.Threading.CancellationToken.None,
            new ArchiveWindow("query_stats", "collection_time", context.TimeRangeStart),
            new ArchiveWindow("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14)));
        List<string> oneStatement;
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            oneStatement = (await DrillDownCollector.ReadRegressedQueriesAsync(connection, context, plan, databases: null))
                .Select(r => JsonSerializer.Serialize(r.Item)).ToList();
        }
        Assert.Equal(5, oneStatement.Count);
        var reads = 1;
        var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };

        await new DrillDownCollector(_duckDb)
        {
            RegressedQueriesHoldTargetForTests = TimeSpan.FromSeconds(targetSeconds),
            AfterRegressedQueriesDatabaseReadForTests = () => reads++,
        }.EnrichFindingsAsync([finding], context);

        Assert.Equal(oneStatement, ((IEnumerable<object>)finding.DrillDown!["regressed_queries"]).Select(i => JsonSerializer.Serialize(i)).ToList());
        Assert.Equal(expectedReads, reads);
    }

    [Fact]
    public async Task AWriterThatAsksBetweenTwoBatches_TakesTheLock()
    {
        Seed();
        var writes = 0;
        var collector = new DuckDbFactCollector(_duckDb) { CostKeep = new PlanRegressionCostKeep(), PlanRegressionHoldTargetForTests = TimeSpan.Zero };
        collector.AfterPlanRegressionDatabaseReadForTests = () =>
        {
            /* A long wait, as ArchiveSpanReadTests uses (#9564): the write lock is one lock for the whole test process, so another
               class's InitializeAsync on a busy runner can hold it for more than 5 s. A read lock kept across batches still fails
               this test: the same thread throws LockRecursionException, and another thread waits out the whole timeout. */
            using var write = _duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
            writes++;
        };
        var context = Context();

        await collector.ReadPlanRegressionCostAsync(context, await PlanAsync(context));

        Assert.Equal(Databases.Length - 1, writes);
    }

    [Fact]
    public void TheNextBatch_ReadsAsManyRowsAsTheTargetAllowsAtTheRateTheLastOneRead()
    {
        var target = TimeSpan.FromSeconds(1);
        Assert.Equal(500, DuckDbFactCollector.NextBatchRows(1000, TimeSpan.FromSeconds(2), target));
        Assert.Equal(4000, DuckDbFactCollector.NextBatchRows(1000, TimeSpan.FromMilliseconds(250), target));
        Assert.Equal(0, DuckDbFactCollector.NextBatchRows(1000, TimeSpan.FromSeconds(2), TimeSpan.Zero));
        Assert.Equal(long.MaxValue, DuckDbFactCollector.NextBatchRows(1000, TimeSpan.Zero, target));
        Assert.Equal(long.MaxValue, DuckDbFactCollector.NextBatchRows(long.MaxValue / 2, TimeSpan.FromMilliseconds(1), target));
    }

}
