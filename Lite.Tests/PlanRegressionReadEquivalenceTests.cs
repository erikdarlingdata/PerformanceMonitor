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
/// #9195: the plan regression cost read carries the statement text for the candidate queries only, not on every row of the 14-day
/// window. The result must stay what it was. Each test runs the statement as it stood before (<see cref="ReferenceSql"/>, kept here
/// as the oracle) and the shipped one over the same store and compares every candidate, text included.
/// The store holds hot rows (the text behind a digest, as the collector writes it now) and Parquet rows (inline text, as an archive
/// from before that holds it), ties on collection_time, NULL execution counts and row counts, two replica roles, a forced plan,
/// two plan_ids sharing one plan hash, rows collected after the window end, and text that only a row the read must ignore carries.
/// </summary>
public sealed class PlanRegressionReadEquivalenceTests : IDisposable
{
    private const int ServerId = 9195;
    private static readonly DateTime Now = Whole(DateTime.UtcNow);
    private static readonly DateTime Old = Now.AddDays(-5);
    private static readonly DateTime Recent = Now.AddHours(-1);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly List<string> _statements = new();
    private long _nextId = 1;

    public PlanRegressionReadEquivalenceTests()
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

    private static DateTime Whole(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private static string Lit(string? s) => s is null ? "NULL" : "'" + s.Replace("'", "''") + "'";

    private static string Num(object? v) => v is null ? "NULL" : Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)!;

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

    /// <summary>One Query Store snapshot row. The text is stored inline, or behind a digest in the text table when
    /// <paramref name="digest"/> is set (what the collector writes now).</summary>
    private string Row(long query, long plan, string? hash, long interval, DateTime collected, DateTime lastExec, long? execs, double cpu,
        double? rows = 10, string? role = null, bool forced = false, long failures = 0, string? text = null, bool digest = false,
        string execType = "Regular", string database = "Db1")
    {
        var textColumn = digest ? "NULL" : Lit(text);
        var digestColumn = digest && text is not null ? $"unhex(sha256({Lit(text)}))" : "NULL";
        return $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_text_digest, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id, interval_start_time_utc, replica_role, avg_rowcount)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', {Lit(database)}, {query}, {plan}, {Lit(execType)}, {Ts(lastExec.AddHours(-1))}, {Ts(lastExec)},
     {textColumn}, {digestColumn}, {Lit("0xQ" + query)}, {Num(execs)}, {Num(cpu)}, {Num(cpu + 20000)}, {Lit(hash)}, {(forced ? "true" : "false")}, {failures},
     {interval}, {Ts(lastExec.AddHours(-1))}, {Lit(role)}, {Num(rows)})";
    }

    private static string DimRow(string text) =>
        $"INSERT INTO query_text_dim (digest, query_text) SELECT unhex(sha256({Lit(text)})), {Lit(text)} WHERE NOT EXISTS (SELECT 1 FROM query_text_dim WHERE digest = unhex(sha256({Lit(text)})))";

    /// <summary>A query's cheap plan in the archive (two intervals, two snapshots each) and its costly newest plan in the hot store.</summary>
    private void Query(long q, string? coldText, string? hotText, double costlyCpu, string? role = null, string database = "Db1",
        bool hotDigest = true, bool forced = false, long failures = 0, DateTime? latest = null, long? planBase = null)
    {
        var newest = latest ?? Recent;
        var cheap = (planBase ?? q * 10) + 1;
        var costly = (planBase ?? q * 10) + 2;
        for (var interval = 0; interval < 2; interval++)
        {
            var coldLast = Old.AddHours(interval);
            _statements.Add(Row(q, cheap, "0xCHEAP" + cheap, 1 + interval, coldLast.AddMinutes(1), coldLast, 30, 20_000, 10, role, text: coldText, database: database));
            _statements.Add(Row(q, cheap, "0xCHEAP" + cheap, 1 + interval, coldLast.AddMinutes(2), coldLast, 60, 20_000, 10, role, text: coldText, database: database));
            var hotLast = newest.AddMinutes(interval * 10);
            _statements.Add(Row(q, costly, "0xCOSTLY" + costly, 11 + interval, newest.AddMinutes(interval * 10 + 1), hotLast, 30, costlyCpu, 10, role, forced, failures, hotText, hotDigest, database: database));
            _statements.Add(Row(q, costly, "0xCOSTLY" + costly, 11 + interval, newest.AddMinutes(interval * 10 + 2), hotLast, 60, costlyCpu, 10, role, forced, failures, hotText, hotDigest, database: database));
        }
    }

    private async Task SeedAsync()
    {
        /* 1: ties on collection_time. Two snapshots of one interval share a stamp: the higher count wins, and on equal counts the higher id. */
        Query(1, null, "SELECT 1 FROM dbo.tie1", 200_000);
        _statements.Add(Row(1, 12, "0xCOSTLY12", 11, Recent.AddMinutes(2), Recent, 60, 450_000, text: "SELECT 1 FROM dbo.tie1", digest: true));
        _statements.Add(Row(1, 12, "0xCOSTLY12", 11, Recent.AddMinutes(2), Recent, 60, 300_000, text: "SELECT 1 FROM dbo.tie1", digest: true));
        /* 2: NULL counts and NULL row counts. A NULL count sorts below every number in a tie, and a NULL row count blanks the plan's rows per call. */
        Query(2, "SELECT 2 FROM dbo.nullcount", "SELECT 2 FROM dbo.nullcount", 210_000, hotDigest: false);
        _statements.Add(Row(2, 22, "0xCOSTLY22", 11, Recent.AddMinutes(2), Recent, null, 900_000, rows: null));
        _statements.Add(Row(2, 22, "0xCOSTLY22", 12, Recent.AddMinutes(12), Recent.AddMinutes(10), 40, 210_000, rows: null));
        /* 3: two replica roles. The primary regressed, the secondary did not. */
        Query(3, "SELECT 3 FROM dbo.replica", "SELECT 3 FROM dbo.replica", 220_000, role: "PRIMARY");
        for (var interval = 0; interval < 2; interval++)
        {
            _statements.Add(Row(3, 31, "0xCHEAP31", 1 + interval, Old.AddMinutes(5), Old, 60, 20_000, role: "SECONDARY", text: "SELECT 3 FROM dbo.replica"));
            _statements.Add(Row(3, 32, "0xCOSTLY32", 11 + interval, Recent.AddMinutes(5), Recent, 60, 20_000, role: "SECONDARY", text: "SELECT 3 FROM dbo.replica", digest: true));
        }
        /* 4: a forced plan with failures. */
        Query(4, "SELECT 4 FROM dbo.forced", "SELECT 4 FROM dbo.forced", 230_000, forced: true, failures: 3);
        /* 5: two plan_ids that share one plan hash. */
        Query(5, null, "SELECT 5 FROM dbo.samehash", 240_000);
        for (var interval = 0; interval < 2; interval++)
            _statements.Add(Row(5, 53, "0xCOSTLY52", 11 + interval, Recent.AddMinutes(interval * 10 + 3), Recent.AddMinutes(interval * 10), 60, 240_000, text: "SELECT 5 FROM dbo.samehash", digest: true));
        /* 6: rows the read must ignore carry text of their own: one collected after the window end, one Aborted, one last run before the
           14-day window. The candidate carries no text, because none of those rows counts. */
        Query(6, null, null, 250_000, hotDigest: false);
        _statements.Add(Row(6, 62, "0xCOSTLY62", 11, Now.AddMinutes(5), Recent, 60, 250_000, text: "SELECT 'after the window end'"));
        _statements.Add(Row(6, 62, "0xCOSTLY62", 11, Recent, Recent, 60, 250_000, text: "SELECT 'aborted'", execType: "Aborted"));
        _statements.Add(Row(6, 61, "0xCHEAP61", 1, Now.AddDays(-30), Now.AddDays(-30), 60, 20_000, text: "SELECT 'too old'"));
        /* 7: SQL Server housekeeping text is left out. */
        Query(7, "EXEC sp_flush_commit_table_on_demand 1000", "EXEC sp_flush_commit_table_on_demand 1000", 260_000);
        /* 8: the newest plan sits in the archive too, so its text comes from Parquet alone, and the same query_id exists in a second
           database whose text differs. */
        Query(8, "SELECT 8 FROM dbo.archiveonly", "SELECT 8 FROM dbo.archiveonly", 270_000, hotDigest: false, latest: Now.AddDays(-2));
        Query(8, "SELECT 'other database' FROM dbo.archiveonly", "SELECT 'other database' FROM dbo.archiveonly", 130_000, database: "Db2", hotDigest: false, latest: Now.AddDays(-2));
        /* 10: the newest plan's rows carry no text while the cheap plan's rows do: the candidate has none, as before. */
        Query(10, "SELECT 10 FROM dbo.coldonly", null, 280_000, hotDigest: false);
        /* 9: no regression: the newest plan costs what the cheap one does. */
        Query(9, "SELECT 9", "SELECT 9", 20_000);
        /* 11: an interval whose newest snapshot holds a LOWER count than an older one (a counter that restarted): the newest row wins,
           not the biggest count. Collection time ranks before the count. */
        Query(11, "SELECT 11 FROM dbo.newest", "SELECT 11 FROM dbo.newest", 290_000, hotDigest: false);
        _statements.Add(Row(11, 112, "0xCOSTLY112", 11, Recent.AddMinutes(3), Recent, 25, 120_000, text: "SELECT 11 FROM dbo.newest"));

        var dims = new[] { "SELECT 1 FROM dbo.tie1", "SELECT 3 FROM dbo.replica", "SELECT 4 FROM dbo.forced", "SELECT 5 FROM dbo.samehash", "EXEC sp_flush_commit_table_on_demand 1000", "SELECT 9" };
        Execute(dims.Select(DimRow).ToArray());
        await FlushAsync();
    }

    private async Task FlushAsync()
    {
        Execute(_statements.ToArray());
        /* Everything older than a day moves to a Parquet file, which keeps the inline text. The hot store keeps the digests. */
        var path = Path.Combine(_tempDir, "archive", "20260101_000000_query_store_stats.parquet").Replace("\\", "/");
        var before = Ts(Now.AddDays(-1));
        Execute(
            $"COPY (SELECT * FROM query_store_stats WHERE collection_time < {before}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM query_store_stats WHERE collection_time < {before}");
        await _duckDb.CreateArchiveViewsAsync();
    }

    private async Task<List<DuckDbFactCollector.PlanRegressionCandidate>> ReadAsync(string sql)
    {
        var context = Context();
        var token = TestContext.Current.CancellationToken;
        var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14), token);
        using var readLock = _duckDb.AcquireReadLock(token);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(token);
        return await DuckDbFactCollector.ReadPlanRegressionCandidatesAsync(connection, context, plan, sql);
    }

    [Fact]
    public async Task TheShippedRead_ReturnsTheCandidatesTheReferenceReadReturns_TextIncluded()
    {
        await SeedAsync();

        var reference = await ReadAsync(ReferenceSql);
        var shipped = await ReadAsync(DuckDbFactCollector.PlanRegressionCostSql);

        /* The comparison is not vacuous: every regressed query is there, with the text its rows hold, and nothing else. */
        var byKey = reference.ToDictionary(c => (c.DatabaseName, c.QueryId));
        Assert.Equal(
            new[] { ("Db1", 1L), ("Db1", 2L), ("Db1", 3L), ("Db1", 4L), ("Db1", 5L), ("Db1", 6L), ("Db1", 8L), ("Db1", 10L), ("Db1", 11L), ("Db2", 8L) }.OrderBy(k => k),
            byKey.Keys.OrderBy(k => k));
        Assert.Equal("SELECT 1 FROM dbo.tie1", byKey[("Db1", 1)].QueryText);
        Assert.Equal("SELECT 2 FROM dbo.nullcount", byKey[("Db1", 2)].QueryText);
        Assert.Equal("SELECT 8 FROM dbo.archiveonly", byKey[("Db1", 8)].QueryText);
        Assert.Equal("SELECT 'other database' FROM dbo.archiveonly", byKey[("Db2", 8)].QueryText);
        Assert.Null(byKey[("Db1", 10)].QueryText);
        Assert.Null(byKey[("Db1", 6)].QueryText);
        Assert.Equal(1, byKey[("Db1", 4)].LatestIsForced);

        /* Ordered by key: two queries whose lost CPU and factor tie are in either order. */
        Assert.Equal(reference.OrderBy(c => c.DatabaseName).ThenBy(c => c.QueryId), shipped.OrderBy(c => c.DatabaseName).ThenBy(c => c.QueryId));
    }

    /// <summary>One plan of a query for the pre-filter tests: two intervals, two snapshots each, all of the given plan hash (NULL for none).</summary>
    private void PlanGroup(long q, long plan, string? hash, double cpu, DateTime lastExec, long firstInterval, string database = "Db1", long? execs = 60)
    {
        for (var interval = 0; interval < 2; interval++)
        {
            var last = lastExec.AddMinutes(interval * 10);
            _statements.Add(Row(q, plan, hash, firstInterval + interval, last.AddMinutes(1), last, 30, cpu, database: database));
            _statements.Add(Row(q, plan, hash, firstInterval + interval, last.AddMinutes(2), last, execs, cpu, database: database));
        }
    }

    private async Task<List<DuckDbFactCollector.RankedPlanRegression>> ReadRankedAsync(string sql, IReadOnlyList<string>? databases, IReadOnlySet<string>? secondaries)
    {
        var context = Context();
        context.SecondaryReplicaDatabases = secondaries;
        var token = TestContext.Current.CancellationToken;
        var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14), token);
        using var readLock = _duckDb.AcquireReadLock(token);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(token);
        return await DuckDbFactCollector.ReadRankedPlanRegressionCandidatesAsync(connection, context, plan, sql, databases);
    }

    /// <summary>
    /// #9681: the statement reads the dedup of a query's rows only when the query has two distinct non-NULL plan hashes. A query whose
    /// newest plan group has a NULL hash, beside two non-NULL groups, keeps all its rows (the NULL ones too), so the newest group is still
    /// the NULL one and `compared` finds no candidate, as before. A read that dropped the NULL-hash rows would see the costly group as the
    /// newest and report the query.
    /// </summary>
    [Fact]
    public async Task AQueryWhoseNewestPlanGroupHasNoHash_StaysNoCandidate_BesideARegressedOne()
    {
        PlanGroup(20, 201, "0xCHEAP201", 20_000, Old, 1);
        PlanGroup(20, 202, "0xCOSTLY202", 300_000, Recent.AddMinutes(-60), 11);
        PlanGroup(20, 203, null, 300_000, Recent.AddMinutes(-30), 21);
        Query(21, null, "SELECT 21", 300_000);
        await SeedAsync();

        var reference = await ReadAsync(ReferenceSql);
        var shipped = await ReadAsync(DuckDbFactCollector.PlanRegressionCostSql);

        Assert.DoesNotContain(reference, c => c.QueryId == 20);
        Assert.Contains(shipped, c => c.QueryId == 21);
        Assert.DoesNotContain(shipped, c => c.QueryId == 20);
        Assert.Equal(reference.OrderBy(c => c.DatabaseName).ThenBy(c => c.QueryId), shipped.OrderBy(c => c.DatabaseName).ThenBy(c => c.QueryId));
    }

    /// <summary>#9681: a heavy query with one plan is left out of the dedup and gives nothing, and the regressed query beside it is found with the values it had.</summary>
    [Fact]
    public async Task AHeavySinglePlanQuery_GivesNothing_AndTheRegressedOneBesideItKeepsItsValues()
    {
        PlanGroup(30, 301, "0xONLY301", 900_000, Old, 1);
        PlanGroup(30, 301, "0xONLY301", 950_000, Recent.AddMinutes(-30), 11);
        Query(31, null, "SELECT 31", 400_000);
        await SeedAsync();

        var reference = await ReadAsync(ReferenceSql);
        var shipped = await ReadAsync(DuckDbFactCollector.PlanRegressionCostSql);

        Assert.DoesNotContain(shipped, c => c.QueryId == 30);
        var regressed = Assert.Single(shipped, c => c.QueryId == 31);
        Assert.Equal(reference.Single(c => c.QueryId == 31), regressed);
        Assert.Equal(reference.OrderBy(c => c.DatabaseName).ThenBy(c => c.QueryId), shipped.OrderBy(c => c.DatabaseName).ThenBy(c => c.QueryId));
    }

    /// <summary>#9681: the statement holds the database and secondary-database markers twice (once in the pre-filter, once in the dedup). Both replacements must run, so a database list and a secondary database give the rows the one-database reads expect.</summary>
    [Fact]
    public async Task TheDatabaseListAndTheSecondaryDatabase_ReachBothPartsOfTheStatement()
    {
        await SeedAsync();
        var all = (await ReadRankedAsync(DuckDbFactCollector.PlanRegressionCostSql, null, null)).Select(r => r.Candidate).ToList();
        Assert.Contains(all, c => c.DatabaseName == "Db1");
        Assert.Contains(all, c => c.DatabaseName == "Db2");

        var db2Only = (await ReadRankedAsync(DuckDbFactCollector.PlanRegressionCostSql, ["Db2"], null)).Select(r => r.Candidate).ToList();
        Assert.Equal(all.Where(c => c.DatabaseName == "Db2").ToList(), db2Only);
        Assert.NotEmpty(db2Only);

        var withoutSecondary = (await ReadRankedAsync(DuckDbFactCollector.PlanRegressionCostSql, null, new HashSet<string> { "Db2" })).Select(r => r.Candidate).ToList();
        Assert.Equal(all.Where(c => c.DatabaseName != "Db2").ToList(), withoutSecondary);

        var both = await ReadRankedAsync(DuckDbFactCollector.PlanRegressionCostSql, ["Db1", "Db2"], new HashSet<string> { "Db2" });
        Assert.Equal(withoutSecondary, both.Select(r => r.Candidate).ToList());
    }

    /// <summary>
    /// #9353: the pass reads its databases one at a time, each under its own hold of the read lock, and puts their rows in the order the
    /// one statement gives them. Same store, both ways: the offenders the pass reports are the one statement's rows in the one statement's order.
    /// </summary>
    [Fact]
    public async Task ReadingTheDatabasesInTurn_RanksTheOffendersAsTheOneStatementDoes()
    {
        await SeedAsync();
        var context = Context();

        await new DuckDbFactCollector(_duckDb).CollectPlanRegressionFactsAsync(context, new List<Fact>());

        var oneStatement = (await ReadAsync(DuckDbFactCollector.PlanRegressionCostSql)).Select(c => (c.DatabaseName, c.QueryId)).ToList();
        var offenders = context.PlanRegressionOffenders!.Select(o => (o.DatabaseName, o.QueryId)).ToList();
        Assert.Contains(offenders, o => o.DatabaseName == "Db1");
        Assert.Contains(offenders, o => o.DatabaseName == "Db2");
        Assert.Equal(oneStatement.Where(offenders.Contains).ToList(), offenders);
    }

    [Fact]
    public async Task AWriterThatAsksBetweenTwoDatabases_TakesTheLock()
    {
        await SeedAsync();
        var collector = new DuckDbFactCollector(_duckDb);
        var writes = 0;
        collector.AfterPlanRegressionDatabaseReadForTests = () =>
        {
            /* A long wait, as ArchiveSpanReadTests uses (#9564, #9810): the write lock is one lock for the whole test process, so another
               class's InitializeAsync on a busy runner can hold it for more than 5 s. A read lock kept across batches still fails
               this test: the same thread throws LockRecursionException, and another thread waits out the whole timeout.
               The store holds two databases with regressions, so there is one gap. */
            using var write = _duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
            writes++;
        };

        await collector.CollectPlanRegressionFactsAsync(Context(), new List<Fact>());

        Assert.Equal(1, writes);
    }

    /// <summary>
    /// #9353: three databases whose queries all lose the same CPU at the same factor, so every row ties on both of the statement's ranking
    /// keys. Three queries lose more (two of them tie at the top, in different databases), 108 tie below them, and the statement keeps 100.
    /// The cut falls inside the tied rows, so which rows survive depends on the order the ties come out in.
    /// </summary>
    private async Task SeedTiesAsync()
    {
        foreach (var database in new[] { "DbC", "DbA", "DbB" })
        {
            for (var q = 1; q <= 36; q++)
                Query(q, null, "SELECT " + q + " FROM dbo.tied", 300_000, database: database, hotDigest: false);
        }

        Query(101, null, "SELECT 101 FROM dbo.top", 500_000, database: "DbB", hotDigest: false);
        Query(102, null, "SELECT 102 FROM dbo.top", 500_000, database: "DbC", hotDigest: false);
        Query(103, null, "SELECT 103 FROM dbo.second", 400_000, database: "DbA", hotDigest: false);
        await FlushAsync();
    }

    [Fact]
    public async Task ReadingTheDatabasesInTurn_GivesTheOneStatementsRowsInItsOrder_WhenRowsTieOnCpuLostAndOnTheCut()
    {
        await SeedTiesAsync();
        var context = Context();
        var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14), TestContext.Current.CancellationToken);

        var oneStatement = await ReadAsync(DuckDbFactCollector.PlanRegressionCostSql);

        var expected = new[] { ("DbB", 101L), ("DbC", 102L), ("DbA", 103L) }
            .Concat(new[] { "DbA", "DbB", "DbC" }.SelectMany(d => Enumerable.Range(1, 36).Select(q => (d, (long)q))))
            .Take(DuckDbFactCollector.PlanRegressionCandidateLimit)
            .ToList();
        Assert.Equal(expected, oneStatement.Select(c => (c.DatabaseName, c.QueryId)).ToList());
        /* The statement's cut is inside the tie: DbC keeps its first 25 queries and drops the other 11. */
        Assert.Contains(("DbC", 25L), expected);
        Assert.DoesNotContain(("DbC", 26L), expected);

        /* The in-turn read prices the first PlanRegressionCostCap rows of the statement's order (#9888), so it is compared with the statement
           under the same cap. With the cap raised to the statement's own limit it gives every row the statement does, and the cut is the
           statement's, inside the tie in DbC. Under the production cap it gives the statement's first rows, and the cut falls inside the tie
           in DbB: its first 11 queries stay and the rest of its rows are left out. */
        foreach (var cap in new int?[] { DuckDbFactCollector.PlanRegressionCandidateLimit, null })
        {
            var effectiveCap = cap ?? DuckDbFactCollector.PlanRegressionCostCap;
            var (inTurn, _) = await new DuckDbFactCollector(_duckDb) { CostKeep = new PlanRegressionCostKeep(), PlanRegressionCostCapForTests = cap }.ReadPlanRegressionCostAsync(context, plan);
            var keptAtCap = expected.Take(effectiveCap).ToList();
            Assert.Equal(keptAtCap, inTurn.Select(c => (c.DatabaseName, c.QueryId)).ToList());
        }

        var keptAtProductionCap = expected.Take(DuckDbFactCollector.PlanRegressionCostCap).ToList();
        Assert.Contains(("DbB", 11L), keptAtProductionCap);
        Assert.DoesNotContain(("DbB", 12L), keptAtProductionCap);
    }

    /// <summary>
    /// #9353: two databases, each with two queries that run on three replicas (a NULL role, PRIMARY and SECONDARY). Every row ties on the
    /// CPU lost, the factor, the database and the query, so only the replica role orders them. A role's plans carry plan_ids of its own
    /// (the latest plan is query * 100 + 12 for PRIMARY, + 22 for SECONDARY, + 32 for no role), which is how a candidate shows its role.
    /// The rows are written SECONDARY first, then no role, then PRIMARY, and the databases in reverse, so no order is the insert order.
    /// </summary>
    private async Task SeedReplicaTiesAsync()
    {
        foreach (var database in new[] { "DbB", "DbA" })
        {
            for (var q = 1; q <= 2; q++)
            {
                Query(q, null, "SELECT " + q + " FROM dbo.roles", 300_000, role: "SECONDARY", database: database, hotDigest: false, planBase: q * 100 + 20);
                Query(q, null, "SELECT " + q + " FROM dbo.roles", 300_000, role: null, database: database, hotDigest: false, planBase: q * 100 + 30);
                Query(q, null, "SELECT " + q + " FROM dbo.roles", 300_000, role: "PRIMARY", database: database, hotDigest: false, planBase: q * 100 + 10);
            }
        }

        await FlushAsync();
    }

    /* Per database, per query: PRIMARY, SECONDARY, then the NULL role last (the statement's ORDER BY on replica_role, NULL last). */
    private static readonly (string Database, long QueryId, string Role, long LatestPlanId)[] ReplicaTieOrder =
        new[] { "DbA", "DbB" }.SelectMany(d => new long[] { 1, 2 }.SelectMany(q => new[]
        {
            (d, q, "PRIMARY", q * 100 + 12), (d, q, "SECONDARY", q * 100 + 22), (d, q, "", q * 100 + 32),
        })).ToArray();

    [Fact]
    public async Task ReadingTheDatabasesInTurn_GivesTheOneStatementsRowsInItsOrder_WhenRowsTieOnEverythingButTheReplicaRole()
    {
        await SeedReplicaTiesAsync();
        var context = Context();
        var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14), TestContext.Current.CancellationToken);

        var (inTurn, _) = await new DuckDbFactCollector(_duckDb) { CostKeep = new PlanRegressionCostKeep() }.ReadPlanRegressionCostAsync(context, plan);
        var oneStatement = await ReadAsync(DuckDbFactCollector.PlanRegressionCostSql);

        var expected = ReplicaTieOrder.Select(r => (r.Database, r.QueryId, r.LatestPlanId)).ToList();
        Assert.Equal(expected, oneStatement.Select(c => (c.DatabaseName, c.QueryId, c.LatestPlanId!.Value)).ToList());
        Assert.Equal(oneStatement, inTurn);
    }

    [Fact]
    public async Task TheRegressedQueriesDrillDown_ReadingTheDatabasesInTurn_GivesTheOneStatementsRowsInItsOrder_WhenRowsTieOnEverythingButTheReplicaRole()
    {
        await SeedReplicaTiesAsync();
        var context = Context();
        var plan = await _duckDb.PlanArchiveReadAsync(
            System.Threading.CancellationToken.None,
            new ArchiveWindow("query_stats", "collection_time", context.TimeRangeStart),
            new ArchiveWindow("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14)));

        List<string> oneStatement;
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            oneStatement = (await DrillDownCollector.ReadRegressedQueriesAsync(connection, context, plan, databases: null))
                .Select(r => System.Text.Json.JsonSerializer.Serialize(r.Item)).ToList();
        }

        var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());
        var inTurn = ((IEnumerable<object>)finding.DrillDown!["regressed_queries"]).Select(i => System.Text.Json.JsonSerializer.Serialize(i)).ToList();

        /* The drill-down keeps five of the twelve, and the cut falls inside the roles of DbA's second query (PRIMARY and SECONDARY stay). */
        static List<(string, long, string)> Keys(List<string> rows) => rows
            .Select(r => System.Text.Json.JsonDocument.Parse(r).RootElement)
            .Select(e => (e.GetProperty("database").GetString()!, e.GetProperty("query_id").GetInt64(), e.GetProperty("replica_role").GetString()!)).ToList();
        Assert.Equal(ReplicaTieOrder.Take(5).Select(r => (r.Database, r.QueryId, r.Role)).ToList(), Keys(oneStatement));
        Assert.Equal(oneStatement, inTurn);
    }

    [Fact]
    public async Task TheRegressedQueriesDrillDown_ReadingTheDatabasesInTurn_GivesTheOneStatementsFive()
    {
        await SeedTiesAsync();
        var context = Context();
        var plan = await _duckDb.PlanArchiveReadAsync(
            System.Threading.CancellationToken.None,
            new ArchiveWindow("query_stats", "collection_time", context.TimeRangeStart),
            new ArchiveWindow("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14)));

        async Task<List<string>> OneStatementAsync(AnalysisContext c)
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return (await DrillDownCollector.ReadRegressedQueriesAsync(connection, c, plan, databases: null))
                .Select(r => System.Text.Json.JsonSerializer.Serialize(r.Item)).ToList();
        }

        async Task<List<string>> InTurnAsync(AnalysisContext c)
        {
            var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };
            await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], c);
            return ((IEnumerable<object>)finding.DrillDown!["regressed_queries"]).Select(i => System.Text.Json.JsonSerializer.Serialize(i)).ToList();
        }

        static List<(string, long)> Keys(List<string> rows) => rows
            .Select(r => System.Text.Json.JsonDocument.Parse(r).RootElement)
            .Select(e => (e.GetProperty("database").GetString()!, e.GetProperty("query_id").GetInt64())).ToList();

        /* Every query: the two that tie at the top (two databases), the one below, then the first two of the tied rows. */
        var oneStatement = await OneStatementAsync(context);
        Assert.Equal(
            new[] { ("DbB", 101L), ("DbC", 102L), ("DbA", 103L), ("DbA", 1L), ("DbA", 2L) },
            Keys(oneStatement));
        Assert.Equal(oneStatement, await InTurnAsync(Context()));

        /* The fact named some of its offenders: only those rows, and the databases they are not in are left unread. */
        var named = Context();
        named.PlanRegressionOffenders = [new("DbC", 5), new("DbA", 7), new("DbC", 102)];
        var namedOne = await OneStatementAsync(named);
        /* The lists match independently, so the pairs are every database listed with every query id listed: five rows exist in DbC and DbA. */
        Assert.Equal(new[] { ("DbC", 102L), ("DbA", 5L), ("DbA", 7L), ("DbC", 5L), ("DbC", 7L) }, Keys(namedOne));
        Assert.Equal(namedOne, await InTurnAsync(named));
    }

    [Fact]
    public async Task AWriterThatAsksBetweenTwoDatabasesOfTheDrillDown_TakesTheLock()
    {
        await SeedTiesAsync();
        var writes = 0;
        var collector = new DrillDownCollector(_duckDb)
        {
            /* A target of nothing makes every database a batch of its own (#9353), so the three of them give two gaps. */
            RegressedQueriesHoldTargetForTests = TimeSpan.Zero,
            AfterRegressedQueriesDatabaseReadForTests = () =>
            {
                /* A long wait, as ArchiveSpanReadTests uses (#9564, #9810): the write lock is one lock for the whole test process, so another
                   class's InitializeAsync on a busy runner can hold it for more than 5 s. A read lock kept across batches still fails
                   this test: the same thread throws LockRecursionException, and another thread waits out the whole timeout. */
                using var write = _duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
                writes++;
            },
        };
        var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };

        await collector.EnrichFindingsAsync([finding], Context());

        /* Three databases, so two gaps. */
        Assert.Equal(2, writes);
    }

    [Fact]
    public async Task TheFact_NamesTheOffendersTheReferenceReadFinds()
    {
        await SeedAsync();
        var context = Context();

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);

        var reference = await ReadAsync(ReferenceSql);
        Assert.Contains(facts, f => f.Key == "PLAN_REGRESSION");
        Assert.Equal(
            reference.Select(c => (c.DatabaseName, c.QueryId)).OrderBy(k => k),
            context.PlanRegressionOffenders!.Select(o => (o.DatabaseName, o.QueryId)).OrderBy(k => k));
    }

    /// <summary>The statement as it stood before #9195: the statement text rides through every step and is read from the deduplicated rows.</summary>
    private static readonly string ReferenceSql = @"
WITH deduped AS
(
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        query_plan_hash,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        avg_rowcount,
        last_execution_time,
        is_forced_plan,
        force_failure_count,
        query_text,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
            ORDER BY collection_time DESC, execution_count DESC, collection_id DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc = 'Regular'
    AND   last_execution_time >= $2
    AND   collection_time <= $3
    /*SEC*/
),
plan_agg AS
(
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        MAX(last_execution_time) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count,
        any_value(query_text) AS query_text,
        CASE WHEN COUNT(avg_rowcount) < COUNT(*) THEN NULL
             ELSE SUM(avg_rowcount * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0)
        END AS rows_per_exec
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, replica_role
),
plan_dedup AS
(
    SELECT
        database_name,
        query_id,
        replica_role,
        query_plan_hash,
        SUM(execs) AS execs,
        SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0) AS dur_per_exec,
        MAX(last_exec) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count,
        arg_max(plan_id, (last_exec, plan_id)) AS newest_plan_id,
        arg_max(plan_id, (execs, plan_id)) AS busiest_plan_id,
        any_value(query_text) AS query_text,
        CASE WHEN COUNT(rows_per_exec) < COUNT(*) THEN NULL
             ELSE SUM(rows_per_exec * execs) / NULLIF(SUM(execs), 0)
        END AS rows_per_exec
    FROM plan_agg
    GROUP BY database_name, query_id, replica_role, query_plan_hash
    HAVING SUM(execs) >= 25
),
ranked AS
(
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY last_exec DESC) AS recency,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY cpu_per_exec ASC) AS cheapness
    FROM plan_dedup
),
compared AS
(
    SELECT
        l.query_id,
        l.cpu_per_exec AS latest_cpu,
        l.dur_per_exec AS latest_dur,
        l.is_forced_plan AS latest_is_forced,
        l.force_failure_count AS force_failure_count,
        b.cpu_per_exec AS best_cpu,
        b.dur_per_exec AS best_dur,
        CASE
            WHEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 2
                THEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0)
            WHEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) >= 4
             AND l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 1.25
                THEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) / 2
        END AS regression_factor,
        l.execs * l.cpu_per_exec AS latest_total_cpu_us,
        " + PerformanceMonitor.Analysis.PlanRegressionRanking.LostCpuExpressionSql + @" AS lost_cpu_us,
        l.database_name,
        b.last_exec AS best_last_exec,
        l.query_text AS query_text,
        l.newest_plan_id AS latest_plan_id,
        b.busiest_plan_id AS best_plan_id,
        l.rows_per_exec AS latest_rows,
        b.rows_per_exec AS best_rows
    FROM ranked AS l
    JOIN ranked AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
      AND b.replica_role IS NOT DISTINCT FROM l.replica_role
      AND b.cheapness = 1
    WHERE l.recency = 1
    AND   l.query_plan_hash <> b.query_plan_hash
)
SELECT
    query_id,
    latest_cpu,
    latest_dur,
    latest_is_forced,
    force_failure_count,
    best_cpu,
    best_dur,
    regression_factor,
    database_name,
    best_last_exec,
    query_text,
    latest_plan_id,
    best_plan_id,
    latest_rows,
    best_rows
FROM compared
WHERE regression_factor >= 2
AND   latest_total_cpu_us >= 10000000
AND   " + PerformanceMonitor.Analysis.PlanRegressionRanking.LostCpuFloorSql + @"
AND   NOT regexp_matches(lower(COALESCE(query_text, '')), '" + PerformanceMonitor.Common.InternalStatements.LowerTextPattern + @"')
ORDER BY " + PerformanceMonitor.Analysis.PlanRegressionRanking.OrderBySql + @"
LIMIT 100";
}
