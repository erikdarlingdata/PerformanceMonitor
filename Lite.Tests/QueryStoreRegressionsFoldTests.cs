/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9362: the Query Store regressions statement now answers the two coverage facts of the empty answer itself, and takes each
/// query's newest kept row with one aggregate instead of a window. The old statement is kept verbatim in
/// <see cref="QueryStoreRegressionsPre9362Sql"/>. Three kinds of pin:
///
/// <para><b>Same rows.</b> Both statements run over one store whose hot table holds the last day and whose older days are Parquet
/// files, at the tool's own cap, at a small cap and uncapped. The rows must match as a multiset and in sequence.</para>
///
/// <para><b>Same coverage.</b> The two facts the read returns equal the separate probe's for every combination, with and without a
/// database filter.</para>
///
/// <para><b>One probe fewer.</b> The empty answers of get_query_store_regressions make no coverage probe with no database named,
/// and one (the server-wide question) with one named, whatever the answer says.</para>
/// </summary>
public sealed class QueryStoreRegressionsFoldTests : IDisposable
{
    private const string BigServer = "FoldBigSrv";
    private const string QuietServer = "FoldQuietSrv";
    private const int Queries = 120;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "LiteTests_9362_fold_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DateTime _now = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    private readonly ServerManager _serverManager;
    private readonly int _bigId;
    private readonly int _quietId;
    private DuckDbInitializer? _duckDb;

    public QueryStoreRegressionsFoldTests()
    {
        Directory.CreateDirectory(_dir);
        _serverManager = new ServerManager(_dir);
        var big = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = BigServer, IsEnabled = true };
        var quiet = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = QuietServer, IsEnabled = true };
        _serverManager.AddServer(big);
        _serverManager.AddServer(quiet);
        _bigId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(big));
        _quietId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(quiet));
    }

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private const string Columns =
        "collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, " +
        "first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count, avg_cpu_time_us, " +
        "avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, query_plan_hash, is_forced_plan, " +
        "force_failure_count, runtime_stats_interval_id, replica_role";

    private static string T(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /// <summary>
    /// One kind of snapshot rows for the big server. <paramref name="at"/> is the day (or hour) the rows are collected near,
    /// <paramref name="idBase"/> keeps collection ids apart. Half the queries (the even ones) cost more in the recent window than
    /// in the baseline, by a factor that varies with the id, so the page is longer than the tool's cap. Every seventh query ran
    /// aborted and every eleventh raised errors in the recent window, so the failure slots are in play. Each interval has two
    /// snapshots, the later one with the larger execution count, so the dedupe has work to do.
    /// </summary>
    private string BigSeed(DateTime at, long idBase, bool recent) => $@"
SELECT
    ({idBase} + qid * 100 + snap * 10 + kind)::BIGINT AS collection_id,
    TIMESTAMP '{T(at)}' + INTERVAL ((qid % 10)) HOUR + INTERVAL (snap * 20) MINUTE AS collection_time,
    {_bigId}::INTEGER AS server_id,
    '{BigServer}' AS server_name,
    CASE WHEN qid % 2 = 0 THEN 'db_a' ELSE 'db_b' END AS database_name,
    qid::BIGINT AS query_id,
    (qid % 3)::BIGINT AS plan_id,
    CASE kind WHEN 0 THEN 'Regular' WHEN 1 THEN 'Aborted' ELSE 'Exception' END AS execution_type_desc,
    TIMESTAMP '{T(at)}' AS first_execution_time,
    TIMESTAMP '{T(at)}' + INTERVAL ((qid % 10)) HOUR + INTERVAL (snap * 20) MINUTE AS last_execution_time,
    CAST(NULL AS VARCHAR) AS module_name,
    'SELECT ' || qid::VARCHAR || ' FROM dbo.t' AS query_text,
    md5((qid % 40)::VARCHAR) AS query_hash,
    (CASE kind WHEN 0 THEN 2000 + qid + snap * 50 WHEN 1 THEN 10 + snap ELSE 5 + snap END)::BIGINT AS execution_count,
    (CASE WHEN {(recent ? "TRUE" : "FALSE")} AND qid % 2 = 0 THEN (5000 + qid * 100) * (1 + ((qid % 9) + 1) * 0.4) ELSE 5000 + qid * 100 END / 2)::BIGINT AS avg_cpu_time_us,
    (CASE WHEN {(recent ? "TRUE" : "FALSE")} AND qid % 2 = 0 THEN (5000 + qid * 100) * (1 + ((qid % 9) + 1) * 0.4) ELSE 5000 + qid * 100 END)::BIGINT AS avg_duration_us,
    (qid * 3)::BIGINT AS avg_logical_io_reads,
    (qid % 4)::BIGINT AS avg_logical_io_writes,
    (qid % 3)::BIGINT AS avg_physical_io_reads,
    md5('p' || qid::VARCHAR) AS query_plan_hash,
    FALSE AS is_forced_plan,
    0::BIGINT AS force_failure_count,
    ({(recent ? 1 : 0) * 10_000 + (long)(_now - at).TotalDays * 100} + qid % 24)::BIGINT AS runtime_stats_interval_id,
    CAST(NULL AS VARCHAR) AS replica_role
FROM (SELECT range AS qid FROM range(1, {Queries + 1})) q
CROSS JOIN (SELECT range AS snap FROM range(0, 2)) s
CROSS JOIN (SELECT range AS kind FROM range(0, 3)) k
WHERE kind = 0 OR (kind = 1 AND {(recent ? "TRUE" : "FALSE")} AND qid % 7 = 0) OR (kind = 2 AND qid % 11 = 0)";

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object[] values)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
            command.Parameters.Add(new DuckDBParameter { Value = value });
        await command.ExecuteNonQueryAsync();
    }

    private async Task<LocalDataService> BuildStoreAsync()
    {
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();
        Directory.CreateDirectory(_duckDb.ArchivePath);

        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();

            /* The recent window (24 hours back) and its newest day of baseline sit in the hot table. */
            await ExecAsync(connection, $"INSERT INTO query_store_stats ({Columns}) {BigSeed(_now.AddHours(-20), 9_000_000, recent: true)}");

            /* Baseline days 3, 4 and 5 back are Parquet files, named as the archive writer names them. */
            for (var d = 3; d <= 5; d++)
            {
                var day = _now.AddDays(-d);
                var path = Path.Combine(_duckDb.ArchivePath, $"{day:yyyyMMdd}_2300_query_store_stats.parquet").Replace('\\', '/');
                await ExecAsync(connection, $"COPY ({BigSeed(day.Date, d * 1_000_000L, recent: false)}) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048)");
            }

            /* The quiet server: three databases of different coverage, one row per interval, equal costs. */
            await InsertQuietAsync(connection, 1, "db_a", _now.AddDays(-10), 1);
            await InsertQuietAsync(connection, 2, "db_a", _now.AddDays(-3), 2);
            await InsertQuietAsync(connection, 3, "db_a", _now.AddMinutes(-30), 3);
            await InsertQuietAsync(connection, 4, "db_b", _now.AddDays(-3), 4);
            await InsertQuietAsync(connection, 5, "db_r", _now.AddMinutes(-30), 5);
        }

        await _duckDb.CreateArchiveViewsAsync();
        return new LocalDataService(_duckDb);
    }

    private async Task InsertQuietAsync(DuckDBConnection connection, long id, string database, DateTime at, long interval) =>
        await ExecAsync(connection, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, execution_count,
     avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, runtime_stats_interval_id, query_text, last_execution_time, first_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, 9, 'Regular', 100, 1000, 1000, 100, $7, 'SELECT 1', $2, $2)",
            id, at, _quietId, QuietServer, database, id, interval);

    /* ---- the statement, old beside new ---- */

    private static async Task<List<object?[]>> RunRawAsync(DuckDbInitializer duckDb, string sql, params object[] values)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
            command.Parameters.Add(new DuckDBParameter { Value = value });
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    private static string Show(object?[] row, int columns) => string.Join("|", row.Take(columns).Select(v => v switch
    {
        null => "<null>",
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("o", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => v.ToString() ?? "",
    }));

    /// <summary>Both statements, with the reader's own parameters and divisors, for a window of <paramref name="hours"/> hours ending now.</summary>
    private async Task<(List<object?[]> Old, List<object?[]> New)> RunBothAsync(int hours, int cap, string? database)
    {
        var start = _now.AddHours(-hours);
        var baseline = start.AddDays(-LocalDataService.BaselineLookbackDays);
        var baseHours = (start - baseline).TotalHours.ToString("R", CultureInfo.InvariantCulture);
        var recentHours = (_now - start).TotalHours.ToString("R", CultureInfo.InvariantCulture);
        var dbClause = database is null ? "" : " AND database_name IN ($4)";
        var baselineIndex = database is null ? 4 : 5;
        var limit = "CAST($" + (baselineIndex + 1) + " AS INTEGER)";

        var values = new List<object> { _bigId, start, _now };
        if (database is not null)
            values.Add(database);
        values.Add(baseline);
        values.Add(cap);

        var oldSql = QueryStoreRegressionsPre9362Sql.Build(dbClause, baselineIndex, "", baseHours, recentHours, "FALSE", limit);
        var newSql = LocalDataService.RegressionsSql(dbClause, baselineIndex, "", baseHours, recentHours, "FALSE", limit);
        var oldRows = await RunRawAsync(_duckDb!, oldSql, values.ToArray());
        var newRows = (await RunRawAsync(_duckDb!, newSql, values.ToArray())).Where(r => r[39] is not null).ToList();
        return (oldRows, newRows);
    }

    private const int StatementColumns = 39;

    /// <summary>
    /// The tool asks for its limit plus one (default 30, so 31), a small cap shows the cut, and an uncapped read shows every row.
    /// A filtered run covers the database clause the coverage fold shares. Every row of the old statement is in the new one, and
    /// the sequence is the same.
    /// </summary>
    [Theory]
    [InlineData(24, 5, null)]
    [InlineData(24, 31, null)]
    [InlineData(24, 10_000, null)]
    [InlineData(24, 31, "db_a")]
    [InlineData(24, 10_000, "db_b")]
    public async Task TheStatement_ReturnsTheRowsOfTheOneItReplaced_InTheSameOrder(int hours, int cap, string? database)
    {
        await BuildStoreAsync();
        var (oldRows, newRows) = await RunBothAsync(hours, cap, database);

        Assert.True(oldRows.Count >= Math.Min(cap, 5), "the oracle must return rows or the comparison proves nothing");
        if (cap == 31)
        {
            Assert.Equal(31, oldRows.Count);
            Assert.True(Convert.ToInt64(oldRows[0][38]) > 31, "the cap must cut the page, or the cap case proves nothing");
        }

        Assert.Equal(oldRows.Count, newRows.Count);
        var oldShown = oldRows.Select(r => Show(r, StatementColumns)).ToList();
        var newShown = newRows.Select(r => Show(r, StatementColumns)).ToList();
        Assert.Equal(oldShown, newShown);
        Assert.Equal(oldShown.OrderBy(x => x, StringComparer.Ordinal), newShown.OrderBy(x => x, StringComparer.Ordinal));
        Assert.Equal(Enumerable.Range(1, newRows.Count).Select(i => (long)i), newRows.Select(r => Convert.ToInt64(r[39])));
    }

    /// <summary>The page kinds are all present, or the equality above would hide a failure-slot or a cost-order difference.</summary>
    [Fact]
    public async Task TheSeededPage_HoldsCostRowsAndBothFailureKinds()
    {
        await BuildStoreAsync();
        var (oldRows, _) = await RunBothAsync(24, 10_000, null);
        Assert.Contains(oldRows, r => r[17] is true);
        Assert.Contains(oldRows, r => r[18] is true && r[17] is false);
        Assert.Contains(oldRows, r => r[19] is true && r[17] is false);
    }

    /// <summary>
    /// Two rows of one query tie on collection_time, and one collection_id is the larger: the newest kept row is the larger id,
    /// as the window it replaced took. The text the read returns is the text of that row.
    /// </summary>
    [Fact]
    public async Task TheNewestKeptRow_OnATieOfCollectionTime_IsTheLargerCollectionId()
    {
        var service = await BuildStoreAsync();
        var at = _now.AddHours(-2);
        using (var readLock = _duckDb!.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            foreach (var (id, text) in new[] { (9_999_001L, "the smaller id"), (9_999_003L, "the larger id"), (9_999_002L, "the middle id") })
            {
                /* Distinct plan ids so the three rows are three intervals and none is deduped away. */
                await ExecAsync(connection, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, execution_count,
     avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, runtime_stats_interval_id, query_text, last_execution_time, first_execution_time)
VALUES ($1, $2, $3, $4, 'db_a', 4, $5, 'Regular', 2000, 90000, 90000, 100, 777, $6, $2, $2)",
                    id, at, _bigId, BigServer, id, text);
            }
        }

        var (oldRows, newRows) = await RunBothAsync(24, 10_000, null);
        var oldRow = oldRows.Single(r => Convert.ToInt64(r[1]) == 4 && (string)r[0]! == "db_a");
        var newRow = newRows.Single(r => Convert.ToInt64(r[1]) == 4 && (string)r[0]! == "db_a");
        Assert.Equal(9_999_003L, Convert.ToInt64(oldRow[14]));
        Assert.Equal(9_999_003L, Convert.ToInt64(newRow[14]));
        Assert.Equal(oldRow[16], newRow[16]);

        var rows = await service.GetQueryStoreRegressionsAsync(_bigId, 24, 10_000, asOfUtc: _now);
        Assert.Equal("the larger id", rows.Single(r => r.QueryId == 4 && r.DatabaseName == "db_a").QueryTextSample);
    }

    /* ---- the coverage facts ---- */

    /// <summary>The combinations a window can have: both sides, a recent side only, a baseline side only, and neither.</summary>
    [Theory]
    [InlineData(24, 0, null)]
    [InlineData(24 * 6, 0, null)]
    [InlineData(1, 0, null)]
    [InlineData(24, 40, null)]
    [InlineData(24, 0, "db_a")]
    [InlineData(24 * 6, 0, "db_b")]
    [InlineData(1, 0, "db_a")]
    [InlineData(24, 40, "db_a")]
    [InlineData(24, 0, "no_such_db")]
    public async Task TheCoverageTheReadReturns_IsTheSeparateProbesAnswer(int hours, int asOfDaysBack, string? database)
    {
        var service = await BuildStoreAsync();
        var asOf = _now.AddDays(-asOfDaysBack);
        IReadOnlyList<string>? databases = database is null ? null : new[] { database };

        var read = await service.GetQueryStoreRegressionsWithCoverageAsync(_bigId, hours, 31, databases, asOfUtc: asOf);
        var probe = await service.GetQueryStoreRegressionCoverageAsync(_bigId, hours, asOf, databases);

        Assert.Equal(probe, (read.HasBaseline, read.HasRecent));
    }

    [Fact]
    public async Task TheCoverageCombinations_AreAllReached()
    {
        var service = await BuildStoreAsync();
        var seen = new HashSet<(bool, bool)>();
        foreach (var (hours, daysBack) in new[] { (24, 0), (24 * 6, 0), (1, 0), (24, 40) })
        {
            var read = await service.GetQueryStoreRegressionsWithCoverageAsync(_bigId, hours, 31, asOfUtc: _now.AddDays(-daysBack));
            seen.Add((read.HasBaseline, read.HasRecent));
        }

        Assert.Equal(4, seen.Count);
    }

    /* ---- the tool's coverage calls ---- */

    private async Task<(string Status, string Message, int Probes)> AskAsync(LocalDataService service, int hours, string? database, string? asOf)
    {
        var before = service.QueryStoreRegressionCoverageProbes;
        var json = await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, QuietServer, hours, database, as_of: asOf);
        var root = JsonDocument.Parse(json).RootElement;
        return (root.GetProperty("status").GetString()!, root.GetProperty("message").GetString()!, service.QueryStoreRegressionCoverageProbes - before);
    }

    private string AsOf(int daysBack) => _now.AddDays(-daysBack).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    [Fact]
    public async Task NoDatabaseNamed_EveryEmptyAnswerMakesNoCoverageProbe()
    {
        var service = await BuildStoreAsync();

        var clear = await AskAsync(service, 24, null, null);
        Assert.Equal("empty", clear.Status);
        Assert.Contains("all-clear", clear.Message, StringComparison.Ordinal);

        var noBaseline = await AskAsync(service, 48, null, AsOf(8));
        Assert.Equal("unavailable", noBaseline.Status);
        Assert.Contains("no baseline", noBaseline.Message, StringComparison.Ordinal);

        var noRecent = await AskAsync(service, 24, null, AsOf(1));
        Assert.Equal("empty", noRecent.Status);
        Assert.Contains("Widen hours_back", noRecent.Message, StringComparison.Ordinal);

        Assert.Equal(0, clear.Probes);
        Assert.Equal(0, noBaseline.Probes);
        Assert.Equal(0, noRecent.Probes);
    }

    [Fact]
    public async Task ADatabaseNamed_EveryEmptyAnswerMakesOneCoverageProbe_AndAnswersForThatDatabase()
    {
        var service = await BuildStoreAsync();

        var clear = await AskAsync(service, 24, "db_a", null);
        Assert.Equal("empty", clear.Status);
        Assert.Contains("all-clear", clear.Message, StringComparison.Ordinal);

        var onlyBaseline = await AskAsync(service, 24, "db_b", null);
        Assert.Equal("empty", onlyBaseline.Status);
        Assert.Contains("nothing of it collected IN the last", onlyBaseline.Message, StringComparison.Ordinal);

        var onlyRecent = await AskAsync(service, 24, "db_r", null);
        Assert.Equal("unavailable", onlyRecent.Status);
        Assert.Contains("no Query Store capture of database_name 'db_r'", onlyRecent.Message, StringComparison.Ordinal);

        var neither = await AskAsync(service, 24, "no_such_db", null);
        Assert.Equal("empty", neither.Status);
        Assert.Contains("matched database_name 'no_such_db'", neither.Message, StringComparison.Ordinal);

        Assert.Equal(1, clear.Probes);
        Assert.Equal(1, onlyBaseline.Probes);
        Assert.Equal(1, onlyRecent.Probes);
        Assert.Equal(1, neither.Probes);
    }
}
