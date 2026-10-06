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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5381: the three Query Store reads (top queries, comparison, regressions) rank and aggregate on narrow columns and
/// read <c>query_text</c> afterwards, by key, for the final rows only. Two kinds of pin:
///
/// <para><b>Parity.</b> The pre-change SQL is kept verbatim in <see cref="QueryStoreOldReadSql"/> as an oracle and run
/// beside the shipped read over a store whose newest day sits in the hot table and whose older days are archived
/// parquet files. Every column of every row must match. The store carries the shapes the text lookup could get wrong:
/// a text only the older rows have (the latest row's is NULL), a query with no text at all, one query_hash shared by
/// several query_ids with different texts, a WAITFOR text that ranks first, a query seen only in the baseline and one
/// only in the recent window.</para>
///
/// <para><b>Memory.</b> A synthetic archive with wide texts, one row group per day like the archive writer's, is read
/// under a low <c>memory_limit</c>. The old SQL runs out of memory there (asserted, so the limit cannot drift loose
/// without failing), and the shipped read does not.</para>
/// </summary>
[Collection(QueryStoreMemoryPinCollection.Name)]
public sealed class QueryStoreTextAfterRankingTests : IDisposable
{
    private const int ServerId = 5381;
    private const int Queries = 60;
    private const int SnapshotsPerDay = 3;
    private const int ArchivedDays = 4;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "LiteTests_5381_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DateTime _anchor = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddHours(12), DateTimeKind.Unspecified);
    private DuckDbInitializer? _duckDb;

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private async Task<DuckDbInitializer> BuildStoreAsync(int textRepeat, int queries = Queries, bool hotToday = true)
    {
        Directory.CreateDirectory(_dir);
        /* Assigned straight to the field, which Dispose() releases: DuckDbInitializerDisposalGuardTests (#5208) accepts a field only when it is written this way. */
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        var duckDb = _duckDb;
        await duckDb.InitializeAsync();
        Directory.CreateDirectory(duckDb.ArchivePath);

        using (var readLock = duckDb.AcquireReadLock())
        using (var connection = duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            /* Day 0 (today) is the HOT table; days 1..4 are one parquet file each, named the way the archive writer
               names them. The same query_ids recur every day, as Query Store ids do. */
            if (hotToday)
            {
                await ExecAsync(connection, "INSERT INTO query_store_stats (" + Columns + ") " + SeedSelect(0, textRepeat, queries));
            }

            for (var day = hotToday ? 1 : 0; day <= ArchivedDays; day++)
            {
                var date = _anchor.Date.AddDays(-day);
                var path = Path.Combine(duckDb.ArchivePath, $"{date:yyyyMMdd}_2300_query_store_stats.parquet").Replace('\\', '/');
                await ExecAsync(connection, $"COPY ({SeedSelect(day, textRepeat, queries)}) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)");
            }
        }

        await duckDb.CreateArchiveViewsAsync();
        return duckDb;
    }

    private const string Columns =
        "collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, " +
        "first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count, avg_cpu_time_us, " +
        "avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, query_plan_hash, is_forced_plan, " +
        "force_failure_count, runtime_stats_interval_id, replica_role";

    /// <summary>One day of the seed. Text is constant per query_id, as it is in Query Store.</summary>
    private string SeedSelect(int day, int textRepeat, int queries)
    {
        var dayStart = _anchor.Date.AddDays(-day).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var idBase = (long)(ArchivedDays - day + 1) * 1_000_000L;
        /* Recent-only queries (queries+1..queries+3) exist on day 0 only; baseline-only ones (the last five) never on day 0. */
        var presence = day == 0
            ? $"(qid <= {queries - 5} OR qid > {queries})"
            : $"qid <= {queries}";
        /* qid 7: its newest snapshot (hot, last snap) has NULL text. qid 13: never any text. qid 11: a WAITFOR text, ranked first. */
        var text = $@"CASE
            WHEN qid = 13 THEN NULL
            WHEN qid = 7 AND {day} = 0 AND snap = {SnapshotsPerDay - 1} THEN NULL
            WHEN qid = 11 THEN 'WAITFOR DELAY ''00:00:01''; ' || repeat(md5(qid::VARCHAR), {textRepeat})
            ELSE 'SELECT ' || qid::VARCHAR || ' FROM dbo.t ' || repeat(md5(qid::VARCHAR), {textRepeat}) END";
        /* qids 7, 11 and 13 are expensive so they sit in the top page; every fifth query regressed on day 0. */
        var duration = $@"CASE
            WHEN qid = 11 THEN 90000 WHEN qid = 13 THEN 80000 WHEN qid = 7 THEN 70000
            WHEN {day} = 0 AND qid % 5 = 0 THEN 4000 + qid * 37
            ELSE 1000 + qid * 10 END";
        return $@"
SELECT
    ({idBase} + qid * 10 + snap)::BIGINT AS collection_id,
    TIMESTAMP '{dayStart}' + INTERVAL ((qid % 10) + 1) HOUR + INTERVAL (snap * 20) MINUTE AS collection_time,
    {ServerId}::INTEGER AS server_id,
    'srv' AS server_name,
    CASE WHEN qid % 2 = 0 THEN 'db_even' ELSE 'db_odd' END AS database_name,
    qid::BIGINT AS query_id,
    qid::BIGINT AS plan_id,
    'Regular' AS execution_type_desc,
    TIMESTAMP '{dayStart}' AS first_execution_time,
    TIMESTAMP '{dayStart}' + INTERVAL ((qid % 10) + 1) HOUR + INTERVAL (snap * 20) MINUTE AS last_execution_time,
    CAST(NULL AS VARCHAR) AS module_name,
    {text} AS query_text,
    CASE WHEN qid > {queries - 5} THEN md5('u' || qid::VARCHAR) ELSE md5((qid % 40)::VARCHAR) END AS query_hash,
    (100 + snap * 50 + qid % 7)::BIGINT AS execution_count,
    (({duration}) / 2)::BIGINT AS avg_cpu_time_us,
    ({duration})::BIGINT AS avg_duration_us,
    (qid * 3)::BIGINT AS avg_logical_io_reads,
    (qid % 4)::BIGINT AS avg_logical_io_writes,
    (qid % 3)::BIGINT AS avg_physical_io_reads,
    md5('p' || qid::VARCHAR) AS query_plan_hash,
    FALSE AS is_forced_plan,
    0::BIGINT AS force_failure_count,
    ({day} * 100 + qid % 24)::BIGINT AS runtime_stats_interval_id,
    CAST(NULL AS VARCHAR) AS replica_role
FROM (SELECT range AS qid FROM range(1, {queries + 4})) q
CROSS JOIN (SELECT range AS snap FROM range(0, {SnapshotsPerDay})) s
WHERE {presence}";
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<List<Dictionary<string, object?>>> OracleAsync(string sql, params object[] parameters)
    {
        using var readLock = _duckDb!.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(new DuckDBParameter { Value = parameter });
        }

        var rows = new List<Dictionary<string, object?>>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object?>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            rows.Add(row);
        }

        return rows;
    }

    private static string Snake(string pascal) => Regex.Replace(pascal, "(?<=[a-z0-9])(?=[A-Z])", "_").ToLowerInvariant();

    /// <summary>One value in a form both sides can be compared in: NULL and the model's 0 / "" default are the same.</summary>
    private static string Normalize(object? value) => value switch
    {
        null => "",
        string s => s,
        DateTime d => d.Ticks.ToString(CultureInfo.InvariantCulture),
        bool b => b ? "1" : "",
        System.Numerics.BigInteger big => big.IsZero ? "" : big.ToString(CultureInfo.InvariantCulture),
        IConvertible c => Math.Abs(c.ToDouble(CultureInfo.InvariantCulture)) < double.Epsilon ? "" : c.ToDouble(CultureInfo.InvariantCulture).ToString("R", CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    private static void AssertSameRows<T>(List<Dictionary<string, object?>> oracle, IReadOnlyList<T> actual, Dictionary<string, string>? columnToProperty = null, params string[] ignoredColumns)
    {
        Assert.Equal(oracle.Count, actual.Count);
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);
        var compared = 0;
        for (var i = 0; i < oracle.Count; i++)
        {
            foreach (var (column, expected) in oracle[i])
            {
                if (ignoredColumns.Contains(column))
                {
                    continue;
                }

                var propertyName = columnToProperty is not null && columnToProperty.TryGetValue(column, out var mapped) ? mapped : column.Replace("_", "");
                Assert.True(properties.TryGetValue(propertyName, out var property), $"no property for column {column}");
                var want = Normalize(expected);
                var got = Normalize(property!.GetValue(actual[i]));
                /* DOUBLE sums and averages can differ in the last bit between two plans that add in a different order. */
                if (want != got && double.TryParse(want, NumberStyles.Float, CultureInfo.InvariantCulture, out var w)
                    && double.TryParse(got, NumberStyles.Float, CultureInfo.InvariantCulture, out var g))
                {
                    Assert.True(Math.Abs(w - g) <= 1e-9 * Math.Max(Math.Abs(w), Math.Abs(g)), $"{column}: {want} vs {got}");
                }
                else
                {
                    Assert.True(want == got, $"row {i} {column}: {(want.Length > 60 ? want[..60] : want)} vs {(got.Length > 60 ? got[..60] : got)}");
                }

                compared++;
            }
        }

        Assert.True(compared > 0);
    }

    private static bool IsOutOfMemory(Exception e) => e.ToString().Contains("Out of Memory", StringComparison.OrdinalIgnoreCase);

    // ---- parity ----

    [Fact]
    public async Task TopQueries_ReturnTheOldReadsRows_ColumnForColumn()
    {
        var duckDb = await BuildStoreAsync(textRepeat: 3);
        const int top = 12;

        /* hoursBack 24 is day 0 (the hot table) only; 120 reaches every archived day as well. */
        foreach (var hoursBack in new[] { 24, 120 })
        {
            var oracle = await OracleAsync(
                QueryStoreOldReadSql.TopQueries.Replace("@@CANDIDATES@@", (top + 5).ToString(CultureInfo.InvariantCulture)),
                ServerId, _anchor.AddHours(-hoursBack), _anchor, top);
            var actual = await new LocalDataService(duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack, top, asOfUtc: _anchor);

            Assert.Equal(top, actual.Count);
            AssertSameRows(oracle, actual, ignoredColumns: ["page_ord", "candidate_count"]);
            /* The shapes the text lookup could get wrong, in the page: */
            Assert.DoesNotContain(actual, r => r.QueryId == 11);                              // WAITFOR, ranked first, filtered
            Assert.Equal("", Assert.Single(actual, r => r.QueryId == 13).QueryText);           // no text anywhere
            Assert.StartsWith("SELECT 7 FROM", Assert.Single(actual, r => r.QueryId == 7).QueryText, StringComparison.Ordinal); // newest row's text is NULL
        }
    }

    [Fact]
    public async Task Comparison_ReturnsTheOldReadsRows_ColumnForColumn()
    {
        var duckDb = await BuildStoreAsync(textRepeat: 3);
        var (cs, ce, bs, be) = (_anchor.AddHours(-24), _anchor, _anchor.AddHours(-48), _anchor.AddHours(-24));

        var oracle = await OracleAsync(QueryStoreOldReadSql.Comparison, ServerId, cs, ce, bs, be);
        var actual = await new LocalDataService(duckDb).GetQueryStoreComparisonAsync(ServerId, cs, ce, bs, be);

        Assert.True(oracle.Count > 20);
        Assert.Contains(actual, r => r.ExecutionCount == 0 && r.BaselineExecutionCount > 0);   // baseline only
        Assert.Contains(actual, r => r.ExecutionCount > 0 && r.BaselineExecutionCount == 0);   // recent only
        oracle = [.. oracle.OrderBy(r => (string?)r["database_name"], StringComparer.Ordinal).ThenBy(r => (string?)r["query_hash"], StringComparer.Ordinal)];
        var sorted = actual.OrderBy(r => r.DatabaseName, StringComparer.Ordinal).ThenBy(r => r.QueryHash, StringComparer.Ordinal).ToList();
        AssertSameRows(oracle, sorted, new Dictionary<string, string>
        {
            ["exec_count"] = "ExecutionCount",
            ["avg_cpu_ms"] = "AvgCpuMs",
            ["avg_reads"] = "AvgReads",
            ["baseline_exec_count"] = "BaselineExecutionCount",
            ["baseline_avg_duration_ms"] = "BaselineAvgDurationMs",
            ["baseline_avg_cpu_ms"] = "BaselineAvgCpuMs",
            ["baseline_avg_reads"] = "BaselineAvgReads",
        });
        /* Several query_ids share one query_hash here (60 ids over 40 hashes), with different texts: the MAX must agree. */
        Assert.Contains(oracle, r => ((string?)r["query_text"] ?? "").Length > 0);
    }

    /// <summary>
    /// The review of #5396 (F2): a query whose NEWEST kept interval has no text, with an older kept interval in the same
    /// period that has one, showed that older text under the old per-period MAX and showed '' under the winner-row read.
    /// The extra rows go into the hot table, so every shape sits inside the windows the seed's days already fill.
    /// </summary>
    [Fact]
    public async Task Comparison_ShowsAnOlderKeptRowsText_WhenTheNewestKeptRowHasNone()
    {
        var duckDb = await BuildStoreAsync(textRepeat: 3);
        using (var readLock = duckDb.AcquireReadLock())
        using (var connection = duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            /* 901: the newest interval is NULL, an older one has text. */
            await InsertExtraAsync(connection, 9011, 901, "h901", 9011, _anchor.AddHours(-30), "older text 901");
            await InsertExtraAsync(connection, 9012, 901, "h901", 9012, _anchor.AddHours(-2), null);
            /* 903 + 904 share a hash: 903's newest is NULL but its older text sorts HIGHER than 904's, so dropping it changes the MAX. */
            await InsertExtraAsync(connection, 9031, 903, "h903", 9031, _anchor.AddHours(-60), "zz 903");
            await InsertExtraAsync(connection, 9032, 903, "h903", 9032, _anchor.AddHours(-1), null);
            await InsertExtraAsync(connection, 9041, 904, "h903", 9041, _anchor.AddHours(-3), "aa 904");
            /* 905: the text is two UTC days back, behind two NULL rows, so the per-day walk has to go past a day. */
            await InsertExtraAsync(connection, 9051, 905, "h905", 9051, _anchor.AddHours(-50), "day-old 905");
            await InsertExtraAsync(connection, 9052, 905, "h905", 9052, _anchor.AddHours(-26), null);
            await InsertExtraAsync(connection, 9053, 905, "h905", 9053, _anchor.AddHours(-1), null);
            /* 906: baseline period only. */
            await InsertExtraAsync(connection, 9061, 906, "h906", 9061, _anchor.AddHours(-100), "baseline text 906");
            await InsertExtraAsync(connection, 9062, 906, "h906", 9062, _anchor.AddHours(-80), null);
            /* 907: ONE interval snapshotted twice; the older snapshot has text, the kept (newer) one does not. Old read: ''. */
            await InsertExtraAsync(connection, 9071, 907, "h907", 9070, _anchor.AddHours(-5), "snapshot text 907");
            await InsertExtraAsync(connection, 9072, 907, "h907", 9070, _anchor.AddHours(-4), null);
        }

        var (cs, ce, bs, be) = (_anchor.AddHours(-72), _anchor, _anchor.AddHours(-120), _anchor.AddHours(-72));
        var oracle = await OracleAsync(QueryStoreOldReadSql.Comparison, ServerId, cs, ce, bs, be);
        var actual = await new LocalDataService(duckDb).GetQueryStoreComparisonAsync(ServerId, cs, ce, bs, be);

        oracle = [.. oracle.OrderBy(r => (string?)r["database_name"], StringComparer.Ordinal).ThenBy(r => (string?)r["query_hash"], StringComparer.Ordinal)];
        var sorted = actual.OrderBy(r => r.DatabaseName, StringComparer.Ordinal).ThenBy(r => r.QueryHash, StringComparer.Ordinal).ToList();
        AssertSameRows(oracle, sorted, new Dictionary<string, string>
        {
            ["exec_count"] = "ExecutionCount",
            ["avg_cpu_ms"] = "AvgCpuMs",
            ["avg_reads"] = "AvgReads",
            ["baseline_exec_count"] = "BaselineExecutionCount",
            ["baseline_avg_duration_ms"] = "BaselineAvgDurationMs",
            ["baseline_avg_cpu_ms"] = "BaselineAvgCpuMs",
            ["baseline_avg_reads"] = "BaselineAvgReads",
        });
        Assert.Equal("older text 901", Assert.Single(actual, r => r.QueryHash == "h901").QueryText);
        Assert.Equal("zz 903", Assert.Single(actual, r => r.QueryHash == "h903").QueryText);
        Assert.Equal("day-old 905", Assert.Single(actual, r => r.QueryHash == "h905").QueryText);
        Assert.Equal("baseline text 906", Assert.Single(actual, r => r.QueryHash == "h906").QueryText);
        Assert.Equal("", Assert.Single(actual, r => r.QueryHash == "h907").QueryText);
    }

    /// <summary>
    /// The regressions fallback (a key whose latest kept row has no text) walks the window one UTC day at a time, newest
    /// first, and stops at the first day with a text, instead of one MAX over every file of the window. Both window ends
    /// are inclusive, including a row collected exactly at either.
    /// </summary>
    [Fact]
    public async Task WindowTextFallback_WalksTheWindowNewestUtcDayFirst()
    {
        var duckDb = await BuildStoreAsync(textRepeat: 3);
        using (var readLock = duckDb.AcquireReadLock())
        using (var connection = duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            /* 'zzz older' sorts above 'aaa newer', so a MAX over the whole window and a newest-day-first walk disagree. */
            await InsertExtraAsync(connection, 9101, 910, "h910", 9101, _anchor.AddDays(-3), "zzz older");
            await InsertExtraAsync(connection, 9102, 910, "h910", 9102, _anchor.AddDays(-1).AddHours(-2), "aaa newer");
            await InsertExtraAsync(connection, 9103, 910, "h910", 9103, _anchor.AddHours(-1), null);
            await InsertExtraAsync(connection, 9111, 911, "h911", 9111, _anchor.AddDays(-3), "edge start 911");
            await InsertExtraAsync(connection, 9121, 912, "h912", 9121, _anchor, "edge end 912");
        }

        using var read = await new LocalDataService(duckDb).OpenConnectionAsync();
        var from = _anchor.AddDays(-3);
        Assert.Equal("aaa newer", await LocalDataService.ReadQueryStoreWindowTextAsync(read, ServerId, "db_odd", 910, from, _anchor));
        Assert.Equal("edge start 911", await LocalDataService.ReadQueryStoreWindowTextAsync(read, ServerId, "db_odd", 911, from, _anchor));
        Assert.Equal("edge end 912", await LocalDataService.ReadQueryStoreWindowTextAsync(read, ServerId, "db_odd", 912, from, _anchor));
        Assert.Null(await LocalDataService.ReadQueryStoreWindowTextAsync(read, ServerId, "db_odd", 910, from.AddDays(2), _anchor.AddDays(-1)));
        Assert.Null(await LocalDataService.ReadQueryStoreWindowTextAsync(read, ServerId, "db_odd", 913, from, _anchor));
    }

    private async Task InsertExtraAsync(DuckDBConnection connection, long collectionId, long queryId, string hash, long interval, DateTime when, string? text)
    {
        var t = when.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var first = when.Date.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        await ExecAsync(connection, $@"
INSERT INTO query_store_stats ({Columns})
VALUES ({collectionId}, TIMESTAMP '{t}', {ServerId}, 'srv', 'db_odd', {queryId}, {queryId}, 'Regular',
        TIMESTAMP '{first}', TIMESTAMP '{t}', NULL, {(text is null ? "NULL" : "'" + text + "'")}, '{hash}', 10, 500, 1000, 5, 0, 0, 'p{queryId}',
        FALSE, 0, {interval}, NULL)");
    }

    [Fact]
    public async Task Regressions_ReturnTheOldReadsRows_ColumnForColumn()
    {
        var duckDb = await BuildStoreAsync(textRepeat: 3);

        var oracle = await OracleAsync(QueryStoreOldReadSql.Regressions, ServerId, _anchor.AddHours(-24), _anchor, _anchor.AddHours(-24).AddDays(-LocalDataService.BaselineLookbackDays), 50);
        var actual = await new LocalDataService(duckDb).GetQueryStoreRegressionsAsync(ServerId, 24, 50, asOfUtc: _anchor);

        Assert.True(oracle.Count >= 5);
        AssertSameRows(oracle, actual);
        Assert.Contains(actual, r => r.QueryTextSample.Length > 0);
    }

    [Fact]
    public void Utf8Order_AgreesWithDuckDbsVarcharOrder()
    {
        /* U+1F600 (a surrogate pair) sorts ABOVE U+FFFD in UTF-8 and below it in UTF-16. */
        Assert.True(LocalDataService.CompareUtf8Order("😀", "�") > 0);
        Assert.True(LocalDataService.CompareUtf8Order("a", "ab") < 0);
        Assert.True(LocalDataService.CompareUtf8Order("b", "ab") > 0);
        Assert.Equal(0, LocalDataService.CompareUtf8Order("same", "same"));
    }

    // ---- memory ----

    /// <summary>
    /// The archive under test: five days (4 parquet files, one row group each, like the archive writer's, plus day 0),
    /// 300 recurring queries x 3 snapshots, ~100 KB of text each (~86 MB of text per day).
    ///
    /// <para>Why each pin is built the way it is. A DuckDB read's peak memory depends on the thread count, on what else
    /// runs in the same instance, and on how many files the scan opens at once, so a pin that leaves those free passes
    /// or fails with the test runner's load (the first version of these pins did: the old statement ran out of memory
    /// when the test ran alone and fitted when other classes ran beside it). Here every pin builds its OWN database file
    /// (BuildStoreAsync: a fresh folder per test), sets <c>threads = 1</c> and a fixed <c>memory_limit</c>, and the class
    /// is in a collection that does not run in parallel with other collections. With one thread the old statement's peak
    /// is the same on every run: the margins below were measured by sweeping the limit (comparison: new fits from 150MB,
    /// old fails up to 500MB; regressions: new fits from 150MB, old fails up to 225MB), and each pin sits well inside
    /// both edges.</para>
    /// </summary>
    private const int WideTextRepeat = 3000;
    private const int WideQueries = 300;
    private const string TightMemoryLimit = "200MB";
    private const string TopQueriesMemoryLimit = "400MB";

    /// <summary>What <c>ArchiveService</c> does (lower the instance's memory_limit), plus one thread so the peak is repeatable.</summary>
    private async Task PinMemoryAsync(string limit)
    {
        using var readLock = _duckDb!.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        await ExecAsync(connection, "SET threads = 1");
        await ExecAsync(connection, $"SET memory_limit = '{limit}'");
    }

    [Fact]
    public async Task TopQueries_FitWhereTheOldReadFits_AndReturnTheWideText()
    {
        var duckDb = await BuildStoreAsync(WideTextRepeat, WideQueries, hotToday: false);
        await PinMemoryAsync(TopQueriesMemoryLimit);

        /* No RED half for this read, on purpose: at one thread the old statement's peak is its text lateral reading one
           row group at a time, the same row group the new read's by-key lookup needs, so the two need the same ~300MB
           over these five days (both fail at 250MB, both pass at 300MB). The old read only runs out of memory on a window
           of many days AND many threads (the repro: 14 and 30 day windows of a 30 file archive at 1GB, default threads),
           which this fixture cannot build repeatably. So the pin is the one that matters for a rewrite: wherever the old
           statement fits, the new read fits and returns the same rows (the parity test above holds the rows). */
        var old = await Record.ExceptionAsync(() => OracleAsync(
            QueryStoreOldReadSql.TopQueries.Replace("@@CANDIDATES@@", "10"), ServerId, _anchor.AddHours(-120), _anchor, 5));
        Assert.True(old is null, "the old SQL is expected to fit at this limit, so the new read is held to it: " + old);

        var rows = await new LocalDataService(duckDb).GetQueryStoreTopQueriesAsync(ServerId, 120, 5, asOfUtc: _anchor);
        Assert.Equal(5, rows.Count);
        Assert.All(rows, r => Assert.True(r.QueryId == 13 || r.QueryText.Length > 90_000));
    }

    [Fact]
    public async Task Comparison_FitsALowMemoryLimit_WhereTheOldReadDoesNot()
    {
        var duckDb = await BuildStoreAsync(WideTextRepeat, WideQueries, hotToday: false);
        await PinMemoryAsync(TightMemoryLimit);
        var (cs, ce, bs, be) = (_anchor.AddHours(-24), _anchor, _anchor.AddHours(-96), _anchor.AddHours(-24));

        var old = await Record.ExceptionAsync(() => OracleAsync(QueryStoreOldReadSql.Comparison, ServerId, cs, ce, bs, be));
        Assert.True(old is not null && IsOutOfMemory(old), "the old SQL must run out of memory at this limit: " + old);

        var rows = await new LocalDataService(duckDb).GetQueryStoreComparisonAsync(ServerId, cs, ce, bs, be);
        Assert.True(rows.Count > 20);
        Assert.Contains(rows, r => r.QueryText.Length > 90_000);
    }

    [Fact]
    public async Task Regressions_FitALowMemoryLimit_WhereTheOldReadDoesNot()
    {
        var duckDb = await BuildStoreAsync(WideTextRepeat, WideQueries, hotToday: false);
        await PinMemoryAsync(TightMemoryLimit);

        var old = await Record.ExceptionAsync(() => OracleAsync(
            QueryStoreOldReadSql.Regressions, ServerId, _anchor.AddHours(-24), _anchor, _anchor.AddHours(-24).AddDays(-LocalDataService.BaselineLookbackDays), 50));
        Assert.True(old is not null && IsOutOfMemory(old), "the old SQL must run out of memory at this limit: " + old);

        var rows = await new LocalDataService(duckDb).GetQueryStoreRegressionsAsync(ServerId, 24, 50, asOfUtc: _anchor);
        Assert.True(rows.Count >= 5);
        Assert.Contains(rows, r => r.QueryTextSample.Length > 90_000);
    }

    /// <summary>
    /// The by-key text lookups carry the time filter, so the parquet scan can skip every row group whose collection_time
    /// range cannot hold the rows asked for. Without it a lookup for one row decodes the text column of every day.
    /// </summary>
    [Fact]
    public async Task TheTextLookups_CarryTheTimeFilterIntoTheScan()
    {
        var duckDb = await BuildStoreAsync(textRepeat: 3);
        var from = _anchor.AddDays(-2);
        var to = _anchor.AddDays(-1);

        var byRow = await OracleAsync("EXPLAIN " + LocalDataService.TextByRowSql([1_000_000L + 10, 1_000_000L + 11]), ServerId, from, to);
        var window = await OracleAsync("EXPLAIN " + LocalDataService.WindowTextSql, ServerId, "db_odd", 7L, from, to);
        foreach (var (name, plan) in new[] { ("by-row", byRow), ("window", window) })
        {
            var text = string.Join("\n", plan.SelectMany(r => r.Values).Select(v => v?.ToString()));
            Assert.True(text.Contains("collection_time", StringComparison.Ordinal),
                $"the {name} text lookup's plan does not mention collection_time:\n{text}");
            /* The bound must be on the scan itself (a Filters line of the parquet scan), not only in a later FILTER. */
            Assert.Matches(@"(?s)(READ_PARQUET|PARQUET_SCAN|TABLE_SCAN).*Filters:.*collection_time", text);
            /* Both bounds must be literal comparisons on the column, which is what DuckDB turns into a row-group skip: a bound
               wrapped in a function (epoch_ms(collection_time) >= ...) still prints under Filters: but skips nothing. */
            var bare = Regex.Replace(text, @"[\s│┌┐└┘├┤┬┴┼─]+", "");
            Assert.Contains("collection_time>=", bare, StringComparison.Ordinal);
            Assert.Contains("collection_time<=", bare, StringComparison.Ordinal);
        }
    }

    // ---- shape ----

    [Fact]
    public void TheShippedReads_DoNotCarryTheWideColumnsThroughARankingWindow()
    {
        var root = FindRepoRoot();
        var store = File.ReadAllText(Path.Combine(root, "Lite", "Services", "LocalDataService.QueryStore.cs"));
        var regressions = File.ReadAllText(Path.Combine(root, "Lite", "Services", "LocalDataService.QueryStoreRegressions.cs"));

        /* The old top-queries lateral and its SELECT * dedup are what carried text through every row of the window. */
        Assert.DoesNotContain("LEFT JOIN LATERAL", store, StringComparison.Ordinal);
        Assert.DoesNotContain("        *,\n        ROW_NUMBER()", store.Replace("\r\n", "\n"), StringComparison.Ordinal);
        /* No dedup CTE in either file selects the wide columns beside its ROW_NUMBER. */
        foreach (var source in new[] { store, regressions })
        {
            foreach (Match m in Regex.Matches(source, @"SELECT(?<cols>[^()]*?)ROW_NUMBER\(\) OVER", RegexOptions.Singleline))
            {
                Assert.DoesNotContain("query_text", m.Groups["cols"].Value, StringComparison.Ordinal);
                Assert.DoesNotContain("query_plan_text", m.Groups["cols"].Value, StringComparison.Ordinal);
            }
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}

/// <summary>
/// The memory pins read a DuckDB database under a fixed memory_limit and one thread, so no other test class may run
/// beside them: the collection is not parallelised with the rest of the assembly.
/// </summary>
[CollectionDefinition(QueryStoreMemoryPinCollection.Name, DisableParallelization = true)]
public sealed class QueryStoreMemoryPinCollection
{
    public const string Name = "QueryStoreMemoryPins";
}
