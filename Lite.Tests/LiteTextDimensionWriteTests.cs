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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9241 part 2: the write path of the query text dimension. Runs the collectors' real <c>WriteBatch</c> against real DuckDB files
/// and reads the stored rows back. The oracle writes the same rows through the old inline path into one store and through the
/// dimension into another, and the reads of both must agree.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class LiteTextDimensionWriteTests : IDisposable
{
    private const string ServerName = "TextDimServer";
    private static readonly DateTime s_now = DateTime.UtcNow;

    /// <summary>The id the MCP tools resolve for <see cref="ServerName"/>, so the oracle's rows are the ones every reader asks for.</summary>
    private static readonly int s_oracleServerId = RemoteCollectorService.GetServerId(new ServerConnection { ServerName = ServerName, DisplayName = ServerName });

    private readonly string _tempDir;
    private readonly List<DuckDbInitializer> _stores = [];

    public LiteTextDimensionWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTextDimWrite_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        foreach (var store in _stores)
        {
            store.Dispose();
        }

        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private async Task<DuckDbInitializer> NewStoreAsync(string name)
    {
        var store = new DuckDbInitializer(Path.Combine(_tempDir, name + ".duckdb"));
        _stores.Add(store);
        await store.InitializeAsync();
        return store;
    }

    /// <summary>A store whose vh_ views are gone, which is the shape a store has when a view could not be built: a write is inline.</summary>
    private static async Task DropTextViewsAsync(DuckDbInitializer store)
    {
        foreach (var table in LiteTextDimensions.All.Select(d => d.Table).Distinct())
        {
            await ExecAsync(store, $"DROP VIEW IF EXISTS {LiteTextDimensions.ViewName(table)}");
        }
    }

    private static async Task RestoreTextViewsAsync(DuckDbInitializer store)
    {
        foreach (var table in LiteTextDimensions.All.Select(d => d.Table).Distinct())
        {
            await ExecAsync(store, LiteTextDimensions.CreateViewSql(table));
        }
    }

    private static async Task ExecAsync(DuckDbInitializer store, string sql)
    {
        using var connection = store.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<long> CountAsync(DuckDbInitializer store, string sql)
    {
        using var connection = store.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private static async Task<List<string?>> StringsAsync(DuckDbInitializer store, string sql)
    {
        using var connection = store.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var result = new List<string?>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            result.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
        }

        return result;
    }

    private static CollectorContext ContextFor(DuckDbInitializer store, int serverId, DateTime at, ICollectorDeltaCalculator deltas) =>
        new() { ServerId = serverId, ServerName = ServerName, CollectionTime = at, Deltas = deltas };

    /// <summary>The collector's real batch write on its own connection under the store's read lock, as a collector holds it.</summary>
    private static int Write<TRow>(DuckDbInitializer store, ICollectorDefinition<TRow> definition, List<TRow> rows, int serverId, DateTime at, ICollectorDeltaCalculator? deltas = null)
    {
        using var readLock = store.AcquireReadLock();
        using var connection = store.CreateConnection();
        connection.Open();
        return RemoteCollectorService.WriteBatch(connection, definition, rows, serverId, ServerName, at, ContextFor(store, serverId, at, deltas ?? new DeltaCalculator()));
    }

    private static QueryStatsCollector.Row StatsRow(string? text, string hash = "0xAA", long executions = 100) => new()
    {
        DatabaseName = "AppDb",
        QueryHash = hash,
        QueryPlanHash = "0xBB",
        CreationTime = s_now.AddDays(-1),
        LastExecutionTime = s_now.AddMinutes(-1),
        ExecutionCount = executions,
        TotalWorkerTime = executions * 1000,
        TotalElapsedTime = executions * 2000,
        TotalLogicalReads = executions * 50,
        TotalSpills = executions * 3,
        SqlHandle = "0x01" + hash[2..],
        PlanHandle = "0x02" + hash[2..],
        QueryText = text,
    };

    private static QueryStoreCollector.Row StoreRow(string? text, long queryId = 7, long planId = 70) => new()
    {
        DatabaseName = "AppDb",
        QueryId = queryId,
        PlanId = planId,
        ExecutionTypeDesc = "Regular",
        FirstExecutionTime = s_now.AddHours(-2),
        LastExecutionTime = s_now.AddMinutes(-2),
        QueryText = text,
        QueryHash = "0xAB" + queryId,
        ExecutionCount = 25,
        AvgDurationUs = 4000,
        MinDurationUs = 100,
        MaxDurationUs = 9000,
        AvgCpuTimeUs = 3000,
        MaxCpuTimeUs = 7000,
        AvgLogicalIoReads = 500,
        QueryPlanHash = "0xCD" + planId,
        IntervalStartTimeUtc = s_now.AddMinutes(-30),
    };

    private static PlanCorrectionCollector.Row CorrectionRow(string? text, long queryId = 7) => new()
    {
        DbName = "AppDb",
        DesiredState = "ON",
        ActualState = "ON",
        RecommendationName = "rec" + queryId,
        RecommendationType = "FORCE_LAST_GOOD_PLAN",
        QueryId = queryId,
        QueryText = text,
        RegressedPlanId = 11,
        LastGoodPlanId = 10,
        EstimatedGainSeconds = 12.5,
    };

    /// <summary>The three diverted tables, each with a writer of its rows from a list of texts.</summary>
    private sealed record Diverted(string Table, Func<DuckDbInitializer, List<string?>, int, DateTime, ICollectorDeltaCalculator, int> Write);

    private static readonly Diverted[] s_diverted =
    [
        new("query_stats", (s, texts, id, at, d) => Write(s, QueryStatsCollector.Instance, texts.Select((t, i) => StatsRow(t, "0x" + (0xA0 + i).ToString("X")[^2..], 100 + 10 * (long)(at - s_now).TotalMinutes + 200)).ToList(), id, at, d)),
        new("query_store_stats", (s, texts, id, at, d) => Write(s, QueryStoreCollector.Instance, texts.Select((t, i) => StoreRow(t, 100 + i, 1000 + i)).ToList(), id, at, d)),
        new("plan_correction", (s, texts, id, at, d) => Write(s, PlanCorrectionCollector.Instance, texts.Select((t, i) => CorrectionRow(t, 200 + i)).ToList(), id, at, d)),
    ];

    public static IEnumerable<object[]> DivertedTables() => s_diverted.Select(d => new object[] { d.Table });

    private static Diverted For(string table) => s_diverted.Single(d => d.Table == table);

    [Theory]
    [MemberData(nameof(DivertedTables))]
    public async Task OneText_CollectedManyTimesAcrossRoundsAndServers_IsStoredOnce(string table)
    {
        var store = await NewStoreAsync("once");
        var diverted = For(table);
        var deltas = new DeltaCalculator();
        var text = "SELECT * FROM dbo.Orders WHERE OrderId = @id";

        for (var round = 0; round < 3; round++)
        {
            foreach (var serverId in new[] { 1, 2 })
            {
                diverted.Write(store, [text], serverId, s_now.AddMinutes(-10 + round), deltas);
            }
        }

        Assert.Equal(1, await CountAsync(store, "SELECT count(*) FROM query_text_dim"));
        Assert.Equal(6, await CountAsync(store, $"SELECT count(*) FROM {table} WHERE query_text_digest IS NOT NULL AND query_text IS NULL"));
        Assert.Equal(6, await CountAsync(store, $"SELECT count(*) FROM v_{table} WHERE query_text = '{text}'"));
    }

    [Fact]
    public async Task TheSameTextInQueryStatsAndQueryStore_ShareOneDimensionRow()
    {
        var store = await NewStoreAsync("shared");
        var text = "EXEC dbo.Shared";
        For("query_stats").Write(store, [text], 1, s_now.AddMinutes(-3), new DeltaCalculator());
        For("query_store_stats").Write(store, [text], 1, s_now.AddMinutes(-3), new DeltaCalculator());
        For("plan_correction").Write(store, [text], 1, s_now.AddMinutes(-3), new DeltaCalculator());

        Assert.Equal(1, await CountAsync(store, "SELECT count(*) FROM query_text_dim"));
    }

    [Theory]
    [MemberData(nameof(DivertedTables))]
    public async Task EdgeTexts_EmptyNullLongAndNonAscii_RoundTripExactly(string table)
    {
        var store = await NewStoreAsync("edge");
        var longText = new string('x', 5000) + "-end";
        var nonAscii = "SELECT N'日本語 ñ ü' AS \"ключ\", N'\U0001F642'";
        var texts = new List<string?> { "", null, longText, nonAscii };

        For(table).Write(store, texts, 1, s_now.AddMinutes(-3), new DeltaCalculator());

        var back = (await StringsAsync(store, $"SELECT coalesce(query_text, '<null>') FROM v_{table}"))
            .Select(t => t == "<null>" ? null : t).ToList();
        Assert.Equal(texts.OrderBy(t => t, StringComparer.Ordinal).ToList(), back.OrderBy(t => t, StringComparer.Ordinal).ToList());

        /* NULL text stores no dimension row and no digest. The empty string does store one, and reads back empty, not NULL. */
        Assert.Equal(3, await CountAsync(store, "SELECT count(*) FROM query_text_dim"));
        Assert.Equal(1, await CountAsync(store, $"SELECT count(*) FROM {table} WHERE query_text_digest IS NULL"));
        Assert.Equal(1, await CountAsync(store, $"SELECT count(*) FROM v_{table} WHERE query_text = ''"));
        Assert.Equal(1, await CountAsync(store, $"SELECT count(*) FROM v_{table} WHERE query_text = '{longText}'"));
    }

    [Fact]
    public async Task TwoWritersOfTheSameNewText_OnSeparateConnections_StoreOneDimensionRow()
    {
        var store = await NewStoreAsync("race");
        var texts = new List<string> { "T" };
        const int rounds = 8;

        for (var round = 0; round < rounds; round++)
        {
            /* A fresh batch of long texts each round, so the second writer reaches its anti-join while the first is still publishing. */
            var batch = Enumerable.Range(0, 200).Select(i => (string?)($"round {round} text {i} " + new string('p', 3000))).ToList();
            using var barrier = new Barrier(2);
            var tasks = new[] { 1, 2 }.Select(serverId => Task.Run(() =>
            {
                barrier.SignalAndWait();
                return For("query_stats").Write(store, batch, serverId, s_now.AddMinutes(-5), new DeltaCalculator());
            })).ToArray();
            await Task.WhenAll(tasks);
            texts.AddRange(batch!);
        }

        Assert.Equal(rounds * 200, await CountAsync(store, "SELECT count(*) FROM query_text_dim"));
        Assert.Equal(0, await CountAsync(store, "SELECT count(*) FROM (SELECT digest FROM query_text_dim GROUP BY digest HAVING count(*) > 1)"));
        Assert.Equal(rounds * 200 * 2, await CountAsync(store, "SELECT count(*) FROM v_query_stats"));
    }

    private sealed class RollbackRows : List<QueryStatsCollector.Row>, IBaselineRollback
    {
        public int RolledBack { get; private set; }

        public void RollBackBaselines() => RolledBack++;
    }

    [Fact]
    public async Task AFailedPublish_StoresNothingAndRollsTheBaselinesBack()
    {
        var store = await NewStoreAsync("fail");
        /* A view that exists over a missing dimension table: the write takes the dimension path and its transaction fails. */
        await ExecAsync(store, "DROP TABLE query_text_dim");
        var rows = new RollbackRows { StatsRow("SELECT 1") };

        Assert.ThrowsAny<Exception>(() => Write(store, QueryStatsCollector.Instance, rows, 1, s_now));

        Assert.Equal(1, rows.RolledBack);
        Assert.Equal(0, await CountAsync(store, "SELECT count(*) FROM query_stats"));
        /* The stage is gone and the next write on a fresh connection is not blocked by the failed one. */
        await ExecAsync(store, LiteTextDimensions.CreateQueryTextDimTable);
        Assert.Equal(1, Write(store, QueryStatsCollector.Instance, [StatsRow("SELECT 2")], 1, s_now));
        Assert.Equal(1, await CountAsync(store, "SELECT count(*) FROM v_query_stats WHERE query_text = 'SELECT 2'"));
    }

    [Theory]
    [MemberData(nameof(DivertedTables))]
    public async Task AStoreWithoutItsTextView_WritesTheTextInline(string table)
    {
        var store = await NewStoreAsync("noview");
        await DropTextViewsAsync(store);

        For(table).Write(store, ["SELECT inline"], 1, s_now.AddMinutes(-3), new DeltaCalculator());

        Assert.Equal(0, await CountAsync(store, "SELECT count(*) FROM query_text_dim"));
        Assert.Equal(1, await CountAsync(store, $"SELECT count(*) FROM {table} WHERE query_text = 'SELECT inline' AND query_text_digest IS NULL"));
    }

    /// <summary>The same rows through the inline path (the store has no vh_ view while they are written) and through the dimension.</summary>
    private async Task<(DuckDbInitializer Inline, DuckDbInitializer Dimension)> SeedBothAsync()
    {
        var inline = await NewStoreAsync("oracle_inline");
        var dimension = await NewStoreAsync("oracle_dim");
        await DropTextViewsAsync(inline);

        var texts = new List<string?> { "SELECT a FROM t", "SELECT b FROM t", "", null, new string('z', 4500), "SELECT N'日本語'" };
        foreach (var (store, deltas) in new[] { (inline, new DeltaCalculator()), (dimension, new DeltaCalculator()) })
        {
            foreach (var diverted in s_diverted)
            {
                /* Two rounds on one server, then a second server repeating the first text. */
                diverted.Write(store, texts, s_oracleServerId, s_now.AddMinutes(-20), deltas);
                diverted.Write(store, texts, s_oracleServerId, s_now.AddMinutes(-10), deltas);
                diverted.Write(store, [texts[0]], s_oracleServerId + 1, s_now.AddMinutes(-10), deltas);
            }
        }

        await RestoreTextViewsAsync(inline);
        return (inline, dimension);
    }

    [Theory]
    [MemberData(nameof(DivertedTables))]
    public async Task Oracle_TheViewOverTheDimensionStoreEqualsTheViewOverTheInlineStore(string table)
    {
        var (inline, dimension) = await SeedBothAsync();
        Assert.True(await CountAsync(inline, $"SELECT count(*) FROM v_{table}") > 0);

        /* collection_id comes from a generator that differs per write, so the comparison leaves that one column out. */
        var read = $"SELECT * EXCLUDE (collection_id) FROM v_{table}";
        var inlineRows = await RowsAsync(inline, read);
        var dimensionRows = await RowsAsync(dimension, read);

        Assert.Equal(inlineRows.Count, dimensionRows.Count);
        Assert.Equal(inlineRows.OrderBy(r => r, StringComparer.Ordinal), dimensionRows.OrderBy(r => r, StringComparer.Ordinal));

        /* And in the database itself, both directions, on every column of the view: nothing differs and no row is lost or doubled.
           The inline store's rows go through a Parquet file, which keeps every column's type. */
        var file = Path.Combine(_tempDir, $"oracle_{table}.parquet").Replace(Path.DirectorySeparatorChar, '/');
        await ExecAsync(inline, $"COPY (SELECT * EXCLUDE (collection_id) FROM v_{table}) TO '{file}' (FORMAT PARQUET)");
        Assert.Equal(0, await CountAsync(dimension, $"SELECT count(*) FROM (SELECT * EXCLUDE (collection_id) FROM v_{table} EXCEPT ALL SELECT * FROM read_parquet('{file}'))"));
        Assert.Equal(0, await CountAsync(dimension, $"SELECT count(*) FROM (SELECT * FROM read_parquet('{file}') EXCEPT ALL SELECT * EXCLUDE (collection_id) FROM v_{table})"));
    }

    private static async Task<List<string>> RowsAsync(DuckDbInitializer store, string sql)
    {
        using var connection = store.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var result = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var sb = new StringBuilder();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                sb.Append(reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)).Append('|');
            }

            result.Add(sb.ToString());
        }

        return result;
    }

    /// <summary>The first text <see cref="SeedBothAsync"/> writes to every table, which every reader below reads.</summary>
    private const string SeededText = "SELECT a FROM t";

    [Fact]
    public async Task Oracle_KeyReaders_ReturnTheSameResultsFromBothStores()
    {
        var (inline, dimension) = await SeedBothAsync();
        var serverManager = new ServerManager(_tempDir);
        var connection = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        serverManager.AddServer(connection);
        var serverId = s_oracleServerId;

        var readers = new (string Name, Func<LocalDataService, Task<object?>> Read)[]
        {
            ("query store top", async s => await s.GetQueryStoreTopQueriesAsync(serverId, hoursBack: 6)),
            ("query store comparison", async s => (await s.GetQueryStoreComparisonAsync(serverId, s_now.AddHours(-1), s_now.AddMinutes(1), s_now.AddHours(-6), s_now.AddHours(-1))).Select(i => new { i.QueryHash, i.QueryText }).OrderBy(i => i.QueryHash, StringComparer.Ordinal).ToList()),
            ("query stats top by cpu", async s => (await s.GetTopQueriesByCpuAsync(serverId, hoursBack: 6)).Select(r => new { r.QueryHash, r.QueryText }).OrderBy(r => r.QueryHash, StringComparer.Ordinal).ToList()),
            ("plan corrections", async s => await s.GetPlanCorrectionsAsync(serverId, hoursBack: 6)),
            ("mcp top queries by cpu", async s => await McpQueryTools.GetTopQueriesByCpu(s, serverManager, ServerName, 6, 10)),
        };

        foreach (var (name, read) in readers)
        {
            var fromInline = Serialize(await read(new LocalDataService(inline)));
            var fromDimension = Serialize(await read(new LocalDataService(dimension)));
            /* The seeded text must be in BOTH results: a reader that returned no rows, or rows with no text, would still be equal. */
            Assert.True(fromInline.Contains(SeededText, StringComparison.Ordinal), $"{name}: the inline store result lacks the seeded text: {fromInline[..Math.Min(300, fromInline.Length)]}");
            Assert.True(fromDimension.Contains(SeededText, StringComparison.Ordinal), $"{name}: the dimension store result lacks the seeded text: {fromDimension[..Math.Min(300, fromDimension.Length)]}");
            Assert.Equal(fromInline, fromDimension);
        }

        /* The history reads carry metrics and no text, so they are compared for equality and for rows. */
        var historyInline = await new LocalDataService(inline).GetQueryStoreHistoryAsync(serverId, "AppDb", 100, hoursBack: 6);
        var historyDimension = await new LocalDataService(dimension).GetQueryStoreHistoryAsync(serverId, "AppDb", 100, hoursBack: 6);
        Assert.NotEmpty(historyInline);
        Assert.Equal(Serialize(historyInline), Serialize(historyDimension));
        var statsInline = await new LocalDataService(inline).GetQueryStatsHistoryAsync(serverId, "AppDb", "0xA0", hoursBack: 6);
        var statsDimension = await new LocalDataService(dimension).GetQueryStatsHistoryAsync(serverId, "AppDb", "0xA0", hoursBack: 6);
        Assert.NotEmpty(statsInline);
        Assert.Equal(Serialize(statsInline), Serialize(statsDimension));

        /* The fact collector reads query_stats and Query Store text for the analysis. */
        var context = new AnalysisContext { ServerId = serverId, ServerName = ServerName, TimeRangeStart = s_now.AddHours(-4), TimeRangeEnd = s_now.AddMinutes(1) };
        var inlineFacts = await FactsAsync(inline, context);
        var dimensionFacts = await FactsAsync(dimension, context);
        Assert.NotEmpty(inlineFacts);
        Assert.Equal(inlineFacts, dimensionFacts);
    }

    private static async Task<List<string>> FactsAsync(DuckDbInitializer store, AnalysisContext context)
    {
        await SecondaryReplicaScope.EnsureAsync(store, context);
        var facts = await new DuckDbFactCollector(store).CollectFactsAsync(context);
        return facts.Select(f => $"{f.Source}|{f.Key}|{f.Value}|{f.DatabaseName}|{f.ObjectName}").OrderBy(f => f, StringComparer.Ordinal).ToList();
    }

    private static string Serialize(object? value) => value as string ?? JsonSerializer.Serialize(value);
}
