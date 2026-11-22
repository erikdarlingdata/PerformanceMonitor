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
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9241 part 3: what the maintenance steps do with the query text dimension. The periodic archive and the Query Store slice
/// repair remove the texts no row references any more, the reset starts a new store with an empty dimension after exporting
/// every text, and the import flushes a source store's text into Parquet whatever schema version the source has.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class LiteTextDimensionMaintenanceTests : IDisposable
{
    private static readonly DateTime s_hot = DateTime.UtcNow.AddMinutes(-5);
    private static readonly DateTime s_old = DateTime.UtcNow.AddDays(-30);

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public LiteTextDimensionMaintenanceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTextDimMaint_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private async Task<DuckDbInitializer> InitAsync(string? path = null)
    {
        var initializer = new DuckDbInitializer(path ?? _dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeAsync();
        return initializer;
    }

    private async Task<DuckDBConnection> OpenAsync(string? path = null)
    {
        var connection = new DuckDBConnection($"Data Source={path ?? _dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object[] parameters)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = p });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<long> CountAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private static async Task<List<string?>> StringsAsync(DuckDBConnection connection, string sql)
    {
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

    private static async Task<List<string?>> SortedAsync(DuckDBConnection connection, string sql) =>
        (await StringsAsync(connection, sql)).OrderBy(s => s, StringComparer.Ordinal).ToList();

    private static Task AddToDimensionAsync(DuckDBConnection connection, string text) =>
        ExecAsync(connection, $"INSERT INTO query_text_dim VALUES ({LiteTextDimensions.DigestSql("$1")}, $1)", text);

    /// <summary>A query_stats row that carries only a digest, the way the write path stores it. The text must already be in the dimension.</summary>
    private static Task InsertStatsAsync(DuckDBConnection connection, long id, DateTime at, string digestOf) =>
        ExecAsync(connection,
            $"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, query_text, query_text_digest) "
            + $"VALUES ({id}, $1, 1, 'S1', NULL, {LiteTextDimensions.DigestSql("$2")})",
            at, digestOf);

    private static Task InsertStoreStatsAsync(DuckDBConnection connection, long id, DateTime at, string digestOf, long queryId = 1, long? intervalId = null) =>
        ExecAsync(connection,
            $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, "
            + $"first_execution_time, last_execution_time, execution_count, avg_duration_us, runtime_stats_interval_id, query_text, query_text_digest) "
            + $"VALUES ({id}, $1, 1, 'S1', 'db1', {queryId}, {queryId * 10}, 'Regular', $1, $1, 10, 500, {(intervalId is null ? "NULL" : intervalId.ToString())}, NULL, {LiteTextDimensions.DigestSql("$2")})",
            at, digestOf);

    private static Task InsertCorrectionAsync(DuckDBConnection connection, long id, DateTime at, string digestOf) =>
        ExecAsync(connection,
            $"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, query_text, query_text_digest) "
            + $"VALUES ({id}, $1, 1, 'S1', 'db1', NULL, {LiteTextDimensions.DigestSql("$2")})",
            at, digestOf);

    /// <summary>Removes the digest column and the view over it. DuckDB refuses DROP COLUMN on an indexed table, so the indexes go and come back.</summary>
    private static async Task DropDigestColumnAsync(DuckDBConnection connection, string table)
    {
        var indexes = await StringsAsync(connection, $"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table}'");
        foreach (var index in indexes) await ExecAsync(connection, $"DROP INDEX IF EXISTS {index}");
        await ExecAsync(connection, $"DROP VIEW IF EXISTS vh_{table}");
        await ExecAsync(connection, $"ALTER TABLE {table} DROP COLUMN query_text_digest");
        foreach (var statement in Schema.GetAllIndexStatements()) await ExecAsync(connection, statement);
    }

    private static string Text(string name) => $"SELECT '{name}' /* é€ */ " + new string('x', 5000);

    private static async Task<List<string?>> DimensionTextsAsync(DuckDBConnection connection) =>
        await SortedAsync(connection, "SELECT query_text FROM query_text_dim");

    private static List<string?> Sorted(params string[] texts) => texts.OrderBy(s => s, StringComparer.Ordinal).Cast<string?>().ToList();

    private static string FileOf(string dir, string table) =>
        Directory.GetFiles(dir, $"*_{table}.parquet").Single().Replace("\\", "/");

    /// <summary>
    /// The periodic archive removes the texts no hot row references and keeps the others: a text only in archived rows goes, a text
    /// a hot row still uses stays whether or not older rows of it were archived, and a text shared by two tables stays when one
    /// table's rows were archived. The dimension still holds one row per digest, and every hot row and every Parquet row reads its text.
    /// </summary>
    [Fact]
    public async Task ThePeriodicArchive_RemovesUnusedTexts_AndKeepsEveryTextAHotRowReferences()
    {
        var duckDb = await InitAsync();
        string Told = Text("old only"), Tshared = Text("old and hot"), Thot = Text("hot only"), Tcross = Text("old stats, hot store"),
            Tstore = Text("old store only"), Torphan = Text("no row");
        using (var connection = await OpenAsync())
        {
            foreach (var t in new[] { Told, Tshared, Thot, Tcross, Tstore, Torphan }) await AddToDimensionAsync(connection, t);
            await InsertStatsAsync(connection, 1, s_old, Told);
            await InsertStatsAsync(connection, 2, s_old, Tshared);
            await InsertStatsAsync(connection, 3, s_hot, Tshared);
            await InsertStatsAsync(connection, 4, s_hot, Thot);
            await InsertStatsAsync(connection, 5, s_old, Tcross);
            await InsertStoreStatsAsync(connection, 1, s_hot, Tcross, queryId: 1);
            await InsertStoreStatsAsync(connection, 2, s_old, Tstore, queryId: 2);
        }

        await new ArchiveService(duckDb, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        using var read = await OpenAsync();
        Assert.Equal(Sorted(Tshared, Thot, Tcross), await DimensionTextsAsync(read));
        Assert.Equal(await CountAsync(read, "SELECT COUNT(*) FROM query_text_dim"), await CountAsync(read, "SELECT COUNT(DISTINCT digest) FROM query_text_dim"));

        Assert.Equal(Sorted(Tshared, Thot), await SortedAsync(read, "SELECT query_text FROM vh_query_stats"));
        Assert.Equal(Sorted(Tcross), await SortedAsync(read, "SELECT query_text FROM vh_query_store_stats"));

        var statsFile = FileOf(_archiveDir, "query_stats");
        Assert.Equal(Sorted(Told, Tshared, Tcross), await SortedAsync(read, $"SELECT query_text FROM read_parquet('{statsFile}')"));
        Assert.Equal(Sorted(Tstore), await SortedAsync(read, $"SELECT query_text FROM read_parquet('{FileOf(_archiveDir, "query_store_stats")}')"));
        Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM v_query_stats WHERE query_text IS NULL"));
    }

    /// <summary>
    /// The cleanup never deletes a digest a row of any of the three tables references, and removes the one nothing references.
    /// A text each table uses alone and one all three share stay.
    /// </summary>
    [Fact]
    public async Task TheCleanup_KeepsADigestAnyOfTheThreeTablesReferences_AndRemovesTheUnreferencedOne()
    {
        await InitAsync();
        string Tstats = Text("stats"), Tstore = Text("store"), Tcorrection = Text("correction"), Tall = Text("all three"), Torphan = Text("orphan");
        using var connection = await OpenAsync();
        foreach (var t in new[] { Tstats, Tstore, Tcorrection, Tall, Torphan }) await AddToDimensionAsync(connection, t);
        await InsertStatsAsync(connection, 1, s_hot, Tstats);
        await InsertStatsAsync(connection, 2, s_hot, Tall);
        await InsertStoreStatsAsync(connection, 1, s_hot, Tstore, queryId: 1);
        await InsertStoreStatsAsync(connection, 2, s_hot, Tall, queryId: 2);
        await InsertCorrectionAsync(connection, 1, s_hot, Tcorrection);
        await InsertCorrectionAsync(connection, 2, s_hot, Tall);

        var removed = TextDimensionWriter.RemoveUnreferenced(connection);

        Assert.Equal(1L, removed);
        Assert.Equal(Sorted(Tstats, Tstore, Tcorrection, Tall), await DimensionTextsAsync(connection));
        Assert.Equal(0L, await CountAsync(connection, "SELECT COUNT(*) FROM vh_plan_correction WHERE query_text IS NULL"));

        /* A second run has nothing to remove. */
        Assert.Equal(0L, TextDimensionWriter.RemoveUnreferenced(connection));
    }

    /// <summary>
    /// A cleanup that fails does not fail the step it runs after: with the dimension table gone the cleanup throws, the helper
    /// returns 0 and the archive still archives and deletes its rows.
    /// </summary>
    [Fact]
    public async Task ACleanupFailure_DoesNotFailThePeriodicArchive()
    {
        var duckDb = await InitAsync();
        using (var connection = await OpenAsync())
        {
            await AddToDimensionAsync(connection, Text("a"));
            await InsertStatsAsync(connection, 1, s_old, Text("a"));
            await DropDigestColumnAsync(connection, "plan_correction");
        }

        /* plan_correction has no digest column now, so the cleanup's reference check on it fails after query_stats' rows go. */
        var complete = await new ArchiveService(duckDb, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        using var read = await OpenAsync();
        Assert.True(complete);
        Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM query_stats"));
        Assert.Single(Directory.GetFiles(_archiveDir, "*_query_stats.parquet"));
        Assert.Equal(1L, await CountAsync(read, "SELECT COUNT(*) FROM query_text_dim"));
    }

    /// <summary>
    /// The slice repair collapses split Query Store slices and then removes the texts nothing references: the text only a removed
    /// slice used goes, the surviving slice's text and an ordinary row's text stay, and every remaining row still reads its text.
    /// </summary>
    [Fact]
    public async Task TheSliceRepair_ThenCleanup_LeavesEveryRemainingRowsTextReadable()
    {
        var duckDb = await InitAsync();
        string Tsurvivor = Text("survivor slice"), Tremoved = Text("removed slice"), Tother = Text("ordinary row"), Torphan = Text("orphan");
        using (var connection = await OpenAsync())
        {
            foreach (var t in new[] { Tsurvivor, Tremoved, Tother, Torphan }) await AddToDimensionAsync(connection, t);

            /* One interval stored as two tied slices with different digests (the lower rowid survives), an ordinary interval, and
               a text no row uses. */
            await InsertStoreStatsAsync(connection, 1, s_hot, Tsurvivor, queryId: 1, intervalId: 5001);
            await InsertStoreStatsAsync(connection, 2, s_hot, Tremoved, queryId: 1, intervalId: 5001);
            await InsertStoreStatsAsync(connection, 3, s_hot, Tother, queryId: 2, intervalId: 5002);
        }

        var result = await new QueryStoreSliceRepairService(duckDb, _archiveDir).RepairAsync();

        using var read = await OpenAsync();
        Assert.Equal(1L, result.RowsRemoved);
        Assert.Equal(Sorted(Tsurvivor, Tother), await DimensionTextsAsync(read));
        Assert.Equal(Sorted(Tsurvivor, Tother), await SortedAsync(read, "SELECT query_text FROM v_query_store_stats"));
        Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM v_query_store_stats WHERE query_text IS NULL"));
    }

    /// <summary>
    /// The reset exports every row's text to Parquet through the dimension, deletes the store file so the new store starts with an
    /// empty dimension, and a collector write after the reset stores its text in the dimension and reads it back.
    /// </summary>
    [Fact]
    public async Task TheReset_ExportsEveryText_StartsWithAnEmptyDimension_AndLaterWritesGoThroughIt()
    {
        var duckDb = await InitAsync();
        string Ta = Text("a"), Tb = Text("b"), Tc = Text("c");
        List<string?> beforeStats, beforeStore;
        using (var connection = await OpenAsync())
        {
            foreach (var t in new[] { Ta, Tb, Tc }) await AddToDimensionAsync(connection, t);
            await InsertStatsAsync(connection, 1, s_hot, Ta);
            await InsertStatsAsync(connection, 2, s_hot, Ta);
            await InsertStatsAsync(connection, 3, s_hot, Tb);
            await InsertStoreStatsAsync(connection, 1, s_hot, Tc, queryId: 1);
            beforeStats = await SortedAsync(connection, "SELECT query_text FROM vh_query_stats");
            beforeStore = await SortedAsync(connection, "SELECT query_text FROM vh_query_store_stats");
        }

        Assert.Equal(Sorted(Ta, Ta, Tb), beforeStats);

        await new ArchiveService(duckDb, _archiveDir).ArchiveAllAndResetAsync();

        using (var read = await OpenAsync())
        {
            Assert.Equal(beforeStats, await SortedAsync(read, $"SELECT query_text FROM read_parquet('{FileOf(_archiveDir, "query_stats")}')"));
            Assert.Equal(beforeStore, await SortedAsync(read, $"SELECT query_text FROM read_parquet('{FileOf(_archiveDir, "query_store_stats")}')"));
            Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM query_text_dim"));
            Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM query_stats"));
        }

        var written = Write(duckDb, Text("after the reset"));

        using var after = await OpenAsync();
        Assert.Equal(1, written);
        Assert.Equal(Sorted(Text("after the reset")), await DimensionTextsAsync(after));
        Assert.Equal(1L, await CountAsync(after, "SELECT COUNT(*) FROM query_stats WHERE query_text IS NULL AND query_text_digest IS NOT NULL"));
        Assert.Equal(Sorted(Text("after the reset")), await SortedAsync(after, "SELECT query_text FROM vh_query_stats"));
    }

    private static int Write(DuckDbInitializer store, string text)
    {
        var at = DateTime.UtcNow;
        var row = new QueryStatsCollector.Row
        {
            DatabaseName = "AppDb",
            QueryHash = "0xAA",
            QueryPlanHash = "0xBB",
            CreationTime = at.AddDays(-1),
            LastExecutionTime = at.AddMinutes(-1),
            ExecutionCount = 100,
            TotalWorkerTime = 100_000,
            TotalElapsedTime = 200_000,
            TotalLogicalReads = 5000,
            SqlHandle = "0x01AA",
            PlanHandle = "0x02AA",
            QueryText = text,
        };
        using var readLock = store.AcquireReadLock();
        using var connection = store.CreateConnection();
        connection.Open();
        var context = new CollectorContext { ServerId = 1, ServerName = "S1", CollectionTime = at, Deltas = new DeltaCalculator() };
        return RemoteCollectorService.WriteBatch(connection, QueryStatsCollector.Instance, [row], 1, "S1", at, context);
    }

    /// <summary>
    /// A store written before the digest columns (an older source) and one written after them flush to the same Parquet shape: the
    /// text on every row and no digest column.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheImportFlush_CarriesTheTextOnEveryRow_AndNoDigestColumn(bool olderThanV85)
    {
        var source = Path.Combine(_tempDir, olderThanV85 ? "old-source" : "new-source");
        Directory.CreateDirectory(source);
        var sourceDb = Path.Combine(source, "monitor.duckdb");
        string Ta = Text("a"), Tb = Text("b");

        var store = await InitAsync(sourceDb);
        using (var connection = await OpenAsync(sourceDb))
        {
            if (olderThanV85)
            {
                /* The v84 shape: inline text, no digest columns, no dimension, no vh_ views. */
                await ExecAsync(connection, "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, query_text) VALUES (1, $1, 1, 'S1', $2), (2, $1, 1, 'S1', $3)", s_hot, Ta, Tb);
                await ExecAsync(connection, "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_text) VALUES (1, $1, 1, 'S1', 'db1', $2)", s_hot, Ta);
                foreach (var table in new[] { "query_stats", "query_store_stats", "plan_correction" })
                {
                    await DropDigestColumnAsync(connection, table);
                }

                await ExecAsync(connection, "DROP TABLE query_text_dim");
                await ExecAsync(connection, "UPDATE schema_version SET version = 84");
            }
            else
            {
                await AddToDimensionAsync(connection, Ta);
                await AddToDimensionAsync(connection, Tb);
                await InsertStatsAsync(connection, 1, s_hot, Ta);
                await InsertStatsAsync(connection, 2, s_hot, Tb);
                await InsertStoreStatsAsync(connection, 1, s_hot, Ta);
            }
        }

        store.Dispose();
        _initializers.Clear();

        var (failure, flushed, _) = DataImportService.FlushOldDatabase(source);

        Assert.Equal(DataImportOpenFailure.None, failure);
        Assert.Equal(2, flushed);
        var archive = Path.Combine(source, "archive");
        using var read = await OpenAsync(Path.Combine(_tempDir, "reader.duckdb"));
        var statsFile = FileOf(archive, "query_stats");
        Assert.Equal(Sorted(Ta, Tb), await SortedAsync(read, $"SELECT query_text FROM read_parquet('{statsFile}')"));
        Assert.Equal(Sorted(Ta), await SortedAsync(read, $"SELECT query_text FROM read_parquet('{FileOf(archive, "query_store_stats")}')"));
        Assert.Equal(0L, await CountAsync(read, $"SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet('{statsFile}')) WHERE column_name = 'query_text_digest'"));
    }

    /// <summary>
    /// A file written before schema 85 (text inline, no digest column) and one written now read the same through <c>v_</c>, the
    /// view that unions the hot rows with the Parquet files: one column set, and the text on every row.
    /// </summary>
    [Fact]
    public async Task AnOldParquetFile_AndANewOne_ReadTheSameThroughTheView()
    {
        var duckDb = await InitAsync();
        string Told = Text("in the old file"), Tnew = Text("in the new file"), Thot = Text("hot");
        using (var connection = await OpenAsync())
        {
            /* The old file: the table's columns before the digest column, with the text inline. */
            var oldFile = Path.Combine(_archiveDir, "20250101_0000_query_stats.parquet").Replace("\\", "/");
            await ExecAsync(connection, "CREATE TEMP TABLE old_rows AS SELECT * EXCLUDE (query_text_digest) FROM query_stats LIMIT 0");
            await ExecAsync(connection, "INSERT INTO old_rows (collection_id, collection_time, server_id, server_name, query_text) VALUES (1, $1, 1, 'S1', $2)", s_old, Told);
            await ExecAsync(connection, $"COPY old_rows TO '{oldFile}' (FORMAT PARQUET)");

            /* The new file comes out of a real archive pass over rows that carry only a digest. */
            await AddToDimensionAsync(connection, Tnew);
            await AddToDimensionAsync(connection, Thot);
            await InsertStatsAsync(connection, 2, s_old.AddDays(-1), Tnew);
            await InsertStatsAsync(connection, 3, s_hot, Thot);
        }

        await new ArchiveService(duckDb, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);
        await duckDb.CreateArchiveViewsAsync();

        using var read = await OpenAsync();
        Assert.Equal(Sorted(Told, Tnew, Thot), await SortedAsync(read, "SELECT query_text FROM v_query_stats"));
        Assert.Equal(3L, await CountAsync(read, "SELECT COUNT(*) FROM v_query_stats"));
        Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM v_query_stats) WHERE column_name = 'query_text_digest'"));
    }
}
