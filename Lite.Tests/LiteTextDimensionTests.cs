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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9241 part 1: the query text dimension, the digest columns, the <c>vh_</c> views and the exports that read through them.
/// Nothing writes a digest yet, so these seed one by hand: a hot row that carries a digest and NULL inline text is what the
/// write path of the next part stores, and every reader and every export must already return its text.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class LiteTextDimensionTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public LiteTextDimensionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTextDim_" + Guid.NewGuid().ToString("N")[..8]);
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

    private async Task<DuckDbInitializer> InitAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
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

    private async Task ExecAsync(DuckDBConnection connection, string sql, params object[] parameters)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = p });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<List<string>> ColumnsAsync(DuckDBConnection connection, string relation)
    {
        var result = new List<string>();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT column_name || ' ' || column_type FROM (DESCRIBE SELECT * FROM {relation})";
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    private async Task<long> CountAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>Puts <paramref name="text"/> in the dimension and returns nothing: the digest is computed in DuckDB, as the writer does.</summary>
    private Task AddToDimensionAsync(DuckDBConnection connection, string text) =>
        ExecAsync(connection, $"INSERT INTO query_text_dim VALUES ({LiteTextDimensions.DigestSql("$1")}, $1)", text);

    /// <summary>One query_stats row. <paramref name="inlineText"/> null leaves the column NULL, and <paramref name="digestOf"/> null leaves the digest NULL.</summary>
    private Task InsertQueryStatsAsync(DuckDBConnection connection, long id, string? inlineText, string? digestOf) =>
        ExecAsync(connection,
            $"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, query_text, query_text_digest) "
            + $"VALUES ({id}, TIMESTAMP '2026-01-01 00:00:00' + INTERVAL ({id}) MINUTE, 1, 'S1', $1, "
            + (digestOf is null ? "NULL" : LiteTextDimensions.DigestSql("$2")) + ")",
            digestOf is null ? [inlineText ?? (object)DBNull.Value] : [inlineText ?? (object)DBNull.Value, digestOf]);

    private Task InsertQueryStoreStatsAsync(DuckDBConnection connection, long id, string? inlineText, string? digestOf) =>
        ExecAsync(connection,
            $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_text, query_text_digest) "
            + $"VALUES ({id}, TIMESTAMP '2026-01-01 00:00:00' + INTERVAL ({id}) MINUTE, 1, 'S1', 'db1', $1, "
            + (digestOf is null ? "NULL" : LiteTextDimensions.DigestSql("$2")) + ")",
            digestOf is null ? [inlineText ?? (object)DBNull.Value] : [inlineText ?? (object)DBNull.Value, digestOf]);

    private static readonly string LongText = "SELECT /* é€𝄞 */ " + new string('x', 5000) + " FROM dbo.t";
    private const string NonAsciiText = "SELECT N'Привет, 世界 \U0001F600' AS greeting";

    /// <summary>
    /// A v84 store gets the digest columns and the dimension at the next start and is stamped v85, nothing is rewritten, and its
    /// inline rows read the same text through <c>v_</c>. The upgraded store ends with the same columns, in the same order, as a
    /// fresh one, for the two tables and for the <c>v_</c> view over each.
    /// </summary>
    [Fact]
    public async Task AV84Store_UpgradesToV85_KeepsItsInlineText_AndMatchesAFreshStoreColumn_ForColumn()
    {
        var first = await InitAsync();
        using (var connection = await OpenAsync())
        {
            await InsertQueryStatsAsync(connection, 1, "SELECT 1", null);
            await InsertQueryStatsAsync(connection, 2, null, null);
            await InsertQueryStatsAsync(connection, 3, "", null);
            await InsertQueryStoreStatsAsync(connection, 1, LongText, null);

            /* The v84 shape: no digest columns, no dimension, no vh_ views, stamped 84. DuckDB refuses DROP COLUMN on an indexed
               table, so the indexes go and come straight back. */
            foreach (var table in new[] { "query_stats", "query_store_stats", "plan_correction" })
            {
                using var names = connection.CreateCommand();
                names.CommandText = $"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table}'";
                var indexes = new List<string>();
                using (var reader = await names.ExecuteReaderAsync(TestContext.Current.CancellationToken))
                {
                    while (await reader.ReadAsync(TestContext.Current.CancellationToken)) indexes.Add(reader.GetString(0));
                }

                foreach (var index in indexes) await ExecAsync(connection, $"DROP INDEX IF EXISTS {index}");
                await ExecAsync(connection, $"DROP VIEW IF EXISTS vh_{table}");
                await ExecAsync(connection, $"ALTER TABLE {table} DROP COLUMN query_text_digest");
            }

            foreach (var statement in Schema.GetAllIndexStatements()) await ExecAsync(connection, statement);
            await ExecAsync(connection, "DROP TABLE query_text_dim");
            await ExecAsync(connection, "UPDATE schema_version SET version = 84");
        }

        first.Dispose();
        _initializers.Clear();

        var upgraded = await InitAsync();

        using (var connection = await OpenAsync())
        {
            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(connection, "SELECT MAX(version) FROM schema_version"));
            Assert.Equal(0L, await CountAsync(connection, "SELECT COUNT(*) FROM query_text_dim"));

            /* Zero rewrite: the rows kept their inline text, and a NULL stays NULL and an empty string stays empty. */
            Assert.Equal(3L, await CountAsync(connection, "SELECT COUNT(*) FROM v_query_stats"));
            Assert.Equal("SELECT 1", await ScalarTextAsync(connection, "SELECT query_text FROM v_query_stats WHERE collection_id = 1"));
            Assert.Null(await ScalarTextAsync(connection, "SELECT query_text FROM v_query_stats WHERE collection_id = 2"));
            Assert.Equal("", await ScalarTextAsync(connection, "SELECT query_text FROM v_query_stats WHERE collection_id = 3"));
            Assert.Equal(LongText, await ScalarTextAsync(connection, "SELECT query_text FROM v_query_store_stats WHERE collection_id = 1"));
        }

        /* A fresh store, built by the same code. */
        var freshPath = Path.Combine(_tempDir, "fresh", "fresh.duckdb");
        Directory.CreateDirectory(Path.GetDirectoryName(freshPath)!);
        var fresh = new DuckDbInitializer(freshPath);
        _initializers.Add(fresh);
        await fresh.InitializeAsync();

        using var upgradedConnection = await OpenAsync();
        using var freshConnection = await OpenAsync(freshPath);
        foreach (var relation in new[] { "query_stats", "query_store_stats", "v_query_stats", "v_query_store_stats", "vh_query_stats", "vh_query_store_stats", "plan_correction", "v_plan_correction", "vh_plan_correction" })
        {
            Assert.Equal(await ColumnsAsync(freshConnection, relation), await ColumnsAsync(upgradedConnection, relation));
        }

        /* The digest is a column of the tables, and of no view: a reader sees the columns it always saw. */
        Assert.Contains("query_text_digest BLOB", await ColumnsAsync(freshConnection, "query_stats"));
        Assert.DoesNotContain(await ColumnsAsync(freshConnection, "v_query_stats"), c => c.StartsWith("query_text_digest", StringComparison.Ordinal));
        Assert.Contains("query_text VARCHAR", await ColumnsAsync(freshConnection, "v_query_stats"));
        Assert.Contains("query_text VARCHAR", await ColumnsAsync(freshConnection, "v_query_store_stats"));
        _ = upgraded;
    }

    private async Task<string?> ScalarTextAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value is null or DBNull ? null : (string)value;
    }

    /// <summary>
    /// A hot row that carries a digest and NULL inline text reads the dimension's text through <c>v_</c>, for both tables. A NULL text
    /// with no digest is NULL, an empty string is empty, inline text wins over the dimension, and a text over 4 KB and a non-ASCII
    /// one come back exactly. The row count is the table's: a LEFT JOIN against one dimension row per digest adds and drops nothing.
    /// </summary>
    [Fact]
    public async Task TheViewReadsTheDimensionText_AndKeepsNullEmptyLongAndNonAsciiExact()
    {
        await InitAsync();
        using var connection = await OpenAsync();

        await AddToDimensionAsync(connection, "SELECT 1 FROM dimension_only");
        await AddToDimensionAsync(connection, LongText);
        await AddToDimensionAsync(connection, NonAsciiText);
        await AddToDimensionAsync(connection, "dimension text the inline text must beat");

        await InsertQueryStatsAsync(connection, 1, null, "SELECT 1 FROM dimension_only");
        await InsertQueryStatsAsync(connection, 2, null, LongText);
        await InsertQueryStatsAsync(connection, 3, null, NonAsciiText);
        await InsertQueryStatsAsync(connection, 4, null, null);
        await InsertQueryStatsAsync(connection, 5, "", null);
        await InsertQueryStatsAsync(connection, 6, "inline text", "dimension text the inline text must beat");
        /* A digest with no dimension row stays NULL rather than failing or dropping the row. */
        await InsertQueryStatsAsync(connection, 7, null, "never added to the dimension");
        await InsertQueryStatsAsync(connection, 8, null, "SELECT 1 FROM dimension_only");

        await InsertQueryStoreStatsAsync(connection, 1, null, LongText);
        await InsertQueryStoreStatsAsync(connection, 2, null, NonAsciiText);
        await InsertQueryStoreStatsAsync(connection, 3, null, null);
        await InsertQueryStoreStatsAsync(connection, 4, "", null);

        Assert.Equal(8L, await CountAsync(connection, "SELECT COUNT(*) FROM v_query_stats"));
        Assert.Equal(4L, await CountAsync(connection, "SELECT COUNT(*) FROM v_query_store_stats"));

        async Task<string?> Stats(long id) => await ScalarTextAsync(connection, $"SELECT query_text FROM v_query_stats WHERE collection_id = {id}");
        Assert.Equal("SELECT 1 FROM dimension_only", await Stats(1));
        Assert.Equal(LongText, await Stats(2));
        Assert.Equal(NonAsciiText, await Stats(3));
        Assert.Null(await Stats(4));
        Assert.Equal("", await Stats(5));
        Assert.Equal("inline text", await Stats(6));
        Assert.Null(await Stats(7));
        Assert.Equal("SELECT 1 FROM dimension_only", await Stats(8));

        async Task<string?> Store(long id) => await ScalarTextAsync(connection, $"SELECT query_text FROM v_query_store_stats WHERE collection_id = {id}");
        Assert.Equal(LongText, await Store(1));
        Assert.Equal(NonAsciiText, await Store(2));
        Assert.Null(await Store(3));
        Assert.Equal("", await Store(4));
    }

    /// <summary>
    /// <c>ArchiveReadPlan.Apply</c> and <c>SourceFor</c> read a hot diverted table through its <c>vh_</c> view: run against the store,
    /// the statement returns the dimension's text where the bare table would return NULL, and a table with no diverted text is
    /// still read as the bare table.
    /// </summary>
    [Fact]
    public async Task AHotPlan_ReadsADivertedTableThroughTheTextView()
    {
        var duckDb = await InitAsync();
        using var connection = await OpenAsync();
        await AddToDimensionAsync(connection, NonAsciiText);
        await InsertQueryStatsAsync(connection, 1, null, NonAsciiText);

        var plan = await duckDb.PlanArchiveReadAsync("query_stats", "collection_time", DateTime.UtcNow.AddDays(-1), TestContext.Current.CancellationToken);
        Assert.True(plan.IsHot("query_stats"), "no Parquet file exists, so the table reads as hot");

        Assert.Equal(NonAsciiText, await ScalarTextAsync(connection, plan.Apply("SELECT query_text FROM v_query_stats")));
        Assert.Equal(NonAsciiText, await ScalarTextAsync(connection, $"SELECT query_text FROM {plan.SourceFor("query_stats")}"));
        Assert.Null(await ScalarTextAsync(connection, "SELECT query_text FROM query_stats"));

        /* A table with no diverted text: the bare hot table, as before. */
        var waitPlan = await duckDb.PlanArchiveReadAsync("wait_stats", "collection_time", DateTime.UtcNow.AddDays(-1), TestContext.Current.CancellationToken);
        Assert.Equal("wait_stats", waitPlan.SourceFor("wait_stats"));
        Assert.Equal("wait_stats", waitPlan.Apply("v_wait_stats"));
    }

    /// <summary>
    /// The periodic archive pass writes the text into Parquet on every row for a hot row that carries only a digest, and the
    /// <c>v_</c> view reads it back from the file. An older build reads every archived file, because the file's shape is unchanged.
    /// </summary>
    [Fact]
    public async Task TheScheduledExport_WritesTheDimensionTextIntoParquet()
    {
        var duckDb = await InitAsync();
        using (var connection = await OpenAsync())
        {
            await AddToDimensionAsync(connection, LongText);
            await AddToDimensionAsync(connection, NonAsciiText);
            await InsertQueryStatsAsync(connection, 1, null, LongText);
            await InsertQueryStatsAsync(connection, 2, null, NonAsciiText);
            await InsertQueryStatsAsync(connection, 3, null, null);
            await InsertQueryStoreStatsAsync(connection, 1, null, NonAsciiText);
        }

        await new ArchiveService(duckDb, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        using var read = await OpenAsync();
        Assert.Equal(0L, await CountAsync(read, "SELECT COUNT(*) FROM query_stats"));

        var statsFile = Directory.GetFiles(_archiveDir, "*_query_stats.parquet").Single().Replace("\\", "/");
        Assert.Equal(3L, await CountAsync(read, $"SELECT COUNT(*) FROM read_parquet('{statsFile}')"));
        Assert.Equal(LongText, await ScalarTextAsync(read, $"SELECT query_text FROM read_parquet('{statsFile}') WHERE collection_id = 1"));
        Assert.Equal(NonAsciiText, await ScalarTextAsync(read, $"SELECT query_text FROM read_parquet('{statsFile}') WHERE collection_id = 2"));
        Assert.Null(await ScalarTextAsync(read, $"SELECT query_text FROM read_parquet('{statsFile}') WHERE collection_id = 3"));

        var storeFile = Directory.GetFiles(_archiveDir, "*_query_store_stats.parquet").Single().Replace("\\", "/");
        Assert.Equal(NonAsciiText, await ScalarTextAsync(read, $"SELECT query_text FROM read_parquet('{storeFile}')"));
    }

    /// <summary>The size-triggered reset exports the same way: the file carries the text, not the digest column's absence of it.</summary>
    [Fact]
    public async Task TheResetExport_WritesTheDimensionTextIntoParquet()
    {
        var duckDb = await InitAsync();
        using (var connection = await OpenAsync())
        {
            await AddToDimensionAsync(connection, NonAsciiText);
            await InsertQueryStatsAsync(connection, 1, null, NonAsciiText);
            await InsertQueryStoreStatsAsync(connection, 1, null, NonAsciiText);
        }

        await new ArchiveService(duckDb, _archiveDir).ArchiveAllAndResetAsync();

        using var read = await OpenAsync();
        var statsFile = Directory.GetFiles(_archiveDir, "*_query_stats.parquet").Single().Replace("\\", "/");
        Assert.Equal(NonAsciiText, await ScalarTextAsync(read, $"SELECT query_text FROM read_parquet('{statsFile}')"));
        var storeFile = Directory.GetFiles(_archiveDir, "*_query_store_stats.parquet").Single().Replace("\\", "/");
        Assert.Equal(NonAsciiText, await ScalarTextAsync(read, $"SELECT query_text FROM read_parquet('{storeFile}')"));
    }
}
