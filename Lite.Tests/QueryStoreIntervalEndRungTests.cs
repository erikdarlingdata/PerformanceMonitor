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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite v66 / Darling V155 (#4765): <c>query_store_stats</c> gains <c>interval_end_time_utc</c> — when the Query
/// Store interval a row belongs to ENDED, the counterpart of v49's <c>interval_start_time_utc</c>. A rate divides an
/// interval's totals by the interval's length; until now the only length available was the time since the previous
/// STORED interval, and Query Store stores no row for an interval with no executions, so an interval that follows
/// a quiet one divided by the gap plus its own length. This rung only stores the end; the reads change separately.
/// The column is TIMESTAMP, nullable, and the LAST payload column, because the DuckDB appender is positional and an
/// existing file can only get it from an <c>ALTER TABLE ... ADD COLUMN</c>, which appends.
///
/// <para>The collector's SQL and reader are <see cref="QueryStoreCollectorDefinitionTests"/>; the Darling side is
/// <c>Darling.Tests/QueryStoreIntervalEndRungTests</c>. What is here is the DuckDB ladder, the generator, and the
/// climb on a real DuckDB file: a v65 database with its <c>v_</c> view present over the narrower table, the shape
/// every existing Lite database is in the morning of the upgrade.</para>
/// </summary>
public sealed class QueryStoreIntervalEndRungTests : IDisposable
{
    private const string Column = "interval_end_time_utc";

    private readonly string _tempDir;

    public QueryStoreIntervalEndRungTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryStoreIntervalEndRungTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>The v66 block and its entry in the shared column list: stated against
    /// <c>CurrentSchemaVersion</c> as a floor so the next rung does not turn it red.</summary>
    [Fact]
    public void TheLiteMigration_AddsTheIntervalEnd_AtSchemaVersion66_ThroughTheSharedColumnList()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 66);

        var entry = Assert.Single(DuckDbInitializer.AddedColumnsForVersion(66));
        Assert.Equal(("query_store_stats", Column, "TIMESTAMP"), (entry.Table, entry.Column, entry.Type));
        Assert.Contains(entry, DuckDbInitializer.AddedColumns);

        var source = Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");
        var start = source.IndexOf("if (fromVersion < 66)", StringComparison.Ordinal);
        Assert.True(start >= 0, "RunMigrationsAsync has no v66 step");

        var block = source[start..Math.Min(source.Length, start + 4000)];
        Assert.Contains("AddMissingColumnsAsync(connection, AddedColumnsForVersion(66))", block, StringComparison.Ordinal);
        Assert.Contains("#4765", block, StringComparison.Ordinal);
    }

    /// <summary>The generated table ends with the interval end, after the tier-2 pair, and so does the frozen
    /// golden's entry — the tail-append that is its one legal edit. A fresh file and an upgraded one then share one
    /// physical column order.</summary>
    [Fact]
    public async Task AFreshFile_EndsQueryStoreStatsWithTheIntervalEnd_AfterTheTier2Pair()
    {
        var dbPath = Path.Combine(_tempDir, "lite-fresh.duckdb");
        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();

        var rows = await ColumnsAsync(conn);
        Assert.Equal(("interval_end_time_utc", "TIMESTAMP", "YES"), rows[^1]);
        Assert.Equal(("interval_start_time_utc", "TIMESTAMP", "YES"), rows[^2]);
        Assert.Equal("runtime_stats_interval_id", rows[^3].Name);

        /* The table's column list IS the collector's payload behind the four prefix columns, so the two cannot drift. */
        var payload = QueryStoreCollector.Instance.PayloadColumns.Select(c => c.Name).ToList();
        Assert.Equal(Column, payload[^1]);
        Assert.Equal(payload, rows.Skip(rows.Count - payload.Count).Select(r => r.Name).ToList());

        var golden = GoldenCollectorSchema.Tables["query_store_stats"].Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.EndsWith("    interval_start_time_utc TIMESTAMP,\n    interval_end_time_utc TIMESTAMP\n)", golden, StringComparison.Ordinal);
    }

    /// <summary>
    /// The upgrade itself, on a real DuckDB file: a database initialized at the current schema, the column
    /// dropped and its version stamped back to 65 — its <c>v_</c> view present over the narrower table — then
    /// re-initialized. The column comes back nullable at the tail, the pre-rung row reads NULL through the
    /// re-expanded view, a post-rung row lands with its end, and the version reads 66 or newer.
    /// </summary>
    [Fact]
    public async Task AV65Database_ClimbsToV66_AndItsOldRowsReadNull()
    {
        var dbPath = Path.Combine(_tempDir, "lite-v65.duckdb");

        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            /* DuckDB refuses to DROP a column a view or index depends on, so the dependents go first for the
               fixture's sake; the passthrough is then put BACK over the narrower table, because that is the state
               an existing database is in when the ALTER runs — the ADD COLUMN must succeed with the view present,
               and Lite's CreateArchiveViewsAsync re-expands it afterwards. */
            var dependents = await ListAsync(conn, "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%query_store_stats%'");
            Assert.Contains("v_query_store_stats", dependents);
            foreach (var dependent in dependents) await ExecAsync(conn, $"DROP VIEW IF EXISTS {dependent}");

            foreach (var index in await ListAsync(conn, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'query_store_stats'"))
                await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");

            await ExecAsync(conn, $"ALTER TABLE query_store_stats DROP COLUMN {Column}");
            await ExecAsync(conn, "CREATE VIEW v_query_store_stats AS SELECT * FROM query_store_stats");

            await ExecAsync(conn, "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, runtime_stats_interval_id, interval_start_time_utc) "
                + "VALUES (-1, TIMESTAMP '2026-07-02 12:00:00', 1, 'srv', 'db', 1, 1, 9001, TIMESTAMP '2026-07-02 10:00:00')");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (65)");

            Assert.Equal(0L, await CountAsync(conn, $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'query_store_stats' AND column_name = '{Column}'"));
        }

        using var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();
        await upgraded.CreateArchiveViewsAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 66);
            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(conn, "SELECT MAX(version) FROM schema_version"));
            Assert.Equal(1L, await CountAsync(conn, "SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'v_query_store_stats'"));

            var rows = await ColumnsAsync(conn);
            Assert.Equal(("interval_end_time_utc", "TIMESTAMP", "YES"), rows[^1]);
            Assert.Equal("interval_start_time_utc", rows[^2].Name);

            /* The pre-rung row reads NULL through the re-expanded view; a post-rung row lands with its end. */
            await ExecAsync(conn, "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc) "
                + "VALUES (-2, TIMESTAMP '2026-07-02 13:00:00', 1, 'srv', 'db', 1, 1, 9002, TIMESTAMP '2026-07-02 12:00:00', TIMESTAMP '2026-07-02 13:00:00')");

            Assert.Equal(DBNull.Value, await ScalarAsync(conn, $"SELECT {Column} FROM v_query_store_stats WHERE collection_id = -1"));
            Assert.Equal(new DateTime(2026, 7, 2, 13, 0, 0), await ScalarAsync(conn, $"SELECT {Column} FROM v_query_store_stats WHERE collection_id = -2"));
        }
    }

    private static async Task<List<(string Name, string Type, string Nullable)>> ColumnsAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'query_store_stats' ORDER BY ordinal_position";
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(string Name, string Type, string Nullable)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }

    private static async Task<List<string>> ListAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }

    private static async Task<long> CountAsync(DuckDBConnection conn, string sql) =>
        Convert.ToInt64(await ScalarAsync(conn, sql));
}
