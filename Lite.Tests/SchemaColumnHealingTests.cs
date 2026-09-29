using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4727 item 2: the columns that schema versions 60 to 65 add are re-applied on every start of an existing
/// data file, so a file where one failed to apply (or that was stamped without it) gets it back instead of
/// failing every batch for that table. The stamp is not held back: the heal does not depend on it.
/// </summary>
public sealed class SchemaColumnHealingTests : IDisposable
{
    /// <summary>The 15 columns versions 60 to 65 add, spelled out here on purpose so a trimmed production list fails.</summary>
    private static readonly (string Table, string Column, string Type)[] NewerColumns =
    {
        ("wait_stats", "sample_interval_seconds", "INTEGER"),
        ("file_io_stats", "sample_interval_seconds", "INTEGER"),
        ("latch_stats", "sample_interval_seconds", "INTEGER"),
        ("spinlock_stats", "sample_interval_seconds", "INTEGER"),
        ("procedure_stats", "sample_interval_seconds", "INTEGER"),
        ("memory_grant_stats", "sample_interval_seconds", "INTEGER"),
        ("query_stats", "statement_start_offset", "INTEGER"),
        ("query_stats", "statement_end_offset", "INTEGER"),
        ("perfmon_stats", "cntr_type", "INTEGER"),
        ("cpu_utilization_stats", "sample_time_utc", "TIMESTAMP"),
        ("server_properties", "time_zone_id", "VARCHAR"),
        ("query_store_health", "query_capture_mode", "VARCHAR"),
        ("query_store_health", "wait_stats_capture_mode", "VARCHAR"),
        ("ag_replica_states", "group_id", "VARCHAR"),
        ("ag_database_replica_states", "group_id", "VARCHAR"),
    };

    private readonly string _tempDir;
    private readonly string _dbPath;

    public SchemaColumnHealingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "SchemaColumnHealingTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "lite.duckdb");
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AFileStampedCurrent_ButMissingTheNewerColumns_GetsEveryOneBackOnTheNextStart()
    {
        using (var first = new DuckDbInitializer(_dbPath))
        {
            await first.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();

            /* DuckDB refuses DROP COLUMN on a table that has an index (a Dependency Error); a view over the table
               does not block it. So only the indexes go, and they are put straight back with the columns gone:
               the file then looks the way a real one does when a column add failed, with its indexes and its v_
               views all standing and the stamp at the current version. */
            foreach (var table in NewerColumns.Select(c => c.Table).Distinct())
            {
                foreach (var index in await ListAsync(conn, $"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table}'"))
                    await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
                foreach (var (_, column, _) in NewerColumns.Where(c => c.Table == table))
                    await ExecAsync(conn, $"ALTER TABLE {table} DROP COLUMN {column}");
            }

            foreach (var statement in Schema.GetAllIndexStatements())
                await ExecAsync(conn, statement);

            /* The arrange step holds: every column gone, every index and view back, the stamp current. */
            foreach (var (table, column, _) in NewerColumns)
            {
                Assert.Equal(0L, await CountAsync(conn, $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '{column}'"));
                Assert.True(await CountAsync(conn, $"SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = '{table}'") >= 1, $"index on {table}");
                Assert.Equal(1L, await CountAsync(conn, $"SELECT COUNT(*) FROM duckdb_views() WHERE NOT internal AND view_name = 'v_{table}'"));
            }

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(conn, "SELECT MAX(version) FROM schema_version"));
        }

        using (var second = new DuckDbInitializer(_dbPath))
        {
            await second.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();
            foreach (var (table, column, type) in NewerColumns)
            {
                var types = await ListAsync(conn, $"SELECT data_type FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '{column}'");
                Assert.True(types.Count == 1, $"{table}.{column} is back");
                Assert.Equal(type, types[0]);

                /* The passthrough view reads the restored column too (Lite rebuilds every v_ view on start). */
                Assert.Contains(column, await ListAsync(conn, $"SELECT column_name FROM (DESCRIBE SELECT * FROM v_{table})"));
            }

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(conn, "SELECT MAX(version) FROM schema_version"));
        }
    }

    /// <summary>
    /// A column add that still fails logs ONE Error naming the table and column, and the loop moves on to the
    /// next entry, so the start continues and the next start retries it. A type DuckDB does not know makes the
    /// ALTER itself fail on a table that exists.
    /// </summary>
    [Fact]
    public async Task AColumnAddThatFails_LogsOneErrorNamingTheColumn_AndTheNextEntryIsStillAdded()
    {
        var log = new CapturingLogger();
        using var initializer = new DuckDbInitializer(_dbPath, log);
        await initializer.InitializeAsync();
        log.Entries.Clear();

        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();

        await initializer.AddMissingColumnsAsync(conn, new[]
        {
            (0, "wait_stats", "broken_column", "NOT_A_TYPE"),
            (0, "wait_stats", "healthy_column", "INTEGER"),
        });

        var error = Assert.Single(log.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains("wait_stats.broken_column", error.Message, StringComparison.Ordinal);
        Assert.Equal(0L, await CountAsync(conn, "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'wait_stats' AND column_name = 'broken_column'"));
        Assert.Equal(1L, await CountAsync(conn, "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'wait_stats' AND column_name = 'healthy_column'"));
    }

    /// <summary>
    /// A file older than a table has no such table when a migration step runs (the table statements after the
    /// steps create it, with the column). That is not a failure and must not read as one in the log.
    /// </summary>
    [Fact]
    public async Task ATableThatDoesNotExistYet_IsSkippedWithoutAWarningOrAnError()
    {
        var log = new CapturingLogger();
        using var initializer = new DuckDbInitializer(_dbPath, log);
        await initializer.InitializeAsync();
        log.Entries.Clear();

        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();

        await initializer.AddMissingColumnsAsync(conn, new[] { (0, "table_created_later", "some_column", "INTEGER") });

        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Warning);
    }

    private sealed class CapturingLogger : ILogger<DuckDbInitializer>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<List<string>> ListAsync(DuckDBConnection conn, string sql)
    {
        var rows = new List<string>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetValue(0)?.ToString() ?? "");
        return rows;
    }
}
