using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests that DuckDbInitializer creates all expected tables and indexes.
/// No SQL Server required — these run purely against a temp DuckDB file.
/// </summary>
public class DuckDbSchemaTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public DuckDbSchemaTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
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

    [Fact]
    public async Task InitializeAsync_CreatesAllTables()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var expectedTables = new[]
        {
            "schema_version",
            "servers",
            "collection_schedule",
            "collection_log",
            "wait_stats",
            "query_stats",
            "cpu_utilization_stats",
            "file_io_stats",
            "memory_stats",
            "memory_clerks",
            "deadlocks",
            "procedure_stats",
            "query_store_stats",
            "query_snapshots",
            "tempdb_stats",
            "perfmon_stats",
            "server_config",
            "database_config",
            "memory_grant_stats",
            "waiting_tasks",
            "blocked_process_reports",
            "database_scoped_config",
            "trace_flags",
            "running_jobs",
            "config_alert_log"
        };

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        foreach (var table in expectedTables)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'";
            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            Assert.True(count == 1, $"Table '{table}' should exist but was not found");
        }
    }

    [Fact]
    public async Task InitializeAsync_SetsCorrectSchemaVersion()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT MAX(version) FROM schema_version";
        var version = Convert.ToInt32(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion, version);
    }

    [Fact]
    public async Task InitializeAsync_IsIdempotent()
    {
        var initializer = new DuckDbInitializer(_dbPath);

        /* Run twice — should not throw */
        await initializer.InitializeAsync();
        await initializer.InitializeAsync();

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT MAX(version) FROM schema_version";
        var version = Convert.ToInt32(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion, version);
    }

    [Fact]
    public async Task InitializeAsync_CreatesArchiveDirectory()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var archivePath = Path.Combine(_tempDir, "archive");
        Assert.True(Directory.Exists(archivePath), "Archive directory should be created");
    }

    [Fact]
    public void SchemaStatements_MatchTableCount()
    {
        /* Verify GetAllTableStatements returns the expected number of tables */
        var tableCount = 0;
        foreach (var _ in Schema.GetAllTableStatements())
            tableCount++;

        /* 24 tables from Schema (schema_version is created separately by DuckDbInitializer) */
        Assert.Equal(24, tableCount);
    }

    [Fact]
    public async Task InitializeAsync_CreatesIndexes()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        /* Verify at least some indexes exist by checking duckdb_indexes */
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM duckdb_indexes()";
        var indexCount = Convert.ToInt32(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        /* We create 18 indexes */
        Assert.True(indexCount >= 18, $"Expected >= 18 indexes, found {indexCount}");
    }
}
