using System;
using System.IO;
using System.Text.RegularExpressions;
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
            "config_alert_log",
            "config_mute_rules"
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

        /* 28 tables from Schema (schema_version is created separately by DuckDbInitializer) */
        Assert.Equal(28, tableCount);
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

    /// <summary>
    /// DuckDB does not support NOT NULL on ALTER TABLE ADD COLUMN.
    /// This test scans the migration source code to prevent regressions,
    /// including multi-line statements where ADD COLUMN and NOT NULL
    /// appear on different lines within the same SQL statement.
    /// </summary>
    [Fact]
    public void Migrations_DoNotUseNotNullOnAlterTableAddColumn()
    {
        var sourceFile = FindSourceFile("DuckDbInitializer.cs");
        Assert.True(sourceFile != null, "Could not find DuckDbInitializer.cs in the Lite project tree");

        var content = File.ReadAllText(sourceFile!);

        // Strip line comments (// ...) and block comments (/* ... */)
        var stripped = Regex.Replace(content, @"//[^\r\n]*", " ");
        stripped = Regex.Replace(stripped, @"/\*.*?\*/", " ", RegexOptions.Singleline);

        // Match ADD COLUMN ... NOT NULL within the same SQL statement (up to the next semicolon).
        // RegexOptions.IgnoreCase + Singleline so . matches newlines.
        var pattern = @"ADD\s+COLUMN\b[^;]*?\bNOT\s+NULL\b";
        var matches = Regex.Matches(stripped, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);

        var violations = new System.Collections.Generic.List<string>();
        foreach (Match m in matches)
        {
            // Find the line number in the original content for a useful error message
            int lineNum = content[..m.Index].Split('\n').Length;
            var snippet = m.Value.Replace("\r", "").Replace("\n", " ");
            if (snippet.Length > 120) snippet = snippet[..120] + "...";
            violations.Add($"Line ~{lineNum}: {snippet}");
        }

        Assert.True(violations.Count == 0,
            "DuckDB does not support NOT NULL on ALTER TABLE ADD COLUMN. " +
            "Use a nullable column with DEFAULT instead.\n\nViolations:\n" +
            string.Join("\n", violations));
    }

    private static string? FindSourceFile(string fileName)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8; i++)
        {
            var candidate = Path.Combine(dir, "Lite", "Database", fileName);
            if (File.Exists(candidate)) return candidate;
            var parent = Directory.GetParent(dir);
            if (parent == null) break;
            dir = parent.FullName;
        }
        return null;
    }
}
