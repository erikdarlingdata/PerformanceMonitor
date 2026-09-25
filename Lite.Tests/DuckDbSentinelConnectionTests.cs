/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4262: <see cref="DuckDbInitializer"/> now keeps one sentinel <see cref="DuckDBConnection"/> open on
/// <c>ConnectionString</c> for the app's life, so every other caller's transient connection attaches to
/// DuckDB.NET's cached native handle instead of paying full open/close cost. That handle is exactly why a
/// live sentinel turned <see cref="DuckDbInitializer.ResetDatabaseAsync"/> into a silent no-op before this
/// fix: deleting the file did not drop the handle a fresh connection would be handed, so the "reinitialized"
/// database kept serving the deleted file's rows. These tests seed that exact precondition — a live
/// sentinel — for every path that deletes, replaces or re-creates the database file, and assert the reset
/// is actually visible afterward.
/// </summary>
/* CollectionResetGate and ArchiveService's own static s_archiveLock are process-wide: a test here calling
   ArchiveAllAndResetAsync races any other test doing the same under xUnit's default cross-class
   parallelism (confirmed — this class flaked against MuteRulesSurviveResetTests before both were tagged
   into the same serialized collection CollectionResetGateTests already defines). */
[Collection("CollectionResetGate")]
public class DuckDbSentinelConnectionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public DuckDbSentinelConnectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
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

    /// <summary>
    /// G1's repro (issuecomment-5836854869): with the sentinel live, seed a row, reset, and a brand-new
    /// connection must see an empty table — not the deleted file's rows served back out of the cached
    /// handle. Fails on the pre-fix shape where <c>ResetDatabaseAsync</c> deletes the file but never closes
    /// the sentinel (confirmed by temporarily no-opping <c>ReleaseSentinel</c> and re-running this test).
    /// </summary>
    [Fact]
    public async Task ResetDatabaseAsync_WithLiveSentinel_NewConnectionSeesEmptyTable()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await SeedCollectionLogRowAsync(initializer, "TestCollector");

        using (var precondition = initializer.CreateConnection())
        {
            await precondition.OpenAsync();
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(precondition, "SELECT COUNT(*) FROM collection_log")));
        }

        await initializer.ResetDatabaseAsync();

        using var afterReset = initializer.CreateConnection();
        await afterReset.OpenAsync();
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(afterReset, "SELECT COUNT(*) FROM collection_log")));
    }

    /// <summary>
    /// The same hazard through the real production path (#4262 ruling item 2): the 512MB emergency reset
    /// runs <c>ArchiveService.ArchiveAllAndResetAsync</c> -&gt; <c>ResetDatabaseAsync</c> on every archive
    /// cycle, not just when a test calls <c>ResetDatabaseAsync</c> directly. A live sentinel must not leave
    /// archived rows sitting in the hot DuckDB table beside their parquet copies.
    /// </summary>
    [Fact]
    public async Task ArchiveAllAndResetAsync_WithLiveSentinel_ClearsHotTableThroughRealPath()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await SeedCollectionLogRowAsync(initializer, "ArchivedCollector");

        var archiveService = new ArchiveService(initializer, _archiveDir);
        await archiveService.ArchiveAllAndResetAsync();

        using var afterReset = initializer.CreateConnection();
        await afterReset.OpenAsync();
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(afterReset, "SELECT COUNT(*) FROM collection_log")));
    }

    /// <summary>
    /// A start with a schema migration pending (#2748's exact v47 precondition, reused here because it is
    /// already proven to migrate cleanly) must still run <c>RunMigrationsAsync</c> to completion, and the
    /// sentinel must open only after — never mid-migration, when the schema is not yet the caller-visible
    /// shape. Proof that a handle is actually live: <see cref="File.Delete(string)"/> alone does NOT
    /// conflict with DuckDB's own share-delete handle (confirmed empirically — the file unlinks
    /// immediately regardless), so this opens with <see cref="FileShare.None"/> instead, which Windows
    /// refuses whenever ANY other handle is open on the file, no matter what sharing that other handle
    /// requested.
    /// </summary>
    [Fact]
    public async Task InitializeAsync_WithPendingSchemaMigration_MigratesThenOpensSentinel()
    {
        using (var seed = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await seed.OpenAsync();
            await ExecAsync(seed, "CREATE TABLE schema_version (version INTEGER NOT NULL)");
            await ExecAsync(seed, "INSERT INTO schema_version VALUES (47)");
            await ExecAsync(seed, @"CREATE TABLE server_properties (
                server_id INTEGER NOT NULL,
                collection_time TIMESTAMP NOT NULL,
                cpu_count INTEGER NOT NULL,
                hyperthread_ratio INTEGER NOT NULL,
                physical_memory_mb BIGINT NOT NULL
            )");
            await ExecAsync(seed, "INSERT INTO server_properties VALUES (1, current_timestamp, 4, 1, 16384)");
            await ExecAsync(seed, "CREATE INDEX idx_server_properties_time ON server_properties(server_id, collection_time)");
        }

        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (var verify = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await verify.OpenAsync();
            Assert.Equal(
                (long)DuckDbInitializer.CurrentSchemaVersion,
                Convert.ToInt64(await ScalarAsync(verify, "SELECT MAX(version) FROM schema_version")));
        }

        Assert.Throws<IOException>(() => File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose());

        initializer.Dispose();
    }

    /// <summary>
    /// Dispose closes the sentinel (#4262): once the app is shutting down, nothing should still be
    /// pinning the file open, so an exclusive open — refused above while the sentinel was live — must now
    /// succeed, and a plain delete (the same operation any uninstall/reset path would need) must too.
    /// </summary>
    [Fact]
    public async Task Dispose_ClosesSentinel_FileDeletableAfterward()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        Assert.Throws<IOException>(() => File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose());

        initializer.Dispose();

        File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None).Dispose();

        var ex = Record.Exception(() => File.Delete(_dbPath));
        Assert.Null(ex);
        Assert.False(File.Exists(_dbPath));
    }

    private static async Task SeedCollectionLogRowAsync(DuckDbInitializer initializer, string collector)
    {
        using var readLock = initializer.AcquireReadLock();
        using var connection = initializer.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
