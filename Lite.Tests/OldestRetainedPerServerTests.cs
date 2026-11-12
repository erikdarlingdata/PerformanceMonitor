/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// #8305: the time range picker's "Data starts" note names the oldest row the store holds FOR THIS SERVER. The archive's span
/// is read over every server's rows, so a store whose archive holds months of another server's rows must not lend that start
/// to a server added today: its oldest retained row is its own first row, and a server with no row yet has none. The answer is
/// read once per server per archive generation, and read again when the archive changes. Its own store, not the shared fixture,
/// because it writes Parquet files and rebuilds the archive views.
/// </summary>
public sealed class OldestRetainedPerServerTests : IDisposable
{
    private const int ServerA = 83051;
    private const int ServerB = 83052;
    private const int ServerWithNoRow = 83053;

    private static readonly DateTime ArchivedA = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime HotA = new(2026, 6, 20, 8, 30, 0, DateTimeKind.Utc);
    private static readonly DateTime FirstB = new(2026, 6, 25, 9, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime LaterArchivedB = new(2026, 6, 15, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _initializer;

    public OldestRetainedPerServerTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

    public void Dispose()
    {
        _initializer?.Dispose();
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

    private static string Row(long id, int serverId, DateTime collectionTime) =>
        $"({id}, {serverId}, 'SERVER-{serverId}', 'wait_stats', TIMESTAMP '{collectionTime:yyyy-MM-dd HH:mm:ss}', 'SUCCESS')";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private string ParquetPath(string name) => Path.Combine(_tempDir, "archive", name).Replace("\\", "/");

    private const string Columns = "(log_id, server_id, server_name, collector_name, collection_time, status)";

    /// <summary>A store whose archive holds one row of server A from <see cref="ArchivedA"/>, and whose hot table holds a later row
    /// of A and the first row of server B, added after the archive was written.</summary>
    private async Task<DuckDbInitializer> StoreAsync()
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _initializer = new DuckDbInitializer(dbPath);
        await _initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, $"INSERT INTO collection_log {Columns} VALUES {Row(1, ServerA, ArchivedA)}");
            await ExecuteAsync(connection, $"COPY collection_log TO '{ParquetPath("20260601_0000_collection_log.parquet")}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM collection_log");
            await ExecuteAsync(connection, $"INSERT INTO collection_log {Columns} VALUES {Row(2, ServerA, HotA)}, {Row(3, ServerB, FirstB)}");
        }

        await _initializer.CreateArchiveViewsAsync();
        return _initializer;
    }

    [Fact]
    public async Task ANewServersOldestRetained_IsItsOwnFirstRow_NotAnotherServersArchivedRow()
    {
        var service = new LocalDataService(await StoreAsync());

        var oldestB = await service.GetOldestRetainedAsync(ServerB, TestContext.Current.CancellationToken);
        var oldestA = await service.GetOldestRetainedAsync(ServerA, TestContext.Current.CancellationToken);

        Assert.Equal(FirstB, oldestB);
        Assert.Equal(DateTimeKind.Utc, oldestB!.Value.Kind);
        Assert.Equal(ArchivedA, oldestA);
    }

    [Fact]
    public async Task AServerWithNoRowYet_HasNoOldestRetained_WhileTheArchiveHoldsOtherServersRows()
    {
        var service = new LocalDataService(await StoreAsync());

        var oldest = await service.GetOldestRetainedAsync(ServerWithNoRow, TestContext.Current.CancellationToken);

        Assert.Null(oldest);
    }

    [Fact]
    public async Task TheAnswerHoldsUntilTheArchiveChanges_ThenReadsTheServersArchivedRow()
    {
        var initializer = await StoreAsync();
        var service = new LocalDataService(initializer);
        Assert.Equal(FirstB, await service.GetOldestRetainedAsync(ServerB, TestContext.Current.CancellationToken));

        /* An archive pass moves an older row of B into a second file. Until the archive views are rebuilt the cached answer stands;
           the rebuild bumps the archive generation, and the next read finds the archived row. */
        using (var connection = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "test.duckdb")}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, $"INSERT INTO collection_log {Columns} VALUES {Row(4, ServerB, LaterArchivedB)}");
            await ExecuteAsync(connection, $"COPY (SELECT * FROM collection_log WHERE log_id = 4) TO '{ParquetPath("20260615_0000_collection_log.parquet")}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM collection_log WHERE log_id = 4");
        }

        Assert.Equal(FirstB, await service.GetOldestRetainedAsync(ServerB, TestContext.Current.CancellationToken));

        await initializer.CreateArchiveViewsAsync();

        Assert.Equal(LaterArchivedB, await service.GetOldestRetainedAsync(ServerB, TestContext.Current.CancellationToken));
        Assert.Equal(ArchivedA, await service.GetOldestRetainedAsync(ServerA, TestContext.Current.CancellationToken));
    }
}
