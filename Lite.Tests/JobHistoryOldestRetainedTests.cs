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
/// #8342: the oldest job history the store holds, for the Job History tab's picker note, at the scope the tab shows: one server's
/// own oldest row, archived or hot, or the fleet's at All Servers. A server with no row has none, whatever the archive holds for
/// the others. The answer is read once per scope per archive generation, and read again when the archive changes. Its own store,
/// not the shared fixture, because it writes Parquet files and rebuilds the archive views.
/// </summary>
public sealed class JobHistoryOldestRetainedTests : IDisposable
{
    private const int ServerA = 83424;
    private const int ServerB = 83425;
    private const int ServerWithNoRow = 83426;

    private static readonly DateTime ArchivedA = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime HotA = new(2026, 6, 20, 8, 30, 0, DateTimeKind.Utc);
    private static readonly DateTime FirstB = new(2026, 6, 25, 9, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime LaterArchivedB = new(2026, 6, 15, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _initializer;

    public JobHistoryOldestRetainedTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

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

    private const string Columns = "(job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, run_status, run_duration_seconds, retries_attempted, run_datetime)";

    private static string Run(long id, int serverId, DateTime collectionTime) =>
        $"({id}, TIMESTAMP '{collectionTime:yyyy-MM-dd HH:mm:ss}', {serverId}, 'SERVER-{serverId}', {id}, 'job-{serverId}', 'Nightly', true, 0, 1, 5, 0, TIMESTAMP '{collectionTime:yyyy-MM-dd HH:mm:ss}')";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private string ParquetPath(string name) => Path.Combine(_tempDir, "archive", name).Replace("\\", "/");

    /// <summary>A store whose archive holds one run of server A from <see cref="ArchivedA"/>, and whose hot table holds a later run
    /// of A and the first run of server B, added after the archive was written.</summary>
    private async Task<DuckDbInitializer> StoreAsync()
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _initializer = new DuckDbInitializer(dbPath);
        await _initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, $"INSERT INTO job_history {Columns} VALUES {Run(1, ServerA, ArchivedA)}");
            await ExecuteAsync(connection, $"COPY job_history TO '{ParquetPath("20260601_0000_job_history.parquet")}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM job_history");
            await ExecuteAsync(connection, $"INSERT INTO job_history {Columns} VALUES {Run(2, ServerA, HotA)}, {Run(3, ServerB, FirstB)}");
        }

        await _initializer.CreateArchiveViewsAsync();
        return _initializer;
    }

    [Fact]
    public async Task OneServersOldest_IsItsOwnFirstRun_AndAllServersIsTheFleetsArchivedOne()
    {
        var service = new LocalDataService(await StoreAsync());

        var oldestB = await service.GetOldestJobHistoryAsync(ServerB, TestContext.Current.CancellationToken);
        var oldestA = await service.GetOldestJobHistoryAsync(ServerA, TestContext.Current.CancellationToken);
        var fleet = await service.GetOldestJobHistoryAsync(null, TestContext.Current.CancellationToken);

        Assert.Equal(FirstB, oldestB);
        Assert.Equal(DateTimeKind.Utc, oldestB!.Value.Kind);
        Assert.Equal(ArchivedA, oldestA);
        Assert.Equal(ArchivedA, fleet);
    }

    [Fact]
    public async Task AServerWithNoRunYet_HasNoOldest_WhileTheArchiveHoldsOtherServersRuns()
    {
        var service = new LocalDataService(await StoreAsync());

        Assert.Null(await service.GetOldestJobHistoryAsync(ServerWithNoRow, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TheAnswerHoldsUntilTheArchiveChanges_ThenReadsTheServersArchivedRun()
    {
        var initializer = await StoreAsync();
        var service = new LocalDataService(initializer);
        Assert.Equal(FirstB, await service.GetOldestJobHistoryAsync(ServerB, TestContext.Current.CancellationToken));

        /* An archive pass moves an older run of B into a second file. Until the archive views are rebuilt the cached answer stands;
           the rebuild bumps the archive generation, and the next read finds the archived run. */
        using (var connection = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "test.duckdb")}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, $"INSERT INTO job_history {Columns} VALUES {Run(4, ServerB, LaterArchivedB)}");
            await ExecuteAsync(connection, $"COPY (SELECT * FROM job_history WHERE job_history_id = 4) TO '{ParquetPath("20260615_0000_job_history.parquet")}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM job_history WHERE job_history_id = 4");
        }

        Assert.Equal(FirstB, await service.GetOldestJobHistoryAsync(ServerB, TestContext.Current.CancellationToken));

        await initializer.CreateArchiveViewsAsync();

        Assert.Equal(LaterArchivedB, await service.GetOldestJobHistoryAsync(ServerB, TestContext.Current.CancellationToken));
        Assert.Equal(ArchivedA, await service.GetOldestJobHistoryAsync(null, TestContext.Current.CancellationToken));
    }
}
