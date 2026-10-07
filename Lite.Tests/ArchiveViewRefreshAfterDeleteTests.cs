/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// An archive view reads a table's files through globs baked in when the view is built, and DuckDB fails the
/// whole read at bind when one of them matches nothing. Deleting the last file behind a glob therefore broke
/// every read of that table's <c>v_</c> view until the next refresh, up to an hour later. Every place that
/// deletes archive files now rebuilds the views straight after.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveViewRefreshAfterDeleteTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveViewRefreshAfterDeleteTests()
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

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<long> ScalarAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* Writes rows [from, to) of collection_log to an archive file with the table's real schema, then clears them
       from the hot table, the way archival does. */
    private async Task ArchiveRowsAsync(string fileName, int from, int to)
    {
        await ExecAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range({from}, {to}) t(i)");
        var path = Path.Combine(_archiveDir, fileName).Replace("\\", "/");
        await ExecAsync($"COPY collection_log TO '{path}' (FORMAT PARQUET)");
        await ExecAsync("DELETE FROM collection_log");
    }

    [Fact]
    public async Task RetentionDeletingTheLastPartFile_LeavesTheViewReadable()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        var thisMonth = DateTime.UtcNow.ToString("yyyyMM");

        await ArchiveRowsAsync("200001_collection_log_pt001.parquet", 0, 40);
        await ArchiveRowsAsync($"{thisMonth}_collection_log.parquet", 40, 50);
        await initializer.CreateArchiveViewsAsync();
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));

        var deleted = await new RetentionService(_archiveDir).CleanupOldArchivesAndRefreshViewsAsync(initializer);

        Assert.Equal(1, deleted);
        Assert.Equal(10, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
    }

    [Fact]
    public async Task RetentionDeletingTheLastPlainFile_WhilePartFilesRemain_LeavesTheViewReadable()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        var thisMonth = DateTime.UtcNow.ToString("yyyyMM");

        await ArchiveRowsAsync("200001_collection_log.parquet", 0, 40);
        await ArchiveRowsAsync($"{thisMonth}_collection_log_pt001.parquet", 40, 50);
        await initializer.CreateArchiveViewsAsync();
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));

        var deleted = await new RetentionService(_archiveDir).CleanupOldArchivesAndRefreshViewsAsync(initializer);

        Assert.Equal(1, deleted);
        Assert.Equal(10, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
    }

    /// <summary>
    /// The two tests above drive <see cref="RetentionService"/> directly. This one drives the background
    /// service's own retention step, the code that runs once a day in the app: a step that goes back to the
    /// plain delete leaves the view holding a glob for the file it just removed, and every read of that view
    /// fails until the next archival refresh (#4720). The service is built without a collector because the
    /// retention step never touches one, and the step is due once its daily interval has passed.
    /// </summary>
    [Fact]
    public async Task TheBackgroundServicesRetentionStep_RebuildsTheViewsAfterItDeletesAFile()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        var thisMonth = DateTime.UtcNow.ToString("yyyyMM");

        await ArchiveRowsAsync("200001_collection_log_pt001.parquet", 0, 40);
        await ArchiveRowsAsync($"{thisMonth}_collection_log.parquet", 40, 50);
        await initializer.CreateArchiveViewsAsync();
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));

        var service = new CollectionBackgroundService(null!, initializer, retentionService: new RetentionService(_archiveDir));
        const BindingFlags nonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        typeof(CollectionBackgroundService).GetField("_lastRetentionTime", nonPublic)!
            .SetValue(service, DateTime.UtcNow.AddDays(-2));
        var step = typeof(CollectionBackgroundService).GetMethod("RunRetentionIfDueAsync", nonPublic)!;

        await (Task)step.Invoke(service, null)!;

        Assert.False(File.Exists(Path.Combine(_archiveDir, "200001_collection_log_pt001.parquet")));
        Assert.Equal(10, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
    }

    /// <summary>
    /// The files a killed size-triggered reset promoted are removed at the start of the next run. The views
    /// built at startup already hold a glob for them, and a reset that then fails to export never rebuilds the
    /// views, so the removal has to.
    /// </summary>
    [Fact]
    public async Task RemovingFilesLeftByAKilledReset_LeavesTheViewReadable_EvenWhenTheNextResetIsAbandoned()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ArchiveRowsAsync("20260901_0000_collection_log.parquet", 0, 40);
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(100, 105) t(i)");
        await initializer.CreateArchiveViewsAsync();
        File.WriteAllLines(Path.Combine(_archiveDir, PreservedTableRestore.ResetExportMarkerFileName), ["20260901_0000_collection_log.parquet"]);

        var service = new ArchiveService(initializer, _archiveDir);
        service.BeforeTableExportForTests = _ => throw new IOException("There is not enough space on the disk.");
        await service.ArchiveAllAndResetAsync();

        Assert.False(File.Exists(Path.Combine(_archiveDir, "20260901_0000_collection_log.parquet")));
        Assert.Equal(5, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
    }
}
