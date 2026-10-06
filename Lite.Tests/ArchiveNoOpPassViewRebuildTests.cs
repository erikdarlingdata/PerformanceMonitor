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
/// #5377: every archive-view rebuild bumps the archive generation and throws away every cached watermark. The
/// hourly archive pass rebuilt the views in its <c>finally</c> even when it archived nothing and compaction changed
/// nothing, so each pass cost the next read of each table its watermark. The rebuild now runs only when the
/// archive's files changed in the pass, and still runs when a pass changed files and then failed.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveNoOpPassViewRebuildTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-archive-noop-tests-").FullName;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private DuckDbInitializer? _duckDb;

    public ArchiveNoOpPassViewRebuildTests()
    {
        _dbPath = Path.Combine(_dir, "pm.duckdb");
        _archiveDir = Path.Combine(_dir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private async Task<ArchiveService> OpenAsync()
    {
        _duckDb = new DuckDbInitializer(_dbPath);
        await _duckDb.InitializeAsync();
        return new ArchiveService(_duckDb, _archiveDir) { TimestampForTests = "20260901_0000" };
    }

    private async Task AddOldWaitStatsRowAsync()
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type)
VALUES (1, now() - INTERVAL 30 DAY, 1, 'S1', 'SOS_SCHEDULER_YIELD')";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task APassThatArchivesNothing_LeavesTheGenerationAlone()
    {
        var service = await OpenAsync();
        var before = _duckDb!.ArchiveViewGeneration;

        await service.ArchiveOldDataAsync();
        await service.ArchiveOldDataAsync();

        Assert.Equal(before, _duckDb.ArchiveViewGeneration);
        Assert.Empty(Directory.GetFiles(_archiveDir, "*.parquet"));
    }

    [Fact]
    public async Task APassThatArchivesATable_BumpsTheGeneration()
    {
        var service = await OpenAsync();
        await AddOldWaitStatsRowAsync();
        var before = _duckDb!.ArchiveViewGeneration;

        await service.ArchiveOldDataAsync();

        Assert.NotEmpty(Directory.GetFiles(_archiveDir, "*.parquet"));
        Assert.True(_duckDb.ArchiveViewGeneration > before, "an archived table did not rebuild the views");
    }

    [Fact]
    public async Task APassThatPromotesAFileAndThenFails_StillRebuildsTheViews()
    {
        var service = await OpenAsync();
        await AddOldWaitStatsRowAsync();

        /* The seam fires after the file is promoted and before its rows are deleted, and a plain exception (not a
           simulated kill) is what a failure there looks like: the per-table catch logs it and the pass goes on. */
        service.AfterPromoteForTests = table =>
        {
            if (table == "wait_stats")
            {
                throw new InvalidOperationException("promote-then-fail");
            }
        };
        var rebuilds = 0;
        _duckDb!.OnArchiveViewRebuildForTests = () => rebuilds++;
        var before = _duckDb.ArchiveViewGeneration;

        await service.ArchiveOldDataAsync();

        Assert.NotEmpty(Directory.GetFiles(_archiveDir, "*.parquet"));
        Assert.True(rebuilds > 0, "a pass that left a promoted file behind did not rebuild the views");
        Assert.True(_duckDb.ArchiveViewGeneration > before);
    }
}
