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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: a read whose window starts after the newest archived row must not open an archive file. The Queries tab's top-N
/// read and the data-start floor's window query both read <c>v_query_stats</c>, which is the hot table plus every daily
/// Parquet file, however short the window. Each test caches the archive span with one real read, then makes the archive file
/// unreadable: a read that still opens it throws.
/// </summary>
public sealed class ArchiveWindowReadScalingTests : IDisposable
{
    private const int ServerId = 7256;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private long _nextId = 1;

    public ArchiveWindowReadScalingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    private static DateTime Whole(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task ExecuteAsync(params string[] statements)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private string Row(DateTime collected, string hash, long cpu) =>
        $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, max_dop, query_text)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', '{hash}', '{hash}', {Ts(collected)}, 10, {cpu}, {cpu}, 1, 'SELECT {hash}')";

    /// <summary>Moves every hot row older than <paramref name="before"/> into a Parquet file and rebuilds the views, which bumps
    /// the archive view generation. Returns the file's path.</summary>
    private async Task<string> ArchiveAsync(DateTime before)
    {
        var path = Path.Combine(_archiveDir, "20260101_000000_query_stats.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM query_stats WHERE collection_time < {Ts(before)}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM query_stats WHERE collection_time < {Ts(before)}");
        await _duckDb.CreateArchiveViewsAsync();
        return path;
    }

    [Fact]
    public async Task TopQueriesByCpu_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(
            Row(Whole(NowUtc.AddDays(-10)), "archived", 999),
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100));
        var archiveFile = await ArchiveAsync(before: NowUtc.AddDays(-1));

        var first = await _service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 6);
        Assert.Equal("recent", Assert.Single(first).QueryHash);

        /* The archive file can no longer be read. A read that still opens it throws. */
        File.WriteAllText(archiveFile, "not a parquet file");

        var second = await _service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 6);
        Assert.Equal("recent", Assert.Single(second).QueryHash);
    }

    [Fact]
    public async Task TopQueriesByCpu_AWindowThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        await ExecuteAsync(
            Row(Whole(NowUtc.AddDays(-10)), "archived", 999),
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100));
        await ArchiveAsync(before: NowUtc.AddDays(-1));

        var rows = await _service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24 * 12);

        Assert.Equal(["archived", "recent"], rows.Select(r => r.QueryHash).OrderBy(h => h, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task TheDataStartFloor_AWindowNewerThanTheArchive_OpensNoArchiveFileForItsWindowQuery()
    {
        /* An older row of the server sits in the hot table too, so the older-row probe stops there and the window query is the
           only read that could reach the archive. */
        var start = Whole(NowUtc.AddHours(-6));
        var firstInWindow = Whole(start.AddHours(1));
        await ExecuteAsync(
            Row(Whole(NowUtc.AddDays(-10)), "archived", 999),
            Row(Whole(start.AddHours(-1)), "olderHot", 50),
            Row(firstInWindow, "recent", 100));
        var archiveFile = await ArchiveAsync(before: NowUtc.AddDays(-1));

        var first = await _service.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, start, Whole(NowUtc));
        Assert.Equal(start, first);

        File.WriteAllText(archiveFile, "not a parquet file");

        var second = await _service.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, start, Whole(NowUtc));
        Assert.Equal(start, second);
    }

    [Fact]
    public async Task TheDataStartFloor_AWindowThatReachesIntoTheArchive_StillFindsTheArchivedRow()
    {
        /* The only row inside the window is archived: the window query must read the archive to find it. */
        var start = Whole(NowUtc.AddDays(-12));
        var archivedInWindow = Whole(NowUtc.AddDays(-10));
        await ExecuteAsync(
            Row(archivedInWindow, "archived", 999),
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100));
        await ArchiveAsync(before: NowUtc.AddDays(-1));

        var floor = await _service.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, start, Whole(NowUtc.AddDays(-9)));

        Assert.Equal(archivedInWindow, floor);
    }

    private string SnapshotRow(DateTime collected, string hash) =>
        $@"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     wait_type, cpu_time_ms, total_elapsed_time_ms, query_hash)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 55, 'Db', 'SELECT {hash}', 'running', 'LCK_M_X', 10, 20, '{hash}')";

    /// <summary>Moves every hot row of <paramref name="table"/> older than <paramref name="before"/> into a Parquet file and rebuilds
    /// the views. Returns the file's path.</summary>
    private async Task<string> ArchiveTableAsync(string table, DateTime before)
    {
        var path = Path.Combine(_archiveDir, $"20260101_000000_{table}.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM {table} WHERE collection_time < {Ts(before)}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM {table} WHERE collection_time < {Ts(before)}");
        await _duckDb.CreateArchiveViewsAsync();
        return path;
    }

    /// <summary>Every windowed read of query_stats and query_snapshots that #7256 sends to the hot table alone, over a 6 hour window.</summary>
    private Dictionary<string, Func<Task>> ConvertedReads(DateTime recentSnapshot)
    {
        var start = Whole(NowUtc.AddHours(-6));
        var end = Whole(NowUtc);
        return new Dictionary<string, Func<Task>>
        {
            ["GetActiveQuerySlicerDataAsync"] = () => _service.GetActiveQuerySlicerDataAsync(ServerId, 6),
            ["GetLatestQuerySnapshotsAsync"] = () => _service.GetLatestQuerySnapshotsAsync(ServerId, 6),
            ["GetActiveQueriesPageAsync"] = () => _service.GetActiveQueriesPageAsync(ServerId, 6, 100),
            ["GetQuerySnapshotsByWaitTypeAsync"] = () => _service.GetQuerySnapshotsByWaitTypeAsync(ServerId, "LCK_M_X", 6),
            ["GetAllQuerySnapshotsInRangeAsync"] = () => _service.GetAllQuerySnapshotsInRangeAsync(ServerId, 6),
            ["GetLongRunningQueriesAsync"] = () => _service.GetLongRunningQueriesAsync(ServerId, 1),
            ["GetSnapshotPlanTextAsync"] = () => _service.GetSnapshotPlanTextAsync(ServerId, recentSnapshot, 55, 0, live: false),
            ["GetCpuByDatabaseAsync"] = () => _service.GetCpuByDatabaseAsync(ServerId, start, end, 10),
            ["GetLoadByCallerAsync"] = () => _service.GetLoadByCallerAsync(ServerId, start, end, 10, 3),
            ["GetQueryPileupsAsync"] = () => _service.GetQueryPileupsAsync(ServerId, start, end, 2, 10),
            ["GetDatabaseResourceUsageAsync"] = () => _service.GetDatabaseResourceUsageAsync(ServerId, 6),
            ["GetTopResourceConsumersAsync"] = () => _service.GetTopResourceConsumersAsync(ServerId, 6),
            ["GetExpensiveQueriesAsync"] = () => _service.GetExpensiveQueriesAsync(ServerId, 6),
            ["GetHighImpactQueriesAsync"] = () => _service.GetHighImpactQueriesAsync(ServerId, 6),
            ["GetQueryHeatmapCellsAsync"] = () => _service.GetQueryHeatmapCellsAsync(ServerId, HeatmapMetric.Cpu, 6),
            ["GetBucketedQueryDurationTrendAsync"] = () => _service.GetBucketedQueryDurationTrendAsync(ServerId, 6, end, 5),
            ["GetFleetWindowFloorAsync(QueryStats)"] = () => _service.GetFleetWindowFloorAsync(QueryWindowRelation.QueryStats, start, end),
            ["GetFleetWindowFloorAsync(QuerySnapshots)"] = () => _service.GetFleetWindowFloorAsync(QueryWindowRelation.QuerySnapshots, start, end),
        };
    }

    [Fact]
    public async Task EveryConvertedRead_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        var recentSnapshot = Whole(NowUtc.AddHours(-2));
        await ExecuteAsync(
            Row(Whole(NowUtc.AddDays(-10)), "archived", 999),
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100),
            /* An older row of the server in each hot table, so the fleet floor's per-server older-row probe stops there. */
            Row(Whole(NowUtc.AddHours(-8)), "olderHot", 50),
            SnapshotRow(Whole(NowUtc.AddDays(-10)), "archived"),
            SnapshotRow(Whole(NowUtc.AddHours(-8)), "olderHot"),
            SnapshotRow(recentSnapshot, "recent"));
        var statsFile = await ArchiveTableAsync("query_stats", NowUtc.AddDays(-1));
        var snapshotsFile = await ArchiveTableAsync("query_snapshots", NowUtc.AddDays(-1));
        var reads = ConvertedReads(recentSnapshot);

        /* Each read runs once against real archive files, which caches the span of each archive. */
        foreach (var read in reads.Values)
        {
            await read();
        }

        /* The archive files can no longer be read. A read that still opens one throws. */
        File.WriteAllText(statsFile, "not a parquet file");
        File.WriteAllText(snapshotsFile, "not a parquet file");

        var failed = new List<string>();
        foreach (var (name, read) in reads)
        {
            try
            {
                await read();
            }
            catch (Exception ex)
            {
                failed.Add($"{name}: {ex.GetType().Name}");
            }
        }

        Assert.Empty(failed);
    }

    [Fact]
    public async Task ConvertedReads_AWindowThatReachesIntoTheArchive_StillReadTheArchive()
    {
        await ExecuteAsync(
            Row(Whole(NowUtc.AddDays(-10)), "archived", 999),
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100),
            SnapshotRow(Whole(NowUtc.AddDays(-10)), "archived"),
            SnapshotRow(Whole(NowUtc.AddHours(-2)), "recent"));
        await ArchiveTableAsync("query_stats", NowUtc.AddDays(-1));
        await ArchiveTableAsync("query_snapshots", NowUtc.AddDays(-1));

        var snapshots = await _service.GetAllQuerySnapshotsInRangeAsync(ServerId, hoursBack: 24 * 12);
        var expensive = await _service.GetExpensiveQueriesAsync(ServerId, hoursBack: 24 * 12);
        var heatmap = await _service.GetQueryHeatmapCellsAsync(ServerId, HeatmapMetric.Cpu, hoursBack: 24 * 12);

        Assert.Equal(2, snapshots.Count);
        Assert.Equal(2, expensive.Count);
        Assert.Equal(2, heatmap.Count);
    }
}
