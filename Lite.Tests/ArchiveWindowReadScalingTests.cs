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
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
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

    /* Synchronous on purpose: the database lock is thread-affine (#2463), so no await may sit between taking it and releasing it (#7256). */
    private Task ExecuteAsync(params string[] statements)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
        return Task.CompletedTask;
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

    private string ProcedureRow(DateTime collected, string procedure, long cpu) =>
        $@"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', 'dbo', '{procedure}', 'SQL_STORED_PROCEDURE', {Ts(collected)}, 10, {cpu}, {cpu})";

    private string FileIoRow(DateTime collected, long readBytes) =>
        $@"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', 'Db_data', 'ROWS', 'D:\\Db.mdf', 100, 5, 0, {readBytes}, 0, 1, 0, 60)";

    [Fact]
    public async Task ReadsThatNameASecondArchivedView_ReadItsHotTableWhenItsOwnArchiveIsOlderThanTheWindow()
    {
        await ExecuteAsync(
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100),
            ProcedureRow(Whole(NowUtc.AddDays(-10)), "OldProc", 999),
            ProcedureRow(Whole(NowUtc.AddHours(-2)), "NewProc", 100),
            FileIoRow(Whole(NowUtc.AddDays(-10)), 1048576),
            FileIoRow(Whole(NowUtc.AddHours(-2)), 2097152));
        /* query_stats has no archive. The other two do, and each file holds nothing from the last 6 hours. */
        var procedureFile = await ArchiveTableAsync("procedure_stats", NowUtc.AddDays(-1));
        var fileIoFile = await ArchiveTableAsync("file_io_stats", NowUtc.AddDays(-1));
        var start = Whole(NowUtc.AddHours(-6));
        var end = Whole(NowUtc);
        var reads = new Dictionary<string, Func<Task>>
        {
            ["GetCpuByDatabaseAsync"] = () => _service.GetCpuByDatabaseAsync(ServerId, start, end, 10),
            ["GetDatabaseResourceUsageAsync"] = () => _service.GetDatabaseResourceUsageAsync(ServerId, 6),
            ["GetTopResourceConsumersAsync"] = () => _service.GetTopResourceConsumersAsync(ServerId, 6),
            ["HasProcedureStatInWindowAsync"] = () => _service.HasProcedureStatInWindowAsync(ServerId, 6, end),
            ["GetBucketedProcedureDurationTrendAsync"] = () => _service.GetBucketedProcedureDurationTrendAsync(ServerId, 6, end, 5),
        };

        /* Each read runs once against real archive files, which caches the span of each archive. */
        foreach (var read in reads.Values)
        {
            await read();
        }

        File.WriteAllText(procedureFile, "not a parquet file");
        File.WriteAllText(fileIoFile, "not a parquet file");

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
        var cpu = Assert.Single(await _service.GetCpuByDatabaseAsync(ServerId, start, end, 10));
        Assert.Equal(100, cpu.ProcedureCpuUs);
    }

    [Fact]
    public async Task ReadsThatNameASecondArchivedView_StillReadItsArchiveWhenItReachesIntoTheWindow()
    {
        await ExecuteAsync(
            Row(Whole(NowUtc.AddHours(-2)), "recent", 100),
            ProcedureRow(Whole(NowUtc.AddDays(-10)), "OldProc", 700),
            ProcedureRow(Whole(NowUtc.AddHours(-2)), "NewProc", 100));
        await ArchiveTableAsync("procedure_stats", NowUtc.AddDays(-1));

        var row = Assert.Single(await _service.GetCpuByDatabaseAsync(ServerId, Whole(NowUtc.AddDays(-12)), Whole(NowUtc), 10));

        Assert.Equal(800, row.ProcedureCpuUs);
    }

    /// <summary>#7498: the stored-row question answers from the archive when the only row in the window is archived.</summary>
    [Fact]
    public async Task TheStoredProcedureRowQuestion_FindsARowThatOnlyTheArchiveHolds()
    {
        await ExecuteAsync(ProcedureRow(Whole(NowUtc.AddDays(-10)), "OldProc", 700));
        await ArchiveTableAsync("procedure_stats", NowUtc.AddDays(-1));

        Assert.True(await _service.HasProcedureStatInWindowAsync(ServerId, 12 * 24, Whole(NowUtc)));
        Assert.False(await _service.HasProcedureStatInWindowAsync(ServerId, 6, Whole(NowUtc)));
    }

    [Fact]
    public async Task TheBucketedQueryTrend_ReadsTheArchivedPointJustBeforeTheWindow_ForTheFirstPointsInterval()
    {
        var end = Whole(NowUtc);
        var start = end.AddHours(-6);
        /* The statement reads one gap policy before the window start, so the first point has its previous point: 2400 s apart, not the stored 60. */
        await ExecuteAsync(
            $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, max_dop, sample_interval_seconds)
VALUES ({_nextId++}, {Ts(start.AddMinutes(-30))}, {ServerId}, 'TestSrv', 'Db', 'h1', 'h1', {Ts(start)}, 1, 1, 1, 1, 60),
       ({_nextId++}, {Ts(start.AddMinutes(10))}, {ServerId}, 'TestSrv', 'Db', 'h1', 'h1', {Ts(start)}, 2400, 1, 1, 1, 60)");
        await ArchiveTableAsync("query_stats", start);

        var points = await _service.GetBucketedQueryDurationTrendAsync(ServerId, 6, end, 5);

        var point = Assert.Single(points);
        Assert.Equal(1.0, point.ExecutionsPerSecond!.Value, 3);
    }

    [Theory]
    [InlineData("CollectBadActorFactsAsync", "query_stats")]
    [InlineData("CollectActiveQueryFactsAsync", "query_snapshots")]
    public async Task AnArchiveRebuildDuringAFactRead_AddsEachFactOnce(string collectMethod, string table)
    {
        var window = Whole(NowUtc.AddHours(-2));
        await ExecuteAsync(
            $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, max_dop, query_text)
VALUES ({_nextId++}, {Ts(Whole(NowUtc.AddDays(-10)))}, {ServerId}, 'TestSrv', 'Db', 'old', 'old', {Ts(window)}, 200, 5000000, 5000000, 1, 'SELECT 1'),
       ({_nextId++}, {Ts(window)}, {ServerId}, 'TestSrv', 'Db', 'busy', 'busy', {Ts(window)}, 200, 5000000, 5000000, 1, 'SELECT 2')",
            SnapshotRow(Whole(NowUtc.AddDays(-10)), "old"),
            SnapshotRow(window, "busy"));
        await ArchiveTableAsync(table, NowUtc.AddDays(-1));

        var collector = new PerformanceMonitorLite.Analysis.DuckDbFactCollector(_duckDb);
        var method = typeof(PerformanceMonitorLite.Analysis.DuckDbFactCollector).GetMethod(collectMethod, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var context = new PerformanceMonitor.Analysis.AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestSrv",
            TimeRangeStart = Whole(NowUtc.AddHours(-6)),
            TimeRangeEnd = Whole(NowUtc),
        };
        Task Collect(List<PerformanceMonitor.Analysis.Fact> into) => (Task)method.Invoke(collector, [context, into])!;

        /* The first run caches the archive span. */
        var warm = new List<PerformanceMonitor.Analysis.Fact>();
        await Collect(warm);
        var expected = Assert.Single(warm);

        /* The second run waits behind the write lock, and the archive view generation moves while it waits: its hot read is thrown away and
           the view is read instead. Each fact still arrives once. */
        var facts = new List<PerformanceMonitor.Analysis.Fact>();
        var waitBudget = TimeSpan.FromSeconds(30);
        var writeLockTaken = new ManualResetEventSlim();
        var startReader = new ManualResetEventSlim();
        Task? run = null;

        /* The write lock is thread-affine (#2463): a hold across an await can resume on another thread, throw on dispose and leave the static
           lock held for every later test. So it is taken, held and released on one dedicated thread, with no await inside. The thread bumps
           the generation only once the reader is parked on the lock (a deterministic signal, not a sleep) and every wait is bounded. */
        var holder = DuckDbLockThread.RunAsync(() =>
        {
            using (_duckDb.AcquireWriteLock(waitBudget))
            {
                var waitingBefore = DuckDbInitializer.WaitingReadCountForTests;
                writeLockTaken.Set();
                if (!startReader.Wait(waitBudget))
                {
                    return "the test never started the reader";
                }
                if (!TestWait.TryWaitUntil(() => DuckDbInitializer.WaitingReadCountForTests > waitingBefore, waitBudget, TimeSpan.FromMilliseconds(1)))
                {
                    return "the read never reached the lock wait";
                }
                if (run!.IsCompleted)
                {
                    return "the read finished while the write lock was held";
                }
                _duckDb.BumpArchiveViewGeneration();
                return "";
            }
        });

        Assert.True(writeLockTaken.Wait(waitBudget, TestContext.Current.CancellationToken), "the write lock was not taken");
        run = Task.Run(() => Collect(facts), TestContext.Current.CancellationToken);
        startReader.Set();
        var holderResult = await holder.WaitAsync(waitBudget + waitBudget, TestContext.Current.CancellationToken);
        Assert.Equal("", holderResult);

        await run.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        var fact = Assert.Single(facts);
        Assert.Equal(expected.Key, fact.Key);
    }
}
