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
/// #7256: the windowed reads of <c>v_procedure_stats</c> in <c>LocalDataService.QueryStats.cs</c> read the hot table alone when
/// the procedure_stats archive holds nothing in the window, as the query_stats reads do. Each test caches the archive spans with
/// one real read, then makes the archive files unreadable: a read that still opens one throws.
/// </summary>
public sealed class ProcedureStatsArchiveWindowTests : IDisposable
{
    private const int ServerId = 7256;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private long _nextId = 1;

    public ProcedureStatsArchiveWindowTests()
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

    private string ProcedureRow(DateTime collected, string procedure, long cpu) =>
        $@"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
     sql_handle, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', 'dbo', '{procedure}', 'SQL_STORED_PROCEDURE', '{procedure}', {Ts(collected)}, 10, {cpu}, {cpu}, 60)";

    private string QueryRow(DateTime collected, string hash, string sqlHandle) =>
        $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, max_dop, query_text, sample_interval_seconds)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', '{hash}', '{sqlHandle}', {Ts(collected)}, 10, 5, 5, 1, 'SELECT {hash}', 60)";

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

    private async Task SeedAsync()
    {
        await ExecuteAsync(
            ProcedureRow(Whole(NowUtc.AddDays(-10)), "OldProc", 700),
            ProcedureRow(Whole(NowUtc.AddHours(-2)), "NewProc", 100),
            ProcedureRow(Whole(NowUtc.AddHours(-3)), "NewProc", 100),
            QueryRow(Whole(NowUtc.AddDays(-10)), "oldq", "OldProc"),
            QueryRow(Whole(NowUtc.AddHours(-2)), "newq", "NewProc"));
    }

    /// <summary>Every windowed read of procedure_stats in QueryStats.cs, over the last 6 hours (the comparison: that range against the 6 hours before it).</summary>
    private Dictionary<string, Func<Task>> Reads()
    {
        var end = Whole(NowUtc);
        return new Dictionary<string, Func<Task>>
        {
            ["GetProcedureStatsHistoryAsync"] = () => _service.GetProcedureStatsHistoryAsync(ServerId, "Db", "dbo", "NewProc", 6),
            ["GetProcStatsSlicerDataAsync"] = () => _service.GetProcStatsSlicerDataAsync(ServerId, 6),
            ["GetTopProceduresByCpuAsync"] = () => _service.GetTopProceduresByCpuAsync(ServerId, 6),
            ["GetProcedureStatsComparisonAsync"] = () => _service.GetProcedureStatsComparisonAsync(ServerId, end.AddHours(-6), end, end.AddHours(-12), end.AddHours(-6)),
            ["GetProcedureDurationTrendAsync"] = () => _service.GetProcedureDurationTrendAsync(ServerId, 6),
        };
    }

    private string StoreRow(DateTime collected, long queryId, string? text) =>
        $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, runtime_stats_interval_id, interval_start_time_utc)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', {queryId}, {queryId}, 'Regular', {Ts(collected.AddMinutes(-10))}, {Ts(collected)},
     {(text is null ? "NULL" : "'" + text + "'")}, '0x00000000000000{queryId}', 5, 100, 200, 1, 1, 1, 1, {Ts(collected.AddMinutes(-10))})";

    [Fact]
    public async Task QueryStoreTopQueries_EveryCandidateHasHotText_OpensNoArchiveFile()
    {
        await ExecuteAsync(
            StoreRow(Whole(NowUtc.AddDays(-10)), 11, "SELECT archived"),
            StoreRow(Whole(NowUtc.AddHours(-2)), 11, "SELECT hot"));
        var storeFile = await ArchiveTableAsync("query_store_stats", NowUtc.AddDays(-1));

        var first = Assert.Single(await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6));
        Assert.Equal("SELECT hot", first.QueryText);

        /* The archive file can no longer be read. A read that still opens it throws. */
        File.WriteAllText(storeFile, "not a parquet file");

        var second = Assert.Single(await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6));
        Assert.Equal("SELECT hot", second.QueryText);
    }

    [Fact]
    public async Task QueryStoreTopQueries_ACandidateWhoseOnlyTextIsArchived_StillGetsItsText()
    {
        await ExecuteAsync(
            StoreRow(Whole(NowUtc.AddDays(-10)), 12, "SELECT archived only"),
            StoreRow(Whole(NowUtc.AddHours(-2)), 12, null),
            StoreRow(Whole(NowUtc.AddHours(-2)), 13, "SELECT hot"));
        await ArchiveTableAsync("query_store_stats", NowUtc.AddDays(-1));

        var rows = await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6);

        Assert.Equal("SELECT archived only", Assert.Single(rows, r => r.QueryId == 12).QueryText);
        Assert.Equal("SELECT hot", Assert.Single(rows, r => r.QueryId == 13).QueryText);
    }

    [Fact]
    public async Task EveryWindowedProcedureStatsRead_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await SeedAsync();
        var procedureFile = await ArchiveTableAsync("procedure_stats", NowUtc.AddDays(-1));
        var queryFile = await ArchiveTableAsync("query_stats", NowUtc.AddDays(-1));
        var reads = Reads();

        /* Each read runs once against real archive files, which caches the span of each archive. */
        foreach (var read in reads.Values)
        {
            await read();
        }

        /* The archive files can no longer be read. A read that still opens one throws. */
        File.WriteAllText(procedureFile, "not a parquet file");
        File.WriteAllText(queryFile, "not a parquet file");

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
        var top = Assert.Single(await _service.GetTopProceduresByCpuAsync(ServerId, 6));
        Assert.Equal("NewProc", top.ObjectName);
        Assert.Equal(2, (await _service.GetProcedureStatsHistoryAsync(ServerId, "Db", "dbo", "NewProc", 6)).Count);
    }

    [Fact]
    public async Task WindowedProcedureStatsReads_AWindowThatReachesIntoTheArchive_StillReadTheArchive()
    {
        await SeedAsync();
        await ArchiveTableAsync("procedure_stats", NowUtc.AddDays(-1));
        await ArchiveTableAsync("query_stats", NowUtc.AddDays(-1));
        var end = Whole(NowUtc);

        var history = await _service.GetProcedureStatsHistoryAsync(ServerId, "Db", "dbo", "OldProc", 24 * 12);
        var top = await _service.GetTopProceduresByCpuAsync(ServerId, 24 * 12);
        var slicer = await _service.GetProcStatsSlicerDataAsync(ServerId, 24 * 12);
        var comparison = await _service.GetProcedureStatsComparisonAsync(ServerId, end.AddHours(-6), end, end.AddDays(-11), end.AddDays(-9));

        Assert.Single(history);
        Assert.Equal(["NewProc", "OldProc"], top.Select(r => r.ObjectName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        /* One bucket per hour with a row: the archived one, and the hot rows of 3 and 2 hours ago. */
        Assert.Equal(3, slicer.Count);
        var old = Assert.Single(comparison, r => r.ObjectName == "OldProc");
        Assert.Equal(10, old.BaselineExecutionCount);
        /* The baseline range is in the query_stats archive too, so the statement text for its handle comes from there. */
        Assert.Equal("SELECT oldq", old.QueryText);
    }
}
