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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: a Lite tab load of 66 to 90 seconds while collection ran, with the lock-holder report naming the forced-plan
/// alert read and the archive read. These pin the three changes: a slow read's log line splits lock wait, open and
/// query time, a read whose window starts after the newest archived row never opens an archive file, and the
/// older-row probe of the data-start floor answers as it did.
/// </summary>
public sealed class ArchiveSpanReadTests : IDisposable
{
    private const int ServerId = 7256;
    private const string Db = "ForcedDb";

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private long _nextId = 1;
    private int _archiveFiles;

    public ArchiveSpanReadTests()
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

    private string Row(int server, DateTime collected, long failures) =>
        $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, is_forced_plan, force_failure_count, plan_forcing_type, last_force_failure_reason) VALUES ({_nextId++}, {Ts(collected)}, {server}, 'S{server}', '{Db}', 11, 22, true, {failures}, 'MANUAL', 'GENERAL_FAILURE')";

    /// <summary>Moves every hot row older than <paramref name="before"/> (and, when given, newer than <paramref name="after"/>) into a
    /// Parquet archive file, then rebuilds the views, which bumps the archive view generation. Returns the file's path.</summary>
    private async Task<string> ArchiveAsync(DateTime before, DateTime? after = null)
    {
        var path = Path.Combine(_archiveDir, $"2026010{++_archiveFiles}_000000_query_store_stats.parquet").Replace("\\", "/");
        var range = $"collection_time < {Ts(before)}" + (after is DateTime a ? $" AND collection_time >= {Ts(a)}" : "");
        await ExecuteAsync(
            $"COPY (SELECT * FROM query_store_stats WHERE {range}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM query_store_stats WHERE {range}");
        await _duckDb.CreateArchiveViewsAsync();
        return path;
    }

    [Fact]
    public async Task AWindowNewerThanTheArchive_ReadsTheHotTable_AndOpensNoArchiveFile()
    {
        /* The archive holds only old rows. The window rows, two collections of one forced plan whose failure counter rose, are hot. */
        await ExecuteAsync(
            Row(ServerId, Whole(NowUtc.AddDays(-5)), 0),
            Row(ServerId, Whole(NowUtc.AddMinutes(-50)), 1),
            Row(ServerId, Whole(NowUtc.AddMinutes(-20)), 4));
        var archiveFile = await ArchiveAsync(before: NowUtc.AddDays(-1));

        var first = await _service.GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken);
        Assert.Equal(3, Assert.Single(first).FailureDelta);

        /* The archive file can no longer be read. A read that still opens it throws. */
        File.WriteAllText(archiveFile, "not a parquet file");

        var second = await _service.GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken);
        Assert.Equal(3, Assert.Single(second).FailureDelta);
    }

    [Fact]
    public async Task AWindowThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        /* The earlier collection sits in the archive, the later one in the hot table: the delta needs both. */
        await ExecuteAsync(
            Row(ServerId, Whole(NowUtc.AddMinutes(-90)), 1),
            Row(ServerId, Whole(NowUtc.AddMinutes(-20)), 4));
        await ArchiveAsync(before: NowUtc.AddMinutes(-60));

        var rows = await _service.GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken);

        Assert.Equal(3, Assert.Single(rows).FailureDelta);
    }

    [Fact]
    public async Task ANewArchiveGeneration_SendsTheReadBackIntoTheArchive()
    {
        /* The span is cached while the archive is old. Then the hot rows move to Parquet (the archive-and-reset does this and bumps
           the generation), so the cached span is stale and the read must look at the archive again. */
        await ExecuteAsync(
            Row(ServerId, Whole(NowUtc.AddDays(-5)), 0),
            Row(ServerId, Whole(NowUtc.AddMinutes(-50)), 1),
            Row(ServerId, Whole(NowUtc.AddMinutes(-20)), 4));
        await ArchiveAsync(before: NowUtc.AddDays(-1));
        Assert.Equal(3, Assert.Single(await _service.GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken)).FailureDelta);

        await ArchiveAsync(before: NowUtc.AddMinutes(10), after: NowUtc.AddDays(-1));

        Assert.Equal(3, Assert.Single(await _service.GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken)).FailureDelta);
    }

    [Fact]
    public async Task AStoreWithNoArchive_ReadsTheHotTable()
    {
        await ExecuteAsync(
            Row(ServerId, Whole(NowUtc.AddMinutes(-50)), 1),
            Row(ServerId, Whole(NowUtc.AddMinutes(-20)), 4));

        Assert.Equal(3, Assert.Single(await _service.GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken)).FailureDelta);
    }

    /* The older-row probe. Window start S: the first row inside the window is after S, so the floor is S when any row of the server
       is older than S and the first row otherwise. */
    public static IEnumerable<object[]> OlderRowShapes() =>
    [
        ["none", false],
        ["hot", true],
        ["archive", true],
        ["archive of another server", false],
        ["hot and archive", true],
    ];

    [Theory]
    [MemberData(nameof(OlderRowShapes))]
    public async Task TheDataStartFloor_AnswersAsItDid_WithAndWithoutAnOlderRow(string shape, bool olderRowOfTheServerExists)
    {
        var start = Whole(NowUtc.AddHours(-6));
        var end = Whole(NowUtc);
        var firstInWindow = Whole(start.AddMinutes(30));
        var older = Whole(start.AddHours(-2));

        var archived = new List<string>();
        var hotOlder = new List<string>();
        if (shape is "hot" or "hot and archive") hotOlder.Add(Row(ServerId, older, 0));
        if (shape is "archive" or "hot and archive") archived.Add(Row(ServerId, older.AddMinutes(-5), 0));
        if (shape == "archive of another server") archived.Add(Row(ServerId + 1, older, 0));

        await ExecuteAsync([Row(ServerId, firstInWindow, 1), Row(ServerId, Whole(start.AddHours(1)), 2), .. archived]);
        if (archived.Count > 0)
        {
            /* Every row older than the start so far is an archived one: they all move to Parquet. */
            await ArchiveAsync(before: start);
        }
        else
        {
            await _duckDb.CreateArchiveViewsAsync();
        }

        if (hotOlder.Count > 0)
        {
            await ExecuteAsync([.. hotOlder]);
        }

        var floor = await _service.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, ServerId, start, end, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(olderRowOfTheServerExists ? start : firstInWindow, floor);
    }

    [Fact]
    public async Task ASlowReadsLogLine_SeparatesTheLockWaitFromTheQueryTime()
    {
        var logDirectory = Path.Combine(_tempDir, "logs");
        var oldThreshold = QueryLogger.ThresholdSeconds;
        var oldEnabled = QueryLogger.IsEnabled;
        QueryLogger.Initialize(logDirectory);
        QueryLogger.SetEnabled(true);
        QueryLogger.SetThreshold(0);
        try
        {
            /* A writer holds the lock for 400 ms with the read queued behind it. The read's own work is a few milliseconds. */
            var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var release = new ManualResetEventSlim();
            var holder = Task.Factory.StartNew(() =>
            {
                using var writeLock = _duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
                signal.SetResult();
                release.Wait(TimeSpan.FromMinutes(2));
            }, TaskCreationOptions.LongRunning);
            await signal.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var read = Task.Run(() => _service.GetQueryWindowFloorAsync(
                QueryWindowRelation.QueryStoreStats, ServerId, NowUtc.AddHours(-1), NowUtc), TestContext.Current.CancellationToken);
            await Task.Delay(400, TestContext.Current.CancellationToken);
            Assert.False(read.IsCompleted, "the read waits behind the writer");
            release.Set();
            await holder;
            await read.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        }
        finally
        {
            QueryLogger.SetThreshold(oldThreshold);
            QueryLogger.SetEnabled(oldEnabled);
        }

        var log = File.ReadAllText(QueryLogger.GetCurrentLogFile());
        var match = Regex.Match(log, @"Context:\s+GetQueryWindowFloorAsync \(lock wait (\d+) ms, open (\d+) ms, query and row read (\d+) ms\)");
        Assert.True(match.Success, "the slow-read line carries the three phases: " + log[..Math.Min(log.Length, 600)]);
        var lockWait = int.Parse(match.Groups[1].Value);
        var queryAndRows = int.Parse(match.Groups[3].Value);
        Assert.True(lockWait >= 300, $"the queued read's wait is reported as lock wait ({lockWait} ms)");
        Assert.True(queryAndRows < lockWait, $"the wait is not counted as query time ({queryAndRows} ms query, {lockWait} ms lock wait)");
    }
}
