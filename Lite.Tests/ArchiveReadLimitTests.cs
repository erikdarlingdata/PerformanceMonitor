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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5377: the limiter that caps expensive archive reads at two, in isolation. Instances only, never
/// <see cref="ArchiveReadLimiter.Shared"/>, so no other test's read can be starved by a slot held here.
/// </summary>
public sealed class ArchiveReadLimiterTests
{
    [Fact]
    public void TheSharedLimiter_IsTwoWide() => Assert.Equal(2, ArchiveReadLimiter.Shared.Width);

    [Fact]
    public async Task AThirdConcurrentRead_Waits_UntilASlotIsFreed()
    {
        var limiter = new ArchiveReadLimiter(2);
        using var first = await limiter.EnterAsync(CancellationToken.None);
        using var second = await limiter.EnterAsync(CancellationToken.None);
        Assert.Equal(0, limiter.Available);

        var third = limiter.EnterAsync(CancellationToken.None);
        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.False(third.IsCompleted, "a third read must wait while two hold slots");

        first.Dispose();
        using var slot = await third.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(0, limiter.Available);
    }

    [Fact]
    public async Task ACancelledWaiter_LeavesPromptly_AndReleasesNothingItDidNotTake()
    {
        var limiter = new ArchiveReadLimiter(2);
        var first = await limiter.EnterAsync(CancellationToken.None);
        var second = await limiter.EnterAsync(CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var waiter = limiter.EnterAsync(cts.Token);
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(waiter.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await waiter.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(0, limiter.Available);

        /* Had the cancelled waiter released a slot it never held, one dispose would now show two free. */
        first.Dispose();
        Assert.Equal(1, limiter.Available);
        second.Dispose();
        Assert.Equal(2, limiter.Available);
    }

    [Fact]
    public async Task DisposingASlotTwice_ReleasesItOnce()
    {
        var limiter = new ArchiveReadLimiter(2);
        var slot = await limiter.EnterAsync(CancellationToken.None);
        slot.Dispose();
        slot.Dispose();
        Assert.Equal(2, limiter.Available);
    }

    [Fact]
    public async Task AnAlreadyCancelledToken_TakesNoSlot()
    {
        var limiter = new ArchiveReadLimiter(2);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await limiter.EnterAsync(cts.Token));
        Assert.Equal(2, limiter.Available);
    }
}

/// <summary>#5377: concurrent first callers for one cache key share one read.</summary>
public sealed class ArchiveWatermarkSingleFlightTests
{
    [Fact]
    public async Task ConcurrentFirstCallers_ShareOneRead()
    {
        var cache = new ArchiveWatermarkCache();
        var calls = 0;
        var release = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<object?> Read() { Interlocked.Increment(ref calls); return release.Task; }

        var callers = Enumerable.Range(0, 8).Select(_ => cache.GetOrReadAsync("k", 1, Read)).ToArray();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref calls));

        release.SetResult(42);
        foreach (var value in await Task.WhenAll(callers))
            Assert.Equal(42, value);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AGenerationBump_ReadsOnceMore_ThenServesTheNewGenerationFromCache()
    {
        var cache = new ArchiveWatermarkCache();
        var calls = 0;
        Task<object?> Read() { calls++; return Task.FromResult<object?>(calls); }

        await cache.GetOrReadAsync("k", 1, Read);
        await cache.GetOrReadAsync("k", 2, Read);
        await cache.GetOrReadAsync("k", 2, Read);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task ANewVariant_OverwritesTheEntryInPlace()
    {
        var cache = new ArchiveWatermarkCache();
        var calls = 0;
        Task<object?> Read() { calls++; return Task.FromResult<object?>(calls); }

        await cache.GetOrReadAsync("k", 1, Read, variant: 10);
        await cache.GetOrReadAsync("k", 1, Read, variant: 10);
        Assert.Equal(1, calls);
        await cache.GetOrReadAsync("k", 1, Read, variant: 11);
        Assert.Equal(2, calls);

        var entries = (System.Collections.ICollection)typeof(ArchiveWatermarkCache)
            .GetField("_entries", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(cache)!;
        Assert.Single(entries);
    }

    [Fact]
    public async Task AFailedRead_FailsEveryJoiner_AndStoresNothing()
    {
        var cache = new ArchiveWatermarkCache();
        var calls = 0;
        var fail = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<object?> Read() { Interlocked.Increment(ref calls); return fail.Task; }

        var callers = Enumerable.Range(0, 4).Select(_ => cache.GetOrReadAsync("k", 1, Read)).ToArray();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        fail.SetException(new InvalidOperationException("boom"));
        foreach (var caller in callers)
            await Assert.ThrowsAsync<InvalidOperationException>(() => caller);
        Assert.Equal(1, calls);

        Assert.Equal(7, await cache.GetOrReadAsync("k", 1, () => Task.FromResult<object?>(7)));
    }

    [Fact]
    public async Task WhenTheRunningCallerIsCancelled_AJoinerReadsInsteadOfInheritingTheCancellation()
    {
        var cache = new ArchiveWatermarkCache();
        using var leaderCts = new CancellationTokenSource();
        var leaderStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var leader = cache.GetOrReadAsync("k", 1, async () =>
        {
            leaderStarted.SetResult();
            await Task.Delay(Timeout.Infinite, leaderCts.Token);
            return null;
        });
        await leaderStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        var joiner = cache.GetOrReadAsync("k", 1, () => Task.FromResult<object?>("joiner read"));
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(joiner.IsCompleted);

        leaderCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leader);
        Assert.Equal("joiner read", await joiner.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AJoinerThatIsCancelled_LeavesWithoutDisturbingTheRead()
    {
        var cache = new ArchiveWatermarkCache();
        var release = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var leader = cache.GetOrReadAsync("k", 1, () => release.Task);

        using var cts = new CancellationTokenSource();
        var joiner = cache.GetOrReadAsync("k", 1, () => Task.FromResult<object?>("never"), cancellationToken: cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => joiner);

        release.SetResult(5);
        Assert.Equal(5, await leader);
        Assert.Equal(5, await cache.GetOrReadAsync("k", 1, () => Task.FromResult<object?>("never")));
    }
}

/// <summary>
/// #5377 on a real DuckDB store with a real archive-and-reset (hot rows in the live table, cold rows in
/// Parquet): the per-database watermark read is one grouped archive read per table and generation, its
/// answers equal the old per-database reads' answers, the uncached floored read is gone except for the one
/// band that needs it, and the archive reads take the limiter before the read lock.
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both
   process-wide, so this class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class ArchiveGroupedWatermarkReadTests : IDisposable
{
    private static readonly DateTime Now = new(2026, 5, 1, 18, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Floor = Now.AddHours(-3);

    private const string Table = "query_store_stats";
    private const string Column = "last_execution_time";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public ArchiveGroupedWatermarkReadTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        CollectionResetGate.ResetForTests();
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

    private sealed class Reads(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task<DateTime?> DatabaseTimeAsync(
            int serverId, string databaseName, DateTime? since = null, CancellationToken cancellationToken = default) =>
            GetLastCollectedTimeForDatabaseAsync(serverId, Table, Column, "database_name", databaseName, cancellationToken, since);

        public Task<DateTime?> TimeAsync(int serverId, string table, string column, CancellationToken cancellationToken = default) =>
            GetLastCollectedTimeAsync(serverId, table, column, cancellationToken);

        public int CacheEntryCount()
        {
            var cache = typeof(RemoteCollectorService)
                .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
                .Single(f => f.FieldType == typeof(ArchiveWatermarkCache))
                .GetValue(this)!;
            return ((System.Collections.ICollection)typeof(ArchiveWatermarkCache)
                .GetField("_entries", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(cache)!).Count;
        }
    }

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task ExecuteAsync(params string[] statements)
    {
        await _duckDb.InitializeAsync();
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private Task ResetAsync() => new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

    private static long _nextId = 1;

    private static string QueryStore(int server, DateTime collected, string db, DateTime lastExecution) =>
        $"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, last_execution_time) VALUES ({Interlocked.Increment(ref _nextId)}, {Ts(collected)}, {server}, 'S{server}', '{db}', {Ts(lastExecution)})";

    private static readonly string[] Databases = ["Active", "Idle", "Band", "LiveOnly", "ArchOnlyRecent", "Missing"];

    /// <summary>
    /// A store covering every shape the watermark read meets. Cold (Parquet): Active (old and recent), Idle (old
    /// only), Band (two rows collected just after the floor whose values are older than it), ArchOnlyRecent, and
    /// server 2's Active. Hot (live table) after the reset: Active (recent) and LiveOnly.
    /// </summary>
    private async Task SeedHotAndParquetAsync()
    {
        await ExecuteAsync(
            QueryStore(1, Now.AddHours(-6), "Active", Now.AddHours(-6)),
            QueryStore(1, Now.AddHours(-1), "Active", Now.AddHours(-1)),
            QueryStore(1, Now.AddHours(-6), "Idle", Now.AddHours(-6)),
            QueryStore(1, Now.AddHours(-6), "Idle", Now.AddHours(-6).AddMinutes(1)),
            QueryStore(1, Floor.AddMinutes(5), "Band", Floor.AddMinutes(-20)),
            QueryStore(1, Floor.AddMinutes(20), "Band", Floor.AddMinutes(-30)),
            QueryStore(1, Now.AddHours(-2), "ArchOnlyRecent", Now.AddHours(-2)),
            QueryStore(2, Now.AddHours(-6), "Active", Now.AddHours(-6)));
        await ResetAsync();
        await ExecuteAsync(
            QueryStore(1, Now.AddMinutes(-10), "Active", Now.AddMinutes(-10)),
            QueryStore(1, Now.AddMinutes(-5), "LiveOnly", Now.AddMinutes(-5)));
    }

    /// <summary>The answer the OLD per-database read gave, run straight against the view (the oracle).</summary>
    private async Task<DateTime?> OldAnswerAsync(int serverId, string db, DateTime? since)
    {
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        async Task<DateTime?> MaxAsync(string table, bool floored)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT MAX({Column}) FROM {table} WHERE server_id = $1 AND database_name = $2"
                + (floored ? " AND collection_time > $3" : "");
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = db });
            if (floored)
                cmd.Parameters.Add(new DuckDBParameter { Value = since!.Value });
            return await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken) is DateTime d ? d : null;
        }

        var live = await MaxAsync(Table, floored: since is not null);
        var archived = await MaxAsync("v_" + Table, floored: false);
        if (since is DateTime s && (archived is null || archived.Value <= s))
            archived = await MaxAsync("v_" + Table, floored: true);
        return live is null ? archived : archived is null ? live : (live >= archived ? live : archived);
    }

    [Fact]
    public async Task ManyDatabases_OneGeneration_CostExactlyOneArchiveRead()
    {
        await SeedHotAndParquetAsync();
        var reads = new Reads(_duckDb);

        foreach (var db in Databases)
            await reads.DatabaseTimeAsync(1, db, since: null, TestContext.Current.CancellationToken);
        await reads.DatabaseTimeAsync(2, "Active", since: null, TestContext.Current.CancellationToken);

        Assert.Equal(1, reads.ArchiveViewReadsForTests);
    }

    [Fact]
    public async Task AGenerationBump_ReReadsOnce()
    {
        await SeedHotAndParquetAsync();
        var reads = new Reads(_duckDb);

        foreach (var db in Databases)
            await reads.DatabaseTimeAsync(1, db, since: null, TestContext.Current.CancellationToken);
        Assert.Equal(1, reads.ArchiveViewReadsForTests);

        _duckDb.BumpArchiveViewGeneration();
        foreach (var db in Databases)
            await reads.DatabaseTimeAsync(1, db, since: null, TestContext.Current.CancellationToken);
        Assert.Equal(2, reads.ArchiveViewReadsForTests);
    }

    [Fact]
    public async Task EveryAnswer_EqualsTheOldPerDatabaseAnswer_OnAHotAndParquetStore()
    {
        await SeedHotAndParquetAsync();
        var reads = new Reads(_duckDb);

        DateTime?[] floors = [null, Floor, Floor.AddMinutes(-40), Floor.AddMinutes(2), Floor.AddMinutes(10),
            Floor.AddMinutes(19), Floor.AddMinutes(25), Floor.AddMinutes(70), Now.AddHours(-1), Now];
        foreach (var server in new[] { 1, 2, 3 })
        {
            foreach (var db in Databases)
            {
                foreach (var floor in floors)
                {
                    Assert.Equal(
                        await OldAnswerAsync(server, db, floor),
                        await reads.DatabaseTimeAsync(server, db, floor, TestContext.Current.CancellationToken));
                }
            }
        }
    }

    [Fact]
    public async Task AnUnknownDatabase_ReadsNull()
    {
        await SeedHotAndParquetAsync();
        Assert.Null(await new Reads(_duckDb).DatabaseTimeAsync(1, "NoSuchDatabase", Floor, TestContext.Current.CancellationToken));
        Assert.Null(await new Reads(_duckDb).DatabaseTimeAsync(1, "NoSuchDatabase", null, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AnIdleDatabase_WithNoRowNewerThanTheFloor_ReadsNull_WithoutAFlooredArchiveRead()
    {
        /* #2344's contract on hot + parquet rows, and the read it no longer needs: the idle database's archived
           and live rows are all 6 h old, the floor is 3 h, the map's newest collection is at or below the floor, so
           the answer is NULL and the only archive read is the grouped one. */
        await SeedHotAndParquetAsync();
        await ExecuteAsync(QueryStore(1, Now.AddHours(-6), "Idle", Now.AddHours(-6)));
        var reads = new Reads(_duckDb);

        Assert.Null(await reads.DatabaseTimeAsync(1, "Idle", Floor, TestContext.Current.CancellationToken));
        Assert.Equal(1, reads.ArchiveViewReadsForTests);
    }

    [Fact]
    public async Task ADatabaseWithNoArchivedRow_NeverRunsAFlooredArchiveRead()
    {
        await SeedHotAndParquetAsync();
        var reads = new Reads(_duckDb);

        Assert.Equal(Now.AddMinutes(-5), await reads.DatabaseTimeAsync(1, "LiveOnly", Floor, TestContext.Current.CancellationToken));
        Assert.Null(await reads.DatabaseTimeAsync(1, "Missing", Floor, TestContext.Current.CancellationToken));
        Assert.Equal(1, reads.ArchiveViewReadsForTests);
    }

    [Fact]
    public async Task TheFlooredBand_IsReadOncePerHour_AndFilteredExactlyInMemory()
    {
        /* Band: rows collected at Floor+5m (value Floor-20m) and Floor+20m (value Floor-30m); the archive's
           maximum, Floor-20m, is below every floor used here, but a row collected after the floor exists. */
        await SeedHotAndParquetAsync();
        var reads = new Reads(_duckDb);

        /* Floor+2m: both rows were collected after it, so the greater value, Floor-20m. */
        Assert.Equal(Floor.AddMinutes(-20), await reads.DatabaseTimeAsync(1, "Band", Floor.AddMinutes(2), TestContext.Current.CancellationToken));
        Assert.Equal(2, reads.ArchiveViewReadsForTests);

        /* Floor+10m (same hour): only the second row qualifies, from the cached rows with no new read. */
        Assert.Equal(Floor.AddMinutes(-30), await reads.DatabaseTimeAsync(1, "Band", Floor.AddMinutes(10), TestContext.Current.CancellationToken));
        Assert.Equal(2, reads.ArchiveViewReadsForTests);

        /* Floor+25m: neither row was collected after it. */
        Assert.Null(await reads.DatabaseTimeAsync(1, "Band", Floor.AddMinutes(25), TestContext.Current.CancellationToken));
        Assert.Equal(2, reads.ArchiveViewReadsForTests);
    }

    [Fact]
    public async Task TheFlooredBand_AcrossHourBuckets_OverwritesItsEntryInsteadOfGrowingTheCache()
    {
        await SeedHotAndParquetAsync();
        var reads = new Reads(_duckDb);

        await reads.DatabaseTimeAsync(1, "Band", Floor.AddMinutes(2), TestContext.Current.CancellationToken);
        var after = reads.CacheEntryCount();

        /* Floor is 15:00, so Floor-15m (14:45) is in the 14:00 bucket and Floor+3m in the 15:00 one. */
        Assert.Equal(Floor.AddMinutes(-20), await reads.DatabaseTimeAsync(1, "Band", Floor.AddMinutes(-15), TestContext.Current.CancellationToken));
        Assert.Equal(after, reads.CacheEntryCount());
        await reads.DatabaseTimeAsync(1, "Band", Floor.AddMinutes(3), TestContext.Current.CancellationToken);
        Assert.Equal(after, reads.CacheEntryCount());
    }

    [Fact]
    public async Task EightConcurrentDatabases_BehindAHeldLimiter_ShareOneRead()
    {
        await SeedHotAndParquetAsync();
        var limiter = new ArchiveReadLimiter(1);
        var reads = new Reads(_duckDb) { ArchiveReadLimiterForTests = limiter };

        var held = await limiter.EnterAsync(CancellationToken.None);
        var callers = Enumerable.Range(0, 8)
            .Select(i => reads.DatabaseTimeAsync(1, Databases[i % Databases.Length], since: null, TestContext.Current.CancellationToken))
            .ToArray();
        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.All(callers, c => Assert.False(c.IsCompleted));

        held.Dispose();
        await Task.WhenAll(callers).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(1, reads.ArchiveViewReadsForTests);
    }

    private static ReaderWriterLockSlim DbLock() =>
        (ReaderWriterLockSlim)typeof(DuckDbInitializer)
            .GetField("s_dbLock", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;

    [Fact]
    public async Task AnArchiveRead_TakesTheLimiterBeforeTheReadLock()
    {
        await SeedHotAndParquetAsync();
        var steps = new List<string>();
        var dbLock = DbLock();
        var reads = new Reads(_duckDb)
        {
            ArchiveReadStepForTests = step => steps.Add($"{step}:{(dbLock.IsReadLockHeld ? "read lock held" : "no read lock")}")
        };

        await reads.DatabaseTimeAsync(1, "Active", since: null, TestContext.Current.CancellationToken);

        Assert.Equal(["limiter:no read lock", "readlock:read lock held", "read:read lock held"], steps);
    }

    [Fact]
    public async Task ARead_WaitingForASlot_HoldsNoReadLock_SoAWriterIsNotParked()
    {
        await SeedHotAndParquetAsync();
        var limiter = new ArchiveReadLimiter(1);
        var reads = new Reads(_duckDb) { ArchiveReadLimiterForTests = limiter };

        var held = await limiter.EnterAsync(CancellationToken.None);
        var waiting = reads.TimeAsync(1, Table, Column, TestContext.Current.CancellationToken);
        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.False(waiting.IsCompleted, "the read is waiting for a slot");

        /* A writer (the CHECKPOINT) gets in at once: the waiting read took the limiter first and holds no read
           lock. Acquired and released with no await between, because the lock is thread-affine. */
        var writer = _duckDb.AcquireWriteLock(TimeSpan.FromSeconds(10));
        writer.Dispose();

        held.Dispose();
        /* The server-wide maximum: LiveOnly's live row, five minutes before Now. */
        Assert.Equal(Now.AddMinutes(-5), await waiting.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ACancelledRead_LeavesTheQueue_AndTheSlotsAreUnchanged()
    {
        await SeedHotAndParquetAsync();
        var limiter = new ArchiveReadLimiter(2);
        var reads = new Reads(_duckDb) { ArchiveReadLimiterForTests = limiter };

        var first = await limiter.EnterAsync(CancellationToken.None);
        var second = await limiter.EnterAsync(CancellationToken.None);
        using var cts = new CancellationTokenSource();
        var waiting = reads.TimeAsync(1, Table, Column, cts.Token);
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.False(waiting.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await waiting.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(0, limiter.Available);
        first.Dispose();
        second.Dispose();
        Assert.Equal(2, limiter.Available);
    }
}

/// <summary>#5377: the forced-plan alert's read of <c>v_query_store_stats</c> shares the archive-read limit.</summary>
public sealed class ForcePlanArchiveReadLimitTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public ForcePlanArchiveReadLimitTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task TheForcedPlanRead_WaitsForASlot_ThenTakesTheLimiterBeforeTheReadLock()
    {
        var limiter = new ArchiveReadLimiter(1);
        var steps = new List<string>();
        var dbLock = (ReaderWriterLockSlim)typeof(DuckDbInitializer)
            .GetField("s_dbLock", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        var service = new LocalDataService(_duckDb)
        {
            ArchiveReadLimiterForTests = limiter,
            ArchiveReadStepForTests = step => steps.Add($"{step}:{(dbLock.IsReadLockHeld ? "read lock held" : "no read lock")}")
        };

        var held = await limiter.EnterAsync(CancellationToken.None);
        var read = service.GetForcePlanFailuresAsync(1, TestContext.Current.CancellationToken);
        await Task.Delay(250, TestContext.Current.CancellationToken);
        Assert.False(read.IsCompleted, "the read must wait while the only slot is held");
        Assert.Empty(steps);

        held.Dispose();
        Assert.Empty(await read.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
        Assert.Equal(["limiter:no read lock", "readlock:read lock held"], steps);
        Assert.Equal(1, limiter.Available);
    }

    [Fact]
    public async Task ACancelledForcedPlanRead_LeavesTheQueue_AndReleasesNothingItDidNotTake()
    {
        var limiter = new ArchiveReadLimiter(2);
        var service = new LocalDataService(_duckDb) { ArchiveReadLimiterForTests = limiter };

        var first = await limiter.EnterAsync(CancellationToken.None);
        var second = await limiter.EnterAsync(CancellationToken.None);
        using var cts = new CancellationTokenSource();
        var read = service.GetForcePlanFailuresAsync(1, cts.Token);
        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.False(read.IsCompleted);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await read.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        first.Dispose();
        Assert.Equal(1, limiter.Available);
        second.Dispose();
        Assert.Equal(2, limiter.Available);
    }
}
