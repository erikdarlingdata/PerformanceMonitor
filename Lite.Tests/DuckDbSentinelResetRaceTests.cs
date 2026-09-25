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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4343: ten <c>RemoteCollectorService</c>/<c>DeltaCalculator</c> call sites open a transient DuckDB
/// connection with no lock at all, so with the sentinel live (#4262) one of those connections can attach
/// to an instance <see cref="DuckDbInitializer.ResetDatabaseAsync"/> is tearing down mid-reset. Every site
/// now takes the read or write lock first. These tests exercise the two shapes the review round asked for
/// that #4339's first pass left undone: a real collection-cycle write racing a real reset, and the
/// thread-pool retry branch <c>OpenDatabaseAsync</c> takes on a transient file lock.
/// </summary>
[Collection("CollectionResetGate")]
public class DuckDbSentinelResetRaceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public DuckDbSentinelResetRaceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
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

    /// <summary>Exposes the runner's protected state accessors — the same seam <c>CollectorStateStoreTests</c>
    /// uses — so these tests drive the real <see cref="RemoteCollectorService.SaveCollectorStateAsync"/> and
    /// <see cref="RemoteCollectorService.GetCollectorStateAsync"/> rather than a stand-in that merely copies
    /// their lock-and-connect shape.</summary>
    private sealed class CollectorStateAccess(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task SaveAsync(int serverId, string collector, IReadOnlyDictionary<string, string> state) =>
            SaveCollectorStateAsync(serverId, collector, state, CancellationToken.None);

        public Task<Dictionary<string, string>> LoadAsync(int serverId, string collector) =>
            GetCollectorStateAsync(serverId, collector, CancellationToken.None);
    }

    /// <summary>
    /// #4343's core claim for the collection-write sites: a reset that starts while a collection-cycle
    /// write holds its connection must wait for it rather than race it, and the next cycle's write must
    /// land in the file the reset actually produced.
    ///
    /// <para><b>Why a head start, not a synchronization point.</b> On the fixed shape the outcome does not
    /// depend on timing at all — <see cref="RemoteCollectorService.SaveCollectorStateAsync"/> and
    /// <see cref="DuckDbInitializer.ResetDatabaseAsync"/> both take <c>s_dbLock</c>, so
    /// <see cref="ReaderWriterLockSlim"/>'s own exclusivity serializes them regardless of exactly when
    /// either starts — there is no CI-timing-dependent way for this test to go red on correct code. The
    /// 25ms head start and the 4,000-entry state dict exist only to give the BROKEN shape (the write with
    /// no lock at all) a realistic chance of still having its connection open when the reset's
    /// <c>ReleaseSentinel()</c>/<c>File.Delete</c> runs — the exact precondition #4343 exists to make safe.
    /// Confirmed empirically: reverting <c>SaveCollectorStateAsync</c>'s <c>AcquireWriteLock()</c> call
    /// turns this test red (see the PR body for the run).</para>
    /// </summary>
    [Fact]
    public async Task ResetDatabaseAsync_StartingWhileACollectionCycleWriteHoldsItsConnection_WaitsAndTheNextWriteLandsInTheNewFile()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        var writer = new CollectorStateAccess(initializer);

        /* Big enough that the upsert loop's connection stays open for a real slice of wall-clock time —
           the fixed shape does not need this width (the lock makes the order exact either way), but the
           broken shape does, to make the race land inside the still-open connection's window. */
        var racingState = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < 4000; i++)
            racingState[$"key_{i}"] = $"value_{i}";

        var writeTask = Task.Run(() => writer.SaveAsync(serverId: 1, "racer", racingState));

        /* A head start: long enough that the writer has entered its lock and begun the loop before the
           reset even attempts to start, on either shape. */
        await Task.Delay(25);

        var resetTask = Task.Run(() => initializer.ResetDatabaseAsync());

        await Task.WhenAll(writeTask, resetTask);

        /* The racer started first and (fixed shape) holds the write lock for its whole loop, so the reset
           can only run after it releases — the racer's state must not have survived into the fresh table.
           A reset that ran WHILE the racer's connection was still open (broken shape) can instead leave the
           racer's row sitting in the reinitialized database (attached to the same live handle) or throw
           partway through the loop and leave the store in whatever partial state that produced. */
        var racerState = await writer.LoadAsync(1, "racer");
        Assert.True(racerState.Count == 0,
            $"Expected the racer's pre-reset state to be gone after ResetDatabaseAsync, found {racerState.Count} key(s) — " +
            "the reset did not wait for the collection-cycle write that held the connection.");

        /* The next cycle's write, issued only once the race has settled, must land in the file the
           sentinel is now actually serving. */
        await writer.SaveAsync(serverId: 2, "after-reset", new Dictionary<string, string> { ["k"] = "v" });
        var readBack = await writer.LoadAsync(2, "after-reset");
        Assert.True(readBack.TryGetValue("k", out var value) && value == "v",
            "The write issued after ResetDatabaseAsync settled did not land in the new file.");

        initializer.Dispose();
    }

    /// <summary>
    /// The thread-pool retry branch round 1 asked for: <c>OpenDatabaseAsync</c> retries a transient DuckDB
    /// open failure with a blocking <c>Thread.Sleep</c>, not <c>await Task.Delay</c> (#4262 round 1 finding
    /// 4), specifically so the retry loop keeps the SAME thread identity across every attempt even with no
    /// <c>SynchronizationContext</c> to pin it — the shape <c>Task.Run</c> produces. A real Windows
    /// sharing violation (<see cref="FileShare.None"/>, proven to conflict with any other opener regardless
    /// of the other opener's own requested share mode by this file's sibling class's own
    /// <c>InitializeAsync_WithPendingSchemaMigration_MigratesThenOpensSentinel</c>) forces the retry branch
    /// for real, rather than asserting against a mocked exception.
    /// </summary>
    [Fact]
    public async Task InitializeAsync_TransientSharingViolationOnThreadPoolThread_RetriesAndReleasesTheWriteLock()
    {
        var seed = new DuckDbInitializer(_dbPath);
        await seed.InitializeAsync();
        seed.Dispose();

        var blocker = File.Open(_dbPath, FileMode.Open, FileAccess.Read, FileShare.None);
        try
        {
            var initializer = new DuckDbInitializer(_dbPath);

            /* Task.Run: no SynchronizationContext, exactly the shape the Thread.Sleep-not-Task.Delay
               comment on OpenDatabaseAsync is about. */
            var initTask = Task.Run(() => initializer.InitializeAsync());

            /* Past the retry loop's 1s Thread.Sleep, so at least one retry attempt runs while the file is
               still locked and a later one finds it free — proving the loop, not just a lucky first
               attempt. */
            await Task.Delay(TimeSpan.FromMilliseconds(1200));
            blocker.Dispose();

            await initTask;

            /* Proves the write lock InitializeAsync took on the thread-pool thread was released by the
               SAME thread that entered it, not leaked on a stranded thread identity (#2463) — a leaked
               lock would make this throw TimeoutException instead of returning immediately. */
            using (initializer.AcquireWriteLock(TimeSpan.FromSeconds(2)))
            {
            }

            initializer.Dispose();
        }
        finally
        {
            blocker.Dispose();
        }
    }
}
