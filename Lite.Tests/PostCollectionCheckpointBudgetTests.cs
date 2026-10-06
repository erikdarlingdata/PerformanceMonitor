/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5371: the CHECKPOINT after a collection round waits for the process-wide write lock, and a waiting writer on a
/// <see cref="ReaderWriterLockSlim"/> makes every NEW reader wait too. With no timeout, one slow read parked the
/// CHECKPOINT, and the CHECKPOINT parked every UI read and the next collector behind it. The wait is now bounded by
/// the write-lock budget: on a timeout this round's CHECKPOINT is skipped and the next round retries.
///
/// <para>In the reset-gate collection because the write lock is one per process: a test that holds a read lock for a
/// moment should not run beside the tests that take the write lock with a timeout.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class PostCollectionCheckpointBudgetTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-checkpoint-budget-tests-").FullName;
    private DuckDbInitializer? _duckDb;

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private async Task<RemoteCollectorService> OpenAsync()
    {
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        await _duckDb.InitializeAsync();
        return new RemoteCollectorService(_duckDb, new ServerManager(_dir), new ScheduleManager(_dir));
    }

    /* A reader that holds the read lock until released. The lock is thread-affine, so it is taken and released on
       one dedicated thread. */
    private static (Thread Thread, ManualResetEventSlim Release) HoldReadLock(DuckDbInitializer duckDb)
    {
        var held = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var thread = new Thread(() =>
        {
            using var readLock = duckDb.AcquireReadLock();
            held.Set();
            release.Wait();
        })
        { IsBackground = true };
        thread.Start();
        Assert.True(held.Wait(TimeSpan.FromSeconds(10)), "the reader never took the read lock");
        return (thread, release);
    }

    [Fact]
    public async Task ASlowReader_MakesTheCheckpointGiveUpWithinTheBudget_NotWaitForTheReader()
    {
        var service = await OpenAsync();
        var budget = TimeSpan.FromMilliseconds(300);
        var (reader, release) = HoldReadLock(_duckDb!);
        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            /* On the pool, because the lock wait is synchronous: a method that blocks on it never returns a task to
               time out, so a wait with no bound would hang this test instead of failing it. */
            var ran = await Task.Run(() => service.RunPostCollectionCheckpointAsync(budget, CancellationToken.None))
                .WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);
            stopwatch.Stop();

            Assert.False(ran, "the CHECKPOINT ran under a read lock it could not have had");
            Assert.True(stopwatch.Elapsed >= budget - TimeSpan.FromMilliseconds(50), $"gave up after {stopwatch.ElapsedMilliseconds} ms, before the budget");
            Assert.True(stopwatch.Elapsed < budget + TimeSpan.FromSeconds(5), $"waited {stopwatch.ElapsedMilliseconds} ms for a {budget.TotalMilliseconds} ms budget");
        }
        finally
        {
            release.Set();
            reader.Join(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task AReaderStartedWhileTheCheckpointWaits_IsNotBlockedPastTheBudget()
    {
        var service = await OpenAsync();
        var budget = TimeSpan.FromMilliseconds(500);
        var (reader, release) = HoldReadLock(_duckDb!);
        try
        {
            var checkpoint = Task.Run(() => service.RunPostCollectionCheckpointAsync(budget, CancellationToken.None));

            /* Let the CHECKPOINT start waiting, then start a reader behind it: a waiting writer holds back new
               readers, so without a bound this reader waits for the slow one. */
            await Task.Delay(100, TestContext.Current.CancellationToken);
            var started = System.Diagnostics.Stopwatch.StartNew();
            var late = Task.Run(() =>
            {
                using var readLock = _duckDb!.AcquireReadLock();
                return started.Elapsed;
            });

            var waited = await late.WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);
            Assert.True(waited < budget + TimeSpan.FromSeconds(5), $"the late reader waited {waited.TotalMilliseconds} ms behind a {budget.TotalMilliseconds} ms budget");
            Assert.False(await checkpoint);
        }
        finally
        {
            release.Set();
            reader.Join(TimeSpan.FromSeconds(10));
        }
    }

    [Fact]
    public async Task WithNothingReading_TheCheckpointRuns()
    {
        var service = await OpenAsync();

        Assert.True(await service.RunPostCollectionCheckpointAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
    }
}
