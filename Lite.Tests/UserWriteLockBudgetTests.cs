/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5650 lock fairness: a write the USER started (Dismiss, Dismiss All, a tag edit) waits longer for the store write
/// lock than a background writer that skips and retries on its own.
///
/// <para>Both budgets are injected per instance, so nothing here sleeps through the shipped 5 s and 30 s. The
/// background budget is 150 ms and the user budget is 10 s; a holder keeps the lock for 700 ms, which is longer than
/// the first and far shorter than the second (10 s here). <c>s_dbLock</c> is process-wide and thread-affine, so the holder is a
/// dedicated thread that enters and leaves it on that one thread.</para>
/// </summary>
[Collection("timing")]
public sealed class UserWriteLockBudgetTests : IClassFixture<SharedDuckDbFixture>
{
    private static readonly TimeSpan BackgroundBudget = TimeSpan.FromMilliseconds(150);
    private static readonly TimeSpan UserBudget = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan HoldFor = TimeSpan.FromMilliseconds(700);

    private readonly SharedDuckDbFixture _fixture;

    public UserWriteLockBudgetTests(SharedDuckDbFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task UserWrite_SucceedsWhenTheHolderReleasesAfterLongerThanTheBackgroundBudget()
    {
        var service = new LocalDataService(_fixture.DuckDb, BackgroundBudget, UserBudget);
        using var holder = HoldTheWriteLock();

        var waited = Stopwatch.StartNew();
        using (await Task.Run(() => service.OpenUserWriteConnectionAsync()))
        {
        }
        waited.Stop();

        Assert.True(
            waited.Elapsed > BackgroundBudget,
            $"the user write returned after {waited.Elapsed.TotalMilliseconds:F0} ms - it did not wait out the holder, so " +
            "this test proved nothing about the budget");
    }

    [Fact]
    public async Task BackgroundWrite_StillGivesUpAtItsOwnBudget()
    {
        var service = new LocalDataService(_fixture.DuckDb, BackgroundBudget, UserBudget);
        using var holder = HoldTheWriteLock();

        var waited = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<StoreLockTimeoutException>(() => Task.Run(() => service.OpenWriteConnectionAsync()));
        waited.Stop();

        Assert.True(
            waited.Elapsed < HoldFor,
            $"the background write waited {waited.Elapsed.TotalMilliseconds:F0} ms, past its {BackgroundBudget.TotalMilliseconds:F0} ms " +
            "budget and into the holder's hold: it was given the user budget");
        Assert.Contains("HoldTheWriteLock", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UserWrite_StillTimesOutAtItsOwnBudgetAndNamesTheHolder()
    {
        var service = new LocalDataService(_fixture.DuckDb, BackgroundBudget, TimeSpan.FromMilliseconds(250));
        using var holder = HoldTheWriteLock();

        var ex = await Assert.ThrowsAsync<StoreLockTimeoutException>(() => Task.Run(() => service.OpenUserWriteConnectionAsync()));

        Assert.Contains("HoldTheWriteLock", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheShippedUserBudgetIsThirtySecondsAndNeverBelowTheHostBudget()
    {
        Assert.Equal(30, LocalDataService.DefaultUserWriteLockBudget.TotalSeconds);
        Assert.True(LocalDataService.UserWriteLockBudget >= LocalDataService.DefaultUserWriteLockBudget);
        Assert.True(LocalDataService.UserWriteLockBudget >= LocalDataService.WriteLockBudget);
        Assert.True(LocalDataService.UserWriteLockBudget > LocalDataService.DefaultWriteLockBudget);
    }

    /// <summary>
    /// Takes the write lock on its own thread and keeps it for <c>HoldFor</c>. Named so the
    /// holder registry's text, which the timeout message quotes, names this method.
    /// </summary>
    private LockHolder HoldTheWriteLock()
    {
        var acquired = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var thread = new Thread(() =>
        {
            using (_fixture.DuckDb.AcquireWriteLock())
            {
                acquired.Set();
                release.Wait(HoldFor);
            }
        })
        { IsBackground = true, Name = "UserWriteLockBudgetTests holder" };
        thread.Start();
        Assert.True(acquired.Wait(TimeSpan.FromSeconds(10)), "the holder never got the write lock");
        return new LockHolder(thread, release);
    }

    private sealed class LockHolder(Thread thread, ManualResetEventSlim release) : IDisposable
    {
        public void Dispose()
        {
            release.Set();
            thread.Join(TimeSpan.FromSeconds(10));
        }
    }
}
