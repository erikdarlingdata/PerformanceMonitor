/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5750: <see cref="NotificationRouteCache.Invalidate"/> starts ONE background reload, so an alert after an edit rarely waits.
/// Single-flight (an Invalidate during a reload costs one more load, not one per call), no wait for the alert path once the
/// reload has finished, the synchronous fallback in <c>Current</c>, and a failing loader neither spins nor loses the routes in
/// force. Run on <see cref="NotificationRouteCacheCore"/>, the logic behind the static <see cref="NotificationRouteCache"/>.
/// </summary>
public sealed class NotificationRouteCacheBackgroundTests
{
    /* Each test gets its own instance: the static cache is invalidated by every server and tag writer in the process, so a
       count of loads on it would include another test's Invalidate. The static class is a thin facade over the same logic. */
    private readonly NotificationRouteCacheCore _cache = new();

    private static NotificationRoute Route(int id) => new(id, "High CPU", "https://teams.cache.test/hook", "", "", "", "", true);

    private async Task WaitForIdleAsync()
    {
        /* Polled by count, not by clock: a time limit is judged against the runner's load (#5602). */
        for (var i = 0; _cache.BackgroundReloadRunning; i++)
        {
            Assert.True(i < 3000, "The background reload did not finish.");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task ManyInvalidatesDuringOneReload_CostOneMoreLoad_NotOnePerCall()
    {
        var calls = 0;
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        _cache.Configure(() =>
        {
            Interlocked.Increment(ref calls);
            entered.Set();
            release.Wait(TimeSpan.FromSeconds(10));
            return new[] { Route(1) };
        });

        _cache.Invalidate();
        Assert.True(entered.Wait(TimeSpan.FromSeconds(10)), "Invalidate did not start a background reload.");
        Assert.True(_cache.BackgroundReloadRunning);

        for (var i = 0; i < 5; i++)
        {
            _cache.Invalidate();
        }

        Assert.Equal(1, Volatile.Read(ref calls));
        release.Set();
        await WaitForIdleAsync();

        /* The first load, plus exactly one more for the five Invalidates that arrived while it ran. */
        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task OnceTheBackgroundReloadHasFinished_CurrentDoesNotLoadAgain()
    {
        var calls = 0;
        _cache.Configure(() =>
        {
            Interlocked.Increment(ref calls);
            return new[] { Route(7) };
        });

        /* Configure primes the first load (#5750 review), so the routes are loaded once without any Invalidate. */
        await WaitForIdleAsync();
        Assert.Equal(1, Volatile.Read(ref calls));
        _cache.Invalidate();
        await WaitForIdleAsync();
        Assert.Equal(2, Volatile.Read(ref calls));

        /* A read that reached the loader would show in the count. */
        for (var i = 0; i < 1000; i++)
        {
            Assert.Equal(7, _cache.Current[0].RouteId);
        }

        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public void AReadThatBeatsTheBackgroundLoad_StillGetsTheRoutes_OffTheUiThread()
    {
        var calls = 0;
        _cache.Configure(() =>
        {
            Interlocked.Increment(ref calls);
            return new[] { Route(3) };
        });

        /* This read either joins the load Configure primed or runs it: either way it returns the loaded routes, once. */
        Assert.Equal(3, _cache.Current[0].RouteId);
        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task ConfigurePrimesTheFirstLoad_WithoutAnyRead()
    {
        using var entered = new ManualResetEventSlim(false);
        _cache.Configure(() =>
        {
            entered.Set();
            return new[] { Route(4) };
        });

        Assert.True(entered.Wait(TimeSpan.FromSeconds(10)), "Configure did not start the first load.");
        await WaitForIdleAsync();
        Assert.Equal(4, _cache.Current[0].RouteId);
    }

    [Fact]
    public async Task ADispatcherThreadRead_NeverRunsTheLoaderInline_AndReadsTheLastSnapshot()
    {
        var testThread = Environment.CurrentManagedThreadId;
        var loadThreads = new System.Collections.Concurrent.ConcurrentBag<int>();
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var cache = new NotificationRouteCacheCore(isUiThread: () => Environment.CurrentManagedThreadId == testThread);
        cache.Configure(() =>
        {
            loadThreads.Add(Environment.CurrentManagedThreadId);
            entered.Set();
            release.Wait(TimeSpan.FromSeconds(10));
            return new[] { Route(9) };
        });
        Assert.True(entered.Wait(TimeSpan.FromSeconds(10)), "Configure did not start the first load.");

        /* The load is stuck (the stand-in for a DuckDB write lock). The "dispatcher" read returns the held snapshot, which is
           empty before the first load, at once: on the old shape it joined the stuck load through the gate or ran it itself. */
        Assert.Empty(cache.Current);
        Assert.DoesNotContain(testThread, loadThreads);

        release.Set();
        for (var i = 0; cache.BackgroundReloadRunning; i++)
        {
            Assert.True(i < 3000, "The background reload did not finish.");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        /* Once loaded, the same thread reads the loaded routes, still without having loaded anything. */
        Assert.Equal(9, cache.Current[0].RouteId);
        Assert.DoesNotContain(testThread, loadThreads);
    }

    [Fact]
    public async Task AnAlertBeforeTheFirstLoad_WaitsForTheRoutes_InsteadOfReadingTheEmptyList()
    {
        var testThread = Environment.CurrentManagedThreadId;
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var cache = new NotificationRouteCacheCore(isUiThread: () => Environment.CurrentManagedThreadId == testThread);
        cache.Configure(() =>
        {
            entered.Set();
            release.Wait(TimeSpan.FromSeconds(10));
            return new[] { Route(11) };
        });

        /* The loader is inside its load, which holds the cache's gate: the worst case, and the only order this test checks (it
           was racy before: the wait ran either ahead of the primed load or behind it). A wait that took the gate would block
           this thread until the loader's own 10 second wait ran out and then report itself completed. */
        Assert.True(entered.Wait(TimeSpan.FromSeconds(10)), "Configure did not start the first load.");

        /* What EmailAlertService does before it reads the routes: await the first load. The wait holds no thread, so the
           "dispatcher" is free, and the alert resumes with the routes loaded rather than the pre-load empty list. */
        var wait = cache.WaitForFirstLoadAsync(TimeSpan.FromSeconds(30));
        Assert.False(wait.IsCompleted);
        Assert.Empty(cache.Current);

        release.Set();
        await wait.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal(11, cache.Current[0].RouteId);
    }

    [Fact]
    public void EveryAlertSendInEmailAlertService_AwaitsTheFirstRouteLoad_BeforeTheSendCoreReadsTheRoutes()
    {
        var lines = Lite.Tests.ParitySource.ReadFile("Lite/Services/EmailAlertService.cs").Split('\n');
        var sends = 0;
        for (var i = 0; i < lines.Length; i++)
        {
            if (!lines[i].Contains("await _core.TrySendAsync(", StringComparison.Ordinal))
            {
                continue;
            }

            sends++;
            Assert.Contains("await NotificationRouteCache.WaitForFirstLoadAsync(", lines[i - 1], StringComparison.Ordinal);
        }

        Assert.Equal(2, sends);
    }

    [Fact]
    public async Task TheFirstLoadWait_IsBounded_AndEndsAtOnceWithNoLoaderOrAfterTheLoad()
    {
        var neverLoads = new NotificationRouteCacheCore(isUiThread: () => false);
        await neverLoads.WaitForFirstLoadAsync(TimeSpan.Zero);

        using var release = new ManualResetEventSlim(false);
        var stuck = new NotificationRouteCacheCore(isUiThread: () => false);
        stuck.Configure(() =>
        {
            release.Wait(TimeSpan.FromSeconds(60));
            return new[] { Route(12) };
        });

        /* The loader is stuck until the test releases it, so a wait with no bound would not return here. */
        await stuck.WaitForFirstLoadAsync(TimeSpan.FromMilliseconds(50));
        release.Set();

        var loaded = new NotificationRouteCacheCore(isUiThread: () => false);
        loaded.Configure(() => new[] { Route(13) });
        Assert.Equal(13, loaded.Current[0].RouteId);
        await loaded.WaitForFirstLoadAsync(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task AFailingBackgroundReload_KeepsTheRoutesInForce_AndDoesNotSpin()
    {
        var calls = 0;
        var fail = false;
        _cache.Configure(() =>
        {
            Interlocked.Increment(ref calls);
            return fail ? throw new InvalidOperationException("store unavailable") : new[] { Route(5) };
        });
        await WaitForIdleAsync();
        Assert.Equal(5, _cache.Current[0].RouteId);
        Assert.Equal(1, calls);

        fail = true;
        _cache.Invalidate();
        await WaitForIdleAsync();
        await Task.Delay(300, TestContext.Current.CancellationToken);

        /* One failed attempt, then the 30 second retry window: no loop, no read-triggered retry, the old routes still serve. */
        Assert.Equal(2, Volatile.Read(ref calls));
        Assert.Equal(5, _cache.Current[0].RouteId);
        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public void WithNoLoader_InvalidateStartsNothing()
    {
        _cache.Configure(null);
        _cache.Invalidate();
        Assert.False(_cache.BackgroundReloadRunning);
        Assert.Empty(_cache.Current);
    }
}
