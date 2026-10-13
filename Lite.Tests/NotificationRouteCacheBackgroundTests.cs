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

        _cache.Invalidate();
        await WaitForIdleAsync();
        Assert.Equal(1, Volatile.Read(ref calls));

        /* A read that reached the loader would show in the count. */
        for (var i = 0; i < 1000; i++)
        {
            Assert.Equal(7, _cache.Current[0].RouteId);
        }

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public void WhenNoBackgroundReloadHasRun_CurrentStillReloadsOnTheCallingThread()
    {
        var calls = 0;
        var loadedOn = -1;
        _cache.Configure(() =>
        {
            Interlocked.Increment(ref calls);
            loadedOn = Environment.CurrentManagedThreadId;
            return new[] { Route(3) };
        });

        /* Configure marks the routes stale without starting a reload: this read is the fallback. */
        Assert.Equal(3, _cache.Current[0].RouteId);
        Assert.Equal(1, calls);
        Assert.Equal(Environment.CurrentManagedThreadId, loadedOn);
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
        _cache.Invalidate();
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
