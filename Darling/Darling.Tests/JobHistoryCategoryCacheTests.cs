/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Job History category read is the slowest statement of the tab on a large store, and a field store logged five runs of it
/// in an hour of light use. A load that was superseded while the read ran threw the answer away, so the next load started the read
/// again, and going back to a window already read ran it again too. <see cref="JobHistoryCategoryCache"/> keeps the answer per window
/// and server and gives every caller of a key that is still running the same task. The Lite tab uses the same class.
/// </summary>
public sealed class JobHistoryCategoryCacheTests
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(30);
    private static readonly DateTime T0 = new(2026, 11, 11, 15, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// The fake store hands out tasks the test completes by hand, so a cache that stopped answering from memory would leave a caller
    /// waiting on a task nobody completes: the class would hang until the test job's own timeout instead of failing. Every wait on a task
    /// the cache returned goes through this, so that regression fails within seconds.
    /// </summary>
    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(10);

    private static Task<T> Done<T>(Task<T> task) => task.WaitAsync(WaitLimit);

    private static JobHistoryChoicesKey Key(int? serverId, int hours = 24) =>
        JobHistoryChoicesKey.For(startSlides: true, startUtc: T0.AddHours(-hours), endUtc: T0, endIsLive: true, serverId);

    /// <summary>A read the test completes by hand, and a count of how many times the store would have been asked.</summary>
    private sealed class FakeStore
    {
        public int Reads { get; private set; }

        public TaskCompletionSource<List<string>> Pending { get; private set; } = new();

        public Func<Task<List<string>>> Read => () =>
        {
            Reads++;
            Pending = new TaskCompletionSource<List<string>>();
            return Pending.Task;
        };
    }

    /// <summary>
    /// A load that is superseded while the read runs and the loads that follow it share the one read. RED on the old tab: only a load
    /// that was still current when its read arrived told the freshness the read had happened, so each of these loads started the
    /// statement again.
    /// </summary>
    [Fact]
    public async Task LoadsThatArriveWhileTheReadRuns_ShareOneRead()
    {
        var cache = new JobHistoryCategoryCache(Lifetime);
        var store = new FakeStore();
        var key = Key(null);

        var first = cache.GetAsync(key, T0, store.Read);
        var second = cache.GetAsync(key, T0.AddSeconds(2), store.Read);
        var third = cache.GetAsync(key, T0.AddSeconds(9), store.Read);

        Assert.Equal(1, store.Reads);
        Assert.False(first.IsCompleted);

        store.Pending.SetResult(["Database Maintenance", "ETL Loads"]);

        Assert.Equal(["Database Maintenance", "ETL Loads"], await Done(first));
        Assert.Same(await Done(first), await Done(second));
        Assert.Same(await Done(first), await Done(third));
        Assert.Equal(1, cache.ReadsStarted);
    }

    /// <summary>Going back to a window already read, and a refresh inside the lifetime, answer from memory.</summary>
    [Fact]
    public async Task AWindowAlreadyRead_AnswersFromMemory_WhateverWasReadInBetween()
    {
        var cache = new JobHistoryCategoryCache(Lifetime);
        var store = new FakeStore();

        var day = Key(null, 24);
        var month = Key(null, 24 * 30);
        var oneServer = Key(7, 24);

        var a = cache.GetAsync(day, T0, store.Read);
        store.Pending.SetResult(["A"]);
        await Done(a);
        var b = cache.GetAsync(month, T0.AddMinutes(1), store.Read);
        store.Pending.SetResult(["A", "B"]);
        await Done(b);
        var c = cache.GetAsync(oneServer, T0.AddMinutes(2), store.Read);
        store.Pending.SetResult(["A"]);
        await Done(c);
        Assert.Equal(3, store.Reads);

        Assert.Equal(["A"], await Done(cache.GetAsync(day, T0.AddMinutes(10), store.Read)));
        Assert.Equal(["A", "B"], await Done(cache.GetAsync(month, T0.AddMinutes(11), store.Read)));
        Assert.Equal(["A"], await Done(cache.GetAsync(oneServer, T0.AddMinutes(12), store.Read)));
        Assert.Equal(3, store.Reads);
    }

    /// <summary>A finished answer is read again once it is older than the lifetime, and not before.</summary>
    [Fact]
    public async Task AFinishedAnswer_IsReadAgain_OnlyAfterTheLifetime()
    {
        var cache = new JobHistoryCategoryCache(Lifetime);
        var store = new FakeStore();
        var key = Key(null);

        var first = cache.GetAsync(key, T0, store.Read);
        store.Pending.SetResult(["A"]);
        await Done(first);

        await Done(cache.GetAsync(key, T0 + Lifetime, store.Read));
        Assert.Equal(1, store.Reads);

        var again = cache.GetAsync(key, T0 + Lifetime + TimeSpan.FromSeconds(1), store.Read);
        Assert.Equal(2, store.Reads);
        store.Pending.SetResult(["A", "C"]);
        Assert.Equal(["A", "C"], await Done(again));
    }

    /// <summary>A read that failed is not kept: every caller waiting on it sees the failure, and the next caller reads again.</summary>
    [Fact]
    public async Task AFailedRead_IsNotKept()
    {
        var cache = new JobHistoryCategoryCache(Lifetime);
        var store = new FakeStore();
        var key = Key(null);

        var first = cache.GetAsync(key, T0, store.Read);
        var waiting = cache.GetAsync(key, T0.AddSeconds(1), store.Read);
        store.Pending.SetException(new InvalidOperationException("canceling statement due to statement timeout"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => Done(first));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Done(waiting));
        Assert.Equal(1, store.Reads);

        var retry = cache.GetAsync(key, T0.AddSeconds(2), store.Read);
        Assert.Equal(2, store.Reads);
        store.Pending.SetResult(["A"]);
        Assert.Equal(["A"], await Done(retry));
    }

    /// <summary>The cache is bounded: past its capacity the oldest finished read goes, and a read still running never does.</summary>
    [Fact]
    public async Task PastItsCapacity_TheOldestFinishedReadGoes_AndARunningReadStays()
    {
        var cache = new JobHistoryCategoryCache(Lifetime, capacity: 2);
        var store = new FakeStore();

        var one = cache.GetAsync(Key(1), T0, store.Read);
        store.Pending.SetResult(["one"]);
        await Done(one);
        var two = cache.GetAsync(Key(2), T0.AddSeconds(1), store.Read);
        store.Pending.SetResult(["two"]);
        await Done(two);
        var three = cache.GetAsync(Key(3), T0.AddSeconds(2), store.Read);
        var runningThree = store.Pending;

        /* Key 1 was the oldest finished read, so it went when key 3 started; key 3 is still running and key 2 is kept. */
        Assert.Equal(3, store.Reads);
        Assert.Equal(["two"], await Done(cache.GetAsync(Key(2), T0.AddSeconds(3), store.Read)));
        Assert.Equal(3, store.Reads);
        Assert.Same(three, cache.GetAsync(Key(3), T0.AddSeconds(3), store.Read));
        Assert.Equal(3, store.Reads);

        var oneAgain = cache.GetAsync(Key(1), T0.AddSeconds(4), store.Read);
        Assert.Equal(4, store.Reads);
        store.Pending.SetResult(["one again"]);
        runningThree.SetResult(["three"]);
        Assert.Equal(["one again"], await Done(oneAgain));
        Assert.Equal(["three"], await Done(three));
    }
}
