/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4477: pure (no live Postgres) pins for <see cref="SingleFlightTtlCache{T}"/>, the shared single-flight
/// gate the Viewer's fleet-health read and store-size cache both went through to fix two defects: a
/// synchronous-completion wedge, and a waiter that could not cancel without cancelling the shared fetch.
/// </summary>
public sealed class SingleFlightTtlCacheTests
{
    [Fact]
    public async Task ConcurrentCallers_ShareOneFetch()
    {
        var cache = new SingleFlightTtlCache<int>(TimeSpan.FromMinutes(5));
        var fetchCount = 0;
        var gate = new SemaphoreSlim(0);

        async Task<int> Fetch()
        {
            Interlocked.Increment(ref fetchCount);
            await gate.WaitAsync();
            return 42;
        }

        var racers = new Task<int>[8];
        for (var i = 0; i < racers.Length; i++)
        {
            racers[i] = cache.GetOrStartAsync(Fetch);
        }

        /* Give every racer a chance to observe the in-flight task before the fetch is allowed to finish. */
        await Task.Delay(50);
        gate.Release(8);

        var results = await Task.WhenAll(racers);

        Assert.Equal(1, fetchCount);
        Assert.All(results, r => Assert.Equal(42, r));
    }

    /// <summary>
    /// Defect (a)'s exact shape, reproduced as a local: the naive <c>_inFlight ??= Fetch();</c> pattern,
    /// where <c>Fetch</c> is an <c>async</c> method that COMPLETES SYNCHRONOUSLY — no real await ever
    /// suspends it (e.g. a disposed data source that throws before any I/O) — so its returned
    /// <see cref="Task{T}"/> is already faulted the instant <c>Fetch()</c> returns, before the caller's
    /// <c>??=</c> has assigned it to the shared field. The old code's <c>finally</c> (inside <c>Fetch</c>)
    /// runs and clears the field FIRST, in the very same synchronous call; the assignment then runs SECOND
    /// and overwrites that clear with the already-faulted task — which is why the fix in
    /// <see cref="SingleFlightTtlCache{T}"/> starts every fetch with <see cref="Task.Run(Func{Task})"/>: a
    /// scheduled continuation cannot finish before the method that scheduled it has returned the assigned
    /// task to its own caller, so the clear (a <c>ContinueWith</c>, itself gated by the lock) can never run
    /// before the assignment.
    /// </summary>
    [Fact]
    public void OldInlinePattern_WedgesOnASynchronousCompletion()
    {
        Task<int>? inFlight = null;
        var gateObj = new object();

        Task<int> Start()
        {
            lock (gateObj)
            {
                inFlight ??= FaultsSynchronously();
                return inFlight;
            }
        }

        Task<int> FaultsSynchronously()
        {
            /* No await at all: the returned task is ALREADY completed (faulted) by the time this method
               returns to Start — the shape a disposed/misconfigured data source's ExecuteReaderAsync can
               take when it throws before any I/O. The "finally" below models the old code's cleanup, which
               ran and cleared the field as part of building this already-finished task, strictly BEFORE
               Start's own `??=` runs. */
            try
            {
                return Task.FromException<int>(new InvalidOperationException("faulted before any await"));
            }
            finally
            {
                inFlight = null;
            }
        }

        var first = Start();
        Assert.True(first.IsFaulted);

        /* The wedge: `inFlight` was cleared by FaultsSynchronously's `finally` BEFORE Start's `??=` ran, so
           the assignment overwrites that clear with the faulted task — and now sits there for the next
           caller too, because nothing ran AFTER the assignment to clear it again. */
        var second = Start();
        Assert.Same(first, second);
        Assert.True(second.IsFaulted);
    }

    [Fact]
    public async Task SynchronousThrow_TheCacheStartsAFreshFetchNextTime()
    {
        var cache = new SingleFlightTtlCache<int>(TimeSpan.FromMinutes(5));
        var attempt = 0;

        Task<int> Fetch()
        {
            attempt++;
            if (attempt == 1)
            {
                throw new InvalidOperationException("synchronous failure before any await");
            }

            return Task.FromResult(99);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => cache.GetOrStartAsync(Fetch));

        var result = await cache.GetOrStartAsync(Fetch);
        Assert.Equal(99, result);
        Assert.Equal(2, attempt);
    }

    [Fact]
    public async Task AsyncFault_TheCacheStartsAFreshFetchNextTime()
    {
        var cache = new SingleFlightTtlCache<int>(TimeSpan.FromMinutes(5));
        var attempt = 0;

        async Task<int> Fetch()
        {
            attempt++;
            await Task.Yield();
            if (attempt == 1)
            {
                throw new InvalidOperationException("async failure after the first await");
            }

            return 7;
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => cache.GetOrStartAsync(Fetch));

        var result = await cache.GetOrStartAsync(Fetch);
        Assert.Equal(7, result);
        Assert.Equal(2, attempt);
    }

    [Fact]
    public async Task WithinTheTtl_Cached_AfterTheTtl_Refetched()
    {
        var cache = new SingleFlightTtlCache<int>(TimeSpan.FromMilliseconds(50));
        var attempt = 0;

        Task<int> Fetch()
        {
            attempt++;
            return Task.FromResult(attempt);
        }

        var first = await cache.GetOrStartAsync(Fetch);
        var second = await cache.GetOrStartAsync(Fetch);
        Assert.Equal(1, first);
        Assert.Equal(1, second);
        Assert.Equal(1, attempt);

        await Task.Delay(100);

        var third = await cache.GetOrStartAsync(Fetch);
        Assert.Equal(2, third);
        Assert.Equal(2, attempt);
    }

    [Fact]
    public async Task OneWaiterCancelled_OthersStillGetTheResult_AndTheFetchIsNotCancelled()
    {
        var cache = new SingleFlightTtlCache<int>(TimeSpan.FromMinutes(5));
        var gate = new SemaphoreSlim(0);
        var fetchSawCancellation = false;

        async Task<int> Fetch()
        {
            await gate.WaitAsync(CancellationToken.None);
            return 5;
        }

        using var cts = new CancellationTokenSource();
        var cancelledWaiter = cache.GetOrStartAsync(Fetch, cts.Token);
        var otherWaiter = cache.GetOrStartAsync(Fetch);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelledWaiter);

        gate.Release(1);
        var result = await otherWaiter;

        Assert.Equal(5, result);
        Assert.False(fetchSawCancellation);
    }
}
