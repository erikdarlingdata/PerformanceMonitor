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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_store_host</c>'s 5-minute shared cache (#4214 round-1 review, Medium 2): pins the four promises the
/// design makes — one live gather however many callers race it, nothing gathered again inside the TTL, a
/// fresh gather once the TTL elapses, and a failed gather never cached. Exercised through
/// <see cref="StoreHostProfileCache"/> directly with a counting gather delegate and the injected clock — no
/// real <see cref="HostProfile"/> source (a store connection, a disk read) is needed for any of it.
/// </summary>
public sealed class StoreHostProfileCacheTests
{
    private static readonly DateTime T0 = new(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>A minimal, valid <see cref="HostProfile"/> — the cache never reads into its fields, only
    /// caches/returns the reference, so the values themselves are arbitrary.</summary>
    private static HostProfile FixtureProfile() => new()
    {
        Platform = "linux",
        IsContainerized = false,
        ProcessorCount = 4,
        Memory = new HostMemoryProfile(8_589_934_592, null, 8_589_934_592, true, "GlobalMemoryStatusEx"),
        DataVolume = new HostDataVolumeProfile(107_374_182_400, 53_687_091_200, "ext4", true),
        IsManagedStore = false,
        Store = new HostStoreFacts("17.4", "2.99.0", 1_000_000, 99.0, 0, 0, 0),
        Settings = Array.Empty<HostSettingProfile>(),
        Cloud = CloudIdentity.None,
    };

    /// <summary>Two callers racing a cold cache cost ONE gather. Deterministic, no <c>Task.Delay</c>: call 1's
    /// gather signals <c>started</c> after incrementing the counter and before parking on <c>release</c>, so
    /// awaiting <c>started</c> proves call 1 already holds the cache's gate; call 2 started against that same
    /// held gate then has a synchronous prefix that runs only as far as its own <c>await</c> on the gate
    /// (<c>SemaphoreSlim.WaitAsync</c> never completes synchronously when the semaphore is already held), so
    /// it is provably parked — not merely fast — before the counter is checked.</summary>
    [Fact]
    public async Task TwoConcurrentCalls_GatherOnce()
    {
        var counter = 0;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5), () => T0);

        async Task<HostProfile> Gather1(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            started.SetResult();
            await release.Task;
            return FixtureProfile();
        }

        Task<HostProfile> Gather2(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            return Task.FromResult(FixtureProfile());
        }

        var call1 = cache.GetOrGatherAsync(Gather1, CancellationToken.None);
        await started.Task;

        var call2 = cache.GetOrGatherAsync(Gather2, CancellationToken.None);

        Assert.False(call2.IsCompleted);
        Assert.Equal(1, counter);

        release.SetResult();
        var results = await Task.WhenAll(call1, call2);

        Assert.Equal(1, counter);
        Assert.Same(results[0].Profile, results[1].Profile);
        Assert.Equal(results[0].GatheredAtUtc, results[1].GatheredAtUtc);
    }

    /// <summary>A call inside the 5-minute TTL is served the cached entry: no second gather, and the same
    /// <c>GatheredAtUtc</c> (the first gather's clock reading) comes back both times.</summary>
    [Fact]
    public async Task ACallInsideFiveMinutes_GathersNothing()
    {
        var counter = 0;
        var now = T0;
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5), () => now);

        Task<HostProfile> Gather(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            return Task.FromResult(FixtureProfile());
        }

        var first = await cache.GetOrGatherAsync(Gather, CancellationToken.None);

        now = now.AddMinutes(4);
        var second = await cache.GetOrGatherAsync(Gather, CancellationToken.None);

        Assert.Equal(1, counter);
        Assert.Same(first.Profile, second.Profile);
        Assert.Equal(first.GatheredAtUtc, second.GatheredAtUtc);
        Assert.Equal(T0, second.GatheredAtUtc);
    }

    /// <summary>A call after the clock has advanced 5 minutes past the cached entry's <c>GatheredAtUtc</c>
    /// gathers again.</summary>
    [Fact]
    public async Task ACallAfterFiveMinutes_GathersAgain()
    {
        var counter = 0;
        var now = T0;
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5), () => now);

        Task<HostProfile> Gather(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            return Task.FromResult(FixtureProfile());
        }

        await cache.GetOrGatherAsync(Gather, CancellationToken.None);

        now = now.AddMinutes(5);
        var second = await cache.GetOrGatherAsync(Gather, CancellationToken.None);

        Assert.Equal(2, counter);
        Assert.Equal(now, second.GatheredAtUtc);
    }

    /// <summary>A throwing gather is never cached: the exception propagates out of the FIRST call untouched
    /// (<see cref="StoreHostProfileCache.GetOrGatherAsync"/> wraps nothing), and the gate's <c>finally</c>
    /// still releases it, so a SECOND call is free to gather again rather than finding a stuck, faulted
    /// cache.</summary>
    [Fact]
    public async Task AFailedGather_IsNotCached()
    {
        var counter = 0;
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5), () => T0);

        Task<HostProfile> ThrowingGather(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            throw new InvalidOperationException("gather failed (test)");
        }

        Task<HostProfile> Gather(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            return Task.FromResult(FixtureProfile());
        }

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => cache.GetOrGatherAsync(ThrowingGather, CancellationToken.None));

        var second = await cache.GetOrGatherAsync(Gather, CancellationToken.None);

        Assert.Equal(2, counter);
        Assert.NotNull(second.Profile);
    }

    /// <summary>#4203: a gather cancelled through the CALLER's own token (<c>get_store_host</c>'s linked
    /// source over <see cref="CancellationTokenSource.CreateLinkedTokenSource(CancellationToken)"/>) is never
    /// cached, the same as <see cref="AFailedGather_IsNotCached"/> proves for any other exception —
    /// <see cref="StoreHostProfileCache.GetOrGatherAsync"/> draws no distinction between the two: it only
    /// assigns its cache entry after a gather call that returns, and <see cref="OperationCanceledException"/>
    /// propagates out of that call exactly like any other throw.</summary>
    [Fact]
    public async Task ACancelledGather_IsNotCached()
    {
        var counter = 0;
        var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5), () => T0);

        Task<HostProfile> CancellingGather(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            throw new OperationCanceledException("gather cancelled (test)");
        }

        Task<HostProfile> Gather(CancellationToken token)
        {
            Interlocked.Increment(ref counter);
            return Task.FromResult(FixtureProfile());
        }

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => cache.GetOrGatherAsync(CancellingGather, CancellationToken.None));

        var second = await cache.GetOrGatherAsync(Gather, CancellationToken.None);

        Assert.Equal(2, counter);
        Assert.NotNull(second.Profile);
    }
}
