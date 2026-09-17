/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pure in-memory logic — no store, no connection, so unlike <see cref="QueryStorePlanFetchTests"/>'s
/// probe-shape pins these run the real behavior, not just the SQL shape. #3189 measured the store
/// probe at 52.7% of query_store's wall time re-examining references that are, in large majority,
/// already known; #3216 fixed the touch guard's write volume but left the probe's own SELECT
/// unconditional (see <see cref="QueryStoreProbeCache"/>'s doc for the post-#3216 remeasurement). These
/// pin the memoization that answers it: what counts as fresh, what forces a miss, and that a bad
/// verdict is never cached.
/// </summary>
public sealed class QueryStoreProbeCacheTests
{
    private static readonly DateTime T0 = new(2026, 9, 12, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(6);

    [Fact]
    public void IsFresh_FalseForNeverConfirmedReference()
    {
        var cache = new QueryStoreProbeCache();

        Assert.False(cache.IsFresh(serverId: 1, databaseName: "db", id: 100, hash: "h1", nowUtc: T0, ttl: Ttl));
    }

    [Fact]
    public void Confirm_ThenIsFresh_TrueImmediatelyAndJustBeforeTtl()
    {
        var cache = new QueryStoreProbeCache();
        cache.Confirm(serverId: 1, databaseName: "db", id: 100, hash: "h1", nowUtc: T0);

        Assert.True(cache.IsFresh(1, "db", 100, "h1", T0, Ttl));
        Assert.True(cache.IsFresh(1, "db", 100, "h1", T0 + Ttl - TimeSpan.FromSeconds(1), Ttl));
    }

    [Fact]
    public void IsFresh_FalseAtAndAfterTtlExpiry()
    {
        var cache = new QueryStoreProbeCache();
        cache.Confirm(serverId: 1, databaseName: "db", id: 100, hash: "h1", nowUtc: T0);

        /* Strict "<" in IsFresh: a confirmation exactly TTL old must re-probe, not ride one instant
           longer than the width it was given. */
        Assert.False(cache.IsFresh(1, "db", 100, "h1", T0 + Ttl, Ttl));
        Assert.False(cache.IsFresh(1, "db", 100, "h1", T0 + Ttl + TimeSpan.FromHours(1), Ttl));
    }

    [Fact]
    public void IsFresh_FalseWhenHashDiffers_InPlaceRewriteIsAlwaysAMiss()
    {
        /* The correctness argument for keying on hash: a plan/text rewritten in place keeps its id but
           changes its content, and a stale-hash cache entry must never answer for the NEW hash — it has
           to fall through to a real probe, which is what actually detects HashStale and refetches. */
        var cache = new QueryStoreProbeCache();
        cache.Confirm(serverId: 1, databaseName: "db", id: 100, hash: "h1", nowUtc: T0);

        Assert.False(cache.IsFresh(1, "db", 100, "h2", T0, Ttl));
    }

    [Fact]
    public void IsFresh_ScopedToServerAndDatabase_SameIdElsewhereIsAMiss()
    {
        var cache = new QueryStoreProbeCache();
        cache.Confirm(serverId: 1, databaseName: "db", id: 100, hash: "h1", nowUtc: T0);

        Assert.False(cache.IsFresh(serverId: 2, databaseName: "db", id: 100, hash: "h1", nowUtc: T0, ttl: Ttl));
        Assert.False(cache.IsFresh(serverId: 1, databaseName: "other", id: 100, hash: "h1", nowUtc: T0, ttl: Ttl));
    }

    [Fact]
    public void IsFresh_NullHashIsAValidKey_DistinctFromAnyRealHash()
    {
        /* Legacy rows or content-less markers can carry a null hash on the store side; the cache must
           treat null as its own key, not as "unknown, always miss" or "matches anything". */
        var cache = new QueryStoreProbeCache();
        cache.Confirm(serverId: 1, databaseName: "db", id: 100, hash: null, nowUtc: T0);

        Assert.True(cache.IsFresh(1, "db", 100, null, T0, Ttl));
        Assert.False(cache.IsFresh(1, "db", 100, "h1", T0, Ttl));
    }

    [Fact]
    public void SelectNeedingProbe_ExcludesFreshEntries_KeepsMissesAndExpired()
    {
        var cache = new QueryStoreProbeCache();
        cache.Confirm(serverId: 1, databaseName: "db", id: 1, hash: "h1", nowUtc: T0);                 // fresh -> excluded
        cache.Confirm(serverId: 1, databaseName: "db", id: 2, hash: "h2", nowUtc: T0 - Ttl);            // expired -> included

        var references = new List<(long Id, string? Hash)>
        {
            (1, "h1"),   // cache hit
            (2, "h2"),   // expired
            (3, "h3"),   // never seen
            (1, "h9"),   // same id, different hash -> miss despite id 1 being cached under h1
        };

        var needingProbe = cache.SelectNeedingProbe(serverId: 1, databaseName: "db", references, nowUtc: T0, ttl: Ttl);

        Assert.Equal(3, needingProbe.Count);
        Assert.DoesNotContain((1L, "h1"), needingProbe);
        Assert.Contains((2L, "h2"), needingProbe);
        Assert.Contains((3L, "h3"), needingProbe);
        Assert.Contains((1L, "h9"), needingProbe);
    }

    [Fact]
    public void SelectNeedingProbe_EmptyWhenEverythingIsFresh()
    {
        var cache = new QueryStoreProbeCache();
        cache.Confirm(1, "db", 1, "h1", T0);
        cache.Confirm(1, "db", 2, "h2", T0);

        var needingProbe = cache.SelectNeedingProbe(
            1, "db", new List<(long Id, string? Hash)> { (1, "h1"), (2, "h2") }, T0, Ttl);

        Assert.Empty(needingProbe);
    }

    [Fact]
    public void SelectNeedingProbe_AllWhenCacheIsEmpty()
    {
        var cache = new QueryStoreProbeCache();
        var references = new List<(long Id, string? Hash)> { (1, "h1"), (2, "h2"), (3, null) };

        var needingProbe = cache.SelectNeedingProbe(1, "db", references, T0, Ttl);

        Assert.Equal(references, needingProbe);
    }

    [Fact]
    public void Confirm_Overwrites_LaterConfirmationExtendsFreshness()
    {
        var cache = new QueryStoreProbeCache();
        cache.Confirm(1, "db", 1, "h1", T0);
        cache.Confirm(1, "db", 1, "h1", T0 + TimeSpan.FromHours(3));

        /* Had the first confirmation not been overwritten, this instant would already be past the
           original T0-anchored TTL. */
        Assert.True(cache.IsFresh(1, "db", 1, "h1", T0 + Ttl - TimeSpan.FromMinutes(1), Ttl));
    }
}
