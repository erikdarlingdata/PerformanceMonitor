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
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The per-physical-connection session id cache (#5132): the unit halves run anywhere, the live halves need a
/// real SQL Server (DARLING_TEST_SQL, optional _USER / _PASSWORD) and a store (DARLING_TEST_PG) and skip
/// when those are unset, as CI does.
/// </summary>
public sealed class TargetSessionIdCacheTests
{
    [Fact]
    public async Task TheSameConnectionIdIsLookedUpOnce()
    {
        var cache = new TargetSessionIdCache(8);
        var id = Guid.NewGuid();
        var calls = 0;
        Task<int> Lookup(CancellationToken _) { calls++; return Task.FromResult(57); }

        Assert.Equal(57, (await cache.ResolveAsync(id, Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(57, (await cache.ResolveAsync(id, Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(1, calls);
        Assert.Equal(1, cache.LookupCount);
    }

    [Fact]
    public async Task ADifferentConnectionIdIsANewLookup()
    {
        var cache = new TargetSessionIdCache(8);
        var calls = 0;
        Task<int> Lookup(CancellationToken _) { calls++; return Task.FromResult(50 + calls); }

        Assert.Equal(51, (await cache.ResolveAsync(Guid.NewGuid(), Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(52, (await cache.ResolveAsync(Guid.NewGuid(), Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task TheBoundIsHonoured_AndTheOldestEntryIsEvicted()
    {
        var cache = new TargetSessionIdCache(3);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        var calls = 0;
        Task<int> Lookup(CancellationToken _) { calls++; return Task.FromResult(60 + calls); }

        foreach (var id in ids)
        {
            await cache.ResolveAsync(id, Lookup, null, TestContext.Current.CancellationToken);
        }

        Assert.Equal(3, cache.Count);
        Assert.Equal(4, calls);

        /* The newest three still hit; the first was evicted and asks again. */
        await cache.ResolveAsync(ids[3], Lookup, null, TestContext.Current.CancellationToken);
        Assert.Equal(4, calls);
        await cache.ResolveAsync(ids[0], Lookup, null, TestContext.Current.CancellationToken);
        Assert.Equal(5, calls);
        Assert.Equal(3, cache.Count);
        Assert.Equal(4096, TargetSessionIdCache.Capacity);
    }

    [Fact]
    public async Task AFailedLookupIsNullAndNotCached()
    {
        var cache = new TargetSessionIdCache(8);
        var id = Guid.NewGuid();

        Assert.Null((await cache.ResolveAsync(id, _ => throw new InvalidOperationException("boom"), null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(0, cache.Count);

        /* A non-positive answer is not a session id either. */
        Assert.Null((await cache.ResolveAsync(id, _ => Task.FromResult(0), null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(0, cache.Count);

        /* The next run tries again and succeeds. */
        Assert.Equal(44, (await cache.ResolveAsync(id, _ => Task.FromResult(44), null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(3, cache.LookupCount);
    }

    [Fact]
    public async Task AnEmptyConnectionIdIsNeverLookedUp()
    {
        var cache = new TargetSessionIdCache(8);
        Assert.Null((await cache.ResolveAsync(Guid.Empty, _ => Task.FromResult(9), null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(0, cache.LookupCount);
    }

    [Fact]
    public async Task AClosedConnection_HasAnEmptyConnectionId_SoTheLookupIsSkipped()
    {
        var cache = new TargetSessionIdCache(8);
        using var connection = new SqlConnection("Server=127.0.0.1,1;Connect Timeout=1;Encrypt=false");
        Assert.Null((await cache.ResolveAsync(connection, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.LookupCount);
    }

    [Fact]
    public async Task ARealCancellationPropagates_AndTheCacheIsUnchanged()
    {
        var cache = new TargetSessionIdCache(8);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await cache.ResolveAsync(Guid.NewGuid(), ct => { ct.ThrowIfCancellationRequested(); return Task.FromResult(5); }, null, cts.Token));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task ALookupTimeout_WithoutACancelledToken_IsStillNullNotAThrow()
    {
        var cache = new TargetSessionIdCache(8);
        var result = await cache.ResolveAsync(Guid.NewGuid(), _ => throw new OperationCanceledException(), null, TestContext.Current.CancellationToken);
        Assert.Null(result.Spid);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task TheAnswerIsFiledUnderTheConnectionIdReadAfterTheLookup()
    {
        var cache = new TargetSessionIdCache(8);
        var before = Guid.NewGuid();
        var after = Guid.NewGuid();

        var first = await cache.ResolveAsync(before, _ => Task.FromResult(61), null, TestContext.Current.CancellationToken, () => after);
        Assert.Equal(61, first.Spid);
        Assert.Equal(after, first.Key);
        Assert.Equal(1, cache.Count);

        /* The old id has no entry (a lookup is issued again); the new id hits. */
        var calls = 0;
        Task<int> Lookup(CancellationToken _) { calls++; return Task.FromResult(99); }
        Assert.Equal(61, (await cache.ResolveAsync(after, Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(0, calls);
        Assert.Equal(99, (await cache.ResolveAsync(before, Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AnEvictedConnectionIdAsksAgain_AndAReplacedConnectionIsDiscarded()
    {
        var cache = new TargetSessionIdCache(2);
        var id = Guid.NewGuid();
        var other = Guid.NewGuid();
        var calls = 0;
        Task<int> Lookup(CancellationToken _) { calls++; return Task.FromResult(70 + calls); }

        await cache.ResolveAsync(id, Lookup, null, TestContext.Current.CancellationToken);
        await cache.ResolveAsync(other, Lookup, null, TestContext.Current.CancellationToken);

        Assert.False(cache.DiscardIfReplaced(id, id));
        Assert.False(cache.DiscardIfReplaced(Guid.Empty, id));
        Assert.Equal(2, cache.Count);

        Assert.True(cache.DiscardIfReplaced(id, Guid.NewGuid()));
        Assert.Equal(1, cache.Count);
        Assert.Equal(73, (await cache.ResolveAsync(id, Lookup, null, TestContext.Current.CancellationToken)).Spid);
        Assert.Equal(3, calls);

        /* The queue stays consistent with the dictionary: the bound still holds after an eviction. */
        await cache.ResolveAsync(Guid.NewGuid(), Lookup, null, TestContext.Current.CancellationToken);
        Assert.Equal(2, cache.Count);
    }

    internal static MonitoredServer LiveServer(string host)
    {
        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        return new MonitoredServer
        {
            Name = "darling-spid-e2e",
            Host = host,
            Auth = string.IsNullOrEmpty(user) ? "integrated" : "sql",
            Username = user,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };
    }

    [Fact]
    public async Task Live_TheProductConnection_ReturnsTheTrueSpid_AndAPooledReopenIssuesNoSecondLookup()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(host), "Set DARLING_TEST_SQL to run the live session id check.");
        var ct = TestContext.Current.CancellationToken;

        var connectionString = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(LiveServer(host!), Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD")))
        {
            Pooling = true,
            MaxPoolSize = 1,
        }.ConnectionString;
        Assert.True(new SqlConnectionStringBuilder(connectionString).MultipleActiveResultSets, "the product string is MARS");

        var cache = new TargetSessionIdCache(8);
        int first;
        Guid firstId;
        await using (var one = new SqlConnection(connectionString))
        {
            await one.OpenAsync(ct);
            Assert.Equal(0, one.ServerProcessId);
            first = ((await cache.ResolveAsync(one, null, ct)).Spid).GetValueOrDefault();
            firstId = one.ClientConnectionId;

            using var truth = new SqlCommand("SELECT @@SPID;", one);
            Assert.Equal((int)(short)first, Convert.ToInt32(await truth.ExecuteScalarAsync(ct)));
        }

        Assert.True(first > 0);
        Assert.Equal(1, cache.LookupCount);

        await using (var two = new SqlConnection(connectionString))
        {
            await two.OpenAsync(ct);
            Assert.Equal(firstId, two.ClientConnectionId);
            Assert.Equal(first, (await cache.ResolveAsync(two, null, ct)).Spid);
        }

        Assert.Equal(1, cache.LookupCount);
        SqlConnection.ClearAllPools();
    }
}
