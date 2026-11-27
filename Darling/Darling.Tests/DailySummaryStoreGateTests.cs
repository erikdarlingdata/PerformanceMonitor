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
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8824: the daily-summary reader routes on the store's rollup gate, and the coverage probe behind it is the longest statement of
/// a first read on a large store. The gate was kept per data source for five minutes, so the warm pass's probe (the worker's pool)
/// never served a visitor's first read (the web host's pool), and a visit more than five minutes after a pass found nothing fresh
/// even through one pool. The store-keyed gate is shared by every pool over one store and serves an answer past the five minutes
/// for up to an hour while one background probe replaces it. These tests run on fake probes and a moving clock, no store.
/// </summary>
public sealed class DailySummaryStoreGateTests
{
    private static readonly (RollupAvailability Rollups, RollupCoverage Coverage) First = (RollupAvailability.All, RollupCoverage.Unknown);
    private static readonly (RollupAvailability Rollups, RollupCoverage Coverage) Second = (RollupAvailability.None, RollupCoverage.Unknown);

    private static NpgsqlDataSource Pool() => NpgsqlDataSource.Create("Host=unused.invalid;Database=none");

    /* Every test gets its own gates (xUnit builds one instance of this class per test). The service's shared set is cleared by
       DarlingHealthReader.ResetRangeCacheForTests, which the live classes beside this one call at any moment. */
    private readonly ComposeStoreAvailability.StoreGates _gates = new();

    private ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> Read(
        object store, NpgsqlDataSource pool, Probe probe, Func<DateTime> clock)
        => ComposeStoreAvailability.GetMeasuredRollupsForStoreAsync(store, pool, probe.Run, CancellationToken.None, clock, _gates);

    private sealed class Probe
    {
        private int _calls;
        private readonly Func<int, Task<(RollupAvailability Rollups, RollupCoverage Coverage)>> _answer;

        public Probe(Func<int, Task<(RollupAvailability Rollups, RollupCoverage Coverage)>> answer) => _answer = answer;

        public int Calls => Volatile.Read(ref _calls);

        public Task<(RollupAvailability Rollups, RollupCoverage Coverage)> Run(NpgsqlDataSource pool, CancellationToken token)
            => _answer(Interlocked.Increment(ref _calls));
    }

    [Fact]
    public async Task TwoPoolsOverOneStore_ShareOneProbe()
    {
        await using var worker = Pool();
        await using var web = Pool();
        var store = new object();
        var now = new DateTime(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);
        var probe = new Probe(_ => Task.FromResult(First));

        var warmed = await Read(store, worker, probe, () => now);
        var visited = await Read(store, web, probe, () => now.AddMinutes(4));

        Assert.Equal(RollupAvailability.All, warmed.Rollups);
        Assert.Equal(RollupAvailability.All, visited.Rollups);
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public async Task AResetOfTheSharedGatesBetweenTwoReads_DoesNotMakeTheSecondReadProbeAgain()
    {
        await using var worker = Pool();
        await using var web = Pool();
        var store = new object();
        var now = new DateTime(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);
        var probe = new Probe(_ => Task.FromResult(First));

        await Read(store, worker, probe, () => now);

        /* What DarlingHealthReader.ResetRangeCacheForTests does when a live class that runs beside this one calls it. */
        ComposeStoreAvailability.ResetStoreGatesForTests();

        await Read(store, web, probe, () => now.AddMinutes(4));

        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public async Task AnAnswerPastTheReprobeInterval_IsServedAtOnce_AndOneBackgroundProbeReplacesIt()
    {
        await using var worker = Pool();
        await using var web = Pool();
        await using var web2 = Pool();
        var store = new object();
        var now = new DateTime(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);
        var gate = new TaskCompletionSource<(RollupAvailability Rollups, RollupCoverage Coverage)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var probe = new Probe(call => call == 1 ? Task.FromResult(First) : gate.Task);

        await Read(store, worker, probe, () => now);

        /* A visit ten minutes after the pass: older than the five-minute interval, far inside the hour. The probe it starts is held
           open, and the visit still comes back with the answer it has. A second visit beside it starts no second probe. */
        var later = now.AddMinutes(10);
        var visit = Read(store, web, probe, () => later).AsTask();
        var beside = Read(store, web2, probe, () => later).AsTask();
        Assert.True(visit.IsCompleted, "the visit waited for the probe");
        Assert.True(beside.IsCompleted, "the second visit waited for the probe");
        Assert.Equal(RollupAvailability.All, (await visit).Rollups);
        await WaitUntilAsync(() => probe.Calls == 2);
        Assert.Equal(2, probe.Calls);

        /* The probe lands and the next visit routes on its answer. */
        gate.SetResult(Second);
        var routed = RollupAvailability.All;
        for (var i = 0; i < 400 && routed != RollupAvailability.None; i++)
        {
            routed = (await Read(store, web, probe, () => later)).Rollups;
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Equal(RollupAvailability.None, routed);
        Assert.Equal(2, probe.Calls);
    }

    [Fact]
    public async Task AnAnswerPastTheStaleLimit_IsAwaited_NotServed()
    {
        await using var worker = Pool();
        await using var web = Pool();
        var store = new object();
        var now = new DateTime(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);
        var gate = new TaskCompletionSource<(RollupAvailability Rollups, RollupCoverage Coverage)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var probe = new Probe(call => call == 1 ? Task.FromResult(First) : gate.Task);

        await Read(store, worker, probe, () => now);

        var later = now + ComposeStoreAvailability.StaleServeLimit + TimeSpan.FromMinutes(1);
        var visit = Read(store, web, probe, () => later).AsTask();
        await WaitUntilAsync(() => probe.Calls == 2);
        Assert.False(visit.IsCompleted, "an answer older than the limit was served without waiting for the probe");

        gate.SetResult(Second);
        Assert.Equal(RollupAvailability.None, (await visit).Rollups);
    }

    [Fact]
    public async Task AFailedProbe_IsNeitherCachedNorServed()
    {
        await using var worker = Pool();
        await using var web = Pool();
        var store = new object();
        var now = new DateTime(2026, 11, 17, 12, 0, 0, DateTimeKind.Utc);
        var probe = new Probe(call => call <= 2
            ? Task.FromException<(RollupAvailability Rollups, RollupCoverage Coverage)>(new NpgsqlException("57014: canceling statement due to statement timeout"))
            : Task.FromResult(First));

        /* The shared probe fails and the measured accessor probes again, which fails too: the caller's error, and nothing kept. */
        await Assert.ThrowsAsync<NpgsqlException>(() => Read(store, worker, probe, () => now).AsTask());

        var answer = await Read(store, web, probe, () => now);
        Assert.Equal(RollupAvailability.All, answer.Rollups);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var i = 0; i < 400 && !condition(); i++)
        {
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }
}
