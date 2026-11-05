/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7644: the web Overview's baseline read shares the worker's compute and does not throw a started compute away.
/// Nothing here needs a store: the key tests seed the process's shared tier the way the worker's pass fills it, over a data
/// source that cannot connect, so a read that misses the shared tier answers "no baseline" and one that hits it answers the
/// seeded bucket. The compute tests hand the provider's single-flight a stand-in compute.
/// </summary>
public sealed class OverviewBaselineSharedComputeTests
{
    /* Wednesday 2026-11-04 06:30 UTC is "now"; the page's 24-hour window starts Tuesday 06:30, which is YESTERDAY's UTC day.
       The worker's pass asked at 06:10 today. */
    private static readonly DateTime Now = new(2026, 11, 4, 6, 30, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = Now.AddHours(-24);
    private static readonly DateTime WorkerAnalysisTime = new(2026, 11, 4, 6, 10, 0, DateTimeKind.Utc);
    private const int ServerId = 7;

    private static readonly string[] DailyArms =
    [
        MetricNames.Cpu, MetricNames.IoLatency, MetricNames.IoWriteLatency, MetricNames.Blocking, MetricNames.Deadlock,
    ];

    private static string KindOf(PgBaselineProvider provider) => provider.GetType().FullName!;

    private static PgBaselineProvider.CachedBaseline EntryAt(DateTime roundedKey, double mean) => new()
    {
        ComputedAt = roundedKey,
        RealTime = DateTime.UtcNow,
        FreshUntilUtc = DateTime.UtcNow.AddDays(1),
        Clock = LocalClockWindow.Utc(roundedKey),
        /* Tuesday 06:00, the bucket the window start falls in. */
        Buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>
        {
            [(6, 2)] = new BaselineBucket
            {
                HourOfDay = 6, DayOfWeek = 2, Mean = mean, StdDev = 5, SampleCount = 500, DistinctDays = 20, Tier = BaselineTier.Full, Median = mean, Mad = 3,
            },
        },
    };

    private static NpgsqlDataSource Unreachable() => NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1;Command Timeout=1");

    [Theory]
    [InlineData(MetricNames.Cpu)]
    [InlineData(MetricNames.IoLatency)]
    [InlineData(MetricNames.IoWriteLatency)]
    [InlineData(MetricNames.Blocking)]
    [InlineData(MetricNames.Deadlock)]
    [InlineData(MetricNames.WaitMsPerSec)]
    [InlineData(MetricNames.BlockingPerMinute)]
    public async Task AWebReadAt24HourWindowStart_HitsTheEntryTheWorkersPassStored(string metric)
    {
        await using var postgres = Unreachable();
        var shared = new BaselineCache();
        var provider = new PgBaselineProvider(postgres, null, shared);

        /* The worker's pass stored its compute under its own analysis time's key (the day for a daily-cache arm, the hour for the others). */
        var workerKey = provider.RoundedKeyTime(metric, WorkerAnalysisTime);
        shared.Put(KindOf(provider), ServerId, PgBaselineProvider.CacheKeyFor(ServerId, metric, key: null), EntryAt(workerKey, mean: 42));

        var bucket = await provider.GetBaselineSharedAsync(ServerId, metric, WindowStart, Now, CancellationToken.None);

        Assert.Equal(42, bucket.Mean);
    }

    [Fact]
    public async Task TheOverviewLanes_AllResolveToTheWorkersEntries()
    {
        await using var postgres = Unreachable();
        var shared = new BaselineCache();
        var provider = new PgBaselineProvider(postgres, null, shared);
        foreach (var metric in new[] { MetricNames.Cpu, MetricNames.WaitMsPerSec, MetricNames.IoLatency, MetricNames.BlockingPerMinute })
        {
            shared.Put(KindOf(provider), ServerId, PgBaselineProvider.CacheKeyFor(ServerId, metric, key: null),
                EntryAt(provider.RoundedKeyTime(metric, WorkerAnalysisTime), mean: 42));
        }

        var buckets = await DarlingWebOverviewBaselines.ReadLaneBucketsAsync(provider, ServerId, WindowStart, Now, CancellationToken.None);

        Assert.Equal(4, buckets.Length);
        Assert.All(buckets, b => Assert.Equal(42, b.Mean));
    }

    [Fact]
    public void EveryDailyCacheArm_KeysOnTheDay_SoNowAndTheWorkersAnalysisTimeShareOneKey()
    {
        var provider = new PgBaselineProvider(Unreachable());
        foreach (var metric in DailyArms)
        {
            Assert.Equal(provider.RoundedKeyTime(metric, WorkerAnalysisTime), provider.RoundedKeyTime(metric, Now));
            Assert.NotEqual(provider.RoundedKeyTime(metric, WorkerAnalysisTime), provider.RoundedKeyTime(metric, WindowStart));
        }
    }

    private static PgBaselineProvider.CachedBaseline Computed(DateTime roundedKey) => EntryAt(roundedKey, mean: 10);

    [Fact]
    public async Task TwoConcurrentRequestsForOneKey_RunOneCompute()
    {
        await using var postgres = Unreachable();
        var provider = new PgBaselineProvider(postgres);
        var key = provider.RoundedKeyTime(MetricNames.IoLatency, Now);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var computes = 0;
        async Task<PgBaselineProvider.CachedBaseline> Compute(CancellationToken ct)
        {
            Interlocked.Increment(ref computes);
            await gate.Task;
            return Computed(key);
        }

        var first = provider.GetOrComputeSharedAsync(ServerId, "k", key, Compute, TimeSpan.FromMinutes(1), CancellationToken.None);
        var second = provider.GetOrComputeSharedAsync(ServerId, "k", key, Compute, TimeSpan.FromMinutes(1), CancellationToken.None);
        gate.SetResult();
        var results = await Task.WhenAll(first, second);

        Assert.Equal(1, computes);
        Assert.NotNull(results[0]);
        Assert.Same(results[0], results[1]);
    }

    [Fact]
    public async Task ARequestCancelledMidCompute_LeavesTheComputeRunning_AndTheResultCachedForTheNext()
    {
        await using var postgres = Unreachable();
        var provider = new PgBaselineProvider(postgres);
        var key = provider.RoundedKeyTime(MetricNames.IoLatency, Now);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var computes = 0;
        var ownTokenCancelled = false;
        async Task<PgBaselineProvider.CachedBaseline> Compute(CancellationToken ct)
        {
            Interlocked.Increment(ref computes);
            started.SetResult();
            /* A real compute's connection and command stop on their token. */
            await gate.Task.WaitAsync(ct);
            ownTokenCancelled = ct.IsCancellationRequested;
            return Computed(key);
        }

        using var browser = new CancellationTokenSource();
        var aborted = provider.GetOrComputeSharedAsync(ServerId, "k", key, Compute, TimeSpan.FromMinutes(1), browser.Token);
        await started.Task;
        browser.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => aborted);

        /* The reload joins the compute that is still running. */
        var reload = provider.GetOrComputeSharedAsync(ServerId, "k", key, Compute, TimeSpan.FromMinutes(1), CancellationToken.None);
        gate.SetResult();
        var entry = await reload;
        Assert.NotNull(entry);
        Assert.False(ownTokenCancelled, "the browser's abort cancelled the compute");

        /* And once it has finished, the next request is a cache hit, not a compute. */
        var later = await provider.GetOrComputeSharedAsync(ServerId, "k", key, Compute, TimeSpan.FromMinutes(1), CancellationToken.None);
        Assert.Same(entry, later);
        Assert.Equal(1, computes);
    }

    [Fact]
    public async Task AComputeThatOutrunsItsOwnTimeout_AnswersNoBaseline_AndTheNextRequestStartsAgain()
    {
        await using var postgres = Unreachable();
        var provider = new PgBaselineProvider(postgres);
        var key = provider.RoundedKeyTime(MetricNames.IoLatency, Now);
        var computes = 0;
        async Task<PgBaselineProvider.CachedBaseline> Hangs(CancellationToken ct)
        {
            Interlocked.Increment(ref computes);
            await Task.Delay(Timeout.Infinite, ct);
            return Computed(key);
        }

        var first = await provider.GetOrComputeSharedAsync(ServerId, "k", key, Hangs, TimeSpan.FromMilliseconds(50), CancellationToken.None);
        var second = await provider.GetOrComputeSharedAsync(ServerId, "k", key, Hangs, TimeSpan.FromMilliseconds(50), CancellationToken.None);

        Assert.Null(first);
        Assert.Null(second);
        Assert.Equal(2, computes);
    }
}
