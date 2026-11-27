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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9013: the metrics snapshot cache. A scrape never reads the store: it returns the cached snapshot and, when that is
/// old, starts one background refresh. The clock is <see cref="TimerTimeProvider"/>, so the refresh timeout's timer
/// fires only when a test advances it.
/// </summary>
public sealed class DarlingMetricsSnapshotCacheTests
{
    private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

    private readonly TimerTimeProvider _time = new();
    private readonly CapturingTestLogger _log = new();
    private int _calls;

    private DarlingMetricsSnapshotCache NewCache(
        Func<int, CancellationToken, Task<MetricsSnapshot>> refresh, Func<bool>? canRefresh = null) =>
        new(ct => refresh(Interlocked.Increment(ref _calls), ct), canRefresh ?? (() => true), _time, _log, CancellationToken.None);

    private MetricsSnapshot Fresh() => new(_time.GetUtcNow().UtcDateTime, Array.Empty<MetricSample>(), Array.Empty<ServerCollectionStamp>());

    private Task<MetricsSnapshot> FreshAsync(int call, CancellationToken ct) => Task.FromResult(Fresh());

    private static async Task SettleAsync(DarlingMetricsSnapshotCache cache)
    {
        var inFlight = cache.InFlight;
        if (inFlight is not null)
        {
            await inFlight.WaitAsync(s_wait);
        }
    }

    [Fact]
    public async Task FirstScrape_HasNoSnapshot_AndStartsOneRefresh()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = NewCache(async (call, ct) =>
        {
            await release.Task;
            return Fresh();
        });

        var first = cache.Scrape();
        Assert.Null(first.Snapshot);
        Assert.False(first.ServeServers);
        Assert.False(first.LastRefreshSucceeded);
        Assert.Null(first.LastRefreshSeconds);
        Assert.NotNull(cache.InFlight);

        TestWait.WaitUntil(() => Volatile.Read(ref _calls) == 1, s_wait);
        cache.Scrape();
        cache.Scrape();

        release.SetResult();
        await SettleAsync(cache);

        Assert.Equal(1, Volatile.Read(ref _calls));
        var after = cache.Scrape();
        Assert.NotNull(after.Snapshot);
        Assert.True(after.ServeServers);
        Assert.True(after.LastRefreshSucceeded);
        Assert.Equal(0d, after.LastRefreshSeconds);
    }

    [Fact]
    public async Task WarmSnapshot_UnderRefreshAge_StartsNoRefresh()
    {
        var cache = NewCache(FreshAsync);
        cache.Scrape();
        await SettleAsync(cache);

        _time.Advance(TimeSpan.FromSeconds(10));
        for (var i = 0; i < 5; i++)
        {
            Assert.NotNull(cache.Scrape().Snapshot);
        }

        Assert.Null(cache.InFlight);
        Assert.Equal(1, Volatile.Read(ref _calls));
    }

    [Fact]
    public async Task AtRefreshAge_OneRefresh_ForManyConcurrentScrapes()
    {
        var cache = NewCache(FreshAsync);
        cache.Scrape();
        await SettleAsync(cache);
        Assert.Equal(1, Volatile.Read(ref _calls));

        _time.Advance(DarlingMetricsSnapshotCache.RefreshAge);
        Parallel.For(0, 50, new ParallelOptions { MaxDegreeOfParallelism = 50 }, _ => cache.Scrape());
        await SettleAsync(cache);

        Assert.Equal(2, Volatile.Read(ref _calls));
        Assert.Equal(_time.GetUtcNow().UtcDateTime, cache.Scrape().Snapshot!.BuiltAtUtc);
        Assert.Null(cache.InFlight);
    }

    [Fact]
    public async Task AClockSteppedBack_DoesNotFreezeTheSnapshot_AndNeverWritesANegativeAge()
    {
        var cache = NewCache(FreshAsync);
        cache.Scrape();
        await SettleAsync(cache);
        var before = cache.Scrape().Snapshot!;

        _time.Advance(TimeSpan.FromMinutes(-10));
        var stepped = cache.Scrape();
        var text = PrometheusText.Write(stepped, _time.GetUtcNow().UtcDateTime);

        Assert.Contains("darling_metrics_snapshot_age_seconds 0\n", text, StringComparison.Ordinal);
        Assert.False(stepped.ServeServers);
        await SettleAsync(cache);
        Assert.Equal(2, Volatile.Read(ref _calls));
        var after = cache.Scrape();
        Assert.NotSame(before, after.Snapshot);
        Assert.Equal(_time.GetUtcNow().UtcDateTime, after.Snapshot!.BuiltAtUtc);
        Assert.True(after.ServeServers);
    }

    [Fact]
    public async Task Scrape_DuringAHungRefresh_AnswersAtOnce_AndStartsNoSecond()
    {
        var hung = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = NewCache(async (call, ct) =>
        {
            if (call > 1)
            {
                await hung.Task;
            }

            return Fresh();
        });
        cache.Scrape();
        await SettleAsync(cache);
        var warm = cache.Scrape().Snapshot!;

        _time.Advance(DarlingMetricsSnapshotCache.RefreshAge);
        var during = cache.Scrape();
        TestWait.WaitUntil(() => Volatile.Read(ref _calls) == 2, s_wait);
        var inFlight = cache.InFlight;
        Assert.NotNull(inFlight);
        Assert.Same(warm, during.Snapshot);

        _time.Advance(TimeSpan.FromSeconds(65));
        var late = cache.Scrape();
        Assert.Same(warm, late.Snapshot);
        Assert.True(late.ServeServers);
        Assert.Same(inFlight, cache.InFlight);
        Assert.Equal(2, Volatile.Read(ref _calls));

        hung.SetResult();
        await SettleAsync(cache);
        Assert.Equal(2, Volatile.Read(ref _calls));
    }

    [Fact]
    public async Task Timeout_IsAFailure_WithOneWarning()
    {
        var cache = NewCache(async (call, ct) =>
        {
            if (call == 1)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }

            return Fresh();
        });

        cache.Scrape();
        TestWait.WaitUntil(() => Volatile.Read(ref _calls) == 1, s_wait);
        _time.Advance(DarlingMetricsSnapshotCache.RefreshTimeout);
        await SettleAsync(cache);

        var scrape = cache.Scrape();
        await SettleAsync(cache);
        Assert.Null(scrape.Snapshot);
        Assert.False(scrape.LastRefreshSucceeded);
        Assert.Equal(DarlingMetricsSnapshotCache.RefreshTimeout.TotalSeconds, scrape.LastRefreshSeconds);
        Assert.Equal(1, _log.CountAtLevel(LogLevel.Warning));
        Assert.Contains(_log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("timed out", StringComparison.Ordinal));
    }

    [Fact]
    public async Task FailureStreak_LogsOnce_RecoveryLogsOnce()
    {
        var cache = NewCache((call, ct) => call <= 3
            ? Task.FromException<MetricsSnapshot>(new InvalidOperationException("store said no"))
            : Task.FromResult(Fresh()));

        for (var i = 0; i < 3; i++)
        {
            cache.Scrape();
            await SettleAsync(cache);
            _time.Advance(DarlingMetricsSnapshotCache.RefreshAge);
        }

        var failed = cache.Scrape();
        Assert.False(failed.LastRefreshSucceeded);
        Assert.NotNull(failed.LastRefreshSeconds);
        await SettleAsync(cache);

        Assert.Equal(4, Volatile.Read(ref _calls));
        Assert.Equal(1, _log.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, _log.CountAtLevel(LogLevel.Information));
        Assert.Contains(_log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
            && l.Contains("InvalidOperationException", StringComparison.Ordinal)
            && l.Contains("store said no", StringComparison.Ordinal));
        Assert.Empty(_log.ExceptionsAtLevel(LogLevel.Warning));
        Assert.True(cache.Scrape().LastRefreshSucceeded);
    }

    [Fact]
    public async Task OldSnapshot_IsServedUntilDropAge_ThenServersAreDropped()
    {
        var cache = NewCache((call, ct) => call == 1
            ? Task.FromResult(Fresh())
            : Task.FromException<MetricsSnapshot>(new InvalidOperationException("store down")));
        cache.Scrape();
        await SettleAsync(cache);
        var warm = cache.Scrape().Snapshot!;

        _time.Advance(DarlingMetricsSnapshotCache.DropAge - TimeSpan.FromSeconds(1));
        var nearly = cache.Scrape();
        await SettleAsync(cache);
        Assert.Same(warm, nearly.Snapshot);
        Assert.True(nearly.ServeServers);

        _time.Advance(TimeSpan.FromSeconds(1));
        var dropped = cache.Scrape();
        await SettleAsync(cache);
        Assert.Same(warm, dropped.Snapshot);
        Assert.False(dropped.ServeServers);
        Assert.False(dropped.LastRefreshSucceeded);
    }

    [Fact]
    public async Task AfterAFailure_TheNextRefresh_WaitsRefreshAge()
    {
        var cache = NewCache((call, ct) => call == 1
            ? Task.FromResult(Fresh())
            : Task.FromException<MetricsSnapshot>(new InvalidOperationException("store down")));
        cache.Scrape();
        await SettleAsync(cache);

        _time.Advance(DarlingMetricsSnapshotCache.RefreshAge);
        cache.Scrape();
        await SettleAsync(cache);
        Assert.Equal(2, Volatile.Read(ref _calls));
        Assert.False(cache.Scrape().LastRefreshSucceeded);

        for (var i = 0; i < 3; i++)
        {
            _time.Advance(TimeSpan.FromSeconds(15));
            cache.Scrape();
            Assert.Null(cache.InFlight);
            Assert.Equal(2, Volatile.Read(ref _calls));
        }

        _time.Advance(TimeSpan.FromSeconds(10));
        cache.Scrape();
        await SettleAsync(cache);
        Assert.Equal(3, Volatile.Read(ref _calls));
    }

    [Fact]
    public async Task StoppingCancellation_LogsNothing_AndStartsNoStreak()
    {
        using var stopping = new CancellationTokenSource();
        var cache = new DarlingMetricsSnapshotCache(
            async ct =>
            {
                Interlocked.Increment(ref _calls);
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                return Fresh();
            },
            () => true,
            _time,
            _log,
            stopping.Token);

        cache.Scrape();
        TestWait.WaitUntil(() => Volatile.Read(ref _calls) == 1, s_wait);
        stopping.Cancel();
        await SettleAsync(cache);

        var scrape = cache.Scrape();
        Assert.Equal(0, _log.CountAtLevel(LogLevel.Warning));
        Assert.Null(scrape.Snapshot);
        Assert.False(scrape.LastRefreshSucceeded);
        Assert.Null(scrape.LastRefreshSeconds);
    }

    [Fact]
    public void CanRefreshFalse_StartsNothing()
    {
        var cache = NewCache(FreshAsync, canRefresh: () => false);

        for (var i = 0; i < 3; i++)
        {
            var scrape = cache.Scrape();
            Assert.Null(scrape.Snapshot);
            Assert.False(scrape.ServeServers);
        }

        Assert.Null(cache.InFlight);
        Assert.Equal(0, Volatile.Read(ref _calls));
    }
}
