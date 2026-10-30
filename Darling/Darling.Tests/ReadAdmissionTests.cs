/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #5506 part 3: read admission. At most the limit of heavy reads run at once; more wait oldest first up to the queue
/// limit and the wait cap, and past that a read is refused. Light reads never wait, and a read nested in a flow that holds
/// a slot takes no second one. Unit-level: the admission object alone, with a clock the test moves.
/// </summary>
public sealed class ReadAdmissionTests
{
    private const string HeavyRoute = "heavy_probe";
    private const string LightRoute = "light_probe";

    private static ReadAdmission Build(int limit, TimeSpan? waitCap = null, TimeProvider? time = null)
    {
        var admission = new ReadAdmission(time ?? TimeProvider.System, waitCap ?? ReadAdmission.DefaultWaitCap, null, limit);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long>
        {
            [(ReadSurface.Mcp, HeavyRoute, ReadWindowBand.None)] = 3_000,
            [(ReadSurface.Mcp, LightRoute, ReadWindowBand.SevenDays)] = 1_999,
        });
        return admission;
    }

    /* Each admission runs in a flow of its own, as two requests do: a slot held by the test's own flow would make every
       later call from it a nested one. */
    private static Task<ReadAdmissionTicket> AdmitApart(ReadAdmission admission, string route, string band = ReadWindowBand.None, CancellationToken cancellationToken = default) =>
        Task.Run(() => admission.AdmitAsync(ReadSurface.Mcp, route, band, cancellationToken));

    private static async Task WaitUntil(Func<bool> condition)
    {
        for (var i = 0; i < 500 && !condition(); i++)
        {
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.True(condition(), "The admission did not reach the expected state in 5 seconds.");
    }

    [Fact]
    public async Task AtLimitTwo_AThirdHeavyRead_WaitsThenRuns_WithItsQueueTime()
    {
        var admission = Build(2);
        var first = await AdmitApart(admission, HeavyRoute);
        var second = await AdmitApart(admission, HeavyRoute);
        Assert.True(first.MayRun);
        Assert.Null(first.QueueMs);
        Assert.Equal(2, admission.Stats().Running);

        var third = AdmitApart(admission, HeavyRoute);
        await WaitUntil(() => admission.Stats().Queued == 1);
        Assert.False(third.IsCompleted);

        first.Dispose();
        var admitted = await third;
        Assert.True(admitted.MayRun);
        Assert.NotNull(admitted.QueueMs);

        var route = Assert.Single(admission.Stats().Routes);
        Assert.Equal(3, route.Admitted);
        Assert.Equal(1, route.Queued);
        Assert.Equal(0, route.Rejected);

        second.Dispose();
        admitted.Dispose();
        Assert.Equal(0, admission.Stats().Running);
    }

    [Fact]
    public async Task AFullQueue_RefusesWithTheSentenceAndTheCounts()
    {
        var admission = Build(2);
        var held = new List<ReadAdmissionTicket> { await AdmitApart(admission, HeavyRoute), await AdmitApart(admission, HeavyRoute) };
        var waiting = new List<Task<ReadAdmissionTicket>>();
        for (var i = 0; i < ReadAdmission.QueueLimit; i++)
        {
            waiting.Add(AdmitApart(admission, HeavyRoute));
            var expected = i + 1;
            await WaitUntil(() => admission.Stats().Queued == expected);
        }

        var refused = await AdmitApart(admission, HeavyRoute);
        Assert.False(refused.MayRun);
        var refusal = Assert.IsType<ReadAdmissionRefusal>(refused.Refusal);
        Assert.Equal(2, refusal.Running);
        Assert.Equal(ReadAdmission.QueueLimit, refusal.Queued);

        /* 18 reads ahead at a p90 of 3 s over a limit of 2: 27 seconds. */
        Assert.Equal(27, refusal.RetryAfterSeconds);
        Assert.Equal(
            "The service is running as many long reads as it allows (2), and 16 more are waiting. Try again in about 27 seconds.",
            refusal.Sentence);
        Assert.Equal(1, admission.Stats().Routes.Single().Rejected);

        foreach (var ticket in held)
        {
            ticket.Dispose();
        }

        foreach (var task in waiting)
        {
            (await task).Dispose();
        }
    }

    [Fact]
    public async Task AWaitCapOf100Ms_RefusesAtEnqueue_WhenTheExpectedWaitIsLonger()
    {
        var admission = Build(1, TimeSpan.FromMilliseconds(100), new TimerTimeProvider());
        using var held = await AdmitApart(admission, HeavyRoute);

        var refused = await AdmitApart(admission, HeavyRoute);
        Assert.NotNull(refused.Refusal);
        Assert.Equal(0, admission.Stats().Queued);

        /* The expected wait is 2 s; a retry-after is never under 5 s. */
        Assert.Equal(ReadAdmission.MinRetryAfterSeconds, refused.Refusal!.RetryAfterSeconds);
    }

    [Fact]
    public async Task AQueuedRead_IsRefusedWhenTheCapPassesWhileItWaits()
    {
        var clock = new TimerTimeProvider();
        var admission = Build(1, TimeSpan.FromSeconds(10), clock);
        using var held = await AdmitApart(admission, HeavyRoute);

        var queued = AdmitApart(admission, HeavyRoute);
        await WaitUntil(() => admission.Stats().Queued == 1);

        clock.Advance(TimeSpan.FromSeconds(10));
        var refused = await queued;
        Assert.NotNull(refused.Refusal);
        Assert.Equal(10_000, refused.QueueMs);
        Assert.Equal(0, admission.Stats().Queued);
    }

    [Fact]
    public async Task ACallerThatLeavesTheQueue_IsToldItLeft()
    {
        var admission = Build(1);
        using var held = await AdmitApart(admission, HeavyRoute);
        using var leave = new CancellationTokenSource();

        var queued = AdmitApart(admission, HeavyRoute, cancellationToken: leave.Token);
        await WaitUntil(() => admission.Stats().Queued == 1);
        leave.Cancel();

        var left = await queued;
        Assert.True(left.CallerLeft);
        Assert.Null(left.Refusal);
        Assert.Equal(0, admission.Stats().Queued);
    }

    [Fact]
    public async Task ALightRead_FinishesWhileAHeavyReadHoldsTheOnlySlot()
    {
        var admission = Build(1);
        using var held = await AdmitApart(admission, HeavyRoute);

        var light = await AdmitApart(admission, LightRoute, ReadWindowBand.SevenDays).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Same(ReadAdmissionTicket.Passed, light);

        var unknownShort = await AdmitApart(admission, "never_seen", ReadWindowBand.OneDay).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Same(ReadAdmissionTicket.Passed, unknownShort);
    }

    [Fact]
    public async Task ANestedCall_AtLimitOne_TakesNoSecondSlot()
    {
        var admission = Build(1);
        var outcome = await Task.Run(async () =>
        {
            using var outer = await admission.AdmitAsync(ReadSurface.Mcp, HeavyRoute, ReadWindowBand.None, CancellationToken.None);
            Assert.True(outer.MayRun);

            /* A child of the flow (a panel run from a tool, a reader fanned out with Task.WhenAll) is the same read. */
            var inner = await Task.Run(() => admission.AdmitAsync(ReadSurface.Compose, HeavyRoute, ReadWindowBand.SevenDays, CancellationToken.None))
                .WaitAsync(TimeSpan.FromSeconds(5));
            return inner;
        });

        Assert.Same(ReadAdmissionTicket.Passed, outcome);
        Assert.Equal(0, admission.Stats().Running);
    }

    [Fact]
    public async Task ALimitOfZero_AdmitsEverything()
    {
        var admission = Build(0);
        var tickets = await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => AdmitApart(admission, HeavyRoute)));
        Assert.All(tickets, t => Assert.Same(ReadAdmissionTicket.Passed, t));
    }

    [Fact]
    public void Configure_SetsTheLimit_AndANegativeValueIsOff()
    {
        var admission = Build(2);
        admission.Configure(3);
        Assert.Equal(3, admission.Limit);
        admission.Configure(-1);
        Assert.Equal(0, admission.Limit);
        Assert.Equal(2, new DarlingConfig().Reads.MaxHeavyReads);
    }

    [Theory]
    [InlineData(ReadWindowBand.None, false)]
    [InlineData(ReadWindowBand.OneHour, false)]
    [InlineData(ReadWindowBand.OneDay, false)]
    [InlineData(ReadWindowBand.SevenDays, true)]
    [InlineData(ReadWindowBand.Longer, true)]
    public void APairWithNoHistory_TakesTheBandPrior(string band, bool heavy)
    {
        var admission = Build(2);
        Assert.Equal(heavy, admission.IsHeavy(ReadSurface.Web, "never_seen", band, out _));
    }

    [Fact]
    public void TheThreshold_IsAP90AboveTwoSeconds_AndTheSurfacesKeepSeparateRows()
    {
        var admission = Build(2);
        Assert.True(admission.IsHeavy(ReadSurface.Mcp, HeavyRoute, ReadWindowBand.None, out var heavyMs));
        Assert.Equal(3_000, heavyMs);

        /* Learned light at 1,999 ms beats the 7d prior. */
        Assert.False(admission.IsHeavy(ReadSurface.Mcp, LightRoute, ReadWindowBand.SevenDays, out _));

        /* The web surface has no row for the MCP route: the prior applies. */
        Assert.True(admission.IsHeavy(ReadSurface.Web, LightRoute, ReadWindowBand.SevenDays, out _));
    }

    [Fact]
    public void BuildTable_SumsRowsOfOnePair_AndTakesTheP90FromTheBuckets()
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        long[] Counts(long fastCount, long slowCount)
        {
            var counts = new long[bounds.Length + 1];
            counts[ReadLatencyAccumulator.BucketIndexFor(100)] = fastCount;
            counts[ReadLatencyAccumulator.BucketIndexFor(5_000)] = slowCount;
            return counts;
        }

        var table = ReadAdmission.BuildTable(new[]
        {
            /* Two hours of one pair: 85 fast + 15 slow in total, so the p90 is in the slow bucket. */
            (ReadSurface.Mcp, "split", "7d", Counts(80, 5), 4_000L),
            (ReadSurface.Mcp, "split", "7d", Counts(5, 10), 4_800L),

            /* 95 fast, 5 slow: the p90 is fast. */
            (ReadSurface.Mcp, "mostly_fast", "7d", Counts(95, 5), 4_000L),

            /* A row whose array does not match the bounds is skipped. */
            (ReadSurface.Mcp, "bad_shape", "7d", new long[] { 1, 2 }, 10L),
        });

        Assert.Equal(4_800, table[(ReadSurface.Mcp, "split", "7d")]);
        Assert.True(table[(ReadSurface.Mcp, "mostly_fast", "7d")] < ReadAdmission.HeavyThresholdMs);
        Assert.False(table.ContainsKey((ReadSurface.Mcp, "bad_shape", "7d")));
    }

    [Fact]
    public void BuildTable_AP90InThe1500To2000Bucket_StaysLight_AndTheNextBucketIsHeavy()
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        long[] Counts(int bucketMs, long slowCount)
        {
            var counts = new long[bounds.Length + 1];
            counts[ReadLatencyAccumulator.BucketIndexFor(100)] = 100 - slowCount;
            counts[ReadLatencyAccumulator.BucketIndexFor(bucketMs)] = slowCount;
            return counts;
        }

        var table = ReadAdmission.BuildTable(new[]
        {
            /* 20 of 100 reads in (1,500, 2,000]: the p90 bucket's upper bound is 2,000, a real p90 of at most 2,000. */
            (ReadSurface.Mcp, "in_2000_bucket", "7d", Counts(1_800, 20), 2_000L),
            /* 20 of 100 reads in (2,000, 3,000]: the p90 is above 2,000. */
            (ReadSurface.Mcp, "in_3000_bucket", "7d", Counts(2_500, 20), 3_000L),
        });

        Assert.Equal(2_000, table[(ReadSurface.Mcp, "in_2000_bucket", "7d")]);
        Assert.Equal(3_000, table[(ReadSurface.Mcp, "in_3000_bucket", "7d")]);

        var admission = Build(2);
        admission.SetTable(table);
        Assert.False(admission.IsHeavy(ReadSurface.Mcp, "in_2000_bucket", "7d", out _));
        Assert.True(admission.IsHeavy(ReadSurface.Mcp, "in_3000_bucket", "7d", out _));
    }

    [Fact]
    public async Task ARebuildSliceThatExpires_KeepsTheLastTable_AndTheFlushAfterItStillRuns()
    {
        var flushed = false;
        var finished = await ReadAdmission.RunRebuildSliceAsync(
            async token =>
            {
                await Task.Delay(Timeout.Infinite, token);
                return true;
            },
            TimeSpan.FromMilliseconds(50), logger: null, CancellationToken.None);
        flushed = true;

        Assert.False(finished);
        Assert.True(flushed);
    }

    [Fact]
    public async Task ARebuildThatThrows_KeepsTheLastTable_AndTheFlushAfterItStillRuns()
    {
        var flushed = false;
        var finished = await ReadAdmission.RunRebuildSliceAsync(
            _ => throw new InvalidOperationException("the scan failed"),
            TimeSpan.FromSeconds(30), logger: null, CancellationToken.None);
        flushed = true;

        Assert.False(finished);
        Assert.True(flushed);
    }

    [Fact]
    public async Task ARebuildSlice_EndsWithTheBudget_WhenTheBudgetEnds()
    {
        using var budget = new CancellationTokenSource();
        budget.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ReadAdmission.RunRebuildSliceAsync(
            async token =>
            {
                await Task.Delay(Timeout.Infinite, token);
                return true;
            },
            TimeSpan.FromSeconds(30), logger: null, budget.Token));
    }

    [Fact]
    public void TheHourlySweep_RebuildsUnderItsOwnSlice_AndReopensTheConnectionBeforeTheFlush()
    {
        var source = File.ReadAllText(Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var rebuild = source.IndexOf("ReadAdmission.RunRebuildSliceAsync(", StringComparison.Ordinal);
        var flush = source.IndexOf("await _readLatency.FlushAsync(", StringComparison.Ordinal);
        Assert.True(rebuild > 0 && flush > rebuild, "The rebuild runs in its own slice, before the read-latency flush.");
        Assert.Contains("connection.State != ConnectionState.Open", source.Substring(rebuild, flush - rebuild));
        Assert.DoesNotContain("_readAdmission.RefreshAsync(connection", source);
    }

    [Fact]
    public void TheHealthReads_AndTheWriteTools_SkipAdmission()
    {
        Assert.False(ReadAdmission.IsAdmitted("get_read_latency"));
        Assert.False(ReadAdmission.IsAdmitted("list_servers"));
        Assert.False(ReadAdmission.IsAdmitted("create_custom_view"));
        Assert.True(ReadAdmission.IsAdmitted("get_wait_stats"));
        Assert.True(ReadAdmission.IsAdmitted("get_ag_health"));
    }

    [Fact]
    public void TheBusyEnvelope_CarriesTheRetryAfterAndTheCounts()
    {
        var text = ReadAdmission.McpBusyText(new ReadAdmissionRefusal("Sentence.", 20, 2, 16));
        var body = JsonNode.Parse(text)!.AsObject();
        Assert.Equal("busy", body["status"]!.GetValue<string>());
        Assert.Equal("Sentence.", body["message"]!.GetValue<string>());
        Assert.Equal(20, body["hints"]!["retry_after_seconds"]!.GetValue<int>());
        Assert.Equal(2, body["hints"]!["running"]!.GetValue<int>());
        Assert.Equal(16, body["hints"]!["queued"]!.GetValue<int>());
    }
}

/// <summary>A clock that moves only when a test moves it, with timers that fire when it passes their due time.</summary>
internal sealed class TimerTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private readonly List<FakeTimer> _timers = new();
    private long _ticks;

    public override DateTimeOffset GetUtcNow() => new DateTimeOffset(2026, 10, 14, 0, 0, 0, TimeSpan.Zero).AddTicks(Interlocked.Read(ref _ticks));

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Interlocked.Read(ref _ticks);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new FakeTimer(this, callback, state);
        timer.Change(dueTime, period);
        return timer;
    }

    public void Advance(TimeSpan by)
    {
        List<FakeTimer> due;
        lock (_gate)
        {
            _ticks += by.Ticks;
            due = _timers.Where(t => t.DueAt <= _ticks).ToList();
            foreach (var timer in due)
            {
                _timers.Remove(timer);
            }
        }

        foreach (var timer in due)
        {
            timer.Fire();
        }
    }

    private sealed class FakeTimer(TimerTimeProvider owner, TimerCallback callback, object? state) : ITimer
    {
        public long DueAt { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner._gate)
            {
                owner._timers.Remove(this);
                if (dueTime != Timeout.InfiniteTimeSpan)
                {
                    DueAt = owner._ticks + dueTime.Ticks;
                    owner._timers.Add(this);
                }
            }

            return true;
        }

        public void Fire() => callback(state);

        public void Dispose()
        {
            lock (owner._gate)
            {
                owner._timers.Remove(this);
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
