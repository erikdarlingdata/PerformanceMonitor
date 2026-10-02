/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#4442 scope 2: the read-latency histogram accumulator — the <c>CollectorCostAccumulator</c> shape
/// applied to <c>(surface, route, outcome)</c> keys, unit-tested the same way that accumulator is: concurrency,
/// drain-resets, and bucket-edge placement.</summary>
public sealed class ReadLatencyAccumulatorTests
{
    [Fact]
    public async Task Record_ManyConcurrentThreads_DrainTotalsMatch()
    {
        var accumulator = new ReadLatencyAccumulator();
        const int threads = 16;
        const int perThread = 200;

        var tasks = Enumerable.Range(0, threads).Select(t => Task.Run(() =>
        {
            for (var i = 0; i < perThread; i++)
            {
                accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50 + (t + i) % 40);
            }
        }));

        await Task.WhenAll(tasks);

        var drained = accumulator.Drain();
        var row = Assert.Single(drained);
        Assert.Equal(threads * perThread, row.Count);
        Assert.Equal(threads * perThread, row.BucketCounts.Sum());
    }

    [Fact]
    public void Drain_ResetsTheWindow_SoASecondDrainIsEmpty()
    {
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Web, "get_blocking", ReadOutcome.Ok, 42);

        var first = accumulator.Drain();
        Assert.Single(first);

        var second = accumulator.Drain();
        Assert.Empty(second);
    }

    [Fact]
    public void Record_QuietWindow_DrainsNothing()
    {
        var accumulator = new ReadLatencyAccumulator();
        Assert.Empty(accumulator.Drain());
    }

    [Fact]
    public void BucketIndexFor_ExactlyOnABound_LandsInThatBoundsBucket_NotTheNextOneUp()
    {
        var tenMsIndex = System.Array.IndexOf(ReadLatencyAccumulator.BucketUpperBoundsMs, 10);
        Assert.Equal(tenMsIndex, ReadLatencyAccumulator.BucketIndexFor(10));

        var oneSecondIndex = System.Array.IndexOf(ReadLatencyAccumulator.BucketUpperBoundsMs, 1_000);
        Assert.Equal(oneSecondIndex, ReadLatencyAccumulator.BucketIndexFor(1_000));
    }

    [Fact]
    public void BucketIndexFor_AboveTheLastBound_LandsInTheOverflowBucket()
    {
        var lastBound = ReadLatencyAccumulator.BucketUpperBoundsMs[^1];
        var overflowIndex = ReadLatencyAccumulator.BucketUpperBoundsMs.Length;

        Assert.Equal(overflowIndex, ReadLatencyAccumulator.BucketIndexFor(lastBound + 1));
        Assert.Equal(overflowIndex, ReadLatencyAccumulator.BucketIndexFor(lastBound * 100));
    }

    [Fact]
    public void Record_TracksCountTotalAndMax()
    {
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Compose, "compose:wait_stats", ReadOutcome.Ok, 10);
        accumulator.Record(ReadSurface.Compose, "compose:wait_stats", ReadOutcome.Ok, 30);
        accumulator.Record(ReadSurface.Compose, "compose:wait_stats", ReadOutcome.Ok, 5);

        var row = Assert.Single(accumulator.Drain());
        Assert.Equal(3, row.Count);
        Assert.Equal(45, row.TotalMs);
        Assert.Equal(30, row.MaxMs);
    }

    [Fact]
    public void Record_DifferentOutcomes_AreSeparateBuckets()
    {
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Web, "get_plan_capture_readiness", ReadOutcome.Ok, 20);
        accumulator.Record(ReadSurface.Web, "get_plan_capture_readiness", ReadOutcome.Timeout, 60_000);

        var drained = accumulator.Drain();
        Assert.Equal(2, drained.Count);
        Assert.Contains(drained, r => r.Outcome == ReadOutcome.Ok && r.Count == 1);
        Assert.Contains(drained, r => r.Outcome == ReadOutcome.Timeout && r.Count == 1);
    }
}
