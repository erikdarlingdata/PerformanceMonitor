/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
    public void OutcomeLabels_AreTheClosedSet_AndTheOldFiveKeepTheirStoredSpelling()
    {
        var all = Enum.GetValues<ReadOutcome>();

        Assert.Equal(
            new[] { "ok", "timeout", "cancelled", "error", "limit", "fallback_raw", "gate_failed", "queue_wait", "busy", "cache_hit" },
            all.Select(ReadLatencyAccumulator.OutcomeLabel).ToArray());
        foreach (var old in all.Where(o => o <= ReadOutcome.Limit))
        {
            Assert.Equal(old.ToString().ToLowerInvariant(), ReadLatencyAccumulator.OutcomeLabel(old));
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => ReadLatencyAccumulator.OutcomeLabel((ReadOutcome)99));
    }

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

    /// <summary>#5506: the window band is part of the key, so a 1-hour read and a 7-day read of one route land in two
    /// buckets; the old four-argument overload records under <c>none</c>.</summary>
    [Fact]
    public void Record_DifferentWindowBands_AreSeparateBuckets_AndTheOldOverloadRecordsNone()
    {
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 20, ReadWindowBand.OneHour, 0, 0);
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 9_000, ReadWindowBand.SevenDays, 0, 0);
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 8_000, ReadWindowBand.SevenDays, 0, 0);
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 30);
        accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 40, null, 0, 0);

        var drained = accumulator.Drain();
        Assert.Equal(3, drained.Count);
        Assert.Contains(drained, r => r.WindowBand == ReadWindowBand.OneHour && r.Count == 1 && r.MaxMs == 20);
        Assert.Contains(drained, r => r.WindowBand == ReadWindowBand.SevenDays && r.Count == 2 && r.MaxMs == 9_000);
        Assert.Contains(drained, r => r.WindowBand == ReadWindowBand.None && r.Count == 2);
    }

    /// <summary>#5506: per key the hour keeps the most reads already in flight when one started (MAX, not the last or the
    /// sum) and the summed connection wait; a negative wait adds nothing.</summary>
    [Fact]
    public void Record_KeepsTheMaxInFlight_AndSumsTheWait()
    {
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Web, "get_blocking", ReadOutcome.Ok, 10, ReadWindowBand.OneDay, 3, 40);
        accumulator.Record(ReadSurface.Web, "get_blocking", ReadOutcome.Ok, 10, ReadWindowBand.OneDay, 9, 0);
        accumulator.Record(ReadSurface.Web, "get_blocking", ReadOutcome.Ok, 10, ReadWindowBand.OneDay, 2, 15);
        accumulator.Record(ReadSurface.Web, "get_blocking", ReadOutcome.Ok, 10, ReadWindowBand.OneDay, 0, -5);

        var row = Assert.Single(accumulator.Drain());
        Assert.Equal(9, row.MaxInFlight);
        Assert.Equal(55, row.WaitTotalMs);
        Assert.Equal(4, row.Count);
    }
}
