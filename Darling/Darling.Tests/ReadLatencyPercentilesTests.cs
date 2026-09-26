/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#4442 scope 2: pure percentile math over <see cref="ReadLatencyAccumulator"/>'s fixed buckets.
/// <see cref="Percentiles_OverAFieldShapedFixture_LandInTheExpectedBuckets"/> is the field-shaped fixture named
/// in the PR body: 1,000 samples, 90% fast (50-200 ms), 9% slow (1-5 s), 1% very slow (20-60 s).</summary>
public sealed class ReadLatencyPercentilesTests
{
    private static readonly int[] s_bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;

    private static long[] NewCounts() => new long[s_bounds.Length + 1];

    private static void Add(long[] counts, long elapsedMs, int times = 1)
    {
        counts[ReadLatencyAccumulator.BucketIndexFor(elapsedMs)] += times;
    }

    [Fact]
    public void FromBuckets_AllZero_ReturnsNull()
    {
        var counts = NewCounts();
        Assert.Null(ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.5));
    }

    [Fact]
    public void FromBuckets_MismatchedLengths_Throws()
    {
        var counts = new long[s_bounds.Length]; // one short of the required overflow slot
        Assert.Throws<ArgumentException>(() => ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.5));
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(1.0)]
    [InlineData(-0.1)]
    [InlineData(1.5)]
    public void FromBuckets_POutOfRange_Throws(double p)
    {
        var counts = NewCounts();
        Add(counts, 50);
        Assert.Throws<ArgumentOutOfRangeException>(() => ReadLatencyPercentiles.FromBuckets(counts, s_bounds, p));
    }

    [Fact]
    public void FromBuckets_OverflowBucket_IsFlaggedAtLeast()
    {
        var counts = NewCounts();
        Add(counts, s_bounds[^1] + 5_000); // above every finite bound

        var p50 = ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.5)!.Value;
        Assert.True(p50.IsAtLeast);
        Assert.Equal(s_bounds[^1], p50.UpperBoundMs);
    }

    [Fact]
    public void FromBuckets_AllInOneFiniteBucket_IsNotFlaggedAtLeast()
    {
        var counts = NewCounts();
        Add(counts, 90);

        var p50 = ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.5)!.Value;
        Assert.False(p50.IsAtLeast);
        Assert.Equal(100, p50.UpperBoundMs);
    }

    /// <summary>The field-shaped fixture (named in the PR body): 1,000 samples — 900 uniformly spread 50-200
    /// ms (fast reads), 90 uniformly spread 1,000-5,000 ms (the slow tail), 10 uniformly spread 20,000-60,000
    /// ms (the rare very-slow reads a statement_timeout would eventually catch). p50 must land in the fast
    /// band's buckets, p95 must have crossed into the slow band, and p99 must have crossed into the very-slow
    /// band — each an upper-bound estimate, never lower than the true percentile.</summary>
    [Fact]
    public void Percentiles_OverAFieldShapedFixture_LandInTheExpectedBuckets()
    {
        var counts = NewCounts();
        var rng = new Random(20260926);

        for (var i = 0; i < 900; i++)
        {
            Add(counts, 50 + rng.Next(0, 151)); // 50-200 ms inclusive
        }

        for (var i = 0; i < 90; i++)
        {
            Add(counts, 1_000 + rng.Next(0, 4_001)); // 1,000-5,000 ms inclusive
        }

        for (var i = 0; i < 10; i++)
        {
            Add(counts, 20_000 + rng.Next(0, 40_001)); // 20,000-60,000 ms inclusive
        }

        var p50 = ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.50)!.Value;
        var p95 = ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.95)!.Value;
        var p99 = ReadLatencyPercentiles.FromBuckets(counts, s_bounds, 0.99)!.Value;

        // p50 sits inside the 90% fast band: at or under 200 ms, and never flagged AtLeast (there is plenty
        // of finite headroom above it).
        Assert.False(p50.IsAtLeast);
        Assert.True(p50.UpperBoundMs <= 200, $"p50 upper bound was {p50.UpperBoundMs} ms, expected <= 200 ms.");

        // p95 (the 90th-to-99th count) has crossed out of the fast band into the slow tail: strictly above
        // 200 ms, at or under 5,000 ms.
        Assert.True(p95.UpperBoundMs > 200, $"p95 upper bound was {p95.UpperBoundMs} ms, expected > 200 ms.");
        Assert.True(p95.UpperBoundMs <= 5_000, $"p95 upper bound was {p95.UpperBoundMs} ms, expected <= 5,000 ms.");

        // p99 (the last 1%) has crossed into the very-slow band: strictly above 5,000 ms.
        Assert.True(p99.UpperBoundMs > 5_000, $"p99 upper bound was {p99.UpperBoundMs} ms, expected > 5,000 ms.");
    }
}
