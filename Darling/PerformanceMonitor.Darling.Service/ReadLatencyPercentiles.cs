/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// A percentile answered from <see cref="ReadLatencyAccumulator"/> buckets: an UPPER-BOUND ESTIMATE, not the
/// real value. The histogram only ever knows which bucket a duration landed in, never the exact millisecond
/// within it, so the honest answer to "what's the p95" is "at most this many ms" — <see cref="IsAtLeast"/>
/// says when even that upper bound is itself a floor, because the true value fell in the overflow bucket and
/// there is no bound above it to report.
/// </summary>
public readonly record struct ReadLatencyPercentileEstimate(long UpperBoundMs, bool IsAtLeast);

/// <summary>
/// Pure percentile math over <see cref="ReadLatencyAccumulator"/>'s fixed log-scale buckets (#4442 scope 2).
/// No store, no clock, no accumulator reference — just the counts and the bounds that produced them, so a
/// field-shaped fixture pins the math without a live read anywhere near it.
/// </summary>
public static class ReadLatencyPercentiles
{
    /// <summary>
    /// The bucket upper bound at which the cumulative count first reaches <paramref name="p"/> (0 &lt; p &lt;
    /// 1) of the total. Null when <paramref name="counts"/> sums to zero — there is no percentile of nothing.
    /// <paramref name="bounds"/> must have exactly one fewer entry than <paramref name="counts"/> (the last
    /// count is the overflow bucket, one past every finite bound); that bucket's answer is flagged
    /// <see cref="ReadLatencyPercentileEstimate.IsAtLeast"/> — the true duration only known to exceed the last
    /// bound, never by how much.
    ///
    /// <para><paramref name="maxMs"/> (#5633 item 23) is the slowest duration actually seen over the same samples. No
    /// duration exceeds it, so no percentile can either: a bucket bound above it is capped to it. A capped value is
    /// no longer a bucket bound, but it is still an UPPER-BOUND estimate (the true percentile is at or below it), so
    /// <see cref="ReadLatencyPercentileEstimate.IsAtLeast"/> stays false and the "at most" reading stays true. Max 3,568
    /// ms with every percentile in the 5,000 ms bucket answers 3,568, not a p99 above the max.</para>
    /// </summary>
    public static ReadLatencyPercentileEstimate? FromBuckets(long[] counts, int[] bounds, double p, long? maxMs = null)
    {
        if (counts is null || bounds is null || counts.Length != bounds.Length + 1)
        {
            throw new ArgumentException(
                "counts must have exactly one more entry than bounds (the overflow bucket).", nameof(counts));
        }

        if (p is <= 0 or >= 1)
        {
            throw new ArgumentOutOfRangeException(nameof(p), p, "p must be strictly between 0 and 1.");
        }

        long total = 0;
        foreach (var count in counts)
        {
            total += count;
        }

        if (total == 0)
        {
            return null;
        }

        /* The smallest cumulative count that reaches p*total — an upper-bound percentile (the real value
           could be anywhere at or below the bucket's own upper bound), never an interpolation across it. */
        var target = Math.Ceiling(p * total);
        long cumulative = 0;

        for (var i = 0; i < bounds.Length; i++)
        {
            cumulative += counts[i];
            if (cumulative >= target)
            {
                return new ReadLatencyPercentileEstimate(CapAtMax(bounds[i], maxMs), IsAtLeast: false);
            }
        }

        /* Every finite bucket exhausted without reaching target — the percentile lives in the overflow
           bucket. The last finite bound is reported as a FLOOR ("≥"), not a made-up finite value. */
        return new ReadLatencyPercentileEstimate(bounds[^1], IsAtLeast: true);
    }

    /// <summary>A bucket bound capped at the observed maximum when it is higher (a negative max is "unknown", not a cap).</summary>
    private static long CapAtMax(long boundMs, long? maxMs) =>
        maxMs is >= 0 && boundMs > maxMs ? maxMs.Value : boundMs;
}
