/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Decision-table pins for the shared <see cref="CpuAttribution"/> (#2320) — the attributed-CPU
/// disclosure both SKUs' get_top_queries_by_cpu / get_top_procedures_by_cpu serve. The contract under
/// pin: the ratio is measured-or-omitted (never invented — missing samples, missing core count, or
/// thin coverage all degrade to null + a reason), the low note fires under half, and above the
/// process's own measured CPU the note calls the number impossible rather than presenting it —
/// the 137%-of-the-box claim is the whole reason the marker exists. This SAME table is pinned
/// identically in Darling.Tests so the two SKUs cannot drift.
/// </summary>
public sealed class CpuAttributionTests
{
    private static readonly DateTime Start = new(2026, 8, 18, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime End = Start.AddHours(1);

    /// <summary>Full coverage, healthy ratio: 25% of 8 cores over an hour = 7,200 CPU-seconds;
    /// 5,000 ranked seconds is 0.694 — present, rounded to 3, no note.</summary>
    [Fact]
    public void HealthyRatio_NoNote()
    {
        var result = CpuAttribution.Compute(
            rankedCpuSeconds: 5000, Start, End,
            sampleCount: 60, firstSampleUtc: Start, lastSampleUtc: End, avgSqlCpuPercent: 25, cpuCount: 8);

        Assert.Equal(5000, result.RankedCpuSeconds);
        Assert.Equal(7200, result.SqlCpuSecondsInWindow);
        Assert.Equal(0.694, result.AttributedCpuRatio);
        Assert.Null(result.Note);
    }

    /// <summary>The pre-#2290 shape this feature exists for: the ranking explains ~10% of the box,
    /// and now something says so instead of letting the caller chase the visible tenth.</summary>
    [Fact]
    public void LowRatio_SaysNotTheWholeStory()
    {
        var result = CpuAttribution.Compute(720, Start, End, 60, Start, End, 25, 8);

        Assert.Equal(0.1, result.AttributedCpuRatio);
        Assert.NotNull(result.Note);
        Assert.Contains("10%", result.Note, StringComparison.Ordinal);
        Assert.Contains("not the whole story", result.Note, StringComparison.Ordinal);
    }

    /// <summary>The 137% case — worker_time summing to more CPU than the process consumed is an
    /// impossible claim, and the note must say to distrust the numbers, not decorate them.</summary>
    [Fact]
    public void OverAttribution_IsFlaggedImpossible()
    {
        var result = CpuAttribution.Compute(9864, Start, End, 60, Start, End, 25, 8);

        Assert.Equal(1.37, result.AttributedCpuRatio);
        Assert.NotNull(result.Note);
        Assert.Contains("137%", result.Note, StringComparison.Ordinal);
        Assert.Contains("impossible-claim", result.Note, StringComparison.Ordinal);
    }

    /// <summary>Just above 1.0 is sampling noise between two independent series, not a lie —
    /// the impossible flag waits for the slack threshold.</summary>
    [Fact]
    public void SlightlyOverOne_CarriesNoNote()
    {
        var result = CpuAttribution.Compute(7500, Start, End, 60, Start, End, 25, 8);

        Assert.Equal(1.042, result.AttributedCpuRatio);
        Assert.Null(result.Note);
    }

    [Fact]
    public void NoSamples_OmitsRatio_AndSaysWhy()
    {
        var result = CpuAttribution.Compute(5000, Start, End, 0, null, null, null, 8);

        Assert.Equal(5000, result.RankedCpuSeconds);
        Assert.Null(result.SqlCpuSecondsInWindow);
        Assert.Null(result.AttributedCpuRatio);
        Assert.Contains("no cpu_utilization samples", result.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void NoCoreCount_OmitsRatio_AndSaysWhy()
    {
        var result = CpuAttribution.Compute(5000, Start, End, 60, Start, End, 25, cpuCount: 0);

        Assert.Null(result.AttributedCpuRatio);
        Assert.Contains("core count unavailable", result.Note, StringComparison.Ordinal);
    }

    /// <summary>#2320's explicit degrade rule: a server whose CPU series starts mid-window (added,
    /// or monitoring resumed) would deflate the denominator and inflate the ratio — omit instead.</summary>
    [Fact]
    public void PartialCoverage_OmitsRatio_WithThePercentage()
    {
        var result = CpuAttribution.Compute(5000, Start, End, 30, Start.AddMinutes(30), End, 25, 8);

        Assert.Null(result.AttributedCpuRatio);
        Assert.NotNull(result.Note);
        Assert.Contains("50%", result.Note, StringComparison.Ordinal);
        Assert.Contains("partial denominator", result.Note, StringComparison.Ordinal);
    }

    /// <summary>Samples straddling the window edges clamp to full coverage — a series wider than the
    /// window is the NORMAL case (the store holds more history than any one read).</summary>
    [Fact]
    public void SamplesBeyondTheWindow_ClampToFullCoverage()
    {
        var result = CpuAttribution.Compute(
            5000, Start, End, 120, Start.AddHours(-1), End.AddHours(1), 25, 8);

        Assert.Equal(0.694, result.AttributedCpuRatio);
    }

    /// <summary>An idle box measures zero CPU-seconds; a ratio against zero is undefined, and the
    /// measured zero is still reported so the caller sees WHY.</summary>
    [Fact]
    public void ZeroMeasuredCpu_OmitsRatio_ReportsTheZero()
    {
        var result = CpuAttribution.Compute(5000, Start, End, 60, Start, End, avgSqlCpuPercent: 0, cpuCount: 8);

        Assert.Equal(0, result.SqlCpuSecondsInWindow);
        Assert.Null(result.AttributedCpuRatio);
        Assert.Contains("zero", result.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyWindow_OmitsRatio()
    {
        var result = CpuAttribution.Compute(5000, Start, Start, 60, Start, End, 25, 8);

        Assert.Null(result.AttributedCpuRatio);
        Assert.Contains("window is empty", result.Note, StringComparison.Ordinal);
    }

    /// <summary>The numerator is rounded for emission but the ratio divides the RAW value — rounding
    /// before dividing would move the third decimal on big windows.</summary>
    [Fact]
    public void RankedSecondsRoundToOneDecimal_RatioToThree()
    {
        var result = CpuAttribution.Compute(1234.5678, Start, End, 60, Start, End, 25, 8);

        Assert.Equal(1234.6, result.RankedCpuSeconds);
        Assert.Equal(0.171, result.AttributedCpuRatio);
    }
}
