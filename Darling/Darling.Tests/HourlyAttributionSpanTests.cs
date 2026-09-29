/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>Pins the denominator of the hourly cpu_attribution share: the span the rollup actually served
/// (first bucket to the materialization ceiling), never the requested window.</summary>
public sealed class HourlyAttributionSpanTests
{
    private static readonly DateTime Day = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Hourly_LateFirstBucketAndLowCeiling_UsesTheServedSpan()
    {
        var start = Day.AddHours(10).AddMinutes(37);
        var asOf = Day.AddHours(20).AddMinutes(20);
        var (from, to, note) = DarlingMcpDataTools.HourlyAttributionSpan(true, start, asOf, Day.AddHours(13), Day.AddHours(18));

        Assert.Equal(Day.AddHours(13), from);
        Assert.Equal(Day.AddHours(18), to);
        Assert.Contains("served span", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Hourly_ShareDividesByTheServedSpan_NotTheRequestedWindow()
    {
        var start = Day.AddHours(10).AddMinutes(37);
        var asOf = Day.AddHours(20).AddMinutes(20);
        var (from, to, _) = DarlingMcpDataTools.HourlyAttributionSpan(true, start, asOf, Day.AddHours(13), Day.AddHours(18));

        // 25% of 8 cores over the 5 served hours = 36,000 CPU-seconds; 27,000 ranked seconds is 0.75.
        var served = CpuAttribution.Compute(27000, from, to, 300, from, to, 25, 8);
        Assert.Equal(36000, served.SqlCpuSecondsInWindow);
        Assert.Equal(0.75, served.AttributedCpuRatio);

        // The requested window (about 9.7 h) would give a different, smaller share.
        var requested = CpuAttribution.Compute(27000, start, asOf, 300, from, to, 25, 8);
        Assert.NotEqual(served.AttributedCpuRatio, requested.AttributedCpuRatio);
    }

    [Fact]
    public void Hourly_NullCeiling_SaysUnknown_NotThatNoSpanWasServed()
    {
        var start = Day.AddHours(10);
        var asOf = Day.AddHours(20);
        var (from, to, note) = DarlingMcpDataTools.HourlyAttributionSpan(true, start, asOf, Day.AddHours(10), null);

        Assert.Equal(start, from);
        Assert.Equal(asOf, to);
        Assert.Contains("ceiling unknown; the end edge is not verified", note, StringComparison.Ordinal);
        Assert.DoesNotContain("served no bucket span", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Raw_KeepsTheRequestedWindow_AndSaysNothing()
    {
        var start = Day.AddHours(10).AddMinutes(37);
        var asOf = Day.AddHours(20).AddMinutes(20);
        var (from, to, note) = DarlingMcpDataTools.HourlyAttributionSpan(false, start, asOf, null, null);

        Assert.Equal(start, from);
        Assert.Equal(asOf, to);
        Assert.Null(note);
    }
}
