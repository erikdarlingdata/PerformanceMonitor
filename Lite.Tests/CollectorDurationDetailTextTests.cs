/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: the Duration Trends hover's fourth line is worded in one place for both apps (<c>CollectorDurationHoverText</c> in
/// PerformanceMonitor.Ui), and Lite's own method only hands it the bucket's run count and average. These pin the exact strings through
/// Lite's method (the Darling viewer pins the same strings through the shared helper, in <c>ViewerCollectorDurationHoverTests</c>) and
/// that Lite keeps no copy of the words.
/// </summary>
public class CollectorDurationDetailTextTests
{
    private static string Detail(long runs, double averageMs, string culture = "en-US")
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo(culture);
            return ServerTab.CollectorDurationDetail(new CollectorDurationBucket { CollectorName = "wait_stats", RunCount = runs, AverageDurationMs = averageMs });
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    /* The exact strings, for one run (singular), many runs, a thousands separator, a whole average and a fractional one. */
    [Theory]
    [InlineData(1L, 50.0, "Slowest of 1 run; average 50 ms")]
    [InlineData(12L, 61.0, "Slowest of 12 runs; average 61 ms")]
    [InlineData(1234L, 2000.0, "Slowest of 1,234 runs; average 2,000 ms")]
    [InlineData(10L, 910.8, "Slowest of 10 runs; average 910.8 ms")]
    [InlineData(12L, 0.5, "Slowest of 12 runs; average 0.5 ms")]
    public void TheDetail_SaysTheRunCountAndTheAverage_InTheseExactWords(long runs, double average, string expected) =>
        Assert.Equal(expected, Detail(runs, average));

    /* The words follow the current culture, as they always did here. */
    [Fact]
    public void TheDetail_FollowsTheCurrentCulture() =>
        Assert.Equal("Slowest of 1.234 runs; average 910,8 ms", Detail(1234, 910.8, "de-DE"));

    /* Lite words nothing itself: its method is one call to the shared helper, and the chart file that holds the method keeps no copy of the text. */
    [Fact]
    public void TheMethod_HandsTheSharedHelperTheRunCountAndTheAverage_AndItsChartFileHoldsNoCopyOfTheWords()
    {
        var charts = Lite.Tests.ParitySource.ReadFile("Lite/Controls/ServerTab.Charts.cs");

        Assert.Single(Regex.Matches(
            charts,
            @"internal static string CollectorDurationDetail\(CollectorDurationBucket bucket\) =>\s*CollectorDurationHoverText\.Detail\(bucket\.RunCount,\s*bucket\.AverageDurationMs\);"));
        Assert.DoesNotContain("Slowest of", charts, StringComparison.Ordinal);
    }
}
