/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9533 through Lite's own calculator: the read watermark and the baseline advance live in the shared core, and Lite's
/// <see cref="DeltaCalculator"/> inherits them. A restart gives Lite's calculator no watermark, because it is never seeded.
/// </summary>
public sealed class QueryStatsReadWatermarkLiteTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
    private static readonly string[] Groups = { "query_stats_exec" };

    [Fact]
    public void ARestartedCalculator_HasNoWatermark_AndOneArmedByARead_IsTakenOnceWithItsGap()
    {
        ICollectorDeltaCalculator deltas = new DeltaCalculator(NullLogger.Instance);
        Assert.Null(deltas.TakeReadWatermark(1, "query_stats_exec", T0));

        deltas.ArmReadWatermark(1, "query_stats_exec", T0.AddSeconds(-2), T0);
        var taken = deltas.TakeReadWatermark(1, "query_stats_exec", T0.AddSeconds(60));
        Assert.Equal(T0.AddSeconds(-2), taken!.Value.ChangedSince);
        Assert.Equal(90, taken.Value.MaxAgeSeconds);
        Assert.Null(deltas.TakeReadWatermark(1, "query_stats_exec", T0.AddSeconds(120)));
    }

    [Fact]
    public void AKeyTheReadDidNotReturn_KeepsItsValue_AndItsIntervalRunsFromTheAdvancedTimestamp()
    {
        ICollectorDeltaCalculator deltas = new DeltaCalculator(NullLogger.Instance);
        const string key = "0x01:0:-1:0x02";
        deltas.CalculateDeltaWithInterval(1, "query_stats_exec", key, 50, out _, T0, 3600);
        deltas.AdvanceUnreturnedBaselines(1, "query_stats_exec", Groups, new Dictionary<string, DateTime> { [key] = T0 }, T0.AddMinutes(-10), false, T0);

        deltas.AdvanceUnreturnedBaselines(1, "query_stats_exec", Groups, new Dictionary<string, DateTime>(), T0.AddSeconds(60).AddMinutes(-10), true, T0.AddSeconds(60));

        var delta = deltas.CalculateDeltaWithInterval(1, "query_stats_exec", key, 80, out var interval, T0.AddSeconds(120), 3600);
        Assert.Equal(30, delta);
        Assert.Equal(60, interval);
    }
}
