/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7663: <c>get_overview_baselines</c> tells a metric that sat at exactly 0 for an hour (samples held, no spread) apart from an hour with no
/// samples. The reply keeps the zero-activity hour as a step marked <c>zero_activity</c>, so the page can say "this metric sat at 0" and not
/// "not enough history". A step with a usable band carries no such mark. Nothing here needs a store: the shared tier is seeded the way the
/// worker's pass fills it, over a data source that cannot connect.
/// </summary>
public sealed class OverviewBaselineZeroActivityTests
{
    private static readonly DateTime Now = new(2026, 11, 4, 6, 30, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = Now.AddHours(-24);
    private const int ServerId = 7;

    private static NpgsqlDataSource Unreachable() => NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1;Command Timeout=1");

    private static PgBaselineProvider.CachedBaseline EntryAt(DateTime roundedKey, double mean, double stdDev) => new()
    {
        ComputedAt = roundedKey,
        RealTime = DateTime.UtcNow,
        FreshUntilUtc = DateTime.UtcNow.AddDays(1),
        Clock = LocalClockWindow.Utc(roundedKey),
        /* Tuesday 06:00, the hour the 24-hour window starts in: 500 samples that all read the same value. */
        Buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>
        {
            [(6, 2)] = new BaselineBucket
            {
                HourOfDay = 6, DayOfWeek = 2, Mean = mean, StdDev = stdDev, SampleCount = 500, DistinctDays = 20, Tier = BaselineTier.Full, Median = mean, Mad = 0,
            },
        },
    };

    [Fact]
    public async Task AnHourAtExactlyZero_IsKeptAsAZeroActivityStep_NotDroppedLikeAnHourWithNoSamples()
    {
        await using var postgres = Unreachable();
        var shared = new BaselineCache();
        var provider = new PgBaselineProvider(postgres, null, shared);
        var kind = provider.GetType().FullName!;
        /* CPU and blocking sat at 0 all hour; waits and I/O varied. */
        foreach (var (metric, mean, sd) in new[] { (MetricNames.Cpu, 0d, 0d), (MetricNames.WaitMsPerSec, 42d, 5d), (MetricNames.IoLatencyChart, 3d, 1d), (MetricNames.BlockingPerMinute, 0d, 0d) })
        {
            shared.Put(kind, ServerId, PgBaselineProvider.CacheKeyFor(ServerId, metric, key: null), EntryAt(provider.RoundedKeyTime(metric, Now), mean, sd));
        }

        var lanes = await DarlingWebOverviewBaselines.ReadLaneStepsAsync(provider, ServerId, WindowStart, Now, CancellationToken.None);

        Assert.Equal(4, lanes.Length);
        foreach (var lane in lanes) Assert.NotEmpty(lane);
        var cpu = DarlingWebOverviewBaselines.Step(lanes[0][0]);
        Assert.True(cpu["zero_activity"]?.GetValue<bool>());
        Assert.Equal(500, cpu["sample_count"]!.GetValue<long>());
        Assert.True(DarlingWebOverviewBaselines.Step(lanes[3][0])["zero_activity"]?.GetValue<bool>());
        /* A step with a band is not marked, so the page draws it. */
        Assert.Null(DarlingWebOverviewBaselines.Step(lanes[1][0])["zero_activity"]);
        Assert.True(DarlingWebOverviewBaselines.Step(lanes[1][0])["effective_std_dev"]!.GetValue<double>() > 0);
    }
}
