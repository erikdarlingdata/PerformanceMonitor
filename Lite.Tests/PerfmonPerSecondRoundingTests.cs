/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// How the perfmon payloads round a per-second figure. Four decimals print a rate under 0.00005 as 0, and a bucket can
/// be a day wide: one count in 86,400 s is 0.0000116 a second, so a nonzero delta published as a rate of 0, which a
/// reader takes for "nothing happened". <see cref="TrendPayloads.RoundRate"/> keeps the four decimals every
/// payload has published and gives a rate that small two significant digits instead. Lite and Darling build their
/// <c>get_perfmon_stats</c> rows and <c>get_perfmon_trend</c> points through the same <c>TrendPayloads</c> builders,
/// so these pins hold for both; the twin of the payload pins is in <c>PerfmonPerSecondPayloadTests</c> in Darling.Tests.
/// </summary>
public sealed class PerfmonPerSecondRoundingTests
{
    private static readonly DateTime Bucket0 = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void RoundRate_KeepsFourDecimals_AndTwoSignificantDigitsWhereFourWouldKeepFewer()
    {
        /* From 0.001 up, four decimals, as every perfmon payload has published them. */
        Assert.Equal(0.22, TrendPayloads.RoundRate(66.0 / 300), precision: 12);
        Assert.Equal(0.2333, TrendPayloads.RoundRate(70.0 / 300), precision: 12);
        Assert.Equal(0.0033, TrendPayloads.RoundRate(1.0 / 300), precision: 12);
        Assert.Equal(11641.2346, TrendPayloads.RoundRate(11641.234567), precision: 12);

        /* Below it, two significant digits: one count over an hour, over six hours, over a day. */
        Assert.Equal(0.00028, TrendPayloads.RoundRate(1.0 / 3600), precision: 12);
        Assert.Equal(0.000046, TrendPayloads.RoundRate(1.0 / 21600), precision: 12);
        Assert.Equal(0.000012, TrendPayloads.RoundRate(1.0 / 86400), precision: 12);

        /* A true zero is a zero, and a value that is not a number passes through untouched. */
        Assert.Equal(0.0, TrendPayloads.RoundRate(0));
        Assert.True(double.IsNaN(TrendPayloads.RoundRate(double.NaN)));
    }

    private static PerfmonBucketPoint Bucket(long delta, long seconds, double? peak = null) =>
        new(Bucket0, AvgValue: 0, MaxValue: 0, LastValue: 37,
            RatedDelta: delta, RatedSeconds: seconds, UnknowableCollections: 0, UnknowableDelta: null, UnrecordedDelta: null,
            PeakPerSecond: peak ?? delta / 300.0, CntrType: PerfmonCounterTypes.PerfCounterBulkCount);

    private static JsonElement TrendPoint(PerfmonBucketPoint point, int bucketMinutes)
    {
        using var doc = JsonDocument.Parse(TrendPayloads.PerfmonTrend(
            "SRV1", "Number of Deadlocks/sec", 168, new[] { point }, bucketMinutes, requested: true,
            autoBudget: TrendBuckets.McpPointBudget, discontinuities: Array.Empty<object>()));
        return doc.RootElement.GetProperty("trend")[0].Clone();
    }

    /// <summary>The largest bucket a caller can ask for is a day, 86,400 s. One deadlock in it is a rate that four
    /// decimals erase; it publishes as the rate it is.</summary>
    [Fact]
    public void OneCountOverTheLargestBucket_PublishesItsRate_NotZero()
    {
        var seconds = TrendBuckets.MaxBucketMinutes * 60L;

        var point = TrendPoint(Bucket(1, seconds), TrendBuckets.MaxBucketMinutes);

        Assert.Equal(0.000012, point.GetProperty("per_second").GetDouble(), precision: 12);
        Assert.Equal(1, point.GetProperty("delta_value").GetInt64());
        Assert.Equal(seconds, point.GetProperty("sample_interval_seconds").GetInt64());
    }

    /// <summary>No bucket width on the ladder, nor the day-wide maximum, turns a single count into a rate of 0, and
    /// each publishes it within the two digits' own rounding (5%) of delta over seconds.</summary>
    [Fact]
    public void OneCountOverEveryBucketWidth_PublishesANonzeroRate()
    {
        foreach (var width in TrendBuckets.LadderMinutes)
        {
            var seconds = width * 60L;

            var rate = TrendPoint(Bucket(1, seconds), width).GetProperty("per_second").GetDouble();

            Assert.True(rate > 0, $"a {width}-minute bucket with one count published a per_second of 0");
            Assert.InRange(rate * seconds, 0.95, 1.05);
        }
    }

    /// <summary>The busiest single collection is rated by the same rule: one count in the longest interval the
    /// calculator rates (3,600 s) keeps its two digits.</summary>
    [Fact]
    public void ThePeakRate_IsRoundedByTheSameRule()
    {
        var point = TrendPoint(Bucket(1, 86400, peak: 1.0 / 3600), 1440);

        Assert.Equal(0.00028, point.GetProperty("peak_per_second").GetDouble(), precision: 12);
    }

    /// <summary>The latest-snapshot row is rated by the same rule: one count in the longest interval the calculator
    /// rates keeps two significant digits, and the row keeps the running total beside it.</summary>
    [Fact]
    public void ALatestRow_IsRoundedByTheSameRule_AndKeepsItsTotal()
    {
        var row = TrendPayloads.PerfmonLatestRow("Number of Deadlocks/sec", "", 37, 1, 3600, PerfmonCounterTypes.PerfCounterBulkCount);

        Assert.Equal(0.00028, Assert.IsType<double>(row["per_second"]), precision: 12);
        Assert.Equal(37L, row["value"]);
    }
}
