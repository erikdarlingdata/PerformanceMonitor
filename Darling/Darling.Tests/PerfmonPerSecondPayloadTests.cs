/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling's <c>get_perfmon_stats</c> row, built without a store: the reader's row goes through
/// <see cref="DarlingMcpDataTools.PerfmonRowPayload"/> exactly as the tool sends it. A rate counter's <c>value</c> is
/// its running total, so the row also carries <c>per_second</c>, its delta over the seconds since the previous
/// collection, by the rule both desktop charts plot with. Lite's tool builds the same row through the same shared
/// builder, and <c>PerfmonCounterTypeReadTests</c> in Lite.Tests checks it end to end. The store half (the reader
/// selecting the interval) is <c>DarlingMcpDataToolsSurfaceAndSqlTests.PerfmonSql_LatestSnapshot_ValueAndDelta</c>
/// here and <c>PerfmonCounterTypeLivePostgresTests</c> against a real store.
/// </summary>
public sealed class PerfmonPerSecondPayloadTests
{
    private static JsonElement Payload(string counter, long value, long? delta, int? type, int? interval, string instance = "") =>
        JsonDocument.Parse(JsonSerializer.Serialize(
            DarlingMcpDataTools.PerfmonRowPayload(new DarlingDataReader.PerfmonRow(counter, instance, value, delta, type, interval)),
            McpHelpers.JsonOptions)).RootElement;

    [Fact]
    public void ARateRow_CarriesItsDeltaOverTheStoredInterval_BesideTheRunningTotal()
    {
        var row = Payload("Batch Requests/sec", 11_641, 66, PerfmonCounterTypes.PerfCounterBulkCount, 300);

        Assert.Equal(0.22, row.GetProperty("per_second").GetDouble(), precision: 10);
        Assert.Equal(11_641, row.GetProperty("value").GetInt64());
        Assert.Equal(66, row.GetProperty("delta_value").GetInt64());
        Assert.Equal("rate", row.GetProperty("counter_kind").GetString());

        /* The keys the tool published before stay where they were; per_second is added after them. */
        Assert.Equal(
            new[] { "counter_name", "instance_name", "value", "delta_value", "cntr_type", "counter_kind", "per_second" },
            row.EnumerateObject().Select(p => p.Name).ToArray());
    }

    /// <summary>An interval of 0 is the calculator's "no delta was knowable" (a first collection, a counter reset,
    /// a restart): the rate is unknown, which is null, not 0.</summary>
    [Fact]
    public void ARateRowWithNoKnowableDelta_SaysNull_NotZero()
    {
        var row = Payload("SQL Compilations/sec", 4_000, 0, PerfmonCounterTypes.PerfCounterBulkCount, 0);

        Assert.Equal(JsonValueKind.Null, row.GetProperty("per_second").ValueKind);
    }

    /// <summary>A gauge's value is its reading, and an average's numerator is not a rate: neither row has the key,
    /// so a caller never takes its absence of a rate for an unknown one.</summary>
    [Fact]
    public void AGaugeRow_AndAnAverageRow_HaveNoPerSecondKey()
    {
        var gauge = Payload("Total Server Memory (KB)", 8_000_000, null, PerfmonCounterTypes.PerfCounterLargeRawCount, null);
        Assert.False(gauge.TryGetProperty("per_second", out _));
        Assert.Equal(8_000_000, gauge.GetProperty("value").GetInt64());
        Assert.Equal(JsonValueKind.Null, gauge.GetProperty("delta_value").ValueKind);

        var average = Payload("Lock waits", 5_000, 40, PerfmonCounterTypes.PerfAverageBulk, 300, "Average wait time (ms)");
        Assert.False(average.TryGetProperty("per_second", out _));
    }

    /// <summary>A row written before the type was stored is rated the way the charts rate it: by a name that says
    /// <c>/sec</c>.</summary>
    [Fact]
    public void ARowWithNoStoredType_IsRatedByItsName()
    {
        Assert.Equal(0.2, Payload("Legacy Transactions/sec", 900, 60, null, 300).GetProperty("per_second").GetDouble(), precision: 10);
        Assert.False(Payload("Legacy Counter", 120, 20, null, 300).TryGetProperty("per_second", out _));
    }

    /// <summary>A counter that seldom fires keeps its rate and its running total side by side. One deadlock in 300 s is
    /// 0.0033 a second; one in the longest interval the calculator rates (3,600 s) keeps two significant digits, where
    /// four decimals made it 0.0003. The rule itself, <c>TrendPayloads.RoundRate</c>, is pinned value by value in
    /// Lite.Tests' <c>PerfmonPerSecondRoundingTests</c>.</summary>
    [Fact]
    public void ARateThatSeldomFires_KeepsItsRate_BesideItsTotal()
    {
        var fiveMinutes = Payload("Number of Deadlocks/sec", 37, 1, PerfmonCounterTypes.PerfCounterBulkCount, 300);
        Assert.Equal(0.0033, fiveMinutes.GetProperty("per_second").GetDouble(), precision: 12);
        Assert.Equal(37, fiveMinutes.GetProperty("value").GetInt64());

        var anHour = Payload("Number of Deadlocks/sec", 37, 1, PerfmonCounterTypes.PerfCounterBulkCount, 3600);
        Assert.Equal(0.00028, anHour.GetProperty("per_second").GetDouble(), precision: 12);
        Assert.Equal(37, anHour.GetProperty("value").GetInt64());
    }

    /// <summary>Darling's <c>get_perfmon_trend</c> builds its points through the shared <c>TrendPayloads.PerfmonTrend</c>.
    /// The largest bucket a caller can ask for is a day, 86,400 s, and one deadlock in it is a rate that four decimals
    /// erase. It publishes as the rate it is, and the busiest collection is rounded the same way.</summary>
    [Fact]
    public void ADayWideTrendBucket_WithOneCount_PublishesItsRate_NotZero()
    {
        var seconds = TrendBuckets.MaxBucketMinutes * 60L;
        var bucket = new PerfmonBucketPoint(
            new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified), AvgValue: 0, MaxValue: 0, LastValue: 37,
            RatedDelta: 1, RatedSeconds: seconds, UnknowableCollections: 0, UnknowableDelta: null, UnrecordedDelta: null,
            PeakPerSecond: 1.0 / 3600, CntrType: PerfmonCounterTypes.PerfCounterBulkCount);

        using var doc = JsonDocument.Parse(TrendPayloads.PerfmonTrend(
            "SRV1", "Number of Deadlocks/sec", 168, new[] { bucket }, TrendBuckets.MaxBucketMinutes, requested: true,
            autoBudget: TrendBuckets.McpPointBudget, discontinuities: Array.Empty<object>()));
        var point = doc.RootElement.GetProperty("trend")[0];

        Assert.Equal(0.000012, point.GetProperty("per_second").GetDouble(), precision: 12);
        Assert.Equal(0.00028, point.GetProperty("peak_per_second").GetDouble(), precision: 12);
        Assert.Equal(1, point.GetProperty("delta_value").GetInt64());
    }
}
