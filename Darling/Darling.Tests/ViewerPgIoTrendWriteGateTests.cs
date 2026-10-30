/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6540 review: the Viewer's I/O Trend must not plot Writes/sec on a server with no write counters (Amazon
/// Aurora). The reader coalesces that side to a measured 0, so an ungated chart draws a flat "idle" line; the
/// web's <c>get_pg_io_trend</c> nulls it on <c>write_counters_tracked</c> false, and the Viewer wrapper must too.
/// </summary>
public sealed class ViewerPgIoTrendWriteGateTests
{
    private static DarlingPgTrendReader.PgIoTrendPoint Point(bool writesTracked) => new(
        new DateTime(2026, 3, 5, 10, 0, 0, DateTimeKind.Utc), 60, Reads: 600, ReadTimeMs: 0, Writes: 0, WriteTimeMs: 0,
        Extends: 0, Hits: 0, Evictions: 0, ReadBytes: 0, WriteBytes: 0,
        ReadsPerSecond: 10, WritesPerSecond: writesTracked ? 3.5 : 0.0, ExtendsPerSecond: 0, HitsPerSecond: 0,
        ReadBytesPerSecond: null, WriteBytesPerSecond: null, AvgReadMs: null, AvgWriteMs: null, CacheHitPct: null,
        WriteCountersTracked: writesTracked, BytesMeasured: false, BytesEstimable: false, CounterReset: false);

    [Fact]
    public void WithoutWriteCounters_NoWriteRateIsPlotted()
    {
        var points = new[] { Point(writesTracked: false), Point(writesTracked: false) };
        var trend = new ViewerDataService.PgIoTrend("client backend / normal", 1, points, false, false,
            WriteCountersTracked: points.Any(p => p.WriteCountersTracked));

        Assert.False(trend.WriteCountersTracked);
        Assert.All(points, p => Assert.Null(trend.WritesPerSecondOf(p)));
    }

    [Fact]
    public void WithWriteCounters_TheMeasuredWriteRateIsPlotted()
    {
        var points = new[] { Point(writesTracked: true) };
        var trend = new ViewerDataService.PgIoTrend("client backend / normal", 1, points, false, false,
            WriteCountersTracked: points.Any(p => p.WriteCountersTracked));

        Assert.Equal(3.5, trend.WritesPerSecondOf(points[0]));
    }
}
