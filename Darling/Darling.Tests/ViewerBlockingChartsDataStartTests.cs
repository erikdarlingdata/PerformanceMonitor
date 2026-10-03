/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the Blocking tab's event and zero-fill charts draw as an event, and what their "Showing since" note names (#4966): the earlier
/// of the coverage start and the earliest bucket the chart actually draws. A zero bucket is the chart's baseline, not an event.
/// Pure logic: no WPF object is built, so every fact runs on any platform.
/// </summary>
public sealed class ViewerBlockingChartsDataStartTests
{
    private static readonly DateTime RangeStart = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    private static DateTime At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    [Fact]
    public void ACountChart_DrawsOnlyTheNonZeroBuckets_AsEvents()
    {
        var data = new[] { new BlockingTrendPoint(At(1), 0), new BlockingTrendPoint(At(2), 3), new BlockingTrendPoint(At(3), 1) };

        Assert.Equal(new DateTime?[] { At(2), At(3) }, ViewerBlockingChartsDataStart.BlockingTrendTimesDrawn(data).ToArray());
        Assert.Empty(ViewerBlockingChartsDataStart.BlockingTrendTimesDrawn(Array.Empty<BlockingTrendPoint>()));
    }

    /* The first non-zero bucket is later than the coverage: the note names the coverage. An event earlier than the coverage (a first
       collection's history) wins. A chart with only zero buckets still names the coverage. */
    [Fact]
    public void TheStart_IsTheEarlierOfTheCoverageAndTheFirstBucketDrawn()
    {
        var later = new[] { new BlockingTrendPoint(At(2), 4) };
        var earlier = new[] { new BlockingTrendPoint(At(0, 6), 1), new BlockingTrendPoint(At(2), 4) };
        var zeros = new[] { new BlockingTrendPoint(At(1), 0) };

        Assert.Equal(At(2), StartOf(At(2), later));
        Assert.Equal(At(2), StartOf(At(2), zeros));
        Assert.Equal(At(0, 6), StartOf(At(2), earlier));
    }

    [Fact]
    public void AnEmptyRead_NamesTheCoverageAlone_AndNothingWithoutOne()
    {
        Assert.Equal(At(2), StartOf(At(2), Array.Empty<BlockingTrendPoint>()));
        Assert.Null(StartOf(null, Array.Empty<BlockingTrendPoint>()));
    }

    [Fact]
    public void AProbeThatThrows_CostsOnlyTheNote()
    {
        /* A failed probe has no coverage; the note is dropped even though buckets were drawn. */
        var drawn = new[] { new BlockingTrendPoint(At(3), 2) };
        Assert.Null(ViewerEventDataStart.Of(null, ViewerEventDataStart.EarliestOf(ViewerBlockingChartsDataStart.BlockingTrendTimesDrawn(drawn).ToList()), probeFailed: true));
    }

    private static DateTime? StartOf(DateTime? coverage, BlockingTrendPoint[] drawn) =>
        ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(ViewerBlockingChartsDataStart.BlockingTrendTimesDrawn(drawn).ToList()));
}
