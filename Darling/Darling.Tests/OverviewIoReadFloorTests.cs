/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Ui;
using ScottPlot.Plottables;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7624: the Overview I/O lane marked any point above the band at 2 ms or more, however few reads it held, so one slow read in a
/// bucket drew a red dot the web strip (#7621) did not. The lane now marks a point only when its reads reach 250 per fifteen minutes
/// of the span they were counted over. These tests run the real lane and count the red dots ScottPlot holds, and check the floor
/// for a bucket, for a collector slower than the bucket, and for the point's own read count.
/// </summary>
public sealed class OverviewIoReadFloorTests
{
    private const double Bar = PgTargetScorer.IoBaselineBucketMinimumReads;

    private static readonly DateTime T0 = new(2026, 11, 3, 0, 0, 0, DateTimeKind.Unspecified);

    private static BaselineBucket Band() => new() { Mean = 1, StdDev = 0.5, SampleCount = 500 }; // band 0..2 ms

    private static List<(double Time, double Value)> Slow(int count, double ms = 5) =>
        Enumerable.Range(0, count).Select(i => (T0.AddMinutes(i).ToOADate(), ms)).ToList();

    /// <summary>The marker scatter the lane adds on top of the line: no line, just dots.</summary>
    private static int RedDots(ScottPlot.WPF.WpfPlot chart) =>
        chart.Plot.GetPlottables<Scatter>().Where(s => s.LineWidth == 0).Sum(s => s.Data.GetScatterPoints().Count());

    private static int DrawIo(List<(double, double)> data, IReadOnlyList<long>? reads, long minReads, double minAnomalyValue = 2)
    {
        var dots = -1;
        StaTestThread.Run(() =>
        {
            var control = new CorrelatedTimelineLanesControl();
            control.Initialize(null!, 1);
            control.UpdateLane(control.FileIoChart, "I/O ms", data, "#81C784", baseline: Band(),
                minAnomalyValue: minAnomalyValue, pointReads: reads, minPointReads: minReads);
            dots = RedDots(control.FileIoChart);
        });
        return dots;
    }

    [Fact]
    public void APointAboveTheBandWithTooFewReads_GetsNoMarker()
    {
        var data = Slow(10);
        var fewReads = Enumerable.Repeat(3L, 10).ToList();

        Assert.Equal(0, DrawIo(data, fewReads, OverviewIoReadFloor.MinReadsFor(1, Bar)));
    }

    [Fact]
    public void TheSamePointWithEnoughReads_GetsAMarker()
    {
        var data = Slow(10);
        var manyReads = Enumerable.Repeat(5_000L, 10).ToList();

        Assert.Equal(10, DrawIo(data, manyReads, OverviewIoReadFloor.MinReadsFor(1, Bar)));
    }

    [Fact]
    public void OnlyThePointsWhoseReadsReachTheFloor_AreMarked()
    {
        var data = Slow(10);
        var reads = Enumerable.Range(0, 10).Select(i => i % 2 == 0 ? 5_000L : 3L).ToList();

        Assert.Equal(5, DrawIo(data, reads, OverviewIoReadFloor.MinReadsFor(1, Bar)));
    }

    [Fact]
    public void TheTwoMillisecondFloorStillApplies_WhateverTheReads()
    {
        var manyReads = Enumerable.Repeat(5_000L, 10).ToList();
        Assert.Equal(0, DrawIo(Slow(10, ms: 1.5), manyReads, OverviewIoReadFloor.MinReadsFor(1, Bar))); // inside the band

        // Above the band (upper is 2) but under a 3 ms floor: no marker even with plenty of reads.
        Assert.Equal(0, DrawIo(Slow(10, ms: 2.5), manyReads, OverviewIoReadFloor.MinReadsFor(1, Bar), minAnomalyValue: 3));
    }

    [Fact]
    public void ALaneWithNoReadCounts_KeepsItsMarkersAsBefore()
    {
        Assert.Equal(10, DrawIo(Slow(10), reads: null, minReads: 0));
    }

    [Theory]
    [InlineData(1, 17)]   // 250 per 15 minutes, a minute's worth, rounded up
    [InlineData(5, 84)]
    [InlineData(15, 250)]
    [InlineData(30, 500)]
    [InlineData(0, 250)]  // a span the read did not state counts as fifteen minutes, as the web strip does
    public void TheFloor_ScalesWithTheBucketWidth(int bucketMinutes, long expected)
    {
        var times = new[] { T0, T0.AddMinutes(bucketMinutes) };
        Assert.Equal(expected, OverviewIoReadFloor.MinReadsFor(bucketMinutes, times, Bar));
    }

    [Fact]
    public void ACollectorSlowerThanTheBucket_UsesTheCollectionStepAsTheSpan()
    {
        // One-minute buckets, but the collector runs every five minutes: each point holds five minutes of reads.
        var times = Enumerable.Range(0, 12).Select(i => T0.AddMinutes(5 * i)).ToList();
        Assert.Equal(84, OverviewIoReadFloor.MinReadsFor(5, times, Bar));
        Assert.Equal(84, OverviewIoReadFloor.MinReadsFor(1, times, Bar));

        // A point of 60 reads clears a one-minute floor (17) but not the five-minute floor (84).
        var data = times.Select(t => (t.ToOADate(), 5.0)).ToList();
        var reads = Enumerable.Repeat(60L, 12).ToList();
        Assert.Equal(12, DrawIo(data, reads, OverviewIoReadFloor.MinReadsFor(1, new[] { T0, T0.AddMinutes(1) }, Bar)));
        Assert.Equal(0, DrawIo(data, reads, OverviewIoReadFloor.MinReadsFor(1, times, Bar)));
    }

    [Fact]
    public void ABucketWiderThanTheCollectionStep_KeepsTheBucketWidthAsTheSpan()
    {
        // 30-minute buckets over a one-minute collector: the step (1) is under the bucket (30), so the bucket is the span.
        var times = Enumerable.Range(0, 12).Select(i => T0.AddMinutes(30 * i)).ToList();
        Assert.Equal(500, OverviewIoReadFloor.MinReadsFor(30, times, Bar));
    }

    [Fact]
    public void AnOverviewPoint_ReadsThroughTheSharedDecision()
    {
        // band 0..2, floor 2 ms: above the band with enough reads, above with too few, below the band with too few (the web strip drops that too).
        Assert.True(OverviewIoReadFloor.IsAnomaly(5, 2, 0, 2, reads: 250, minReads: 250));
        Assert.False(OverviewIoReadFloor.IsAnomaly(5, 2, 0, 2, reads: 249, minReads: 250));
        Assert.False(OverviewIoReadFloor.IsAnomaly(1, 2, 1.5, 2, reads: 10, minReads: 250));
        Assert.True(OverviewIoReadFloor.IsAnomaly(1, 2, 1.5, 2, reads: 1_000, minReads: 250));
        Assert.True(OverviewIoReadFloor.IsAnomaly(5, 2, 0, 2, reads: 0, minReads: 0));
    }

    private static readonly List<(DateTime Time, long Reads, int BucketMinutes)> TwoMinutePoints = new()
    {
        (T0, 900, 1), (T0.AddMinutes(1), 10, 1), (T0.AddMinutes(2), 400, 1)
    };

    [Fact]
    public void LaneInputs_GiveTheReadsInPointOrder_AndTheFloorFromTheBucketWidth()
    {
        var (reads, minReads) = OverviewIoReadFloor.LaneInputs(TwoMinutePoints, Bar);
        Assert.Equal(new long[] { 900, 10, 400 }, reads);
        Assert.Equal(17, minReads); // one-minute bucket, one-minute step: 250 / 15 rounded up
    }

    [Fact]
    public void LaneInputs_TakeTheFloorFromTheStep_WhenTheCollectorIsSlowerThanTheBucket()
    {
        var points = Enumerable.Range(0, 4).Select(i => (T0.AddMinutes(5 * i), (long)i, 1)).ToList();
        Assert.Equal(84, OverviewIoReadFloor.LaneInputs(points, Bar).MinReads);
    }

    [Fact]
    public void LaneInputs_OfNoPoints_HaveNoReadsAndTheFifteenMinuteFloor()
    {
        var (reads, minReads) = OverviewIoReadFloor.LaneInputs(new List<(DateTime, long, int)>(), Bar);
        Assert.Empty(reads);
        Assert.Equal(250, minReads);
    }
}
