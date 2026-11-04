/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1944: chart lines must not connect across collection gaps. The mechanism is cadence-derived — three
/// times the series' median positive spacing — so these pin both directions: real outages break, and
/// ordinary jitter, duplicate timestamps, and slow-but-steady series do NOT shatter.
/// </summary>
public class TimeSeriesGapsTests
{
    private static double[] MinuteCadence(int count, double startDay = 45_000.0)
        => Enumerable.Range(0, count).Select(i => startDay + i / 1440.0).ToArray();

    [Fact]
    public void NoGaps_ReturnsTheOriginalArrays_ByReference()
    {
        var xs = MinuteCadence(30);
        var ys = xs.Select(_ => 1.0).ToArray();

        var (outXs, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Same(xs, outXs);
        Assert.Same(ys, outYs);
    }

    [Fact]
    public void AnOutage_BreaksTheLine_WithOneNaNInsideTheGap()
    {
        /* 20 one-minute points, a 60-minute hole, 20 more. */
        var before = MinuteCadence(20);
        var after = MinuteCadence(20, before[^1] + 60.0 / 1440.0);
        var xs = before.Concat(after).ToArray();
        var ys = xs.Select(_ => 5.0).ToArray();

        var (outXs, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Equal(xs.Length + 1, outXs.Length);
        var nanIndexes = outYs.Select((y, i) => (y, i)).Where(p => double.IsNaN(p.y)).Select(p => p.i).ToArray();
        var nan = Assert.Single(nanIndexes);

        /* The break sits strictly inside the gap, between the last point before and the first point after. */
        Assert.True(outXs[nan] > before[^1] && outXs[nan] < after[0],
            $"break at {outXs[nan]} is not inside the gap ({before[^1]} .. {after[0]})");

        /* Every real point survives, in order, with its own value. */
        Assert.Equal(xs, outXs.Where((_, i) => i != nan));
        Assert.All(outYs.Where((_, i) => i != nan), y => Assert.Equal(5.0, y));
    }

    [Fact]
    public void TwoOutages_ProduceTwoBreaks()
    {
        var a = MinuteCadence(10);
        var b = MinuteCadence(10, a[^1] + 30.0 / 1440.0);
        var c = MinuteCadence(10, b[^1] + 45.0 / 1440.0);
        var xs = a.Concat(b).Concat(c).ToArray();
        var ys = xs.Select(_ => 2.0).ToArray();

        var (outXs, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Equal(xs.Length + 2, outXs.Length);
        Assert.Equal(2, outYs.Count(double.IsNaN));
    }

    [Fact]
    public void SchedulerJitter_StaysConnected()
    {
        /* One-minute cadence with up to 2.5x jitter on one spacing: inside the 3x threshold, no break —
           a chart that shatters on ordinary scheduler drift is the failure mode the multiplier exists
           to prevent. */
        var xs = MinuteCadence(20).ToArray();
        for (var i = 10; i < 20; i++)
        {
            xs[i] += 1.5 / 1440.0;
        }
        var ys = xs.Select(_ => 3.0).ToArray();

        var (outXs, _) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Same(xs, outXs);
    }

    [Fact]
    public void DuplicateTimestamps_DoNotDragTheCadenceToZero()
    {
        /* Several series sharing one collection cycle produce duplicate timestamps. Zero spacings must not
           become the median (threshold 0 would break the line between every distinct pair). */
        var xs = MinuteCadence(10).SelectMany(x => new[] { x, x }).ToArray();
        var ys = xs.Select(_ => 4.0).ToArray();

        var (outXs, _) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Same(xs, outXs);
    }

    [Fact]
    public void SlowSeries_WithProportionalGap_StillBreaks()
    {
        /* A daily collector with a five-day hole: the cadence derivation scales, no constant involved. */
        var xs = Enumerable.Range(0, 10).Select(i => 45_000.0 + i).Concat(
                 Enumerable.Range(0, 10).Select(i => 45_014.0 + i)).ToArray();
        var ys = xs.Select(_ => 6.0).ToArray();

        var (_, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Equal(1, outYs.Count(double.IsNaN));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void TooFewPointsForACadence_PassThrough(int count)
    {
        var xs = MinuteCadence(Math.Max(count, 1)).Take(count).ToArray();
        var ys = xs.Select(_ => 1.0).ToArray();

        var (outXs, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Same(xs, outXs);
        Assert.Same(ys, outYs);
    }

    [Fact]
    public void AllDuplicateTimestamps_NoMeasurableCadence_PassThrough()
    {
        var xs = Enumerable.Repeat(45_000.0, 8).ToArray();
        var ys = xs.Select(_ => 1.0).ToArray();

        var (outXs, _) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Same(xs, outXs);
    }

    /* #7508: the Query Store Duration Trend is hourly points, then one point per interval from the seam (routing.raw_from). */
    private const double Start = 45_000.0;
    private static double Hours(int h) => Start + h / 24.0;
    private static double Minutes(double afterHours, int m) => Start + afterHours / 24.0 + m / 1440.0;

    private static (double[] Xs, double[] Seam) TwoGrains(int[] hourlyAt, int rawMinutes, int[]? rawSkipped = null)
    {
        var xs = hourlyAt.Select(Hours).Concat(
            Enumerable.Range(0, rawMinutes).Where(m => rawSkipped is null || !rawSkipped.Contains(m)).Select(m => Minutes(22, m))).ToArray();
        return (xs, new[] { Hours(22) });
    }

    private static int Breaks(double[] ys) => ys.Count(double.IsNaN);

    [Fact]
    public void ASeriesJoiningTwoGrains_WithNoSeam_ShattersTheHourlyPart_AsItAlwaysDid()
    {
        var (xs, _) = TwoGrains(Enumerable.Range(0, 22).ToArray(), 90);
        var ys = xs.Select(_ => 1.0).ToArray();

        var (_, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);

        Assert.Equal(22, Breaks(outYs));
    }

    [Fact]
    public void ASeriesJoiningTwoGrains_WithTheSeam_DrawsOneLine()
    {
        var (xs, seam) = TwoGrains(Enumerable.Range(0, 22).ToArray(), 90);
        var ys = xs.Select(_ => 1.0).ToArray();

        var (outXs, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys, seam[0]);

        Assert.Equal(0, Breaks(outYs));
        Assert.Same(xs, outXs);
    }

    [Fact]
    public void AnOutageInsideTheHourlySide_StillBreaks_WithTheSeam()
    {
        var (xs, seam) = TwoGrains(Enumerable.Range(0, 10).Concat(Enumerable.Range(16, 6)).ToArray(), 90);
        var ys = xs.Select(_ => 1.0).ToArray();

        var (_, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys, seam[0]);

        Assert.Equal(1, Breaks(outYs));
    }

    [Fact]
    public void AnOutageInsideTheRawSide_StillBreaks_WithTheSeam()
    {
        var (xs, seam) = TwoGrains(Enumerable.Range(0, 22).ToArray(), 90, Enumerable.Range(30, 20).ToArray());
        var ys = xs.Select(_ => 1.0).ToArray();

        var (_, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys, seam[0]);

        Assert.Equal(1, Breaks(outYs));
    }

    [Fact]
    public void AnOutageAcrossTheSeam_StillBreaks_WhenTheHourlySideIsTooShortForASpacing()
    {
        /* Hourly points at 00:00 and 01:00, a silence, then one-minute points from 20:00. The hourly side borrows the raw side's
           threshold, so the one-hour step and the silence both break. */
        var xs = new[] { Hours(0), Hours(1) }.Concat(Enumerable.Range(0, 60).Select(m => Minutes(20, m))).ToArray();
        var ys = xs.Select(_ => 1.0).ToArray();

        var (_, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys, Hours(20));

        Assert.Equal(2, Breaks(outYs));
    }

    [Fact]
    public void AnOutageInsideARawSideTooShortForASpacing_StillBreaks()
    {
        /* 22 hourly points, then 2 raw points 19 hours apart. The raw side borrows the hourly side's threshold. */
        var xs = Enumerable.Range(0, 22).Select(h => Hours(h)).Concat(new[] { Hours(22), Hours(41) }).ToArray();
        var ys = xs.Select(_ => 1.0).ToArray();

        var (_, outYs) = TimeSeriesGaps.BreakAtGaps(xs, ys, Hours(22));

        Assert.Equal(1, Breaks(outYs));
    }

    [Fact]
    public void WithNeitherSideLongEnoughForASpacing_TheSeamAddsNothing()
    {
        var xs = new[] { Hours(0), Hours(1), Hours(20), Minutes(20, 1) };
        var ys = xs.Select(_ => 1.0).ToArray();

        var plain = TimeSeriesGaps.BreakAtGaps(xs, ys);
        var seamed = TimeSeriesGaps.BreakAtGaps(xs, ys, Hours(20));

        Assert.Equal(1, Breaks(plain.Ys));
        Assert.Equal(plain.Xs, seamed.Xs);
    }

    [Fact]
    public void ASeamWithEveryPointOnOneSideOfIt_IsThePlainRule()
    {
        var xs = MinuteCadence(30).Concat(MinuteCadence(30, 45_000.5)).ToArray();
        var ys = xs.Select(_ => 1.0).ToArray();

        var plain = TimeSeriesGaps.BreakAtGaps(xs, ys);
        var past = TimeSeriesGaps.BreakAtGaps(xs, ys, 46_000.0);
        var before = TimeSeriesGaps.BreakAtGaps(xs, ys, 44_000.0);

        Assert.Equal(1, Breaks(plain.Ys));
        Assert.Equal(plain.Xs, past.Xs);
        Assert.Equal(plain.Xs, before.Xs);
    }
}
