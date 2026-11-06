/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A web time-series chart breaks its line, area and stacked bands where no samples were collected (#5644 W7): the service host
/// is off overnight, and a line drawn point to point across the silence read as data. These run the shipped
/// <c>charts.js</c> under Node (<c>web-chart-gap-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebChartGapBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-gap-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped chart script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the chart gap harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart gap harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static int N(JsonElement r, string scenario, string mark) => r.GetProperty(scenario).GetProperty(mark).GetInt32();

    [Fact]
    public void ALineChart_WithNoGap_DrawsOneLinePerSeries()
    {
        var r = Run();
        Assert.Equal(2, N(r, "noGapLine", "lines"));
        Assert.Equal(0, N(r, "noGapLine", "dots"));
    }

    [Fact]
    public void ALineChart_BreaksEachSeriesAtACollectionGap()
    {
        var r = Run();
        Assert.Equal(4, N(r, "gapLine", "lines"));
        Assert.Equal(0, N(r, "gapLine", "dots"));
    }

    [Fact]
    public void NoLineSegment_SpansTheGap()
    {
        var r = Run();
        var runs = r.GetProperty("gapLineXs").EnumerateArray().Select(l => l.EnumerateArray().Select(x => x.GetDouble()).ToArray()).ToArray();
        Assert.Equal(2, runs.Length);
        // The widest step inside either run is one 1-minute bucket; the gap is 7 hours, so no step comes near it.
        var steps = runs.SelectMany(run => run.Zip(run.Skip(1), (a, b) => b - a)).ToArray();
        Assert.True(steps.Max() < runs[1][0] - runs[0][^1], "a step inside a run is shorter than the gap between runs");
    }

    [Fact]
    public void AnAreaChart_BreaksItsFillAndLineAtTheGap()
    {
        var r = Run();
        Assert.Equal(4, N(r, "gapArea", "lines"));
        Assert.Equal(4, N(r, "gapArea", "areas"));
    }

    [Fact]
    public void AStackedChart_BreaksEachBandAtTheGap()
    {
        var r = Run();
        Assert.Equal(2, N(r, "noGapStacked", "areas"));
        Assert.Equal(4, N(r, "gapStacked", "areas"));
    }

    [Fact]
    public void AStackedBarChart_IsLeftAlone()
    {
        var r = Run();
        Assert.Equal(N(r, "noGapStackedBar", "bars"), N(r, "gapStackedBar", "bars"));
        Assert.Equal(0, N(r, "gapStackedBar", "lines"));
        Assert.Equal(0, N(r, "gapStackedBar", "areas"));
    }

    [Fact]
    public void TheDualAxisOverlay_BreaksAtTheGapToo()
    {
        var r = Run();
        Assert.Equal(1, N(r, "noGapOverlay", "overlays"));
        Assert.Equal(2, N(r, "gapOverlay", "overlays"));
    }

    [Fact]
    public void ALoneBucketAfterAGap_IsADot_AndTheStretchBeforeItStaysALine()
    {
        var r = Run();
        Assert.Equal(1, N(r, "loneAfterGap", "lines"));
        Assert.Equal(1, N(r, "loneAfterGap", "dots"));
    }

    [Fact]
    public void OneOrTwoBuckets_NeverBreak()
    {
        var r = Run();
        Assert.Equal(2, N(r, "single", "dots"));
        Assert.Equal(0, N(r, "single", "lines"));
        Assert.Equal(2, N(r, "two", "lines"));
        Assert.Equal(0, N(r, "two", "dots"));
    }

    [Fact]
    public void AMissedCycle_BreaksOnlyPastTheFloorAndThreeTimesTheMedianInterval()
    {
        var r = Run();
        Assert.Equal(1, N(r, "smallMiss", "lines"));
        Assert.Equal(2, N(r, "bigMiss", "lines"));
        Assert.Equal(1, N(r, "slowCollector", "lines"));
        Assert.Equal(2, N(r, "slowCollectorBreak", "lines"));
    }

    [Fact]
    public void TheGapThreshold_IsPerSeries_ACoarseSeriesOverFineBucketsIsOneLineUnlessItHasAGapOfItsOwn()
    {
        // 1-minute buckets, a series sampled every 15 minutes: the rows' median interval is 1 minute (threshold 5 minutes), which
        // used to cut the 15-minute series into a dot per sample. Its own spacing makes the threshold 45 minutes.
        var r = Run();
        Assert.Equal(1, N(r, "coarseOverlayNoGap", "overlays"));
        Assert.Equal(0, N(r, "coarseOverlayNoGap", "dots"));
        Assert.Equal(1, N(r, "coarseOverlayNoGap", "lines"));
        Assert.Equal(2, N(r, "coarseOverlayGap", "overlays"));    // a real 7 hour silence still breaks it
        Assert.Equal(0, N(r, "coarseOverlayGap", "dots"));
        Assert.Equal(2, N(r, "coarsePrimary", "lines"));
        Assert.Equal(0, N(r, "coarsePrimary", "dots"));
    }

    [Fact]
    public void ASeriesOfEvents_HasNoCadenceToBeSilentAgainst_SoItDrawsOneLineAndNoLoneDots()
    {
        // #5644 H4b: clusters of events with minutes to hours between them have a tiny median interval; the 5 minute floor
        // used to cut that into mostly lone dots.
        var r = Run();
        Assert.Equal(1, N(r, "eventsLine", "lines"));
        Assert.Equal(0, N(r, "eventsLine", "dots"));
        Assert.Equal(1, N(r, "eventsArea", "lines"));
        Assert.Equal(1, N(r, "eventsArea", "areas"));
        Assert.Equal(0, N(r, "eventsArea", "dots"));
        Assert.Equal("Infinity", r.GetProperty("eventsThreshold").GetString());
    }

    [Fact]
    public void ASampledSeriesMissingSomeSamples_StillBreaksAtALongSilence()
    {
        var r = Run();
        Assert.Equal(2, N(r, "jitterGap", "lines"));
        Assert.Equal(0, N(r, "jitterGap", "dots"));
    }

    [Fact]
    public void TheThreshold_HasNothingToCompareUnderThreeBuckets_AndNeverFallsUnderTheFloor()
    {
        var r = Run().GetProperty("threshold");
        Assert.Equal("Infinity", r.GetProperty("empty").GetString());
        Assert.Equal("Infinity", r.GetProperty("one").GetString());
        Assert.Equal("Infinity", r.GetProperty("two").GetString());
        Assert.Equal(5 * 60000, r.GetProperty("oneMinute").GetDouble());
        Assert.Equal(45 * 60000, r.GetProperty("fifteenMinute").GetDouble());
    }

    [Fact]
    public void SplitAtGaps_StartsANewRunWhereTheJumpExceedsTheThreshold()
    {
        var r = Run();
        Assert.Equal(new[] { "0,1,2", "3,4" }, r.GetProperty("split").EnumerateArray().Select(e => e.GetString()!).ToArray());
    }

    [Fact]
    public void ASeriesJoiningTwoGrains_DrawsOneLine_WhenTheSeamIsKnown()
    {
        var r = Run();
        Assert.Equal(1, N(r, "seamJoined", "lines"));
        Assert.Equal(0, N(r, "seamJoined", "dots"));
        Assert.Equal(1, N(r, "seamArea", "areas"));
        Assert.Equal(0, N(r, "seamArea", "dots"));
    }

    [Fact]
    public void ASeriesJoiningTwoGrains_WithNoSeam_KeepsTheOneRuleItAlwaysHad()
    {
        var r = Run();
        /* the hourly steps (and the step into the minute points) read as gaps: 22 lone dots and the minute stretch as a line */
        Assert.Equal(1, N(r, "seamAbsent", "lines"));
        Assert.Equal(22, N(r, "seamAbsent", "dots"));
        Assert.Equal(2, N(r, "seamAllRawOutage", "lines"));
        Assert.Equal(2, N(r, "seamBad", "lines"));
        Assert.Equal(1, N(r, "seamAllRaw", "lines"));
        var t = r.GetProperty("seamThreshold");
        Assert.Equal(t.GetProperty("plain").GetDouble(), t.GetProperty("oneSide").GetDouble());
    }

    [Fact]
    public void AnOutageInsideEitherSideOfTheSeam_StillBreaksTheLine()
    {
        var r = Run();
        Assert.Equal(2, N(r, "seamHourlyOutage", "lines"));
        Assert.Equal(0, N(r, "seamHourlyOutage", "dots"));
        Assert.Equal(2, N(r, "seamRawOutage", "lines"));
        Assert.Equal(0, N(r, "seamRawOutage", "dots"));
    }

    [Fact]
    public void AnOutageNextToASideTooShortForASpacing_StillBreaks()
    {
        var r = Run();
        /* Two hourly points, a 19 hour silence, then one-minute points: the hourly side borrows the raw side's threshold. */
        Assert.Equal(1, N(r, "seamShortHourlyOutage", "lines"));
        Assert.Equal(2, N(r, "seamShortHourlyOutage", "dots"));
        /* 22 hourly points, then 2 raw points 19 hours apart: the raw side borrows the hourly side's. */
        Assert.Equal(1, N(r, "seamShortRawOutage", "lines"));
        Assert.Equal(1, N(r, "seamShortRawOutage", "dots"));
        /* Neither side can judge: the seam adds nothing. */
        Assert.True(r.GetProperty("seamBothShortIsPlain").GetBoolean());
    }

    [Fact]
    public void EachSideOfTheSeam_IsJudgedAgainstItsOwnSpacing()
    {
        var t = Run().GetProperty("seamThreshold");
        Assert.Equal(3 * 60 * 60000, t.GetProperty("hourlySide").GetDouble());
        Assert.Equal(5 * 60000, t.GetProperty("rawSide").GetDouble());
        Assert.Equal(3 * 60 * 60000, t.GetProperty("crossing").GetDouble());
    }

    /// <summary>
    /// #7572: a quiet file's 15 minute buckets with readings at 5:45 PM, 6:00 PM, 7:30 PM and 11:45 PM. The steps (15, 90 and 255 minutes) show no
    /// cadence of their own, so the chart drew one line from 7:30 to 11:45. With the bucket the reply states, the line is 5:45 to 6:00 and the
    /// two later readings are dots, in a line, an area and a stacked chart alike.
    /// </summary>
    [Fact]
    public void ATrendWithAKnownBucket_BreaksAtAnyStepLongerThanOneBucket_AndShowsALoneReadingAsADot()
    {
        var r = Run();
        Assert.Equal(1, N(r, "bucketQuiet", "lines"));
        Assert.Equal(2, N(r, "bucketQuiet", "dots"));
        Assert.Equal(1, N(r, "bucketQuietArea", "areas"));
        Assert.Equal(2, N(r, "bucketQuietArea", "dots"));
        Assert.Equal(2, N(r, "bucketQuietStacked", "dots"));
        /* Without the bucket the same rows are one unbroken line. */
        Assert.Equal(0, N(r, "bucketQuietNoBucket", "dots"));
    }

    /// <summary>
    /// #7572: a bucket is the chart's width (1 minute for a day), not the collector's cadence. A collector scheduled every 5 minutes leaves a row
    /// every fifth bucket: the rows' own step (5 minutes, with half again for a collection a bucket early or late) is what a gap is judged by, so the
    /// chart stays one line, an outage still breaks it, and a strip whose reply states a 1 minute bucket stays one line too.
    /// </summary>
    [Fact]
    public void ACollectorSlowerThanTheBucket_KeepsItsLine_AndAnOutageStillBreaksIt()
    {
        var r = Run();
        Assert.Equal(1, N(r, "slowCollector", "lines"));
        Assert.Equal(0, N(r, "slowCollector", "dots"));
        Assert.Equal(2, N(r, "slowCollectorOutage", "lines"));
        Assert.Equal(15 * 60000, r.GetProperty("quietRowsStep").GetDouble());
        Assert.Equal(7.5 * 60000, r.GetProperty("slowCollectorStep").GetDouble());
        Assert.Equal(1, N(r, "stripSlowCollector", "lineRuns"));
        Assert.Equal(0, N(r, "stripSlowCollector", "dots"));
    }

    [Fact]
    public void ATrendWithAKnownBucket_StaysOneLine_WhenEveryBucketIsPresent_AndBreaksAtAFigureLessBucket()
    {
        var r = Run();
        Assert.Equal(1, N(r, "bucketFull", "lines"));
        Assert.Equal(0, N(r, "bucketFull", "dots"));
        /* A bucket whose row has no figure is not a point: the line stops before it and starts after it. */
        Assert.Equal(2, N(r, "bucketNullMiddle", "lines"));
        var t = r.GetProperty("bucketThreshold");
        Assert.Equal(15 * 60000, t.GetProperty("stated").GetDouble());
        Assert.Equal("16200000", t.GetProperty("ignoredWhenAbsent").GetString());
        Assert.Equal("16200000", t.GetProperty("bad").GetString());
        var minutes = t.GetProperty("minutes");
        Assert.Equal(15 * 60000, minutes[0].GetDouble());
        Assert.Equal("null", minutes[1].GetString());
        Assert.Equal("null", minutes[2].GetString());
        Assert.Equal("null", minutes[3].GetString());
    }

    /// <summary>
    /// #7544: a range strip joins every sample and closes the area under the line, so a stretch with no data drew a ramp the chart above it did not.
    /// An hour of 5 minute samples, a 3 hour hole and another hour is two runs, in the line and in the area. A burst and then one mark hours later
    /// is a run and a stem with a dot. One sample is still the #7416 stem. A rule of "never" joins everything.
    /// </summary>
    [Fact]
    public void ARangeStrip_BreaksItsLineAndAreaAtAStepLargerThanItsNormalStep()
    {
        var r = Run();
        foreach (var scenario in new[] { "stripHole", "stripHoleDerived" })
        {
            Assert.Equal(2, N(r, scenario, "lineRuns"));
            Assert.Equal(2, N(r, scenario, "areaRuns"));
        }

        Assert.Equal(1, N(r, "stripNoHole", "lineRuns"));
        Assert.Equal(1, N(r, "stripNoHole", "areaRuns"));
        Assert.Equal(2, N(r, "stripBurst", "lineRuns"));
        Assert.Equal(1, N(r, "stripBurst", "areaRuns"));
        Assert.Equal(1, N(r, "stripBurst", "dots"));
        Assert.Equal(1, N(r, "stripOne", "lineRuns"));
        Assert.Equal(0, N(r, "stripOne", "areaRuns"));
        Assert.Equal(1, N(r, "stripOne", "dots"));
        Assert.Equal(1, N(r, "stripNever", "lineRuns"));
        Assert.Equal(1, N(r, "stripSeam", "lineRuns"));
    }

    /// <summary>
    /// #7692: a read with rows only where queries ran (Query Duration Trend on a quiet server: steps of 12, 1, 23, 1, 1, 256 and 9 minutes) has uneven
    /// steps, so the chart read it as events and drew one diagonal across the 256 minute step. A panel that names the reply's bucket (bucketKey) and the
    /// range strip above Top Queries (bucketed) break at that step, and a busy server's line breaks only at its one hole.
    /// </summary>
    [Fact]
    public void AQuietServersQueryDurationTrend_BreaksAtTheHole_OnTheChartAndTheStrip()
    {
        var r = Run();
        int[] Runs(string scenario) => r.GetProperty(scenario).GetProperty("runs").EnumerateArray().Select(e => e.GetInt32()).ToArray();

        /* rows at minutes 0, 12, 13 | 36, 37, 38 | 294, 303: no polyline holds a point from each side of the 256 minute step */
        Assert.Equal(new[] { 3, 3, 2 }, Runs("quietPanel"));
        Assert.Equal(0, r.GetProperty("quietPanel").GetProperty("dots").GetInt32());
        Assert.Equal(new[] { 8 }, Runs("quietPanelNoKey"));
        Assert.Equal(new[] { 61, 61 }, Runs("busyPanel"));
        Assert.Equal(3, N(r, "quietStrip", "lineRuns"));
        Assert.Equal(1, N(r, "quietStripNoBucketed", "lineRuns"));
        var g = r.GetProperty("quietStripGaps");
        Assert.Equal("null", g.GetProperty("noBucketInReply").GetString());
        /* the harness prints this one through String() (the sibling above is "null" when no step applies), so the step is its text */
        Assert.Equal("60000", g.GetProperty("stepWins").GetString());
    }

    /// <summary>
    /// #7544: <c>Number(null)</c> is 0, which made a row with no figure a sample at zero (one dot, and "Whole range" over grids with no rows).
    /// A strip row with no figure is no sample. The step a strip is judged by is its own, else the reply's bucket, else the series' own cadence.
    /// </summary>
    [Fact]
    public void ARangeStripRowWithNoFigure_IsNoSample_AndTheStepComesFromTheStripThenTheReply()
    {
        var r = Run();
        var values = r.GetProperty("stripValues").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "NaN", "NaN", "NaN", "0", "3.5", "7" }, values);
        var g = r.GetProperty("stripGaps");
        Assert.Equal(60000, g.GetProperty("blocking").GetDouble());
        Assert.Equal("null", g.GetProperty("bucket").GetString());
        Assert.Equal("null", g.GetProperty("none").GetString());
        Assert.Equal(DateTimeOffset.Parse("2026-01-01T04:00:00Z").ToUnixTimeMilliseconds(), g.GetProperty("seam").GetProperty("seamMs").GetInt64());
    }
}
