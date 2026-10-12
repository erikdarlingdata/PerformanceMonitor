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
}
