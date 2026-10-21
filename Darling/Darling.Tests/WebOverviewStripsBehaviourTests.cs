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
/// The web Overview's five stacked strips (#6291), run under Node (<c>web-overview-strips-harness.mjs</c>) on a stand-in DOM: the
/// shared crosshair reads each strip at its OWN sample times, a gap in one strip's samples reads no value rather than a neighbour's,
/// a drag across the strips zooms all five, and a strip with no samples says so instead of drawing a zero line. Node is skipped when
/// it is not installed.
/// </summary>
public sealed class WebOverviewStripsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-overview-strips-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the overview strips harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the overview strips harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static (double? T, double? V)[] Samples(JsonElement series) =>
        series.EnumerateArray().Select(e => (
            e.GetProperty("t").ValueKind == JsonValueKind.Null ? (double?)null : e.GetProperty("t").GetDouble(),
            e.GetProperty("value").ValueKind == JsonValueKind.Null ? (double?)null : e.GetProperty("value").GetDouble())).ToArray();

    /// <summary>
    /// Five reads sample on five clocks (CPU each minute, waits every 5, memory every 10, file I/O every 15, blocking only when it
    /// happened). The crosshair at one time reads each strip's own nearest sample, and the file I/O figure is the files' latencies
    /// weighted by their reads (900 reads at 2 ms and 100 at 12 ms is 3 ms, not the 7 ms mean of the two averages).
    /// </summary>
    [Fact]
    public void TheCrosshair_ReadsEachStripAtItsOwnSampleTimes_AndTheWeightedFileIoLatency()
    {
        var r = Run("crosshair");

        Assert.Equal(5, r.GetProperty("stripCount").GetInt32());
        var at22 = r.GetProperty("at22");
        Assert.Equal((22.0, 32.0), Samples(at22.GetProperty("cpu"))[0]);
        Assert.Equal((20.0, 140.0), Samples(at22.GetProperty("waits"))[0]);
        Assert.Equal((20.0, 2002.0), Samples(at22.GetProperty("memory"))[0]);
        Assert.Equal((15.0, 3.0), Samples(at22.GetProperty("io"))[0]);
        // Blocking happened at minutes 12 and 47 only: nothing is read at minute 22.
        Assert.All(Samples(at22.GetProperty("blocking")), s => Assert.Null(s.V));
        Assert.Equal("12:22  no sample here", r.GetProperty("at22Text").GetProperty("blocking").GetString());

        // On the blocking bucket the blocking series answers; the deadlock series (an empty read) still has no sample.
        var at12 = Samples(r.GetProperty("at12").GetProperty("blocking"));
        Assert.Equal((12.0, 3.0), at12[0]);
        Assert.Null(at12[1].V);

        Assert.Equal((30.0, 3.0), Samples(r.GetProperty("at30").GetProperty("io"))[0]);
        // A pointer move drives the same lookup, over one crosshair line.
        Assert.Equal("12:30  SQL 40 %  |  Total 45 %", r.GetProperty("pointerText").GetProperty("cpu").GetString());
        Assert.Equal(1, r.GetProperty("hoverLines").GetInt32());
    }

    /// <summary>
    /// The CPU samples have a gap (minutes 31 to 44). A crosshair inside it reads no CPU value, where nearest-neighbour-at-any-distance
    /// would have printed minute 30's or 45's; the other strips, sampled through the gap, still answer. The CPU lines break at the gap.
    /// </summary>
    [Fact]
    public void ACrosshairInsideOneStripsGap_ReadsNoValueThere_WhileTheOtherStripsStillAnswer()
    {
        var r = Run("crosshair");
        var at38 = r.GetProperty("at38");

        Assert.All(Samples(at38.GetProperty("cpu")), s => Assert.Null(s.V));
        Assert.Equal((40.0, 180.0), Samples(at38.GetProperty("waits"))[0]);
        Assert.Equal((40.0, 2004.0), Samples(at38.GetProperty("memory"))[0]);
        Assert.Equal("12:38  no sample here", r.GetProperty("at38Text").GetProperty("cpu").GetString());

        Assert.Equal(4, Run("zoom").GetProperty("cpuPolylinesFull").GetInt32());
    }

    /// <summary>
    /// A drag across the plot zooms every strip: the callback gets the brushed span, a click (a drag under 8 px) zooms nothing, and a
    /// redraw on that span clips all five strips to it and labels the shared axis with it.
    /// </summary>
    [Fact]
    public void ADragAcrossTheStrips_ZoomsAllFiveToTheBrushedSpan_AndAClickDoesNot()
    {
        var r = Run("zoom");

        Assert.True(r.GetProperty("brushShown").GetBoolean());
        Assert.InRange(r.GetProperty("zoomed").GetProperty("from").GetDouble(), 19.9, 20.1);
        Assert.InRange(r.GetProperty("zoomed").GetProperty("to").GetDouble(), 39.9, 40.1);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("clickZoomed").ValueKind);
        Assert.Equal(5, r.GetProperty("stripsDrawnZoomed").GetInt32());
        // CPU: minute 19 (the neighbour before the span) to minute 30 (the gap starts at 31): one broken line of 12 points; waits: 20..40 every 5, plus the neighbour either side.
        Assert.Equal(12, r.GetProperty("cpuPointsZoomed").GetInt32());
        Assert.Equal(7, r.GetProperty("waitPointsZoomed").GetInt32());
        var labels = r.GetProperty("axisLabels").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal("12:20", labels.First());
        Assert.Equal("12:40", labels.Last());
    }

    /// <summary>
    /// A strip whose read answered nothing or failed draws its reason, not a zero line: no polyline, no bar, and the read's own words (or
    /// the blocking strip's "nothing happened" sentence) where the line would be. A strip with samples is unaffected.
    /// </summary>
    [Fact]
    public void AStripWithNoSamples_SaysSo_InsteadOfDrawingAZeroLine()
    {
        var r = Run("empty");

        Assert.Equal(0, r.GetProperty("cpu").GetProperty("lines").GetInt32());
        Assert.Equal("No CPU samples in this window.", r.GetProperty("cpu").GetProperty("text").GetString());
        Assert.Equal(0, r.GetProperty("waits").GetProperty("lines").GetInt32());
        Assert.Equal("boom", r.GetProperty("waits").GetProperty("text").GetString());
        Assert.Equal(0, r.GetProperty("blocking").GetProperty("bars").GetInt32());
        Assert.Equal("No blocking or deadlocks in this window.", r.GetProperty("blocking").GetProperty("text").GetString());
        Assert.Equal(1, r.GetProperty("memoryLines").GetInt32());
    }
}
