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

    /// <summary>
    /// The readout is the crosshair's time in the browser's local time (the harness pins New York, where 12:xx UTC on 1 January is 07:xx),
    /// two spaces, then the strip's values. The clock's AM/PM suffix and its space are the machine's locale, so only its HH:mm is compared.
    /// </summary>
    private static void AssertReadout(string clock, string rest, string? actual)
    {
        Assert.NotNull(actual);
        Assert.StartsWith(clock, actual);
        Assert.EndsWith("  " + rest, actual);
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
        AssertReadout("07:22", "no sample here", r.GetProperty("at22Text").GetProperty("blocking").GetString());

        // On the blocking bucket the blocking series answers; the deadlock series (an empty read) still has no sample.
        var at12 = Samples(r.GetProperty("at12").GetProperty("blocking"));
        Assert.Equal((12.0, 3.0), at12[0]);
        Assert.Null(at12[1].V);

        Assert.Equal((30.0, 3.0), Samples(r.GetProperty("at30").GetProperty("io"))[0]);
        // A pointer move drives the same lookup, over one crosshair line.
        AssertReadout("07:30", "SQL 40 %  |  Total 45 %", r.GetProperty("pointerText").GetProperty("cpu").GetString());
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
        AssertReadout("07:38", "no sample here", r.GetProperty("at38Text").GetProperty("cpu").GetString());

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
        Assert.StartsWith("07:20", labels.First());
        Assert.StartsWith("07:40", labels.Last());
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

    /// <summary>
    /// The Viewer's baseline rule on the web (#6291): a flat band of mean +- 2 effective sigma, a marker on a point above the band AND
    /// at or above the lane's floor (CPU 10, waits 100, I/O 2) or below the band, markers on the SQL CPU line only, a band and no
    /// markers on blocking, and none at all for a lane the store has too little history for, which says so instead.
    /// </summary>
    [Fact]
    public void TheBaselineBandAndMarkers_FollowTheViewersRule_AndALaneWithoutHistoryDrawsNeither()
    {
        var r = Run("baselines");

        var full = r.GetProperty("full");
        var cpuBand = full.GetProperty("cpu").GetProperty("band")[0];
        Assert.Equal(10, cpuBand.GetProperty("lower").GetDouble());
        Assert.Equal(30, cpuBand.GetProperty("upper").GetDouble());
        Assert.Equal(20, cpuBand.GetProperty("mean").GetDouble());
        Assert.Equal(100, full.GetProperty("waits").GetProperty("band")[0].GetProperty("lower").GetDouble());
        Assert.Equal(140, full.GetProperty("waits").GetProperty("band")[0].GetProperty("upper").GetDouble());

        /* CPU's SQL line is 10 + minute for 0..30 and 45..60; above 30 is minutes 21..30 and 45..60. Total CPU is never marked. */
        var cpuMarked = r.GetProperty("cpuMarkedValues").EnumerateArray().Select(e => e.GetDouble()).ToArray();
        Assert.Equal(Enumerable.Range(21, 10).Concat(Enumerable.Range(45, 16)).Select(m => 10.0 + m).ToArray(), cpuMarked);
        /* Waits are 100 + 10 * i for i 0..12; above 140 is 150..220. */
        Assert.Equal(Enumerable.Range(5, 8).Select(i => 100.0 + 10 * i).ToArray(),
            r.GetProperty("waitMarkedValues").EnumerateArray().Select(e => e.GetDouble()).ToArray());
        /* The weighted I/O latency is 3 ms, above a 1.4 ms band and over the 2 ms floor, at all five buckets. */
        Assert.Equal(5, r.GetProperty("ioMarkerCount").GetInt32());
        Assert.Equal(JsonValueKind.Null, full.GetProperty("blocking").GetProperty("markers").ValueKind);
        Assert.Equal(JsonValueKind.Array, full.GetProperty("blocking").GetProperty("band").ValueKind);
        Assert.Equal(JsonValueKind.Null, full.GetProperty("memory").GetProperty("band").ValueKind);

        /* 1.5 ms is over the band but under the 2 ms floor: unmarked. 2.5 is marked, and 0.1 is under the band: marked at any value. */
        Assert.Equal(new[] { 2.5, 0.1 }, r.GetProperty("floorMarkedValues").EnumerateArray().Select(e => e.GetDouble()).ToArray());
        Assert.Equal(0, r.GetProperty("clampedLower").GetDouble());

        var partial = r.GetProperty("partial");
        Assert.Equal(JsonValueKind.Null, partial.GetProperty("waits").GetProperty("band").ValueKind);
        Assert.Equal(JsonValueKind.Null, partial.GetProperty("waits").GetProperty("markers").ValueKind);
        Assert.Equal("No baseline yet: not enough history.", partial.GetProperty("waits").GetProperty("note").GetString());
        Assert.Equal(JsonValueKind.Array, partial.GetProperty("cpu").GetProperty("band").ValueKind);

        /* A bucket with no samples, or no dispersion, is no baseline: never a band at 0. */
        foreach (var lane in new[] { "cpu", "waits" })
        {
            Assert.Equal(JsonValueKind.Null, r.GetProperty("flat").GetProperty(lane).GetProperty("band").ValueKind);
            Assert.Equal("No baseline yet: not enough history.", r.GetProperty("flat").GetProperty(lane).GetProperty("note").GetString());
        }

        foreach (var lane in new[] { "cpu", "waits", "io", "blocking" })
        {
            Assert.Equal(JsonValueKind.Null, r.GetProperty("emptyRead").GetProperty(lane).GetProperty("band").ValueKind);
            Assert.Equal("No baseline yet: not enough history.", r.GetProperty("emptyRead").GetProperty(lane).GetProperty("note").GetString());
            Assert.Equal("Baseline unavailable.", r.GetProperty("failedRead").GetProperty(lane).GetProperty("note").GetString());
        }

        /* A database filter narrows the I/O and blocking lines but not the server-wide baseline: those two draw no band. CPU and waits keep theirs. */
        var filtered = r.GetProperty("filtered");
        Assert.Equal(JsonValueKind.Null, filtered.GetProperty("io").GetProperty("band").ValueKind);
        Assert.Equal(JsonValueKind.Null, filtered.GetProperty("blocking").GetProperty("band").ValueKind);
        Assert.Contains("database filter", filtered.GetProperty("io").GetProperty("note").GetString());
        Assert.Equal(JsonValueKind.Array, filtered.GetProperty("cpu").GetProperty("band").ValueKind);

        var drawn = r.GetProperty("drawn");
        Assert.Equal(1, drawn.GetProperty("cpu").GetProperty("band").GetInt32());
        Assert.Equal(1, drawn.GetProperty("cpu").GetProperty("mean").GetInt32());
        Assert.Equal(26, drawn.GetProperty("cpu").GetProperty("dots").GetInt32());
        Assert.Equal(0, drawn.GetProperty("waits").GetProperty("band").GetInt32());
        Assert.Equal(0, drawn.GetProperty("waits").GetProperty("dots").GetInt32());
        Assert.Equal(new[] { "No baseline yet: not enough history." },
            drawn.GetProperty("waits").GetProperty("note").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(1, drawn.GetProperty("blocking").GetProperty("band").GetInt32());
        Assert.Equal(0, drawn.GetProperty("blocking").GetProperty("dots").GetInt32());
        Assert.Equal(0, drawn.GetProperty("memory").GetProperty("band").GetInt32());
    }

    /// <summary>
    /// The axis labels and the crosshair readout are in the browser's local time, the zone the zoom chip and the rest of the page show
    /// (#6360). They used to read UTC, so a reader in New York saw 20:28 on the axis under a chip that said 4:28:55 PM. The window here is
    /// 12:00-13:00 UTC, which is 07:00-08:00 in the zone the harness pins.
    /// </summary>
    [Fact]
    public void TheAxisAndTheReadout_ReadInLocalTime_NotUtc()
    {
        var r = Run("localtime");

        var labels = r.GetProperty("axisLabels").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.StartsWith("07:00", labels.First());
        Assert.StartsWith("08:00", labels.Last());
        Assert.DoesNotContain(labels, l => l.StartsWith("12:") || l.StartsWith("13:"));
        AssertReadout("07:22", "SQL 32 %  |  Total 37 %", r.GetProperty("readout").GetString());
    }

    /// <summary>
    /// On a reload the panel's host is built before it is in the page, so the first draw measures no width and falls back to 1000. Once the
    /// host is attached and laid out (328 px, a phone) the observer's first report must redraw the strips at 328, not leave the 1000 px
    /// drawing for the browser to scale down to a third of its size, with titles and axis text about 4 px tall (#6360).
    /// </summary>
    [Fact]
    public void OnFirstLoad_TheStripsAreRedrawnAtTheWidthTheHostIsLaidOutAt()
    {
        var r = Run("firstload");

        var before = r.GetProperty("afterFill").EnumerateArray().Single();
        Assert.Equal("1000", before.GetProperty("width").GetString());
        var after = r.GetProperty("afterObserver").EnumerateArray().Single();
        Assert.Equal("328", after.GetProperty("width").GetString());
        Assert.StartsWith("0 0 328 ", after.GetProperty("viewBox").GetString());
    }

    /// <summary>
    /// The Waits strip's axis starts at 0 like the other strips. It used to start at the series minimum, so a window whose waits ran
    /// from 292 to 500 ms/sec was drawn on a 292-500 axis and a small swing looked like a spike. The harness waits run 100 to 220.
    /// </summary>
    [Fact]
    public void TheWaitsAxis_StartsAtZero_NotAtTheLowestSample()
    {
        var r = Run("waitsaxis");

        var waits = r.GetProperty("waits").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal(2, waits.Length);
        Assert.Equal("0", waits[1]);
        var memory = r.GetProperty("memory").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Equal("0", memory[1]);
    }
}
