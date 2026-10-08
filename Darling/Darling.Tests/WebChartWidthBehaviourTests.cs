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
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A web line, scatter or ranked bar chart is drawn at the width its box really has, one SVG unit per CSS pixel (#5586).
///
/// <para><b>The defect.</b> Both charts drew a 1000 x 320 viewBox with <c>preserveAspectRatio="none"</c> and CSS gave the
/// SVG <c>width: 100%</c> but capped its height at 300 px, so above about 940 px the width kept growing while the height
/// stopped, and the axis text was stretched sideways (about 2.1 times at 2,000 px). Below 1,000 px the whole chart,
/// text included, shrank. The bar chart drew a 1000-wide viewBox that CSS scaled with the panel, so its labels grew on a
/// wide panel and shrank on a narrow one.</para>
///
/// <para><b>The fix.</b> The chart is drawn for the width of its plot box and redrawn when that width changes, from the
/// same data and the same spec (so the zoom window, the hidden series and the annotations are unchanged, and nothing is
/// fetched). The x tick count follows the width. These run the shipped <c>charts.js</c> under Node
/// (<c>web-chart-width-harness.mjs</c>) against a stand-in DOM with a ResizeObserver and a frame queue. Node is skipped
/// when it is not installed; the source pins at the end hold without it.</para>
/// </summary>
public sealed class WebChartWidthBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        // The labels are local wall-clock times: pin the zone so a run does not depend on the machine's (the harness sets
        // America/New_York itself for the clock-change cases).
        psi.Environment["TZ"] = "UTC";
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-width-harness.mjs"));
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
                Assert.Fail("the chart width harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart width harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>The viewBox is as wide as the box and as tall as the drawn height; the SVG's CSS size is the viewBox, so
    /// the x and y scales are equal (and one), and nothing in the drawing carries a scale transform.</summary>
    [Theory]
    [InlineData("line", 600)]
    [InlineData("line", 2000)]
    [InlineData("scatter", 600)]
    [InlineData("scatter", 2000)]
    public void TheChart_IsDrawnAtTheBoxWidth_WithEqualScalesOnBothAxes(string kind, int width)
    {
        var g = Run().GetProperty(kind).GetProperty(width.ToString());
        Assert.Equal(width, g.GetProperty("vbW").GetDouble());
        Assert.Equal(300, g.GetProperty("vbH").GetDouble());
        Assert.Equal(g.GetProperty("vbW").GetDouble(), g.GetProperty("cssW").GetDouble());
        Assert.Equal(g.GetProperty("vbH").GetDouble(), g.GetProperty("cssH").GetDouble());
        // rendered width / viewBox width == rendered height / viewBox height, and both are one: no stretched text
        Assert.Equal(g.GetProperty("xScale").GetDouble(), g.GetProperty("yScale").GetDouble());
        Assert.Equal(1d, g.GetProperty("xScale").GetDouble());
        Assert.Equal(JsonValueKind.Null, g.GetProperty("aspect").ValueKind);
        Assert.Equal(0, g.GetProperty("transformed").GetInt32());
        Assert.True(g.GetProperty("texts").GetInt32() > 0);
    }

    /// <summary>A chart is built before it is on the page (no width yet): it draws at the default width and the first
    /// measurement redraws it. That holds for the bar chart too.</summary>
    [Fact]
    public void AChartThatIsNotMountedYet_DrawsAtTheDefaultWidth()
    {
        var r = Run();
        Assert.Equal(1000, r.GetProperty("unmounted").GetProperty("vbW").GetDouble());
        Assert.Equal(1000, r.GetProperty("unmountedScatter").GetProperty("vbW").GetDouble());
        Assert.Equal(1000, r.GetProperty("unmountedBar").GetProperty("vbW").GetDouble());
    }

    /// <summary>The bar chart's viewBox is as wide as its box and as tall as its bars need (a row each, so the width never
    /// changes the height), and the SVG's CSS size is the viewBox: equal scales, both one, so the 12px labels are 12px on
    /// any panel. RED proof: with the old 1000-wide viewBox (<c>preserveAspectRatio="xMinYMin meet"</c>) the viewBox is 1000
    /// wide at 600 and at 2,000 px.</summary>
    [Theory]
    [InlineData(600)]
    [InlineData(2000)]
    public void TheBarChart_IsDrawnAtTheBoxWidth_WithEqualScalesOnBothAxes(int width)
    {
        var r = Run();
        var g = r.GetProperty("bar").GetProperty(width.ToString());
        Assert.Equal(width, g.GetProperty("vbW").GetDouble());
        Assert.Equal(26d + 5 * 34, g.GetProperty("vbH").GetDouble());
        Assert.Equal(g.GetProperty("vbW").GetDouble(), g.GetProperty("cssW").GetDouble());
        Assert.Equal(g.GetProperty("vbH").GetDouble(), g.GetProperty("cssH").GetDouble());
        Assert.Equal(g.GetProperty("xScale").GetDouble(), g.GetProperty("yScale").GetDouble());
        Assert.Equal(1d, g.GetProperty("xScale").GetDouble());
        Assert.Equal(JsonValueKind.Null, g.GetProperty("aspect").ValueKind);
        Assert.Equal(0, g.GetProperty("transformed").GetInt32());
        Assert.Equal(5, g.GetProperty("rows").GetInt32());
        Assert.Equal(1, g.GetProperty("thresholds").GetInt32());
        // a different bar count changes the height and not the scale
        var few = r.GetProperty("barFew");
        Assert.Equal(26d + 2 * 34, few.GetProperty("vbH").GetDouble());
        Assert.Equal(1d, few.GetProperty("xScale").GetDouble());
    }

    /// <summary>At 260, 360, 600 and 2,000 px no label runs off the left edge or into its bar track, the value of every bar
    /// ends inside the box, and a long label is cut to what its gutter holds (most characters on the widest panel). The
    /// harness measures a glyph at 8.76 px (0.73 em), 15 % or more over the 7.6 px the script's own estimate is, so a layout
    /// that is too tight for a wide font fails here. RED proof: without the width-driven gutters a 360 px panel keeps the 220
    /// px label gutter and 110 px value gutter of the old 1000-wide drawing, and the values run past the right edge.</summary>
    [Theory]
    [InlineData(260)]
    [InlineData(360)]
    [InlineData(600)]
    [InlineData(2000)]
    public void TheBarChart_LabelsAndValues_StayInsideTheBox_AtAnyWidth(int width)
    {
        var g = Run().GetProperty("bar").GetProperty(width.ToString());
        Assert.True(g.GetProperty("labelLeftMin").GetDouble() >= 0, "a label starts left of the box at " + width + " px");
        Assert.True(g.GetProperty("labelRight").GetDouble() < g.GetProperty("trackLeft").GetDouble(), "a label reaches its track at " + width + " px");
        Assert.True(g.GetProperty("valueRightMax").GetDouble() <= width, "a value runs past the right edge at " + width + " px");
        Assert.True(g.GetProperty("trackRight").GetDouble() <= width - 40, "the bar track is too wide for its value at " + width + " px");
        Assert.EndsWith("\u2026", g.GetProperty("firstLabel").GetString(), StringComparison.Ordinal);
    }

    /// <summary>A bar panel whose longest label has any length from 1 to 30 draws it whole at 2,000 px, with no ellipsis: the
    /// gutter is sized for that label. RED proof: with the gutter rounded to the nearest pixel instead of up, the longest
    /// label is one character short (and cut) at lengths 3, 4, 5, 9, 10, 14, 15, 16, 20, 21, 25, 26, 27 and 30.</summary>
    [Fact]
    public void TheBarChart_ShowsItsLongestLabelWhole_AtEveryLengthFrom1To30()
    {
        var lengths = Run().GetProperty("barLabelLengths").EnumerateArray().ToArray();
        Assert.Equal(30, lengths.Length);
        foreach (var l in lengths)
        {
            var len = l.GetProperty("len").GetInt32();
            Assert.False(l.GetProperty("cut").GetBoolean(), "a label of " + len + " characters was cut");
            Assert.Equal(len, l.GetProperty("shown").GetInt32());
        }
    }

    [Fact]
    public void AWideBarPanel_ShowsMoreOfALongLabel_ThanANarrowOne()
    {
        var bar = Run().GetProperty("bar");
        int Chars(int w) => bar.GetProperty(w.ToString()).GetProperty("longestLabel").GetInt32();
        Assert.True(Chars(360) < Chars(600), "360 px showed as many label characters as 600 px");
        Assert.True(Chars(600) < Chars(2000), "600 px showed as many label characters as 2,000 px");
        Assert.Equal(30, Chars(2000));
    }

    /// <summary>A resize redraws the bar chart once, on the next frame.</summary>
    [Fact]
    public void ABarChartResize_RedrawsOnce()
    {
        var z = Run().GetProperty("barResize");
        Assert.Equal(1, z.GetProperty("queued").GetInt32());
        Assert.Equal(1, z.GetProperty("redraws").GetInt32());
        Assert.Equal(2000, z.GetProperty("vbW").GetDouble());
    }

    /// <summary>A resize from 600 to 2,000 px redraws once, on the next frame; a repeat within a pixel and a height-only
    /// change draw nothing; the zoom window, the hidden series, the annotations and the reset chip are unchanged; and
    /// nothing is fetched.</summary>
    [Fact]
    public void AResize_RedrawsOnce_AndKeepsTheChartsState()
    {
        var r = Run();
        Assert.Equal(20d, r.GetProperty("zoomHeldBefore")[0].GetDouble(), 0.1);
        var z = r.GetProperty("resize");
        Assert.Equal(1, z.GetProperty("queuedAfterResize").GetInt32());
        Assert.Equal(1, z.GetProperty("queuedAfterNoise").GetInt32());
        Assert.True(z.GetProperty("drawnOnlyAtFlush").GetBoolean());
        Assert.Equal(1, z.GetProperty("redrawsOnResize").GetInt32());
        Assert.Equal(0, z.GetProperty("redrawsOnSameSize").GetInt32());
        Assert.Equal(2000, z.GetProperty("geometry").GetProperty("vbW").GetDouble());

        Assert.Equal(1, z.GetProperty("polylines")[0].GetInt32()); // one of the two series is hidden
        Assert.Equal(1, z.GetProperty("polylines")[1].GetInt32());
        Assert.Equal(1, z.GetProperty("annotations")[1].GetInt32());
        Assert.Equal(z.GetProperty("annotations")[0].GetInt32(), z.GetProperty("annotations")[1].GetInt32());
        Assert.Equal(1, z.GetProperty("chip")[1].GetInt32());
        Assert.True(z.GetProperty("zoomKept").GetBoolean());
        // the zoomed domain is the same: its first and last tick label are unchanged
        Assert.Equal(z.GetProperty("firstLabel")[0].GetString(), z.GetProperty("firstLabel")[1].GetString());
        Assert.Equal(z.GetProperty("lastLabel")[0].GetString(), z.GetProperty("lastLabel")[1].GetString());
        Assert.Equal(0, z.GetProperty("fetchCalls").GetInt32());
    }

    /// <summary>The observer is disconnected once the chart has left the page, even with a redraw still queued, and
    /// every chart a refresh replaces lets go of its own.</summary>
    [Fact]
    public void TheObserver_IsDisconnected_WhenTheChartLeavesThePage()
    {
        var r = Run();
        var gone = r.GetProperty("removed");
        Assert.Equal(1, gone.GetProperty("disconnects").GetInt32());
        Assert.Equal(0, gone.GetProperty("redrawsAfterRemoval").GetInt32());
        Assert.Equal(1, gone.GetProperty("disconnectsAfterFinalReport").GetInt32());
        Assert.All(r.GetProperty("leak").EnumerateArray(), n => Assert.Equal(1, n.GetInt32()));
    }

    /// <summary>The same pointer position (as a share of the plot) names the same point at 600 px and, after the resize,
    /// at 2,000 px; the zoom drag brushes the same span; and the 8 px threshold is 8 CSS pixels at the new width.</summary>
    [Fact]
    public void HoverAndTheZoomDrag_MapAPointerToTheSameDataPoint_AfterAResize()
    {
        var r = Run();
        var hover = r.GetProperty("hover");
        Assert.Equal(hover[0].GetString(), hover[1].GetString());
        Assert.NotEqual(hover[1].GetString(), hover[2].GetString());

        var drag = r.GetProperty("dragAfterResize");
        Assert.Equal(20d, drag[0].GetDouble(), 0.1);
        Assert.Equal(40d, drag[1].GetDouble(), 0.1);

        Assert.False(r.GetProperty("sevenPx").GetBoolean());
        Assert.True(r.GetProperty("ninePx").GetBoolean());
    }

    /// <summary>The x tick labels never overlap at 360 and 600 px (a phone and a narrow panel), the first and last stay
    /// inside the plot, and a wide panel gets more ticks. The harness measures a label with its own estimate, 17 % wider
    /// than the 0.6 em a character the script plans with, so a script estimate that is too tight fails here.</summary>
    [Theory]
    [InlineData(360)]
    [InlineData(600)]
    [InlineData(2000)]
    public void TheXTickLabels_DoNotOverlap_AtAnyWidth(int width)
    {
        var t = Run().GetProperty("ticks").GetProperty(width.ToString());
        foreach (var gap in new[] { "shortGap", "datedGap", "overlayGap" })
        {
            Assert.True(t.GetProperty(gap).GetDouble() >= 8, gap + " at " + width + " px was " + t.GetProperty(gap).GetDouble());
        }

        Assert.True(t.GetProperty("datedFirst").GetDouble() >= 58);
        Assert.True(t.GetProperty("datedLast").GetDouble() <= t.GetProperty("plotRight").GetDouble());
    }

    [Fact]
    public void AWidePanel_GetsMoreXTicks_NotWiderOnes()
    {
        var t = Run().GetProperty("ticks");
        int Count(int w) => t.GetProperty(w.ToString()).GetProperty("shortCount").GetInt32();
        Assert.True(Count(360) < Count(600), "360 px carried as many ticks as 600 px");
        Assert.True(Count(600) < Count(2000), "600 px carried as many ticks as 2,000 px");
    }

    [Theory]
    [InlineData(360)]
    [InlineData(600)]
    public void TheScatterXLabels_DoNotOverlap_AtANarrowWidth(int width)
    {
        var s = Run().GetProperty("scatterTicks").GetProperty(width.ToString());
        Assert.True(s.GetProperty("count").GetInt32() >= 2);
        Assert.True(s.GetProperty("gap").GetDouble() >= 8);
    }

    /// <summary>No two x labels read the same at any width and window. A brush-zoomed chart's window can be a few minutes
    /// or seconds wide, and the axis shows HH:mm: ticks are whole minutes capped at the window's minutes, and a window too
    /// short for a minute per tick shows seconds. RED proof: with the pre-fix tick count (the plot width alone) the
    /// 6-minute window at 2,000 px gets 11 labels over 7 distinct minutes.</summary>
    [Fact]
    public void NoTwoXLabelsReadTheSame_AtAnyWidthAndWindow()
    {
        var d = Run().GetProperty("distinctLabels");
        foreach (var name in new[] { "sixMinutes2000", "sixMinutes1000", "twoMinutes2000", "ninetySeconds600", "ninetySeconds2000", "fortyFiveSeconds2000", "thirtyDays" })
        {
            var labels = d.GetProperty(name).EnumerateArray().Select(l => l.GetString()!).ToArray();
            Assert.True(labels.Length >= 2, name + " drew fewer than two labels");
            Assert.True(labels.Distinct().Count() == labels.Length, name + " repeats a label: " + string.Join(" | ", labels));
        }

        // a step of a minute or more: whole-minute ticks, one per minute at most, with no seconds
        Assert.Equal(7, d.GetProperty("sixMinutes2000").GetArrayLength());
        Assert.All(d.GetProperty("sixMinutes2000").EnumerateArray(), l => Assert.Equal(1, l.GetString()!.Count(c => c == ':')));
        // too short for a minute per tick: seconds
        Assert.All(d.GetProperty("ninetySeconds600").EnumerateArray(), l => Assert.Equal(2, l.GetString()!.Count(c => c == ':')));
        // a long window is unchanged: evenly spaced ticks, ten intervals at this width
        Assert.Equal(
            d.GetProperty("thirtyDaysEvenly").EnumerateArray().Select(l => l.GetString()),
            d.GetProperty("thirtyDays").EnumerateArray().Select(l => l.GetString()));
    }

    /// <summary>A window across a clock change (the repeated hour in the autumn, the skipped one in the spring) gives every
    /// x label its zone, so no two read the same ("01:00 AM EDT", "01:00 AM EST"), and the longer labels still keep their gap
    /// and stay inside the plot. A window with one offset has no zone text. The harness runs these in America/New_York. RED
    /// proof: without the zone name the 4-hour window over 2026-11-01 at 600 px reads "01:00 AM" twice.</summary>
    [Fact]
    public void AWindowAcrossAClockChange_LabelsEveryTickWithItsZone_AndNoTwoLabelsReadTheSame()
    {
        var d = Run().GetProperty("dst");
        Assert.Contains("EDT", d.GetProperty("zone").GetString(), StringComparison.Ordinal);
        string[] Labels(string name) => d.GetProperty(name).EnumerateArray().Select(l => l.GetString()!).ToArray();
        foreach (var name in new[] { "fallBack600", "fallBackFrom0100At600", "fallBack2000", "springForward600", "springForward2000" })
        {
            var labels = Labels(name);
            Assert.True(labels.Length >= 3, name + " drew fewer than three labels");
            Assert.True(labels.Distinct().Count() == labels.Length, name + " repeats a label: " + string.Join(" | ", labels));
            Assert.All(labels, l => Assert.True(l.EndsWith("EDT", StringComparison.Ordinal) || l.EndsWith("EST", StringComparison.Ordinal), name + ": " + l));
        }

        // the repeated hour: the autumn window shows 01:00 under both zones
        Assert.Contains("01:36 AM EDT", Labels("fallBack2000"));
        Assert.Contains("01:00 AM EST", Labels("fallBack2000"));

        // one offset: the label is unchanged, with no zone text
        foreach (var name in new[] { "sameOffset600", "sameOffset2000" })
        {
            Assert.All(Labels(name), l => Assert.Matches(@"^\d\d:\d\d [AP]M$", l));
        }

        foreach (var width in new[] { 360, 600, 2000 })
        {
            var fit = d.GetProperty("fit" + width);
            Assert.True(fit.GetProperty("gap").GetDouble() >= 8, "zone labels overlap at " + width + " px");
            Assert.True(fit.GetProperty("first").GetDouble() >= 58);
            Assert.True(fit.GetProperty("last").GetDouble() <= fit.GetProperty("plotRight").GetDouble());
        }
    }

    /// <summary>In a 260 px box (a phone) the line and scatter charts are drawn at 260 px, not wider, so the host clips nothing:
    /// no labels overlap and the last ends inside the box. RED proof: with the old 300 px floor the drawn width is 300 and
    /// the last x label ends at 284, past the 260 px box.</summary>
    [Theory]
    [InlineData("line")]
    [InlineData("scatter")]
    public void ANarrowChart_IsDrawnAtTheBoxWidth_AndKeepsItsLabelsInside(string kind)
    {
        var g = Run().GetProperty("narrow").GetProperty(kind);
        Assert.Equal(260, g.GetProperty("vbW").GetDouble());
        Assert.Equal(260, g.GetProperty("cssW").GetDouble());
        Assert.Equal(1d, g.GetProperty("xScale").GetDouble());
        Assert.True(g.GetProperty("count").GetInt32() >= 2);
        Assert.True(g.GetProperty("gap").GetDouble() >= 8, "labels overlap at 260 px");
        Assert.True(g.GetProperty("first").GetDouble() >= 58);
        Assert.True(g.GetProperty("last").GetDouble() <= 260, "the last x label runs past the box");
    }

    /// <summary>The floor is 200 px: a box of 200 px is drawn at 200, and a narrower one at 200 (the host clips the rest).</summary>
    [Fact]
    public void TheChartFloor_Is200Px()
    {
        var n = Run().GetProperty("narrow");
        Assert.Equal(200, n.GetProperty("floorLine").GetProperty("vbW").GetDouble());
        Assert.Equal(200, n.GetProperty("belowFloorLine").GetProperty("vbW").GetDouble());
    }

    /// <summary>The tooltip is placed inside the box the reader sees. Below the 200 px floor the SVG is wider than its clipped
    /// host; RED proof: clamping to the SVG's width puts the tooltip 20 px further right (76 px, not 56) in a 180 px box.</summary>
    [Fact]
    public void TheTooltip_StaysInsideTheVisibleBox_WhenTheSvgIsWiderThanItsHost()
    {
        var tip = Run().GetProperty("tooltip");
        Assert.True(
            tip.GetProperty("left").GetDouble() + tip.GetProperty("tooltipWidth").GetDouble() <= tip.GetProperty("visibleWidth").GetDouble(),
            "the tooltip reaches past the visible box");
    }

    /// <summary>A narrow scatter keeps every x gridline and thins only the labels, and the first and the last tick are
    /// always labelled (a stride tick next to the last gives way to it).</summary>
    [Fact]
    public void TheScatter_KeepsEveryGridline_AndLabelsTheFirstAndLastTick()
    {
        var g = Run().GetProperty("scatterGrid");
        var narrow = g.GetProperty("360");
        var wide = g.GetProperty("2000");
        Assert.Equal(wide.GetProperty("gridlines").GetInt32(), narrow.GetProperty("gridlines").GetInt32());
        Assert.True(narrow.GetProperty("labels").GetArrayLength() < wide.GetProperty("labels").GetArrayLength(), "the narrow plot did not thin its labels");
        Assert.Equal(wide.GetProperty("labels")[0].GetString(), narrow.GetProperty("labels")[0].GetString());
        var wl = wide.GetProperty("labels");
        var nl = narrow.GetProperty("labels");
        Assert.Equal(wl[wl.GetArrayLength() - 1].GetString(), nl[nl.GetArrayLength() - 1].GetString());

        var six = g.GetProperty("six");
        Assert.Equal(6, six.GetProperty("gridlines").GetInt32());
        Assert.Equal(new[] { "0 ms", "4 ms", "10 ms" }, six.GetProperty("labels").EnumerateArray().Select(l => l.GetString()).ToArray());
    }

    /// <summary>The scatter's first and last x labels are anchored inward, so the last one never runs past the plot's right
    /// edge, and its x unit caption sits on the top row, not on the label row where it overprinted the last label. RED
    /// proof: with the middle-anchored, clamped last label and the caption at the bottom right, the last label is a
    /// "middle" anchor and the caption shares the label row.</summary>
    [Theory]
    [InlineData(600)]
    [InlineData(2000)]
    public void TheScatter_AnchorsItsEndLabelsInward_AndKeepsTheUnitCaptionOffTheLabelRow(int width)
    {
        var e = Run().GetProperty("scatterEdges").GetProperty(width.ToString());
        Assert.Equal("start", e.GetProperty("firstAnchor").GetString());
        Assert.Equal("end", e.GetProperty("lastAnchor").GetString());
        Assert.True(e.GetProperty("lastX").GetDouble() <= e.GetProperty("plotRight").GetDouble());
        Assert.NotEqual(e.GetProperty("labelRowY").GetDouble(), e.GetProperty("captionY").GetDouble());
    }

    /// <summary>A poll rebuilds every chart. A chart that was measured before (keyed by its menuKey, or the widthKey the
    /// composed panels pass to the line, scatter and bar charts) is drawn at that width, so the observer's first report is
    /// within a pixel and the build is the only one; a chart with no key is drawn at the default and redrawn once, as before.</summary>
    [Fact]
    public void AnAlreadyMeasuredChart_IsBuiltOncePerPoll()
    {
        var poll = Run().GetProperty("poll");
        foreach (var name in new[] { "keyedLine", "widthKeyLine", "keyedScatter", "keyedBar" })
        {
            Assert.Equal(1, poll.GetProperty(name).GetProperty("builds").GetInt32());
            Assert.Equal(1234, poll.GetProperty(name).GetProperty("vbW").GetDouble());
        }

        Assert.Equal(2, poll.GetProperty("unkeyedLine").GetProperty("builds").GetInt32());
        Assert.Equal(2, poll.GetProperty("unkeyedBar").GetProperty("builds").GetInt32());
    }

    /// <summary>Every composed-panel call of the three renderers passes a width key (the panel's identity, which exists
    /// for the editor preview too), so no composed chart is built twice per poll. The other callers reach the line chart
    /// through <c>zoomableLineChart</c>, which always sets a menuKey. A source pin: the harness cannot run compose.js.</summary>
    [Fact]
    public void EveryRendererCallInCompose_PassesAWidthKey()
    {
        var compose = ReadRepoFileLf(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "compose.js"));
        foreach (var renderer in new[] { "renderLineChart", "renderScatterChart", "renderBarChart" })
        {
            var at = compose.IndexOf(renderer + "({", StringComparison.Ordinal);
            Assert.True(at >= 0, renderer + " is no longer called from compose.js");
            Assert.Single(System.Text.RegularExpressions.Regex.Matches(compose, renderer + @"\(\{"));
            // the call and the rest of its switch case, up to the case's break
            var window = compose.Substring(at, compose.IndexOf("break;", at, StringComparison.Ordinal) - at);
            Assert.Contains("widthKey: widthId", window, StringComparison.Ordinal);
        }

        Assert.Contains("const widthId = composedPanelId(panelSpec, opts.scope, opts.panelSlot);", compose, StringComparison.Ordinal);
        var charts = ReadRepoFileLf(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "charts.js"));
        Assert.Contains("menuKey: id + \"|\" + scope,", charts, StringComparison.Ordinal);
    }

    /// <summary>Source pins for what the harness cannot see: the stretching viewBox is gone from all three charts, the CSS
    /// that would stretch the SVG no longer applies to them, and the print rule (a source pin, because a print layout is
    /// not something Node can run) puts the plot SVG back in the flow at the page width.</summary>
    [Fact]
    public void TheStretchingViewBox_IsGone_AndTheCssDoesNotScaleThePlotSvg()
    {
        var charts = ReadRepoFileLf(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "charts.js"));
        Assert.DoesNotContain("preserveAspectRatio: \"none\"", charts, StringComparison.Ordinal);
        Assert.DoesNotContain("preserveAspectRatio: \"xMinYMin meet\"", charts, StringComparison.Ordinal);
        Assert.DoesNotContain("BAR_W", charts, StringComparison.Ordinal);
        Assert.Contains("class: \"plot-svg\"", charts, StringComparison.Ordinal);

        var css = ReadRepoFileLf(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css"));
        Assert.Contains(".chart svg.plot-svg { position: absolute;", css, StringComparison.Ordinal);
        Assert.Contains(".chart .chart-plot { position: relative; width: 100%; overflow: hidden; }", css, StringComparison.Ordinal);
        Assert.Contains("@media print {", css, StringComparison.Ordinal);
        Assert.Contains(".chart svg.plot-svg { position: static; width: 100% !important; height: auto !important; }", css, StringComparison.Ordinal);
        Assert.Contains(".chart .chart-plot { height: auto !important; overflow: visible; }", css, StringComparison.Ordinal);
    }
}
