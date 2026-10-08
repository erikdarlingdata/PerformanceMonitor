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
/// A web line or scatter chart is drawn at the width its box really has, one SVG unit per CSS pixel (#5586).
///
/// <para><b>The defect.</b> Both charts drew a 1000 x 320 viewBox with <c>preserveAspectRatio="none"</c> and CSS gave the
/// SVG <c>width: 100%</c> but capped its height at 300 px, so above about 940 px the width kept growing while the height
/// stopped, and the axis text was stretched sideways (about 2.1 times at 2,000 px). Below 1,000 px the whole chart,
/// text included, shrank.</para>
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
    /// measurement redraws it. The bar chart keeps its own fixed viewBox.</summary>
    [Fact]
    public void AChartThatIsNotMountedYet_DrawsAtTheDefaultWidth_AndTheBarChartIsUntouched()
    {
        var r = Run();
        Assert.Equal(1000, r.GetProperty("unmounted").GetProperty("vbW").GetDouble());
        Assert.Equal(1000, r.GetProperty("unmountedScatter").GetProperty("vbW").GetDouble());
        Assert.StartsWith("0 0 1000 ", r.GetProperty("barViewBox").GetString(), StringComparison.Ordinal);
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

    /// <summary>The tooltip is placed inside the box the reader sees. Below 300 px the SVG is wider than its clipped host;
    /// RED proof: clamping to the SVG's width puts the tooltip at 176 px in a 260 px box.</summary>
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

    /// <summary>A poll rebuilds every chart. A chart that was measured before (keyed by its menuKey, or the widthKey the
    /// composed panels and the scatter pass) is drawn at that width, so the observer's first report is within a pixel and
    /// the build is the only one; a chart with no key is drawn at the default and redrawn once, as before.</summary>
    [Fact]
    public void AnAlreadyMeasuredChart_IsBuiltOncePerPoll()
    {
        var poll = Run().GetProperty("poll");
        foreach (var name in new[] { "keyedLine", "widthKeyLine", "keyedScatter" })
        {
            Assert.Equal(1, poll.GetProperty(name).GetProperty("builds").GetInt32());
            Assert.Equal(1234, poll.GetProperty(name).GetProperty("vbW").GetDouble());
        }

        Assert.Equal(2, poll.GetProperty("unkeyedLine").GetProperty("builds").GetInt32());
    }

    /// <summary>Source pins for what the harness cannot see: the stretching viewBox is gone from both charts, the CSS
    /// that would stretch the SVG no longer applies to them, and the print rule (a source pin, because a print layout is
    /// not something Node can run) puts the plot SVG back in the flow at the page width.</summary>
    [Fact]
    public void TheStretchingViewBox_IsGone_AndTheCssDoesNotScaleThePlotSvg()
    {
        var charts = ReadRepoFileLf(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "charts.js"));
        Assert.DoesNotContain("preserveAspectRatio: \"none\"", charts, StringComparison.Ordinal);
        Assert.Contains("class: \"plot-svg\"", charts, StringComparison.Ordinal);

        var css = ReadRepoFileLf(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "app.css"));
        Assert.Contains(".chart svg.plot-svg { position: absolute;", css, StringComparison.Ordinal);
        Assert.Contains(".chart .chart-plot { position: relative; width: 100%; overflow: hidden; }", css, StringComparison.Ordinal);
        Assert.Contains("@media print {", css, StringComparison.Ordinal);
        Assert.Contains(".chart svg.plot-svg { position: static; width: 100% !important; height: auto !important; }", css, StringComparison.Ordinal);
        Assert.Contains(".chart .chart-plot { height: auto !important; overflow: visible; }", css, StringComparison.Ordinal);
    }
}
