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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Dragging across a server-tab or FinOps line chart narrows its x-axis to the brushed span over the points it already
/// loaded, a reset chip puts the full axis back, and the zoom survives the 60 s poll's rebuild. These run the shipped
/// <c>charts.js</c> under Node (<c>web-chart-zoom-harness.mjs</c>): a 101-point series, a pointer drag across the plot,
/// then a rebuild under the same and under a different key. Node is skipped when it is not installed; the source pins
/// at the end hold without it.
/// </summary>
public sealed class WebChartZoomBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-zoom-harness.mjs"));
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
                Assert.Fail("the chart zoom harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart zoom harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Labels(JsonElement shown) =>
        shown.GetProperty("labels").EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ABrush_NarrowsTheDomainToTheBrushedSpan_OverTheLoadedPoints()
    {
        var r = Run();
        Assert.False(r.GetProperty("before").GetProperty("chip").GetBoolean());
        // The drag ran from minute 20 to minute 40; the held span is that, to within a pixel of the 8 px floor.
        Assert.InRange(r.GetProperty("zoom").GetProperty("from").GetDouble(), 19.9, 20.1);
        Assert.InRange(r.GetProperty("zoom").GetProperty("to").GetDouble(), 39.9, 40.1);
        // 21 points inside the span plus one neighbour each side, so the line reaches both axis edges.
        Assert.Equal(23, r.GetProperty("appliedPoints").GetInt32());
        Assert.InRange(r.GetProperty("appliedWindow")[0].GetDouble(), 19.9, 20.1);
        Assert.InRange(r.GetProperty("appliedWindow")[1].GetDouble(), 39.9, 40.1);
        var zoomed = r.GetProperty("zoomed");
        Assert.True(zoomed.GetProperty("chip").GetBoolean());
        Assert.NotEqual(Labels(r.GetProperty("before")), Labels(zoomed));
    }

    [Fact]
    public void Reset_RestoresTheFullDomain_AndClearsTheHeldZoom()
    {
        var r = Run();
        Assert.True(r.GetProperty("preReset").GetProperty("chip").GetBoolean());
        var post = r.GetProperty("postReset");
        Assert.False(post.GetProperty("chip").GetBoolean());
        Assert.Equal(Labels(r.GetProperty("before")), Labels(post));
        Assert.False(r.GetProperty("postResetHeld").GetBoolean());
    }

    [Fact]
    public void TheZoom_IsReappliedAfterARebuild_UnderTheSameKey_AndNotUnderAnother()
    {
        var r = Run();
        var rebuilt = r.GetProperty("rebuilt");
        Assert.True(rebuilt.GetProperty("chip").GetBoolean());
        Assert.Equal(Labels(r.GetProperty("zoomed")), Labels(rebuilt));

        foreach (var other in new[] { "otherServer", "otherChart", "otherRange" })
        {
            var shown = r.GetProperty(other);
            Assert.False(shown.GetProperty("chip").GetBoolean(), other + " drew a zoom that belongs to another key");
            Assert.Equal(Labels(r.GetProperty("before")), Labels(shown));
        }
    }

    [Fact]
    public void AShortDrag_IsAClick_AndHoldsNoZoom()
    {
        Assert.False(Run().GetProperty("clickHeld").GetBoolean());
    }

    [Fact]
    public void AnEmptySpanBrush_StoresNothing_AndALaterPointInTheSpanDoesNotZoom()
    {
        var r = Run();
        Assert.False(r.GetProperty("emptyHeld").GetBoolean());
        Assert.False(r.GetProperty("emptyChip").GetBoolean());
        Assert.False(r.GetProperty("emptyLater").GetProperty("chip").GetBoolean());
        Assert.Equal(Labels(r.GetProperty("emptyLaterBaseline")), Labels(r.GetProperty("emptyLater")));
    }

    [Fact]
    public void AHeldZoom_ThatNoLongerHoldsAPoint_IsDroppedAtDraw()
    {
        var r = Run();
        Assert.False(r.GetProperty("agedOutHeld").GetBoolean());
        Assert.False(r.GetProperty("agedOutChip").GetBoolean());
    }

    [Fact]
    public void TheZoomedLine_KeepsOneNeighbourEachSide_AndThePlotClipsAtTheEdge()
    {
        var r = Run();
        var minutes = r.GetProperty("edgeMinutes").EnumerateArray().Select(e => e.GetDouble()).ToArray();
        Assert.Equal(20d, minutes.First());
        Assert.Equal(41d, minutes.Last());
        Assert.Equal(22, minutes.Length);
        Assert.True(r.GetProperty("lineClipped").GetBoolean());
        var clip = r.GetProperty("clipRect");
        var xs = r.GetProperty("lineXs").EnumerateArray().Select(e => e.GetDouble()).ToArray();
        Assert.True(xs.First() < clip.GetProperty("x").GetDouble());
        Assert.True(xs.Last() > clip.GetProperty("x").GetDouble() + clip.GetProperty("w").GetDouble());
    }

    [Fact]
    public void TheScope_IgnoresTheHashQuery()
    {
        Assert.True(Run().GetProperty("scopeQueryStripped").GetBoolean());
    }

    private static string Js(params string[] parts) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" }.Concat(parts).ToArray())
            .ReplaceLineEndings("\n");

    /// <summary>Every <c>renderLineChart(</c> caller in the web script either wraps the zoom (<c>zoomableLineChart</c>, or
    /// passes <c>onZoom</c> itself) or is named here with the reason it is exempt.</summary>
    [Fact]
    public void EveryLineChartCaller_PassesOnZoom_OrIsOnTheExemptList()
    {
        // File → why it draws a line chart without the client-side zoom.
        var exempt = new System.Collections.Generic.Dictionary<string, string>
        {
            // Custom Views re-run the panel on the brushed window through their own onZoom.
            ["compose.js"] = "passes onZoom",
            // The wrapper itself.
            ["charts.js"] = "zoomableLineChart supplies onZoom",
        };
        var root = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");
        foreach (var file in Directory.EnumerateFiles(root, "*.js", SearchOption.AllDirectories))
        {
            var name = Path.GetFileName(file);
            var text = File.ReadAllText(file).ReplaceLineEndings("\n");
            var calls = Regex.Matches(text, @"(?<![\w.])renderLineChart\s*\(").Count;
            if (name == "charts.js")
            {
                // The declaration, and the wrapper's own call.
                calls -= 2;
                Assert.True(calls >= 0, "charts.js lost the renderLineChart declaration or the wrapper's call.");
            }
            if (calls == 0)
            {
                continue;
            }

            Assert.True(exempt.ContainsKey(name), name + " calls renderLineChart directly; use zoomableLineChart or list it as exempt with its reason.");
            Assert.True(name == "charts.js" || text.Contains("onZoom"), name + " is exempt because it passes onZoom, but does not.");
        }

        // The server-tab and FinOps chart callers go through the zoomable wrapper.
        Assert.Equal(5, Regex.Matches(Js("pages", "server-tabs.js"), @"zoomableLineChart\(").Count);
        Assert.Contains("zoomableLineChart(", Js("pages", "finops", "version-store.js"));
        var panels = Js("panels.js");
        var vizLine = panels.Substring(panels.IndexOf("function vizLine(", StringComparison.Ordinal));
        vizLine = vizLine.Substring(0, vizLine.IndexOf("\n}\n", StringComparison.Ordinal));
        Assert.Contains("zoomableLineChart(", vizLine);
        Assert.DoesNotContain("renderLineChart(", vizLine);
    }

    [Fact]
    public void CustomViews_ShareTheChipFromCharts_AndStillPassOnZoom()
    {
        var compose = Js("compose.js");
        Assert.Contains("zoomChip", compose);
        Assert.DoesNotContain("function zoomChip(", compose);
        Assert.Contains("onZoom,", compose);
        Assert.Contains("export function zoomChip(", Js("charts.js"));
    }
}
