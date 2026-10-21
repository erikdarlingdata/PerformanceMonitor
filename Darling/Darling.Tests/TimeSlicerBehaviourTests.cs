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
/// #6301: the time-range slicer over the web's Top Queries and Top Procedures cards. The shipped page (server-tabs.js, panels.js, util.js and
/// time-slicer.js) runs under Node in <c>time-slicer-harness.mjs</c> against a scripted /api/read answer: the harness drags the slicer, picks a row,
/// redraws the tab, presses a key and resets, and prints what the page asked for and drew. The window is the last 24 hours ending at a fixed minute,
/// so a drag from 25% to 50% of the strip is hours 6 to 12 and its <c>as_of</c> is plain arithmetic. Node is skipped when it is not installed.
/// </summary>
public sealed class TimeSlicerBehaviourTests
{
    private static readonly Lazy<JsonElement?> Result = new(Run);

    private static JsonElement? Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "time-slicer-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return null;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the time slicer harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the time slicer harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement Page()
    {
        var run = Result.Value;
        if (run is null)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
        }

        return run!.Value;
    }

    private static (string? Hours, string? AsOf) Ask(JsonElement ask) =>
        (ask.GetProperty("hours").GetString(), ask.GetProperty("as_of").GetString());

    /// <summary>The CPU tab draws one slicer over each of its two grids, and each strip draws the scripted duration trend as a path.</summary>
    [Fact]
    public void EachRankedCard_HasASlicer_WithItsStripDrawn()
    {
        var page = Page();
        Assert.Equal(2, page.GetProperty("slicers").GetInt32());
        Assert.True(page.GetProperty("stripDrawn").GetBoolean());
        Assert.Equal(("24", null), Ask(page.GetProperty("initial").GetProperty("queries")));
        Assert.Equal(("24", null), Ask(page.GetProperty("initial").GetProperty("procedures")));
    }

    /// <summary>Releasing a drag from 25% to 50% of the strip makes BOTH grids read hours 6 to 12 of the window: 6 hours ending at that window's end.</summary>
    [Fact]
    public void ReleasingTheDrag_MakesTheGridsReadTheSelectedWindow()
    {
        var dragged = Page().GetProperty("dragged");
        var expectedAsOf = dragged.GetProperty("expectedAsOf").GetString();
        Assert.Equal(("6", expectedAsOf), Ask(dragged.GetProperty("queries")));
        Assert.Equal(("6", expectedAsOf), Ask(dragged.GetProperty("procedures")));
        Assert.StartsWith("Narrowed to", dragged.GetProperty("readout").GetString());
        Assert.Contains("narrowed to", Page().GetProperty("draggedTitle").GetString());
    }

    /// <summary>Picking a Top Queries row overlays THAT query's own trend (get_query_trend, keyed on its hash) over the page range on the strip.</summary>
    [Fact]
    public void PickingARow_OverlaysThatRowsTrend()
    {
        var overlay = Page().GetProperty("overlay");
        var ask = overlay.GetProperty("ask");
        Assert.Equal("0x1", ask.GetProperty("hash").GetString());
        Assert.Equal(("24", null), Ask(ask));
        Assert.StartsWith("M", overlay.GetProperty("path").GetString());
        Assert.Contains("db1", overlay.GetProperty("note").GetString());
        Assert.True(overlay.GetProperty("selected").GetBoolean());
    }

    /// <summary>The narrowed window survives the tab being drawn again (a tab switch, the 60 s rebuild) and the row pick comes back with it.</summary>
    [Fact]
    public void TheNarrowedWindow_SurvivesARedraw()
    {
        var survived = Page().GetProperty("survived");
        Assert.Equal(("6", Page().GetProperty("dragged").GetProperty("expectedAsOf").GetString()), Ask(survived.GetProperty("queries")));
        Assert.StartsWith("Narrowed to", survived.GetProperty("readout").GetString());
        Assert.True(survived.GetProperty("overlayAgain").GetBoolean());
    }

    /// <summary>An arrow key moves the selection, and the grids read the moved window once the keys stop.</summary>
    [Fact]
    public void AnArrowKey_MovesTheSelection_AndTheGridsReadIt()
    {
        var page = Page();
        var keyed = Ask(page.GetProperty("keyed").GetProperty("queries"));
        Assert.Equal("6", keyed.Hours);
        Assert.NotNull(keyed.AsOf);
        Assert.True(
            DateTime.Parse(keyed.AsOf!).ToUniversalTime() > DateTime.Parse(page.GetProperty("dragged").GetProperty("expectedAsOf").GetString()!).ToUniversalTime(),
            "ArrowRight moves the window later");
    }

    /// <summary>Reset puts the page's own range back: both grids read it with no end of their own.</summary>
    [Fact]
    public void Reset_RestoresThePageRange()
    {
        var reset = Page().GetProperty("reset");
        Assert.Equal(("24", null), Ask(reset.GetProperty("queries")));
        Assert.Equal(("24", null), Ask(reset.GetProperty("procedures")));
        Assert.StartsWith("Whole range", reset.GetProperty("readout").GetString());
    }

    /// <summary>A narrowing belongs to the page range it was made under: a different range starts whole.</summary>
    [Fact]
    public void AChangedPageRange_DropsTheNarrowing()
    {
        var other = Page().GetProperty("otherRange");
        Assert.Equal(("24", null), Ask(other.GetProperty("queries")));
        Assert.StartsWith("Whole range", other.GetProperty("readout").GetString());
    }
}
