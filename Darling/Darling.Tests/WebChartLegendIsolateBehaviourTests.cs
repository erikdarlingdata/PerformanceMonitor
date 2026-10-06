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
/// A web line chart's legend hides, shows and isolates a series (#5247), and an entry that drills keeps its drill beside
/// the switch, by mouse and keyboard. These run the shipped <c>charts.js</c> under Node
/// (<c>web-chart-legend-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebChartLegendIsolateBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-legend-harness.mjs"));
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
                Assert.Fail("the chart legend harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart legend harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement S(JsonElement r, string name) => r.GetProperty(name);
    private static string[] Off(JsonElement s) => s.GetProperty("off").EnumerateArray().Select(e => e.GetString()!).ToArray();
    private static int Lines(JsonElement s) => s.GetProperty("lines").GetInt32();
    private static double Top(JsonElement s) => s.GetProperty("top").GetDouble();
    private static string[] Drilled(JsonElement s) => s.GetProperty("drilled").EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ClickingAnEntry_HidesTheSeries_AndClickingAgainShowsIt()
    {
        var r = Run();
        Assert.Equal(3, Lines(S(r, "initial")));
        Assert.Equal(2, Lines(S(r, "afterHide")));
        Assert.Equal(3, Lines(S(r, "afterShow")));
        Assert.Empty(Off(S(r, "afterShow")));
    }

    [Fact]
    public void AHiddenSeries_StaysInTheLegend_MarkedHidden()
    {
        var hidden = S(Run(), "afterHide");
        Assert.Equal(new[] { "BIG" }, Off(hidden));
        Assert.Equal("false", hidden.GetProperty("pressed").GetProperty("BIG").GetString());
        Assert.Equal("true", hidden.GetProperty("pressed").GetProperty("SMALL").GetString());
    }

    [Fact]
    public void TheAxis_RescalesToTheVisibleSeries()
    {
        var r = Run();
        Assert.True(Top(S(r, "initial")) >= 1900, "the dominant series sets the full axis");
        Assert.True(Top(S(r, "rescaled")) <= 10, "with it hidden, the axis fits the small series");
        Assert.True(Top(S(r, "isolated")) <= 5);
        Assert.Equal(Top(S(r, "initial")), Top(S(r, "afterShow")));
    }

    [Fact]
    public void AStackedChart_RestacksOverTheVisibleSeries()
    {
        var r = Run();
        Assert.True(Top(S(r, "stackedBefore")) >= 2900);
        Assert.True(Top(S(r, "stackedAfter")) <= 10);
    }

    [Fact]
    public void Isolate_ShowsExactlyOneSeries_AndASecondIsolateRestoresAll()
    {
        var r = Run();
        Assert.Equal(1, Lines(S(r, "isolated")));
        Assert.Equal(new[] { "BIG", "MID" }, Off(S(r, "isolated")));
        Assert.Equal(3, Lines(S(r, "isolateUndone")));
    }

    [Fact]
    public void ShowAll_AppearsOnlyWhileSomethingIsHidden_AndRestoresEverything()
    {
        var r = Run();
        Assert.False(S(r, "initial").GetProperty("showAll").GetBoolean());
        Assert.True(S(r, "afterHide").GetProperty("showAll").GetBoolean());
        Assert.Equal("Show all (2 hidden)", S(r, "beforeShowAll").GetProperty("showAllText").GetString());
        var after = S(r, "afterShowAll");
        Assert.False(after.GetProperty("showAll").GetBoolean());
        Assert.Equal(3, Lines(after));
    }

    [Fact]
    public void ToggleNeverHidesTheLastVisibleSeries()
    {
        var kept = S(Run(), "lastKept");
        Assert.Equal(1, Lines(kept));
        Assert.Equal(new[] { "BIG", "SMALL" }, Off(kept));
    }

    [Fact]
    public void TheHiddenState_SurvivesARebuildOfTheSameChart()
    {
        var r = Run();
        Assert.Equal(new[] { "BIG" }, Off(S(r, "rebuilt")));
        Assert.Equal(2, Lines(S(r, "rebuilt")));
        Assert.Equal("big", S(r, "held")[0].GetString());
    }

    [Fact]
    public void TheHiddenState_DoesNotLeakToAnotherServerRangeOrChart()
    {
        var r = Run();
        foreach (var other in new[] { "otherServer", "otherRange", "otherChart" })
        {
            Assert.Empty(Off(S(r, other)));
            Assert.Equal(3, Lines(S(r, other)));
        }
    }

    [Fact]
    public void AComposedPanel_KeepsItsHiddenSeriesAcrossARebuild_AndDoesNotLeakToAnotherPanelOrServer()
    {
        var r = Run();
        Assert.Equal(new[] { "AAA" }, Off(S(r, "composedHidden")));
        Assert.Equal(new[] { "AAA" }, Off(S(r, "composedRebuilt")));
        Assert.Empty(Off(S(r, "composedOtherPanel")));
        Assert.Empty(Off(S(r, "composedOtherServer")));
    }

    [Fact]
    public void ExportToCsv_CarriesExactlyTheShownSeries()
    {
        var r = Run();
        Assert.Equal(new[] { "BIG", "SMALL", "MID" }, S(r, "csvAll").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal(new[] { "SMALL", "MID" }, S(r, "csvHidden").EnumerateArray().Select(e => e.GetString()!).ToArray());
    }

    // A legend entry that carries a drill (a grouped series, onSelect) keeps it beside the switch: the label drills, the
    // swatch hides or shows. The two gestures never overlap, and neither one loses its keyboard path.
    [Fact]
    public void ADrillingEntry_DrillsFromItsLabel_AndItsSwatchHidesOrShowsWithoutDrilling()
    {
        var r = Run();
        var start = S(r, "drillInitial");
        Assert.Equal("item drillable switchable", start.GetProperty("itemClass").GetString());
        Assert.Equal("Filter to BIG", start.GetProperty("itemTitle").GetString());
        Assert.Equal("true", start.GetProperty("swatchPressed").GetString());

        var label = S(r, "drillAfterLabelClick");
        Assert.Equal(new[] { "BIG" }, Drilled(label));
        Assert.Empty(Off(label));
        Assert.Equal(3, Lines(label));

        var swatch = S(r, "drillAfterSwatchClick");
        Assert.Equal(new[] { "BIG" }, Drilled(swatch));
        Assert.Equal(new[] { "BIG" }, Off(swatch));
        Assert.Equal(2, Lines(swatch));
        Assert.Equal("false", swatch.GetProperty("swatchPressed").GetString());

        var hidden = S(r, "drillWhileHidden");
        Assert.Equal(new[] { "BIG", "BIG" }, Drilled(hidden));
        Assert.Equal(new[] { "BIG" }, Off(hidden));
    }

    [Fact]
    public void ADrillingEntrysLabel_IsAKeyboardControl_WhereEnterAndSpaceDrill_AndOtherKeysDoNot()
    {
        var r = Run();
        var start = S(r, "drillInitial");
        Assert.Equal("button", start.GetProperty("labelRole").GetString());
        Assert.Equal("0", start.GetProperty("labelTab").GetString());

        var keys = S(r, "drillAfterKeys");
        Assert.Equal(new[] { "SMALL", "MID" }, Drilled(keys));
        Assert.Empty(Off(keys));
        Assert.Equal(3, Lines(keys));
    }

    [Fact]
    public void OnADrillingEntry_TheSwatchSwitchesByKeyAndDoubleClick_AndNeverDrills()
    {
        var r = Run();
        Assert.Equal(new[] { "SMALL" }, Off(S(r, "drillSwatchEnter")));
        Assert.Equal(new[] { "BIG", "MID" }, Off(S(r, "drillSwatchShiftEnter")));
        Assert.Equal(1, Lines(S(r, "drillSwatchShiftEnter")));
        Assert.Empty(Off(S(r, "drillSwatchDblClick")));
        foreach (var step in new[] { "drillSwatchEnter", "drillSwatchShiftEnter", "drillSwatchDblClick" })
            Assert.Empty(Drilled(S(r, step)));
    }

    [Fact]
    public void ADrillingEntryWithNoSwitch_DrillsFromTheWholeEntry_ByClickOrKey_OncePerClick()
    {
        var r = Run();
        var start = S(r, "noSwitchInitial");
        Assert.Equal("item drillable", start.GetProperty("itemClass").GetString());
        Assert.Equal("button", start.GetProperty("itemRole").GetString());
        Assert.Equal("0", start.GetProperty("itemTab").GetString());
        Assert.Equal(0, start.GetProperty("switchable").GetInt32());
        Assert.False(start.GetProperty("showAll").GetBoolean());

        Assert.Equal(new[] { "BIG" }, Drilled(S(r, "noSwitchSwatchClick")));
        Assert.Equal(new[] { "BIG", "SMALL" }, Drilled(S(r, "noSwitchLabelClick")));
        var keys = S(r, "noSwitchKeys");
        Assert.Equal(new[] { "BIG", "SMALL", "MID", "MID" }, Drilled(keys));
        Assert.Equal(3, Lines(keys));
    }

    [Fact]
    public void TheKeyboard_TogglesWithEnterOrSpace_IsolatesWithShiftEnter_AndLeavesOtherKeysAlone()
    {
        var r = Run();
        Assert.Equal("button", S(r, "kbRoles").GetProperty("role").GetString());
        Assert.Equal("0", S(r, "kbRoles").GetProperty("tab").GetString());
        Assert.Equal(new[] { "BIG" }, Off(S(r, "kbEnterHides")));
        Assert.Empty(Off(S(r, "kbSpaceShows")));
        Assert.Empty(Off(S(r, "kbOtherKey")));
        Assert.Equal(1, Lines(S(r, "kbShiftEnterIsolates")));
        Assert.Equal(new[] { "BIG", "SMALL" }, Off(S(r, "kbShiftEnterIsolates")));
        Assert.Empty(Off(S(r, "kbShiftClickRestores")));
        // Only Enter and Space are claimed (so the page does not scroll on Space); any other key is left alone.
        Assert.Equal(new[] { "Enter", " ", "Enter" }, r.GetProperty("kbPrevented").EnumerateArray().Select(e => e.GetString()!).ToArray());
    }

    [Fact]
    public void AHeldHiddenSet_ThatWouldHideEverySeriesOfARefreshedChart_DrawsThemAll_NotAnEmptyPlot()
    {
        var shrunk = S(Run(), "shrunk");
        Assert.Equal(2, Lines(shrunk));
        Assert.Empty(Off(shrunk));
        Assert.False(shrunk.GetProperty("showAll").GetBoolean());
    }
}
