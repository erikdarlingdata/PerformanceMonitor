/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A saved dashboard's poll builds each composed panel again, and the new build must draw the zoom and the drill the
/// reader set (#7923). These run the shipped <c>compose.js</c> under Node (<c>web-composed-view-hold-harness.mjs</c>):
/// the harness brushes a zoom and drills on a panel card, builds the same panel again, and reports the body of that second
/// run and the chips it drew. Skipped when Node is not installed.
/// </summary>
public sealed class WebComposedViewHoldBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-composed-view-hold-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped composed panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the composed view hold harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the composed view hold harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement S(JsonElement r, string name) => r.GetProperty(name);
    private static string? Str(JsonElement s, string name) => s.GetProperty(name).ValueKind == JsonValueKind.Null ? null : s.GetProperty(name).GetString();
    private static string[] Filters(JsonElement s) => s.GetProperty("filters").EnumerateArray().Select(e => e.GetString()!).ToArray();
    private static int ZoomChips(JsonElement s) => s.GetProperty("zoomChip").GetInt32();
    private static int DrillChips(JsonElement s) => s.GetProperty("drillChip").GetInt32();

    [Fact]
    public void ABrushZoom_IsDrawnAgainByTheNextBuildOfThePanel()
    {
        var r = Run();
        var zoomed = S(r, "zoomed");
        Assert.NotNull(Str(zoomed, "windowStart"));
        var rebuilt = S(r, "zoomRebuilt");
        Assert.Equal(Str(zoomed, "windowStart"), Str(rebuilt, "windowStart"));
        Assert.Equal(Str(zoomed, "windowEnd"), Str(rebuilt, "windowEnd"));
        Assert.Equal(1, ZoomChips(rebuilt));
    }

    [Fact]
    public void ADrill_IsDrawnAgainByTheNextBuildOfThePanel_BesideTheHeldZoom()
    {
        var r = Run();
        Assert.Equal(new[] { "wait=AAA" }, Filters(S(r, "drilled")));
        var rebuilt = S(r, "drillRebuilt");
        Assert.Equal(new[] { "wait=AAA" }, Filters(rebuilt));
        Assert.Equal(1, DrillChips(rebuilt));
        Assert.Equal(Str(S(r, "zoomed"), "windowStart"), Str(rebuilt, "windowStart"));
        Assert.Equal(1, ZoomChips(rebuilt));
    }

    [Fact]
    public void TheReadersOwnClear_IsHeld_SoTheNextBuildDrawsNeither()
    {
        var r = Run();
        var zoomCleared = S(r, "zoomClearedRebuilt");
        Assert.Null(Str(zoomCleared, "windowStart"));
        Assert.Equal(0, ZoomChips(zoomCleared));
        Assert.Equal(new[] { "wait=AAA" }, Filters(zoomCleared));

        var allCleared = S(r, "allClearedRebuilt");
        Assert.Empty(Filters(allCleared));
        Assert.Equal(0, DrillChips(allCleared));
        Assert.Equal(0, ZoomChips(allCleared));
    }

    [Theory]
    [InlineData("otherPanel")]
    [InlineData("otherServer")]
    [InlineData("otherPage")]
    [InlineData("otherRange")]
    public void TheHold_DoesNotReachAnotherPanelServerPageOrRange(string scenario)
    {
        var other = S(Run(), scenario);
        Assert.Null(Str(other, "windowStart"));
        Assert.Empty(Filters(other));
        Assert.Equal(0, ZoomChips(other));
        Assert.Equal(0, DrillChips(other));
    }

    [Fact]
    public void APanelWithNoSlot_TheEditorPreview_HoldsNothing()
    {
        var r = Run();
        Assert.NotNull(Str(S(r, "previewZoomed"), "windowStart"));
        var rebuilt = S(r, "previewRebuilt");
        Assert.Null(Str(rebuilt, "windowStart"));
        Assert.Equal(0, ZoomChips(rebuilt));
    }

    [Theory]
    [InlineData("varsSame")]
    [InlineData("varsReordered")]
    public void TheSameViewVariables_InAnyKeyOrder_KeepTheHeldDrillAndZoom(string scenario)
    {
        var r = Run();
        var rebuilt = S(r, scenario);
        var drilled = S(r, "varsDrilled");
        Assert.NotNull(Str(drilled, "windowStart"));
        Assert.Equal(Str(drilled, "windowStart"), Str(rebuilt, "windowStart"));
        Assert.Equal(Str(drilled, "windowEnd"), Str(rebuilt, "windowEnd"));
        Assert.Equal(new[] { "wait=AAA" }, Filters(rebuilt));
        Assert.Equal(1, ZoomChips(rebuilt));
        Assert.Equal(1, DrillChips(rebuilt));
    }

    [Theory]
    [InlineData("varsChanged")]
    [InlineData("varsChangedBack")]
    public void AChangedViewVariable_DropsTheHeldDrillAndZoom_AndTheEntryIsGone(string scenario)
    {
        // varsChangedBack builds again under the ORIGINAL values: it draws neither only if the changed build removed the entry.
        var other = S(Run(), scenario);
        Assert.Null(Str(other, "windowStart"));
        Assert.Empty(Filters(other));
        Assert.Equal(0, ZoomChips(other));
        Assert.Equal(0, DrillChips(other));
    }
}
