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
/// #7409: runs the shipped PostgreSQL Waits, Activity and I/O tabs against scripted answers and checks what the page draws. A panel
/// whose read answers "not collected" is not drawn, the two tab banners stop saying the tab needs Amazon Aurora when it fills
/// elsewhere, the I/O Busiest tile draws a dash when nothing was read and says "(by reads)" when the read count ranked, the
/// Blocking Sampling tiles draw true zeros when captures exist and a dash when none do, and a dash is never drawn in the small size.
/// </summary>
public sealed class WebPgPageTruthsBehaviourTests
{
    private static readonly Lazy<JsonElement> Page = new(RunHarness);

    private static JsonElement RunHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-page-truths-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the page truths harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the page truths harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string Text(string scenario, string tile) => Page.Value.GetProperty(scenario).GetProperty(tile).GetProperty("text").GetString()!;

    private static bool Small(string scenario, string tile) => Page.Value.GetProperty(scenario).GetProperty(tile).GetProperty("small").GetBoolean();

    [Theory]
    [InlineData("Wait Events")]
    [InlineData("Sampled Waits")]
    [InlineData("Wait Trend")]
    public void AWaitsPanel_WhoseReadAnswersNotCollected_IsNotDrawn_AndOneWithRowsIs(string title)
    {
        Assert.True(Page.Value.GetProperty("notCollected").GetProperty(title).GetBoolean(), title + " stayed on the page after a not collected answer");
        Assert.False(Page.Value.GetProperty("withRows").GetProperty(title).GetBoolean(), title + " was hidden although its read had rows");
    }

    [Fact]
    public void TheWaitsBanner_NoLongerSaysTheTabStaysEmpty_AndNamesTheTwoPanelsThatFill()
    {
        var banner = Page.Value.GetProperty("wait").GetProperty("note").GetString()!;

        Assert.DoesNotContain("stays empty", banner);
        Assert.Contains("Sampled Waits", banner);
        Assert.Contains("Wait Trend", banner);
        Assert.Contains("Amazon Aurora", banner);
    }

    [Fact]
    public void TheActivityBanner_SaysTopQueryShapesNeedsPgStatStatements_NotAurora()
    {
        var banner = Page.Value.GetProperty("activity").GetProperty("note").GetString()!;

        Assert.DoesNotContain("stays empty", banner);
        Assert.Contains("pg_stat_statements", banner);
        Assert.False(banner.Contains("Top Query Shapes needs Amazon Aurora", StringComparison.Ordinal), banner);
    }

    [Fact]
    public void TheBusiestTile_DrawsAFullSizeDash_WhenNothingWasRead()
    {
        Assert.Equal("—", Text("ioNothing", "Busiest"));
        Assert.False(Small("ioNothing", "Busiest"), "the dash was drawn in the small tile size");
        Assert.Equal("0", Text("ioNothing", "Reads"));
    }

    [Fact]
    public void TheBusiestTile_SaysByReads_OnlyWhenTheReadCountRanked()
    {
        Assert.Equal("client backend/relation/normal (by reads)", Text("ioByReads", "Busiest"));
        Assert.True(Small("ioByReads", "Busiest"), "a long name is drawn in the small size");
        Assert.Equal("client backend/relation/normal", Text("ioByTime", "Busiest"));
    }

    [Fact]
    public void TheBlockingSamplingTiles_DrawTrueZeros_WhenCapturesExistAndNothingWasBlocked()
    {
        Assert.Equal("1437", new string(Text("blockedNone", "Captures").Where(char.IsDigit).ToArray()));
        Assert.Equal("0", Text("blockedNone", "Worst chain victims"));
        Assert.Equal("0", Text("blockedNone", "Cycles"));
        /* One decimal place, and the separator follows the machine's locale: the digits are all zero. */
        var percent = Text("blockedNone", "Of captures (%)");
        Assert.NotEqual("—", percent);
        Assert.All(percent.Where(char.IsDigit), digit => Assert.Equal('0', digit));
    }

    [Fact]
    public void TheBlockingSamplingTiles_DrawDashes_WhenNoCaptureExists()
    {
        foreach (var tile in new[] { "Of captures (%)", "Worst chain victims", "Cycles" })
            Assert.Equal("—", Text("blockedUnavailable", tile));
    }

    [Fact]
    public void ADashInASmallTile_IsDrawnAtTheFullTileSize_AndARealValueKeepsTheSmallSize()
    {
        Assert.Equal("—", Text("blockedNone", "Root state"));
        Assert.False(Small("blockedNone", "Root state"), "the Root state dash was drawn in the small size");
        Assert.False(Small("blockedUnavailable", "Root state"));
        Assert.Equal("idle in transaction", Text("blockedWithChain", "Root state"));
        Assert.True(Small("blockedWithChain", "Root state"));
    }
}
