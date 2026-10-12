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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every panel built from a row-capped read says so (#5644 W12): the System Events "system_health CPU" chart plotted only the
/// newest 50 events under a "last 24 hours" title with no notice, and the Active Queries grid showed about an hour under the same
/// title. These run the shipped <c>util.js</c> under Node (<c>web-partial-window-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebPartialWindowBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-partial-window-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

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
                Assert.Fail("the partial window harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the partial window harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string? S(JsonElement r, string name) => r.GetProperty(name).ValueKind == JsonValueKind.Null ? null : r.GetProperty(name).GetString();

    [Fact]
    public void ARowCappedChart_DrawsTheSameNoticeAsItsGrid_WorkedForAChart()
    {
        var r = Run();
        Assert.StartsWith("partial window: this grid shows only the newest rows", S(r, "gridServerNote"));
        var chart = S(r, "chartServerNote")!;
        Assert.StartsWith("partial window: this chart shows only the newest rows", chart);
        Assert.Contains("The chart covers", chart);
        Assert.DoesNotContain("grid", chart);
    }

    [Fact]
    public void AChart_DoesNotDrawADataStartNote_BecauseItsAxisShowsTheEmptySpan()
    {
        var r = Run();
        Assert.Null(S(r, "chartDataStart"));
        Assert.Null(S(r, "chartWindowNoteOff"));
    }

    [Fact]
    public void ACutNewestFirstPage_GetsANoticeWithItsCountsAndOldestRow()
    {
        var r = Run();
        var oldest = S(r, "oldestLocal")!;
        Assert.Equal("partial window: this grid shows only the newest 50 of 219 snapshots, back to " + oldest + ", because it stops at its row limit.", S(r, "activeGrid"));
        Assert.StartsWith("partial window: this chart shows only the newest 50 of 219 snapshots", S(r, "activeChart"));
        Assert.Equal("partial window: this grid shows only the newest rows, back to " + oldest + ", because it stops at its row limit.", S(r, "activeNoCounts"));
    }

    [Fact]
    public void ACutPage_ThatNamesNoTime_OrIsNotCut_OrOwnsItsNote_GetsNothing()
    {
        var r = Run();
        Assert.Null(S(r, "activeStat"));
        Assert.Null(S(r, "notCut"));
        Assert.Null(S(r, "rankedByOther"));
        Assert.Null(S(r, "noOldest"));
        Assert.Null(S(r, "ownNote"));
    }

    [Fact]
    public void TheServersOwnNote_IsNotDrawnTwice()
    {
        var r = Run();
        Assert.Contains("The window started at S.", S(r, "serverNoteWins"));
    }

    [Fact]
    public void AnIsoInstantInASentence_PrintsInTheGridsLocalTimeFormat()
    {
        // #5644 W20: server messages and status lines carried raw UTC stamps beside local-time grids.
        var r = Run();
        var a = S(r, "isoLocal")!;
        var b = S(r, "isoZLocal")!;
        Assert.Equal("stored at " + a + " and not collected since " + b + ".", S(r, "sentence"));
        Assert.Equal("last event " + a, S(r, "emptyStripText"));
        Assert.Equal("since " + b, S(r, "noticeStripText"));
        // G1a: plainText (lane C, W8) and localizeIsoText (lane E, W20) both run in the strips.
        Assert.Equal("TRUNCATED: since " + b + ": 9 indexes match and 5 were returned. Rows are ordered by total wait time descending.", S(r, "bothEmpty"));
        Assert.Equal(S(r, "bothEmpty"), S(r, "bothNotice"));
        // The restart banner's start-time pair is the server's own clock (not UTC): the strip leaves it as written, never shifted.
        Assert.Equal(S(r, "bannerText"), S(r, "bannerNotice"));
        Assert.Equal("nothing to change 10/9/2026, 10:36:32 AM", S(r, "noStamp"));
        Assert.Equal("null", S(r, "nonString"));
        Assert.DoesNotContain("T", a);
    }

    [Fact]
    public void ACappedPanelsHeading_DropsItsWindowLabel_AndAnyOtherKeepsIt()
    {
        var r = Run();
        var oldest = S(r, "oldestLocal")!;
        Assert.Equal("newest rows only, back to " + oldest, S(r, "subtitleActive"));
        Assert.Equal("newest rows only, back to " + oldest, S(r, "subtitleServer"));
        Assert.Null(S(r, "subtitleDataStart"));
        Assert.Null(S(r, "subtitleCovered"));
        Assert.Equal("Active Queries newest rows only, back to " + oldest, S(r, "headingCapped"));
        Assert.Equal(" not the heading", S(r, "bodyUntouched"));
        Assert.Equal("Active Queries last 24 hours", S(r, "headingCovered"));
        Assert.Equal("No sub", S(r, "headingWithoutSub"));
    }
}
