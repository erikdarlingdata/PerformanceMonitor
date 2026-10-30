/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
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
    private static JsonElement Run(string? auditBody = null)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-partial-window-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        if (auditBody is not null) psi.ArgumentList.Add(auditBody);

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
    public void ALeftOutNote_IsDrawnOnAnEmptyAnswer_OnlyForAGridThatNamesIt()
    {
        var r = Run();
        Assert.StartsWith("These databases were left out of this read", S(r, "leftOutNamed"));
        Assert.StartsWith("These databases were left out of this read", S(r, "leftOutMore"));
        Assert.Null(S(r, "leftOutUnnamed"));
        Assert.Null(S(r, "leftOutOtherKey"));
        Assert.Null(S(r, "leftOutAbsent"));
        Assert.Null(S(r, "leftOutBlank"));
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
    public void ASpaceSeparatedOrUtcMarkedInstant_PrintsLocal_AndAServerClockOneIsLeftAlone()
    {
        // #5644 W20 (H2): the alert text spells an instant "2026-10-11 12:48:52Z" and "... 23:13:00 UTC"; the old pattern needed a "T".
        var h = Run().GetProperty("h2");
        string G(string n) => h.GetProperty(n).GetString()!;
        Assert.Equal(G("expSpaceZ"), G("spaceZ"));
        Assert.Equal(G("expSpaceUtc"), G("spaceUtc"));
        Assert.Equal(G("expFrac7"), G("frac7"));
        Assert.Equal(G("expSpaceFrac7"), G("spaceFrac7"));
        Assert.Equal(G("expSpaceZ"), G("stripUtc"));
        Assert.Equal("start time 2026-09-27 21:32:43 (server clock)", G("serverClock"));
    }

    [Fact]
    public void AnAlertBodyOnTheWeb_ShowsItsTimeLinesLocal_AndLeavesStatementsAndServerClockLinesAlone()
    {
        // #5644 W20 (H2): Triage and Alert History printed "Incident Since: 2026-10-12 23:13:00Z", "Failed At: ...Z" and a time-of-day-only "23:12:45 UTC".
        var h = Run().GetProperty("h2");
        string G(string n) => h.GetProperty(n).GetString()!;
        var here = h.GetProperty("here");
        string H(string n) => here.GetProperty(n).GetString()!;
        var expected = string.Join("\n", new[]
        {
            "Blocking chain (x2)",
            "  Incident Since: " + H("inc"),
            "  Blocked Query: SELECT 1 WHERE d = '2026-01-01 00:00:00Z'",
            "  Failed At: " + H("fail"),
            "  Started: 2026-09-27 21:32:43 (server clock, UTC offset not collected)",
            "  Window: " + H("fail") + " → " + H("end"),
            "",
            "Newest blocked process sample: " + H("samp") + ", 5m 20s old",
        });
        Assert.Equal(expected, G("body"));
        // No newest_sample on the row, or one whose time of day is not the line's: the time-of-day-only line is left as stored.
        Assert.Contains("Newest blocked process sample: 23:12:45 UTC, 5m 20s old", G("bodyNoSample"));
        Assert.Contains("Newest blocked process sample: 23:12:45 UTC, 5m 20s old", G("bodyOtherClock"));
        Assert.Contains("  Incident Since: " + H("inc"), G("bodyNoSample"));
        Assert.Contains("Incident Since: 2026-10-12 23:13:00Z", G("bodyRaw"));
    }

    [Fact]
    public void TheCollectionHistoryAuditAlert_AgreesItsSourceCount_AndKeepsEveryOtherClauseAsWritten()
    {
        // #6649: Alert History printed "in 1 source(s)". The body is CollectionHistoryAudit.Render's own output: ONE line of
        // ". "-joined clauses (one flagged source, every other real hourly source unread), so only its first sentence may change.
        var sources = CollectionHistoryAudit.Rollups;
        var rendered = CollectionHistoryAudit.Render(
            new DateTime(2026, 10, 20, 0, 0, 0, DateTimeKind.Utc),
            new (CollectionHistoryAudit.Rollup, IReadOnlyList<CollectionHistoryAudit.FlaggedRange>)[]
            {
                (sources[0], new[] { new CollectionHistoryAudit.FlaggedRange(18, 21, 0, 40, 0, 0) }),
            },
            sources.Skip(1).ToList())
            // #6700: Render now says "in 1 source". An alert row stored before that fix still holds "in 1 source(s)", and
            // that row is what the web rewrite is for, so the body here is the old row's text.
            .Replace("in 1 source, where usual", "in 1 source(s), where usual", StringComparison.Ordinal);
        Assert.DoesNotContain('\n', rendered);
        Assert.Contains("in 1 source(s), where usual", rendered);

        var h2 = Run(rendered).GetProperty("h2");
        var body = h2.GetProperty("auditBody").GetString()!;
        Assert.Equal(rendered.Replace("in 1 source(s), where usual", "in 1 source, where usual", StringComparison.Ordinal), body);

        // The match ends at its own sentence: a later clause that the full plain-text pass would rewrite prints as written.
        var probe = h2.GetProperty("auditBodyWithProbe").GetString()!;
        Assert.Contains("Probe: 29 database(s) were left out (see databases_left_out).", probe);
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
