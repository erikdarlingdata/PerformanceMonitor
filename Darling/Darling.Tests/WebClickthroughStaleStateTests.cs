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
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Release click-through findings on stale state and one slow read (#5489). The web half runs the shipped page
/// script under Node (<c>web-clickthrough-stale-harness.mjs</c>): the fleet card chips of a server that stopped
/// collecting, and the Wait Stats tab's read count. The Viewer half drives the WPF fleet card's own properties,
/// which are the twin of the web chips. Node is skipped when it is not installed.
/// </summary>
public sealed class WebClickthroughStaleStateTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-clickthrough-stale-harness.mjs"));
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
                Assert.Fail("the click-through harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the click-through harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Texts(JsonElement chips) =>
        chips.EnumerateArray().Select(c => c.GetProperty("text").GetString()!).ToArray();

    [Fact]
    public void AServerPastTheOfflineMark_ReadsNotAvailableWithItsAge_NotLiveLookingValues()
    {
        var r = Run("cards");
        var offline = r.GetProperty("offline");
        var texts = Texts(offline);

        /* CPU, Threads, Memory, Blocking and Deadlocks: n/a with the age, never 0% / 77 free / ok. */
        for (var i = 0; i < 5; i++)
        {
            Assert.Contains("n/a", texts[i], StringComparison.Ordinal);
            Assert.Contains("last collected 12d ago", texts[i], StringComparison.Ordinal);
            Assert.Contains("sev-Unknown", offline[i].GetProperty("cls").GetString(), StringComparison.Ordinal);
        }

        Assert.DoesNotContain(texts, t => t.Contains("0%", StringComparison.Ordinal) || t.Contains("free", StringComparison.Ordinal) || t.Contains("ok", StringComparison.Ordinal));
        Assert.StartsWith("CollectorsStale", texts[5], StringComparison.Ordinal);
    }

    /// <summary>#6901: a chip's detail is clamped to one line, so its full text must sit on the detail's title, or a long sentence is unreadable.</summary>
    [Fact]
    public void AChipWithALongDetail_KeepsTheFullDetailOnItsTitle()
    {
        var agent = Run("longDetail").GetProperty("chips").EnumerateArray()
            .Single(c => c.GetProperty("text").GetString()!.StartsWith("Agent alerts", StringComparison.Ordinal));
        var detail = agent.GetProperty("detail").GetString();
        Assert.Equal("No alert is set for severity 19 through 25 errors. No alert is set for error 823, 824 or 825 (corruption).", detail);
        Assert.Equal(detail, agent.GetProperty("detailTitle").GetString());
    }

    /// <summary>
    /// #7395: on the server page a long tile detail (20 overdue backup items) stops at about three lines and opens on demand with the full text on its
    /// title, a short one shows in full, and the fleet card's tiles are left as they were built.
    /// </summary>
    [Fact]
    public void TheServerPageTiles_ClampALongDetailAndOpenItOnDemand_AndLeaveShortOnesAndTheFleetCardAlone()
    {
        var r = Run("serverTiles");
        var server = r.GetProperty("server").EnumerateArray().ToArray();
        var protection = server.Single(t => t.GetProperty("label").GetString()!.StartsWith("Protection", StringComparison.Ordinal));
        Assert.Equal("detail clamp-lines", protection.GetProperty("cls").GetString());
        Assert.Equal("button", protection.GetProperty("role").GetString());
        Assert.Equal("false", protection.GetProperty("expanded").GetString());
        Assert.Equal(protection.GetProperty("textLen").GetInt32(), protection.GetProperty("titleLen").GetInt32());

        /* The short tiles (CPU, Threads, the one Agent alerts gap) are not clamped: no class, no button role. */
        foreach (var t in server.Where(t => t.TryGetProperty("cls", out _) && !t.GetProperty("label").GetString()!.StartsWith("Protection", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("clamp-lines", t.GetProperty("cls").GetString(), StringComparison.Ordinal);
            Assert.Equal(JsonValueKind.Null, t.GetProperty("role").ValueKind);
        }

        /* The fleet card keeps its one-line ellipsis: the same long detail has no clamp class and no button role. */
        var card = r.GetProperty("fleetCard").EnumerateArray().Single(t => t.GetProperty("label").GetString()!.StartsWith("Protection", StringComparison.Ordinal));
        Assert.Equal("detail", card.GetProperty("cls").GetString());
        Assert.Equal(JsonValueKind.Null, card.GetProperty("role").ValueKind);

        /* A click opens it, Enter closes it again, and each stops its event so a card around the tile is not also activated. */
        Assert.Equal("false", r.GetProperty("before").GetString());
        Assert.Equal("true", r.GetProperty("afterClick").GetString());
        Assert.Equal("false", r.GetProperty("afterEnter").GetString());
        Assert.Equal(2, r.GetProperty("stopped").GetInt32());
    }

    /// <summary>
    /// #7395: a tile the reader opened stays open when the server head is drawn again (every poll and tab click), matched by its label
    /// since its text changes. A tile that went short has nothing to reopen and starts closed if it grows back, and another server's head
    /// (its own remembered set) starts closed.
    /// </summary>
    [Fact]
    public void AnOpenedServerTile_StaysOpenWhenTheHeadIsDrawnAgain_AndStartsClosedOnAnotherServerOrAfterItWentShort()
    {
        var r = Run("serverTilesReopen");
        Assert.Equal("false", r.GetProperty("closedAtFirst").GetString());
        Assert.Equal("true", r.GetProperty("openedByClick").GetString());
        // The CSS reads the open class (.detail.clamp-lines.open), not aria-expanded, so the class is what shows the tile open.
        Assert.Contains("open", r.GetProperty("clsAfterClick").GetString()!.Split(' '));
        Assert.Equal(["Protection"], r.GetProperty("labelsAfterClick").EnumerateArray().Select(l => l.GetString()).ToArray());

        var poll = r.GetProperty("afterPoll");
        Assert.Equal("true", poll.GetProperty("expanded").GetString());
        Assert.Contains("clamp-lines", poll.GetProperty("cls").GetString(), StringComparison.Ordinal);
        Assert.Contains("open", poll.GetProperty("cls").GetString()!.Split(' '));
        Assert.Single(r.GetProperty("labelsAfterPoll").EnumerateArray());

        Assert.DoesNotContain("clamp-lines", r.GetProperty("afterShort").GetProperty("cls").GetString(), StringComparison.Ordinal);
        Assert.Empty(r.GetProperty("labelsAfterShort").EnumerateArray());
        Assert.Equal("false", r.GetProperty("afterRegrown").GetProperty("expanded").GetString());
        Assert.DoesNotContain("open", r.GetProperty("afterRegrown").GetProperty("cls").GetString()!.Split(' '));
        Assert.Equal("false", r.GetProperty("afterAnotherServer").GetProperty("expanded").GetString());
    }

    /// <summary>#6828: the server header's Collectors chip counts a WARNING collector as a warning, not as healthy.</summary>
    [Fact]
    public void TheCollectorsChip_ShowsTheWarningCount_AndDoesNotCountWarningsAsHealthy()
    {
        var texts = Texts(Run("collectorsWarning").GetProperty("chips"));
        Assert.Contains("39 healthy · 3 warning · 0 failing", texts[5], StringComparison.Ordinal);
    }

    /// <summary>#6828: the chip's detail ends with the not-collected count in the Viewer's order, and shows none when there are none.</summary>
    [Fact]
    public void TheCollectorsChip_ShowsTheNotCollectedCount_LastAndOnlyWhenAboveZero()
    {
        var shown = Texts(Run("collectorsWarning").GetProperty("chips"));
        Assert.Contains("39 healthy · 3 warning · 0 failing · 2 not collected", shown[5], StringComparison.Ordinal);
        var none = Texts(Run("collectorsNoneNotCollected").GetProperty("chips"));
        Assert.DoesNotContain("not collected", none[5], StringComparison.Ordinal);
    }

    /// <summary>#6178: the Overview Threads chip of an Azure SQL Database server gives the reason beside its "n/a"; a box with no reading keeps a bare "n/a".</summary>
    [Fact]
    public void TheThreadsChip_GivesTheReasonOnAzureSqlDatabase_AndNothingOnABox()
    {
        var r = Run("azureThreads");
        Assert.Equal("Threadsn/aAzure SQL Database has no thread data", Texts(r.GetProperty("azure"))[1]);
        Assert.Equal("Threadsn/a", Texts(r.GetProperty("box"))[1]);
    }

    /// <summary>#7590: a server whose connect failed shows its last values as "last known" in the Unknown tone and a Collectors chip that says collection is waiting on the connection with the error, not "Stale" and not the stale-server n/a tiles.</summary>
    [Fact]
    public void AnUnreachableServer_ShowsLastKnownValuesAndCollectorsWaitingOnTheConnection()
    {
        var r = Run("unreachable");
        var down = r.GetProperty("down");
        var texts = Texts(down);

        foreach (var i in new[] { 0, 1, 2 })
        {
            Assert.Contains("last known, 2h ago", texts[i], StringComparison.Ordinal);
            Assert.Contains("sev-Unknown", down[i].GetProperty("cls").GetString(), StringComparison.Ordinal);
        }

        Assert.DoesNotContain("last collected", string.Concat(texts), StringComparison.Ordinal);
        Assert.Equal("CollectorsWaitingwaiting on the connection · Login failed for user 'pm'.", texts[5]);
        Assert.Contains("sev-Unknown", down[5].GetProperty("cls").GetString(), StringComparison.Ordinal);

        Assert.Equal("CollectorsWaitingwaiting on the connection · server unreachable", Texts(r.GetProperty("downNeverCollected"))[5]);
        Assert.Contains("last known, no value", Texts(r.GetProperty("downNeverCollected"))[0], StringComparison.Ordinal);

        /* Only a stale server (no failed connect) keeps the n/a tiles. */
        Assert.StartsWith("CollectorsStale", Texts(r.GetProperty("stale"))[5], StringComparison.Ordinal);
    }

    [Fact]
    public void AnOnlineServer_KeepsItsLiveValues()
    {
        var online = Texts(Run("cards").GetProperty("online"));
        Assert.Equal("Total CPU0%SQL 0%", online[0]);
        Assert.StartsWith("Threads77 free", online[1], StringComparison.Ordinal);
        Assert.StartsWith("MemoryOK", online[2], StringComparison.Ordinal);
    }

    [Fact]
    public void AStaleAvailabilityGroupCard_SaysNoCurrentData_AndAFreshOneDoesNot()
    {
        var r = Run("agStale");
        var stale = Texts(r.GetProperty("stale")).Single();
        Assert.StartsWith("Stale: no current data. ", stale, StringComparison.Ordinal);
        Assert.Contains("last collected", stale, StringComparison.Ordinal);
        var fresh = Texts(r.GetProperty("fresh")).Single();
        Assert.DoesNotContain("Stale", fresh, StringComparison.Ordinal);
        Assert.Contains("collected just now", fresh, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWaitStatsTab_AsksForTheSpinlockAndLatchListsOnce_NotOncePerPanel()
    {
        /* The Spinlock Stats grid and the Spinlock Trend picker (and the latch pair) read the same tool with the
           same arguments; at 7 days on a large store the spinlock read took 3.0 s and 5.7 s, paid twice. */
        var r = Run("waits");
        Assert.Equal(1, r.GetProperty("spinlock").GetInt32());
        Assert.Equal(1, r.GetProperty("latch").GetInt32());
    }

    [Fact]
    public void ASharedRead_CancelsOnlyTheCallerThatAborted_AndALaterCallSendsAFreshRequest()
    {
        var r = Run("joinAbort");
        Assert.Equal("aborted", r.GetProperty("first").GetString());
        Assert.Equal("data", r.GetProperty("second").GetString());
        Assert.Equal(1, r.GetProperty("sentTogether").GetInt32());
        Assert.Equal("data", r.GetProperty("later").GetString());
        Assert.Equal(2, r.GetProperty("sent").GetInt32());
    }

    [Fact]
    public void TheViewerFleetCard_OfflineReadsDashesAndAge_NotLiveValues()
    {
        var card = new ServerSummaryItem
        {
            IsOnline = false,
            CpuPercent = 0,
            MemoryMb = 908,
            TotalThreads = 100,
            CurrentWorkers = 23,
            LastCollectionTime = DateTime.UtcNow.AddDays(-12),
        };

        Assert.Equal("--", card.CpuDisplay);
        Assert.Equal("--", card.MemoryDisplay);
        Assert.Equal("--", card.ThreadsDisplay);
        Assert.Equal("--", card.BlockingDisplay);
        Assert.Equal("--", card.DeadlockDisplay);
        Assert.StartsWith("last collected 1w ago", card.CpuDetail, StringComparison.Ordinal);
        Assert.Equal("", card.ThreadsDetail);

        var live = new ServerSummaryItem { IsOnline = true, CpuPercent = 0, MemoryMb = 908, TotalThreads = 100, CurrentWorkers = 23 };
        Assert.Equal("0%", live.CpuDisplay);
        Assert.Equal("0.9 GB", live.MemoryDisplay);
        Assert.Equal("OK", live.ThreadsDisplay);
    }
}
