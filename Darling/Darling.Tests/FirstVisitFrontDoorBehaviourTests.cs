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
/// #9220: a first visit to a server's page through a front door of three stores. The page starts its panel reads before the server's
/// fleet card answers (#8554), and the card of a server with no recent collection (is_online false) used to make it restart them,
/// so the reads in flight were aborted and sent again. These run the shipped <c>app.js</c> and Server page under Node
/// (<c>web-first-visit-front-door-harness.mjs</c>) with three stores whose fleet reads answer at their own virtual times, and count the
/// reads the page aborted or sent twice. The same holds for a first visit by a partial server name, which the card's arrival moved to the
/// server's own name by a new render that aborted the batch in flight and read everything again. Node is skipped when it is not installed.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class FirstVisitFrontDoorBehaviourTests
{
    private const string OfflineCard = "{\"is_online\":false}";

    private static JsonElement Run(string scenario, string tab, string card, int ownStoreFleetMs, int panelReadMs, params (string Key, string Value)[] env)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-first-visit-front-door-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        psi.ArgumentList.Add(tab);
        psi.Environment["CARD"] = card;
        psi.Environment["OWN_MS"] = ownStoreFleetMs.ToString();
        psi.Environment["PANEL_MS"] = panelReadMs.ToString();
        foreach (var (key, value) in env) psi.Environment[key] = value;

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
                Assert.Fail("the first visit harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the first visit harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static void AssertNothingRestarted(JsonElement r)
    {
        Assert.Equal(0, r.GetProperty("aborted").GetArrayLength());
        Assert.Equal(0, r.GetProperty("sentMoreThanOnce").GetArrayLength());
        Assert.Equal(0, r.GetProperty("abortCalls").GetArrayLength());
        Assert.True(r.GetProperty("reads").GetInt32() > 0, "the page sent no panel reads at all");
    }

    // The server's own store answers last. Case 1: after every panel read has finished. Case 2: while the panel reads are still out.
    [Theory]
    [InlineData("overview", 1500, 400)]
    [InlineData("overview", 700, 1500)]
    [InlineData("queries", 1500, 400)]
    [InlineData("queries", 700, 1500)]
    public void FirstVisit_OfStaleServer_KeepsThePanelBatch_AndSendsEachReadOnce(string tab, int ownStoreFleetMs, int panelReadMs)
    {
        AssertNothingRestarted(Run("firstVisit", tab, OfflineCard, ownStoreFleetMs, panelReadMs));
    }

    [Theory]
    [InlineData("overview")]
    [InlineData("queries")]
    public void FirstVisit_OfServerWithRecentCollection_KeepsThePanelBatch(string tab)
    {
        AssertNothingRestarted(Run("firstVisit", tab, "", 700, 1500));
    }

    [Fact]
    public void FirstVisit_ByLinkWithNoStore_OfStaleServer_KeepsThePanelBatch()
    {
        AssertNothingRestarted(Run("firstVisitBareLink", "overview", OfflineCard, 1500, 400));
    }

    // Running Jobs words its empty state by the card's is_online (ctx.offline), so this tab still draws its panels again for a stale server.
    [Fact]
    public void FirstVisit_OfStaleServer_OnTheActivityTab_StillDrawsRunningJobsWithTheCard()
    {
        var r = Run("firstVisit", "activity", OfflineCard, 1500, 400);
        Assert.True(r.GetProperty("sentMoreThanOnce").GetArrayLength() > 0);
    }

    // A first visit by a route name that is only PART of the server's own name (it equals neither the card's display name nor its server
    // name): the panel reads go out under that name, and the card's arrival moves the link to the server's own name. The reads in flight
    // stay, so none is aborted and none is sent a second time under the new name.
    private const string PartialCards = "[{\"server_name\":\"alpha.corp.test\",\"display_name\":\"alpha-prod\"},{\"server_name\":\"beta\",\"display_name\":\"beta\"}]";
    private const string TwoMatchingCards = "[{\"server_name\":\"alpha.corp.test\",\"display_name\":\"alpha-prod\"},{\"server_name\":\"alpha2.corp.test\",\"display_name\":\"alpha-two\"}]";

    private static JsonElement RunPartial(string scenario, string tab, string card, int ownStoreFleetMs, int panelReadMs, string cards) =>
        Run(scenario, tab, card, ownStoreFleetMs, panelReadMs, ("CARDS", cards), ("ROUTE", "alpha"), ("FULL", "alpha.corp.test"), ("NEXT_TAB", tab == "queries" ? "overview" : "queries"));

    private static void AssertReadsUnder(JsonElement byServer, params string[] allowedNames)
    {
        var total = 0;
        foreach (var p in byServer.EnumerateObject())
        {
            Assert.Contains(p.Name, allowedNames);
            total += p.Value.GetInt32();
        }

        Assert.True(total > 0, "no panel read was sent");
    }

    [Theory]
    [InlineData("overview", 700, 1500)]
    [InlineData("overview", 1500, 400)]
    [InlineData("queries", 700, 1500)]
    [InlineData("queries", 1500, 400)]
    public void FirstVisit_ByPartialServerName_KeepsThePanelBatch_AndMovesTheLinkToTheOwnName(string tab, int ownStoreFleetMs, int panelReadMs)
    {
        var r = RunPartial("partialFirstVisit", tab, "", ownStoreFleetMs, panelReadMs, PartialCards);
        AssertNothingRestarted(r);
        // Every read went out under the route's name, once: none was sent again under the card's name.
        AssertReadsUnder(r.GetProperty("byServer"), "alpha", "null");
        Assert.Equal("#/s/use1/server/alpha.corp.test/" + tab, r.GetProperty("finalHash").GetString());
    }

    [Fact]
    public void FirstVisit_ByPartialServerName_OfStaleServer_KeepsThePanelBatch()
    {
        var r = RunPartial("partialFirstVisit", "overview", OfflineCard, 700, 1500, PartialCards);
        AssertNothingRestarted(r);
        Assert.Equal("#/s/use1/server/alpha.corp.test/overview", r.GetProperty("finalHash").GetString());
    }

    // Running Jobs words its empty state by is_online, so a stale server's card still draws this tab's panels afresh, under the own name.
    [Fact]
    public void FirstVisit_ByPartialServerName_OfStaleServer_OnTheActivityTab_StillDrawsWithTheCard()
    {
        var r = RunPartial("partialFirstVisit", "activity", OfflineCard, 700, 1500, PartialCards);
        Assert.True(r.GetProperty("aborted").GetArrayLength() > 0);
        Assert.Equal("#/s/use1/server/alpha.corp.test/activity", r.GetProperty("finalHash").GetString());
    }

    // After the move, the page's own poll and a tab click read by the server's own name.
    [Fact]
    public void AfterThePartialNameMoves_ThePollAndATabClick_ReadByTheOwnName()
    {
        var r = RunPartial("partialThenPollAndTab", "overview", "", 700, 400, PartialCards);
        Assert.Equal(0, r.GetProperty("aborted").GetArrayLength());
        AssertReadsUnder(r.GetProperty("afterPoll"), "alpha.corp.test", "null");
        AssertReadsUnder(r.GetProperty("afterTabClick"), "alpha.corp.test", "null");
        Assert.Equal("#/s/use1/server/alpha.corp.test/queries", r.GetProperty("finalHash").GetString());
    }

    private static void AssertEveryReadUsesTheHeldRange(JsonElement byHours)
    {
        var total = 0;
        foreach (var p in byHours.EnumerateObject())
        {
            // The held range is a live 36 hours. A read with no hours at all is not a panel read of the range (the catalog, the alert badge).
            Assert.True(p.Name is "36" or "null", "a panel read went out with hours=" + p.Name + " instead of the held range");
            if (p.Name == "36") total += p.Value.GetInt32();
        }

        Assert.True(total > 0, "no panel read carried the held range");
    }

    // A custom range held under the PARTIAL name (a range set from the link before the card lands) follows the move to the own name. The reads in
    // flight were sent with it and are kept (none aborted, none sent again), the picker the new render draws names it, and the page's next poll
    // reads the own name with the same range instead of the preset.
    [Theory]
    [InlineData("overview")]
    [InlineData("queries")]
    public void FirstVisit_ByPartialServerName_WithACustomRangeHeld_KeepsTheBatch_AndTheRangeFollowsTheMove(string tab)
    {
        var r = RunPartial("partialCustomRange", tab, "", 700, 1500, PartialCards);
        AssertNothingRestarted(r);
        AssertReadsUnder(r.GetProperty("byServer"), "alpha", "null");
        AssertEveryReadUsesTheHeldRange(r.GetProperty("byHours"));
        Assert.Equal(new[] { "Past 36 hours" }, r.GetProperty("pickerText").EnumerateArray().Select(e => e.GetString()).ToArray());
        AssertReadsUnder(r.GetProperty("afterPoll"), "alpha.corp.test", "null");
        AssertEveryReadUsesTheHeldRange(r.GetProperty("afterPollHours"));
        Assert.Equal("#/s/use1/server/alpha.corp.test/" + tab, r.GetProperty("finalHash").GetString());
    }

    // The restart path (a stale server's Running Jobs tab draws afresh under the own name) keeps the range too: nothing reads the preset, and the
    // picker still names the held range.
    [Fact]
    public void FirstVisit_ByPartialServerName_WithACustomRangeHeld_OnTheRestartPath_KeepsTheRange()
    {
        var r = RunPartial("partialCustomRange", "activity", OfflineCard, 700, 1500, PartialCards);
        Assert.True(r.GetProperty("aborted").GetArrayLength() > 0, "the stale server's Running Jobs tab should draw again under the own name");
        AssertEveryReadUsesTheHeldRange(r.GetProperty("byHours"));
        Assert.Equal(new[] { "Past 36 hours" }, r.GetProperty("pickerText").EnumerateArray().Select(e => e.GetString()).ToArray());
        AssertReadsUnder(r.GetProperty("afterPoll"), "alpha.corp.test", "null");
        AssertEveryReadUsesTheHeldRange(r.GetProperty("afterPollHours"));
    }

    // A partial name that fits two servers shows its message and reads nothing once the card answers; the link stays as it was.
    [Fact]
    public void FirstVisit_ByAmbiguousPartialName_ReadsNothingAfterTheCard()
    {
        var r = RunPartial("partialAmbiguous", "overview", "", 700, 1500, TwoMatchingCards);
        Assert.Equal(0, r.GetProperty("startedAfterTheCard").GetInt32());
        Assert.Equal(0, r.GetProperty("rewrites").GetArrayLength());
        Assert.Equal("#/s/use1/server/alpha/overview", r.GetProperty("finalHash").GetString());
    }

    // The reads a partial-name visit kept are still the page's reads: opening another server ends them.
    [Fact]
    public void LeavingForAnotherServer_AfterAPartialNameMoved_AbortsTheReadsKept()
    {
        var r = RunPartial("leaveForAnotherServer", "queries", "", 700, 1500, PartialCards);
        Assert.Equal(0, r.GetProperty("abortedBeforeLeaving").GetInt32());
        var started = r.GetProperty("startedForAlpha").GetInt32();
        Assert.True(started > 0);
        Assert.Equal(0, r.GetProperty("answeredForAlpha").GetInt32());
        Assert.Equal(started, r.GetProperty("abortedForAlpha").GetInt32());
    }

    // A real navigation still ends the page's reads: the first server's reads are aborted when the person opens another server.
    [Fact]
    public void LeavingForAnotherServer_AbortsTheReadsOfTheServerLeft()
    {
        var r = Run("leaveForAnotherServer", "queries", OfflineCard, 700, 1500);
        Assert.Equal(0, r.GetProperty("abortedBeforeLeaving").GetInt32());
        var started = r.GetProperty("startedForAlpha").GetInt32();
        Assert.True(started > 0);
        Assert.Equal(0, r.GetProperty("answeredForAlpha").GetInt32());
        Assert.Equal(started, r.GetProperty("abortedForAlpha").GetInt32());
    }
}
