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
/// reads the page aborted or sent twice. Node is skipped when it is not installed.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class FirstVisitFrontDoorBehaviourTests
{
    private const string OfflineCard = "{\"is_online\":false}";

    private static JsonElement Run(string scenario, string tab, string card, int ownStoreFleetMs, int panelReadMs)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-first-visit-front-door-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        psi.ArgumentList.Add(tab);
        psi.Environment["CARD"] = card;
        psi.Environment["OWN_MS"] = ownStoreFleetMs.ToString();
        psi.Environment["PANEL_MS"] = panelReadMs.ToString();

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
