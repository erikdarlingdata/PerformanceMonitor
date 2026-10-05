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
/// The web sidebar's server search and tag grouping (#5243): the shipped <c>sidebar.js</c> and the grouping rules it
/// shares with the Fleet page (<c>fleet-groups.js</c>), run under Node (<c>sidebar-harness.mjs</c>) against a fake DOM
/// and a stubbed <c>localStorage</c>. Skipped when Node is not installed.
/// </summary>
public sealed class SidebarSearchGroupTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "sidebar-harness.mjs"));
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
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the sidebar harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the sidebar harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.ValueKind == JsonValueKind.String ? e.GetString()! : e.ToString()).ToArray();

    private static readonly string[] AllFive = { "Alpha", "Bravo", "Charlie", "Delta", "Echo" };

    [Fact]
    public void TheSearch_MatchesDisplayName_ServerName_AndTagName_IgnoringCaseAndEdgeSpaces()
    {
        if (!TryRun("filter", out var r)) return;

        Assert.Equal(new[] { "Alpha" }, Strings(r.GetProperty("byDisplay")));
        Assert.Equal(new[] { "Charlie" }, Strings(r.GetProperty("byServerName")));
        Assert.Equal(new[] { "Bravo", "Echo" }, Strings(r.GetProperty("byTag")));
        Assert.Equal(new[] { "Alpha", "Bravo" }, Strings(r.GetProperty("byTagCase")));
        Assert.Equal(new[] { "Delta" }, Strings(r.GetProperty("padded")));
        Assert.Equal(AllFive, Strings(r.GetProperty("empty")));
        Assert.True(r.GetProperty("none").GetProperty("empty").GetBoolean());
    }

    [Fact]
    public void TheGroupedList_IsFavourites_ThenTheTagTreeDepthFirst_ThenUntagged_AndAMultiTagServerAppearsUnderEach()
    {
        if (!TryRun("groupedOrder", out var r)) return;

        // Bravo is in Prod (under East) and West: it is listed under both.
        Assert.Equal(
            new[] { "G:East(1)@0", "S:Charlie@1", "G:Prod(2)@1", "S:Alpha@2", "S:Bravo@2", "G:West(2)@0", "S:Bravo@1", "S:Echo@1", "G:Untagged(1)@0", "S:Delta@1" },
            Strings(r.GetProperty("plain")));

        // Favourites lead; the same servers still appear under their tags. The empty Spare tag takes no line.
        Assert.Equal("G:Favourites(2)@0", Strings(r.GetProperty("withFavourites"))[0]);
        Assert.Equal(new[] { "S:Delta@1", "S:Echo@1", "G:East(1)@0" }, Strings(r.GetProperty("withFavourites")).Skip(1).Take(3));

        // Collapsing East hides its servers and the whole Prod subtree; collapsing Untagged hides only its server.
        Assert.Equal(
            new[] { "G:East(1,c)@0", "G:West(2)@0", "S:Bravo@1", "S:Echo@1", "G:Untagged(1)@0", "S:Delta@1" },
            Strings(r.GetProperty("collapsedEast")));
        Assert.Equal("G:Untagged(1,c)@0", Strings(r.GetProperty("collapsedUntagged")).Last());

        // A filter keeps a parent whose child matches (East holds nothing itself), and drops the groups that have no match.
        Assert.Equal(
            new[] { "G:East(0)@0", "G:Prod(1)@1", "S:Bravo@2", "G:West(2)@0", "S:Bravo@1", "S:Echo@1" },
            Strings(r.GetProperty("filtered")));
        Assert.True(r.GetProperty("noMatch").GetProperty("empty").GetBoolean());
    }

    [Fact]
    public void TheSearchTerm_SurvivesTheRepaint_TheNoMatchLineIsOne_AndTheGroupedToggleIsStoredByIdOnly()
    {
        if (!TryRun("paint", out var r)) return;

        Assert.Equal(AllFive, Strings(r.GetProperty("flat")));
        Assert.Equal(new[] { "Alpha", "Bravo" }, Strings(r.GetProperty("typed")));

        // The 60 s poll hands a fresh payload (here in another order) to the same list; the term is still applied.
        Assert.Equal(new[] { "Alpha", "Bravo" }, Strings(r.GetProperty("afterPoll")));
        Assert.Equal("Prod", r.GetProperty("termAfterPoll").GetString());
        Assert.Equal(new[] { "Bravo" }, Strings(r.GetProperty("activeAfterPoll")));

        Assert.Equal("No servers match", r.GetProperty("noMatchText").GetString());
        Assert.Equal(1, r.GetProperty("noMatchRows").GetInt32());
        Assert.Equal(AllFive, Strings(r.GetProperty("cleared")));

        Assert.Equal(
            new[] { "G:East", "S:Charlie", "G:Prod", "S:Alpha", "S:Bravo", "G:West", "S:Bravo", "S:Echo", "G:Untagged", "S:Delta" },
            Strings(r.GetProperty("groupedLines")));
        Assert.True(r.GetProperty("stored").GetProperty("grouped").GetBoolean());
        Assert.Equal(new[] { "G:East", "G:West", "S:Bravo", "S:Echo", "G:Untagged", "S:Delta" }, Strings(r.GetProperty("afterCollapse")));
        Assert.Equal(new[] { "1" }, Strings(r.GetProperty("storedAfterCollapse").GetProperty("collapsedGroups")));
        Assert.DoesNotContain("East", r.GetProperty("storedText").GetString());
    }

    [Fact]
    public void TheGroupedView_AndCollapsedGroups_RoundTripThroughStorage_WithoutDroppingTheCollapsedSidebarFlag()
    {
        if (!TryRun("roundTrip", out var r)) return;

        Assert.True(r.GetProperty("grouped").GetBoolean());
        Assert.Equal(new[] { "2", "untagged", "favourites" }, Strings(r.GetProperty("collapsed")));
        Assert.True(r.GetProperty("sidebarCollapsed").GetBoolean());
        Assert.Equal(1, r.GetProperty("stored").GetProperty("v").GetInt32());
        Assert.Equal(new[] { "untagged" }, Strings(r.GetProperty("afterExpand")));
        Assert.Empty(r.GetProperty("fleetKeys").EnumerateArray());
        Assert.True(r.GetProperty("storesNoNames").GetBoolean());
    }

    [Fact]
    public void CorruptOrForeignStoredSidebarState_FallsBackToTheFlatListWithNothingCollapsed()
    {
        if (!TryRun("corruptStorage", out var r)) return;

        foreach (var name in new[] { "garbage", "arrayRoot", "foreignVersion", "wrongTypes", "badEntries" })
        {
            var c = r.GetProperty("results").GetProperty(name);
            Assert.False(c.GetProperty("grouped").GetBoolean(), name);
            Assert.Equal(AllFive, Strings(c.GetProperty("painted")));
        }

        foreach (var name in new[] { "garbage", "arrayRoot", "foreignVersion", "wrongTypes" })
            Assert.Empty(r.GetProperty("results").GetProperty(name).GetProperty("collapsed").EnumerateArray());

        // Entries that are not a tag id or a fixed group id are dropped; a valid one beside them is kept.
        Assert.Equal(new[] { "untagged" }, Strings(r.GetProperty("results").GetProperty("badEntries").GetProperty("collapsed")));
    }

    [Fact]
    public void TheFleetPage_AndTheSidebar_UseTheSharedHelpers_NeitherKeepsACopy()
    {
        var fleet = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "fleet.js");
        var shared = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "fleet-groups.js");
        var sidebar = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "sidebar.js");
        var app = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js");

        Assert.Contains("import { buildTagGroups, cardMatches } from \"../fleet-groups.js\";", fleet, StringComparison.Ordinal);
        Assert.DoesNotContain("function buildTagGroups(", fleet, StringComparison.Ordinal);
        Assert.DoesNotContain("function cardMatches(", fleet, StringComparison.Ordinal);
        Assert.Contains("export function buildTagGroups(", shared, StringComparison.Ordinal);
        Assert.Contains("export function cardMatches(", shared, StringComparison.Ordinal);
        Assert.Contains("cardMatches(c, fleetFilter)", fleet, StringComparison.Ordinal);

        Assert.Contains("from \"./fleet-groups.js\"", sidebar, StringComparison.Ordinal);
        Assert.DoesNotContain("function cardMatches(", sidebar, StringComparison.Ordinal);
        Assert.Contains("paintServerList(serverList,", app, StringComparison.Ordinal);
        Assert.DoesNotContain("darling.fleet.", sidebar, StringComparison.Ordinal);
        Assert.DoesNotContain("innerHTML", sidebar, StringComparison.Ordinal);
    }
}
