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
/// The web dashboard's per-browser viewer state (<c>viewer-local.js</c>: favourites, acknowledgements, the alert-count
/// badge, severity colours, the collapsed sidebar), run under Node against a stubbed <c>localStorage</c>
/// (<c>viewer-local-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class ViewerLocalStateBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "viewer-local-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "viewer-local.js"));
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the viewer-local harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the viewer-local harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static int[] Ints(JsonElement node) => node.EnumerateArray().Select(e => e.GetInt32()).ToArray();

    [Fact]
    public void AFavourite_SortsFirst_AndSurvivesARebuild()
    {
        if (!TryRun("favorites", out var r)) return;

        Assert.Equal(new[] { 3, 1, 2 }, Ints(r.GetProperty("sorted")));
        Assert.True(r.GetProperty("afterReload").GetBoolean());
        Assert.False(r.GetProperty("afterUntoggle").GetBoolean());
        Assert.Equal(1, r.GetProperty("stored").GetProperty("v").GetInt32());
    }

    [Fact]
    public void Acknowledge_QuietsTheBadgeUntilANewerAlert_AndSurvivesARebuild()
    {
        if (!TryRun("ack", out var r)) return;

        var before = r.GetProperty("before");
        Assert.Equal(2, before.GetProperty("count").GetInt32());
        Assert.True(before.GetProperty("critical").GetBoolean());
        Assert.True(before.GetProperty("show").GetBoolean());
        Assert.Equal(0, r.GetProperty("mutedAndResolved").GetProperty("count").GetInt32());
        Assert.Equal(0, r.GetProperty("dismissed").GetProperty("count").GetInt32());

        Assert.False(r.GetProperty("acked").GetProperty("show").GetBoolean());
        Assert.False(r.GetProperty("afterReloadSameRows").GetProperty("show").GetBoolean());
        Assert.True(r.GetProperty("afterNewer").GetProperty("show").GetBoolean());
        Assert.False(r.GetProperty("persistedCleared").GetBoolean());
    }

    [Fact]
    public void AbsentCorruptOrForeignStorage_DegradesToDefaults_WithoutThrowing()
    {
        if (!TryRun("corrupt", out var r)) return;

        foreach (var name in new[] { "garbage", "arrayRoot", "nullRoot", "foreignVersion", "wrongTypes" })
        {
            var c = r.GetProperty("results").GetProperty(name);
            Assert.Equal(JsonValueKind.Object, c.ValueKind);
            Assert.Empty(c.GetProperty("fav").EnumerateArray());
            Assert.Empty(c.GetProperty("ack").EnumerateArray());
            Assert.All(c.GetProperty("colors").EnumerateArray(), e => Assert.Equal(JsonValueKind.Null, e.ValueKind));
            Assert.False(c.GetProperty("collapsed").GetBoolean());
        }

        // A right-version value keeps only its valid entries.
        var bad = r.GetProperty("results").GetProperty("badEntries");
        Assert.Equal(new[] { 1, 7 }, Ints(bad.GetProperty("fav")));
        Assert.All(bad.GetProperty("colors").EnumerateArray(), e => Assert.Equal(JsonValueKind.Null, e.ValueKind));
        Assert.False(bad.GetProperty("collapsed").GetBoolean());

        Assert.False(r.GetProperty("unreadable").GetProperty("fav").GetBoolean());
        Assert.True(r.GetProperty("unwritableStillWorksInSession").GetBoolean());
        Assert.False(r.GetProperty("noStorage").GetBoolean());
    }

    [Fact]
    public void ASeverityColour_IsOnlyEverAHexLiteral_ElseThePaletteDefault()
    {
        if (!TryRun("colors", out var r)) return;

        Assert.Equal("#aabbcc", r.GetProperty("good").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("afterBad").ValueKind);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("unknownSeverity").ValueKind);
        Assert.Equal("#aabbcc", r.GetProperty("props").GetProperty("--sev-critical").GetString());
        Assert.Single(r.GetProperty("props").EnumerateObject());
        Assert.Equal("#aabbcc", r.GetProperty("afterReload").GetString());

        var edited = r.GetProperty("handEdited").EnumerateArray().ToArray();
        Assert.Equal(JsonValueKind.Null, edited[0].ValueKind);
        Assert.Equal("#00ff00", edited[1].GetString());
    }

    [Fact]
    public void TheCollapsedSidebar_IsRemembered()
    {
        if (!TryRun("sidebar", out var r)) return;

        Assert.False(r.GetProperty("initial").GetBoolean());
        Assert.True(r.GetProperty("afterReload").GetBoolean());
        Assert.False(r.GetProperty("afterExpand").GetBoolean());
    }

    [Fact]
    public void TheBadgeCounts_ShareOneFleetWideAlertRead_AndKeepTheLastCountsWhenARefreshFails()
    {
        if (!TryRun("oneRead", out var r)) return;

        Assert.Equal(1, r.GetProperty("readsWithinTtl").GetInt32());
        Assert.Equal(1, r.GetProperty("count").GetInt32());
        Assert.Equal(2, r.GetProperty("readsAfterTtl").GetInt32());
        Assert.Equal(1, r.GetProperty("countAfterFailedRead").GetInt32());
    }

    private static string[] Strings(JsonElement node) => node.EnumerateArray().Select(e => e.GetString()!).ToArray();

    /// <summary>The six awkward names a database filter must keep as six single names: a comma, a bracket, a leading space, a
    /// quote, a percent sign with a plus, and markup. None is split, trimmed, decoded or interpreted.</summary>
    internal static readonly string[] AwkwardDatabaseNames =
        { "A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>" };

    [Fact]
    public void TheDatabaseFilter_IsSavedPerServerId_AndSurvivesARebuild_WithTheAwkwardNamesWhole()
    {
        if (!TryRun("dbFilter", out var r)) return;

        Assert.Empty(r.GetProperty("none").EnumerateArray());
        Assert.True(r.GetProperty("setSix").GetBoolean());
        var perId = r.GetProperty("perId");
        Assert.Equal(AwkwardDatabaseNames, Strings(perId[0]));
        Assert.Equal(new[] { "Other" }, Strings(perId[1]));
        Assert.Empty(perId[2].EnumerateArray());
        Assert.Equal(AwkwardDatabaseNames, Strings(r.GetProperty("afterReload")));
        var stored = r.GetProperty("stored");
        Assert.Equal(1, stored.GetProperty("v").GetInt32());
        Assert.Equal(AwkwardDatabaseNames, Strings(stored.GetProperty("servers").GetProperty("3")));
        Assert.Equal(6, r.GetProperty("noCopyLeak").GetInt32());

        var cleared = r.GetProperty("afterClear");
        Assert.Empty(cleared[0].EnumerateArray());
        Assert.Equal(new[] { "Other" }, Strings(cleared[1]));
        // Exactly the names given: a duplicate goes, case and spaces stay.
        Assert.Equal(new[] { "b", "B", " b" }, Strings(r.GetProperty("exact")));
    }

    [Fact]
    public void TheDatabaseFilter_ReadsASetOverTheCapOrTheByteBudgetAsNone_NeverATruncatedSet()
    {
        if (!TryRun("dbFilter", out var r)) return;

        Assert.True(r.GetProperty("set50").GetBoolean());
        Assert.Equal(50, r.GetProperty("len50").GetInt32());
        Assert.False(r.GetProperty("set51").GetBoolean());
        Assert.Equal(50, r.GetProperty("len51Kept").GetInt32());
        Assert.Empty(r.GetProperty("stored51").EnumerateArray());

        // 60 CJK characters encode to 540 bytes, so 7 such names fit 4,096 and 50 do not.
        Assert.Equal(557, r.GetProperty("longBytes").GetInt32());
        Assert.True(r.GetProperty("fitsBytes").GetInt32() <= 4096);
        Assert.True(r.GetProperty("fitSet").GetBoolean());
        Assert.Empty(r.GetProperty("storedOverBudget").EnumerateArray());
        Assert.False(r.GetProperty("setOverBudget").GetBoolean());
        Assert.Empty(r.GetProperty("overBudgetKept").EnumerateArray()); // the refused set changed nothing
    }

    [Fact]
    public void TheDatabaseFilter_AForeignVersionOrCorruptOrHandEditedValue_ReadsAsNone_WithoutThrowing()
    {
        if (!TryRun("dbFilter", out var r)) return;

        Assert.Empty(r.GetProperty("foreignVersion").EnumerateArray());
        Assert.Empty(r.GetProperty("garbage").EnumerateArray());
        foreach (var w in r.GetProperty("wrongTypes").EnumerateArray()) Assert.Empty(w.EnumerateArray());
        foreach (var b in r.GetProperty("badId").EnumerateArray()) Assert.False(b.GetBoolean());
        // With private mode or a full quota the choice holds for the page load only.
        Assert.True(r.GetProperty("setNoStorage").GetBoolean());
        Assert.Equal(new[] { "A" }, Strings(r.GetProperty("heldNoStorage")));
    }

    [Fact]
    public void TheShell_WiresTheControlsInAtModuleScope_WithoutTouchingPanelsOrTheApi()
    {
        var app = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "app.js");
        var fleet = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "fleet.js");
        var sidebar = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "sidebar.js");
        var ui = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "viewer-local-ui.js");
        var local = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "viewer-local.js");
        var theme = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "css", "theme.css");

        Assert.Contains("favoritesFirst", sidebar);
        Assert.Contains("alertBadge(c.server_id)", sidebar);
        Assert.Contains("paintServerList(serverList,", app);
        Assert.Contains("refreshAttention(readTool)", app);
        Assert.Contains("initSidebarCollapse", app);
        Assert.Contains("favoritesFirst(SORTS[fleetSort]", fleet);
        Assert.Contains("alertBadge(c.server_id)", fleet);
        Assert.DoesNotContain("sev-", theme);
        Assert.DoesNotContain("innerHTML", ui);
        Assert.DoesNotContain("innerHTML", local);
        Assert.DoesNotContain("fetch(", local);
    }
}
