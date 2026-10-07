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
/// <para>The server page's database picker (#5245, part of #5244): <c>pages/database-filter.js</c>, built on
/// <c>multi-picker.js</c> and mounted by <c>pages/server.js</c>. The real page modules run under Node
/// (<c>web-database-filter-picker-harness.mjs</c>) on a fake DOM, a recording <c>fetch</c> and a stand-in tab registry that
/// records the filter each panel batch was built under. Node is skipped when it is not installed.</para>
/// </summary>
public sealed class WebDatabaseFilterPickerBehaviourTests
{
    private static string[] Strings(JsonElement node) => node.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-database-filter-picker-harness.mjs"));
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
                Assert.Fail("the database picker harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the database picker harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void APostgreSqlServer_GetsNoPicker_AndNoFilter()
    {
        var r = Run("postgres");

        Assert.Equal(0, r.GetProperty("buttons").GetInt32());
        Assert.Equal(0, r.GetProperty("slotChildren").GetInt32());
        // A choice stored for that id is not applied to a PostgreSQL page.
        Assert.All(r.GetProperty("filters").EnumerateArray(), f => Assert.Equal(JsonValueKind.Null, f.ValueKind));
    }

    [Fact]
    public void WithNoCard_TheButtonIsDisabled_AndNoFilterIsActive()
    {
        var r = Run("unavailable");

        Assert.Equal(new[] { "Databases: unavailable" }, Strings(r.GetProperty("labels")));
        Assert.True(r.GetProperty("disabled")[0].GetBoolean());
        Assert.All(r.GetProperty("filters").EnumerateArray(), f => Assert.Equal(JsonValueKind.Null, f.ValueKind));
    }

    [Fact]
    public void Apply_StoresExactlyTheCheckedSet_RedrawsOnce_AndNeverTurnsAFullSetIntoAll()
    {
        var r = Run("applyExact");

        // The stored choice is applied to the very first panel batch.
        Assert.Equal(new[] { "db01", "db02" }, Strings(r.GetProperty("filterAtFirstBuild").GetProperty("databases")));
        Assert.True(r.GetProperty("popoverHiddenAtStart").GetBoolean());
        Assert.Equal(0, r.GetProperty("inventoryFetchedBeforeOpen").GetInt32());
        Assert.Equal("/api/server-databases?server=SRV1", r.GetProperty("inventoryUrl").GetString());
        Assert.True(r.GetProperty("popoverOpen").GetBoolean());
        Assert.Equal(0, r.GetProperty("selectAllButtons").GetInt32());
        Assert.Equal(2, r.GetProperty("checkedAtOpen").GetInt32());
        Assert.Equal(37, r.GetProperty("checkedAfterAll").GetInt32());

        // Every offered name checked: exactly those 37 are stored. It is not "All".
        Assert.Equal(37, r.GetProperty("storedCount").GetInt32());
        Assert.Equal(37, r.GetProperty("stored").GetArrayLength());
        Assert.Equal(1, r.GetProperty("redraws").GetInt32());
        Assert.Equal(37, r.GetProperty("filterAfterApply").GetProperty("databases").GetArrayLength());
        Assert.Equal("Databases: 37 of 37", r.GetProperty("labelAfterApply").GetString());
        Assert.True(r.GetProperty("popoverClosed").GetBoolean());

        // "All databases", and an Apply with nothing checked, clear the choice.
        var all = r.GetProperty("afterAll");
        Assert.Equal(JsonValueKind.Null, all.GetProperty("stored").ValueKind);
        Assert.Equal(1, all.GetProperty("redraws").GetInt32());
        Assert.Equal(JsonValueKind.Null, all.GetProperty("filter").ValueKind);
        Assert.Equal("Databases: All", all.GetProperty("label").GetString());
        var empty = r.GetProperty("afterEmpty");
        Assert.Equal(JsonValueKind.Null, empty.GetProperty("stored").ValueKind);
        Assert.Equal("Databases: All", empty.GetProperty("label").GetString());
    }

    [Fact]
    public void TheLabel_ReadsTwo_UntilTheInventoryLoads_ThenTwoOfThirtySeven()
    {
        var r = Run("applyExact");

        Assert.Equal("Databases: 2", r.GetProperty("labelBeforeLoad").GetString());
        Assert.Equal("Databases: 2 of 37", r.GetProperty("labelAfterLoad").GetString());
    }

    [Fact]
    public void AStoredNameTheInventoryNoLongerOffers_StaysCheckedAndListed_AndSurvivesApply()
    {
        var r = Run("storedMissing");

        Assert.Equal(new[] { "A", "B", "Gone" }, Strings(r.GetProperty("listed")));
        Assert.Equal(new[] { "A", "Gone" }, Strings(r.GetProperty("checked")));
        Assert.Equal(new[] { "A", "Gone" }, Strings(r.GetProperty("stored")));
    }

    [Fact]
    public void TheFiftyFirstCheck_IsRefusedWithASentence()
    {
        var r = Run("limit51");

        Assert.Equal(50, r.GetProperty("checkedBefore").GetInt32());
        Assert.False(r.GetProperty("fiftyFirstChecked").GetBoolean());
        Assert.True(r.GetProperty("fiftyFirstDisabled").GetBoolean());
        Assert.Equal(50, r.GetProperty("checkedAfter").GetInt32());
        Assert.Equal("Limit reached: uncheck a database to pick another.", r.GetProperty("hint").GetString());
        Assert.Equal(50, r.GetProperty("storedCount").GetInt32());
    }

    [Fact]
    public void ACheckPastFourThousandNinetySixEncodedBytes_IsRefusedWithASentence_For50LongNonAsciiNames()
    {
        var r = Run("byteBudget");

        // Each name is 557 encoded bytes (60 CJK characters at 9 bytes each, two digits, and the 15-byte key): 7 fit.
        Assert.Equal(7, r.GetProperty("checked").GetInt32());
        Assert.Contains("too long", r.GetProperty("hint").GetString());
        Assert.Equal(43, r.GetProperty("uncheckedLeft").GetInt32());
        Assert.Equal(43, r.GetProperty("disabledLeft").GetInt32());
        Assert.Equal(7, r.GetProperty("storedCount").GetInt32());
        Assert.True(r.GetProperty("bytes").GetInt32() <= 4096);
    }

    [Fact]
    public void TheSixAwkwardNames_AreListedAndStoredAsSixSingleNames_AndNeverAsMarkup()
    {
        var r = Run("awkward");

        Assert.Equal(ViewerLocalStateBehaviourTests.AwkwardDatabaseNames, Strings(r.GetProperty("listed")));
        Assert.Equal(0, r.GetProperty("markupNodes").GetInt32());
        Assert.Equal(ViewerLocalStateBehaviourTests.AwkwardDatabaseNames, Strings(r.GetProperty("stored")));
        Assert.Equal(ViewerLocalStateBehaviourTests.AwkwardDatabaseNames, Strings(r.GetProperty("filter").GetProperty("databases")));
        Assert.Equal("Databases: 6 of 6", r.GetProperty("label").GetString());
        Assert.Equal(ViewerLocalStateBehaviourTests.AwkwardDatabaseNames, Strings(r.GetProperty("relisted")));
        Assert.Equal(6, r.GetProperty("relistedChecked").GetInt32());
        Assert.Equal(0, r.GetProperty("markupNodesAfter").GetInt32());
    }

    [Fact]
    public void WhenTheRouteCutTheList_ThePopoverSaysSo_AndTheLabelCountsAPlus()
    {
        var r = Run("cutList");

        Assert.Equal(40, r.GetProperty("offered").GetInt32());
        Assert.Equal(
            new[] { "The list stops at 40 databases, so more are not shown. Type part of a name in the search box to find it." },
            Strings(r.GetProperty("note")));
        Assert.Equal("Databases: 1 of 40+", r.GetProperty("label").GetString());
    }

    [Fact]
    public void WhenTheRouteCutNothing_NoNoteShows_AndTheLabelKeepsThePlainCount()
    {
        var r = Run("uncutList");

        Assert.Empty(Strings(r.GetProperty("note")));
        Assert.Equal("Databases: 1 of 40", r.GetProperty("label").GetString());
    }

    [Fact]
    public void ACutLists_SearchBox_AsksTheRouteOnce_AfterAPause_AndShowsTheDatabasePastTheCap()
    {
        var r = Run("cutSearch");

        // The first page says it is cut and tells the reader to type a name; typing asks nothing until the pause is over.
        Assert.Equal(
            new[] { "The list stops at 40 databases, so more are not shown. Type part of a name in the search box to find it." },
            Strings(r.GetProperty("noteBefore")));
        Assert.Equal(0, r.GetProperty("searchUrlsBefore").GetInt32());
        Assert.Equal(0, r.GetProperty("searchUrlsDuringPause").GetInt32());

        // Three keystrokes are one ask, for the last text, and the list is that answer: a name that was never in the first page.
        Assert.Equal(new[] { "/api/server-databases?server=SRV1&search=zz-past" }, Strings(r.GetProperty("searchUrls")));
        Assert.Equal(new[] { "zz-past-the-cap" }, Strings(r.GetProperty("listedAfter")));
        Assert.Empty(Strings(r.GetProperty("noteAfter")));
        Assert.Equal("Databases: All", r.GetProperty("label").GetString());

        // The found name can be checked and applied. Clearing the box shows the first page again (and the checked name after it),
        // and asks the route for nothing more.
        Assert.Equal(41, r.GetProperty("listedCleared").GetInt32());
        Assert.Equal(1, r.GetProperty("searchUrlsAfterClear").GetInt32());
        Assert.Equal(new[] { "zz-past-the-cap" }, Strings(r.GetProperty("checkedAfterClear")));
        Assert.Equal(new[] { "zz-past-the-cap" }, Strings(r.GetProperty("stored")));
    }

    [Fact]
    public void ACutLists_Search_ShowsOnlyTheNewestAnswer_WhenAnOlderOneLandsLater()
    {
        var r = Run("cutSearchNewestWins");

        Assert.Equal(
            new[] { "/api/server-databases?server=SRV1&search=a1", "/api/server-databases?server=SRV1&search=a2" },
            Strings(r.GetProperty("searchUrls")));
        Assert.Equal(new[] { "a2-new-answer" }, Strings(r.GetProperty("listedMid")));
        Assert.Equal(new[] { "a2-new-answer" }, Strings(r.GetProperty("listedEnd")));
    }

    [Fact]
    public void ACutLists_ServerAnswer_IsListedAsSent_NotFilteredAgainByJavaScriptCaseFolding()
    {
        var r = Run("cutSearchServerFold");

        // "İstanbul" does not lowercase to text that contains "ist" in JavaScript, but the route returned it for "ist".
        Assert.Equal(new[] { "İstanbul", "istanbul2" }, Strings(r.GetProperty("listedAnswered")));
        // While the newer text's answer is in flight, the last answer is narrowed locally; the newest answer then replaces it.
        Assert.Equal(new[] { "istanbul2" }, Strings(r.GetProperty("listedInFlight")));
        Assert.Equal(new[] { "İstanbul", "istanbul2" }, Strings(r.GetProperty("listedNewest")));
    }

    [Fact]
    public void ASearchThatIsItselfCut_SaysSo()
    {
        var r = Run("cutSearchCutAndFails");

        Assert.Equal(40, r.GetProperty("offered").GetInt32());
        Assert.Equal(
            new[] { "The search stops at 40 databases, so more matches are not shown. Type more of the name to narrow it." },
            Strings(r.GetProperty("noteCut")));
    }

    [Fact]
    public void AListThatWasNotCut_FiltersLocally_AndAsksTheRouteForNothing()
    {
        var r = Run("uncutSearch");

        Assert.Empty(Strings(r.GetProperty("searchUrls")));
        Assert.Equal(
            new[] { "db10", "db11", "db12", "db13", "db14", "db15", "db16", "db17", "db18", "db19" },
            Strings(r.GetProperty("listed")));
        Assert.Empty(Strings(r.GetProperty("note")));
    }

    [Fact]
    public void ABlankByTheServicesRuleName_IsRefusedWithTheNameSentence_AndAByteOrderMarkNameIsKept()
    {
        var r = Run("blankName");

        Assert.Contains("a name the filter cannot keep", r.GetProperty("message").GetString());
        Assert.DoesNotContain("too large", r.GetProperty("message").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("stored").ValueKind);
        Assert.Equal(new[] { "1:feff" }, Strings(r.GetProperty("storedBom")));
    }

    [Fact]
    public void WhenTheInventoryCannotBeLoaded_TheSentenceShows_AndTheStoredNamesStayListed()
    {
        var r = Run("loadFails");

        Assert.StartsWith("The list of databases could not be loaded", r.GetProperty("message").GetString());
        Assert.Equal(new[] { "Kept" }, Strings(r.GetProperty("listed")));
        Assert.Equal("Databases: 1", r.GetProperty("label").GetString());
    }

    [Fact]
    public void TheSource_SetsNamesAsTextOnly_AndLeavesTheWaitPickersAlone()
    {
        var picker = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "database-filter.js");
        var multi = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "multi-picker.js");
        var server = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server.js");

        Assert.DoesNotContain("innerHTML", picker);
        Assert.DoesNotContain("insertAdjacentHTML", picker);
        Assert.Contains("selectAll: withSelectAll = true", multi);
        // The search callback is optional: a picker that passes none (the wait pickers) behaves as it did.
        Assert.Contains("onSearch = null", multi);
        Assert.Contains("if (onSearch) onSearch(state.search);", multi);
        // A picker with no `answeredFor` (the wait and perfmon pickers) filters locally, exactly as before.
        Assert.Contains("answeredFor = null", multi);
        Assert.Contains("setActiveDatabaseFilter({ server: current.server, databases })", server);
        Assert.Contains("onApply: redrawPanels", server);
        Assert.Contains("isPostgresTarget(card)", server);
    }
}
