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
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Release click-through findings 13 and 15 and the rest of 4 (part D), run under Node
/// (<c>web-clickthrough-pick-harness.mjs</c>): every server pick list on the web uses the sidebar's order (display name,
/// favourites first), the Deadlocks tile says in a tooltip why it read fewer servers than the fleet has, a stat panel can
/// drop the tiles that have no value, and the collector counts written with underscores read as words. Node is skipped
/// when it is not installed.
/// </summary>
public sealed class WebClickthroughPickListTests
{
    private static JsonElement Run(string scenario, string? input = null)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-clickthrough-pick-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        if (input is not null)
        {
            psi.Environment["HARNESS_INPUT"] = input;
        }

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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the pick list harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the pick list harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheSharedOrder_IsDisplayNameWithFavouritesFirst()
    {
        var r = Run("order");
        string[] byName = ["AG1", "AG2", "PG18", "PGEXT", "SQL2016"];
        Assert.Equal(byName, Strings(r.GetProperty("plain")));
        Assert.Equal(byName, Strings(r.GetProperty("options")));
        Assert.Equal(byName, Strings(r.GetProperty("fromCards")));
        Assert.Equal(["PGEXT", "AG1", "AG2", "PG18", "SQL2016"], Strings(r.GetProperty("withFavorite")));
        Assert.True(r.GetProperty("sidebarSameFunction").GetBoolean());
    }

    [Fact]
    public void TheFinOpsAlertHistoryAndJobHistoryPickers_FollowTheSidebarOrder()
    {
        var r = Run("pages");
        // The registry (and list_servers) answers PG18 first; the sidebar lists AG1, AG2, PG18, PGEXT, SQL2016.
        Assert.Equal(["AG1", "AG2", "PG18 (PostgreSQL)", "PGEXT (PostgreSQL)", "SQL2016"], Strings(r.GetProperty("finops")));
        Assert.Equal(["All servers", "AG1", "AG2", "SQL2016"], Strings(r.GetProperty("jobs")));
        var alerts = r.GetProperty("alerts").EnumerateArray().Select(Strings).Single(s => s.Contains("All servers"));
        Assert.Equal(["All servers", "AG1", "AG2", "PG18", "PGEXT", "SQL2016"], alerts);
    }

    [Fact]
    public void TheScopeAndParamPickers_PutFavouritesFirst()
    {
        var r = Run("fleetOptions");
        Assert.Equal(["AG1", "AG2", "PG18", "PGEXT", "SQL2016"], Strings(r.GetProperty("plain")));
        Assert.Equal(["SQL2016", "AG1", "AG2", "PG18", "PGEXT"], Strings(r.GetProperty("withFavorite")));
    }

    [Fact]
    public void EveryHandBuiltServerList_GoesThroughTheSharedOrder()
    {
        foreach (var file in new[] { "js/pages/alerts.js", "js/pages/job-history.js", "js/pages/finops.js", "js/pages/views.js", "js/editor.js" })
        {
            var text = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", file));
            Assert.Contains("orderServers(", text);
            Assert.DoesNotContain("a.label.localeCompare(b.label)", text);
        }
    }

    [Fact]
    public void ThePartialDeadlockLine_SaysWhyInATooltip_AndAFullOneNeedsNone()
    {
        var r = Run("deadlocks");
        Assert.Equal("read 6 of 9 servers", r.GetProperty("text").GetString());
        var title = r.GetProperty("title").GetString()!;
        Assert.Contains("The other 3 servers are not counted", title);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("fullTitle").ValueKind);
        var sub = r.GetProperty("subNodes").EnumerateArray().Single();
        Assert.Equal(title, sub.GetProperty("title").GetString());
    }

    [Fact]
    public void AStatTileWithNoValue_CanBeHidden_ButAKeptTileAndOneWithItsOwnSentenceStay()
    {
        var r = Run("stats");
        Assert.Equal(["Count", "Plain dash", "Has why"], Strings(r.GetProperty("labels")));
    }

    [Fact]
    public void TheAlertingReadsPanel_MarksItsWhichRanForAndNewestTilesHideWhenEmpty()
    {
        var text = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));
        var start = text.IndexOf("const ALERT_READ_STATS = [", StringComparison.Ordinal);
        var block = text[start..text.IndexOf("];", start, StringComparison.Ordinal)];
        var tiles = block.Split('\n').Where(l => l.TrimStart().StartsWith("{ key: \"alert_read_health.", StringComparison.Ordinal)).ToArray();
        Assert.Equal(9, tiles.Count(l => l.Contains("hideWhenEmpty: true", StringComparison.Ordinal)));
        // The counts stay: a zero is a value, and a count tile is never dropped.
        Assert.All(tiles.Where(l => l.Contains("format: \"int\"", StringComparison.Ordinal)), l => Assert.DoesNotContain("hideWhenEmpty", l));
    }

    [Fact]
    public void ThePageHeader_SaysItMonitorsSqlServerAndPostgreSql()
    {
        var html = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "index.html"));
        Assert.Contains("SQL Server and PostgreSQL monitor", html);
        Assert.DoesNotContain("SQL Server fleet monitor", html);
    }

    [Fact]
    public void APanelThatSaysHideWhenNotCollected_IsNotDrawnOnANotCollectedAnswer_AndOneThatDoesNotSayItIs()
    {
        var r = Run("panelHide");
        Assert.True(r.GetProperty("hidingHidden").GetBoolean());
        Assert.False(r.GetProperty("keepingHidden").GetBoolean());
    }

    [Fact]
    public void AColumnMarkedPlain_ShowsPlainText_AndAnUnmarkedOneShowsTheRawText()
    {
        var r = Run("plainCell");
        Assert.Contains("widen the time range", Strings(r.GetProperty("plain")).Single());
        Assert.DoesNotContain("hours_back", Strings(r.GetProperty("plain")).Single());
        Assert.Contains("hours_back", Strings(r.GetProperty("raw")).Single());
    }

    [Fact]
    public void ANamedColumn_PutsOnlyTheNamedTokensIntoWords_AndLeavesAnErrorsOwnTextAlone()
    {
        var r = Run("plainCell");
        Assert.Equal(
            "Could not find stored procedure 'dbo.get_orders' (timeout=30, sales_2024); widen the time range or call get_collection_health.",
            Strings(r.GetProperty("named")).Single());
        Assert.Contains("call Collection Health", Strings(r.GetProperty("full")).Single());
    }

    [Fact]
    public void TheCollectorGridsMessageColumns_ArePlain_AndTheQueryStorePageTextNamesNoTool()
    {
        var tabs = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));
        foreach (var key in new[] { "note_summary", "output_finding", "regression_finding" })
        {
            Assert.Matches("key: \"" + key + "\"[^}]*plain: true", tabs);
        }

        /* Round-1 M4: a Last Error is a real error message that may echo user data, so only the named-token rules apply. */
        Assert.Matches("key: \"last_error\"[^}]*plain: \"named\"", tabs);

        Assert.DoesNotContain("which is what get_query_trend", tabs, StringComparison.Ordinal);
        Assert.DoesNotContain("no query_store_health capture", tabs, StringComparison.Ordinal);
    }

    [Fact]
    public void AGridThatNamesOrderRows_OpensInThatOrder_AndWorstFirstRanksFailingBeforeHealthyBeforeNotApplicable()
    {
        var r = Run("gridOrder");
        Assert.Equal(new[] { "c_fail", "d_warn", "a_ok", "b_ok", "a_gated" }, Strings(r.GetProperty("worst")));
        Assert.Equal(new[] { "2026-10-03", "2026-10-02", "2026-10-01" }, Strings(r.GetProperty("newest")));
        Assert.Equal(Strings(r.GetProperty("worst")), Strings(r.GetProperty("drawn")));
        Assert.Equal(new[] { "b_ok", "a_gated", "c_fail", "a_ok", "d_warn" }, Strings(r.GetProperty("drawnPlain")));
    }

    [Fact]
    public void TheTabs_HideInstanceCpuOnANotCollectedAnswer_ListCollectorsWorstFirst_AndTheCalendarNewestFirst()
    {
        var r = Run("tabs");
        Assert.Equal(new[] { true }, r.GetProperty("cpuPanels").EnumerateArray().Select(x => x.GetBoolean()).ToArray());
        Assert.Equal(new[] { "c_fail", "a_ok", "b_ok" }, Strings(r.GetProperty("pgCollectors")));
        Assert.Equal(new[] { "c_fail", "a_ok", "b_ok" }, Strings(r.GetProperty("sqlCollectors")));
        Assert.Equal(new[] { "2026-10-03", "2026-10-02", "2026-10-01" }, Strings(r.GetProperty("calendar")));
    }

    [Fact]
    public void CollectorCounts_WrittenWithUnderscores_ReadAsWords_AndLoneNamesAreKept()
    {
        string[] inputs =
        [
            "latest run: shred_gated_1 events_read_0 report_xml_empty_0",
            "shred_gated=1 events_read=0",
            "see pg_stat_statements_2 for the plan",
            "Slot wal_sender_1 is behind",
        ];
        var r = Run("plain", System.Text.Json.JsonSerializer.Serialize(inputs));
        var o = Strings(r.GetProperty("out"));
        Assert.Equal("latest run: shred gated: 1, events read: 0, report xml empty: 0", o[0]);
        Assert.Equal("shred gated: 1, events read: 0", o[1]);
        Assert.Equal(inputs[2], o[2]);
        Assert.Equal(inputs[3], o[3]);
    }
}
