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
using System.IO;
using System.Text.RegularExpressions;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A right-click on a server-tab line chart also offers the one item that matches the chart: Show Blocking at This Time
/// on the blocking charts, Show Deadlocks at This Time on the deadlock charts, Show Active Queries at This Time on the
/// rest (the desktop's chart drill-downs). Each item sets the server's custom range to the time of the drawn point
/// nearest the click +- 30 minutes (the smallest range the picker allows), moves the hash to that tab and, once the router
/// has built it, scrolls the item's own grid into view. These run the shipped <c>charts.js</c> under Node
/// (<c>web-chart-at-time-harness.mjs</c>), and read <c>server-tabs.js</c> for the charts that must hand the server over
/// and name their item, and for the grids the scroll looks for.
/// </summary>
public sealed class WebChartAtTimeBehaviourTests
{
    private const long ClickedMs = 1767225900000; // 2026-01-01 00:05:00 UTC, the middle of the harness chart
    private const long HalfMs = 30 * 60000;

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-at-time-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped chart script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the chart at-time harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart at-time harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[^1]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    private static JsonElement OneCall(JsonElement r, string name)
    {
        var calls = r.GetProperty(name);
        Assert.Equal(1, calls.GetArrayLength());
        return calls[0];
    }

    private static readonly string[] UsualItems = { "Copy Image", "Save Image As...", "Export Data to CSV..." };

    [Theory]
    [InlineData("rightClickItems", "Show Active Queries at This Time")]  // a chart that names no item: the default
    [InlineData("unknownItemItems", "Show Active Queries at This Time")] // an item the page does not know: the default
    [InlineData("blockingItems", "Show Blocking at This Time")]
    [InlineData("deadlockItems", "Show Deadlocks at This Time")]
    public void ARightClickOnAServerChart_OffersOnlyTheOneItemThatMatchesIt_AfterTheUsualOnes(string chart, string item)
    {
        // The desktop's chart drill-downs give each chart the one item that matches what it plots, not all three on every chart.
        Assert.Equal(UsualItems.Append(item).ToArray(), Strings(Run().GetProperty(chart)));
    }

    [Theory]
    [InlineData("queries", "#/server/A/cpu", "#/server/A/queries")]
    [InlineData("blocking", "#/server/A/queries", "#/server/A/blocking")]
    [InlineData("deadlocks", "#/server/A/blocking", "#/server/A/blocking")]
    public void EachItem_AppliesTheClickedTimePlusOrMinusThirtyMinutes_ThenMovesToItsTab(string item, string hashBefore, string hashAfter)
    {
        var r = Run();
        var call = OneCall(r, item);
        Assert.Equal("A", call.GetProperty("server").GetString());
        Assert.Equal(ClickedMs - HalfMs, call.GetProperty("startMs").GetInt64());
        Assert.Equal(ClickedMs + HalfMs, call.GetProperty("endMs").GetInt64());
        // The range is applied before the hash moves, so the tab is built under the new range.
        Assert.Equal(hashBefore, call.GetProperty("hash").GetString());
        if (item == "deadlocks") Assert.Equal(hashAfter, r.GetProperty("hashAfter").GetString());
    }

    [Fact]
    public void EachItem_SetsTheRangeWithoutAPanelRedraw_AndRebuildsThroughTheRouter()
    {
        // The range picker sits in the page head, which only a full render rebuilds. A panel redraw would leave it on the old
        // preset (the tab already open) or start reads the route change then aborts (another tab), so no item asks for one.
        var r = Run();
        foreach (var item in new[] { "queries", "blocking", "deadlocks" })
            Assert.False(OneCall(r, item).GetProperty("redraw").GetBoolean(), item);
        // Another tab: the hash move fires the router's own hashchange, so no event is raised for it.
        Assert.Equal(0, r.GetProperty("dispatchedOnTabChange").GetInt32());
        // The tab already open: setting the same hash fires nothing, so the event is raised here, once.
        Assert.Equal(1, r.GetProperty("dispatchedOnSameTab").GetInt32());
    }

    [Fact]
    public void ARangeThePageRefuses_IsSaidOnTheChart_AndTheTabDoesNotMove()
    {
        var r = Run();
        Assert.Equal("#/server/A/cpu", r.GetProperty("refusedHash").GetString());
        Assert.Equal("The end cannot be in the future.", r.GetProperty("refusedStatus").GetString());
    }

    [Theory]
    [InlineData("scrollQueries", 0, "Active Queries")]
    [InlineData("scrollBlocking", 2, "Blocking")]
    [InlineData("scrollDeadlocks", 3, "Deadlocks")]
    public void EachItem_BringsItsOwnGridIntoView_OnceTheRouterHasBuiltTheTab(string scroll, int index, string title)
    {
        // The Blocking tab holds a Deadlocks chart (panel 1) and, further down, the Deadlocks grid (panel 3). The desktop's
        // Deadlocks item opens its Deadlocks sub-tab, so the item must reach the grid, not the chart that shares its title;
        // the Blocking item reaches the Blocking grid (2), not the Blocking Events chart (0).
        var call = OneCall(Run(), scroll);
        Assert.Equal(title, call.GetProperty("title").GetString());
        Assert.Equal(index, call.GetProperty("index").GetInt32());
        Assert.Equal("start", call.GetProperty("block").GetString());
    }

    [Fact]
    public void TheScrollListener_RunsOnce_AndIsAddedOnlyOnceTheRangeIsTaken()
    {
        var r = Run();
        // Three items ran, so a listener that outlived its hashchange would be left here (and scroll on every later tab click).
        Assert.Equal(0, r.GetProperty("listenersLeft").GetInt32());
        // A range the page refuses opens no tab, so nothing may be left waiting for one either.
        Assert.Equal(0, r.GetProperty("refusedListeners").GetInt32());
    }

    [Fact]
    public void TheGridEachItemScrollsTo_IsAPanelOfTheTabItOpens_AndTheDeadlocksGridSitsBelowItsChart()
    {
        var js = new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" };
        var tabs = File.ReadAllText(PathTo(js.Concat(new[] { "pages", "server-tabs.js" }).ToArray()));
        var charts = File.ReadAllText(PathTo(js.Concat(new[] { "charts.js" }).ToArray()));

        // A tab's source: from its `id: "<id>",` line to the next tab's. The first match is the SQL Server registry's.
        string Tab(string id)
        {
            var open = Regex.Match(tabs, @"\n  \{\r?\n    id: """ + id + @""",");
            Assert.True(open.Success, id);
            var next = Regex.Match(tabs[(open.Index + open.Length)..], @"\n  \{\r?\n    id: """);
            return next.Success ? tabs.Substring(open.Index, open.Length + next.Index) : tabs[open.Index..];
        }

        // The panel titles charts.js names are the grids the tabs really have: a rename would leave the scroll finding nothing.
        Assert.Contains("panel: \"Active Queries\"", charts);
        Assert.Matches(@"table\(\s*""Active Queries"",", Tab("queries"));
        var blocking = Tab("blocking");
        Assert.Contains("panel: \"Blocking\"", charts);
        Assert.Matches(@"table\(\s*""Blocking"",", blocking);
        Assert.Contains("panel: \"Deadlocks\"", charts);
        // Both the chart and the grid are titled "Deadlocks", and the scroll takes the later one, so the grid must be the later.
        var chart = blocking.IndexOf("line(\"Deadlocks\",", StringComparison.Ordinal);
        var grid = Regex.Match(blocking, @"table\(\s*""Deadlocks"",");
        Assert.True(chart >= 0 && grid.Success && grid.Index > chart, "the Deadlocks grid must come after the Deadlocks chart on the Blocking tab");
    }

    [Fact]
    public void TheShippedPicker_TakesTheTimePlusOrMinusThirtyMinutes_AsItsSmallestRange()
    {
        var r = Run();
        Assert.True(r.GetProperty("shippedRangeTaken").GetBoolean(), r.GetProperty("shippedRangeError").ToString());
    }

    [Fact]
    public void AClickPastTheEdgesOfThePlot_UsesTheChartsOwnFirstAndLastTime()
    {
        var r = Run();
        Assert.Equal(1767225600000, r.GetProperty("clampLeft").GetInt64());  // 00:00
        Assert.Equal(1767226200000, r.GetProperty("clampRight").GetInt64()); // 00:10
    }

    [Fact]
    public void AClickBesideADrawnPoint_UsesThatPointsTime_NotTheRawPointerTime()
    {
        // Two points only, at 00:00 and 00:10. The hover tooltip snaps to the nearer one, and so does the menu, so a click a
        // pixel or two beside a one-bucket spike on a wide chart still opens the hour that holds the spike.
        var r = Run();
        Assert.Equal(1767225600000, r.GetProperty("sparseNearLeft").GetInt64());  // 00:00, drawn at the left edge
        Assert.Equal(1767226200000, r.GetProperty("sparseNearRight").GetInt64()); // 00:10, drawn at the right edge
        // The exact middle is equidistant from both: either point, never the 00:05 that no point holds.
        Assert.Contains(r.GetProperty("sparseMiddle").GetInt64(), new[] { 1767225600000, 1767226200000 });
    }

    [Fact]
    public void AClickInTheLastHalfHour_ShiftsTheHourToEndNow_SoThePageTakesIt()
    {
        var call = Run().GetProperty("nearNow");
        var now = 1767225600000 + 7 * 60000;
        Assert.Equal(now, call.GetProperty("endMs").GetInt64());
        Assert.Equal(now - 2 * HalfMs, call.GetProperty("startMs").GetInt64());
    }

    [Fact]
    public void TheServerNameIsEncodedInTheHash()
    {
        Assert.Equal("#/server/Prod%2FOne%20%231/queries", Run().GetProperty("encodedHash").GetString());
    }

    [Fact]
    public void AChartWithoutAtTime_AndAMenuOpenedFromTheButton_ShowNoneOfTheItems()
    {
        var r = Run();
        Assert.Equal(UsualItems, Strings(r.GetProperty("noAtTimeItems")));
        Assert.Equal(UsualItems, Strings(r.GetProperty("buttonItems")));
    }

    [Fact]
    public void ARightClickOffThePlot_OffersNoTimeItems()
    {
        // The listener sits on the whole chart box, but only the drawing names a time. The legend, the status line, the open
        // menu and the button (Shift+F10 on it raises a contextmenu with the button as the target) would map their x through
        // the plot and open an hour the user never pointed at.
        var r = Run();
        Assert.Equal(UsualItems, Strings(r.GetProperty("offPlotLegendItems")));
        Assert.Equal(UsualItems, Strings(r.GetProperty("offPlotStatusItems")));
        Assert.Equal(UsualItems, Strings(r.GetProperty("offPlotButtonItems")));
        // The menu from a click on the plot held the usual three and its one at-this-time item before it was right-clicked itself.
        Assert.Equal(UsualItems.Length + 1, r.GetProperty("offPlotMenuBefore").GetInt32());
        Assert.Equal(UsualItems, Strings(r.GetProperty("offPlotMenuItems")));
    }

    [Fact]
    public void AMenuHeldOpenAcrossTheMinutePollRebuild_KeepsItsClickedTime()
    {
        var r = Run();
        Assert.Equal(UsualItems.Length + 1, r.GetProperty("rebuiltItems").GetArrayLength());
        Assert.Equal(ClickedMs, r.GetProperty("rebuiltMiddle").GetInt64());
    }

    [Fact]
    public void EveryServerTabLineChartHandsItsServerOver_AndNoFinOpsOrCustomViewChartDoes()
    {
        var tabs = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));
        var calls = Regex.Matches(tabs, @"zoomableLineChart\(\{");
        Assert.Equal(9, calls.Count);
        var handed = Regex.Matches(tabs, @"zoomableLineChart\(\{\s*atTime: \{ server \},");
        Assert.Equal(calls.Count, handed.Count);

        var panels = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"));
        Assert.Contains("atTime: desc.atTime || null,", panels);

        foreach (var other in new[] { new[] { "pages", "finops", "version-store.js" }, new[] { "compose.js" }, new[] { "pages", "finops", "utilization.js" } })
        {
            var text = File.ReadAllText(PathTo(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js" }.Concat(other).ToArray()));
            Assert.DoesNotContain("atTime", text);
        }
    }

    /// <summary>
    /// The bracketed span that opens at <paramref name="open"/> ("(" or "{"), through its matching close. A bracket inside a
    /// "double-quoted" string (an empty-state sentence) does not count.
    /// </summary>
    private static string Balanced(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '"')
            {
                for (i++; text[i] != '"'; i++)
                {
                    if (text[i] == '\\') i++;
                }
            }
            else if (c is '(' or '{')
            {
                depth++;
            }
            else if (c is ')' or '}')
            {
                depth--;
                if (depth == 0) return text[open..(i + 1)];
            }
        }

        throw new InvalidOperationException("no closing bracket after index " + open);
    }

    [Fact]
    public void LineAndFanout_HandTheServerAndTheChartsItemOver_UnlessAPanelOptsOut()
    {
        // Most server-tab charts reach the menu through these two: line() (CPU, Memory, Blocking Events, Deadlocks, Lock Waits,
        // tempdb) and fanout() (Current Waits, Blocking and Deadlock Severity, Query Duration, System Health). The nine direct
        // zoomableLineChart calls are counted above; without this, deleting either hand-off passes every other case.
        var tabs = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));
        Assert.Single(Regex.Matches(tabs, @"atTime: opts\.atTime === false \|\| !params \|\| !params\.server \? null : \{ server: params\.server, item: opts\.atTimeItem \|\| ""queries"" \}"));
        Assert.Single(Regex.Matches(tabs, @"atTime: spec\.atTime === false \|\| !params \|\| !params\.server \? null : \{ server: params\.server, item: spec\.atTimeItem \|\| ""queries"" \}"));
    }

    [Fact]
    public void EveryLineOnThePostgresRegistry_OptsOut_BecauseThatPageHasNoQueriesOrBlockingTab()
    {
        // The items open the SQL Server Queries and Blocking tabs. The PostgreSQL page has neither, so its one line chart (the
        // Amazon Aurora CPU panel) passes atTime: false, and no PostgreSQL fanout draws a line.
        var tabs = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));
        var pgStart = tabs.IndexOf("export const POSTGRES_TABS = [", StringComparison.Ordinal);
        Assert.True(pgStart > 0, "the PostgreSQL tab registry moved");
        var pg = tabs[pgStart..tabs.IndexOf("\n];", pgStart, StringComparison.Ordinal)];
        var calls = Regex.Matches(pg, @"(?<![\w.])line\(");
        Assert.True(calls.Count > 0, "the PostgreSQL registry draws no line() chart any more, so this pin has nothing to check");
        foreach (Match call in calls)
            Assert.Contains("atTime: false,", Balanced(pg, call.Index + "line".Length));
        Assert.DoesNotContain("viz: \"line\"", pg);
    }

    [Fact]
    public void TheBlockingAndDeadlockCharts_NameTheirOwnItem_AndNoOtherChartDoes()
    {
        // The desktop gives Blocking Events, Lock Waits and the blocking severity chart "Show Blocking at This Time", and the
        // deadlock charts "Show Deadlocks at This Time". Every other server-tab line chart keeps the Active Queries default.
        var tabs = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

        // line() panels: Blocking Events and Deadlocks sit on both the Overview tab and the Blocking tab.
        foreach (var (title, item, count) in new[] { ("Blocking Events", "blocking", 2), ("Deadlocks", "deadlocks", 2), ("Lock Waits", "blocking", 1) })
        {
            var calls = Regex.Matches(tabs, @"line\(""" + title + @""",");
            Assert.Equal(count, calls.Count);
            foreach (Match call in calls)
                Assert.Contains($"atTimeItem: \"{item}\",", Balanced(tabs, call.Index + "line".Length));
        }

        // fanout() line specs: one read draws two charts, so each spec names its own item.
        foreach (var (title, item) in new[] { ("Blocking Severity", "blocking"), ("Deadlock Severity", "deadlocks") })
        {
            var at = tabs.IndexOf($"title: \"{title}\",", StringComparison.Ordinal);
            Assert.True(at > 0, title);
            Assert.Contains($"atTimeItem: \"{item}\",", Balanced(tabs, tabs.LastIndexOf('{', at)));
        }

        // No other chart names one: 4 blocking charts and 3 deadlock charts, all counted above.
        Assert.Equal(4, Regex.Matches(tabs, @"atTimeItem: ""blocking""").Count);
        Assert.Equal(3, Regex.Matches(tabs, @"atTimeItem: ""deadlocks""").Count);
    }
}
