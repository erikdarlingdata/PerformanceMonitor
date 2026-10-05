/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
/// A right-click on a server-tab line chart also offers Show Active Queries / Blocking / Deadlocks at This Time (the
/// desktop's chart drill-downs). Each item sets the server's custom range to the clicked time +- 30 minutes (the
/// smallest range the picker allows) and moves the hash to that tab. These run the shipped <c>charts.js</c> under Node
/// (<c>web-chart-at-time-harness.mjs</c>), and read <c>server-tabs.js</c> for the charts that must hand the server over.
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

    [Fact]
    public void ARightClickOnAServerChart_OffersTheThreeItemsAfterTheUsualOnes()
    {
        Assert.Equal(
            new[] { "Copy Image", "Save Image As...", "Export Data to CSV...", "Show Active Queries at This Time", "Show Blocking at This Time", "Show Deadlocks at This Time" },
            Strings(Run().GetProperty("rightClickItems")));
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
        var usual = new[] { "Copy Image", "Save Image As...", "Export Data to CSV..." };
        Assert.Equal(usual, Strings(r.GetProperty("noAtTimeItems")));
        Assert.Equal(usual, Strings(r.GetProperty("buttonItems")));
    }

    [Fact]
    public void AMenuHeldOpenAcrossTheMinutePollRebuild_KeepsItsClickedTime()
    {
        var r = Run();
        Assert.Equal(6, r.GetProperty("rebuiltItems").GetArrayLength());
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
}
