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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web Query Store history panel (#5234), from the shipped <c>query-store-history.js</c> run under Node
/// (<c>web-qs-history-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class QueryStoreHistoryBehaviourTests
{
    internal static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-qs-history-harness.mjs"));
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
                Assert.Fail("the Query Store history harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the Query Store history harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    [Fact]
    public void TheHistoryButton_ReadsThePerPlanHistory_AndEachPlanRowReadsItsOwnPlan()
    {
        var r = Run("open");
        Assert.True(r.GetProperty("buttonBefore").GetBoolean());
        Assert.Contains("Loading", Str(r, "loading"));
        Assert.Equal("/api/read/get_query_store_query_history", Str(r, "path"));
        var q = r.GetProperty("query");
        Assert.Equal("srv-a", q.GetProperty("server").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Equal("42", q.GetProperty("query_id").GetString());
        Assert.Equal("24", q.GetProperty("hours").GetString());
        Assert.False(q.TryGetProperty("hours_back", out _));
        Assert.False(q.TryGetProperty("as_of", out _));
        Assert.Equal(1, r.GetProperty("fetches").GetInt32());
        Assert.Equal(2, r.GetProperty("dataRows").GetInt32());
        Assert.Equal(2, r.GetProperty("planButtons").GetInt32());
        var plan = r.GetProperty("plan");
        Assert.Equal("/api/read/get_query_store_plan_xml", Str(plan, "path"));
        Assert.Equal("9", plan.GetProperty("query").GetProperty("plan_id").GetString());
        Assert.Equal("42", plan.GetProperty("query").GetProperty("query_id").GetString());
        var chart = r.GetProperty("chart")[0];
        Assert.Equal("qs-history|Orders|42", Str(chart, "id"));
        Assert.Equal(new[] { "p7", "p9" }, chart.GetProperty("series").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(2, chart.GetProperty("points").GetInt32());
        Assert.True(r.GetProperty("closed").GetBoolean());
    }

    [Fact]
    public void AnOpenPanel_SurvivesARebuild_WithoutASecondRead()
    {
        var r = Run("rebuild");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsTable").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
    }

    [Fact]
    public void WithACustomRange_TheReadIsAnchoredAtTheRangeEnd()
    {
        var r = Run("customRange");
        var q = r.GetProperty("query");
        Assert.Equal("24", q.GetProperty("hours").GetString());
        Assert.Equal("2026-03-04T06:00:00.000Z", q.GetProperty("as_of").GetString());
    }

    [Fact]
    public void AChangedHoursOrRange_ReadsTheOpenPanelAgain_AndTheSameWindowDoesNot()
    {
        var r = Run("windowChange");
        Assert.Equal(0, r.GetProperty("sameWindowRefetched").GetInt32());
        Assert.Equal(1, r.GetProperty("hoursRefetched").GetInt32());
        Assert.Equal("168", r.GetProperty("lastHours").GetString());
        Assert.True(r.GetProperty("stillOpen").GetBoolean());
        Assert.Equal(1, r.GetProperty("rangeRefetched").GetInt32());
        Assert.Equal("2026-03-04T06:00:00.000Z", r.GetProperty("lastAsOf").GetString());
    }

    [Fact]
    public void ACellThePageThrewAway_IsNotDrawnInto_AndLeavesTheRedrawSet()
    {
        var r = Run("detachedCell");
        Assert.Equal(0, r.GetProperty("goneTables").GetInt32());
        Assert.Equal(1, r.GetProperty("keptTables").GetInt32());
        Assert.Equal(1, r.GetProperty("cells").GetInt32());
    }

    [Fact]
    public void WhenTheLastCellForAKeyIsGone_TheKeyLeavesTheRedrawMap()
    {
        var r = Run("lastCellGone");
        Assert.Equal(0, r.GetProperty("goneTables").GetInt32());
        Assert.Empty(r.GetProperty("keys").EnumerateArray());
    }

    [Fact]
    public void ACutPointList_SaysTheNewestAreShown()
    {
        var notices = Run("cutNotice").GetProperty("notices");
        Assert.Single(notices.EnumerateArray());
        Assert.Contains("newest 2", notices[0].GetString());
        Assert.DoesNotContain("first", notices[0].GetString());
    }

    [Fact]
    public void ATruncatedWindow_ShowsTheNotice_AndAFullOneDoesNot()
    {
        var shown = Run("truncated").GetProperty("notices");
        Assert.Single(shown.EnumerateArray());
        Assert.Contains("Stored history starts at", shown[0].GetString());
        Assert.Equal(0, Run("notTruncated").GetProperty("notices").GetInt32());
    }

    [Fact]
    public void AnEmptyAnswer_ShowsItsMessage_AndNoTable()
    {
        var r = Run("empty");
        Assert.Contains("No Query Store history", Str(r, "text"));
        Assert.Equal(0, r.GetProperty("tables").GetInt32());
    }

    [Fact]
    public void AnEmptyAnswer_ShowsTheWindowNoteItCarries_AboveItsMessage()
    {
        var r = Run("emptyWithHints");
        Assert.Single(r.GetProperty("notices").EnumerateArray());
        Assert.Contains("further back than the store holds", r.GetProperty("notices")[0].GetString());
        Assert.Contains("No Query Store history", r.GetProperty("empties")[0].GetString());
        Assert.Equal(0, r.GetProperty("tables").GetInt32());
    }

    [Fact]
    public void AnEmptyAnswer_WhoseWindowWasNotCut_ShowsNoNote()
    {
        var r = Run("emptyWithUncutHints");
        Assert.Equal(0, r.GetProperty("notices").GetInt32());
        Assert.Contains("No Query Store history", Str(r, "text"));
    }

    [Fact]
    public void ARowWithoutAKey_GetsADash()
    {
        var r = Run("noKey");
        Assert.Equal("\u2014", Str(r, "noDb"));
        Assert.Equal("\u2014", Str(r, "noId"));
    }

    [Fact]
    public void TheColumn_IsNotSortedFilteredExportedOrCopied()
    {
        var r = Run("flags");
        Assert.Equal("query_store_history", Str(r, "key"));
        foreach (var f in new[] { "sortable", "filter", "csv", "copy" }) Assert.False(r.GetProperty(f).GetBoolean(), f);
    }
}
