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
/// The web stored-plan viewer (#4843), from the shipped <c>plan-viewer.js</c> run under Node
/// (<c>web-plan-viewer-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class PlanViewerBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plan-viewer-harness.mjs"));
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
                Assert.Fail("the plan viewer harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the plan viewer harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    [Fact]
    public void ThePlanButton_ReadsTheStoredPlan_AndShowsItIndented()
    {
        var r = Run("open");
        Assert.True(r.GetProperty("buttonBefore").GetBoolean());
        Assert.Equal("/api/read/get_plan_xml", Str(r, "path"));
        var q = r.GetProperty("query");
        Assert.Equal("srv-a", q.GetProperty("server").GetString());
        Assert.Equal("0xABCDEF0123456789", q.GetProperty("query_hash").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Contains("Loading", Str(r, "loading"));
        Assert.Equal("pre", Str(r, "preTag"));
        Assert.StartsWith("<ShowPlanXML", Str(r, "pre"));
        Assert.Contains("\n  <BatchSequence>\n    <Batch>", Str(r, "pre"));
        Assert.True(r.GetProperty("hasCopy").GetBoolean());
        Assert.True(r.GetProperty("hasDownload").GetBoolean());
        Assert.Equal("\u2014", Str(r, "noHashCell"));
        Assert.True(r.GetProperty("closed").GetBoolean());
    }

    [Fact]
    public void ThePrettyPrint_IsText_NotMarkup()
    {
        var r = Run("textOnly");
        Assert.Equal(0, r.GetProperty("preChildren").GetInt32());
        Assert.Equal(0, r.GetProperty("scriptElements").GetInt32());
        // The entity-escaped script text in the statement attribute stays escaped text, character for character.
        Assert.Contains("&lt;script&gt;", Str(r, "text"));
        Assert.True(r.GetProperty("lines").GetInt32() > 5);
        Assert.Equal("<a>\n  <b x=\"1>2\">t</b>\n  <c/>\n  <d>\n    <e/>\n  </d>\n</a>", Str(r, "pretty"));
        Assert.Equal("not xml <at all", Str(r, "notXml"));
    }

    [Fact]
    public void Download_IsAnXmlBlob_NamedForTheQueryHash_HoldingTheStoredXml()
    {
        var r = Run("download");
        Assert.Equal("0xABCDEF0123456789.sqlplan", Str(r, "name"));
        Assert.Equal("application/xml", Str(r, "type"));
        Assert.True(r.GetProperty("bodyIsStored").GetBoolean());
        Assert.Equal("a_b_c.sqlplan", Str(r, "safeName"));
    }

    [Fact]
    public void Copy_PutsTheShownTextOnTheClipboard()
    {
        var r = Run("copy");
        Assert.True(r.GetProperty("matchesShown").GetBoolean());
        Assert.Contains("\n  <BatchSequence>", Str(r, "copied"));
    }

    [Fact]
    public void ATruncatedPlan_SaysSo_AndTurnsDownloadOff()
    {
        var r = Run("truncated");
        Assert.True(r.GetProperty("disabled").GetBoolean());
        Assert.Equal(0, r.GetProperty("downloads").GetInt32());
        Assert.Contains("500 KB", Str(r, "notice"));
        Assert.Contains("Download is turned off", Str(r, "notice"));
        Assert.False(r.GetProperty("shown").GetBoolean());
        Assert.True(r.GetProperty("copyOn").GetBoolean());
    }

    [Fact]
    public void AQueryWithNoStoredPlan_SaysSo_WithNoDownload()
    {
        var r = Run("noPlan");
        Assert.Contains("No stored plan found", Str(r, "text"));
        Assert.False(r.GetProperty("hasPre").GetBoolean());
        Assert.False(r.GetProperty("hasDownload").GetBoolean());
    }

    [Fact]
    public void AnOpenPanel_SurvivesTheRebuild_WithoutAnotherRead()
    {
        var r = Run("rebuild");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsPlan").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
        Assert.False(r.GetProperty("otherServerOpen").GetBoolean());
        Assert.True(r.GetProperty("loadingInNew").GetBoolean());
        Assert.True(r.GetProperty("filledNew").GetBoolean());
    }

    private static string[] Strs(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void EachRowKind_ReadsItsOwnTool_WithTheRowsKeyExactlyAsHeld()
    {
        var r = Run("kinds");

        Assert.Equal(new[] { "has_query_plan", "has_live_query_plan" }, Strs(r, "activeKeys"));
        Assert.Equal(new[] { "Plan", "Live plan" }, Strs(r, "activeLabels"));
        Assert.True(r.GetProperty("activeHide").GetBoolean());
        Assert.True(r.GetProperty("planColsNoFilter").GetBoolean());
        Assert.True(r.GetProperty("planColsNoFilterNoCopy").GetBoolean(), "every plan column, planColumn included, sets filter, copy and csv to false: an open panel's XML is the cell's text");

        Assert.Equal("/api/read/get_active_query_plan_xml", Str(r, "activePath"));
        var est = r.GetProperty("activeQuery");
        Assert.Equal("srv-a", Str(est, "server"));
        /* The timestamp is the row's own string, all seven fractional digits, untouched. */
        Assert.Equal("2026-03-04T05:06:07.1234560", Str(est, "collection_time"));
        Assert.Equal("57", Str(est, "session_id"));
        Assert.Equal("3", Str(est, "request_id"));
        Assert.False(est.TryGetProperty("live", out _));

        var live = r.GetProperty("liveQuery");
        Assert.Equal("true", Str(live, "live"));
        Assert.Equal("0", Str(live, "request_id"));       /* a row with no request_id reads as 0 */
        Assert.Equal("\u2014", Str(r, "noFlagCell"));
        Assert.Equal("\u2014", Str(r, "noLiveFlagCell"));

        Assert.Equal("query_id", Str(r, "qsKey"));
        Assert.Equal("/api/read/get_query_store_plan_xml", Str(r, "qsPath"));
        var qs = r.GetProperty("qsQuery");
        Assert.Equal("Orders", Str(qs, "database_name"));
        Assert.Equal("42", Str(qs, "query_id"));
        Assert.Equal("7", Str(qs, "plan_id"));
        Assert.False(r.GetProperty("qsNoPlanIdQuery").TryGetProperty("plan_id", out _));
        Assert.Equal("\u2014", Str(r, "qsNoKey"));

        Assert.Equal("sql_handle", Str(r, "procKey"));
        Assert.True(r.GetProperty("procHide").GetBoolean());
        Assert.Equal("/api/read/get_procedure_plan_xml", Str(r, "procPath"));
        Assert.Equal("0x0300050011223344", Str(r.GetProperty("procQuery"), "sql_handle"));
        Assert.Equal("\u2014", Str(r, "procNoHandle"));

        Assert.True(r.GetProperty("keysUnique").GetBoolean());
    }

    [Fact]
    public void TwoSources_NeverShareAPanel_AndTheQueryHashKeyKeepsItsOldShape()
    {
        var r = Run("kindsDoNotShareAPanel");
        Assert.True(r.GetProperty("aOpen").GetBoolean());
        Assert.False(r.GetProperty("bOpenAfterA").GetBoolean());
        Assert.False(r.GetProperty("liveOpenAfterEst").GetBoolean());
        Assert.Equal(2, r.GetProperty("keys").GetArrayLength());
        Assert.Equal("srv-a|Orders|0xABC", Str(r, "hashKey"));
    }

    [Fact]
    public void EachKind_DownloadsUnderAFileNameOfItsOwn()
    {
        var r = Run("stems");
        var names = Strs(r, "names");
        Assert.Equal(3, names.Distinct().Count());
        Assert.All(names, n => Assert.EndsWith(".sqlplan", n));
        Assert.Equal("active-57-2026-03-04T05_06_07.1234560-live.sqlplan", names[0]);
        Assert.Equal("qs-Orders-42-7.sqlplan", names[1]);
        Assert.Equal("0x03000500AA.sqlplan", names[2]);
    }

    [Fact]
    public void ARowKindPanel_SurvivesTheRebuild_WithoutAnotherRead()
    {
        var r = Run("rebuildKinds");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsPlan").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
    }

    /// <summary>A source's scope (#5234) is part of its panel key, so one plan opened from two places is two panels, and the
    /// scope never goes to the read.</summary>
    [Fact]
    public void ASourceScope_KeepsTheSamePlanInTwoPlacesAsTwoPanels_AndIsNotSentToTheRead()
    {
        var r = Run("scopedSource");
        Assert.True(r.GetProperty("scopedOpen").GetBoolean());
        Assert.False(r.GetProperty("plainOpen").GetBoolean());
        Assert.False(r.GetProperty("otherOpen").GetBoolean());
        Assert.Equal(new[] { "srv-a|@query_store|Orders|42|7|@scope|panel-a" }, Strs(r, "keys"));
        var query = r.GetProperty("query");
        Assert.False(query.TryGetProperty("scope", out _));
        Assert.Equal("7", query.GetProperty("plan_id").GetString());
        Assert.True(r.GetProperty("againOpen").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
    }

    /// <summary>Three rows a build, rebuilt twelve times without a click: first the same rows each time, then new rows each
    /// time. The set holds the grid on the page and the one being built, so six cells at most.</summary>
    [Fact]
    public void RebuildingAPlanGridWithoutAClick_KeepsTheRedrawSetBounded()
    {
        var r = Run("rebuildsBounded");
        foreach (var phase in new[] { "sameRows", "newRows" })
        {
            var p = r.GetProperty(phase);
            var after = p.GetProperty("after").EnumerateArray().Select(e => e.GetInt32()).ToArray();
            Assert.All(after, n => Assert.InRange(n, 3, 6));
            Assert.Equal(6, after[^1]);
            // The cells of the build in progress are not on the page yet, and a later cell of that build must not drop them.
            Assert.Equal(6, p.GetProperty("during").EnumerateArray().Last().GetInt32());
        }
        Assert.Equal(6, r.GetProperty("keys").GetInt32());
    }

    [Fact]
    public void AQueryStoreRowWithNoStoredPlan_SaysSo()
    {
        var r = Run("noPlanKind");
        Assert.Contains("No stored Query Store plan found", Str(r, "text"));
        Assert.False(r.GetProperty("hasPre").GetBoolean());
    }

    [Fact]
    public void ANullSource_IsADash()
    {
        Assert.Equal("\u2014", Str(Run("nullSource"), "cell"));
    }

    [Fact]
    public void TheReproButton_ReadsTheScript_AndShowsItAsTextWithCopyAndSqlDownload()
    {
        var r = Run("repro");
        Assert.True(r.GetProperty("hasReproButton").GetBoolean());
        Assert.Equal("/api/read/get_query_repro_script", Str(r, "path"));
        var q = r.GetProperty("query");
        Assert.Equal("query_store", q.GetProperty("kind").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Equal("42", q.GetProperty("query_id").GetString());
        Assert.Equal("7", q.GetProperty("plan_id").GetString());
        Assert.Equal("USE [Orders];\nselect 1;", Str(r, "pre"));
        Assert.Equal("pre", Str(r, "preTag"));
        Assert.True(r.GetProperty("noNotice").GetBoolean());
        Assert.Equal("USE [Orders];\nselect 1;", Str(r, "clip"));
        Assert.Equal("qs-Orders-42-7.sql", Str(r, "download"));
        Assert.StartsWith("application/sql", Str(r, "downloadType"));
        Assert.True(r.GetProperty("hideLabel").GetBoolean());
    }

    [Fact]
    public void AReproWithNoStoredPlan_SaysSo_AndStillShowsTheScript()
    {
        var r = Run("reproNoPlanFound");
        Assert.Contains("No stored plan was found", Str(r, "notice"));
        Assert.True(r.GetProperty("hasScript").GetBoolean());
        Assert.Equal("query_hash", r.GetProperty("query").GetProperty("kind").GetString());
        Assert.Equal("0xAB", r.GetProperty("query").GetProperty("query_hash").GetString());
    }

    [Fact]
    public void AReproWithNoStoredText_ShowsTheEnvelope_NotAScript()
    {
        var r = Run("reproUnavailable");
        Assert.Contains("No stored query text found", Str(r, "text"));
        Assert.False(r.GetProperty("hasScript").GetBoolean());
    }

    [Fact]
    public void ALivePlanRow_AsksForTheReproWithoutLive()
    {
        var q = Run("activeSendsNoLive").GetProperty("query");
        Assert.Equal("active_snapshot", q.GetProperty("kind").GetString());
        Assert.Equal("57", q.GetProperty("session_id").GetString());
        Assert.False(q.TryGetProperty("live", out _));
    }

    [Fact]
    public void AProcedurePlan_HasNoReproButton()
    {
        var r = Run("procedureHasNoRepro");
        Assert.True(r.GetProperty("hasPlan").GetBoolean());
        Assert.False(r.GetProperty("hasRepro").GetBoolean());
    }

    [Fact]
    public void AnOpenRepro_SurvivesTheRebuild_WithoutAnotherRead_AndClosesWithItsPlan()
    {
        var r = Run("reproSurvivesRebuild");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.True(r.GetProperty("showsScript").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
        Assert.True(r.GetProperty("reproClosedWithPlan").GetBoolean());
    }
}
