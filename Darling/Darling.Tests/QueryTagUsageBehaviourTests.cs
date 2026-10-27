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
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5987: the web by-tag page and the tag column and filter on the query grids, run as the shipped page scripts under Node against a scripted
/// <c>/api/read</c> (<c>query-tag-usage-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class QueryTagUsageBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "query-tag-usage-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["HARNESS_CUT_WINDOW_MESSAGE"] = McpHelpers.CutWindowNothingMessage;
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the query tag usage harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the query tag usage harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[][] Rows(JsonElement e) =>
        e.EnumerateArray().Select(r => r.EnumerateArray().Select(c => c.GetString()!).ToArray()).Where(r => r.Length > 0).ToArray();

    [Fact]
    public void TheByTagPage_ShowsEachTagUntaggedAndTheTotal_ForThePlanCacheSection()
    {
        var run = Run();
        var rows = Rows(run.GetProperty("planRows"));
        Assert.Contains(rows, r => r[0] == "Prod");
        Assert.Contains(rows, r => r[0] == "Prod / Web");
        Assert.Contains(rows, r => r[0] == "untagged");
        Assert.Contains(rows, r => r[0] == "Total");
        // A figure is the read's own: the page adds nothing.
        Assert.Equal("9", rows.Single(r => r[0] == "Total")[1]);
        Assert.Equal("4", rows.Single(r => r[0] == "untagged")[1]);
        // The sections stay apart, and the deadlock section says how it counts.
        Assert.Contains("once for each tag among its processes", run.GetProperty("deadlockText").GetString());
        Assert.Equal("a", run.GetProperty("serverRead").GetProperty("server").GetString());
        // #6156: an empty section says "None in the last 24 hours." in plain words, with no status word in front of it.
        var deadlockText = run.GetProperty("deadlockText").GetString()!;
        Assert.Contains("None in the last 24 hours.", deadlockText);
        Assert.DoesNotContain("empty:", deadlockText);
    }

    [Fact]
    public void ChoosingFleet_ReadsWithoutAServer_AndShowsEveryServer()
    {
        var run = Run();
        var options = run.GetProperty("serverOptions").EnumerateArray().Select(o => o.GetString()).ToArray();
        Assert.Equal("", options[0]);
        Assert.Equal(new[] { "a", "b" }, options.Skip(1).Order().ToArray());
        Assert.False(run.GetProperty("fleetRead").TryGetProperty("server", out _));
        var text = run.GetProperty("fleetText").GetString()!;
        Assert.Contains("a", text);
        Assert.Contains("b", text);
        Assert.Contains("Fleet total", text);
    }

    [Fact]
    public void AnHourlyFleetSection_ShowsTheReadsEdgesNote_AndARawSectionShowsNone()
    {
        // #6794: the hourly rollup leaves out the hours past its ceiling, so the page says so beside the heading.
        var run = Run();
        var hourly = string.Join(" ", run.GetProperty("fleetHourlyNote").EnumerateArray().Select(n => n.GetString()));
        Assert.Contains("Hourly data from ", hourly);
        Assert.Contains("The 10 minutes before the first full hour are not included.", hourly);
        Assert.Contains("The hourly rollup is built only up to ", hourly);
        Assert.Empty(run.GetProperty("rawMutedNotes").EnumerateArray());
        // #6800: the Query Store section of a routed fleet read carries the same kind of note, and the page shows it as it does the plan cache's.
        var queryStore = string.Join(" ", run.GetProperty("fleetQueryStoreNote").EnumerateArray().Select(n => n.GetString()));
        Assert.Contains("Daily and hourly data, with the raw intervals for the edges, from ", queryStore);
        Assert.Contains("The 43 minutes before the first full hour are not included.", queryStore);
    }

    [Fact]
    public void TheEmptyRegressionsPanel_NamesTheTimeTheStoresDataStarts_NotTheFieldAndThePointerToHints()
    {
        // #6831: "starts at effective_start (see hints)" sent a reader to a field the page never shows.
        var run = Run();
        Assert.Equal("empty", run.GetProperty("regressionsEmptyKind").GetString());
        var message = run.GetProperty("regressionsEmptyMessage").GetString()!;
        Assert.StartsWith("Nothing in the part of the window the store covers; the store's data for this read starts at ", message);
        Assert.Contains("2026", message);
        Assert.EndsWith(", so the stretch before it is not a report that nothing happened.", message);
        foreach (var pattern in new[] { @"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", "effective_start", @"\(see hints\)", @"collect\." })
        {
            Assert.DoesNotMatch(pattern, message);
        }
    }

    [Fact]
    public void TheEdgeNotes_AreWordedFromFields_NotRawStampsOrRelationNames()
    {
        // #6831: the page's own words for every edge case, and none of the MCP text's raw UTC stamps, field names or relation names.
        var run = Run();
        var edges = run.GetProperty("edgeCases").EnumerateArray().Select(n => n.ValueKind == JsonValueKind.Null ? null : n.GetString()).ToArray();
        Assert.Contains("The 1 minute before the first full hour is not included.", edges[0]);
        Assert.Contains("The last hour is counted whole, so the data runs to ", edges[0]);
        Assert.Contains("How far the hourly rollup is built is not known", edges[1]);
        Assert.Null(edges[2]);
        Assert.Contains("Less than a minute before the first full hour is not included.", edges[3]);
        Assert.DoesNotContain("under a minute", edges[3]);
        var sources = run.GetProperty("sources").EnumerateArray().Select(n => n.GetString()).ToArray();
        Assert.Equal(["raw collections", "hourly rollup", "Query Store intervals", "hourly Query Store table, raw intervals for the edges", "dmv blocking snapshots"], sources);

        var page = run.GetProperty("fleetPageText").GetString()!;
        foreach (var pattern in new[] { @"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", "effective_start", @"\(see hints\)", @"collect\." })
        {
            Assert.DoesNotMatch(pattern, page);
        }
    }

    [Fact]
    public void TheFleetSections_NameEachServerByDisplayName_AndKeepTheKeyWhenThereIsNone()
    {
        // #6157: the read names a server by its connection key; the heading is the display name list_servers gives it.
        var headings = Run().GetProperty("fleetHeadings").EnumerateArray().Select(h => h.GetString()).ToArray();
        Assert.Equal(new[] { "Server A", "b" }, headings);
    }

    [Fact]
    public void TheAvailabilityGroupCard_SaysAsReportedByTheDisplayName_AndFallsBackToTheKey()
    {
        // #6157: "As reported by <name>" read the connection key before.
        var run = Run();
        Assert.StartsWith("As reported by Server A · ", run.GetProperty("agSubtitleNamed").GetString());
        Assert.StartsWith("As reported by b · ", run.GetProperty("agSubtitleNoDisplayName").GetString());
        Assert.StartsWith("As reported by zz · ", run.GetProperty("agSubtitleUnknown").GetString());
        Assert.StartsWith("As reported by a · ", run.GetProperty("agSubtitleNoFleet").GetString());
    }

    [Fact]
    public void AClickOnATag_OpensItsSubtree_ThroughTheReadsTagParameter()
    {
        var run = Run();
        Assert.Equal("7", run.GetProperty("tagRead").GetProperty("tag").GetString());
        var rows = Rows(run.GetProperty("tagRows"));
        Assert.DoesNotContain(rows, r => r[0] == "untagged");
        Assert.Contains(rows, r => r[0] == "Total (this tag)");
    }

    [Fact]
    public void TheTopQueriesTagFilter_SendsTag_AndTheGridShowsOnlyThatTagsRows()
    {
        var run = Run();
        Assert.False(run.GetProperty("gridFirstAsk").TryGetProperty("tag", out _));
        Assert.Equal(3, run.GetProperty("gridFirst").GetArrayLength());
        Assert.Contains("untagged", run.GetProperty("tagOptions").EnumerateArray().Select(o => o.GetString()));
        Assert.Equal("Prod / Web", run.GetProperty("gridTagAsk").GetProperty("tag").GetString());
        var tagged = run.GetProperty("gridTagged");
        Assert.Equal(1, tagged.GetArrayLength());
        Assert.Contains("select 1", tagged[0].GetProperty("text").GetString());
        Assert.Equal("untagged", run.GetProperty("gridUntaggedAsk").GetProperty("tag").GetString());
        Assert.Equal(1, run.GetProperty("gridUntagged").GetArrayLength());
    }

    [Fact]
    public void ARowWithNoTag_ShowsNoPill_AndATaggedRowShowsItsPath()
    {
        var first = Run().GetProperty("gridFirst");
        var byText = first.EnumerateArray().ToDictionary(r => r.GetProperty("text").GetString()!.Contains("select 2") ? "none" : r.GetProperty("text").GetString()!.Contains("select 1") ? "web" : "batch");
        Assert.Equal(0, byText["none"].GetProperty("pills").GetArrayLength());
        Assert.Equal("Prod / Web", byText["web"].GetProperty("pills")[0].GetString());
    }

    private static string[] StripClasses(JsonElement strips) =>
        strips.EnumerateArray().Select(s => s.GetProperty("cls").GetString()!).ToArray();

    /// <summary>#6800: the fleet read can be refused busy (429, Retry-After) once its learned p90 makes it a long read. The page shows a
    /// notice, not an error, while it waits, sends the read once more after the Retry-After, and draws the answer.</summary>
    [Fact]
    public void ARefusedBusyRead_ShowsANoticeNotAnError_AndIsSentOnceMoreAfterTheRetryAfter()
    {
        var busy = Run().GetProperty("busyOnce");
        Assert.Equal(1, busy.GetProperty("readsBefore").GetInt32());
        Assert.Contains("strip notice", StripClasses(busy.GetProperty("stripsBefore")));
        Assert.DoesNotContain("strip error", StripClasses(busy.GetProperty("stripsBefore")));
        Assert.Equal(2, busy.GetProperty("readsAfter").GetInt32());
        Assert.True(busy.GetProperty("planCardAfter").GetBoolean());
        Assert.DoesNotContain("strip error", StripClasses(busy.GetProperty("stripsAfter")));
    }

    /// <summary>#6800: a second refusal is shown as the error it is, and the page does not ask a third time.</summary>
    [Fact]
    public void ASecondBusyRefusal_IsShownAsAnError_AndNotRetriedAgain()
    {
        var busy = Run().GetProperty("busyTwice");
        Assert.Equal(2, busy.GetProperty("readsAfter").GetInt32());
        Assert.Contains("strip error", StripClasses(busy.GetProperty("stripsAfter")));
        Assert.False(busy.GetProperty("planCardAfter").GetBoolean());
    }
}
