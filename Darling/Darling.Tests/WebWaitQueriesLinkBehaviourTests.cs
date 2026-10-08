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
/// The web wait links (#5235), from the shipped <c>server-tabs.js</c>, <c>charts.js</c>, <c>util.js</c> and <c>panels.js</c>
/// run under Node (<c>web-wait-queries-link-harness.mjs</c>) against a scripted <c>/api/read</c>: the Wait cell of the Wait
/// Stats, Waiting Tasks and Significant Waits grids is a button that opens Active Queries filtered to that wait, over the hour
/// around the row's own time (Wait Stats keeps the range), and the filtered Active Queries panel reads at most 500 rows and
/// carries a strip that clears the filter. Node is skipped when it is not installed.
/// </summary>
public sealed class WebWaitQueriesLinkBehaviourTests
{
    private const long HalfMs = 30 * 60000;
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-wait-queries-link-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped web scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the wait links harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the wait links harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[^1]);
            return doc.RootElement.Clone();
        }
    }

    private static string Js(params string[] rel) =>
        ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", Path.Combine(rel)));

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    private static JsonElement OneCall(JsonElement r, string name)
    {
        var calls = r.GetProperty(name);
        Assert.Equal(1, calls.GetArrayLength());
        return calls[0];
    }

    private const string Markup = "<img src=x onerror=alert(1)>";

    [Fact]
    public void AWaitingTasksRow_OpensActiveQueries_FilteredToItsWait_AtItsTimePlusOrMinusThirtyMinutes()
    {
        var r = Result.Value;
        var call = OneCall(r, "taskCalls");
        var rowMs = r.GetProperty("taskRowFracMs").GetInt64();
        Assert.Equal(rowMs - HalfMs, call.GetProperty("startMs").GetInt64());
        Assert.Equal(rowMs + HalfMs, call.GetProperty("endMs").GetInt64());
        Assert.False(call.GetProperty("redraw").GetBoolean()); // the router rebuilds the page, not a panel redraw
        Assert.Equal("LCK_M_X", r.GetProperty("taskFilter").GetString());
        Assert.Equal("#/server/A/queries", r.GetProperty("taskHash").GetString());
        Assert.Equal("Show the active queries waiting on LCK_M_X around this time", r.GetProperty("taskTitle").GetString());
    }

    [Fact]
    public void ASignificantWaitsRow_OpensActiveQueries_FilteredToItsWait_AtItsEventTime()
    {
        var r = Result.Value;
        var call = OneCall(r, "sigCalls");
        var rowMs = r.GetProperty("sigRowFloorMs").GetInt64();
        Assert.Equal(rowMs - HalfMs, call.GetProperty("startMs").GetInt64());
        Assert.Equal(rowMs + HalfMs, call.GetProperty("endMs").GetInt64());
        Assert.Equal("PAGEIOLATCH_SH", r.GetProperty("sigFilter").GetString());
        Assert.Equal("#/server/A/queries", r.GetProperty("sigHash").GetString());
    }

    [Fact]
    public void AWaitStatsRow_OpensActiveQueries_FilteredToItsWait_KeepingTheRange()
    {
        // A Wait Stats row is a window total with no instant of its own, so no range is applied and the page keeps its own.
        var r = Result.Value;
        Assert.Equal(0, r.GetProperty("statCalls").GetArrayLength());
        Assert.Equal("WRITELOG", r.GetProperty("statFilter").GetString());
        Assert.Equal("#/server/A/queries", r.GetProperty("statHash").GetString());
    }

    [Fact]
    public void ARowTimeWithMicroseconds_IsInsideTheRangeItOpens()
    {
        // The rows carry 6 and 7 fractional digits; a Date holds milliseconds, so the instant must still sit inside the hour.
        var r = Result.Value;
        var task = OneCall(r, "taskCalls");
        var sig = OneCall(r, "sigCalls");
        var taskMs = r.GetProperty("taskRowFracMs").GetInt64();
        var sigMs = r.GetProperty("sigRowFloorMs").GetInt64();
        Assert.InRange(taskMs, task.GetProperty("startMs").GetInt64(), task.GetProperty("endMs").GetInt64() - 1);
        Assert.InRange(sigMs, sig.GetProperty("startMs").GetInt64(), sig.GetProperty("endMs").GetInt64() - 1);
        Assert.Equal(2 * HalfMs, task.GetProperty("endMs").GetInt64() - task.GetProperty("startMs").GetInt64());
    }

    [Fact]
    public void ARowWithNoWaitOrNoParseableTime_HasNoLink()
    {
        // Waiting Tasks rows: LCK_M_X (link), QDS_ASYNC_QUEUE (never linked), no wait, a time that does not parse, markup (link).
        var r = Result.Value;
        Assert.Equal(new[] { "LCK_M_X", Markup }, Strings(r.GetProperty("taskButtons")));
        Assert.Equal(new[] { "LCK_M_X", "QDS_ASYNC_QUEUE", "—", "CXPACKET", Markup }, Strings(r.GetProperty("taskWaitCells")));
        // The Wait Stats and Significant Waits grids leave their QDS_* waits as text too.
        Assert.Equal(new[] { "WRITELOG" }, Strings(r.GetProperty("statButtons")));
        Assert.Equal(new[] { "PAGEIOLATCH_SH" }, Strings(r.GetProperty("sigButtons")));
        Assert.Equal(new[] { "PAGEIOLATCH_SH", "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP" }, Strings(r.GetProperty("sigWaitCells")));
        Assert.Equal(0, r.GetProperty("rejectionsAfterBuild").GetInt32());
    }

    [Fact]
    public void TheFilteredGrid_ReadsWaitTypeAtLimit500_AndTheStripClearsIt()
    {
        var r = Result.Value;
        // Unfiltered: byte-identical to today's request, no strip.
        Assert.Equal("?server=A&hours=24&limit=50", r.GetProperty("unfilteredQuery").GetString());
        Assert.Equal(0, r.GetProperty("unfilteredStrips").GetInt32());
        Assert.Equal("Active Queries last 24 hours", r.GetProperty("unfilteredHeadText").GetString());

        Assert.Equal("?server=A&hours=24&limit=500&wait_type=LCK_M_X", r.GetProperty("filteredQuery").GetString());
        // The heading stays "Active Queries"; the subtitle names the wait; the strip sits between the heading and the grid.
        Assert.Equal("Active Queries last 24 hours, waiting on LCK_M_X", r.GetProperty("filteredHeadText").GetString());
        Assert.Equal(new[] { "h3:", "div:strip", "div:panel-body" }, Strings(r.GetProperty("filteredHeadChildOrder")));
        Assert.Equal(1, r.GetProperty("stripCount").GetInt32());
        Assert.Equal("Only requests waiting on LCK_M_X. Show all active queries", r.GetProperty("stripText").GetString());
        Assert.Equal("Show all active queries", r.GetProperty("showAllLabel").GetString());

        // The button clears the filter and rebuilds through a same-hash hashchange.
        Assert.Equal("", r.GetProperty("filterAfterShowAll").GetString());
        Assert.Equal(1, r.GetProperty("dispatchedAfterShowAll").GetInt32());
        Assert.Equal("#/server/A/waits", r.GetProperty("hashAfterShowAll").GetString());

        // A filtered miss names the wait: the server's envelope does, and so does the panel's own text.
        Assert.Equal(new[] { "No active-query snapshots with wait_type 'WRITELOG' were collected in this window." }, Strings(r.GetProperty("emptyEnvelopeText")));
        Assert.Equal(new[] { "No active queries waiting on WRITELOG were captured in this window." }, Strings(r.GetProperty("emptyRowsText")));
    }

    [Fact]
    public void ARefusedRange_SetsNoFilter_AndKeepsTheTab()
    {
        var r = Result.Value;
        Assert.Equal("", r.GetProperty("refusedFilter").GetString());
        Assert.Equal("#/server/A/waits", r.GetProperty("refusedHash").GetString());
        Assert.Equal(1, r.GetProperty("refusedCalls").GetInt32());
        Assert.Equal(new[] { "The end cannot be in the future." }, Strings(r.GetProperty("refusedNote")));
    }

    [Fact]
    public void AWaitName_RendersAsText_NeverAsMarkup()
    {
        var r = Result.Value;
        Assert.True(r.GetProperty("markupFound").GetBoolean());
        Assert.Equal(0, r.GetProperty("markupChildren").GetInt32());
        Assert.Equal(Markup, r.GetProperty("markupFilter").GetString());
        Assert.Equal("Only requests waiting on " + Markup + ". Show all active queries", r.GetProperty("markupStripText").GetString());
        Assert.Equal(0, r.GetProperty("markupStripSpanChildren").GetInt32());
        Assert.Equal("Active Queries last 24 hours, waiting on " + Markup, r.GetProperty("markupSubtitle").GetString());
    }

    [Fact]
    public void TheWaitColumns_KeepTheirRawKey_SoSortFilterCsvAndCopyReadTheWait()
    {
        // The cell is built by a render function on the column; the column keeps key wait_type, which the grid tools read.
        var tabs = Js("pages", "server-tabs.js");
        Assert.Contains("c.key === \"wait_type\" ? { ...c, render:", tabs, StringComparison.Ordinal);
        Assert.Contains("withWaitLink(WAIT_COLUMNS, server, null)", tabs, StringComparison.Ordinal);
        Assert.Contains("withWaitLink(WAITING_TASK_COLUMNS, server, \"collection_time\")", tabs, StringComparison.Ordinal);
        Assert.Contains("withWaitLink(SIGNIFICANT_WAIT_COLUMNS, server, \"event_time\")", tabs, StringComparison.Ordinal);
        // One predicate says which waits are linked; the grid cell and the chart item both call it.
        Assert.Contains("waitIsLinked(wait)", tabs, StringComparison.Ordinal);
        Assert.Contains("waitIsLinked(wait)", Js("charts.js"), StringComparison.Ordinal);
    }
}
