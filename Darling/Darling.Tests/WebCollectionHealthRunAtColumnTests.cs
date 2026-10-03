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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4938: the web Collection Health tab shows a collector's run time beside "Every (min)" when one is set, and no
/// column when no row has one. The Heaviest Collectors table is fed by <c>sweep_pressure.heaviest_collectors</c> from
/// <c>get_collection_health</c>, so the tool's rows carry <c>run_at</c> and the table lists it as a column that is left
/// out unless a row fills it.
/// </summary>
public sealed class WebCollectionHealthRunAtColumnTests
{
    private static string Slice(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found: {start}");
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor not found after {start}: {end}");
        return source[from..to];
    }

    [Fact]
    public void TheHeaviestCollectorsColumns_ListTheRunTimeBesideEvery_AndLeaveItOutWhenNoRowFillsIt()
    {
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var columns = Slice(js, "const HEAVIEST_COLUMNS = [", "];");

        var every = columns.IndexOf("{ key: \"frequency_minutes\", label: \"Every (min)\", format: \"num1\" },", StringComparison.Ordinal);
        Assert.True(every >= 0, "the Every (min) column is the anchor for this pin");
        var runAt = columns.IndexOf("{ key: \"run_at\", label: \"Run at\", hideWhenEmpty: true },", StringComparison.Ordinal);
        Assert.True(runAt > every, "the run time column sits right after Every (min) and drops out when empty");
        Assert.Single(columns.Split("hideWhenEmpty", StringSplitOptions.None)[1..]);
    }

    [Fact]
    public void TheToolsHeaviestCollectorRows_CarryTheRunTime()
    {
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        var heaviest = Slice(tools, "var heaviest = rows", "var peakCollector");
        Assert.Contains("frequency_minutes = r.FrequencyMinutes,", heaviest, StringComparison.Ordinal);
        Assert.Contains("run_at = ", heaviest, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCollectorsTable_ListsTheRunTimeTheNextRunAndTheSkippedDayNote_AndLeavesEachOutWhenNoRowFillsIt()
    {
        /* The Heaviest Collectors table ranks by cost per minute, which puts a once-a-day collector last, so the table
           that lists every collector carries the run time too. */
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var columns = Slice(js, "const COLLECTOR_COLUMNS = [", "];");

        Assert.Contains("{ key: \"run_at\", label: \"Run at\", hideWhenEmpty: true },", columns, StringComparison.Ordinal);
        Assert.Contains("{ key: \"next_run_utc\", label: \"Next run\", format: \"time\", hideWhenEmpty: true },", columns, StringComparison.Ordinal);
        Assert.Contains("{ key: \"run_time_note\", label: \"Run time\", wrap: true, hideWhenEmpty: true },", columns, StringComparison.Ordinal);
        Assert.True(columns.IndexOf("key: \"last_success\"", StringComparison.Ordinal) < columns.IndexOf("key: \"run_at\"", StringComparison.Ordinal));
    }

    [Fact]
    public void TheToolsFullAndPartialRows_CarryTheRunTimeNextRunAndNote()
    {
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");
        foreach (var field in new[] { "run_at = runTime?.RunAt,", "next_run_utc = runTime?.NextRunUtcText", "run_time_note = runTime?.SkippedDayNote" })
        {
            Assert.Equal(2, tools.Split(field, StringSplitOptions.None).Length - 1);
        }
    }

    [Fact]
    public void TheShippedHeaviestColumns_ShowTheRunTimeWhenARowHasOne_AndNoColumnWhenNoneDoes()
    {
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var literal = Slice(js, "const HEAVIEST_COLUMNS = ", "];") + "]";
        literal = literal["const HEAVIEST_COLUMNS = ".Length..];

        var script = """
            const m = await import(process.argv[1]);
            const columns = new Function("return " + process.argv[2])();
            const keys = (rows) => m.visibleColumns(columns, rows).map((c) => c.key).join(",");
            console.log(JSON.stringify({
              set: keys([{ collector: "index_object_stats", frequency_minutes: 1440, run_at: "02:00" }, { collector: "wait_stats", frequency_minutes: 1, run_at: null }]),
              none: keys([{ collector: "wait_stats", frequency_minutes: 1, run_at: null }, { collector: "query_stats", frequency_minutes: 1 }]),
            }));
            """;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add("--input-type=module");
        psi.ArgumentList.Add("-e");
        psi.ArgumentList.Add(script);
        psi.ArgumentList.Add(new Uri(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js")).AbsoluteUri);
        psi.ArgumentList.Add(literal);

        Process proc;
        try { proc = Process.Start(psi)!; }
        catch (Win32Exception) { Assert.Skip("Node is not installed, so the shipped page script cannot be run."); return; } // The source pins above still hold the change in place.

        using (proc)
        {
            var output = proc.StandardOutput.ReadToEnd().Trim();
            var error = proc.StandardError.ReadToEnd();
            Assert.True(proc.WaitForExit(20000) && proc.ExitCode == 0, "node failed: " + error);
            using var doc = JsonDocument.Parse(output);
            Assert.Contains("frequency_minutes,run_at,", doc.RootElement.GetProperty("set").GetString() + ",", StringComparison.Ordinal);
            Assert.DoesNotContain("run_at", doc.RootElement.GetProperty("none").GetString(), StringComparison.Ordinal);
            Assert.Contains("frequency_minutes", doc.RootElement.GetProperty("none").GetString(), StringComparison.Ordinal);
        }
    }
}
