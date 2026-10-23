/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Diagnostics;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
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
        /* #6384: the rows the tool answers with, not the text that builds them. A collector with a run time carries it
           as run_at, a collector without one carries the key as null, and the rows keep the cadence beside it. */
        var rows = new[]
        {
            new CollectorHealth { CollectorName = "wait_stats", AvgDurationMs = 900, P95DurationMs = 1_500, EffectiveFrequencyMinutes = 1 },
            new CollectorHealth { CollectorName = "perfmon_stats", AvgDurationMs = 300, P95DurationMs = 400, EffectiveFrequencyMinutes = 1 },
        };
        var runTimes = new Dictionary<string, CollectorRunTimeReading>(StringComparer.OrdinalIgnoreCase)
        {
            ["wait_stats"] = new CollectorRunTimeReading("03:15", null),
        };

        var json = JsonSerializer.Serialize(DarlingMcpDataTools.HeaviestCollectorRows(rows, runTimes));
        var heaviest = JsonDocument.Parse(json).RootElement.EnumerateArray().ToDictionary(r => r.GetProperty("collector").GetString()!);

        Assert.Equal("03:15", heaviest["wait_stats"].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.Null, heaviest["perfmon_stats"].GetProperty("run_at").ValueKind);
        Assert.Equal(1, heaviest["wait_stats"].GetProperty("frequency_minutes").GetInt32());
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
        Assert.Contains("{ key: \"run_time_note\", label: \"Run time\", clamp: true, localizeText: true, hideWhenEmpty: true },", columns, StringComparison.Ordinal);
        Assert.True(columns.IndexOf("key: \"last_success\"", StringComparison.Ordinal) < columns.IndexOf("key: \"run_at\"", StringComparison.Ordinal));
    }

    [Fact]
    public void TheToolsFullRow_AlwaysCarriesTheRunTime_AndThePartialAndCompactRowsCarryItOnlyForACollectorThatHasOne()
    {
        var tools = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs");

        /* The full row ends with the three fields, null for a collector with no run time. */
        foreach (var field in new[] { "run_at = runTime?.RunAt,", "next_run_utc = runTime?.NextRunUtcText,", "run_time_note = runTime?.SkippedDayNote" })
        {
            Assert.Equal(1, tools.Split(field, StringSplitOptions.None).Length - 1);
        }

        /* The compact row has a second shape for a collector that has a run time, and that is the only place its run_at and
           next_run_utc are written, so a collector with none adds no bytes to it (Lite carries the same two fields under
           the same rule). */
        foreach (var field in new[] { "run_at = (string?)runTime.RunAt,", "next_run_utc = runTime.NextRunUtcText," })
        {
            Assert.Equal(1, tools.Split(field, StringSplitOptions.None).Length - 1);
        }

        /* #5748: the partial row is one dictionary that leaves out a null, false or 0 key. Its run time keys are written
           in exactly one place, under `if (runTime is not null)`, so a collector with no run time adds no bytes to it. The
           note rides there too and is left out when null, so only a collector with a run time that skipped a day has one. */
        var partialAt = tools.IndexOf("static Dictionary<string, object?> PartialCollectionHealthRow(", StringComparison.Ordinal);
        Assert.True(partialAt > 0);
        var guardAt = tools.IndexOf("if (runTime is not null)", partialAt, StringComparison.Ordinal);
        Assert.True(guardAt > partialAt);
        foreach (var call in new[]
        {
            "AddUnlessDefault(row, \"run_at\", runTime.RunAt);",
            "AddUnlessDefault(row, \"next_run_utc\", runTime.NextRunUtcText);",
            "AddUnlessDefault(row, \"run_time_note\", runTime.SkippedDayNote);",
        })
        {
            Assert.Equal(1, tools.Split(call, StringSplitOptions.None).Length - 1);
            Assert.True(tools.IndexOf(call, StringComparison.Ordinal) > guardAt, call + " must sit under the runTime guard");
        }

        /* The note is the full row's always, so the plain assignment form appears once, in the full row. A compact row
           never has one. */
        Assert.Equal(1, tools.Split("run_time_note = ", StringSplitOptions.None).Length - 1);
        Assert.DoesNotContain("run_time_note = (string?)null", tools, StringComparison.Ordinal);
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
        catch (Win32Exception) { Assert.Skip("Node is not installed, so the shipped page script cannot be run."); throw; } // The source pins above still hold the change in place.

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
