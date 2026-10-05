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
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Source pins and a Node run for the web Job History page: it reads get_job_history with the parameters the tool
/// takes, offers the desktop's columns and filters, keeps its filters across the 60 s rebuild, draws the tool's two
/// notices, and sits as one block at the end of the nav and the router.
/// </summary>
public sealed class JobHistoryPageTests
{
    private static string Wwwroot(params string[] parts) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot" }.Concat(parts).ToArray()).ReplaceLineEndings("\n");

    private static string Page() => Wwwroot("js", "pages", "job-history.js");

    [Fact]
    public void ThePageReadsGetJobHistoryThroughTheSharedReadWithASignal()
    {
        var page = Page();
        Assert.Contains("readToolWithinKeptHistory(\"get_job_history\", readParams(), signal)", page);
        Assert.Contains("readTool(\"list_servers\", {}, controller.signal)", page);
        Assert.Contains("VIZ.table(data,", page);
        Assert.Contains("rowsKey: \"runs\"", page);
        Assert.DoesNotContain("innerHTML", page);
    }

    [Fact]
    public void EveryParameterThePageSendsIsOneTheToolTakes()
    {
        var method = typeof(DarlingMcpJobTools).GetMethod(nameof(DarlingMcpJobTools.GetJobHistory))!;
        Assert.Equal("get_job_history", method.GetCustomAttribute<McpServerToolAttribute>()!.Name);
        var tool = method.GetParameters().Select(p => p.Name!).ToHashSet();
        var sent = Regex.Matches(Page(), "\\bp\\.([a-z_]+) = ").Select(m => m.Groups[1].Value).ToList();
        Assert.NotEmpty(sent);
        // The web route maps the page's hours / server names onto the tool's hours_back / server_name.
        var aliases = new System.Collections.Generic.Dictionary<string, string> { ["hours"] = "hours_back", ["server"] = "server_name" };
        foreach (var name in sent)
            Assert.Contains(aliases.TryGetValue(name, out var mapped) ? mapped : name, tool);
        Assert.Contains("const p = { hours: state.hours, limit: state.limit };", Page());
    }

    [Fact]
    public void TheStatusChoicesAreTheOnesTheToolAccepts()
    {
        var statuses = Regex.Matches(Page(), "\\{ value: \"([A-Za-z]*)\", label: \"[^\"]+\" \\}")
            .Select(m => m.Groups[1].Value).Where(v => v.Length > 0);
        Assert.Equal(new[] { "Failed", "Succeeded", "Retry", "Canceled" }, statuses);
    }

    [Fact]
    public void TheColumnKeysAreTheOnesTheToolEmits()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpJobTools.cs");
        var keys = Regex.Matches(Page(), "\\{ key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal(10, keys.Count);
        foreach (var key in keys)
            Assert.Matches("(?m)^\\s+" + Regex.Escape(key) + " = ", source.ReplaceLineEndings("\n"));
    }

    [Fact]
    public void TheNavEntryAndRouteAreTheLastOnesInTheirLists()
    {
        var html = Wwwroot("index.html");
        var navLinks = Regex.Matches(html, "<a data-route=\"([a-z-]+)\"").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal("job-history", navLinks[^1]);

        var app = Wwwroot("js", "app.js");
        Assert.Contains("import { renderJobHistory } from \"./pages/job-history.js\";", app);
        Assert.Contains("if (h === \"#/job-history\" || h === \"#/job-history/\") return { name: \"jobHistory\" };", app);
        Assert.Contains("else if (r.name === \"jobHistory\") renderJobHistory(main);", app);
        Assert.Contains("if (r.name === \"jobHistory\") return \"job-history\";", app);
    }

    [Fact]
    public void TheActivityTabCarriesAPerServerJobHistoryGrid()
    {
        var tabs = Wwwroot("js", "pages", "server-tabs.js");
        Assert.Contains("\"get_job_history\",\n        { server, hours: ctx.hours, limit: 100 },", tabs);
        Assert.Contains("        JOB_HISTORY_COLUMNS,\n        ctx.label + \", newest 100 runs", tabs);
        var page = Regex.Matches(Page(), "\\{ key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).Where(k => k != "server");
        var block = tabs[tabs.IndexOf("const JOB_HISTORY_COLUMNS = [", StringComparison.Ordinal)..];
        var tab = Regex.Matches(block[..block.IndexOf("];", StringComparison.Ordinal)], "\\{ key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value);
        Assert.Equal(page, tab);
    }

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "job-history-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
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
                Assert.Fail("the job-history harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the job-history harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheFirstBuildReadsTheFleetWithTheDefaultsAndTheDesktopColumns()
    {
        var run = Run();
        var first = run.GetProperty("first");
        var call = Assert.Single(first.GetProperty("calls").EnumerateArray());
        var q = call.GetProperty("q");
        Assert.Equal("24", q.GetProperty("hours").GetString());
        Assert.Equal("100", q.GetProperty("limit").GetString());
        Assert.False(q.TryGetProperty("server", out _));
        Assert.True(call.GetProperty("signal").GetBoolean());
        Assert.Equal("Run Time,Server,Job,Category,Step,Status,Duration,Retries,Last Success,Message", string.Join(",", Strings(run.GetProperty("columns"))));
        Assert.Equal("Run Time,Server,Job,Category,Step,Status,Duration,Retries,Last Success,Message", string.Join(",", Strings(first.GetProperty("headers"))));
        Assert.Equal(2, first.GetProperty("bodyRows").GetInt32());
        Assert.Equal(",srv-a,srv-b", string.Join(",", Strings(first.GetProperty("servers"))));
        Assert.Equal(",Backup,Maintenance", string.Join(",", Strings(first.GetProperty("categories"))));
        // Text only: a value that looks like markup is drawn as its characters.
        Assert.Contains("<b>boom</b>", first.GetProperty("text").GetString());
    }

    [Fact]
    public void TheFiltersReachTheReadAndSurviveARebuild()
    {
        var run = Run();
        var picked = run.GetProperty("picked").GetProperty("calls").EnumerateArray().Last().GetProperty("q");
        Assert.Equal("Failed", picked.GetProperty("status").GetString());
        Assert.Equal("Maintenance", picked.GetProperty("category").GetString());

        var rebuilt = run.GetProperty("rebuilt");
        var q = Assert.Single(rebuilt.GetProperty("calls").EnumerateArray()).GetProperty("q");
        Assert.Equal("Failed", q.GetProperty("status").GetString());
        Assert.Equal("Maintenance", q.GetProperty("category").GetString());
        Assert.Equal("Nightly", q.GetProperty("job_name").GetString());
        Assert.Equal("Nightly", rebuilt.GetProperty("job").GetString());
        Assert.True(rebuilt.GetProperty("jobFocused").GetBoolean());
        Assert.Equal("Failed", rebuilt.GetProperty("selects").GetProperty("Status").GetString());
    }

    [Fact]
    public void AFleetCallNamesNoServerAndAOneServerCallNamesIt()
    {
        var run = Run();
        var one = Assert.Single(run.GetProperty("oneServer").GetProperty("calls").EnumerateArray()).GetProperty("q");
        Assert.Equal("srv-a", one.GetProperty("server").GetString());
        var fleet = Assert.Single(run.GetProperty("fleet").GetProperty("calls").EnumerateArray()).GetProperty("q");
        Assert.False(fleet.TryGetProperty("server", out _));
    }

    [Fact]
    public void TheRetainedDataAndRowLimitNoticesShowOnlyWhenTheReadSetsThem()
    {
        var run = Run();
        var notices = run.GetProperty("notices");
        Assert.Equal(2, notices.GetProperty("noticeCount").GetInt32());
        var text = notices.GetProperty("text").GetString()!;
        Assert.Contains("partial window: the store keeps job history from ", text);
        Assert.Contains("Showing the newest 2 runs; more matched.", text);
        Assert.Equal(0, run.GetProperty("quiet").GetProperty("noticeCount").GetInt32());
    }

    [Fact]
    public void ARebuildInPlaceKeepsTheTypedTextFocusAndCaretWithOneRead()
    {
        var inPlace = Run().GetProperty("inPlace");
        Assert.Single(inPlace.GetProperty("calls").EnumerateArray());
        Assert.Equal("Other", inPlace.GetProperty("job").GetString());
        Assert.True(inPlace.GetProperty("focused").GetBoolean());
        Assert.Equal("2,3", string.Join(",", inPlace.GetProperty("caret").EnumerateArray().Select(x => x.GetInt32())));
    }

    [Fact]
    public void AnEmptyAnswersWindowFactsAreReadFromItsHints()
    {
        var empty = Run().GetProperty("emptyPartial");
        Assert.Equal(1, empty.GetProperty("noticeCount").GetInt32());
        Assert.Contains("partial window: the store keeps job history from ", empty.GetProperty("text").GetString());
    }

    [Fact]
    public void RowsAreColouredLikeTheDesktopGridAndTheCountIsShown()
    {
        var coloured = Run().GetProperty("coloured");
        Assert.Equal("sev-Critical,sev-Warning,band-Offline,sev-Warning,", string.Join(",", Strings(coloured.GetProperty("rowClasses"))));
        Assert.Contains("5 runs shown", coloured.GetProperty("text").GetString());
    }

    [Fact]
    public void TheServerChoicesListSqlServerTargetsOnly()
    {
        Assert.Equal(",srv-a,srv-b", string.Join(",", Strings(Run().GetProperty("first").GetProperty("servers"))));
    }

    [Fact]
    public void TheAgentLineSaysStoppedInRed_RunningOrUnknown_AndRollsUpAcrossServers()
    {
        var agent = Run().GetProperty("agent");
        string Text(string k) => agent.GetProperty(k).GetProperty("text").GetString()!;
        string Class(string k) => agent.GetProperty(k).GetProperty("cls").GetString()!;

        Assert.Equal("SQL Agent is stopped on srv-a", Text("stopped"));
        Assert.Equal("agent-line agent-stopped", Class("stopped"));
        Assert.Equal("SQL Agent running", Text("running"));
        Assert.Equal("agent-line agent-running", Class("running"));
        Assert.Equal("Agent status unknown (no recent snapshot)", Text("unknown"));
        Assert.Equal("agent-line agent-unknown", Class("unknown"));

        Assert.Equal("Agent running on 2 of 4 servers; stopped on srv-b; status unknown on srv-d", Text("rollup"));
        Assert.Equal("agent-line agent-stopped", Class("rollup"));
        Assert.Equal("Agent running on 2 of 2 servers", Text("allRunning"));
        Assert.Equal("agent-line agent-running", Class("allRunning"));

        // An empty answer carries the Agent under hints, and it is shown there: that is when it matters most.
        Assert.Equal("SQL Agent is stopped on srv-a", Text("emptyStopped"));
        Assert.Equal("Agent running on 2 of 3 servers; stopped on srv-a", Text("emptyFleet"));
        // A server with no SQL Agent service is not a stopped one: a plain line, never red, and the roll-up stays green.
        Assert.Equal("No SQL Agent service on srv-x", Text("noService"));
        Assert.Equal("agent-line agent-none", Class("noService"));
        Assert.Equal("Agent running on 1 of 2 servers; no SQL Agent service on srv-x", Text("rollupNoService"));
        Assert.Equal("agent-line agent-running", Class("rollupNoService"));
        Assert.Equal("Agent running on 1 of 3 servers; stopped on srv-b; no SQL Agent service on srv-x", Text("rollupMixed"));
        Assert.Equal("agent-line agent-stopped", Class("rollupMixed"));
        Assert.Equal("No SQL Agent service on srv-x", Text("emptyNoService"));
        // An answer with no Agent fields draws no line.
        Assert.Equal(JsonValueKind.Null, agent.GetProperty("absent").ValueKind);
    }

    [Fact]
    public void TheStoppedAgentLineIsRed()
    {
        var css = Wwwroot("css", "app.css");
        Assert.Matches("\\.agent-line\\.agent-stopped \\{ color: var\\(--err\\);", css);
    }

    [Fact]
    public void TheAgentLineReadsOnlyFieldsTheToolEmits()
    {
        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpJobTools.cs").ReplaceLineEndings("\n");
        foreach (var field in new[] { "agent_running", "agent_status_desc", "next_run", "captured_at", "agents_total", "agents_running", "agents_not_running" })
            Assert.Contains("[\"" + field + "\"]", tool, StringComparison.Ordinal);
        var page = Page();
        Assert.Contains("src.agents_total", page);
        /* The page recognises the no-service server by the exact text the reader serves. */
        Assert.Contains("const NO_AGENT_SERVICE = \"" + DarlingJobReader.NoAgentServiceDescription + "\";", page);
        Assert.Contains("\"agent_running\" in src", page);
    }


    [Fact]
    public void TheRunTimeColumnIsAnInstantFieldSoACustomRangeTrimsTheGrid()
    {
        Assert.Matches("INSTANT_FIELDS = \\[[^\\]]*\"run_time\"", Wwwroot("js", "util.js"));
    }
}
