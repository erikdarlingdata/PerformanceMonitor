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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Web click-through fixes (text): the shipped <c>plain-text.js</c> run under Node, which puts the internal names the
/// service writes for an MCP client (<c>hints.captures</c>, <c>hours_back</c>, <c>denied_since_last_success</c>, collector
/// measurement counts, the engine-gate sentence) into words for the web page. Node is skipped when it is not installed.
/// </summary>
public sealed class WebPlainTextTests
{
    private static string[] Run(string[] inputs, out string notCollectedLine) => RunDoc(inputs, out notCollectedLine).Out;

    private static (string[] Out, string[] Named, string[] Tools, string[] Labels) RunDoc(string[] inputs, out string notCollectedLine)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-plain-text-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(inputs);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            notCollectedLine = "";
            return ([], [], [], []);
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the plain text harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the plain text harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            notCollectedLine = doc.RootElement.GetProperty("line").GetString()!;
            string[] Strs(string name) => doc.RootElement.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();
            return (Strs("out"), Strs("named"), Strs("tools"), Strs("labels"));
        }
    }

    [Fact]
    public void TheHintsPointer_AndHoursBack_AreDroppedOrSaidInWords()
    {
        var o = Run(
        [
            "No blocking was recorded in the last 24 hour(s). 5 collector run(s) DID execute, so this is a genuine all-clear - see hints.captures for which collectors ran and when.",
            "This is a gap rather than a dead collector: widen hours_back, or use get_collection_health to find where it stopped.",
        ], out _);
        Assert.DoesNotContain("hints.captures", o[0]);
        Assert.EndsWith("genuine all-clear.", o[0]);
        Assert.DoesNotContain("hours_back", o[1]);
        Assert.Contains("widen the time range, or use Collection Health to find", o[1]);
    }

    [Fact]
    public void TheHealthOutputSentence_NamesNoField()
    {
        var o = Run(
        [
            "Stored 0 rows across 96 runs with no current denial (denied_since_last_success is false), so this collector read and found nothing.",
            "Stored 0 rows across 96 runs, and denied_since_last_success is true - the newest denial postdates the newest success, so this collector is refused.",
        ], out _);
        Assert.Equal("Stored 0 rows across 96 runs with no current denial, so this collector read and found nothing.", o[0]);
        Assert.DoesNotContain("denied_since_last_success", o[1]);
        Assert.Contains("the newest denial is newer than the newest success", o[1]);
    }

    [Fact]
    public void CollectorMeasurementCounts_ReadAsWords()
    {
        var o = Run(["latest run: shred_gated=1 events_read=0 report_xml_empty=0"], out _);
        Assert.Equal("latest run: shred gated: 1, events read: 0, report xml empty: 0", o[0]);
    }

    [Fact]
    public void UserDataAndRealErrors_ReachThePageAsWritten()
    {
        /* Round-1 M4: only known internal tokens are rewritten. */
        string[] same =
        [
            "Could not find stored procedure 'dbo.get_orders'",
            "Server 'get_prod' not found",
            "timeout=30",
            "A driver said: timeout=30 while connecting",
            "sales_2024",
            "pg_stat_statements_1 was reset",
            "Could not find get_orders_by_day",
        ];
        var o = Run(same, out _);
        Assert.Equal(same, o);
    }

    [Fact]
    public void TheClickThroughCases_StillReadInWords()
    {
        var o = Run(
        [
            "use get_collection_health to find where it stopped",
            "latest run: shred_gated=1 events_read=0 report_xml_empty=0",
            "latest run: events_read=4",
            "latest noted run: shred_gated_1 events_read_0 (3 of 5 runs)",
            "pg_wraparound_stats failed; last_error has the text",
        ], out _);
        Assert.Equal("use Collection Health to find where it stopped", o[0]);
        Assert.Equal("latest run: shred gated: 1, events read: 0, report xml empty: 0", o[1]);
        Assert.Equal("latest run: events read: 4", o[2]);
        Assert.Equal("latest noted run: shred gated: 1, events read: 0 (3 of 5 runs)", o[3]);
        Assert.Equal("the freeze-headroom collector failed; Last Error has the text", o[4]);
    }

    [Fact]
    public void TheNamedRules_TouchOnlyTheNamedTokens()
    {
        string[] inputs =
        [
            "Could not find 'dbo.get_orders' (timeout=30); widen hours_back or use get_collection_health",
            "latest run: events_read=4 shred_gated=1 and last_error shows it",
            "relation \"pg_wraparound_stats\" does not exist",
            "column \"last_error\" of relation \"x\" does not exist",
            "denied_since_last_success is true; move as_of or widen days_back",
        ];
        var (_, named, _, _) = RunDoc(inputs, out _);
        Assert.Equal("Could not find 'dbo.get_orders' (timeout=30); widen the time range or use get_collection_health", named[0]);
        /* Round-2 L1: a Last Error can quote a real relation or column, so field and table names are not rewritten here. */
        Assert.Equal("latest run: events_read=4 shred_gated=1 and last_error shows it", named[1]);
        Assert.Equal("relation \"pg_wraparound_stats\" does not exist", named[2]);
        Assert.Equal("column \"last_error\" of relation \"x\" does not exist", named[3]);
        Assert.Equal("a denial is newer than the last success; move the end date or widen the date range", named[4]);
    }

    [Fact]
    public void TheToolNameList_IsExactlyTheRegisteredGetTools()
    {
        /* The get_ rule rewrites only these names, so the list must be the MCP tool registry's get_ tools, both ways. */
        var (_, _, tools, _) = RunDoc([], out _);
        if (tools.Length == 0)
        {
            return;
        }

        var registry = typeof(PerformanceMonitor.Darling.Service.Mcp.DarlingMcpAlertTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<ModelContextProtocol.Server.McpServerToolAttribute>()?.Name)
            .Where(n => n is not null && n.StartsWith("get_", StringComparison.Ordinal))
            .Select(n => n!)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(registry.Count > 100, $"the registry walk found only {registry.Count} get_ tools");
        Assert.Empty(registry.Except(tools).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Empty(tools.Except(registry).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(tools.Length, tools.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void TheMeasurementLabelList_CoversEveryLabelTheCollectorsWrite()
    {
        var (_, _, tools, labels) = RunDoc([], out _);
        if (tools.Length == 0)
        {
            return;
        }

        var found = new System.Collections.Generic.SortedSet<string>(StringComparer.Ordinal);
        var dir = PathTo("PerformanceMonitor.Collectors");
        foreach (var file in System.IO.Directory.EnumerateFiles(dir, "*.cs", System.IO.SearchOption.TopDirectoryOnly))
        {
            var text = System.IO.File.ReadAllText(file);
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(
                         text, "const string \\w*Measurement\\w*\\s*=\\s*\"([a-z0-9_]+)\"|\\.Measure\\(\"([a-z0-9_]+)\""))
            {
                found.Add(m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value);
            }
        }

        Assert.True(found.Count > 20, $"the label scan found only {found.Count} labels");
        Assert.Empty(found.Except(labels).ToList());
        Assert.Empty(labels.Except(found).ToList());
    }

    [Fact]
    public void TheEngineGateSentence_BecomesOneShortLine_AndOtherTextIsKept()
    {
        var o = Run(
        [
            "example-pg-01 runs PostgreSQL. The pg_cpu_utilization collector does not run on that engine — its own AppliesTo gate excludes it — so this server does not collect the data, and never will. This is a permanent engine capability gap, not a collection outage.",
            "No stored plan for this query. pg_stat_statements has none.",
        ], out var line);
        Assert.Equal(line, o[0]);
        Assert.DoesNotContain("AppliesTo", o[0]);
        Assert.Equal("No stored plan for this query. pg_stat_statements has none.", o[1]);
    }
}

/// <summary>Collection Health on a server without Always On lists the two AG collectors as not applicable.</summary>
public sealed class CollectionHealthAlwaysOnRowsTests
{
    [Theory]
    [InlineData("ag_replica_states", true)]
    [InlineData("ag_database_replica_states", true)]
    [InlineData("wait_stats", false)]
    public void OnlyTheTwoAlwaysOnCollectors_AreAlwaysOnCollectors(string name, bool expected) =>
        Assert.Equal(expected, PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.IsAlwaysOnCollector(name));

    [Fact]
    public void ANotApplicableRow_SaysWhy_WithoutAGateName()
    {
        Assert.Contains("not enabled on this server", PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffMessage);
        Assert.DoesNotContain("needs a look", PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffMessage);
        Assert.Empty(PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffRows([]));
    }

    private static PerformanceMonitor.Darling.Service.Mcp.CollectorHealth AgRow(string name, bool failing)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new PerformanceMonitor.Darling.Service.Mcp.CollectorHealth
        {
            CollectorName = name,
            TotalRuns = 10,
            SuccessCount = failing ? 0 : 10,
            ErrorCount = failing ? 10 : 0,
            LastSuccessTime = failing ? null : last,
            LastRunTime = last,
            LastError = failing ? "Msg 1234: the AG DMV read failed" : null,
        };
    }

    [Fact]
    public void AHealthyAgCollector_IsReplacedByTheNotApplicableRow()
    {
        var healthy = AgRow("ag_replica_states", failing: false);
        var rows = new[] { healthy, AgRow("wait_stats", failing: false) };

        Assert.Equal("HEALTHY", healthy.HealthStatus);
        Assert.Equal(new[] { "wait_stats" }, PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.RowsShownWhenAlwaysOnOff(rows).Select(r => r.CollectorName));
        Assert.Single(PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffRows(rows));
    }

    [Fact]
    public void AFailingAgCollector_KeepsItsLogRowAndItsLastError()
    {
        /* Round-1 L5: a HADR-off server whose AG collector is actually failing used to lose the row, and with it the
           Last Error, behind the not-applicable sentence. */
        var failing = AgRow("ag_database_replica_states", failing: true);
        var rows = new[] { failing, AgRow("ag_replica_states", failing: false) };

        Assert.NotEqual("HEALTHY", failing.HealthStatus);
        var shown = PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.RowsShownWhenAlwaysOnOff(rows);
        Assert.Equal(new[] { "ag_database_replica_states" }, shown.Select(r => r.CollectorName));
        Assert.Equal("Msg 1234: the AG DMV read failed", shown[0].LastError);
        var notApplicable = PerformanceMonitor.Darling.Service.Mcp.DarlingGatedCollectorRows.AlwaysOnOffRows(rows);
        Assert.Single(notApplicable);
        Assert.Contains("ag_replica_states", System.Text.Json.JsonSerializer.Serialize(notApplicable));
        Assert.DoesNotContain("ag_database_replica_states", System.Text.Json.JsonSerializer.Serialize(notApplicable));
    }
}
