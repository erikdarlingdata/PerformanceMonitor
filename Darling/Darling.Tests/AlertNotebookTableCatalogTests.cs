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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every table an alert notebook template emits draws its rows. A template's read cell names the read and
/// <c>viz: "table"</c>; the page resolves <c>rowsKey</c> and <c>columns</c> from the shared table catalog
/// (<c>read-tables.js</c>), so each read the templates emit needs an entry whose keys exist in the read's payload.
/// The reads are found in the template sources; the catalog is read by running the shipped JavaScript under Node
/// (skipped when Node is not installed, the way <see cref="AlertNotebookRenderBehaviourTests"/> does).
/// </summary>
public sealed class AlertNotebookTableCatalogTests
{
    private static readonly string[] s_serviceDir = { "Darling", "PerformanceMonitor.Darling.Service" };

    /// <summary>The reads any template emits as a table: the first argument of each authored read cell call, and
    /// the two templates that write their read name into the cell directly.</summary>
    private static string[] TemplateTableReads()
    {
        var dir = PathTo(s_serviceDir);
        var reads = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var file in Directory.GetFiles(dir, "AlertNotebookEndpoint*.cs"))
        {
            var text = File.ReadAllText(file);
            foreach (Match m in Regex.Matches(text, "(?:AuthoredReadCell|ServerOnlyReadCell)\\(\\s*\"(get_[a-z_0-9]+)\""))
            {
                reads.Add(m.Groups[1].Value);
            }

            foreach (Match m in Regex.Matches(text, "\\[\"read\"\\]\\s*=\\s*\"(get_[a-z_0-9]+)\""))
            {
                reads.Add(m.Groups[1].Value);
            }
        }

        return reads.ToArray();
    }

    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-notebook-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "views.js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the alert notebook harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the alert notebook harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static JsonElement[] Resolve(string[] reads) =>
        TryRun("resolve:" + string.Join(",", reads), out var r)
            ? r.GetProperty("resolved").EnumerateArray().ToArray()
            : Array.Empty<JsonElement>();

    /// <summary>Every (viz, read) pair any authored template emits as a <c>type:"read"</c> cell, from the real builders
    /// (<c>AuthoredReadCell</c>, <c>ServerOnlyReadCell</c>, the self-monitor cells, every inline read cell) for every metric.</summary>
    private static string[] EmittedVizReads()
    {
        var pairs = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var metric in AlertNotebookEndpoint.s_authoredTemplates.SelectMany(row => row.Metrics))
        {
            var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
            Assert.NotNull(template);
            var end = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
            var cells = template!.Value.Invoke(
                metric, "SRV1", "2026-01-01T12:00:00Z", end.AddHours(-24), end,
                new AlertIncident("dedup-key", new[] { "obj1" }, Database: "SalesDb"), null, "Unknown", AlertNotebookEndpoint.AuthoredContext.Empty);
            foreach (var cell in cells.OfType<JsonObject>().Where(c => (string?)c["type"] == "read"))
            {
                pairs.Add((string)cell["viz"]! + "/" + (string)cell["read"]!);
            }
        }

        return pairs.ToArray();
    }

    private static string FieldArrayFor(string viz) => viz switch { "line" => "series", "stat" => "stats", _ => "columns" };

    [Fact]
    public void EveryReadCellEveryTemplateEmits_ResolvesToTheFieldArrayItsVizNeeds()
    {
        var pairs = EmittedVizReads();
        Assert.True(pairs.Length >= 24, "expected the templates to emit about 25 read cells, found " + pairs.Length);
        /* The trend reads are charts: a deadlock or wait trend is a series over time, not a two-column table. */
        Assert.Contains("line/get_deadlock_trend", pairs);
        Assert.Contains("line/get_wait_trend", pairs);

        var resolved = Resolve(pairs);
        if (resolved.Length == 0) return;
        var missing = resolved
            .Where(d => d.GetProperty("rowsKey").ValueKind == JsonValueKind.Null && d.GetProperty("viz").GetString() != "stat"
                || d.GetProperty(FieldArrayFor(d.GetProperty("viz").GetString()!)).GetArrayLength() == 0)
            .Select(d => d.GetProperty("viz").GetString() + "/" + d.GetProperty("read").GetString())
            .ToArray();
        Assert.True(missing.Length == 0, "read cells whose viz gets no fields from the shared catalog: " + string.Join(", ", missing));
    }

    [Fact]
    public void EveryFieldKeyInTheCatalog_IsAFieldTheReadsPayloadBuilds()
    {
        if (!TryRun("catalog", out var r)) return;
        var catalog = r.GetProperty("catalog");
        var mcpDir = PathTo(s_serviceDir.Append("Mcp").ToArray());
        var toolFiles = Directory.GetFiles(mcpDir, "*.cs").ToDictionary(f => f, f => File.ReadAllText(f));
        var bad = new List<string>();
        foreach (var entry in catalog.EnumerateObject())
        {
            var read = entry.Name;
            var keys = new List<string>();
            void Collect(JsonElement e)
            {
                if (e.TryGetProperty("rowsKey", out var rk) && rk.ValueKind == JsonValueKind.String && rk.GetString() != ".") keys.Add(rk.GetString()!);
                if (e.TryGetProperty("xKey", out var xk) && xk.ValueKind == JsonValueKind.String) keys.Add(xk.GetString()!);
                foreach (var arr in new[] { "columns", "series", "stats" })
                {
                    if (e.TryGetProperty(arr, out var a) && a.ValueKind == JsonValueKind.Array)
                    {
                        keys.AddRange(a.EnumerateArray().Select(k => k.GetProperty("key").GetString()!));
                    }
                }
            }

            Collect(entry.Value);
            foreach (var viz in new[] { "line", "stat" })
            {
                if (entry.Value.TryGetProperty(viz, out var sub)) Collect(sub);
            }

            var marker = "Name = \"" + read + "\"";
            var home = toolFiles.Where(kv => kv.Value.Contains(marker, StringComparison.Ordinal)).Select(kv => kv.Value).ToList();
            Assert.True(home.Count == 1, read + ": expected one MCP tool file declaring the read, found " + home.Count);
            foreach (var key in keys.Distinct())
            {
                var assignment = new Regex("\\b" + Regex.Escape(key) + "\\s*=(?!=)|\\bAS\\s+" + Regex.Escape(key) + "\\b");
                var declared = assignment.IsMatch(home[0])
                    || toolFiles.Values.Any(t => t != home[0] && IsPayloadBuilderFor(t, read) && assignment.IsMatch(t));
                if (!declared) bad.Add(read + "." + key);
            }
        }

        Assert.True(bad.Count == 0, "catalog keys the read's payload code never builds: " + string.Join(", ", bad));
    }

    [Fact]
    public void ACellOfEachViz_ReachesThePanelRendererWithItsFields()
    {
        var wanted = new[] { "table/get_blocking", "line/get_deadlock_trend", "stat/get_cpu_scheduler_pressure" };
        if (!TryRun("draw:" + string.Join(",", wanted), out var r)) return;
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
        var drawn = r.GetProperty("reads").EnumerateArray().ToArray();
        Assert.Equal(wanted.Length, drawn.Length);
        foreach (var d in drawn)
        {
            var viz = d.GetProperty("viz").GetString()!;
            var name = viz + "/" + d.GetProperty("read").GetString();
            Assert.True(d.GetProperty(FieldArrayFor(viz)).GetInt32() > 0, name + ": the panel renderer got none of the fields its viz needs");
            if (viz == "line")
            {
                Assert.True(d.GetProperty("rowsKey").ValueKind == JsonValueKind.String && d.GetProperty("xKey").ValueKind == JsonValueKind.String, name + ": a line needs a rowsKey and an xKey");
            }
        }
    }

    [Theory]
    [InlineData("get_blocking")]
    [InlineData("get_top_queries_by_cpu")]
    [InlineData("get_top_procedures_by_cpu")]
    [InlineData("get_wait_stats")]
    [InlineData("get_collection_health")]
    public void TheStarterDashboardPanelOfAReadInTheCatalog_CarriesNoColumnListOfItsOwn(string read)
    {
        var js = File.ReadAllText(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "view-templates.js")).Replace("\r\n", "\n");
        var at = js.IndexOf("read: \"" + read + "\"", StringComparison.Ordinal);
        Assert.True(at > 0, read + " is not a starter dashboard panel");
        var end = js.IndexOf("\n          },", at, StringComparison.Ordinal);
        Assert.DoesNotContain("columns:", js.Substring(at, end - at));
    }

    [Fact]
    public void TheTemplatesEmitTheKnownTableReads()
    {
        var reads = TemplateTableReads();

        Assert.True(reads.Length >= 24, "expected the template sources to name about 25 distinct reads, found " + reads.Length);
        Assert.Contains("get_blocking", reads);
        Assert.Contains("get_collection_log", reads);
        Assert.Contains("get_collector_stall_probes", reads);
        Assert.Contains("get_analysis_findings", reads);
    }

    [Fact]
    public void EveryTableReadATemplateEmits_ResolvesToARowsKeyAndColumns()
    {
        var reads = TemplateTableReads();
        var resolved = Resolve(reads);
        if (resolved.Length == 0) return;

        var missing = resolved
            .Where(d => d.GetProperty("rowsKey").ValueKind == JsonValueKind.Null || d.GetProperty("columns").GetArrayLength() == 0)
            .Select(d => d.GetProperty("read").GetString())
            .ToArray();

        Assert.True(missing.Length == 0, "table reads with no rowsKey or columns in the shared catalog: " + string.Join(", ", missing));
    }

    [Fact]
    public void EveryCatalogColumnKeyAndRowsKey_IsAFieldTheReadsPayloadBuilds()
    {
        var reads = TemplateTableReads();
        var resolved = Resolve(reads);
        if (resolved.Length == 0) return;

        var mcpDir = PathTo(s_serviceDir.Append("Mcp").ToArray());
        var toolFiles = Directory.GetFiles(mcpDir, "*.cs")
            .ToDictionary(f => f, f => File.ReadAllText(f));
        var bad = new List<string>();
        foreach (var d in resolved)
        {
            var read = d.GetProperty("read").GetString()!;
            var keys = d.GetProperty("columns").EnumerateArray().Select(k => k.GetString()!).ToList();
            if (d.GetProperty("rowsKey").ValueKind == JsonValueKind.String && d.GetProperty("rowsKey").GetString() != ".")
            {
                keys.Add(d.GetProperty("rowsKey").GetString()!);
            }

            if (keys.Count == 0) continue;

            var marker = "Name = \"" + read + "\"";
            var home = toolFiles.Where(kv => kv.Value.Contains(marker, StringComparison.Ordinal)).Select(kv => kv.Value).ToList();
            Assert.True(home.Count == 1, read + ": expected one MCP tool file declaring the read, found " + home.Count);

            /* A payload property is spelled `name = ...` in the tool's anonymous object, or as a shared payload
               builder's property in the same folder; the key must appear as one of those, not as a word in prose. */
            foreach (var key in keys.Distinct())
            {
                var assignment = new Regex("\\b" + Regex.Escape(key) + "\\s*=(?!=)|\\bAS\\s+" + Regex.Escape(key) + "\\b");
                var declared = assignment.IsMatch(home[0])
                    || toolFiles.Values.Any(t => t != home[0] && IsPayloadBuilderFor(t, read) && assignment.IsMatch(t));
                if (!declared) bad.Add(read + "." + key);
            }
        }

        Assert.True(bad.Count == 0, "catalog keys the read's payload code never builds: " + string.Join(", ", bad));
    }

    /// <summary>The trend reads build their points in a shared payload class (or a reader's aliased SQL column) rather than in the tool file.</summary>
    private static bool IsPayloadBuilderFor(string fileText, string read) =>
        read.EndsWith("_trend", StringComparison.Ordinal) && fileText.Contains("Trend", StringComparison.Ordinal);

    [Fact]
    public void TheAlertNotebookPageHandsEachBareTableCell_ItsRowsKeyAndColumns()
    {
        var reads = new[] { "get_blocking", "get_top_queries_by_cpu", "get_wait_stats", "get_pg_replication_slots", "get_collection_log" };
        if (!TryRun("draw:" + string.Join(",", reads), out var r)) return;

        Assert.Empty(r.GetProperty("errors").EnumerateArray());
        var drawn = r.GetProperty("reads").EnumerateArray().ToArray();
        Assert.Equal(reads.Length, drawn.Length);
        foreach (var d in drawn)
        {
            var name = d.GetProperty("read").GetString();
            Assert.True(d.GetProperty("rowsKey").ValueKind == JsonValueKind.String, name + ": the panel renderer got no rowsKey");
            Assert.True(d.GetProperty("columns").GetInt32() > 0, name + ": the panel renderer got no columns");
        }
    }
}
