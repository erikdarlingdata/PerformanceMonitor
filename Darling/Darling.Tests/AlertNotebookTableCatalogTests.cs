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
using System.Threading;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every table an alert notebook template emits draws its rows. A template's read cell names the read and
/// <c>viz: "table"</c>; the page resolves <c>rowsKey</c> and <c>columns</c> from the shared table catalog
/// (<c>read-fields.js</c>), so each read the templates emit needs an entry whose keys exist in the read's payload.
/// The reads are found in the template sources; the catalog is read by running the shipped JavaScript under Node
/// (reported as skipped when Node is not installed).
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

    /// <summary>Runs the shipped page script under Node and returns what it printed; the test is reported as SKIPPED
    /// (never a silent pass) when Node is not on the machine. The CI runners are windows-latest images, which carry Node.</summary>
    internal static JsonElement RunNode(string script, params string[] args)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", script));
        foreach (var arg in args) psi.ArgumentList.Add(arg);

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
                Assert.Fail("the page script " + script + " did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, script + " failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    internal static JsonElement TryRun(string scenario)
    {
        return RunNode("alert-notebook-harness.mjs", PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "views.js"), scenario);
    }

    private static string ReadFieldsJs => PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "read-fields.js");

    /// <summary>What the page's real <c>resolveReadTable</c> gives each whole cell (params included, so a catalog choice that
    /// depends on the cell's params is exercised).</summary>
    private static JsonElement[] ResolveCells(IEnumerable<JsonObject> cells)
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, new JsonArray(cells.Select(c => (JsonNode?)c.DeepClone()).ToArray()).ToJsonString());
            return RunNode("read-fields-resolve-harness.mjs", ReadFieldsJs, file).GetProperty("resolved").EnumerateArray().ToArray();
        }
        finally
        {
            File.Delete(file);
        }
    }

    private static JsonObject ReadCellOf(string viz, string read, params (string Key, string Value)[] parameters)
    {
        var p = new JsonObject();
        foreach (var (k, v) in parameters) p[k] = v;
        return new JsonObject { ["type"] = "read", ["read"] = read, ["viz"] = viz, ["title"] = read, ["params"] = p };
    }

    private static JsonObject[] ResolveNames(string[] reads) => reads.Select(r => ReadCellOf("table", r)).ToArray();

    private const string MatchedWaitType = "RESOURCE_SEMAPHORE";
    private const string MatchedCollector = "wait_stats_collector";

    /// <summary>A matched alert-history row whose persisted context carries a RESOURCE_SEMAPHORE wait type and a collector
    /// name, so the builders that emit extra reads only for such a row emit them.</summary>
    private static DarlingAlertReader.AlertHistoryReadRow MatchedRow(string metric, DateTime at)
    {
        var context = new AlertContext { WaitType = MatchedWaitType, CollectorName = MatchedCollector };
        return new DarlingAlertReader.AlertHistoryReadRow(
            at, 1, "SRV1", metric, 1, 1, true, "alert", null, false, null, false, AlertContextSerializer.Serialize(context));
    }

    /// <summary>EVERY <c>type:"read"</c> cell any alert notebook can carry: each exact-name authored template and each prefix
    /// template, built once with no matched row and once with a matched row (RESOURCE_SEMAPHORE, a collector name); and the
    /// mechanical notebook (<c>BuildCellsAsync</c>'s fallback, from <c>SectionsFor</c>) for every <c>SectionsByMetric</c> key
    /// that has no authored template, plus the default sections.</summary>
    private static List<(string Origin, JsonObject Cell)> EveryReadCell()
    {
        var end = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var cells = new List<(string, JsonObject)>();

        void Add(string origin, JsonArray built)
        {
            foreach (var cell in built.OfType<JsonObject>().Where(c => (string?)c["type"] == "read")) cells.Add((origin, cell));
        }

        void Authored(AlertNotebookEndpoint.AuthoredTemplateEntry entry, string metric, AlertNotebookEndpoint.AuthoredContext context)
        {
            var incident = new AlertIncident("dedup-key", new[] { "obj1" }, Database: "SalesDb");
            Add(metric + " (no row)", entry.Invoke(metric, "SRV1", "2026-01-01T12:00:00Z", end.AddHours(-24), end, incident, null, "Unknown", context));
            Add(metric + " (matched row)", entry.Invoke(metric, "SRV1", "2026-01-01T12:00:00Z", end.AddHours(-24), end, incident, MatchedRow(metric, end), "Unknown", context));
        }

        foreach (var metric in AlertNotebookEndpoint.s_authoredTemplates.SelectMany(row => row.Metrics))
        {
            var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
            Assert.NotNull(template);
            Authored(template!.Value, metric, AlertNotebookEndpoint.AuthoredContext.Empty);
        }

        foreach (var (prefix, _, entry) in AlertNotebookEndpoint.s_authoredPrefixTemplates)
        {
            Authored(entry, prefix + "sample", AlertNotebookEndpoint.AuthoredContext.Empty);
        }

        var mechanicalMetrics = DarlingTriageEndpoint.SectionsByMetric.Keys
            .Where(m => AlertNotebookEndpoint.ResolveAuthored(m) is null)
            .Append("No Such Metric (default sections)")
            .ToList();
        foreach (var metric in mechanicalMetrics)
        {
            Assert.Null(AlertNotebookEndpoint.ResolveAuthored(metric));
            var (built, _, _) = AlertNotebookEndpoint.BuildCellsAsync(
                metric, "SRV1", "2026-01-01T12:00:00Z", end, 1, end, null!, null!, null, null, null, null, "Unknown", "24",
                CancellationToken.None).GetAwaiter().GetResult();
            Add("mechanical: " + metric, built);
        }

        return cells;
    }

    private static string FieldArrayFor(string viz) => viz switch { "line" => "series", "stat" => "stats", _ => "columns" };

    private static bool HasNoFields(JsonElement d) =>
        d.GetProperty("rowsKey").ValueKind == JsonValueKind.Null && d.GetProperty("viz").GetString() != "stat"
        || d.GetProperty(FieldArrayFor(d.GetProperty("viz").GetString()!)).GetArrayLength() == 0;

    [Fact]
    public void EveryReadCellEveryNotebookEmits_ResolvesToTheFieldArrayItsVizNeeds()
    {
        var all = EveryReadCell();
        Assert.True(all.Count >= 150, "expected the authored, prefix and mechanical notebooks to emit well over 150 read cells, found " + all.Count);
        Assert.Contains(all, c => c.Origin.StartsWith("mechanical: ", StringComparison.Ordinal));
        Assert.Contains(all, c => c.Origin.Contains("(matched row)", StringComparison.Ordinal));

        /* The matched RESOURCE_SEMAPHORE row is what makes the poison-wait notebook emit its wait trend and its two grant
           tables, and the collector-name row is what makes the self-monitor notebook emit its collector cells. */
        var emitted = all.Select(c => (string)c.Cell["viz"]! + "/" + (string)c.Cell["read"]!).ToHashSet();
        Assert.Contains("line/get_wait_trend", emitted);
        Assert.Contains("table/get_resource_semaphore", emitted);
        Assert.Contains("table/get_memory_grants", emitted);
        Assert.Contains("line/get_deadlock_trend", emitted);
        Assert.Contains(all, c => (string)c.Cell["read"]! == "get_collector_cost" && c.Cell["params"]?["collector_name"] is not null);

        var resolved = ResolveCells(all.Select(c => c.Cell));
        Assert.Equal(all.Count, resolved.Length);
        var missing = Enumerable.Range(0, all.Count)
            .Where(i => HasNoFields(resolved[i]))
            .Select(i => all[i].Origin + " -> " + resolved[i].GetProperty("viz").GetString() + "/" + resolved[i].GetProperty("read").GetString())
            .Distinct()
            .ToArray();
        Assert.True(missing.Length == 0, "read cells whose viz gets no fields from the shared catalog: " + string.Join("; ", missing));
    }

    [Fact]
    public void ACollectorCostCell_ResolvesToTheFleetPartWithoutACollectorName_AndThePerCollectorPartWithOne()
    {
        var resolved = ResolveCells(new[]
        {
            ReadCellOf("table", "get_collector_cost", ("days_back", "7")),
            ReadCellOf("table", "get_collector_cost", ("days_back", "7"), ("collector_name", MatchedCollector)),
        });

        /* Without a collector name the tool answers with every collector (collectors[]); with one, that collector's days (trend[]). */
        Assert.Equal("collectors", resolved[0].GetProperty("rowsKey").GetString());
        Assert.Contains("collector_name", resolved[0].GetProperty("columns").EnumerateArray().Select(k => k.GetString()));
        Assert.Equal("trend", resolved[1].GetProperty("rowsKey").GetString());
        Assert.Contains("day", resolved[1].GetProperty("columns").EnumerateArray().Select(k => k.GetString()));
    }

    /// <summary>Source that builds a read's payload outside its tool method: the shared payload class for the trend reads
    /// (the file, then the method whose body is the payload).</summary>
    private static readonly Dictionary<string, (string[] Folder, string File, string Method)[]> s_sharedBuilders = new(StringComparer.Ordinal)
    {
        ["get_wait_trend"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "WaitTrend") },
        ["get_lock_wait_trend"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "LockWaitTrend") },
        ["get_cpu_utilization"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "CpuUtilization") },
        ["get_tempdb_trend"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "TempDbTrend") },
        ["get_memory_trend"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "MemoryTrend") },
        ["get_perfmon_trend"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "PerfmonTrend") },
        ["get_file_io_trend"] = new[] { (new[] { "PerformanceMonitor.Common", "Mcp" }, "TrendPayloads.cs", "FileIoTrend") },
    };

    /// <summary>Reads whose payload is a typed class serialized with <c>[JsonPropertyName]</c> names, not an anonymous object:
    /// the file that declares the class.</summary>
    private static readonly Dictionary<string, string> s_payloadTypeFiles = new(StringComparer.Ordinal)
    {
        ["get_ag_health"] = "DarlingAgReader.cs",
    };

    /// <summary>A key the payload code spells through a shared constant rather than a literal: the constant it uses.</summary>
    private static readonly Dictionary<string, string> s_keyConstants = new(StringComparer.Ordinal)
    {
        ["get_database_sizes.size_note"] = "AzureSiblingDatabaseSize.RowNoteKey",
    };

    /// <summary>The text from <paramref name="start"/> to the next marker (or the end).</summary>
    private static string SliceTo(string text, int start, string nextMarker)
    {
        var next = text.IndexOf(nextMarker, start + 1, StringComparison.Ordinal);
        return next < 0 ? text.Substring(start) : text.Substring(start, next - start);
    }

    /// <summary>The source that builds one read's payload: its own tool method (from its tool attribute to the next one), plus
    /// the shared builder methods the read is mapped to.</summary>
    private static string PayloadSourceFor(string read, Dictionary<string, string> toolFiles)
    {
        var marker = "[McpServerTool(Name = \"" + read + "\"";
        var homes = toolFiles.Values.Where(v => v.Contains(marker, StringComparison.Ordinal)).ToList();
        Assert.True(homes.Count == 1, read + ": expected one MCP tool file declaring the read, found " + homes.Count);
        var source = SliceTo(homes[0], homes[0].IndexOf(marker, StringComparison.Ordinal), "[McpServerTool(");
        if (s_sharedBuilders.TryGetValue(read, out var builders))
        {
            foreach (var (folder, file, method) in builders)
            {
                var text = File.ReadAllText(PathTo(folder.Append(file).ToArray()));
                var at = text.IndexOf("public static string " + method + "(", StringComparison.Ordinal);
                Assert.True(at >= 0, read + ": shared builder " + method + " not found in " + file);
                source += SliceTo(text, at, "\n    public static ");
            }
        }

        if (s_payloadTypeFiles.TryGetValue(read, out var typeFile))
        {
            source += File.ReadAllText(PathTo(s_serviceDir.Append("Mcp").Append(typeFile).ToArray()));
        }

        return source;
    }

    [Fact]
    public void EveryFieldKeyInTheCatalog_IsAFieldTheReadsOwnPayloadBuilds()
    {
        var catalog = TryRun("catalog").GetProperty("catalog");
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

            /* Every part of the entry (table, line, stat, and any alternative part such as a fleet-wide table). */
            foreach (var part in entry.Value.EnumerateObject().Where(p => p.Value.ValueKind == JsonValueKind.Object))
            {
                Collect(part.Value);
            }

            var source = PayloadSourceFor(read, toolFiles);
            foreach (var key in keys.Distinct())
            {
                var assignment = new Regex("\\b" + Regex.Escape(key) + "\\s*=(?!=)|\\bAS\\s+" + Regex.Escape(key) + "\\b|\\[\"" + Regex.Escape(key) + "\"\\]|JsonPropertyName\\(\"" + Regex.Escape(key) + "\"\\)");
                var viaConstant = s_keyConstants.TryGetValue(read + "." + key, out var constant) && source.Contains(constant, StringComparison.Ordinal);
                if (!assignment.IsMatch(source) && !viaConstant) bad.Add(read + "." + key);
            }
        }

        Assert.True(bad.Count == 0, "catalog keys the read's own payload code never builds: " + string.Join(", ", bad));
    }

    [Fact]
    public void ACellOfEachViz_ReachesThePanelRendererWithItsFields()
    {
        var wanted = new[] { "table/get_blocking", "line/get_deadlock_trend", "stat/get_cpu_scheduler_pressure" };
        var r = TryRun("draw:" + string.Join(",", wanted));
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
        /* The panel object ends at the brace that closes the one the read sits in, found by counting braces, not by an indent. */
        var open = js.LastIndexOf('{', at);
        var depth = 0;
        var end = -1;
        for (var i = open; i < js.Length && end < 0; i++)
        {
            if (js[i] == '{') depth++;
            else if (js[i] == '}' && --depth == 0) end = i;
        }

        Assert.True(end > at, read + ": the panel's closing brace was not found");
        Assert.DoesNotContain("columns:", js.Substring(open, end - open));
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
    public void EveryTableReadATemplateSourceNames_ResolvesToARowsKeyAndColumns()
    {
        var reads = TemplateTableReads();
        var resolved = ResolveCells(ResolveNames(reads));

        var missing = resolved
            .Where(d => d.GetProperty("rowsKey").ValueKind == JsonValueKind.Null || d.GetProperty("columns").GetArrayLength() == 0)
            .Select(d => d.GetProperty("read").GetString())
            .ToArray();

        Assert.True(missing.Length == 0, "table reads with no rowsKey or columns in the shared catalog: " + string.Join(", ", missing));
    }

    [Fact]
    public void TheAlertNotebookPageHandsEachBareTableCell_ItsRowsKeyAndColumns()
    {
        var reads = new[] { "get_blocking", "get_top_queries_by_cpu", "get_wait_stats", "get_pg_replication_slots", "get_collection_log" };
        var r = TryRun("draw:" + string.Join(",", reads));

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
