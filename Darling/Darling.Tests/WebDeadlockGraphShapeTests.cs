/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The drawn-graph shape the web <c>get_deadlock_detail</c> row adds (#5246): the shared parser and layout, shaped
/// per row, with the rest of the payload left exactly as the tool sent it. No store.
/// </summary>
public sealed class WebDeadlockGraphShapeTests
{
    private static string ReadSource(string relative) => ReadRepoFile(relative.Split('/'));

    private static string Fixture(string name) => DeadlockGraphParserTests.LoadFixture(name);

    private static string Payload(params (string Xml, bool Truncated)[] rows)
    {
        var arr = new JsonArray();
        foreach (var (xml, truncated) in rows)
        {
            var row = new JsonObject { ["deadlock_time"] = "2026-01-01T00:00:00Z", ["victim_process_id"] = "p1", ["deadlock_graph_xml"] = xml };
            if (truncated) row["deadlock_graph_xml_truncated"] = true;
            arr.Add(row);
        }
        return new JsonObject { ["server_name"] = "s", ["deadlocks"] = arr }.ToJsonString();
    }

    private static JsonObject GraphOf(string json, int index = 0) =>
        (JsonObject)((JsonObject)JsonNode.Parse(json)!["deadlocks"]!.AsArray()[index]!)["graph"]!;

    [Theory]
    [InlineData("deadlock_2proc_real_sql2025.xml")]
    [InlineData("deadlock_5proc_real_sql2025.xml")]
    [InlineData("deadlock_8proc_multivictim_real_sql2025.xml")]
    [InlineData("deadlock_parallel_selfedge_synthetic.xml")]
    public void TheGraph_MatchesTheSharedParseAndLayout(string fixture)
    {
        var xml = Fixture(fixture);
        var model = DeadlockGraphParser.Parse(xml);
        var (w, h) = DeadlockGraphLayout.Layout(model);

        var g = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((xml, false))));

        Assert.Equal(model.Processes.Count, g["processes"]!.AsArray().Count);
        Assert.Equal(model.Edges.Count, g["edges"]!.AsArray().Count);
        Assert.Equal(model.ComponentBoxes.Count, g["cycles"]!.AsArray().Count);
        Assert.Equal(Math.Round(w, 1), g["width"]!.GetValue<double>());
        Assert.Equal(Math.Round(h, 1), g["height"]!.GetValue<double>());
        Assert.Equal(DeadlockGraphLayout.NodeWidth, g["node_width"]!.GetValue<double>());
        Assert.Equal(model.IsParallel, g["is_parallel"]!.GetValue<bool>());

        var drawn = g["processes"]!.AsArray().Cast<JsonObject>().ToDictionary(p => p["id"]!.GetValue<string>());
        foreach (var p in model.Processes)
        {
            var n = drawn[p.Id];
            Assert.Equal(Math.Round(p.X, 1), n["x"]!.GetValue<double>());
            Assert.Equal(Math.Round(p.Y, 1), n["y"]!.GetValue<double>());
            Assert.Equal(p.Spid, n["spid"]!.GetValue<int>());
            Assert.Equal(p.IsVictim, n["victim"]!.GetValue<bool>());
        }

        var edges = g["edges"]!.AsArray().Cast<JsonObject>().ToList();
        for (var i = 0; i < model.Edges.Count; i++)
        {
            Assert.Equal(model.Edges[i].WaiterProcessId, edges[i]["waiter"]!.GetValue<string>());
            Assert.Equal(model.Edges[i].OwnerProcessId, edges[i]["owner"]!.GetValue<string>());
            Assert.Equal(model.Edges[i].IsSelfEdge, edges[i]["self"]!.GetValue<bool>());
            Assert.Equal(model.Edges[i].RequestMode, edges[i]["request_mode"]?.GetValue<string>() ?? "");
            Assert.Equal(model.Edges[i].OwnerMode, edges[i]["owner_mode"]?.GetValue<string>() ?? "");
        }
    }

    [Fact]
    public void EveryVictim_IsFlagged_AndTheTwoProcessLocksAreReported()
    {
        var multi = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((Fixture("deadlock_8proc_multivictim_real_sql2025.xml"), false))));
        var model = DeadlockGraphParser.Parse(Fixture("deadlock_8proc_multivictim_real_sql2025.xml"));
        var flagged = multi["processes"]!.AsArray().Cast<JsonObject>().Count(p => p["victim"]!.GetValue<bool>());
        Assert.True(model.Processes.Count(p => p.IsVictim) > 1);
        Assert.Equal(model.Processes.Count(p => p.IsVictim), flagged);

        var two = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((Fixture("deadlock_2proc_real_sql2025.xml"), false))));
        var e = two["edges"]!.AsArray().Cast<JsonObject>().First(x => x["resource_label"]!.GetValue<string>() == "PAGE hammerdb_tpcc.dbo.new_order");
        Assert.False(string.IsNullOrEmpty(e["request_mode"]!.GetValue<string>()));
        Assert.False(string.IsNullOrEmpty(e["owner_mode"]!.GetValue<string>()));
        Assert.Equal("hammerdb_tpcc.dbo.neword", two["processes"]!.AsArray().Cast<JsonObject>().First(p => p["victim"]!.GetValue<bool>())["proc_name"]!.GetValue<string>());
    }

    [Fact]
    public void TheParallelSelfEdge_IsMarkedSelf()
    {
        var g = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((Fixture("deadlock_parallel_selfedge_synthetic.xml"), false))));
        Assert.Contains(g["edges"]!.AsArray().Cast<JsonObject>(), e => e["self"]!.GetValue<bool>());
        Assert.True(g["is_parallel"]!.GetValue<bool>());
    }

    [Fact]
    public void AValueTheGraphDidNotCarry_IsLeftOffRatherThanSentEmpty()
    {
        var g = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((Fixture("deadlock_2proc_real_sql2025.xml"), false))));
        foreach (var p in g["processes"]!.AsArray().Cast<JsonObject>())
            foreach (var kv in p.Where(kv => kv.Value is JsonValue v && v.TryGetValue<string>(out _)))
                Assert.False(string.IsNullOrEmpty(kv.Value!.GetValue<string>()), kv.Key);
    }

    [Fact]
    public void MalformedXml_AndATruncatedRow_GetNoGraph_AndAreLeftAlone()
    {
        var json = Payload(("<deadlock><oops", false), ("", false), (Fixture("deadlock_2proc_real_sql2025.xml").Substring(0, 200), true));
        Assert.Equal(json, DarlingWebDeadlockGraph.AddGraphs(json));

        var mixed = DarlingWebDeadlockGraph.AddGraphs(Payload(("<deadlock><oops", false), (Fixture("deadlock_2proc_real_sql2025.xml"), false)));
        var rows = JsonNode.Parse(mixed)!["deadlocks"]!.AsArray();
        Assert.Null(rows[0]!["graph"]);
        Assert.NotNull(rows[1]!["graph"]);
    }

    [Fact]
    public void ADeadlockOverTheCap_IsCountedNotDrawn()
    {
        var sb = new StringBuilder("<deadlock><victim-list><victimProcess id=\"p0\"/></victim-list><process-list>");
        for (var i = 0; i <= DarlingWebDeadlockGraph.MaxGraphProcesses; i++)
            sb.Append("<process id=\"p").Append(i).Append("\" spid=\"").Append(50 + i).Append("\" waitresource=\"KEY: 5:1 (a)\" lockMode=\"X\"><executionStack/><inputbuf>select ").Append(i).Append("</inputbuf></process>");
        sb.Append("</process-list><resource-list/></deadlock>");

        var g = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((sb.ToString(), false))));
        Assert.True(g["too_large"]!.GetValue<bool>());
        Assert.Equal(DarlingWebDeadlockGraph.MaxGraphProcesses + 1, g["process_count"]!.GetValue<int>());
        Assert.Null(g["processes"]);
    }

    [Fact]
    public void ALongStatement_IsCutAndSaysSo()
    {
        var xml = Fixture("deadlock_2proc_real_sql2025.xml");
        var model = DeadlockGraphParser.Parse(xml);
        var text = model.Processes[0].SqlText;
        var padded = new string('x', DarlingWebDeadlockGraph.SqlTextCap + 50);
        var longXml = xml.Replace(text, padded, StringComparison.Ordinal);
        Assert.NotEqual(xml, longXml);

        var g = GraphOf(DarlingWebDeadlockGraph.AddGraphs(Payload((longXml, false))));
        var cut = g["processes"]!.AsArray().Cast<JsonObject>().Single(p => p["sql_text_cut"] is not null);
        Assert.True(cut["sql_text_cut"]!.GetValue<bool>());
        Assert.Equal(DarlingWebDeadlockGraph.SqlTextCap, cut["sql_text"]!.GetValue<string>().Length);
    }

    [Theory]
    [InlineData("{\"status\":\"unavailable\",\"message\":\"No deadlocks\"}")]
    [InlineData("{\"status\":\"error\",\"message\":\"boom\"}")]
    [InlineData("not json at all")]
    [InlineData("[1,2]")]
    [InlineData("")]
    public void AnEnvelope_OrAnError_PassesThroughUnchanged(string json) =>
        Assert.Equal(json, DarlingWebDeadlockGraph.AddGraphs(json));

    [Fact]
    public void EveryOtherKeyOnEveryRow_IsUnchanged()
    {
        var before = Payload((Fixture("deadlock_2proc_real_sql2025.xml"), false), (Fixture("deadlock_5proc_real_sql2025.xml"), false));
        var after = DarlingWebDeadlockGraph.AddGraphs(before);

        var a = JsonNode.Parse(before)!.AsObject();
        var b = JsonNode.Parse(after)!.AsObject();
        Assert.Equal(a["server_name"]!.ToJsonString(), b["server_name"]!.ToJsonString());
        for (var i = 0; i < 2; i++)
        {
            var ra = a["deadlocks"]![i]!.AsObject();
            var rb = b["deadlocks"]![i]!.AsObject();
            foreach (var kv in ra) Assert.Equal(kv.Value!.ToJsonString(), rb[kv.Key]!.ToJsonString());
            Assert.Equal(ra.Count + 1, rb.Count);
        }
    }

    [Fact]
    public void OnlyTheWebRowAddsTheGraph_TheToolAndItsPayloadAreUntouched()
    {
        var web = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs");
        Assert.Contains("DarlingWebDeadlockGraph.AddGraphs(await DarlingMcpBlockingTools.GetDeadlockDetail(", web, StringComparison.Ordinal);

        var tool = ReadSource("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs");
        Assert.DoesNotContain("DeadlockGraphLayout", tool, StringComparison.Ordinal);
        Assert.DoesNotContain("DarlingWebDeadlockGraph", tool, StringComparison.Ordinal);
        Assert.False(File.Exists(PathTo("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebDeadlockGraph.cs")), "the web graph shaping lives outside Mcp/");
    }

    [Fact]
    public void TheGraphScript_NeverParsesMarkup_AndIsWiredIntoTheGrid()
    {
        var js = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "deadlock-graph.js");
        foreach (var banned in new[] { "innerHTML", "insertAdjacentHTML", "DOMParser", "outerHTML" })
            Assert.DoesNotContain(banned, js, StringComparison.Ordinal);
        Assert.DoesNotContain("from \"../charts.js\"", js, StringComparison.Ordinal);

        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains("from \"./deadlock-graph.js\"", tabs, StringComparison.Ordinal);
        Assert.Contains("{ key: \"graph\"", tabs, StringComparison.Ordinal);
    }
}
