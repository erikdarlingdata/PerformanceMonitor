/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The drawn deadlock graph for the Darling web (#5246): the shared <see cref="DeadlockGraphParser"/> and
/// <see cref="DeadlockGraphLayout"/> turn one deadlock's XML into process cards with positions and waits-for
/// edges, and this adds that shape to each <c>get_deadlock_detail</c> row as <c>graph</c>. The browser draws the
/// SVG from it and never parses the XML.
///
/// <para>This lives outside <c>Mcp/</c> on purpose. It is added only by the web dispatch row, so the MCP tool's
/// payload, its response budget, its tools/list entry and its Lite parity do not change.</para>
/// </summary>
public static class DarlingWebDeadlockGraph
{
    /// <summary>A deadlock with more processes than this is not drawn; the row says how many there were. Each
    /// card carries up to <see cref="SqlTextCap"/> characters of statement, and the layout grows with the count.</summary>
    public const int MaxGraphProcesses = 60;

    /// <summary>Statement text sent per process for the side panel; a longer one is cut and says so.</summary>
    public const int SqlTextCap = 8000;

    /// <summary>The graph for one deadlock's XML, or null when the XML holds no process (empty, unparseable, or
    /// not a deadlock graph). A deadlock over <see cref="MaxGraphProcesses"/> returns only
    /// <c>too_large</c> and <c>process_count</c>.</summary>
    public static Dictionary<string, object?>? BuildGraph(string? xml)
    {
        var model = DeadlockGraphParser.Parse(xml);
        if (model.IsEmpty) return null;

        if (model.Processes.Count > MaxGraphProcesses)
        {
            return new Dictionary<string, object?>
            {
                ["too_large"] = true,
                ["process_count"] = model.Processes.Count,
            };
        }

        var (width, height) = DeadlockGraphLayout.Layout(model);

        var processes = model.Processes.Select(p =>
        {
            var node = new Dictionary<string, object?>
            {
                ["id"] = p.Id,
                ["spid"] = p.Spid,
                ["ecid"] = p.Ecid,
                ["victim"] = p.IsVictim,
                ["x"] = Math.Round(p.X, 1),
                ["y"] = Math.Round(p.Y, 1),
                ["wait_time_ms"] = p.WaitTimeMs,
                ["priority"] = p.Priority,
            };
            Put(node, "lock_mode", p.LockMode);
            Put(node, "wait_resource", p.WaitResource);
            Put(node, "contended_object", p.ContentiousObject);
            Put(node, "database_name", p.DatabaseName);
            Put(node, "login_name", p.LoginName);
            Put(node, "host_name", p.HostName);
            Put(node, "client_app", p.ClientApp);
            Put(node, "isolation_level", p.IsolationLevel);
            Put(node, "proc_name", p.ProcName);
            Put(node, "status", p.Status);
            if (p.SqlText.Length > SqlTextCap)
            {
                var cut = McpHelpers.TextElementCutLength(p.SqlText, SqlTextCap);
                node["sql_text"] = p.SqlText[..cut];
                node["sql_text_cut"] = true;
            }
            else
            {
                Put(node, "sql_text", p.SqlText);
            }
            return node;
        }).ToList();

        var edges = model.Edges.Select(e =>
        {
            var edge = new Dictionary<string, object?>
            {
                ["waiter"] = e.WaiterProcessId,
                ["owner"] = e.OwnerProcessId,
                ["self"] = e.IsSelfEdge,
            };
            Put(edge, "resource_kind", e.ResourceKind);
            Put(edge, "resource_label", e.ResourceLabel);
            Put(edge, "request_mode", e.RequestMode);
            Put(edge, "owner_mode", e.OwnerMode);
            return edge;
        }).ToList();

        var cycles = model.ComponentBoxes.Select(b => new Dictionary<string, object?>
        {
            ["index"] = b.Index,
            ["node_count"] = b.NodeCount,
            ["x"] = Math.Round(b.X, 1),
            ["y"] = Math.Round(b.Y, 1),
            ["width"] = Math.Round(b.Width, 1),
            ["height"] = Math.Round(b.Height, 1),
        }).ToList();

        return new Dictionary<string, object?>
        {
            ["width"] = Math.Round(width, 1),
            ["height"] = Math.Round(height, 1),
            ["node_width"] = DeadlockGraphLayout.NodeWidth,
            ["node_height"] = DeadlockGraphLayout.NodeHeight,
            ["is_parallel"] = model.IsParallel,
            ["processes"] = processes,
            ["edges"] = edges,
            ["cycles"] = cycles,
        };
    }

    /// <summary>Adds <c>graph</c> to each row of a <c>get_deadlock_detail</c> payload that carries a whole graph
    /// XML. Anything that is not that payload (a status envelope, an error, text that is not JSON) comes back
    /// unchanged, as does a row whose XML was only a preview or does not parse.</summary>
    public static string AddGraphs(string json)
    {
        if (string.IsNullOrEmpty(json)) return json;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            return json;
        }

        if (root is not JsonObject obj || obj["deadlocks"] is not JsonArray rows) return json;

        var changed = false;
        foreach (var node in rows)
        {
            if (node is not JsonObject row) continue;
            if (row["deadlock_graph_xml_truncated"] is JsonValue t && t.TryGetValue<bool>(out var cut) && cut) continue;
            if (row["deadlock_graph_xml"] is not JsonValue v || !v.TryGetValue<string>(out var xml) || string.IsNullOrWhiteSpace(xml)) continue;

            var graph = BuildGraph(xml);
            if (graph is null) continue;
            row["graph"] = JsonSerializer.SerializeToNode(graph, McpHelpers.JsonOptions);
            changed = true;
        }

        return changed ? obj.ToJsonString(McpHelpers.JsonOptions) : json;
    }

    private static void Put(Dictionary<string, object?> map, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value)) map[key] = value;
    }
}
