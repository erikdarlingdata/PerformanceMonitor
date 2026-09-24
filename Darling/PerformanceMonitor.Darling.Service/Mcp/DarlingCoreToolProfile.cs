/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The <c>/core</c> URL-path profile (#3898 D6/D7, Erik's ruling): Darling-only — Lite gets no profiles — and a
/// REAL subset. A <c>tools/call</c> for a tool outside this set fails on <c>/core</c> exactly as it would for an
/// unknown tool name, not merely hidden from <c>tools/list</c> (see <c>DarlingMcpHostService</c>'s
/// <c>ConfigureSessionOptions</c>, which replaces the per-request <c>McpServerOptions.ToolCollection</c> with
/// this closure only when the request path is <c>/core</c>). The default <c>/</c> path is unchanged and keeps
/// serving every tool, for good.
///
/// <para>The closure: the four entry tools an agent actually starts a conversation from
/// (<see cref="EntryTools"/>), plus every tool name a <c>next_tools</c> recommendation can point at, computed
/// from the SAME tables that build those recommendations (<see cref="ToolRecommendations.AllToolNames"/>,
/// <see cref="PgTargetToolRecommendations.AllToolNames"/>,
/// <see cref="DarlingMcpQueryStoreClutterTools.FixedNextToolNames"/>) rather than hand-kept, so a lane that adds
/// a fact-key row widens <c>/core</c> automatically instead of quietly falling outside it.
/// <c>DarlingCoreToolProfileTests</c> pins the exact set and its count. Erik's ruling: ship <c>/core</c> only
/// while the count sits inside [70, 90] — recompute and report under Handoff, don't ship, if it ever falls
/// outside that band.</para>
/// </summary>
internal static class DarlingCoreToolProfile
{
    /// <summary>The four tools a fresh conversation starts from: the fleet/server discovery pair, the
    /// analysis engine's own entry point, and the read that tells an agent what exists at all.</summary>
    internal static readonly string[] EntryTools =
    [
        "list_servers",
        "get_fleet_overview",
        "analyze_server",
        "get_tool_guide"
    ];

    /// <summary>The closure, computed to a fixed point (see <see cref="Compute"/>). Ordinal, order not
    /// meaningful.</summary>
    internal static IReadOnlySet<string> Closure { get; } = Compute();

    private static HashSet<string> Compute()
    {
        /* Every current next_tools source names its tools from a fact key or a fixed table, never from "which
           tool is this reader in" — so nothing a first pass adds can unlock a tool a second pass would newly
           see today. The do/while below still runs a REAL fixed-point loop rather than a single union, so a
           future source that DOES chain recommendations off a tool's own identity converges here instead of
           silently stopping one hop short. */
        var nameable = new HashSet<string>(StringComparer.Ordinal);
        nameable.UnionWith(ToolRecommendations.AllToolNames);
        nameable.UnionWith(PgTargetToolRecommendations.AllToolNames);
        nameable.UnionWith(DarlingMcpQueryStoreClutterTools.FixedNextToolNames);

        var closure = new HashSet<string>(EntryTools, StringComparer.Ordinal);
        bool added;
        do
        {
            added = false;
            foreach (var tool in nameable)
            {
                if (closure.Add(tool))
                    added = true;
            }
        } while (added);

        return closure;
    }

    /// <summary>Whether a request path is the <c>/core</c> mapping — the ONE condition
    /// <c>DarlingMcpHostService</c>'s <c>ConfigureSessionOptions</c> callback branches on. Segment-matched
    /// (<c>/core</c> and <c>/core/</c>, not <c>/corex</c>) the same way ASP.NET Core routing itself matches
    /// <c>MapMcp("/core")</c>.</summary>
    internal static bool IsCorePath(PathString path) => path.StartsWithSegments("/core");

    /// <summary>The note a <c>/core</c> session's instructions lead with. The shared instructions count and
    /// describe every tool on <c>/</c>, and <c>get_tool_guide</c> reads a catalog of every tool too, so without
    /// it an agent on <c>/core</c> is told about tools that fail here as unknown.</summary>
    internal static string CoreNote =>
        $"## This is the /core endpoint\n\nIt serves {Closure.Count} of this server's tools: {string.Join(", ", EntryTools)}, " +
        "plus every tool an analysis finding's next_tools can name. The tool count and the tool notes below " +
        "describe the full set on /. A tool this endpoint does not serve fails as an unknown tool, even though " +
        "get_tool_guide can still describe it. Connect to / for the rest, including every tool that writes to " +
        "the monitoring store.";

    /// <summary>The instructions a <c>/core</c> session serves: <see cref="CoreNote"/>, then the full set's
    /// instructions unchanged.</summary>
    internal static string CoreInstructions(string? fullInstructions) =>
        string.IsNullOrEmpty(fullInstructions) ? CoreNote : CoreNote + "\n\n" + fullInstructions;

    /// <summary>
    /// Narrows a session's served tools to the closure — the ENTIRE mechanism behind <c>/core</c> being a real
    /// subset rather than a <c>tools/list</c> filter. The returned collection is a fresh
    /// <see cref="McpServerPrimitiveCollection{T}"/> containing only the closure's tools; a tool left out is
    /// not merely unlisted, it is ABSENT from the collection the SDK's own <c>tools/call</c> dispatch consults
    /// before falling back to any handler — and Darling registers no fallback <c>CallToolHandler</c>, so a call
    /// naming an excluded tool gets the SDK's "unknown tool" error, identical to a made-up tool name.
    /// </summary>
    internal static McpServerPrimitiveCollection<McpServerTool> FilterToolCollection(
        McpServerPrimitiveCollection<McpServerTool>? allTools)
    {
        var coreTools = new McpServerPrimitiveCollection<McpServerTool>();

        if (allTools is not null)
        {
            foreach (var tool in allTools)
            {
                if (Closure.Contains(tool.ProtocolTool.Name))
                    coreTools.Add(tool);
            }
        }

        return coreTools;
    }
}
