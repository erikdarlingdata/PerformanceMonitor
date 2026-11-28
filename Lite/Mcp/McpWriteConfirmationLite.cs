/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using static PerformanceMonitor.Common.McpWriteConfirmation;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Asks the user to confirm Lite's two destructive write tools before they run (#9562): <c>delete_mute_rule</c> and
/// <c>remove_server</c>. The flow is the shared <see cref="McpWriteConfirmation"/> (the same one Darling's tools use), so the rules
/// are the same: ask only when the client negotiated the multi round-trip flow and declares elicitation, answer <c>status: declined</c>
/// as a normal result on no or a dismissed question, and run as before for every other client. The write gates
/// (<c>mcp_allow_writes</c>, <c>mcp_allow_server_admin</c>) still decide whether a tool is served at all, and a tool that is not
/// served never asks.
///
/// <para>Lite has no agent-action calls (no <c>agent_action</c> argument, no token that is not the user's), and a call that reaches a
/// served write tool is made with the one token the user set, so no call is skipped as an agent's.</para>
/// </summary>
internal static class McpWriteConfirmationLite
{
    private static readonly IReadOnlyDictionary<string, Func<IDictionary<string, JsonElement>, string?>> Describers =
        new Dictionary<string, Func<IDictionary<string, JsonElement>, string?>>(StringComparer.Ordinal)
        {
            ["delete_mute_rule"] = a => $"Delete mute rule {Q(a, "rule_id")}? Alerts it held back are delivered again.",
            ["remove_server"] = a => $"Remove the monitored server {Q(a, "server_name")}? Lite stops collecting from it, and its saved login, alert state and tag assignments are dropped. Collected history is kept.",
        };

    /// <summary>The tools that ask, by name.</summary>
    internal static IReadOnlyCollection<string> ToolNames { get; } = Describers.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The call-tool filter.</summary>
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> Filter { get; } = McpWriteConfirmation.Create(
        ToolNames,
        (request, arguments, _) => Task.FromResult(Describers[request.Params!.Name](arguments)),
        _ => false);
}
