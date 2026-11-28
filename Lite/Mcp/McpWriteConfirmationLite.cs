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
/// Asks the user to confirm Lite's destructive write tools before they run (#9562): <c>delete_mute_rule</c>, <c>delete_notification_route</c>,
/// <c>delete_query_tag</c>, <c>delete_query_tag_rule</c>, <c>reset_collector_schedules</c> and <c>remove_server</c>. The flow is the shared
/// <see cref="McpWriteConfirmation"/> (the same one Darling's tools use), so the rules are the same: ask only when the client negotiated
/// the multi round-trip flow and declares elicitation, count a yes only on a call that echoes the question's <c>requestState</c> for the
/// same tool and arguments (once), answer <c>status: declined</c> as a normal result on no or a dismissed question, and run as before
/// for every other client. <c>delete_query_tag</c> keeps its own <c>confirm_required</c> step for a tag with children, assignments or
/// rules: a yes to the question does not set its <c>confirm</c>. The write gates
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
            ["delete_notification_route"] = a => $"Delete notification route {Q(a, "route_id")}? Its saved URLs and key are removed, and its alerts fall to the next matching route or the parent channels.",
            ["delete_query_tag"] = a => $"Delete query tag {Q(a, "tag_id")} and its child tags?",
            ["delete_query_tag_rule"] = a => $"Delete query tag rule {Q(a, "rule_id")}? Queries it tagged lose that tag at the next map pass.",
            ["reset_collector_schedules"] = a => Has(a, "server_name")
                ? $"Reset the collector schedule of server {Q(a, "server_name")} back to the default schedule?"
                : "Reset the fleet-wide default collector schedule back to the shipped defaults?",
            ["remove_server"] = a => $"Remove the monitored server {Q(a, "server_name")}? Lite stops collecting from it, and its saved login, alert state and tag assignments are dropped. Collected history is kept.",
        };

    /// <summary>The tools that ask, by name.</summary>
    internal static IReadOnlyCollection<string> ToolNames { get; } = Describers.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The call-tool filter.</summary>
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> Filter { get; } = McpWriteConfirmation.Create(
        ToolNames,
        (request, arguments, _) => Task.FromResult<Question?>(Describers[request.Params!.Name](arguments) is { } text ? new Question(text) : null),
        _ => false);
}
