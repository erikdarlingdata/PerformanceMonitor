/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6292: the web's "Block chain" read, <c>get_block_chain</c>. Web only, with no MCP tool behind it (see
/// <see cref="DarlingWebEndpoints.WebOnlyReadNames"/>): the Blocking page's row action asks for the lead-blocker tree of
/// one blocked-process row. The tree is the Viewer's own, built by the shared reconstruction (<see cref="PgBlockChainRead"/>);
/// this class only keys the read by the row and writes the model as JSON.
/// </summary>
internal static class DarlingWebBlockChain
{
    /// <summary>The longest statement a node carries; a longer one is cut and flagged (the page's other blocking reads cut at the same length).</summary>
    public const int SqlTextLength = DarlingMcpBlockingTools.WebSqlTextPreviewLength;

    public static async Task<string> GetBlockChain(
        NpgsqlDataSource postgres, string? serverName, string eventTime, int blockedSpid, int blockedEcid, CancellationToken cancellationToken)
    {
        /* The row's event_time exactly as get_blocking printed it ("o", UTC, microseconds kept); the store is matched for equality. */
        if (!DarlingMcpPlanTools.TryParseCollectionTime(eventTime, out var eventTimeUtc))
            return McpHelpers.Refusal("event_time", "Expected the event_time exactly as get_blocking returned it (ISO 8601, UTC).");

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, cancellationToken);
        if (error != null) return error;

        try
        {
            var model = await PgBlockChainRead.ReadModelAsync(
                postgres, resolved.ServerId, eventTimeUtc, blockedSpid, blockedEcid, McpCommandDeadlines.ReadSeconds, cancellationToken);

            if (model == null || model.Roots.Count == 0)
                return McpHelpers.Status("empty",
                    $"No blocking chain could be rebuilt for session {blockedSpid} near this report: its blocking did not appear in any captured " +
                    "blocked-process report or DMV blocking snapshot within five minutes of it.");

            return new JsonObject
            {
                ["status"] = "ok",
                ["server"] = resolved.DisplayName,
                ["event_time"] = eventTimeUtc.ToString("o", CultureInfo.InvariantCulture),
                ["session_id"] = blockedSpid,
                ["session_ecid"] = blockedEcid,
                ["cycle_detected"] = model.CycleDetected,
                ["depth_capped"] = model.DepthCapped,
                ["traversal_truncated"] = model.TraversalTruncated,
                ["chain"] = Node(model.Roots[0]),
            }.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_block_chain", ex);
        }
    }

    /// <summary>One session of the tree and, under it, the sessions it blocks. Empty text is null, as the page's other reads send it.</summary>
    internal static JsonObject Node(BlockingChainNode n)
    {
        var children = new JsonArray();
        foreach (var child in n.Children)
            children.Add(Node(child));

        return new JsonObject
        {
            ["spid"] = n.Spid,
            ["ecid"] = n.Ecid,
            ["is_apex"] = n.IsApex,
            ["is_apex_sleeping"] = n.IsApexSleeping,
            ["wait_time_ms"] = n.WaitTimeMs,
            ["lock_mode"] = Text(n.LockMode),
            ["database_name"] = Text(n.DatabaseName),
            ["login_name"] = Text(n.LoginName),
            ["host_name"] = Text(n.HostName),
            ["client_app"] = Text(n.ClientApp),
            ["contentious_object"] = Text(n.ContentiousObject),
            ["tran_started"] = n.TranStarted?.ToString("o", CultureInfo.InvariantCulture),
            ["sql_text"] = Text(McpHelpers.TruncateStatement(n.SqlText, SqlTextLength)),
            ["sql_text_truncated"] = n.SqlText.Length > SqlTextLength,
            ["children"] = children,
        };
    }

    private static string? Text(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
