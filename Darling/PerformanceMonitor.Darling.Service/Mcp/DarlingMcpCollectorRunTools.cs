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
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The run-collectors-now write tool (#5941): queues a one-time collector pass for one monitored server through the same
/// guarded request the web dashboard's buttons use (<see cref="SnapshotNowRequests"/>), waits a short while for the service
/// to finish it, and returns the result the service wrote. Served under <c>mcp.allowWrites</c> like the other write tools. The
/// <c>mcp</c> role holds no table privilege on the command queue; it queues through <c>config.request_snapshot_now</c>, which
/// fixes the command type and accepts nothing but a server id and collector names.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCollectorRunTools
{
    /// <summary>How long the tool waits for the pass before it answers <c>running</c>.</summary>
    internal static readonly TimeSpan WaitBudget = TimeSpan.FromSeconds(45);

    internal static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    [McpServerTool(Name = "run_collectors_now"), Description(
        "Runs collectors ONCE, now, for one monitored server, instead of waiting for their schedule. No collector list = every ENABLED collector that applies to the server's engine. A list of collector names runs only those, each even if disabled (its enabled state and schedule do not change). A collector that does not apply to the server's engine is reported as skipped, never run. Guards: refused (not queued) while a run for that server is queued or going, and within 60 seconds of the last one (the refusal says when the next is allowed); runs share the fleet's sweep slots, so it can wait behind other collection. Runs even while the service is paused. Use it to check collection right after installing an extension or changing a setting; the daily collectors (index_object_stats) and pg_buffer_usage put real load on the server, so name collectors when one is enough. Returns status queued/succeeded/failed/running/refused with collectors_run, rows and skipped; on running (a long pass), read get_collection_health or get_collection_log later.")]
    public static async Task<string> RunCollectorsNow(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string server_name,
        [Description("Optional collector names (see get_collection_health). Omit to run every enabled collector.")] string[]? collectors = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken, NameClash.Refuse);
            if (error is not null)
            {
                return error;
            }

            return await RunCollectorsNowCore(
                new SnapshotNowRequests(new PostgresSnapshotNowStore(postgres, resultThroughMcpFunction: true)),
                resolved.ServerId, resolved.DisplayName, collectors, "mcp", WaitBudget, PollInterval, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("run_collectors_now", ex);
        }
    }

    /// <summary>The tool's body over the request seam, so a test drives it with a fake store and a test clock.</summary>
    internal static async Task<string> RunCollectorsNowCore(
        SnapshotNowRequests requests, int serverId, string serverName, IReadOnlyList<string>? collectors, string requestedBy,
        TimeSpan waitBudget, TimeSpan pollInterval, CancellationToken cancellationToken)
    {
        var outcome = await requests.RequestAsync(serverId, collectors, requestedBy, cancellationToken);
        if (!outcome.Accepted)
        {
            var refusal = new JsonObject
            {
                ["status"] = "refused",
                ["reason"] = outcome.Code,
                ["message"] = outcome.Text,
                ["server"] = serverName,
            };
            if (outcome.NextAllowedUtc is { } next)
            {
                refusal["next_allowed_utc"] = next.ToString("o", CultureInfo.InvariantCulture);
            }

            return refusal.ToJsonString();
        }

        var state = await requests.WaitForResultAsync(outcome.CommandId!.Value, waitBudget, pollInterval, cancellationToken);
        var node = new JsonObject
        {
            ["status"] = state is not null && state.Status is "succeeded" or "failed" ? state.Status : "running",
            ["server"] = serverName,
            ["command_id"] = outcome.CommandId,
        };
        if (state?.ResultJson is not null)
        {
            try
            {
                node["result"] = JsonNode.Parse(state.ResultJson);
            }
            catch (JsonException)
            {
                /* The service wrote it; if it is not JSON the status alone is the answer. */
            }
        }

        if (node["status"]!.GetValue<string>() == "running")
        {
            node["message"] = "The run is still going. Read get_collection_health or get_collection_log for the newest result.";
        }

        return node.ToJsonString();
    }
}
