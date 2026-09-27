/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Availability Group health MCP tool (#991) — the AG topology across the whole monitored fleet in one call.
/// It reads through the SHARED <see cref="DarlingAgReader"/>, the SAME reader (and the SAME banding) that powers
/// the web dashboard's <c>/api/ag</c> and Availability Groups page, so an MCP client and a browser see one
/// consistently-banded topology. A STORED read (no live monitored-server hit).
///
/// <para>Each result group is ONE MONITORED SERVER'S VIEW of one AG. Because every replica of an AG reports the
/// whole AG's replica set, an AG whose replicas are all monitored appears once per monitored replica — deliberately
/// not merged, since the perspectives genuinely differ (several columns are populated only for the LOCAL replica).
/// The group's <c>server_name</c> names whose view it is.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpAgTools
{
    /// <summary>
    /// #4471: the fleet-wide page cap on groups (one card per reporting server's view of one AG), the same shape
    /// as <c>get_analysis_findings</c>' <c>limit</c>. An uncapped fleet-wide call on a production fleet (43
    /// servers, several AGs with many databases) measured 265,794 characters — over 8x the shared
    /// <see cref="McpResponseBudget.DefaultBytes"/> (32 KB). On a 43-server/3-AG/2-replica/3-database fixture
    /// (~2.7 KB/group, close to that reported shape), 11 groups measured 30,115 bytes and 12 measured 32,812 —
    /// so 11 is the largest default that stays under budget on that shape; see <c>DarlingMcpAgToolsTests</c> for
    /// the before/after this was set from.
    /// </summary>
    public const int DefaultGroupLimit = 11;

    [McpServerTool(Name = "get_ag_health"), Description(
        "AG health fleet-wide from each server's latest collection: replica role/state and per-database " +
        "secondary state (queue KB, rate KB/s, lag sec, drain min, suspended+why). One row per REPLICA's view: " +
        "a multi-replica AG appears once per replica, not merged. Severities restate DMV verdicts only, " +
        "un-banded on lag/queue depth; lag reads 0 while suspended, so check secondary_lag_seconds and " +
        "is_suspended yourself. collection_time can be stale after an AG is dropped. Empty: none collected " +
        "fleet-wide or on the server. Scoped to a server whose engine never runs AG collection: not_collected. " +
        "<<GUIDE>> Gets Always On Availability Group health across the monitored fleet from the latest collection per " +
        "server: every AG with its replicas (role, connected/operational state, synchronization health, " +
        "availability and failover mode, endpoint) and its per-database secondary state (synchronization state, " +
        "log-send and redo queue sizes in KB, send/redo rates in KB/s, estimated drain minutes, secondary lag " +
        "seconds, and whether data movement is suspended and why). Each group is one monitored server's VIEW of " +
        "an AG and names that server, so an AG with several monitored replicas appears once per replica — compare " +
        "them to reconcile perspectives (operational_state and recovery_health are populated only for the LOCAL " +
        "replica, connected_state only from the primary — the perspectives genuinely differ by design). " +
        "Severities are computed server-side and restate the DMVs' OWN verdicts " +
        "(states and health strings) only: lag and queue depth are NOT banded, so a badly lagging asynchronous " +
        "secondary whose replica health still reads HEALTHY carries a healthy severity — read secondary_lag_seconds " +
        "and the queue sizes yourself, and read lag together with is_suspended (the DMV reports 0 lag while data " +
        "movement is suspended). Each group carries its collection_time: the collectors write NO row for a server " +
        "with no AGs, so a server whose AGs were dropped keeps returning its last non-empty snapshot until then — " +
        "an old collection_time on a group is that case, not a live reading. Returns an empty result on a fleet " +
        "with no Availability Groups. limit pages the groups, MOST SEVERE FIRST then by the largest " +
        "secondary_lag_seconds/queue depth in the group, so the cap never hides a problem — an uncapped fleet-wide " +
        "call measured 265,794 characters on a 43-server production fleet with several many-database AGs, well " +
        "over an MCP client's typical per-result limit. Default 11 groups; groups_truncated (with " +
        "groups_truncated_note) flags when the scope held more than that — groups_total says how many, " +
        "groups_returned says how many came back, and the fix is to scope by server_name (one server's view is " +
        "rarely more than a handful of groups) or raise limit for the rest.")]
    public static async Task<string> GetAgHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name to limit the topology to one monitored server's view. Optional — omit for the whole fleet.")] string? server_name = null,
        [Description("Maximum groups to return, most severe first, then by the largest lag/queue depth in the group. Default 11, range 1-1000. groups_truncated flags a cut here.")] int limit = DefaultGroupLimit,
        CancellationToken cancellationToken = default)
    {
        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        /* Fleet-wide by default: only resolve when a name was actually supplied. The shared resolver auto-selects
           a sole registered server for an omitted name, which is right for a per-server tool and wrong here — it
           would silently narrow the fleet view on a one-server store. */
        int? serverIdFilter = null;
        string? resolvedName = null;
        if (!string.IsNullOrWhiteSpace(server_name))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error != null) return error;
            serverIdFilter = resolved.ServerId;
            resolvedName = resolved.ServerName;
        }

        try
        {
            var result = await DarlingAgReader.GetAgHealthAsync(postgres, serverIdFilter, cancellationToken: cancellationToken, limit: limit);

            if (result.AvailabilityGroupCount == 0)
            {
                /* #2511: only when the caller SCOPED to one server, because engine edition is per server and
                   the fleet-wide read has no single engine to speak for. A fleet-wide miss keeps the general
                   sentence below, which already says the AG collectors do not run on Azure SQL Database. */
                if (serverIdFilter is int scopedServerId && resolvedName is not null)
                {
                    var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, scopedServerId, resolvedName, "ag_replica_states", cancellationToken);
                    if (gated != null)
                    {
                        return gated;
                    }
                }

                /* The message names the RESOLVED storage name, not the caller's spelling — one condition, and a
                   partial or differently-cased argument reads back as the server it actually matched. */
                return McpHelpers.Status(
                    "empty",
                    resolvedName is null
                        ? "No Availability Groups have been collected from any monitored server. The AG collectors write no rows for an instance that hosts none, and they do not run against Azure SQL Database."
                        : $"No Availability Groups have been collected from {resolvedName}. The AG collectors write no rows for an instance that hosts none, and they do not run against Azure SQL Database.");
            }

            return JsonSerializer.Serialize(result, DarlingAgReader.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_ag_health", ex);
        }
    }
}
