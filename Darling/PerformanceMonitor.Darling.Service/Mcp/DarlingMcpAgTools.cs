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
    [McpServerTool(Name = "get_ag_health"), Description(
        "Gets Always On Availability Group health across the monitored fleet from the latest collection per " +
        "server: every AG with its replicas (role, connected/operational state, synchronization health, " +
        "availability and failover mode, endpoint) and its per-database secondary state (synchronization state, " +
        "log-send and redo queue sizes in KB, send/redo rates in KB/s, estimated drain minutes, secondary lag " +
        "seconds, and whether data movement is suspended and why). Each group is one monitored server's VIEW of " +
        "an AG and names that server, so an AG with several monitored replicas appears once per replica — compare " +
        "them to reconcile perspectives. Severities are computed server-side. Returns an empty result on a fleet " +
        "with no Availability Groups.")]
    public static async Task<string> GetAgHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name to limit the topology to one monitored server's view. Optional — omit for the whole fleet.")] string? server_name = null)
    {
        /* Fleet-wide by default: only resolve when a name was actually supplied. The shared resolver auto-selects
           a sole registered server for an omitted name, which is right for a per-server tool and wrong here — it
           would silently narrow the fleet view on a one-server store. */
        int? serverIdFilter = null;
        string? resolvedName = null;
        if (!string.IsNullOrWhiteSpace(server_name))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
            if (error != null) return error;
            serverIdFilter = resolved.ServerId;
            resolvedName = resolved.ServerName;
        }

        try
        {
            var result = await DarlingAgReader.GetAgHealthAsync(postgres, serverIdFilter);

            if (result.AvailabilityGroupCount == 0)
            {
                /* #2511: only when the caller SCOPED to one server, because engine edition is per server and
                   the fleet-wide read has no single engine to speak for. A fleet-wide miss keeps the general
                   sentence below, which already says the AG collectors do not run on Azure SQL Database. */
                if (serverIdFilter is int scopedServerId && resolvedName is not null)
                {
                    var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, scopedServerId, resolvedName, "ag_replica_states");
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
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_ag_health", ex);
        }
    }
}
