/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for CONNECTED replica health, paired with the <c>pg_replication_stats</c> collector
/// (#2629).
///
/// <para>
/// The other half of <c>get_pg_replication_slots</c>, and the two answer opposite questions. A slot is
/// what the primary RETAINS on a replica's behalf and exists whether or not anybody is attached — a slot
/// with no connection is exactly the shape of the disk-filling incident that motivates monitoring it at
/// all. This reads <c>pg_stat_replication</c>, which is the live connections: who is attached right now,
/// how far behind, and by how much time.
/// </para>
///
/// <para>
/// <b>The worst-in-window figures are the ones to read.</b> A sample of replication lag catches whatever
/// the replica happened to be doing at that instant, and lag is spiky by nature — the peak is the fact
/// that matters, so it travels beside the latest value rather than being averaged into invisibility.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgReplicationStatsTools
{
    [McpServerTool(Name = "get_pg_replication_stats"), Description("Gets the health of CONNECTED PostgreSQL replicas from pg_stat_replication: which replicas are attached, their state and sync state, how many bytes behind they are on send and on replay, and replay lag in milliseconds - with the WORST value seen in the window beside the latest, because lag is spiky and the peak is the fact that matters. This is the counterpart of get_pg_replication_slots and answers a different question: a slot describes what the primary RETAINS for a replica and exists even when nothing is attached, which is the disk-filling case; this describes replicas that are actually connected. A replica that disappears from this tool while its slot persists is the dangerous combination. sync_state distinguishes synchronous replicas, where lag is also commit latency on the primary, from asynchronous ones where it is not. Rows are SAMPLED, so a replica that connected and left between captures may not appear.")]
    public static async Task<string> GetPgReplicationStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgReplicationStatsReader.GetPgReplicationStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_replication_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No replica was connected to {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). On a server with no replicas that is the expected answer. If a "
                        + "replica is SUPPOSED to be attached, check get_pg_replication_slots — a slot "
                        + "that persists with nothing connected to it retains WAL indefinitely, which is "
                        + "the case worth acting on.");
            }

            var replicas = rows.Select(r => new
            {
                application_name = r.ApplicationName,
                client_addr = r.ClientAddr,
                state = r.State,
                /* Synchronous lag is also commit latency on the primary; asynchronous lag is not. The
                   same number means two different things depending on this column. */
                sync_state = r.SyncState,
                sent_bytes_behind = r.SentBytesBehind,
                replay_bytes_behind = r.ReplayBytesBehind,
                worst_replay_bytes_behind = r.WorstReplayBytesBehind,
                replay_lag_ms = r.ReplayLagMs,
                worst_replay_lag_ms = r.WorstReplayLagMs,
                samples = r.Samples,
                total_samples = r.TotalSamples,
                backend_start = r.BackendStart,
                last_seen = r.LastSeen,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                replica_count = rows.Count,
                note = "Read the worst_* columns, not just the latest: lag is spiky and a sample catches "
                     + "one instant. `samples` against `total_samples` says how much of the window each "
                     + "replica was actually connected for — a replica present in only a few captures "
                     + "reconnected repeatedly, which the latest lag figure alone would hide.",
                replicas,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL replication stats failed: {ex.Message}");
        }
    }
}
