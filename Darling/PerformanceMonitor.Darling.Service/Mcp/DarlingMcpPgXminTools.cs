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
/// The MCP surface for xmin-horizon attribution, paired with the <c>pg_xmin_horizon</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgXminTools
{
    /// <summary>
    /// The remedy for a source, which is the whole reason attribution matters: the four causes look
    /// identical from the symptom side and each needs a different action.
    /// </summary>
    internal static string RemedyFor(string source) => source switch
    {
        "session" =>
            "A backend is holding an old snapshot. Find it in pg_stat_activity by the holder pid. "
            + "'idle in transaction' means the application opened a transaction and stopped using it — fix "
            + "the client, or bound it with idle_in_transaction_session_timeout. A genuinely long-running "
            + "query is a different problem: make it faster rather than killing it blindly.",
        "replication_slot" =>
            "A replication slot is retaining an old xmin. If it is inactive and nothing consumes it, it is "
            + "abandoned and should be dropped — an inactive slot also retains WAL without limit by default "
            + "and can fill the volume. Common orphan sources: a removed CDC task, a finished blue/green "
            + "deployment, or a failed major-version upgrade.",
        "replication_slot_catalog" =>
            "A logical decoding slot is holding catalog_xmin, which blocks catalog cleanup specifically. "
            + "Check whether its consumer is still running and keeping up; a stalled logical subscriber "
            + "produces exactly this.",
        "standby_feedback" =>
            "A standby with hot_standby_feedback=on is reporting its xmin back, so a long-running query on "
            + "the REPLICA is preventing cleanup on the primary. That trade is deliberate — it stops the "
            + "replica's queries being cancelled — so the fix is usually the replica query, not the setting. "
            + "Note this is expected to be absent on Aurora, whose replicas share the storage volume.",
        "prepared_transaction" =>
            "An orphaned prepared (two-phase) transaction. These survive disconnects and restarts and hold "
            + "their snapshot until explicitly resolved: COMMIT PREPARED or ROLLBACK PREPARED by gid. Almost "
            + "always a distributed transaction coordinator that failed mid-protocol.",
        _ => "Unrecognized holder source.",
    };

    [McpServerTool(Name = "get_pg_xmin_horizon"), Description("Gets what is holding back the PostgreSQL xmin horizon, attributed by cause. Use this whenever dead tuples or table bloat are growing while autovacuum appears to be running normally - that symptom has four unrelated causes which look identical from the outside, and each needs a completely different fix: a long-running or idle-in-transaction session, an abandoned replication slot, a logical slot holding catalog_xmin, a standby feeding back its xmin, or an orphaned prepared transaction. Reports the oldest holder for each source, which one is currently winning, and how persistent each has been across the window, so a chronic holder can be told apart from a query that merely ran long. Also relevant to wraparound risk: a pinned horizon blocks freezing, so an unattended holder here is an upstream cause of the risk get_pg_wraparound_risk measures. Works on any PostgreSQL target.")]
    public static async Task<string> GetPgXminHorizon(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze, used for the persistence figures. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingPgXminReader.GetPgXminHorizonAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            /* Nothing holding the horizon is the HEALTHY answer, and saying so plainly matters more here
               than for most tools: an operator arrives at this tool BECAUSE bloat is growing, so "no
               holder" is a real finding that redirects the investigation rather than a dead end. */
            if (rows.Count == 0)
            {
                /* ...but only when this server COULD have a horizon. On a SQL Server target "nothing is
                   holding back the xmin horizon" is a confident all-clear about a mechanism that does not
                   exist there, which is the same defect one engine over (#2532). */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_xmin_horizon");
                if (gated != null)
                {
                    return gated;
                }

                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = "no_holder",
                    finding = "Nothing is holding back the xmin horizon in this window. Vacuum is free to "
                            + "reclaim dead rows, so bloat growth has a different cause — look at whether "
                            + "autovacuum is being triggered at all (per-table thresholds and dead-tuple "
                            + "counts) rather than at whether it is being blocked.",
                }, McpHelpers.JsonOptions);
            }

            var holders = rows.Select(r => new
            {
                source = r.Source,
                is_currently_winning = r.IsWinner,
                xmin_age = r.XminAge,
                holder = r.Holder,
                detail = r.Detail,
                measured_at = r.MeasuredAt,
                peak_xmin_age = r.PeakXminAge,
                /* Persistence, not just presence. A source that won nearly every sample is a standing
                   problem someone must own; one that won twice was a query that ran long and finished. */
                samples_as_winner = r.SamplesAsWinner,
                samples = r.Samples,
                pct_of_window_winning = r.Samples > 0
                    ? Math.Round((double)r.SamplesAsWinner / r.Samples * 100, 1)
                    : 0,
                remedy = RemedyFor(r.Source),
            }).ToList();

            var winner = holders.FirstOrDefault(h => h.is_currently_winning) ?? holders[0];

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "holder_present",
                /* Lead with the actionable pair: which cause, and what to do about that cause. */
                winning_source = winner.source,
                winning_holder = winner.holder,
                winning_xmin_age = winner.xmin_age,
                recommended_action = winner.remedy,
                holders,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL xmin horizon failed: {ex.Message}");
        }
    }
}
