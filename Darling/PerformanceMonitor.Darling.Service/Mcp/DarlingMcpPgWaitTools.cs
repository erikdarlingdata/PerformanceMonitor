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
/// The MCP surface for PostgreSQL wait statistics, paired with the <c>pg_wait_stats</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgWaitTools
{
    [McpServerTool(Name = "get_pg_wait_stats"), Description("Gets the top PostgreSQL wait events aggregated over a time period, for Amazon Aurora PostgreSQL targets. Waits reveal what the database spends time waiting on: IO events point at storage or cache misses, Lock events at blocking between sessions, LWLock at internal contention, and LSN is Aurora's storage-durability wait. Background-worker and client-idle waits are already excluded by the collector, so every row here is real work. Note this is a separate tool from get_wait_stats, which covers SQL Server: PostgreSQL has a two-level type/event taxonomy, no signal-wait concept, and reports in microseconds, so the two cannot share one result shape.")]
    public static async Task<string> GetPgWaitStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 20.")] int limit = 20,
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
            var now = windowEnd;
            var rows = await DarlingPgWaitReader.GetPgWaitStatsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit);

            /* An empty result is genuinely ambiguous here in a way it is not for SQL Server: it means
               either no data in the window, or that this server is not a PostgreSQL target at all. Say
               so, rather than letting a caller read "no waits" as "no waiting". */
            if (rows.Count == 0)
            {
                /* Both halves of the old ambiguity are answerable when the store knows the engine (#2532):
                   a SQL Server target gets the dialect answer, a stock PostgreSQL one gets the Aurora-only
                   answer, and both say the gap is permanent instead of inviting a hunt. So the sentence
                   below is reached in exactly two states — an Aurora target with no rows in the window, or
                   a row whose engine_kind is NULL — and it names those rather than repeating the two the
                   capability answer has already ruled out. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wait_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No PostgreSQL wait data for this server and window. On Aurora, the pg_wait_stats "
                        + "collector may not have completed a cycle yet. Otherwise the store has not "
                        + "recorded this server's engine yet, and a target it cannot classify may not be a "
                        + "PostgreSQL one at all — check list_servers.");
            }

            var totalWaitMs = rows.Sum(r => r.TotalWaitTimeMs);

            /* No Take(limit) — the SQL now applies the cap, so the rows returned ARE the rows asked for. */
            var result = rows.Select(r => new
            {
                wait_type = r.WaitType,
                wait_event = r.WaitEvent,
                total_wait_time_ms = Math.Round(r.TotalWaitTimeMs, 1),
                waits = r.TotalWaits,
                avg_wait_time_ms = Math.Round(r.AvgWaitTimeMs, 3),
                /* Share of the window's total wait time. The absolute figure alone does not say whether
                   an event is the story or a rounding error. */
                pct_of_total_wait = totalWaitMs > 0 ? Math.Round(r.TotalWaitTimeMs / totalWaitMs * 100, 1) : 0,
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_wait_time_ms = Math.Round(totalWaitMs, 1),
                waits = result,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL wait stats failed: {ex.Message}");
        }
    }
}
