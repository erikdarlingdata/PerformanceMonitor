// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

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
/// PostgreSQL deadlocks (#2661) — the reports themselves, read out of the server log, rather than the count
/// <c>pg_stat_database</c> keeps.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgDeadlockTools
{
    [McpServerTool(Name = "get_pg_deadlocks"), Description("Gets PostgreSQL deadlocks that were reported in the window, newest first, with the victim process, how many sessions were in the cycle, the lock modes and resources involved, and the victim's full statement text. PostgreSQL writes a complete deadlock report to its server log unconditionally - there is no setting that suppresses it - so this needs nothing configured on the target, unlike plan capture. Each row is one DISTINCT deadlock: the collector re-reads an overlapping tail of the log every cycle on purpose, so the same report is seen several times, and times_seen reports that rather than hiding it. A deadlock that genuinely recurred appears as a separate row, because the participating process IDs differ. Use get_pg_deadlock_detail with a deadlock_hash for the full wait graph and every participant's SQL.")]
    public static async Task<string> GetPgDeadlocks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum deadlocks to return. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return McpHelpers.Status("error", limitError);

        try
        {
            var rows = await DarlingPgDeadlockReader.GetDeadlocksAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_deadlocks")
                    ?? McpHelpers.Status(
                        "no_deadlocks",
                        $"No deadlock was reported on {resolved.ServerName} in the last {hours_back} "
                        + "hour(s). Two different things produce that and they are worth telling apart: the "
                        + "server had no deadlocks, which is the healthy answer; or the log could not be "
                        + "read, which get_pg_plan_capture_readiness reports on because plan capture reads "
                        + "the same file the same way. pg_stat_database's cumulative deadlock counter, in "
                        + "get_pg_database_stats, is the independent check - if it moved and nothing is "
                        + "here, the log is the problem rather than the server.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "deadlocks",
                deadlock_count = rows.Count,
                truncated = rows.Count >= limit,
                note = "occurred_at is when PostgreSQL wrote the report, not when the collector found it. "
                     + "times_seen counts how often the collector saw the SAME report while it stayed inside "
                     + "the log tail it re-reads - it is a property of the read window and not a repeat "
                     + "deadlock, which would appear as its own row with different process IDs.",
                deadlocks = rows.Select(r => new
                {
                    occurred_at = r.OccurredAtUtc,
                    deadlock_hash = r.DeadlockHash,
                    /* The session PostgreSQL cancelled. It is the end whose application saw an error, which
                       is usually the only end anybody noticed. */
                    victim_pid = r.VictimPid,
                    participant_count = r.ParticipantCount,
                    lock_modes = r.LockModes,
                    resources = r.Resources,
                    victim_statement = r.VictimStatement,
                    times_seen = r.TimesSeen,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL deadlocks failed: {ex.Message}");
        }
    }

    [McpServerTool(Name = "get_pg_deadlock_detail"), Description("Gets one PostgreSQL deadlock in full, by the deadlock_hash that get_pg_deadlocks returns: the complete wait graph as PostgreSQL wrote it, naming every participant, the lock each was waiting for, who blocked whom, and each participant's entire statement text. This carries MORE than a SQL Server deadlock graph does - PostgreSQL names the SQL of every session in the cycle, where the SQL Server graph often leaves the non-victim side as a handle. The graph is stored verbatim rather than reassembled, so a lock type the parser does not break out is still readable here.")]
    public static async Task<string> GetPgDeadlockDetail(
        NpgsqlDataSource postgres,
        [Description("The deadlock_hash from get_pg_deadlocks.")] string deadlock_hash,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        if (string.IsNullOrWhiteSpace(deadlock_hash))
        {
            return McpHelpers.Status(
                "error",
                "deadlock_hash is required. Call get_pg_deadlocks first - each row carries the hash that "
                + "identifies it here.");
        }

        try
        {
            var row = await DarlingPgDeadlockReader.GetDeadlockDetailAsync(
                postgres, resolved.ServerId, deadlock_hash.Trim());

            if (row is null)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No deadlock with hash '{deadlock_hash}' is stored for {resolved.ServerName}. The hash "
                    + "identifies one report on one server, so a hash from a different server will not "
                    + "resolve here - and a report can age out of retention while a hash you are holding "
                    + "does not.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                status = "deadlock_detail",
                deadlock_hash = deadlock_hash.Trim(),
                occurred_at = row.Value.OccurredAtUtc,
                victim_pid = row.Value.VictimPid,
                participant_count = row.Value.ParticipantCount,
                lock_modes = row.Value.LockModes,
                resources = row.Value.Resources,
                note = "graph is PostgreSQL's own DETAIL block, verbatim apart from stripped tab indenting. "
                     + "It reads as: one line per wait edge naming who waits for what and who blocks them, "
                     + "then each participant's process ID followed by its full statement.",
                graph = row.Value.GraphText,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL deadlock failed: {ex.Message}");
        }
    }
}
