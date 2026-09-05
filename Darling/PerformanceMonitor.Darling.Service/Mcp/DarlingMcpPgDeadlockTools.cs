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
    [McpServerTool(Name = "get_pg_deadlocks"), Description("Gets PostgreSQL deadlocks that were reported in the window, newest first, with the victim process, how many sessions were in the cycle, the lock modes and resources involved, and the victim's full statement text. PostgreSQL writes a complete deadlock report to its server log unconditionally - there is no setting that suppresses it - so this needs nothing configured on the target, unlike plan capture. Each row is one DISTINCT deadlock, and a deadlock that genuinely recurred appears as a separate row because the participating process IDs differ. times_seen counts how often the collector saw that SAME report, and what a value means depends on the transport. Where the collector reads the log file itself it re-reads an overlapping tail every cycle on purpose, so one report is seen several times and times_seen climbs while it stays in the window. On RDS and Aurora the log API is consume-once, so a report is normally seen once and times_seen normally stays 1: there a low value is the ordinary state and NOT a partial count, so do not read 1 as 'seen once so far, expect more'. It is not guaranteed to be 1 there either - the collector holds its resume position in memory, so a restart re-reads a bounded tail, and a window whose write did not land is offered again - so treat times_seen as a sighting count whose meaning depends on the transport, and never as a count of deadlocks on either. Use get_pg_deadlock_detail with a deadlock_hash for the full wait graph and every participant's SQL.")]
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
                     + "times_seen counts how often the collector saw the SAME report, never how many times "
                     + "the deadlock happened - a genuine repeat appears as its own row with different "
                     + "process IDs. What a given value MEANS depends on the transport: reading the log file "
                     + "directly re-reads an overlapping tail, so times_seen climbs while the report stays "
                     + "in the window and is a property of that read window. On RDS and Aurora the log API "
                     + "is consume-once, so times_seen is normally 1 and a low value there is the ordinary "
                     + "state rather than a partial count. It is not guaranteed to be 1 there: the resume "
                     + "position lives in the collector process, so a restart re-reads a bounded tail, and "
                     + "a window whose write did not land is offered again.",
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

    [McpServerTool(Name = "get_pg_deadlock_detail"), Description("Gets PostgreSQL deadlock graphs in full: the complete wait graph as the server wrote it, naming every participant, the lock each was waiting for, who blocked whom, and each participant's entire statement text. Pass a deadlock_hash from get_pg_deadlocks for one specific report, or omit it to get the most recent graphs. This carries MORE than a SQL Server deadlock graph does - PostgreSQL names the SQL of every session in the cycle, where the SQL Server graph often leaves the non-victim side as a handle. The graph is stored verbatim rather than reassembled, so a lock type the parser does not break out separately is still readable here.")]
    public static async Task<string> GetPgDeadlockDetail(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("A deadlock_hash from get_pg_deadlocks. Omit for the most recent graphs.")] string? deadlock_hash = null,
        [Description("Maximum graphs to return when no hash is given. Default 5.")] int limit = 5)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return McpHelpers.Status("error", limitError);

        try
        {
            var rows = await DarlingPgDeadlockReader.GetDeadlockDetailAsync(
                postgres, resolved.ServerId, deadlock_hash, limit);

            if (rows.Count == 0)
            {
                return string.IsNullOrWhiteSpace(deadlock_hash)
                    ? await DarlingEngineCapability.NotCollectedStatusAsync(
                          postgres, resolved.ServerId, resolved.ServerName, "pg_deadlocks")
                      ?? McpHelpers.Status(
                          "empty",
                          $"No deadlock graph is stored for {resolved.ServerName}. Either the server had no "
                          + "deadlocks, which is the healthy answer, or its log could not be read - "
                          + "get_pg_database_stats carries pg_stat_database's cumulative deadlock counter, "
                          + "which tells those apart.")
                    : McpHelpers.Status(
                          "empty",
                          $"No deadlock with hash '{deadlock_hash}' is stored for {resolved.ServerName}. A "
                          + "hash identifies one report on ONE server, so one from a different server will "
                          + "not resolve here - and a report can age out of retention while a hash you are "
                          + "holding does not.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                status = "deadlock_detail",
                graph_count = rows.Count,
                note = "graph is PostgreSQL's own DETAIL block, verbatim apart from stripped tab indenting. "
                     + "It reads as: one line per wait edge naming who waits for what and who blocks them, "
                     + "then each participant's process ID followed by its full statement.",
                deadlocks = rows.Select(r => new
                {
                    deadlock_hash = r.DeadlockHash,
                    occurred_at = r.OccurredAtUtc,
                    victim_pid = r.VictimPid,
                    participant_count = r.ParticipantCount,
                    lock_modes = r.LockModes,
                    resources = r.Resources,
                    graph = r.GraphText,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL deadlock failed: {ex.Message}");
        }
    }
}
