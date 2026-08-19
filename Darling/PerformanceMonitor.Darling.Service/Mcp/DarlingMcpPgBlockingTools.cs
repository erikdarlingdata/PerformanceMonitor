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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for PostgreSQL blocking chains, paired with the <c>pg_blocking</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgBlockingTools
{
    /// <summary>
    /// What to do about a root blocker, keyed on the root's own state — which is the reason the collector
    /// captures the blocker's state and not just its pid. Every branch here is a different action, and
    /// picking the wrong one is worse than doing nothing: killing a backend that is mid-query loses work,
    /// and tuning a query that is not running fixes nothing.
    /// </summary>
    internal static string RemedyFor(string? rootState, bool idleInTransaction, long xactDurationMs)
    {
        if (idleInTransaction || string.Equals(rootState, "idle in transaction", StringComparison.Ordinal))
        {
            return "The root is IDLE IN TRANSACTION: it holds locks and is running nothing, so there is no "
                 + "query to tune and no work in progress to protect. This is an application defect — a "
                 + "transaction opened and then left open across think time, a missing commit on an error "
                 + "path, or a connection pool handing back a dirty connection. Find the code path from "
                 + "application_name; bound the class of failure with "
                 + "idle_in_transaction_session_timeout so it cannot recur unbounded.";
        }

        if (string.Equals(rootState, "idle in transaction (aborted)", StringComparison.Ordinal))
        {
            return "The root is IDLE IN TRANSACTION (ABORTED): its transaction already failed and every "
                 + "further statement will error, yet it still holds its locks until the client issues "
                 + "ROLLBACK. Nothing it is doing can succeed, so this is the cheapest root to clear. The "
                 + "client is not handling an error it already received — that is the bug.";
        }

        if (string.Equals(rootState, "active", StringComparison.Ordinal))
        {
            return "The root is ACTIVE — a real query holding locks while it runs. Tune the query or shorten "
                 + "the transaction; killing it discards work and the next execution will block the same way. "
                 + "If the statement is fast but the TRANSACTION is long, the lock is being held across "
                 + "statements, and moving the write later in the transaction is usually the fix."
                 + (xactDurationMs > 0
                     ? $" Its transaction had been open {xactDurationMs} ms when sampled."
                     : string.Empty);
        }

        if (rootState is null)
        {
            return "The root's own state was not captured — it left pg_stat_activity between the blocked "
                 + "backends being read and its own row being looked up, which means the chain resolved on "
                 + "its own. Nothing to act on unless it recurs.";
        }

        return $"The root is in state '{rootState}' while holding locks. Identify it in pg_stat_activity by "
             + "pid and establish what transaction it has open; a root that is neither active nor idle in "
             + "transaction usually means it is waiting on something else — a lock outside this chain, or a "
             + "client that stopped reading.";
    }

    [McpServerTool(Name = "get_pg_blocking"), Description("Gets PostgreSQL blocking chains that were captured for a server, assembled from the stored edge list into one entry per chain with its ROOT blocker identified and attributed. Use this when sessions are timing out, waiting, or piling up on a PostgreSQL target, or to check whether a past slowdown involved lock contention. Reports for each captured chain: the root blocker's pid, state, application, username and query text, how many sessions were behind it in total and directly, how deep the chain went, the longest-waiting victim, and how many separate captures that same backend has been the root of - which distinguishes one stuck session from a recurring pattern. Also returns a specific remedy per root state, because an 'idle in transaction' root is an application defect while an 'active' root is a query-tuning problem and the two need opposite responses. IMPORTANT: this is a periodic SAMPLE, not an event log. Unlike SQL Server's blocked-process report, PostgreSQL records nothing on its own, so blocking shorter than the collection interval is never seen and an empty result means 'none was sampled', not 'none happened' - the capture counts in the response say how many samples the window actually contains. Works on any PostgreSQL target including standbys.")]
    public static async Task<string> GetPgBlocking(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum chains to return, worst-first by victim count. Default 50.")] int limit = 50)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        var limitValidation = McpHelpers.ValidateTop(limit);
        if (limitValidation != null) return limitValidation;

        try
        {
            var now = DateTime.UtcNow;
            var startUtc = now.AddHours(-hours_back);

            var chains = await DarlingPgBlockingReader.GetPgBlockingChainsAsync(
                postgres, resolved.ServerId, startUtc, now, limit);

            /* The denominator comes first because it is what makes an empty answer honest. */
            var captures = await DarlingPgBlockingReader.GetPgBlockingCaptureCountsAsync(
                postgres, resolved.ServerId, startUtc, now);

            /* Cycles are read separately and MUST be, because the chain query structurally cannot see them:
               it finds a root by absence, and in a cycle every participant is blocked. Without this the
               tool would report "no blocking" from a capture that recorded a deadlock. */
            var cycles = await DarlingPgBlockingReader.GetPgBlockingCyclesAsync(
                postgres, resolved.ServerId, startUtc, now, limit);

            var cycleEntries = cycles.Select(c => new
            {
                captured_at = c.CapturedAt,
                participant_count = c.ParticipantCount,
                pids = c.Pids,
                database = c.DatabaseName,
                application = c.ApplicationName,
                /* Sessions queued behind the deadlock without being part of it. Previously invisible to
                   BOTH reads — chains cannot see them (no cycle member qualifies as a root) and the cycle
                   walk cannot either (their walks never close). This count is usually what decides
                   urgency: a two-way deadlock is a bug, a two-way deadlock with forty sessions behind it
                   is an outage. */
                blocked_behind_count = c.BlockedBehindCount,
                blocked_behind_pids = c.BlockedBehindPids,
                finding =
                    "These backends were each waiting on a lock held by another member of the same set — a "
                    + "genuine cycle, which is a deadlock. PostgreSQL's deadlock detector resolves it after "
                    + "deadlock_timeout by killing one participant, so this capture landed inside that "
                    + "window and is likely the only record that will ever exist. The fix is ordering: make "
                    + "every code path acquire these objects in the same sequence.",
            }).ToList();

            if (chains.Count == 0 && cycleEntries.Count > 0)
            {
                /* Cycles but no chains. Reporting "no blocking sampled" here would be a flat lie. */
                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = "cycles_only",
                    captures_total = captures.CapturesTotal,
                    captures_with_blocking = captures.CapturesWithBlocking,
                    finding =
                        "Blocking was captured, but every participant was itself blocked — a lock cycle "
                        + "(deadlock) rather than a chain with a root. There is no root blocker to name, "
                        + "which is why these are reported separately.",
                    cycles = cycleEntries,
                }, McpHelpers.JsonOptions);
            }

            if (chains.Count == 0)
            {
                /* Two very different empty answers, and conflating them would be the whole failure mode of
                   a sampled signal. No captures at all means the collector never ran — nothing is known
                   about this window either way. Captures with no blocking is a real all-clear, bounded by
                   the sampling interval. */
                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = captures.CapturesTotal == 0 ? "not_sampled" : "no_blocking_sampled",
                    captures_total = captures.CapturesTotal,
                    captures_with_blocking = 0,
                    first_capture_at = captures.FirstCaptureAt,
                    last_capture_at = captures.LastCaptureAt,
                    finding = captures.CapturesTotal == 0
                        ? "No pg_blocking captures exist for this server in this window, so nothing is known "
                        + "about whether blocking occurred. Check that the collector is enabled and that "
                        + "collection is succeeding (get_collection_health) before concluding anything."
                        : $"No blocking was present in any of the {captures.CapturesTotal} captures in this "
                        + "window. Note the limit of that statement: captures are periodic, so blocking that "
                        + "started and cleared between two of them left no trace. PostgreSQL has no "
                        + "engine-side blocked-process recorder to fall back on.",
                }, McpHelpers.JsonOptions);
            }

            var entries = chains.Select(c => new
            {
                captured_at = c.CapturedAt,
                root_pid = c.RootPid,
                /* Surfaced because it is what samples_as_root counts, and a reader comparing pids across
                   captures without it can be fooled by pid reuse. */
                root_backend_id = c.RootBackendId,
                databases = c.Databases,
                root_username = c.RootUsername,
                root_application = c.RootApplicationName,
                root_state = c.RootState,
                root_is_idle_in_transaction = c.RootIsIdleInTransaction,
                root_query = c.RootQuery,
                root_xact_duration_ms = c.RootXactDurationMs,
                root_query_duration_ms = c.RootQueryDurationMs,
                total_victims = c.TotalVictims,
                direct_victims = c.DirectVictims,
                max_chain_depth = c.MaxDepth,
                worst_victim_wait_ms = c.WorstVictimWaitMs,
                worst_victim_query = c.WorstVictimQuery,
                /* The one-off vs. pattern discriminator, keyed on the stable backend id. NULL when the
                   root's own identity did not resolve — reported as unknown rather than as 1, because a
                   fabricated "seen once" reads as a real finding. */
                samples_as_root = c.SamplesAsRoot,
                samples_as_root_note = c.SamplesAsRoot is null
                    ? "Unknown: this root had already left pg_stat_activity when the edge was captured, so "
                    + "it has no stable backend identity to count appearances of. Not a sign it is new."
                    : null,
                /* Chain-wide, not root-only: the stored flag is an OR across both sides of an edge, so it
                   answers "some text in this chain may be clipped". */
                query_text_may_be_truncated = c.QueryTextMayBeTruncated,
                chain_may_be_truncated = c.ChainMayBeTruncated,
                chain_truncation_note = c.ChainMayBeTruncated
                    ? "This chain hit the read's 32-level walk cap, so total_victims, max_depth and the "
                    + "worst victim are computed over a truncated walk and are FLOORS, not totals."
                    : null,
                recommended_action = RemedyFor(c.RootState, c.RootIsIdleInTransaction, c.RootXactDurationMs),
            }).ToList();

            var worst = entries[0];

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "blocking_sampled",
                captures_total = captures.CapturesTotal,
                captures_with_blocking = captures.CapturesWithBlocking,
                pct_of_captures_with_blocking = captures.CapturesTotal > 0
                    ? Math.Round((double)captures.CapturesWithBlocking / captures.CapturesTotal * 100, 1)
                    : 0,
                /* Lead with the worst chain and what to do about it, the same shape as the xmin tool:
                   the cause, then the action for that cause. */
                worst_chain_victims = worst.total_victims,
                worst_chain_root_state = worst.root_state,
                worst_chain_root_application = worst.root_application,
                recommended_action = worst.recommended_action,
                sampling_caveat =
                    "These are periodic samples of pg_stat_activity, not an event log. PostgreSQL records "
                    + "no blocking on its own, so any episode shorter than the collection interval is "
                    + "invisible here and the counts below are a floor, not a total.",
                chains = entries,
                /* Always present, even when empty, so its absence is never mistaken for "not checked". */
                cycles_sampled = cycleEntries.Count,
                cycles = cycleEntries,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL blocking chains failed: {ex.Message}");
        }
    }
}
