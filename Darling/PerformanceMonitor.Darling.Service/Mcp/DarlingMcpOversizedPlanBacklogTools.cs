/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over the oversized-plan backlog (#3398) — the client surface the V121 worklist shipped without.
///
/// <para><b>The gap this closes was measured, not anticipated.</b> Every other half of the capture-cap work
/// was verifiable from a shipped surface the day the fleet took the build: the cap's byte effect and the
/// probe cache's warm-up both fell out of <c>get_collection_log</c>'s phase blocks. The sweep's FETCH half
/// was the one piece nothing could see, and confirming that <c>captured_at</c> ever moves took hand-run SQL
/// against the store. A store with thousands of pending rows and a fetch half that silently never fires
/// looked identical, from every client, to a healthy one.</para>
///
/// <para><b>A new tool rather than a facet of an existing self-monitoring read.</b>
/// <c>get_collection_health</c> and <c>get_collector_cost</c> both answer "what did our collectors do", and
/// their denominators are runs. This answers "which plans did the cap cost us, and has the errand that goes
/// back for them made progress", whose denominator is PLANS. It is also the only read here over a table the
/// service MUTATES in place rather than appends to, which is why it takes no window.</para>
///
/// <para>Fleet-wide by default and per-server on request, following <c>get_collector_stall_probes</c>: the
/// first question of any backlog is which servers appear in it at all, and the measured answer on one
/// production store was 42 of 42.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpOversizedPlanBacklogTools
{
    /// <summary>
    /// Default rows of backlog detail. Small on purpose: the rollup is the answer to almost every question
    /// about this table, and a listed row is two long hex handles the caller reads whole.
    /// </summary>
    public const int DefaultLimit = 50;

    [McpServerTool(Name = "get_oversized_plan_backlog"), Description(
        "Backlog of cached plans too large to capture inline (over 512 KB/524288 bytes); the only read of this table. No time window: a worklist updated in place, not a series. Read last_captured_at/last_expired_at FIRST: null with pending rows means the sweep's fetch half has never succeeded - indistinguishable from healthy elsewhere. pending+captured+expired=total_rows. observed_bytes is UTF-16 bytes, comparable to the cap. include_rows requires server_name (refused otherwise). Permanently empty below schema V121, or when no plan ever exceeded the cap - a healthy, knob-free state. <<GUIDE>> Gets the backlog of cached execution plans this tool measured as TOO LARGE to capture inline, and what the out-of-band sweep has since done about each one. Plan XML over 512 KB (524288 bytes) for a single query_stats or procedure_stats row is deliberately not captured with the row - the client-side cost of materializing it stalls unrelated collectors - so that row lands in the store with a measured plan size and NO plan content anywhere. This table is the record of those plans, and this is the only read that reports it: every other path to it requires already knowing a specific plan identity. Per server it returns total rows, the three verdict buckets (pending = the sweep's own claim predicate, captured = content is held, expired = a fetch established the handle no longer renders a plan), the attempt figures on still-pending rows, the newest capture and expiry instants, the oldest sighting, and observed_bytes min/median/max. Read last_captured_at and last_expired_at FIRST if the question is whether the sweep's fetch half is alive: the sweep stamps them and nothing else does, so a backlog full of pending rows with a null last_captured_at is a fetch half that has never once succeeded on that server - which is indistinguishable from a healthy backlog on every other surface. The three buckets are a strict partition, so pending + captured + expired always equals total. rows_with_content sits beside captured on purpose and is NOT redundant: the fallback plan reads key on the content column rather than the stamp, so a row counted as captured with no content is a plan get_plan_xml still cannot serve. observed_bytes is the size the MONITORED SERVER measured, in UTF-16 bytes - the same unit as the 512 KB cap, so it is directly comparable to it; the median is a discrete percentile, so it is a size some plan really had rather than an interpolation, and no size here is ever derived from the stored content (a character count would read as half the real byte figure, and computing one would detoast every captured plan). The per-collector census is always returned beside the per-server rollup because the two collectors' shares are not predictable from each other: measured the day after the fleet install, procedure_stats held the larger half of one production store and 3.6% of another. Pass server_name with include_rows to get the capped row listing - the claim key, database_name, query_hash, the measured size and all four stamps - which is how a query_hash gets from here into get_plan_xml to pull the plan the cap declined; it is ordered largest plan first, which is deliberately NOT the sweep's claim order, so it is not a prediction of what gets fetched next. Read-only, unbanded, and it takes no time window: this is a worklist whose rows are updated in place, not a series. Permanently empty on a store below schema V121, and on a deployment whose plans have never exceeded the cap - which is the healthy state and has no knob, because there is nothing to turn off.")]
    public static async Task<string> GetOversizedPlanBacklog(
        NpgsqlDataSource postgres,
        [Description("Optional: server name or display name. Omit for the whole fleet.")] string? server_name = null,
        [Description("If true, also return the per-row listing for the named server. Requires server_name - the claim key is only meaningful within one server. Default false.")] bool include_rows = false,
        [Description("Maximum backlog rows to return when include_rows is true. Default 50.")] int limit = DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        var validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        int? serverId = null;
        var scope = "(fleet)";

        if (!string.IsNullOrWhiteSpace(server_name))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error != null)
            {
                return error;
            }

            serverId = resolved.ServerId;
            scope = resolved.ServerName;
        }

        /* REFUSED rather than silently widened to the fleet, following McpHelpers.ValidateTop: a caller who
           asked for rows asked for the claim keys, and handing back a rollup with no listing and no
           explanation is the silently-dropped-parameter failure. */
        if (include_rows && serverId is null)
        {
            return McpHelpers.Status(
                "precondition",
                "include_rows needs server_name. The row listing carries the claim key — the plan and SQL "
                + "handles with their statement offsets — which identifies a plan in ONE server's cache and "
                + "means nothing outside it, so a fleet-wide listing would be mostly identifiers for servers "
                + "you did not ask about. Call this once without include_rows to see which servers have a "
                + "backlog, then again with server_name set to the one you want.");
        }

        try
        {
            /* The census FIRST, because it is what makes an empty answer honest — get_collector_stall_probes'
               ordering, for its reason. */
            var census = await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, serverId, cancellationToken);

            if (census.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No oversized-plan backlog rows for {scope}, so nothing here says any cached plan was "
                    + "declined for size. That is the EXPECTED answer on most deployments: a row is only "
                    + "written when ONE query_stats or procedure_stats row's plan XML exceeds "
                    + $"{QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes} bytes as measured on the monitored "
                    + "server, which on the measured population is a handful of long-lived plan-cache "
                    + "residents per server rather than a stream. It is also permanently empty on a store "
                    + "below schema V121, and on any store whose service build predates the sweep — check "
                    + "get_store_metrics or the service version before reading this as an all-clear on a "
                    + "store you have not confirmed the rung of.");
            }

            var servers = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, serverId, cancellationToken);

            var rows = include_rows && serverId is { } id
                ? await DarlingOversizedPlanBacklogReader.GetRowsAsync(postgres, id, limit, cancellationToken)
                : null;

            return JsonSerializer.Serialize(
                new
                {
                    scope,
                    capture_cap_bytes = QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes,
                    note = "A worklist, not a series — these rows are updated in place, so there is no window "
                        + "and no trend. last_captured_at and last_expired_at are stamped by the out-of-band "
                        + "sweep and by nothing else, so they are how you tell a sweep that is working from one "
                        + "that has never fetched anything: pending rows with a null last_captured_at is the "
                        + "second. pending, captured and expired are a strict partition of total_rows. "
                        + "rows_with_content is not the same claim as captured_rows — the plan fallbacks key on "
                        + "the content column, so a captured row with no content is a plan get_plan_xml still "
                        + "cannot serve. observed_bytes is the monitored server's own measurement in UTF-16 "
                        + "bytes, directly comparable to capture_cap_bytes; the median is a discrete "
                        + "percentile, so it is a size some plan really had. Nothing here reports the size of "
                        + "the STORED content: a character count would read as half the byte figure, and "
                        + "measuring it would detoast every captured plan. server_name is null for a backlog "
                        + "row whose server has left the registry — those rows are still counted, because "
                        + "retention prunes this table on last_seen_at and not on the registry.",
                    by_collector = census.Select(c => new
                    {
                        collector_name = c.CollectorName,
                        servers = c.Servers,
                        total_rows = c.TotalRows,
                        pending_rows = c.PendingRows,
                        captured_rows = c.CapturedRows,
                        expired_rows = c.ExpiredRows,
                        last_captured_at = Stamp(c.LastCapturedAt),
                        last_expired_at = Stamp(c.LastExpiredAt),
                        min_observed_bytes = c.MinObservedBytes,
                        median_observed_bytes = c.MedianObservedBytes,
                        max_observed_bytes = c.MaxObservedBytes,
                    }),
                    by_server = servers.Select(r => new
                    {
                        server_id = r.ServerId,
                        server_name = r.ServerName,
                        total_rows = r.TotalRows,
                        pending_rows = r.PendingRows,
                        captured_rows = r.CapturedRows,
                        expired_rows = r.ExpiredRows,
                        rows_with_content = r.RowsWithContent,
                        query_stats_rows = r.QueryStatsRows,
                        procedure_stats_rows = r.ProcedureStatsRows,
                        pending_rows_attempted = r.PendingRowsAttempted,
                        pending_attempts = r.PendingAttempts,
                        max_attempts_on_a_pending_row = r.MaxAttemptsOnAPendingRow,
                        oldest_first_seen_at = Stamp(r.OldestFirstSeenAt),
                        last_seen_at = Stamp(r.LastSeenAt),
                        last_attempt_at = Stamp(r.LastAttemptAt),
                        last_captured_at = Stamp(r.LastCapturedAt),
                        last_expired_at = Stamp(r.LastExpiredAt),
                        min_observed_bytes = r.MinObservedBytes,
                        median_observed_bytes = r.MedianObservedBytes,
                        max_observed_bytes = r.MaxObservedBytes,
                    }),
                    rows = rows?.Select(b => new
                    {
                        verdict = b.Verdict,
                        collector_name = b.CollectorName,
                        database_name = b.DatabaseName,
                        query_hash = b.QueryHash,
                        observed_bytes = b.ObservedBytes,
                        plan_handle = b.PlanHandle,
                        sql_handle = b.SqlHandle,
                        statement_start_offset = b.StatementStartOffset,
                        statement_end_offset = b.StatementEndOffset,
                        first_seen_at = Stamp(b.FirstSeenAt),
                        last_seen_at = Stamp(b.LastSeenAt),
                        captured_at = Stamp(b.CapturedAt),
                        expired_at = Stamp(b.ExpiredAt),
                        last_attempt_at = Stamp(b.LastAttemptAt),
                        attempt_count = b.AttemptCount,
                        has_plan_xml = b.HasPlanXml,
                    }),
                    rows_note = include_rows
                        ? "Take query_hash from a query_stats row into get_plan_xml with this server_name and "
                          + "database_name and it resolves through the over-cap fallback — that is the only "
                          + "route to a plan the cap declined. A row with has_plan_xml false has no content to "
                          + "serve yet, whatever its verdict says. Ordered largest plan first, which is NOT the "
                          + "sweep's claim order, so this is not a prediction of the next fetch."
                        : null,
                },
                McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_oversized_plan_backlog", ex);
        }
    }

    /// <summary>
    /// Renders a stored naive-UTC stamp as a round-trip ISO-8601 string, or null.
    ///
    /// <para>Explicit rather than left to the serializer, matching <c>get_collector_stall_probes</c>: these
    /// columns are <c>timestamp without time zone</c> holding UTC, so they arrive with
    /// <c>Kind=Unspecified</c> and <c>System.Text.Json</c> would write them with no offset at all — the one
    /// rendering a caller can read as local time. The whole point of the two capture stamps is comparing
    /// them to an instant the caller already has.</para>
    /// </summary>
    private static string? Stamp(DateTime? value) =>
        value.HasValue
            ? DateTime.SpecifyKind(value.Value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture)
            : null;
}
