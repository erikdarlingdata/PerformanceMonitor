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
        "Backlog of cached plans too large to capture inline (over 524288 bytes). No time window: a worklist updated in place, not a series. Read last_captured_at/last_expired_at FIRST: null with pending rows = fetch never succeeded. pending+captured+expired+dropped=total_rows; dropped = never fetched. rows_*_last_day: the day before the read; hold rows_drained_last_day against sweep_reachable_rows_per_server_last_day. observed_bytes: UTF-16 bytes. include_rows requires server_name. Fleet by_server: worst-first, cut to budget. Permanently empty below schema V121 or with no over-cap plan. <<GUIDE>> Gets the backlog of cached execution plans this tool measured as TOO LARGE to capture inline, and what the out-of-band sweep has since done about each one. Plan XML over 512 KB (524288 bytes) for a single query_stats or procedure_stats row is deliberately not captured with the row - the client-side cost of materializing it stalls unrelated collectors - so that row lands in the store with a measured plan size and NO plan content anywhere. This table is the record of those plans, and this is the only read that reports it: every other path to it requires already knowing a specific plan identity. Per server it returns total rows, the four verdict buckets (pending = the sweep's own claim predicate, captured = content is held, expired = a fetch established the handle no longer renders a plan, dropped = the sweep took the row out of pending without a fetch), the attempt figures on still-pending rows, the newest capture and expiry instants, the oldest sighting, and observed_bytes min/median/max. Beside them, five counts over the day before the read, taken against the read's own clock (last_day_since is where that day starts): rows_added_last_day is rows first sighted in it, rows_captured_last_day rows the sweep captured in it, rows_expired_last_day rows a fetch retired in it and that stand retired now, rows_drained_last_day is captured plus expired, and rows_dropped_last_day is rows dropped unfetched in it, counted when they were dropped (a row dropped, re-sighted and dropped again in the day counts twice). The last-day dropped and added counts are accurate to the hour: the day starts at the top of the hour last_day_since falls in. Each pass drops, before it claims, the never-tried pending rows no claim is going to serve: every row last sighted before the newest sighting of its query (same collector, database_name and query_hash: plan-cache churn re-keys one query's plan under a new handle, and the plan reads serve the latest capture for a query, not one per handle), then the least recently sighted rows past pending_rows_cap_per_server, which is the sweep's daily ceiling times the retention days. A drop is not drain and not an expiry: no fetch was made. The pass that drops a row deletes it and adds it to a per-hour count the store keeps for a day, so the day's count is complete; a re-sighting of that plan afterwards adds it back as a new pending row, which counts as added again and can be dropped and counted again. The size figures (observed_bytes min, median and max) and the oldest sighting cover the rows the store still holds, never the dropped ones. A store upgraded from an older build still holds the dropped rows that build kept, and they count alongside the new counts until they age out, within a day. The table does not record which of the two reasons dropped a row: while pending_rows is under the cap every drop was a superseded sighting, and the oversized_plan_sweep run record in get_collection_log gives each pass's split. Added is a floor on arrival, not the arrival itself: a row re-sighted after it expired loses its expiry stamp and is pending again, but its first sighting is unchanged, so it is not counted as added. Two figures say what the sweep could have done in that day. sweep_passes_last_day is how many sweep passes FINISHED in it, counted from the run records the sweep writes under the fleet sentinel when a pass ends, one per pass; the count is store-wide and includes passes that reached no server (the empty-target passes after a service start, a pass cut short at shutdown). sweep_reachable_rows_per_server_last_day is those passes times the per-pass claim, an upper bound on what the passes could have taken off one server, captured and expired together. When the count is above zero, hold rows_drained_last_day against it: a server drained at or near it with pending rows waiting was at the cap for the passes that finished, which is the design doing its work; a pass or two under it on a day the service restarted is still the cap; up to one pass's claim above it is a pass in flight at the read, whose captures are stamped as they happen while its record is written only when it ends; and a server well below it with pending rows is the one worth a look. When the count is zero, no pass finished in the day, so no drain reads against the figure: the sweep is not running, or a pass has run the whole day without finishing, and the fleet sentinel's oversized_plan_sweep record in get_collection_log is the next read; the note says so in place of the comparison. sweep_ceiling_rows_per_server_per_day is what a day of passes would reach if every pass finished inside its interval; the worker launches a pass only when the previous one has finished, so a pass that overruns its interval costs the next slot, and fewer passes than the interval allows is the ordinary reason the reachable figure sits under the ceiling, not a defect. Added counts every new key, a re-keyed plan of a query already waiting included, so added above drain is not by itself a backlog that grows: pending rises by added and falls by drained plus dropped. For one that is falling, pending divided by drained plus dropped minus added is the days it needs at that rate, and because added is a floor on arrival that figure is a floor on the days. Pending held at pending_rows_cap_per_server with rows dropped every day is a server whose distinct over-cap plans arrive faster than the sweep's ceiling: the largest are fetched and the stalest of the rest are dropped. Read last_captured_at and last_expired_at FIRST if the question is whether the sweep's fetch half is alive: the sweep stamps them and nothing else does, so a backlog full of pending rows with a null last_captured_at is a fetch half that has never once succeeded on that server - which is indistinguishable from a healthy backlog on every other surface. The four buckets are a strict partition, so pending + captured + expired + dropped always equals total. rows_with_content sits beside captured on purpose and is NOT redundant: the fallback plan reads key on the content column rather than the stamp, so a row counted as captured with no content is a plan get_plan_xml still cannot serve. observed_bytes is the size the MONITORED SERVER measured, in UTF-16 bytes - the same unit as the 512 KB cap, so it is directly comparable to it; the median is a discrete percentile, so it is a size some plan really had rather than an interpolation, and no size here is ever derived from the stored content (a character count would read as half the real byte figure, and computing one would detoast every captured plan). The per-collector census is always returned beside the per-server rollup because the two collectors' shares are not predictable from each other: measured the day after the fleet install, procedure_stats held the larger half of one production store and 3.6% of another. Pass server_name with include_rows to get the capped row listing - the claim key, database_name, query_hash, the measured size and all four stamps - which is how a query_hash gets from here into get_plan_xml to pull the plan the cap declined; it is ordered largest plan first, which is deliberately NOT the sweep's claim order, so it is not a prediction of what gets fetched next. Read-only, unbanded, and it takes no time window: this is a worklist whose rows are updated in place, not a series. Permanently empty on a store below schema V121, and on a deployment whose plans have never exceeded the cap - which is the healthy state and has no knob, because there is nothing to turn off.")]
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
            scope = resolved.DisplayName;
        }

        /* REFUSED rather than silently widened to the fleet, following McpHelpers.ValidateTop: a caller who
           asked for rows asked for the claim keys, and handing back a rollup with no listing and no
           explanation is the silently-dropped-parameter failure. */
        if (include_rows && serverId is null)
        {
            return McpHelpers.Status(
                "precondition",
                "include_rows needs server_name. The row listing carries the claim key: the plan and SQL "
                + "handles with their statement offsets, which identifies a plan in ONE server's cache and "
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
                    + $"{QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes.ToString("N0", System.Globalization.CultureInfo.InvariantCulture)} bytes as measured on the monitored "
                    + "server, which on the measured population is a handful of long-lived plan-cache "
                    + "residents per server rather than a stream. It is also permanently empty on a store "
                    + "with an older schema, and on any store whose service build predates the sweep. Before "
                    + "reading this as an all-clear, confirm the store's schema version "
                    + "(get_store_metrics reports it) and the service version.");
            }

            /* #8404: the day the three last-day counts cover ends at this read and starts one day before it. The table
               has no window of its own, so the read's clock is the one clock here, and the payload states where that day
               starts so the counts can be read against it. */
            var lastDaySince = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-1), DateTimeKind.Unspecified);
            var servers = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, serverId, lastDaySince, cancellationToken);
            /* The passes that ran in that day, from the sweep's own run records: the day's drain is held against what
               those passes could reach, not against the count an interval would allow if every pass finished inside it. */
            var passes = await DarlingOversizedPlanBacklogReader.GetSweepPassesLastDayAsync(postgres, lastDaySince, cancellationToken);

            var rows = include_rows && serverId is { } id
                ? await DarlingOversizedPlanBacklogReader.GetRowsAsync(postgres, id, limit, cancellationToken)
                : null;

            /* The fleet answer is one by_server row per registered server, with no limit on the server count, so it is fitted to
               the shared response target worst-first (the most pending rows, then the most rows): the servers left out are the
               ones with the least to look at, and servers_returned / truncated / cut_note say they were. */
            var ordered = servers
                .OrderByDescending(r => r.PendingRows)
                .ThenByDescending(r => r.TotalRows)
                .ThenBy(r => r.ServerId)
                .ToList();
            return ordered.Count == 0
                ? Answer(0)
                : McpResponseBudget.FitPrefix(ordered.Count, Answer).Json;

            string Answer(int shown) => JsonSerializer.Serialize(
                new
                {
                    scope,
                    capture_cap_bytes = QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes,
                    /* #8404: the interval's ceiling, the passes that ran in the day and the drain they could reach on one
                       server, and the start of the day the last-day counts cover, so a reader can hold each server's
                       drain against what the sweep could do and against its arrivals. */
                    sweep_ceiling_rows_per_server_per_day = OversizedPlanBacklogSweep.RowsPerServerPerDayCeiling,
                    sweep_passes_last_day = passes,
                    sweep_reachable_rows_per_server_last_day = OversizedPlanBacklogSweep.ReachableRowsPerServer(passes),
                    /* #8567: the most never-tried pending rows a server keeps; a pass drops the stalest past it unfetched. */
                    pending_rows_cap_per_server = OversizedPlanBacklogSweep.PendingRowsPerServerCap,
                    last_day_since = Stamp(lastDaySince),
                    servers_in_scope = ordered.Count,
                    servers_returned = shown,
                    truncated = shown < ordered.Count,
                    /* Composed here rather than McpResponseBudget.PageCutNote: this read has no window, no database and no row
                       limit over servers, so the one lever that shows a cut server is server_name. */
                    cut_note = shown < ordered.Count
                        ? string.Create(CultureInfo.InvariantCulture,
                            $"TRUNCATED: {ordered.Count:#,0} servers were read; only {shown:#,0} fit the {McpResponseBudget.DefaultBytes:#,0}-byte response budget (pending_rows desc, total_rows desc). Pass server_name to read one of the servers not shown.")
                        : null,
                    note = "A worklist, not a series: these rows are updated in place, so there is no window "
                        + "and no trend. last_captured_at and last_expired_at are stamped by the out-of-band "
                        + "sweep and by nothing else, so they are how you tell a sweep that is working from one "
                        + "that has never fetched anything: pending rows with a null last_captured_at is the "
                        + "second. pending, captured, expired and dropped are a strict partition of total_rows. "
                        + "dropped_rows left pending without a fetch: before it claims, each pass drops the never-tried "
                        + "pending rows last sighted before the newest sighting of their query (collector, database_name "
                        + "and query_hash), then the least recently sighted past pending_rows_cap_per_server. A dropped "
                        + "row is deleted at once and counted by the hour for a day, and a re-sighting adds the plan back as a new pending row. total_rows counts the day's drops with the held rows, while the size "
                        + "figures and the oldest sighting cover held rows only. While "
                        + "pending_rows is under the cap every drop was a superseded sighting; the sweep's run record "
                        + "in get_collection_log gives each pass's split. "
                        + "rows_with_content is not the same claim as captured_rows: the plan fallbacks key on "
                        + "the content column, so a captured row with no content is a plan get_plan_xml still "
                        + "cannot serve. observed_bytes is the monitored server's own measurement in UTF-16 "
                        + "bytes, directly comparable to capture_cap_bytes; the median is a discrete "
                        + "percentile, so it is a size some plan really had. Nothing here reports the size of "
                        + "the STORED content: a character count would read as half the byte figure, and "
                        + "measuring it would detoast every captured plan. server_name is null for a backlog "
                        + "row whose server has left the registry: those rows are still counted, because "
                        + "retention prunes this table on last_seen_at and not on the registry. "
                        + "rows_added_last_day, rows_captured_last_day, rows_expired_last_day, rows_drained_last_day and "
                        + "rows_dropped_last_day count the day before this read, starting at last_day_since, by the read's "
                        + "own clock: rows first sighted, rows the sweep captured, rows a fetch retired and that stand "
                        + "retired now, captured plus expired, and rows dropped unfetched, counted when they were dropped, to the hour. A drop is "
                        + "never drain. Added is a floor on arrival: a row re-sighted after it expired returns "
                        + "to pending with its first sighting unchanged, so it is not counted, but a dropped plan that returns is a new row and is counted as added again. "
                        + "sweep_ceiling_rows_per_server_per_day is what a day of passes would reach if every pass "
                        + "finished inside its interval; the worker launches a pass only after the previous one has "
                        + "finished, so an overrunning pass costs the next slot, and fewer passes than the interval "
                        + "allows is the ordinary reason the reachable figure sits under the ceiling. Added counts "
                        + "every new key, a re-keyed plan of a query already waiting included, so added above drain is "
                        + "not by itself a backlog that grows: pending rises by added and falls by drained plus dropped. "
                        + "For one that is falling, pending divided by drained plus dropped minus added is the days it "
                        + "needs at that rate, a floor because added is one. "
                        + SweepPassesNote(passes),
                    by_collector = census.Select(c => new
                    {
                        collector_name = c.CollectorName,
                        servers = c.Servers,
                        total_rows = c.TotalRows,
                        pending_rows = c.PendingRows,
                        captured_rows = c.CapturedRows,
                        expired_rows = c.ExpiredRows,
                        dropped_rows = c.DroppedRows,
                        last_captured_at = Stamp(c.LastCapturedAt),
                        last_expired_at = Stamp(c.LastExpiredAt),
                        min_observed_bytes = c.MinObservedBytes,
                        median_observed_bytes = c.MedianObservedBytes,
                        max_observed_bytes = c.MaxObservedBytes,
                    }),
                    by_server = ordered.Take(shown).Select(r => new
                    {
                        server_id = r.ServerId,
                        server_name = r.ServerName,
                        total_rows = r.TotalRows,
                        pending_rows = r.PendingRows,
                        captured_rows = r.CapturedRows,
                        expired_rows = r.ExpiredRows,
                        dropped_rows = r.DroppedRows,
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
                        rows_added_last_day = r.RowsAddedLastDay,
                        rows_captured_last_day = r.RowsCapturedLastDay,
                        rows_expired_last_day = r.RowsExpiredLastDay,
                        rows_drained_last_day = r.RowsCapturedLastDay + r.RowsExpiredLastDay,
                        rows_dropped_last_day = r.RowsDroppedLastDay,
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
                          + "database_name and it resolves through the over-cap fallback, that is the only "
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
    /// #8404: the sentence of the note that reads the day's drain against the sweep passes that finished in it. With
    /// no pass finished, nothing reads against the reachable figure: a drain of zero against a reachable figure of
    /// zero is not the cap, it is a sweep that is not running, and the sentinel's run record is the next read. The
    /// count is of passes that finished (the record is written when a pass ends, captures during it), store-wide and
    /// including passes that reached no server, so the reachable figure is an upper bound and a drain a little under
    /// it on a restart day, or up to one claim above it with a pass in flight, is still the cap.
    /// </summary>
    internal static string SweepPassesNote(int passes) =>
        passes == 0
            ? "sweep_passes_last_day is 0: no sweep pass finished in the day before this read, so "
              + "sweep_reachable_rows_per_server_last_day is 0 and no drain reads against it. The sweep is not running, "
              + "or a pass has run the whole day without finishing; the fleet sentinel's oversized_plan_sweep record in "
              + "get_collection_log is the next read."
            : string.Create(CultureInfo.InvariantCulture,
                $"sweep_passes_last_day is {passes}: the passes that finished in the day, from the run records the sweep ")
              + "writes when a pass ends. The count is store-wide and includes passes that reached no server (the "
              + "empty-target passes after a service start, a pass cut short at shutdown), so "
              + "sweep_reachable_rows_per_server_last_day, the passes times the per-pass claim, is an upper bound on "
              + "what one server could have been drained by, captured and expired together. Hold rows_drained_last_day "
              + "against it: at or near it with pending rows waiting is the cap for the passes that finished, a pass or "
              + "two under it on a day the service restarted is still the cap, and up to one pass's claim above it is a "
              + "pass in flight at the read, whose record is not written yet. A server well below it with pending rows "
              + "is the one worth a look.";

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
