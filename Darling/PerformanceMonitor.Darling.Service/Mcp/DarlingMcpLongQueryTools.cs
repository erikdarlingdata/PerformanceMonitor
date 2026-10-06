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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The long-query completion MCP tool — get_long_query_completions — served over Darling's Postgres store
/// (#1496). Returns the longest completed queries (rpc/batch over the trace's duration threshold) plus
/// attentions (cancels/timeouts) captured by the opt-in PerformanceMonitor_Darling_{id}_LongQueryCompletions XE
/// session this install makes, ordered by duration DESC. Mirrors Lite's <c>McpLongQueryTools</c> field-for-field; reads flow through
/// <see cref="DarlingLongQueryReader"/> — a STORED read (no live monitored-server hit).
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpLongQueryTools
{
    [McpServerTool(Name = "get_long_query_completions"), Description("Gets the SLOWEST long-running query completions in the window: rpc/batch completions over the trace's duration threshold, plus attentions (cancels/timeouts), ranked duration DESC, attentions (no duration) last. THE PAGE IS THE window's limit SLOWEST, NOT ITS NEWEST: truncated means the window held more, none slower than the page; oldest/newest_returned_event_time bound the slowest runs' ages, NOT how far the read reached. Collector is opt-in, OFF by default: empty can mean none in the window, or the collector was off — enable 'long_query_completions' in the schedule. <<GUIDE>> Gets the SLOWEST long-running query completions in the window, captured by the opt-in long-query trace: rpc/batch completions whose duration exceeded the trace threshold, plus attentions (client cancels / query timeouts), ranked by duration DESC with attentions (no duration) last. Shows duration, CPU, reads/writes, row count, result (OK/Error/Abort — Abort means the long query was cancelled), the statement text, and the calling session/app/login. THE PAGE IS THE window's limit SLOWEST, NOT ITS NEWEST: completions_returned is how many rows you got and truncated says the window held more than limit. Because the page is duration-RANKED, oldest_returned_event_time / newest_returned_event_time tell you how old the slowest runs are and say NOTHING about how far back the read reached — every row in the window was a candidate, so a truncated page still holds the window's slowest. Both SKUs keep the same population. The collector is OFF by default; if it returns empty, enable the 'long_query_completions' collector in the schedule.")]
    public static Task<string> GetLongQueryCompletions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, slowest first. Default 30. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 30,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
        => GetLongQueryCompletions(postgres, server_name, hours_back, limit, as_of, DatabaseFilter.One(database_name), logger, cancellationToken);

    /// <summary>
    /// #5244: the get_long_query_completions read over a LIST of databases. The MCP tool passes
    /// <c>DatabaseFilter.One(database_name)</c> and the web dispatch passes the repeated keys. The cap applies after the
    /// filter, so the page is the slowest N of the chosen databases, and the empty path says "for the database X" (one name) or
    /// "for the chosen databases" (two or more), never a verdict on a database it did not read.
    /// </summary>
    internal static async Task<string> GetLongQueryCompletions(
        NpgsqlDataSource postgres,
        string? server_name,
        int hours_back,
        int limit,
        string? as_of,
        DatabaseFilter databaseFilter,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var windowStart = now.AddHours(-hours_back);

            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation signal.
               The reader's own LIMIT 200 was invisible to the caller, and `total_completions` published it as
               the window's count. */
            var rows = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, databaseFilter, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "long_query_completions", cancellationToken)
                    /* #2546: this collector is opt-in, so the fall-through below already sends the reader to
                       the schedule — which is the wrong place when the collector IS enabled and its session
                       is missing. The precondition answer names that state instead of quietly blaming a knob
                       that is already switched on. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "long_query_completions", cancellationToken)
                    /* #4966: the window keys ride on an empty answer under hints; not_collected and the precondition stay bare. */
                    ?? McpHelpers.StatusForDatabase("empty",
                        /* #5244 review L3: the precondition answer above already named a collector that is on but broken, so
                           under a filter the likelier cause is the filter itself: the answer ends after the database clause,
                           as get_plan_corrections does, rather than sending the reader to a switch that may already be on. */
                        "No long-running query completions found in the specified time range" + DarlingMcpBlockingTools.ForChosenDatabases(databaseFilter)
                            + (databaseFilter.IsAll ? ". The long_query_completions collector is opt-in (default OFF) — enable it in the collector schedule to capture data." : "."),
                        databaseFilter.Describe(), /* #5244 review L2: the echo rides on an empty answer too */
                        (await DarlingMcpWindowNotice.ReadEventAsync(
                            () => DarlingMcpWindowNotice.Probe(postgres, "long_query_completions", resolved.ServerName, windowStart, now, cancellationToken),
                            null, windowStart, now, "long_query_completions", emptyAnswer: true, logger: logger, cancellationToken: cancellationToken)).AsHints());

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            /* #4966: an event list, so the floor is the earlier of the coverage probe and the oldest event the page shows. The probe
               reads the table on collection_time while the page is ranked by duration, so a first run's backfilled event can be older
               than the probe's floor. The page is the slowest N, so its oldest event depends on limit; that cannot make a false
               notice, as the floor only moves earlier, and a first run reads back 10 minutes, well inside the 90-minute slack. */
            var notice = await DarlingMcpWindowNotice.ReadEventAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "long_query_completions", resolved.ServerName, windowStart, now, cancellationToken),
                page.Min(r => r.EventTime), windowStart, now, "long_query_completions", logger: logger, cancellationToken: cancellationToken);

            var result = page.Select(r => new
            {
                event_time = r.EventTime?.ToString("o"),
                event_type = r.EventType,
                duration_ms = r.DurationMicroseconds.HasValue ? r.DurationMicroseconds.Value / 1000.0 : (double?)null,
                cpu_ms = r.CpuTimeMicroseconds.HasValue ? r.CpuTimeMicroseconds.Value / 1000.0 : (double?)null,
                logical_reads = r.LogicalReads,
                physical_reads = r.PhysicalReads,
                writes = r.Writes,
                row_count = r.RowCount,
                result = r.Result,
                database_name = r.DatabaseName,
                object_name = r.ObjectName,
                statement = McpHelpers.Truncate(r.StatementText, 2000),
                session_id = r.SessionId,
                client_app_name = r.ClientAppName,
                client_pid = r.ClientPid,
                nt_username = r.NtUserName,
                server_principal_name = r.ServerPrincipalName,
                query_hash = r.QueryHash
            });

            var json = JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #4966: where the store's data starts, always present on a data answer (false and null when the store covered the
                   window). No effective_hours_back: this payload carries a page `truncated`, and the census holds that key apart
                   for the window floor. A failed coverage probe leaves the three off (see the return below). */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                /* #5244: which databases the page is limited to: the name for one, "the chosen databases" for two or more, null for all. */
                database_name = databaseFilter.Describe(),
                /* #3541 A3: the page described as a page. Under a duration RANKING the two stamps bound the
                   slowest runs, not the reach — the description says so, and QueryStoreTopWindowTests states
                   the general trap for cost-ranked pages. */
                completions_returned = page.Count,
                truncated,
                oldest_returned_event_time = McpHelpers.FormatEffectiveStart(page.Min(r => r.EventTime)),
                newest_returned_event_time = McpHelpers.FormatEffectiveStart(page.Max(r => r.EventTime)),
                order = "duration_ms_desc",
                completions = result
            }, McpHelpers.JsonOptions);

            /* A failed probe costs the notice, never the rows (see DarlingMcpWindowNotice.ReadAsync). */
            return notice.IsUnavailable ? DarlingMcpWindowNotice.WithoutKeys(json) : json;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_long_query_completions", ex);
        }
    }
}
