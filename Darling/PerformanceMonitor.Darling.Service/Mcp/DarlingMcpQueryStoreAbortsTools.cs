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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6073: <c>get_query_store_aborts</c>, the queries whose Query Store runs ended aborted (a client timeout or cancel) or with an
/// exception, ranked by how many. Store-only and read-only: it reads the collector's <c>query_store_stats</c> snapshots (or
/// <c>query_store_interval_wide</c> under <c>get_query_store_top</c>'s own gate) and never connects to the monitored server.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryStoreAbortsTools
{
    /// <summary>The most queries one answer lists. Each row carries a query text preview, so the cap keeps the page inside the response budget.</summary>
    internal const int MaxTop = 100;

    internal const int DefaultTop = 20;

    /// <summary>The query_text preview length, the one <c>get_query_store_top</c> uses.</summary>
    private const int QueryTextPreviewLength = 400;

    [McpServerTool(Name = "get_query_store_aborts"), Description(
        "Queries ranked by Query Store runs that ended aborted (a client timeout or cancel) or with an exception. A cheap query that times out often ranks low in get_query_store_top (ranked by cost) but first here. " +
        "Each row: run counts by outcome, failed_share (failed runs over the query's own total runs), avg_aborted_duration_ms (aborted runs only), last_failed_time. " +
        "window_truncated marks a window floor, not a page cut; effective_start says where history begins. <<GUIDE>> " +
        "Row fields: regular_runs, aborted_runs, exception_runs, total_runs, failed_runs, failed_share. Queries with no aborted or exception run in the window are not listed. Counts are per Query Store interval, summed over the window; one row folds a query's plans and replica roles. " +
        "Reads the raw tier, or the per-interval table (kept 10 days) by get_query_store_top's own rule; history_source says which answered. Never the daily summary, so counts are exact. " +
        "An empty answer says why: Query Store off or not collected, no aborted or failed runs, or no Query Store data in the window. " +
        "query_text is a 400-character preview by default (query_text_truncated marks a cut row), masked by mcp.dataDetail; full_text=true returns the whole statement. top caps at 100; truncated says more queries matched.")]
    public static async Task<string> GetQueryStoreAborts(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of (default 24).")] int hours_back = 24,
        [Description("Number of queries to list. Default 20, at most 100.")] int top = DefaultTop,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Return each row's full query_text instead of a 400-character preview. Default false.")] bool full_text = false,
        CancellationToken cancellationToken = default) =>
        await GetQueryStoreAborts(postgres, server_name, hours_back, top, DatabaseFilter.One(database_name), as_of, full_text, QueryTextPreviewLength, cancellationToken);

    /// <summary><see cref="GetQueryStoreAborts(NpgsqlDataSource,string,int,int,string,string,bool,CancellationToken)"/> over a SET of databases, with the preview length the caller picks.</summary>
    internal static async Task<string> GetQueryStoreAborts(
        NpgsqlDataSource postgres, string? server_name, int hours_back, int top, DatabaseFilter databases, string? as_of,
        bool full_text, int previewLength, CancellationToken cancellationToken)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        if (top > MaxTop)
            return McpHelpers.Refusal("top", $"top value '{top}' exceeds maximum of {MaxTop} for get_query_store_aborts. Use a smaller value.");

        try
        {
            var requestedStart = windowEnd.AddHours(-hours_back);
            /* One row past the page, so a cut page is observed (Count > top) and never inferred. */
            var read = await DarlingQueryStoreAbortsReader.ReadAsync(postgres, resolved.ServerId, requestedStart, windowEnd, top + 1, databases, cancellationToken);
            var (page, cut) = McpHelpers.BoundPage(read.Rows, top);

            /* The reach of the answer, by whichever tier served (get_query_store_top's own two branches). */
            DateTime effectiveStart;
            bool truncated;
            string? truncationNote;
            if (read.Table is { } plan)
            {
                effectiveStart = plan.ReadStart;
                truncated = RawWindowFloor.IsTruncated(effectiveStart, requestedStart);
                truncationNote = truncated ? DarlingMcpDataTools.QueryStoreTableNote(effectiveStart, plan.StartBound) : null;
            }
            else
            {
                var floor = await DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, requestedStart, windowEnd, cancellationToken);
                effectiveStart = RawWindowFloor.EffectiveStart(floor, requestedStart);
                truncated = RawWindowFloor.IsTruncated(floor, requestedStart);
                truncationNote = truncated
                    ? "The window reaches further back than this server's raw query_store_stats retains, so the older part of it was not read."
                    : null;

                /* #7010: that floor is the oldest row IN the window, so a window holding no row at all answers null, and a null floor read as
                   covered: an empty answer over a window before the data said window_truncated false. The shared notice reads such a window by the
                   collector's runs and the server's first collection, and says not covered when the store holds none of it. */
                if (floor is null && page.Count == 0)
                {
                    DateTime? sharedFloor = null;
                    var notice = await DarlingMcpWindowNotice.ReadAsync(
                        async () => sharedFloor = await DarlingMcpWindowNotice.Probe(postgres, "query_store_stats", resolved.ServerName, requestedStart, windowEnd, cancellationToken),
                        requestedStart, windowEnd, "query_store_stats", emptyAnswer: true, cancellationToken: cancellationToken);
                    if (!notice.IsUnavailable && notice.WindowTruncated)
                    {
                        truncated = true;
                        effectiveStart = sharedFloor is DateTime covered ? RawWindowFloor.EffectiveStart(covered, requestedStart) : windowEnd;
                        truncationNote = notice.TruncationNote;
                    }
                }
            }

            var scopeName = databases.Describe();
            if (page.Count == 0)
            {
                var hints = new
                {
                    effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                    window_truncated = truncated,
                    truncation_note = truncationNote,
                    history_source = read.Table is null ? "raw" : "interval_table"
                };

                /* A measured zero is an answer: the same window and scope read without the outcome filter has runs, and none ended aborted or failed. */
                var anyRuns = (await DarlingDataReader.GetQueryStoreTopAsync(postgres, resolved.ServerId, requestedStart, windowEnd, 1, databases, cancellationToken)).Count > 0;
                if (anyRuns)
                {
                    return McpHelpers.StatusForDatabase(
                        "empty",
                        $"No Query Store query on {resolved.DisplayName} had an aborted or exception run in the last {hours_back} hour(s)"
                        + (scopeName is null ? "" : $" for {(databases.Names.Count > 1 ? DatabaseFilter.ManyDatabasesDescription : "database '" + scopeName + "'")}")
                        + ". Other runs were recorded in the window, so this is a measured zero"
                        + (truncated ? ", over the part of the window the store holds (see effective_start)." : "."),
                        scopeName, hints);
                }

                return McpHelpers.WithDatabase(
                           await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_store", cancellationToken)
                           ?? await DarlingRuntimePrecondition.QueryStoreStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, databases, cancellationToken)
                           ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_store", cancellationToken),
                           scopeName)
                       ?? McpHelpers.StatusForDatabase(
                           "empty",
                           $"No Query Store rows for {resolved.DisplayName} in the {hours_back}-hour window searched"
                           + (truncated ? ", and the store does not hold the whole window (see effective_start)." : ". Query Store may be off for the target databases, or nothing ran."),
                           scopeName, hints);
            }

            var queries = page.Select(r =>
            {
                var failed = r.AbortedRuns + r.ExceptionRuns;
                var text = r.QueryText;
                return new
                {
                    database_name = r.DatabaseName,
                    query_id = r.QueryId,
                    query_hash = r.QueryHash,
                    failed_runs = failed,
                    failed_share = r.TotalRuns > 0 ? Math.Round((double)failed / r.TotalRuns, 4) : (double?)null,
                    aborted_runs = r.AbortedRuns,
                    exception_runs = r.ExceptionRuns,
                    regular_runs = r.RegularRuns,
                    total_runs = r.TotalRuns,
                    avg_aborted_duration_ms = r.AvgAbortedDurationMs is { } ms ? Math.Round(ms, 1) : (double?)null,
                    last_failed_time = McpHelpers.FormatEffectiveStart(r.LastFailedTime),
                    query_text = full_text ? text : McpHelpers.TruncateStatement(text, previewLength),
                    query_text_truncated = !full_text && text != null && text.Length > previewLength
                };
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                database_name = scopeName,
                effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                window_truncated = truncated,
                truncation_note = truncationNote,
                history_source = read.Table is null ? "raw" : "interval_table",
                source_note = read.Table is null
                    ? "Counted from raw query_store_stats, de-duplicated to the newest snapshot of each Query Store interval. Raw rows are kept 4 days on a store with the rollups armed, longer otherwise."
                    : "Counted from query_store_interval_wide, one row per Query Store interval, kept 10 days. The same rows get_query_store_top counts for this window.",
                failed_share_note = "failed_share is a query's own aborted plus exception runs over its own total runs (regular, aborted and exception) in the window.",
                queries_returned = queries.Count,
                truncated = cut,
                queries
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_aborts", ex);
        }
    }
}
