/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The session diagnostic-depth MCP tools — get_session_stats, get_active_queries, get_waiting_tasks —
/// served over Darling's Postgres store. Each tool body mirrors LITE's <c>McpSessionTools</c> /
/// <c>McpWaitTools</c> field-for-field (the store-faithful shape Darling's collector-mirror schema can
/// serve), keeping the tool names + parameter contracts. Reads flow through
/// <see cref="DarlingSessionReader"/> — STORED reads (no live monitored-server hit).
///
/// <para>
/// get_session_stats follows LITE's per-application shape (the store's <c>session_stats</c> table), not the
/// Dashboard's server-wide <c>session_summary_stats</c> summary — the two tools diverge and slices 1+2
/// follow Lite. get_waiting_tasks is a Lite-only tool (the Dashboard has no equivalent); it surfaces the
/// always-NULL <c>resource_description</c> exactly as Lite does.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpSessionTools
{
    [McpServerTool(Name = "get_session_stats"), Description("Gets connection and session statistics grouped by application. Shows connection counts, running/sleeping/dormant breakdown, and aggregate resource usage per application. LATEST IS A TIME: this reads the newest session snapshot, not a window, and captured_at is the instant it was collected - the connection counts are what was connected AT that stamp, not a peak or an average over anything.")]
    public static async Task<string> GetSessionStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var rows = await DarlingSessionReader.GetLatestSessionStatsAsync(postgres, resolved.ServerId, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "session_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No session statistics available. The session collector may not have run yet.");

            var totalConnections = rows.Sum(r => r.ConnectionCount);
            var totalRunning = rows.Sum(r => r.RunningCount);
            var totalSleeping = rows.Sum(r => r.SleepingCount);
            var totalDormant = rows.Sum(r => r.DormantCount);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3653: captured_at, the #3637 census's one spelling for a latest read's stamp. This was the
                   one of the four pre-vocabulary stamps the web surface READ by the old key (server-tabs.js'
                   SESSION_STATS "Collected" tile), which is what held all four out of the roster; the tile
                   moves with it in the same PR, so it is a cut-over and not an alias - see
                   DarlingMcpDataTools.GetServerProperties. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                summary = new
                {
                    total_connections = totalConnections,
                    total_running = totalRunning,
                    total_sleeping = totalSleeping,
                    total_dormant = totalDormant,
                    distinct_applications = rows.Count
                },
                applications = rows.Select(r => new
                {
                    program_name = r.ProgramName,
                    connections = r.ConnectionCount,
                    running = r.RunningCount,
                    sleeping = r.SleepingCount,
                    dormant = r.DormantCount,
                    total_cpu_time_ms = r.TotalCpuTimeMs,
                    total_logical_reads = r.TotalLogicalReads
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_session_stats", ex);
        }
    }

    /// <summary>
    /// #4198: two things make get_active_queries' default call wide, not one dominant field the way
    /// get_deadlock_detail's deadlock_graph_xml is. This row has TWENTY-THREE fields, so the fixed columns
    /// alone (collection_time, every wait/blocking/memory field, login_name, host_name, program_name) run
    /// about 700 bytes/row before query_text is counted — 50 of them is already near the budget on an
    /// empty query_text. A busy production store's default call (limit 50, hours_back 1) measured 56,586
    /// bytes; a synthetic 50-row page shaped like a busy server (every optional field populated, a fifth of
    /// the rows carrying a long literal list) measured 81,489. So both come down: the default row limit to
    /// <see cref="DefaultLimit"/>, and query_text previews to <see cref="QueryTextPreviewLength"/>
    /// (<c>full_text</c> opts back in — #4198 renamed this from <c>full_query_text</c> to match
    /// <c>get_store_query_stats</c>' <c>full_text</c> and <c>get_deadlock_detail</c>'s <c>full_graph</c>,
    /// the shape those already use). <c>truncated</c> / <c>total_snapshots</c>
    /// already tell a caller who wants more to raise limit or narrow hours_back. The web viewer is not this
    /// budget's caller (#4198 lane W2): its <c>/api/read</c> mirror keeps the pre-#4198 2,000-character
    /// preview every caller got before this tool's default fell to 500, through the internal overload below
    /// (the split #3897's trend tools use — one MCP-facing method, one budget-taking overload).
    /// </summary>
    private const int QueryTextPreviewLength = 500;

    /// <inheritdoc cref="QueryTextPreviewLength"/>
    private const int DefaultLimit = 25;

    [McpServerTool(Name = "get_active_queries"), Description("Active query snapshots (sys.dm_exec_requests) in a window ending at as_of: query text, wait, CPU/elapsed ms, blocking, DOP, memory grant GB. database_name/blocking_only filter IN SQL: total_snapshots is the filtered count; truncated means over limit. wait_time_ms, dop, granted_query_memory_gb, open_transaction_count: null = zero or not applicable. Head blockers are never stripped (is_head_blocker); a victim's blocker_not_shown is not_captured, filtered, or past_page. Empty: no snapshot in the window, or none matching filters; not_collected (unfiltered only): engine can't run it. <<GUIDE>> query_text is a preview by default (query_text_truncated: true) — pass full_text for the whole text. Gets active query snapshots captured from sys.dm_exec_requests. Shows what queries were running at each collection point: session ID, query text, wait type, CPU time, elapsed time, blocking info, DOP, and memory grants. Use hours_back to look at a specific time window — critical for finding what was running during a CPU spike or blocking event. EVERY FILTER IS PART OF THE QUERY: database_name and blocking_only are applied in SQL before the page is cut, total_snapshots is the count of snapshot rows in the window that pass your filters, snapshots_returned is how many you got, and truncated says the filtered population held more than limit — raise limit or narrow hours_back when it is true (NEWEST CAPTURE FIRST, highest CPU first within a capture; oldest_returned_collection_time / newest_returned_collection_time bound the page). Applied in SQL, so total_snapshots counts the blocking population and truncated is measured against it. HEAD BLOCKERS ARE NEVER STRIPPED: a session another row in the same capture names as its blocker is on the page whatever its text (including a WAITFOR shell holding locks), flagged is_head_blocker. A victim whose blocker is NOT on the page says why in blocker_not_shown: not_captured (the blocker held no running request at that capture — an idle open transaction is the classic case; get_blocking has its input buffer from the blocked-process report), filtered (your database_name filter excluded it), or past_page (it is in the filtered population but beyond limit).")]
    public static Task<string> GetActiveQueries(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to retrieve. Default 1.")] int hours_back = 1,
        [Description("Filter to a specific database. Applied in SQL; a head blocker in ANOTHER database is then not on the page, and its victims say blocker_not_shown = filtered.")] string? database_name = null,
        [Description("Show only queries involved in blocking: rows with blocking_session_id > 0, plus the head blockers those rows name in the same capture.")] bool blocking_only = false,
        [Description("Maximum number of rows to return. Default 25 (#4198, down from 50 — sized to fit the response budget). The page is bounded by limit, not hours_back — truncated says whether the window held more.")] int limit = DefaultLimit,
        [Description("Return each row's full query text instead of a 500-character preview. Default false.")] bool full_text = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetActiveQueries(postgres, server_name, hours_back, database_name, blocking_only, limit,
            full_text ? null : QueryTextPreviewLength, as_of, cancellationToken);

    /// <summary>
    /// get_active_queries under an explicit query-text preview length (#4198 lane W2): the MCP tool above
    /// passes <see cref="QueryTextPreviewLength"/>, or null when its full_text opt-in (#4198, renamed from
    /// full_query_text to match get_store_query_stats' full_text and the rest of the #4198 tools) asks for
    /// the whole text, and the web viewer's <c>/api/read</c> mirror passes 2000 (the pre-#4198 budget every
    /// caller got, sized for a page that redraws every 30 seconds rather than a model's context). One body,
    /// so the two surfaces differ only in preview length; null means no truncation at all, not "unbounded
    /// preview length" — the ternary below never truncates on a null budget.
    /// </summary>
    internal static async Task<string> GetActiveQueries(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? database_name, bool blocking_only,
        int limit, int? queryTextPreviewLength, string? as_of, CancellationToken cancellationToken = default)
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
            var filter = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();

            /* #3541 A13: the filters ride INTO the read (see ActiveQueriesSql), and the read is asked for one
               row past the cap so truncation is OBSERVED on the filtered population rather than inferred from
               the page. total_snapshots is the SQL's COUNT(*) OVER () of that same population — the number
               used to be rows.Count of an unfiltered window read, a different population from the rows. */
            var page = await DarlingSessionReader.GetActiveQueriesAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, filter, blocking_only, cancellationToken);
            var rows = page.Rows;

            if (rows.Count == 0)
            {
                /* A filtered miss is not a collection miss: with the filters in the query, an empty page under
                   database_name or blocking_only means the window held no snapshot matching them. */
                if (filter != null || blocking_only)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No active query snapshots on {resolved.ServerName} in the last {hours_back} hour(s) matched "
                        + DescribeActiveQueryFilters(filter, blocking_only)
                        + ". The filters were applied in SQL over the whole window, so unfiltered snapshots may well exist — drop them to see what the window holds.");
                }

                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_snapshots", cancellationToken)
                    ?? McpHelpers.Status("empty", "No active query snapshots found in the requested time range.");
            }

            var truncated = rows.Count > limit;
            var shown = truncated ? rows.GetRange(0, limit) : rows;

            /* The page's own (capture, session) pairs, so a victim can say whether its blocker made the page. */
            var onPage = new HashSet<(DateTime, int)>(shown.Select(r => (r.CollectionTime, r.SessionId)));

            var result = shown.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                session_id = r.SessionId,
                database_name = r.DatabaseName,
                status = r.Status,
                cpu_time_ms = r.CpuTimeMs,
                elapsed_time_ms = r.TotalElapsedTimeMs,
                elapsed_time_formatted = r.ElapsedTimeFormatted,
                logical_reads = r.LogicalReads,
                reads = r.Reads,
                writes = r.Writes,
                wait_type = string.IsNullOrEmpty(r.WaitType) ? null : r.WaitType,
                wait_time_ms = r.WaitTimeMs > 0 ? r.WaitTimeMs : (long?)null,
                blocking_session_id = r.BlockingSessionId > 0 ? r.BlockingSessionId : (int?)null,
                /* #3541 A13: the two blocking disclosures. is_head_blocker is why a WAITFOR row can be here;
                   blocker_not_shown names the ONE reason a victim's blocker is not, or is null when it is. */
                is_head_blocker = r.IsHeadBlocker ? true : (bool?)null,
                blocker_not_shown = BlockerNotShown(r, onPage),
                dop = r.Dop > 0 ? r.Dop : (int?)null,
                parallel_worker_count = r.ParallelWorkerCount > 0 ? r.ParallelWorkerCount : (int?)null,
                granted_query_memory_gb = r.GrantedQueryMemoryGb > 0 ? r.GrantedQueryMemoryGb : (double?)null,
                transaction_isolation_level = string.IsNullOrEmpty(r.TransactionIsolationLevel) ? null : r.TransactionIsolationLevel,
                open_transaction_count = r.OpenTransactionCount > 0 ? r.OpenTransactionCount : (int?)null,
                login_name = r.LoginName,
                host_name = r.HostName,
                program_name = r.ProgramName,
                query_text = queryTextPreviewLength.HasValue ? McpHelpers.Truncate(r.QueryText, queryTextPreviewLength.Value) : r.QueryText,
                query_text_truncated = queryTextPreviewLength.HasValue && (r.QueryText?.Length ?? 0) > queryTextPreviewLength.Value
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                filters_applied = new
                {
                    database_name = filter,
                    blocking_only,
                },
                /* The FILTERED population's size, computed in SQL on the same statement as the rows. */
                total_snapshots = page.PopulationCount,
                snapshots_returned = result.Count,
                truncated,
                order = "collection_time_desc",
                oldest_returned_collection_time = shown[^1].CollectionTime.ToString("o"),
                newest_returned_collection_time = shown[0].CollectionTime.ToString("o"),
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_active_queries", ex);
        }
    }

    /// <summary>
    /// The one reason a victim's head blocker is not on the page, or null when it is (or the row is not a
    /// victim). Ordered from the reader's facts outward: never captured (no running request — the idle
    /// open-transaction case) beats filtered beats past the page, because each later reason presupposes the
    /// earlier one did not apply.
    /// </summary>
    private static string? BlockerNotShown(DarlingSessionReader.ActiveQueryRow row, HashSet<(DateTime, int)> onPage)
    {
        if (row.BlockingSessionId <= 0)
            return null;
        if (!row.BlockerInCapture)
            return "not_captured";
        if (!row.BlockerInPopulation)
            return "filtered";
        return onPage.Contains((row.CollectionTime, row.BlockingSessionId)) ? null : "past_page";
    }

    /// <summary>Names the active filters for the filtered-miss sentence, in the caller's own vocabulary.</summary>
    private static string DescribeActiveQueryFilters(string? databaseName, bool blockingOnly)
    {
        if (databaseName != null && blockingOnly)
            return $"database_name '{databaseName}' with blocking_only";
        if (databaseName != null)
            return $"database_name '{databaseName}'";
        return "blocking_only";
    }

    [McpServerTool(Name = "get_waiting_tasks"), Description("Gets recently captured waiting tasks — queries that were actively waiting on a resource at collection time — NEWEST CAPTURE FIRST, longest wait first within a capture. Shows session ID, wait type, duration, blocking session, and database. Complements get_wait_stats by showing individual waiting queries rather than aggregated stats. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: tasks_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_collection_time / newest_returned_collection_time bound the page — under newest-first ordering the oldest stamp IS how far back this read reached, and one busy capture can fill the whole page by itself. Raise limit or narrow hours_back when truncated is true.")]
    public static async Task<string> GetWaitingTasks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description("Maximum rows to return, newest capture first. Default 30. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 30,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
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
            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal. The reader's LIMIT 500 was invisible, and the envelope stated no bound at all. */
            var rows = await DarlingSessionReader.GetWaitingTasksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "waiting_tasks", cancellationToken)
                    ?? McpHelpers.Status("empty", "No waiting tasks captured in the specified time range.");

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            var result = page.Select(r => new
            {
                session_id = r.SessionId,
                wait_type = r.WaitType,
                wait_duration_ms = r.WaitDurationMs,
                blocking_session_id = r.BlockingSessionId,
                database_name = r.DatabaseName,
                resource_description = r.ResourceDescription,
                collection_time = r.CollectionTime.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3541 A3: the envelope was bare — server and rows, no window, no count, no bound. Now the
                   span requested, the page described as a page, and the span the page covers. */
                hours_back,
                tasks_returned = page.Count,
                truncated,
                oldest_returned_collection_time = page.Min(r => r.CollectionTime).ToString("o"),
                newest_returned_collection_time = page.Max(r => r.CollectionTime).ToString("o"),
                order = "collection_time_desc",
                tasks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_waiting_tasks", ex);
        }
    }
}
