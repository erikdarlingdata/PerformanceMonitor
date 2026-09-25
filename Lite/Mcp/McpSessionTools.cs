using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpSessionTools
{
    /// <summary>#4198: see Darling's <c>DarlingMcpSessionTools.QueryTextPreviewLength</c> twin for the
    /// measured bytes both defaults come down from.</summary>
    private const int QueryTextPreviewLength = 500;

    /// <inheritdoc cref="QueryTextPreviewLength"/>
    private const int DefaultLimit = 25;

    [McpServerTool(Name = "get_active_queries"), Description("Active query snapshots (sys.dm_exec_requests) in a window ending at as_of: query text, wait, CPU/elapsed ms, blocking, DOP, memory grant GB. database_name/blocking_only filter IN SQL: total_snapshots is the filtered count; truncated means over limit. wait_time_ms, dop, granted_query_memory_gb, open_transaction_count: null = zero or not applicable. Head blockers are never stripped (is_head_blocker); a victim's blocker_not_shown is not_captured, filtered, or past_page. Empty: no snapshot in the window, or none matching filters; not_collected (unfiltered only): engine can't run it. <<GUIDE>> query_text is a preview by default (query_text_truncated: true) — pass full_text for the whole text. Gets active query snapshots captured from sys.dm_exec_requests. Shows what queries were running at each collection point: session ID, query text, wait type, CPU time, elapsed time, blocking info, DOP, and memory grants. Use hours_back to look at a specific time window — critical for finding what was running during a CPU spike or blocking event. EVERY FILTER IS PART OF THE QUERY: database_name and blocking_only are applied in SQL before the page is cut, total_snapshots is the count of snapshot rows in the window that pass your filters, snapshots_returned is how many you got, and truncated says the filtered population held more than limit — raise limit or narrow hours_back when it is true (NEWEST CAPTURE FIRST, highest CPU first within a capture; oldest_returned_collection_time / newest_returned_collection_time bound the page). Applied in SQL, so total_snapshots counts the blocking population and truncated is measured against it. HEAD BLOCKERS ARE NEVER STRIPPED: a session another row in the same capture names as its blocker is on the page whatever its text (including a WAITFOR shell holding locks), flagged is_head_blocker. A victim whose blocker is NOT on the page says why in blocker_not_shown: not_captured (the blocker held no running request at that capture — an idle open transaction is the classic case; get_blocked_process_reports has its input buffer from the blocked-process report), filtered (your database_name filter excluded it), or past_page (it is in the filtered population but beyond limit).")]
    public static async Task<string> GetActiveQueries(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to retrieve. Default 1.")] int hours_back = 1,
        [Description("Filter to a specific database. Applied in SQL; a head blocker in ANOTHER database is then not on the page, and its victims say blocker_not_shown = filtered.")] string? database_name = null,
        [Description("Show only queries involved in blocking: rows with blocking_session_id > 0, plus the head blockers those rows name in the same capture.")] bool blocking_only = false,
        [Description("Maximum number of rows to return. Default 25 (#4198, down from 50 — sized to fit the response budget). The page is bounded by limit, not hours_back — truncated says whether the window held more.")] int limit = DefaultLimit,
        [Description("Return each row's full query text instead of a 500-character preview. Default false.")] bool full_text = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var filter = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();

            /* #3541 A13: the filters ride INTO the read (GetActiveQueriesPageAsync), asked for one row past
               the cap so truncation is OBSERVED on the filtered population; total_snapshots is that read's
               COUNT(*) OVER () of the same population. Darling's twin is DarlingSessionReader.ActiveQueriesSql. */
            var (rows, populationCount) = await dataService.GetActiveQueriesPageAsync(
                resolved.ServerId, hours_back, limit + 1, filter, blocking_only, asOfUtc: windowEnd);

            if (rows.Count == 0)
            {
                /* A filtered miss is not a collection miss — Darling's twin's words. */
                if (filter != null || blocking_only)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No active query snapshots on {resolved.ServerName} in the last {hours_back} hour(s) matched "
                        + DescribeActiveQueryFilters(filter, blocking_only)
                        + ". The filters were applied in SQL over the whole window, so unfiltered snapshots may well exist — drop them to see what the window holds.");
                }

                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_snapshots")
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
                /* #3541 A13: the two blocking disclosures — see Darling's twin. */
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
                query_text = full_text ? r.QueryText : McpHelpers.Truncate(r.QueryText, QueryTextPreviewLength),
                query_text_truncated = !full_text && (r.QueryText?.Length ?? 0) > QueryTextPreviewLength
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
                total_snapshots = populationCount,
                snapshots_returned = result.Count,
                truncated,
                order = "collection_time_desc",
                oldest_returned_collection_time = shown[^1].CollectionTime.ToString("o"),
                newest_returned_collection_time = shown[0].CollectionTime.ToString("o"),
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_active_queries", ex);
        }
    }

    /// <summary>The one reason a victim's head blocker is not on the page, or null — Darling's twin's ladder:
    /// never captured beats filtered beats past the page, because each later reason presupposes the earlier
    /// one did not apply.</summary>
    private static string? BlockerNotShown(QuerySnapshotRow row, HashSet<(DateTime, int)> onPage)
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

    [McpServerTool(Name = "get_session_stats"), Description("Gets connection and session statistics grouped by application. Shows connection counts, running/sleeping/dormant breakdown, and aggregate resource usage per application. LATEST IS A TIME: this reads the newest session snapshot, not a window, and captured_at is the instant it was collected - the connection counts are what was connected AT that stamp, not a peak or an average over anything.")]
    public static async Task<string> GetSessionStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestSessionStatsAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "session_stats")
                    ?? McpHelpers.Status("unavailable", "No session statistics available. The session collector may not have run yet.");

            var totalConnections = rows.Sum(r => r.ConnectionCount);
            var totalRunning = rows.Sum(r => r.RunningCount);
            var totalSleeping = rows.Sum(r => r.SleepingCount);
            var totalDormant = rows.Sum(r => r.DormantCount);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3653: captured_at, the #3637 census's one spelling for a latest read's stamp - see
                   McpServerInfoTools.GetServerProperties for why it is a cut-over and not an alias. */
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
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_session_stats", ex);
        }
    }
}
