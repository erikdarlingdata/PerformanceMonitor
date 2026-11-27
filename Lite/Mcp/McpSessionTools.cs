using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpSessionTools
{
    /// <summary>#4198: see Darling's <c>DarlingMcpSessionTools.QueryTextPreviewLength</c> twin for the
    /// measured bytes both defaults come down from.</summary>
    private const int QueryTextPreviewLength = 400;

    /// <inheritdoc cref="QueryTextPreviewLength"/>
    private const int DefaultLimit = 25;

    [McpServerTool(Name = "get_active_queries"), Description("Active query snapshots (sys.dm_exec_requests) in a window ending at as_of: text, wait, CPU, blocking, DOP. database_name/blocking_only filter IN SQL: total_snapshots is the filtered count; truncated means over limit or the 32 KB budget (budget_truncated). wait_time_ms, dop, granted_query_memory_gb, open_transaction_count: null = zero or not applicable. Head blockers are never stripped (is_head_blocker); blocker_not_shown is not_captured, filtered, or past_page. Empty: no snapshot in the window, or none matching filters; not_collected (unfiltered only): engine can't run it. <<GUIDE>> query_text is a preview by default (query_text_truncated: true), pass full_text for the whole text. Gets active query snapshots captured from sys.dm_exec_requests. Shows what queries were running at each collection point: session ID, query text, wait type, CPU time, elapsed time, blocking info, DOP, and memory grants. Use hours_back to look at a specific time window: critical for finding what was running during a CPU spike or blocking event. EVERY FILTER IS PART OF THE QUERY: database_name, blocking_only and wait_type are applied in SQL before the page is cut, total_snapshots is the count of snapshot rows in the window that pass your filters, snapshots_returned is how many you got, and truncated says the filtered population held more than limit, raise limit or narrow hours_back when it is true (NEWEST CAPTURE FIRST, highest CPU first within a capture; oldest_returned_collection_time / newest_returned_collection_time bound the page). Applied in SQL, so total_snapshots counts the blocking population and truncated is measured against it. HEAD BLOCKERS ARE NEVER STRIPPED: a session another row in the same capture names as its blocker is on the page whatever its text (including a WAITFOR shell holding locks). is_head_blocker marks one that nothing blocks in turn: in a chain, only the top session has it. A victim whose blocker is NOT on the page says why in blocker_not_shown: not_captured (the blocker held no running request at that capture, an idle open transaction is the classic case; get_blocked_process_reports has its input buffer from the blocked-process report), filtered (your database_name or wait_type filter excluded it), or past_page (it is in the filtered population but beyond limit). When the store did not cover the whole window, effective_start, window_truncated and truncation_note say where its data starts. A row waiting on a page latch (PAGELATCH_ or PAGEIOLATCH_) carries page_latch: page_kind names the fixed allocation and header pages by their page number (file header, PFS, GAM, SGAM, DCM, BCM, boot page) and calls every other page data or index page, which can still be an IAM or system table page; for a data or index page the indexes come from the statement's own stored plan, ranked by the daily index snapshot's page latch waits, with match saying exact (the plan names one index in that database), plan or inferred; an index the snapshot has no row for shows null latch_wait_ms and latch_wait_count." + McpHelpers.UtcStampsDescription)]
    public static async Task<string> GetActiveQueries(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to retrieve. Default 1.")] int hours_back = 1,
        [Description("Filter to one database, in SQL; a head blocker in ANOTHER database is then not on the page, and its victims say blocker_not_shown = filtered.")] string? database_name = null,
        [Description("Show only queries involved in blocking: rows with blocking_session_id > 0, plus the head blockers those rows name in the same capture.")] bool blocking_only = false,
        [Description("Only rows waiting on this wait type at capture, e.g. PAGEIOLATCH_SH: exact name, any case. Applied in SQL. A head blocker on another wait is then off the page (blocker_not_shown = filtered).")] string? wait_type = null,
        [Description("Maximum number of rows to return. Default 25 (down from 50, sized to fit the response budget). The page is bounded by limit, not hours_back: truncated says whether the window held more.")] int limit = DefaultLimit,
        [Description("Return each row's full query text instead of a 400-character preview. Default false.")] bool full_text = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(QueryTagReadScope.ParameterDescription)] string? tag = null,
        [Description(McpLongQueryTools.IncludeMonitorDescription)] bool include_monitor = false)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            /* #5992: the query tag filter. A name no tag carries is an empty answer that says so. */
            var tagScope = await dataService.GetQueryTagReadScopeAsync(resolved.ServerId, tag);
            if (tagScope.Unknown)
            {
                return McpHelpers.Status("empty", QueryTagReadScope.UnknownTagMessage(tagScope.Requested!));
            }

            var filter = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            /* #5235: the wait filter is trimmed the same way, so a blank is none and the echo below names the value that was applied. */
            var waitFilter = string.IsNullOrWhiteSpace(wait_type) ? null : wait_type.Trim();

            /* #3541 A13: the filters ride INTO the read (GetActiveQueriesPageAsync), asked for one row past
               the cap so truncation is OBSERVED on the filtered population; total_snapshots is that read's
               COUNT(*) OVER () of the same population. Darling's twin is DarlingSessionReader.ActiveQueriesSql. */
            var (rows, populationCount) = await dataService.GetActiveQueriesPageAsync(
                resolved.ServerId, hours_back, limit + 1, filter, blocking_only, waitFilter, asOfUtc: windowEnd, tags: tagScope, includeMonitor: include_monitor);

            /* #4966: where this server's query_snapshots start for the window, beside the page cut below — the two
               are different facts: truncated says the window held more than limit, this says the store did not hold
               the window's head. The probe reads coverage (the collector's logged runs count as well as rows: a
               server idle overnight has no snapshot near the start though the store covered it), from the same
               view the page was read from, and is deliberately unfiltered by database_name / blocking_only: the
               floor is a property of the table. Read ahead of the empty answers, which carry it too: an empty
               answer over a window the store does not reach back to is not a true negative. */
            var requestedStart = windowEnd.AddHours(-hours_back);
            var notice = await McpQueryTools.WindowNoticeAsync(
                () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QuerySnapshots, resolved.ServerId, requestedStart, windowEnd),
                requestedStart, windowEnd, "query_snapshots", emptyAnswer: rows.Count == 0);

            if (rows.Count == 0)
            {
                /* A filtered miss is not a collection miss — Darling's twin's words. */
                if (filter != null || blocking_only || waitFilter != null || tagScope.Active)
                {
                    return await McpQueryTools.OfflineInsteadOfEmptyAsync(dataService, resolved.ServerId, windowEnd, McpHelpers.Status(
                        "empty",
                        $"No active query snapshots on {resolved.DisplayName} {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))} matched "
                        + DescribeActiveQueryFilters(filter, blocking_only, waitFilter)
                        + (tagScope.Active ? $" (query tag '{tagScope.Requested}')" : "")
                        + ". The filters were applied in SQL over the whole window, so unfiltered snapshots may well exist: drop them to see what the window holds."
                        + (include_monitor ? "" : " The monitor's own statements are left out. Pass include_monitor=true to include them."),
                        notice.AsHints()), requestedStart, "query_snapshots");
                }

                return await McpEngineCapability.NotCollectedStatusAsync(dataService, serverManager, resolved.ServerId, resolved.DisplayName, "query_snapshots")
                    ?? await McpQueryTools.OfflineInsteadOfEmptyAsync(dataService, resolved.ServerId, windowEnd,
                        McpHelpers.Status("empty", "No active query snapshots found in the requested time range." + (include_monitor ? "" : " The monitor's own statements are left out. Pass include_monitor=true to include them."), notice.AsHints()), requestedStart, "query_snapshots");
            }

            var truncated = rows.Count > limit;
            var shown = truncated ? rows.GetRange(0, limit) : rows;

            /* The page's own (capture, session) pairs, so a victim can say whether its blocker made the page. */
            var onPage = new HashSet<(DateTime, int)>(shown.Select(r => (r.CollectionTime, r.SessionId)));

            /* #9055: a page latch wait names its page in wait_resource. The page kind, and the indexes the row's own stored plan touches
               ranked by the daily index snapshot, are read from the store here, for the page's page latch rows only (Darling's twin is
               DarlingPageLatchReader). */
            var pageLatch = await dataService.DescribePageLatchWaitsAsync(resolved.ServerId, shown);

            var result = shown.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                session_id = r.SessionId,
                database_name = r.DatabaseName,
                status = r.Status,
                cpu_time_ms = r.CpuTimeMs,
                /* The engine's elapsed-time counter is a 32-bit int: a row at its ceiling is "at least this old", so both fields are null and the note says why. */
                elapsed_time_ms = ElapsedTimeCeiling.IsAtCeiling(r.TotalElapsedTimeMs) ? (long?)null : r.TotalElapsedTimeMs,
                elapsed_time_formatted = ElapsedTimeCeiling.IsAtCeiling(r.TotalElapsedTimeMs) ? null : r.ElapsedTimeFormatted,
                elapsed_time_note = ElapsedTimeCeiling.IsAtCeiling(r.TotalElapsedTimeMs) ? ElapsedTimeCeiling.Note : null,
                logical_reads = r.LogicalReads,
                reads = r.Reads,
                writes = r.Writes,
                wait_type = string.IsNullOrEmpty(r.WaitType) ? null : r.WaitType,
                /* #9055: the lock or page the request waits on, as Darling's twin returns it. */
                wait_resource = string.IsNullOrEmpty(r.WaitResource) ? null : r.WaitResource,
                wait_time_ms = r.WaitTimeMs > 0 ? r.WaitTimeMs : (long?)null,
                blocking_session_id = r.BlockingSessionId > 0 ? r.BlockingSessionId : (int?)null,
                /* #3541 A13: the two blocking disclosures — see Darling's twin. */
                is_head_blocker = r.IsHeadBlocker && (r.BlockingSessionId <= 0 || r.BlockingSessionId == r.SessionId) ? true : (bool?)null,
                blocker_not_shown = BlockerNotShown(r, onPage),
                dop = r.Dop > 0 ? r.Dop : (int?)null,
                parallel_worker_count = r.ParallelWorkerCount > 0 ? r.ParallelWorkerCount : (int?)null,
                granted_query_memory_gb = r.GrantedQueryMemoryGb > 0 ? r.GrantedQueryMemoryGb : (double?)null,
                transaction_isolation_level = string.IsNullOrEmpty(r.TransactionIsolationLevel) ? null : r.TransactionIsolationLevel,
                open_transaction_count = r.OpenTransactionCount > 0 ? r.OpenTransactionCount : (int?)null,
                login_name = r.LoginName,
                host_name = r.HostName,
                program_name = r.ProgramName,
                tag = tagScope.TagOfHash(r.QueryHash),
                query_text = full_text ? r.QueryText : McpHelpers.TruncateStatement(r.QueryText, QueryTextPreviewLength),
                query_text_truncated = !full_text && (r.QueryText?.Length ?? 0) > QueryTextPreviewLength
            }).ToList();

            var activePayload = JsonSerializer.SerializeToNode(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* #4966: the window floor, always present (false and null when the store covered the window). No
                   effective_hours_back: this payload carries a page `truncated`, and the census holds that key
                   apart for the window floor (McpPayloadContractCensusTests), so the reach is the instant. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                filters_applied = new
                {
                    database_name = filter,
                    blocking_only,
                    wait_type = waitFilter,
                },
                /* The FILTERED population's size, computed in SQL on the same statement as the rows. */
                total_snapshots = populationCount,
                snapshots_returned = result.Count,
                truncated,
                order = "collection_time_desc",
                /* #4966: where the page's rows stop describes the window the page covers, so it prints like
                   effective_start (UTC, with the Z), and so does the newest bound (#5015); the rows' own times stay as the store holds them. */
                oldest_returned_collection_time = McpHelpers.FormatEffectiveStart(shown[^1].CollectionTime),
                newest_returned_collection_time = McpHelpers.FormatEffectiveStart(shown[0].CollectionTime),
                queries = result
            }, McpHelpers.JsonOptions)!.AsObject();

            /* #5984 round 1 (Darling's twin): the tag is part of the population total_snapshots counts, so filters_applied names it (only when a tag was sent). */
            if (tagScope.Named is { } sessionTagName && activePayload["filters_applied"] is System.Text.Json.Nodes.JsonObject appliedFilters)
            {
                appliedFilters["tag"] = sessionTagName;
            }

            /* #9055: the page latch answer, in place on the row it describes (the rows are in `shown`'s order). */
            if (activePayload["queries"] is System.Text.Json.Nodes.JsonArray queryRows)
            {
                for (var i = 0; i < shown.Count && i < queryRows.Count; i++)
                {
                    if (queryRows[i] is System.Text.Json.Nodes.JsonObject pageRow && pageLatch.TryGetValue((shown[i].CollectionTime, shown[i].SessionId, shown[i].RequestId), out var latchAnswer))
                        pageRow["page_latch"] = latchAnswer.ToJson();
                }
            }

            /* The newest snapshot rows that fit the response budget: rows are left off the end whole, never cut, and every field that
               describes the page (the count, the span, each victim's blocker_not_shown) is that of the rows it kept. `truncated` is true
               for either cut, the row limit or the budget; budget_truncated is the budget alone, as on Darling's twin. */
            return McpResponseBudget.CapArray(
                activePayload, "queries", "newest capture first", "snapshot rows",
                _ => "Pass as_of at oldest_returned_collection_time to read on from there, or narrow the page with database_name, wait_type or blocking_only.",
                McpResponseBudget.DefaultBytes, "budget_truncated", "budget_note",
                (kept, count) =>
                {
                    kept["snapshots_returned"] = count;
                    kept["truncated"] = truncated || count < shown.Count;
                    kept["oldest_returned_collection_time"] = McpHelpers.FormatEffectiveStart(shown[count - 1].CollectionTime);
                    kept["newest_returned_collection_time"] = McpHelpers.FormatEffectiveStart(shown[0].CollectionTime);

                    /* A victim whose blocker was cut from the page says past_page, not nothing. */
                    var keptOnPage = new HashSet<(DateTime, int)>(shown.Take(count).Select(r => (r.CollectionTime, r.SessionId)));
                    if (kept["queries"] is System.Text.Json.Nodes.JsonArray keptRows)
                    {
                        for (var i = 0; i < keptRows.Count && i < shown.Count; i++)
                        {
                            if (keptRows[i] is System.Text.Json.Nodes.JsonObject keptRow && BlockerNotShown(shown[i], keptOnPage) is { } reason)
                                keptRow["blocker_not_shown"] = reason;
                        }
                    }
                });
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

    /// <summary>
    /// Names the active filters for the filtered-miss sentence, in the caller's own vocabulary, joined with " with ":
    /// database_name, then blocking_only, then wait_type (#5235). Darling's twin says the same words.
    /// </summary>
    private static string DescribeActiveQueryFilters(string? databaseName, bool blockingOnly, string? waitType)
    {
        var parts = new List<string>(3);
        if (databaseName != null)
            parts.Add($"database_name '{databaseName}'");
        if (blockingOnly)
            parts.Add("blocking_only");
        if (waitType != null)
            parts.Add($"wait_type '{waitType}'");
        return string.Join(" with ", parts);
    }

    [ReadsLatestSnapshot(LatestSnapshotShape.Stamped)]
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
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "session_stats", awsRdsKind: AwsRdsKind.None)
                    ?? McpHelpers.Status("unavailable", "No session statistics available. The session collector may not have run yet.");

            var totalConnections = rows.Sum(r => r.ConnectionCount);
            var totalRunning = rows.Sum(r => r.RunningCount);
            var totalSleeping = rows.Sum(r => r.SleepingCount);
            var totalDormant = rows.Sum(r => r.DormantCount);

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
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
