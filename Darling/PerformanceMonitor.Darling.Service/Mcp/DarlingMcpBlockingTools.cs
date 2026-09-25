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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The blocking / deadlock diagnostic-depth MCP tools — get_blocking, get_deadlocks,
/// get_deadlock_detail, get_blocked_process_xml — served over Darling's Postgres store. Each tool body
/// mirrors LITE's <c>McpBlockingTools</c> field-for-field (the store-faithful shape Darling's
/// collector-mirror schema can serve, the precedent slices 1+2 set), keeping the tool names + parameter
/// contracts the Dashboard and Lite expose. Reads flow through <see cref="DarlingBlockingReader"/> — a
/// STORED read (no live monitored-server hit) that reproduces the viewer's XE-preferred + DMV-fallback
/// merge and the parsed deadlock process-summary.
///
/// <para>
/// get_blocking carries the Dashboard tool name + params but returns the store-native blocked/blocking
/// pair rows (the shape Lite's get_blocked_process_reports emits) — the Dashboard's pre-computed
/// <c>blocking_tree</c> / <c>wait_time_sec</c> / <c>activity</c> come from its SQL-Server-side wide store
/// and have no column in the Postgres store, so following LITE here keeps the result honest to the
/// collected data. get_blocking_deadlock_stats is intentionally NOT hosted: Darling has no
/// <c>blocking_deadlock_stats</c> aggregate table (the Dashboard populates its <c>collect.blocking_deadlock_stats</c>
/// via a T-SQL analyzer that has no Darling port) and the delta / victim-wait columns have no raw source —
/// a collection gap reported for a later slice rather than faked.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpBlockingTools
{
    [McpServerTool(Name = "get_blocking"), Description("Blocked process report XE + DMV fallback events, newest first, window ends at as_of. not_collected wins if the engine can't run blocked_process_report; else empty means none in the window, or none collected in it. limit caps ROWS, not hours_back: truncated true means raise limit or narrow the window, not widen hours_back. dedup_key scans the whole window before limit, up to a stated ceiling (rows_examined/scan_truncated); a no-match answer is still empty. wait_time_ms is milliseconds. Timestamps are UTC; last_tran/last_batch stamps are de-skewed for direct comparison to event_time.<<GUIDE>>Gets blocking events captured by the blocked process report extended event (plus the always-on DMV blocking-snapshot fallback), NEWEST FIRST. Shows the blocked and blocking sessions, wait types, wait times, and query text for both. Use this first for a quick overview, then use get_blocked_process_xml for deep analysis of prolonged blocking. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: hours_back is the window you ASKED for, events_returned is how many rows you GOT, truncated says the window held more than limit, and oldest_returned_event_time / newest_returned_event_time bound the page you are looking at. Because the page is a contiguous newest-first slice, oldest_returned_event_time IS how far back this read reached — on a server blocking steadily, a 24-hour request at the default limit is answered by the newest few minutes, and nothing in the rows themselves says so. When truncated is true, raise limit or narrow hours_back (or anchor as_of) before drawing a conclusion about the window; widening hours_back cannot help, because the cap is on rows, not time. With dedup_key the read scans the window for the fingerprint BEFORE limit applies (so a matching incident is never lost to the cap), up to a stated scan ceiling: rows_examined is how many rows were fingerprinted and scan_truncated says whether the window held more than the scan could reach. Every timestamp here is UTC: event_time already was, and the six blocked_/blocking_ last_tran/last_batch stamps are de-skewed from the monitored server's local clock by this read, so comparing them against event_time to see whether a transaction predates the block is direct. dedup_key: Optional alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects. The fingerprint scan runs over the window BEFORE limit, up to the scan ceiling the payload reports as rows_examined / scan_truncated.")]
    public static async Task<string> GetBlocking(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 30. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 30,
        [Description("Optional alert fingerprint (the alert's Dedup Key). The key is scoped to the server's display name and the incident's involved objects. The fingerprint scan runs over the window BEFORE limit.")] string? dedup_key = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveWithFingerprintNameAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var filtering = !DarlingIncidentFingerprint.NoFilter(dedup_key);

            /*
                #3541 A3: the cap is the CALLER'S, and truncation is OBSERVED rather than inferred.

                The reader used to cap at 200 rows newest-first whatever `limit` said, and this tool then
                took `limit` of those and published the 200 as `total_events`. Two lies in one payload: the
                count was neither the window's total nor the page's, and a 24-hour request on a server
                blocking steadily was answered from its newest few minutes with nothing saying so. Fetching
                limit + 1 and reading the extra row as the signal is the pattern get_collection_log and
                get_query_heatmap already use; comparing count to the cap cannot tell a window holding
                exactly `limit` events from one holding more.

                Under a dedup_key the fetch is the FINGERPRINT SCAN, not the page: #2159's promise is that
                the filter runs over the window before `limit`, and the hidden 200-row cap was quietly
                breaking it for every incident older than the newest 200 rows. The scan is bounded by a
                STATED ceiling, over-fetched by one for the same reason, so the no-match answer can say the
                scan ran out rather than implying the window was searched.
            */
            var fetch = filtering ? DarlingBlockingReader.FingerprintScanCeiling + 1 : limit + 1;
            var rows = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, fetch);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    ?? McpHelpers.Status("empty", "No blocking events found in the specified time range.");

            var scanTruncated = filtering && rows.Count > DarlingBlockingReader.FingerprintScanCeiling;
            if (scanTruncated) rows = rows.Take(DarlingBlockingReader.FingerprintScanCeiling).ToList();

            /* #2159: fingerprint the WHOLE scan, then filter, then cap. Capping first would let `limit`
               discard the very incident the key names — the caller asked for one specific incident, not for
               the newest `limit` rows that happen to include it. Without a key the scan IS the page plus its
               one sentinel row, so the keys computed here are the ones the page emits. */
            var examined = rows.Count;
            var keys = DarlingIncidentFingerprint.BlockingKeys(
                resolved.FingerprintName,
                rows.Select(r => new BlockingIncidentGrouper.BlockedEvent(
                    r.DatabaseName, r.ContentiousObject, r.BlockedSqlText, r.BlockingSqlText,
                    r.WaitTimeMs, r.LockMode)).ToList());

            if (filtering)
            {
                var wanted = DarlingIncidentFingerprint.NormalizeKey(dedup_key);
                var kept = rows.Where((_, i) => keys[i] == wanted).ToList();
                if (kept.Count == 0)
                    return McpHelpers.Status("empty", DarlingIncidentFingerprint.NoMatchMessage(
                        "blocking events", dedup_key!, resolved.FingerprintName, examined)
                        + ScanCeilingClause(scanTruncated));

                keys = kept.Select(r => wanted).Cast<string?>().ToList();
                rows = kept;
            }

            /* The page: `limit` rows of whatever survived, and the row past it is the truncation signal.
               Under a key this is "more matching rows than limit"; without one it is "more rows in the
               window than limit" — the same field, and both sentences are true of what it measures. */
            var truncated = rows.Count > limit;
            var page = rows.Take(limit).ToList();

            var result = page.Select((r, i) => new
            {
                event_time = r.EventTime?.ToString("o"),
                source = r.Source,
                database_name = r.DatabaseName,
                blocked_spid = r.BlockedSpid,
                blocked_ecid = r.BlockedEcid,
                blocking_spid = r.BlockingSpid,
                blocking_ecid = r.BlockingEcid,
                wait_time_ms = r.WaitTimeMs,
                wait_resource = r.WaitResource,
                lock_mode = r.LockMode,
                contentious_object = string.IsNullOrEmpty(r.ContentiousObject) ? null : r.ContentiousObject,
                blocked_status = r.BlockedStatus,
                blocked_isolation_level = r.BlockedIsolationLevel,
                blocked_log_used = r.BlockedLogUsed,
                blocked_transaction_count = r.BlockedTransactionCount,
                blocked_client_app = r.BlockedClientApp,
                blocked_host_name = r.BlockedHostName,
                blocked_login_name = r.BlockedLoginName,
                blocked_sql_text = McpHelpers.Truncate(r.BlockedSqlText, 2000),
                blocking_status = r.BlockingStatus,
                blocking_isolation_level = r.BlockingIsolationLevel,
                blocking_client_app = r.BlockingClientApp,
                blocking_host_name = r.BlockingHostName,
                blocking_login_name = r.BlockingLoginName,
                blocking_sql_text = McpHelpers.Truncate(r.BlockingSqlText, 2000),
                blocked_transaction_name = r.BlockedTransactionName,
                blocking_transaction_name = r.BlockingTransactionName,
                blocked_last_tran_started = r.BlockedLastTranStartedUtc?.ToString("o"),
                blocking_last_tran_started = r.BlockingLastTranStartedUtc?.ToString("o"),
                blocked_last_batch_started = r.BlockedLastBatchStartedUtc?.ToString("o"),
                blocking_last_batch_started = r.BlockingLastBatchStartedUtc?.ToString("o"),
                blocked_last_batch_completed = r.BlockedLastBatchCompletedUtc?.ToString("o"),
                blocking_last_batch_completed = r.BlockingLastBatchCompletedUtc?.ToString("o"),
                blocked_priority = r.BlockedPriority,
                blocking_priority = r.BlockingPriority,
                has_report_xml = r.HasReportXml,
                dedup_key = keys[i]
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* The span REQUESTED. Kept under its shipped name, and no longer the only span on the page. */
                hours_back,
                dedup_key = filtering ? DarlingIncidentFingerprint.NormalizeKey(dedup_key) : null,
                /*
                    #3541 A3: `total_events` is gone. It was the reader's capped row count — neither the
                    window's total nor the page's — under a name that promised the first. What is published
                    now is what was measured: how many rows this page holds, whether the window held more,
                    and the time span the page actually covers. Newest-first makes the page a contiguous
                    slice of the window's tail, so oldest_returned_event_time IS the reach of this read —
                    the #3287 figure, and the field a caller has to read before believing that a quiet page
                    describes a quiet window. Under a dedup_key the page is the matching rows and the two
                    stamps bound the INCIDENT rather than the reach; the scan fields below carry the reach.
                */
                events_returned = page.Count,
                truncated,
                /* Min/Max over the rows rather than rows[0] / rows[^1]: those coincide only under time
                   ordering, and a dedup_key page is the matching rows rather than a contiguous slice.
                   Enumerable.Min over DateTime? skips nulls and yields null for a page with no stamps. */
                oldest_returned_event_time = page.Min(r => r.EventTime)?.ToString("o"),
                newest_returned_event_time = page.Max(r => r.EventTime)?.ToString("o"),
                order = "event_time_desc",
                /* The fingerprint scan, stated only when one ran: how many window rows were fingerprinted
                   and whether the window held more than the scan could reach. Null rather than 0 without a
                   key, because no scan was made — 0 would read as "a scan found nothing". */
                rows_examined = filtering ? examined : (int?)null,
                scan_truncated = filtering ? scanTruncated : (bool?)null,
                events = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking", ex);
        }
    }

    /// <summary>
    /// The sentence appended to a no-match answer when the fingerprint scan hit
    /// <see cref="DarlingBlockingReader.FingerprintScanCeiling"/>. Empty otherwise, so the shared
    /// <see cref="DarlingIncidentFingerprint.NoMatchMessage"/> stays word-for-word what it was for a scan that
    /// did cover the window — the three causes it names are the whole story in that case, and this one is the
    /// fourth that only exists once the scan can run out.
    /// </summary>
    private static string ScanCeilingClause(bool scanTruncated) =>
        scanTruncated
            ? $" The window held MORE rows than the {DarlingBlockingReader.FingerprintScanCeiling}-row fingerprint scan could reach, so this is not proof the incident is absent from the window — anchor as_of at the alert time with a narrow hours_back and retry."
            : string.Empty;

    [McpServerTool(Name = "get_deadlocks"), Description("Recent deadlock events with victim process info, newest first, window ends at as_of. Use get_deadlock_detail for the graph XML. not_collected wins if the engine can't run deadlocks; then precondition names a fixable gap (e.g. XE session gone); else empty means none in the window, or none collected in it. limit caps ROWS, not hours_back: truncated true means raise limit or narrow the window, not widen hours_back. Darling: dedup_key scans the whole window before limit, up to a stated ceiling (rows_examined/scan_truncated); a no-match answer is still empty. <<GUIDE>> Gets recent deadlock events with victim process info, NEWEST FIRST. Deadlocks occur when two or more sessions permanently block each other. Use get_deadlock_detail for the full deadlock graph XML. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: deadlocks_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_deadlock_time / newest_returned_deadlock_time bound the page — under the newest-first ordering the oldest stamp IS how far back this read reached, so a truncated page says nothing about the earlier part of the window. Raise limit or narrow hours_back when truncated is true; widening hours_back cannot help, because the cap is on rows. With dedup_key the fingerprint scan runs over the window BEFORE limit, up to a stated ceiling (rows_examined / scan_truncated). dedup_key: Optional alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects. The fingerprint scan runs over the window BEFORE limit, up to the scan ceiling the payload reports as rows_examined / scan_truncated.")]
    public static async Task<string> GetDeadlocks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 20. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 20,
        [Description("Optional alert fingerprint (the alert's Dedup Key). The key is scoped to the server's display name and the incident's involved objects. The fingerprint scan runs over the window BEFORE limit.")] string? dedup_key = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveWithFingerprintNameAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var filtering = !DarlingIncidentFingerprint.NoFilter(dedup_key);

            /* #3541 A3: see get_blocking — the caller's limit + 1 as the page fetch, the stated scan ceiling
               + 1 as the fingerprint fetch, and the extra row in either case as the observed signal. The
               reader's own cap was 50 here, which a caller asking for 100 deadlocks never saw. */
            var fetch = filtering ? DarlingBlockingReader.FingerprintScanCeiling + 1 : limit + 1;
            var rows = await DarlingBlockingReader.GetRecentDeadlocksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, fetch);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks")
                    /* #2546: capability first (permanent), then the runtime precondition (fixable), then the
                       read's own miss. A deadlock capture whose XE session is gone records SESSION_MISSING
                       and then returns zero rows forever, which is byte-identical to a server that simply
                       did not deadlock — the one answer nobody should be given without being told. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks")
                    ?? McpHelpers.Status("empty", "No deadlocks found in the specified time range.");

            var scanTruncated = filtering && rows.Count > DarlingBlockingReader.FingerprintScanCeiling;
            if (scanTruncated) rows = rows.Take(DarlingBlockingReader.FingerprintScanCeiling).ToList();

            /* #2159: see get_blocking — fingerprint the scan, filter, then cap. */
            var examined = rows.Count;
            var keys = DarlingIncidentFingerprint.DeadlockKeys(
                resolved.FingerprintName, rows.Select(r => r.DeadlockGraphXml));

            if (filtering)
            {
                var wanted = DarlingIncidentFingerprint.NormalizeKey(dedup_key);
                var kept = rows.Where((_, i) => keys[i] == wanted).ToList();
                if (kept.Count == 0)
                    return McpHelpers.Status("empty", DarlingIncidentFingerprint.NoMatchMessage(
                        "deadlocks", dedup_key!, resolved.FingerprintName, examined)
                        + ScanCeilingClause(scanTruncated));

                keys = kept.Select(r => wanted).Cast<string?>().ToList();
                rows = kept;
            }

            var truncated = rows.Count > limit;
            var page = rows.Take(limit).ToList();

            var result = page.Select((r, i) => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                deadlock_time = r.DeadlockTime?.ToString("o"),
                victim_process_id = r.VictimProcessId,
                victim_sql_text = McpHelpers.Truncate(r.VictimSqlText, 2000),
                process_summary = r.ProcessSummary,
                has_deadlock_xml = r.HasDeadlockXml,
                dedup_key = keys[i]
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                dedup_key = filtering ? DarlingIncidentFingerprint.NormalizeKey(dedup_key) : null,
                /* #3541 A3: the page described as a page — see get_blocking for why `total_deadlocks` went.
                   The ORDER BY is deadlock_time, so the bounds are on that stamp rather than collection_time,
                   and a deadlock whose stamp did not parse (null) is skipped by Min/Max rather than read as
                   the epoch. */
                deadlocks_returned = page.Count,
                truncated,
                oldest_returned_deadlock_time = page.Min(r => r.DeadlockTime)?.ToString("o"),
                newest_returned_deadlock_time = page.Max(r => r.DeadlockTime)?.ToString("o"),
                order = "deadlock_time_desc",
                rows_examined = filtering ? examined : (int?)null,
                scan_truncated = filtering ? scanTruncated : (bool?)null,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlocks", ex);
        }
    }

    /// <summary>
    /// #4198: deadlock_graph_xml is the wide field on this tool — a busy production store's default call
    /// (limit 5, only 3 deadlocks carried a graph in the window) measured 120,454 bytes, almost all of it
    /// this one field. Previewed to this length per graph at default (<c>full_graph: true</c> opts back
    /// in), the same preview-plus-opt-in shape <c>get_store_query_stats</c> uses for <c>full_text</c>. A
    /// <c>dedup_key</c> narrows the page to one named incident, so that call is exempt from the preview —
    /// the caller already paid the cost of naming it and came for the graph.
    /// </summary>
    private const int DeadlockGraphPreviewLength = 2000;

    [McpServerTool(Name = "get_deadlock_detail"), Description("Gets the deadlock graph XML for a specific time range, NEWEST FIRST. Returns the raw XML that can be analyzed for lock resources, process details, and deadlock chains. Only deadlocks that CARRY a graph are counted against limit, so the page is limit graphs rather than limit rows; deadlocks_returned, truncated and oldest_returned_deadlock_time / newest_returned_deadlock_time describe the page the same way get_deadlocks does, and truncated means the window held more graphs than limit. deadlock_graph_xml is a preview by default (deadlock_graph_xml_truncated: true) — pass full_graph for the whole graph; a dedup_key call always gets the whole graph.")]
    public static async Task<string> GetDeadlockDetail(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum deadlocks WITH a graph to return, newest first. Default 5. Read truncated to know whether the window held more.")] int limit = 5,
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null,
        [Description("Return each graph's full XML instead of a 2000-character preview. Default false. A dedup_key call ignores this and always returns the full graph.")] bool full_graph = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveWithFingerprintNameAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;

            var filtering = !DarlingIncidentFingerprint.NoFilter(dedup_key);

            /*
                #3541 A3: the graph predicate moved INTO the SQL (graphOnly), so the page fetch can be the
                caller's limit + 1 over exactly the rows this tool can return. It used to filter
                has_deadlock_xml in C# after a fixed 50-row fetch, so a caller asking for five graphs had
                at most fifty rows to find them in, and a run of graph-less rows at the newest end read as
                "no deadlock XML in the window" while older graphs sat behind the cap. Under a dedup_key the
                fetch is the stated fingerprint scan ceiling + 1, as on get_deadlocks.
            */
            var fetch = filtering ? DarlingBlockingReader.FingerprintScanCeiling + 1 : limit + 1;
            var candidates = await DarlingBlockingReader.GetRecentDeadlocksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, fetch, graphOnly: true);
            var scanTruncated = filtering && candidates.Count > DarlingBlockingReader.FingerprintScanCeiling;
            if (scanTruncated) candidates = candidates.Take(DarlingBlockingReader.FingerprintScanCeiling).ToList();

            /* #2159: the XML filter runs BEFORE the cap and before the fingerprint, because a row without a
               graph has no objects to fingerprint — it could never match a key, and including it would only
               consume one of the `limit` slots the caller wanted spent on real graphs. It is now the SQL's
               predicate rather than a Where() here, for the reason above. */
            if (candidates.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks")
                    ?? McpHelpers.Status("empty", "No deadlock XML available in the specified time range.");

            var examined = candidates.Count;
            var keys = DarlingIncidentFingerprint.DeadlockKeys(
                resolved.FingerprintName, candidates.Select(r => r.DeadlockGraphXml));

            if (filtering)
            {
                var wanted = DarlingIncidentFingerprint.NormalizeKey(dedup_key);
                var kept = candidates.Where((_, i) => keys[i] == wanted).ToList();
                if (kept.Count == 0)
                    return McpHelpers.Status("empty", DarlingIncidentFingerprint.NoMatchMessage(
                        "deadlocks with a graph", dedup_key!, resolved.FingerprintName, examined)
                        + ScanCeilingClause(scanTruncated));

                keys = kept.Select(r => wanted).Cast<string?>().ToList();
                candidates = kept;
            }

            var truncated = candidates.Count > limit;
            var withXml = candidates.Take(limit).ToList();

            /* #4198: filtering (a dedup_key) already narrowed the page to one named incident, so that call
               is exempt from the preview cut — see DeadlockGraphPreviewLength's doc comment. */
            var showFullGraph = full_graph || filtering;

            var result = withXml.Select((r, i) => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                deadlock_time = r.DeadlockTime?.ToString("o"),
                victim_process_id = r.VictimProcessId,
                dedup_key = keys[i],
                deadlock_graph_xml = showFullGraph ? r.DeadlockGraphXml : McpHelpers.Truncate(r.DeadlockGraphXml, DeadlockGraphPreviewLength),
                deadlock_graph_xml_truncated = !showFullGraph && r.DeadlockGraphXml.Length > DeadlockGraphPreviewLength
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                dedup_key = filtering ? DarlingIncidentFingerprint.NormalizeKey(dedup_key) : null,
                /* #3541 A3: the page bounds, on the same names as get_deadlocks. The page here is graphs, so
                   truncated means "more deadlocks WITH a graph than limit", which is the sentence this tool's
                   caller needs. */
                deadlocks_returned = withXml.Count,
                truncated,
                oldest_returned_deadlock_time = withXml.Min(r => r.DeadlockTime)?.ToString("o"),
                newest_returned_deadlock_time = withXml.Max(r => r.DeadlockTime)?.ToString("o"),
                order = "deadlock_time_desc",
                rows_examined = filtering ? examined : (int?)null,
                scan_truncated = filtering ? scanTruncated : (bool?)null,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlock_detail", ex);
        }
    }

    [McpServerTool(Name = "get_blocked_process_xml"), Description("Gets the raw blocked process report XML from extended events, NEWEST FIRST. Contains full detail about both the blocked and blocking sessions for deep analysis. Only rows that CARRY a report (the XE capture; the DMV fallback never has one) are counted against limit; reports_returned, truncated and oldest_returned_event_time / newest_returned_event_time describe the page the same way get_blocking does, and truncated means the window held more reports than limit.")]
    public static async Task<string> GetBlockedProcessXml(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum reports WITH XML to return, newest first. Default 5. Read truncated to know whether the window held more.")] int limit = 5,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;

            /* #3541 A3: same shape as get_deadlock_detail — the report-XML predicate is in the SQL, the XE
               arm alone is read (the DMV fallback never carries a report), and the fetch is the caller's
               limit + 1. It used to take the merged 200-row page and Where() it for XML in C#, so a caller
               asking for five reports had at most the newest 200 merged rows to find them in, DMV rows
               included, and nothing said so. */
            var candidates = await DarlingBlockingReader.GetRecentBlockedProcessReportsWithXmlAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1);
            var truncated = candidates.Count > limit;
            var withXml = candidates.Take(limit).ToList();
            if (withXml.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    /* #2546: same order and same reason as get_deadlocks — a blocked-process capture whose
                       session is gone is indistinguishable here from a server that never blocked. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    ?? McpHelpers.Status("empty", "No blocked process report XML available in the specified time range.");

            var result = withXml.Select(r => new
            {
                event_time = r.EventTime?.ToString("o"),
                database_name = r.DatabaseName,
                blocked_spid = r.BlockedSpid,
                blocking_spid = r.BlockingSpid,
                wait_time_ms = r.WaitTimeMs,
                blocked_process_report_xml = r.BlockedProcessReportXml
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #3541 A3: the page bounds, on get_blocking's names. truncated means "more reports WITH
                   XML in the window than limit". */
                reports_returned = withXml.Count,
                truncated,
                oldest_returned_event_time = withXml.Min(r => r.EventTime)?.ToString("o"),
                newest_returned_event_time = withXml.Max(r => r.EventTime)?.ToString("o"),
                order = "event_time_desc",
                reports = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocked_process_xml", ex);
        }
    }

    [McpServerTool(Name = "get_blocking_trend"), Description("Gets a time-series of blocking event counts per minute over time (blocked process reports, falling back to the always-on DMV blocking snapshot). Useful for identifying patterns (e.g., blocking spikes during batch jobs) or confirming whether blocking is a new, worsening, or resolved issue.")]
    public static async Task<string> GetBlockingTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);
            var points = await DarlingBlockingTrendReader.GetBlockingTrendAsync(
                postgres, resolved.ServerId, start, now);

            if (points.Count == 0)
            {
                /*
                    An empty trend is two facts, and the WRONG one is the reassuring one. "No blocking"
                    reads as an all-clear and a caller who believes it stops looking; "nothing collected"
                    means nothing at all is known about the window. The stored tables cannot tell them
                    apart -- both are an absence of rows in an EDGE table -- so the denominator has to come
                    from collection_log, which records a SUCCESS with zero rows for a collector that ran
                    and saw nothing. Probed only here, on the path that already found nothing.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report");
                if (gated != null)
                {
                    return gated;
                }

                var captures = await DarlingBlockingTrendReader.GetBlockingCaptureCountsAsync(
                    postgres, resolved.ServerId, start, now);
                return await EmptyTrend(
                    "blocking", resolved.ServerName, hours_back, captures,
                    () => DarlingBlockingTrendReader.HasAnyBlockingCollectorRunAsync(postgres, resolved.ServerId));
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = points.Select(p => new { time = p.Time.ToString("o"), count = p.Count })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking_trend", ex);
        }
    }

    [McpServerTool(Name = "get_deadlock_trend"), Description("Gets a time-series of deadlock event counts per minute over time. Useful for identifying patterns or confirming whether deadlock issues are new, worsening, or resolved.")]
    public static async Task<string> GetDeadlockTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);
            var points = await DarlingBlockingTrendReader.GetDeadlockTrendAsync(
                postgres, resolved.ServerId, start, now);

            if (points.Count == 0)
            {
                /* Same two facts as the blocking trend above, same denominator, same reason. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks");
                if (gated != null)
                {
                    return gated;
                }

                var captures = await DarlingBlockingTrendReader.GetDeadlockCaptureCountsAsync(
                    postgres, resolved.ServerId, start, now);
                return await EmptyTrend(
                    /* SINGULAR: the subject lands in "No {subject} was recorded", and "no deadlocks
                       was recorded" is not a sentence. It also reads correctly in the other two,
                       where it modifies the collector rather than the event. */
                    "deadlock", resolved.ServerName, hours_back, captures,
                    () => DarlingBlockingTrendReader.HasAnyDeadlockCollectorRunAsync(postgres, resolved.ServerId));
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = points.Select(p => new { time = p.Time.ToString("o"), count = p.Count })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlock_trend", ex);
        }
    }

    [McpServerTool(Name = "get_lock_wait_trend"), Description("Gets the AGGREGATE lock-wait rate over time for a server: every LCK% wait type's wait summed, in milliseconds per second, plus a wait_types legend naming which types waited. get_wait_trend charts ONE named wait type and get_blocking_trend counts incidents; this is the whole lock family at once, as a rate rather than a count. Use it to see whether a server's lock pressure is rising when no single wait type dominates, to tell a few long blocks from constant low-grade contention, and to pick which LCK type to hand to get_wait_trend next. Rates are over each collection's measured interval, so they compare across servers on different cadences.")]
    public static Task<string> GetLockWaitTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null) =>
        GetLockWaitTrend(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.LockWaitMaxPoints));

    /// <summary>
    /// get_lock_wait_trend under an explicit <paramref name="budget"/> (#3897): the MCP tool passes its own, the web
    /// viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>. The family is one series, so the width is
    /// settled before either read runs.
    /// </summary>
    internal static async Task<string> GetLockWaitTrend(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, int? bucket_minutes, TrendBudget budget)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
        if (bucketError != null) return bucketError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            var points = await DarlingBlockingTrendReader.GetLockWaitTrendAsync(
                postgres, resolved.ServerId, start, end, bucketMinutes);

            if (points.Count == 0)
            {
                /*
                    The denominator here is the DATA, not collection_log — the opposite of the two trends
                    above, and deliberately so. Those read EDGE tables, where a row exists only because
                    something went wrong, so a healthy server has none and a data probe would report it as
                    uncollected. wait_stats is PERIODIC: the collector writes a row every cycle for every
                    wait type it observes, whatever the server is doing, so the presence of ANY wait sample
                    is proof somebody looked. Probing v_wait_stats — the SAME relation this read walks, so it
                    can never report "collected" for rows the read cannot see — is both cheaper and more
                    precise than asking collection_log which collector ran.

                    The LCK% filter is deliberately NOT applied to the probe. A server that has collected
                    wait stats for months and never taken a lock wait is the all-clear this branch is for,
                    and filtering the probe the same way would call that server uncollected.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingDataReader.HasAnyWaitStatAsync(postgres, resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No lock waits recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected wait stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No wait stats have EVER been recorded for {resolved.ServerName}, so this is NOT a report of a server without lock contention — nothing has been stored for it at all. Delta wait stats need a SECOND collection cycle before the first row exists, so on a newly added server this clears itself; otherwise check that collection is running and that the server is enabled.");
            }

            /*
                #3897: the FAMILY as the series, the members as a legend. Until #3897 this published a row per
                (collection, LCK type) — every type the server had ever waited on, at every collection, 96% of
                them zero on DARLING01 — so the payload grew with the number of lock types and the window, and a
                week was 2 MB. The per-type split over time is what get_wait_trend serves for one named type;
                what this read owes is the family's shape and WHICH types made it, and the legend lists every
                type that waited in the window, so no member is dropped to fit a top-N.
            */
            var types = await DarlingBlockingTrendReader.GetLockWaitTypesAsync(postgres, resolved.ServerId, start, end);

            return TrendPayloads.LockWaitTrend(
                resolved.ServerName, hours_back, types, points, bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_lock_wait_trend", ex);
        }
    }

    /// <summary>
    /// The empty answer both per-minute trends give, and the whole point of #2485: <c>trend: []</c> is the
    /// same bytes on a server that had no blocking and on one that collected nothing, and an agent holding
    /// only the JSON cannot tell a clean bill of health from a hole in coverage.
    ///
    /// <para>Two statuses, picked by the denominator rather than by the edge rows. <c>empty</c> means captures
    /// ran in this window and none of them saw the event -- a real all-clear, bounded by the sampling
    /// interval. <c>unavailable</c> means no capture ran, so the window says nothing either way; the
    /// existence probe then separates a server that has never collected this at all from one with a GAP,
    /// because "check that collection is running" and "widen the window" are different next moves.</para>
    ///
    /// <para><paramref name="hints"/> carries the per-collector run counts. A count of three events means
    /// something different in a window of 60 captures than in a window of 4, and the caller cannot supply
    /// that number itself. Lite's twin returns the SAME sentences word for word.</para>
    /// </summary>
    private static async Task<string> EmptyTrend(
        /* SINGULAR ("blocking", "deadlock"): it is the subject of "No {subject} was recorded". */
        string subject,
        string serverName,
        int hoursBack,
        List<DarlingBlockingTrendReader.CaptureCount> captures,
        Func<Task<bool>> hasEverCapturedAsync)
    {
        var captureCount = captures.Sum(c => c.Runs);
        var hints = new
        {
            server = serverName,
            hours_back = hoursBack,
            capture_count = captureCount,
            captures = captures.Select(c => new
            {
                collector = c.CollectorName,
                runs = c.Runs,
                first_run_at = c.FirstRunAt?.ToString("o"),
                last_run_at = c.LastRunAt?.ToString("o"),
            }),
        };

        if (captureCount > 0)
            return McpHelpers.Status(
                "empty",
                $"No {subject} was recorded for {serverName} in the last {hoursBack} hour(s). {captureCount} collector run(s) DID execute over this window, so this is a genuine all-clear rather than missing data — see hints.captures for which collectors ran and when.",
                hints);

        var everCaptured = await hasEverCapturedAsync();
        return McpHelpers.Status(
            "unavailable",
            everCaptured
                ? $"No {subject} collector runs are recorded for {serverName} in the last {hoursBack} hour(s), so this is NOT an all-clear — nothing was captured and the window says nothing either way. Collection HAS run for this server outside the window, so this is a gap rather than a dead collector: widen hours_back, or use get_collection_health to find where it stopped."
                : $"No {subject} collector runs have EVER been recorded for {serverName}, so this is NOT an all-clear — there is nothing to read. Check that collection is running for this server before concluding it was quiet.",
            hints);
    }
}
