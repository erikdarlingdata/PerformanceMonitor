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
    [McpServerTool(Name = "get_blocking"), Description("Gets blocking events captured by the blocked process report extended event (plus the always-on DMV blocking-snapshot fallback). Shows the blocked and blocking sessions, wait types, wait times, and query text for both. Use this first for a quick overview, then use get_blocked_process_xml for deep analysis of prolonged blocking.")]
    public static async Task<string> GetBlocking(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 30.")] int limit = 30,
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveWithFingerprintNameAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return McpHelpers.Status("empty", "No blocking events found in the specified time range.");

            /* #2159: fingerprint the WHOLE window, then filter, then cap. Capping first would let `limit`
               discard the very incident the key names — the caller asked for one specific incident, not for
               the newest `limit` rows that happen to include it. */
            var examined = rows.Count;
            var keys = DarlingIncidentFingerprint.BlockingKeys(
                resolved.FingerprintName,
                rows.Select(r => new BlockingIncidentGrouper.BlockedEvent(
                    r.DatabaseName, r.ContentiousObject, r.BlockedSqlText, r.BlockingSqlText,
                    r.WaitTimeMs, r.LockMode)).ToList());

            if (!DarlingIncidentFingerprint.NoFilter(dedup_key))
            {
                var wanted = DarlingIncidentFingerprint.NormalizeKey(dedup_key);
                var kept = rows.Where((_, i) => keys[i] == wanted).ToList();
                if (kept.Count == 0)
                    return McpHelpers.Status("empty", DarlingIncidentFingerprint.NoMatchMessage(
                        "blocking events", dedup_key!, resolved.FingerprintName, examined));

                keys = kept.Select(r => wanted).Cast<string?>().ToList();
                rows = kept;
            }

            var result = rows.Take(limit).Select((r, i) => new
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
                blocked_last_tran_started = r.BlockedLastTranStarted?.ToString("o"),
                blocking_last_tran_started = r.BlockingLastTranStarted?.ToString("o"),
                blocked_last_batch_started = r.BlockedLastBatchStarted?.ToString("o"),
                blocking_last_batch_started = r.BlockingLastBatchStarted?.ToString("o"),
                blocked_last_batch_completed = r.BlockedLastBatchCompleted?.ToString("o"),
                blocking_last_batch_completed = r.BlockingLastBatchCompleted?.ToString("o"),
                blocked_priority = r.BlockedPriority,
                blocking_priority = r.BlockingPriority,
                has_report_xml = r.HasReportXml,
                dedup_key = keys[i]
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                dedup_key = DarlingIncidentFingerprint.NoFilter(dedup_key) ? null : DarlingIncidentFingerprint.NormalizeKey(dedup_key),
                total_events = rows.Count,
                events = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking", ex);
        }
    }

    [McpServerTool(Name = "get_deadlocks"), Description("Gets recent deadlock events with victim process info. Deadlocks occur when two or more sessions permanently block each other. Use get_deadlock_detail for the full deadlock graph XML.")]
    public static async Task<string> GetDeadlocks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 20.")] int limit = 20,
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveWithFingerprintNameAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingBlockingReader.GetRecentDeadlocksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return McpHelpers.Status("empty", "No deadlocks found in the specified time range.");

            /* #2159: see get_blocking — fingerprint the window, filter, then cap. */
            var examined = rows.Count;
            var keys = DarlingIncidentFingerprint.DeadlockKeys(
                resolved.FingerprintName, rows.Select(r => r.DeadlockGraphXml));

            if (!DarlingIncidentFingerprint.NoFilter(dedup_key))
            {
                var wanted = DarlingIncidentFingerprint.NormalizeKey(dedup_key);
                var kept = rows.Where((_, i) => keys[i] == wanted).ToList();
                if (kept.Count == 0)
                    return McpHelpers.Status("empty", DarlingIncidentFingerprint.NoMatchMessage(
                        "deadlocks", dedup_key!, resolved.FingerprintName, examined));

                keys = kept.Select(r => wanted).Cast<string?>().ToList();
                rows = kept;
            }

            var result = rows.Take(limit).Select((r, i) => new
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
                dedup_key = DarlingIncidentFingerprint.NoFilter(dedup_key) ? null : DarlingIncidentFingerprint.NormalizeKey(dedup_key),
                total_deadlocks = rows.Count,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlocks", ex);
        }
    }

    [McpServerTool(Name = "get_deadlock_detail"), Description("Gets the full deadlock graph XML for a specific time range. Returns the raw XML that can be analyzed for lock resources, process details, and deadlock chains.")]
    public static async Task<string> GetDeadlockDetail(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum deadlocks to return. Default 5.")] int limit = 5,
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveWithFingerprintNameAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingBlockingReader.GetRecentDeadlocksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            /* #2159: the XML filter runs BEFORE the cap and before the fingerprint, because a row without a
               graph has no objects to fingerprint — it could never match a key, and including it would only
               consume one of the `limit` slots the caller wanted spent on real graphs. */
            var candidates = rows.Where(r => r.HasDeadlockXml).ToList();
            if (candidates.Count == 0)
                return McpHelpers.Status("empty", "No deadlock XML available in the specified time range.");

            var examined = candidates.Count;
            var keys = DarlingIncidentFingerprint.DeadlockKeys(
                resolved.FingerprintName, candidates.Select(r => r.DeadlockGraphXml));

            if (!DarlingIncidentFingerprint.NoFilter(dedup_key))
            {
                var wanted = DarlingIncidentFingerprint.NormalizeKey(dedup_key);
                var kept = candidates.Where((_, i) => keys[i] == wanted).ToList();
                if (kept.Count == 0)
                    return McpHelpers.Status("empty", DarlingIncidentFingerprint.NoMatchMessage(
                        "deadlocks with a graph", dedup_key!, resolved.FingerprintName, examined));

                keys = kept.Select(r => wanted).Cast<string?>().ToList();
                candidates = kept;
            }

            var withXml = candidates.Take(limit).ToList();

            var result = withXml.Select((r, i) => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                deadlock_time = r.DeadlockTime?.ToString("o"),
                victim_process_id = r.VictimProcessId,
                dedup_key = keys[i],
                deadlock_graph_xml = r.DeadlockGraphXml
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                dedup_key = DarlingIncidentFingerprint.NoFilter(dedup_key) ? null : DarlingIncidentFingerprint.NormalizeKey(dedup_key),
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlock_detail", ex);
        }
    }

    [McpServerTool(Name = "get_blocked_process_xml"), Description("Gets the raw blocked process report XML from extended events. Contains full detail about both the blocked and blocking sessions for deep analysis.")]
    public static async Task<string> GetBlockedProcessXml(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum reports to return. Default 5.")] int limit = 5)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var withXml = rows.Where(r => r.HasReportXml).Take(limit).ToList();
            if (withXml.Count == 0)
                return McpHelpers.Status("empty", "No blocked process report XML available in the specified time range.");

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
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var points = await DarlingBlockingTrendReader.GetBlockingTrendAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

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
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var points = await DarlingBlockingTrendReader.GetDeadlockTrendAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

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
}
