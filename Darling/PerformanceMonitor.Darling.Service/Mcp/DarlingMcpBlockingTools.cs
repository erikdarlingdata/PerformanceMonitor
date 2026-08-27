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
    [McpServerTool(Name = "get_blocking"), Description("Gets blocking events captured by the blocked process report extended event (plus the always-on DMV blocking-snapshot fallback). Shows the blocked and blocking sessions, wait types, wait times, and query text for both. Use this first for a quick overview, then use get_blocked_process_xml for deep analysis of prolonged blocking.")]
    public static async Task<string> GetBlocking(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 30.")] int limit = 30,
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null,
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
            var rows = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    ?? McpHelpers.Status("empty", "No blocking events found in the specified time range.");

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
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null,
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
            var rows = await DarlingBlockingReader.GetRecentDeadlocksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks")
                    /* #2546: capability first (permanent), then the runtime precondition (fixable), then the
                       read's own miss. A deadlock capture whose XE session is gone records SESSION_MISSING
                       and then returns zero rows forever, which is byte-identical to a server that simply
                       did not deadlock — the one answer nobody should be given without being told. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks")
                    ?? McpHelpers.Status("empty", "No deadlocks found in the specified time range.");

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
        [Description("Optional #1140 alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects.")] string? dedup_key = null,
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
            var rows = await DarlingBlockingReader.GetRecentDeadlocksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            /* #2159: the XML filter runs BEFORE the cap and before the fingerprint, because a row without a
               graph has no objects to fingerprint — it could never match a key, and including it would only
               consume one of the `limit` slots the caller wanted spent on real graphs. */
            var candidates = rows.Where(r => r.HasDeadlockXml).ToList();
            if (candidates.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "deadlocks")
                    ?? McpHelpers.Status("empty", "No deadlock XML available in the specified time range.");

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
        [Description("Maximum reports to return. Default 5.")] int limit = 5,
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
            var rows = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var withXml = rows.Where(r => r.HasReportXml).Take(limit).ToList();
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

    [McpServerTool(Name = "get_lock_wait_trend"), Description("Gets the AGGREGATE lock-wait rate over time for a server: every LCK% wait type's wait milliseconds per second at each collection, the viewer's Blocking Trends lock-wait chart. get_wait_trend charts ONE named wait type and get_blocking_trend counts incidents; this is the whole lock family at once, as a rate rather than a count. Use it to see whether a server's lock pressure is rising when no single wait type dominates, to tell a few long blocks from constant low-grade contention, and to pick which LCK type to hand to get_wait_trend next. The rate is delta wait time divided by the seconds since the previous collection, so it is comparable across servers collecting on different cadences.")]
    public static async Task<string> GetLockWaitTrend(
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
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            var points = await DarlingBlockingTrendReader.GetLockWaitTrendAsync(
                postgres, resolved.ServerId, start, end);

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

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /*
                    Rows, not a pre-pivoted series per wait type. The caller decides whether to sum the
                    family or chart the members, and a pivot here would have to pick a top-N and silently
                    drop the rest — on a read whose premise is that no single LCK type dominates.
                */
                trend = points.Select(p => new
                {
                    collection_time = p.CollectionTime.ToString("o"),
                    wait_type = p.WaitType,
                    wait_time_ms_per_second = Math.Round(p.WaitTimeMsPerSecond, 3),
                }),
            }, McpHelpers.JsonOptions);
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
