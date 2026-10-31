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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6072: the first three questions of a CPU or blocking alert, one read each. Store-only and read-only, Darling only.
/// <c>get_cpu_by_database</c> ranks databases by CPU, <c>get_load_by_caller</c> groups active queries by host, program and login,
/// and <c>get_query_pileups</c> lists the capture times when many sessions ran one statement at once.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpWhoDroveItTools
{
    internal const int DefaultTop = 20;
    internal const int MaxTop = 100;

    /// <summary>The query_text preview length, the one <c>get_query_store_top</c> uses.</summary>
    private const int QueryTextPreviewLength = 400;

    /* ───────────────────────── get_cpu_by_database ───────────────────────── */

    [McpServerTool(Name = "get_cpu_by_database"), Description(
        "Ranks databases by CPU over a window ending at as_of, each with its share of the server total and its heaviest statement. " +
        "cpu_ms is the plan cache's per-statement CPU (query_stats deltas, as get_top_queries_by_cpu sums them) per database; share is of the CPU of EVERY database, not of the page. " +
        "procedure_cpu_ms is the procedure stats' CPU, shown beside cpu_ms and NEVER added to it: a procedure's statements are already in cpu_ms. cpu_source and procedure_cpu_source name where each figure came from. <<GUIDE>> " +
        "statements_window and procedures_window say how far each source reached. " +
        "Rows: database_name, cpu_ms, share, elapsed_ms, executions, statements, top_statement (query_hash, cpu_ms, share_of_database, executions, query_text preview masked by mcp.dataDetail; the text is raw-tier only, so a window older than raw retention has query_hash and no text), procedure_cpu_ms, top_procedure. " +
        "An empty answer says why: the plan cache was not collected, or the window held no statement CPU. databases_returned/truncated: top caps at 100 and truncated says more databases matched. Deltas only, so a plan evicted between samples loses its CPU, the same limit get_top_queries_by_cpu has.")]
    public static Task<string> GetCpuByDatabase(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of. Default 24.")] int hours_back = 24,
        [Description("Number of databases to list. Default 20, at most 100.")] int top = DefaultTop,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetCpuByDatabaseCore(postgres, server_name, hours_back, top, as_of, cancellationToken);

    /// <summary>#7010: the window-floor notice of the raw query_stats table for get_cpu_by_database; the hourly tier answers <see cref="McpWindowNotice.Unavailable"/> (no raw verdict, see the call).</summary>
    private static Task<McpWindowNotice> CpuNoticeAsync(
        NpgsqlDataSource postgres, string serverName, bool hourly, DateTime start, DateTime end, bool emptyAnswer, CancellationToken cancellationToken) =>
        hourly
            ? Task.FromResult(McpWindowNotice.Unavailable)
            : DarlingMcpWindowNotice.ReadAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "query_stats", serverName, start, end, cancellationToken),
                start, end, "query_stats", emptyAnswer: emptyAnswer, cancellationToken: cancellationToken);

    internal static async Task<string> GetCpuByDatabaseCore(
        NpgsqlDataSource postgres, string? server_name, int hours_back, int top, string? as_of, CancellationToken cancellationToken)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        if (top > MaxTop)
            return McpHelpers.Refusal("top", $"top value '{top}' exceeds maximum of {MaxTop} for get_cpu_by_database. Use a smaller value.");

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            /* The same routing get_resource_use_by_tag uses for the plan cache and procedures: the hourly io rollup when the window is past raw
               retention, raw otherwise, each with its own reach. */
            var (planRoute, procRoute) = await DarlingQueryTagRollupRouting.ChooseStatsRoutesAsync(postgres, [resolved.ServerId], start, windowEnd, cancellationToken);

            var fetched = await DarlingWhoDroveItReader.ReadStatementCpuAsync(
                postgres, resolved.ServerId, start, windowEnd, top + 1, planRoute.Hourly, planRoute.Ceiling, cancellationToken);
            var (page, cut) = McpHelpers.BoundPage(fetched, top);

            /* #7010: the window-floor notice of the raw query_stats table, the three keys every window-floor tool writes. The hourly rollup
               tier reads a table that keeps far more than raw does, and its own edges are already in statements_window (served_start,
               start_dropped_from, materialized_to), so that tier carries no raw-floor verdict (Unavailable writes no key). A never-collected
               server answers bare, before the notice is read. */
            if (page.Count == 0)
            {
                var notCollected = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_stats", cancellationToken);
                if (notCollected is not null) return notCollected;

                var emptyNotice = await CpuNoticeAsync(postgres, resolved.ServerName, planRoute.Hourly, start, windowEnd, emptyAnswer: true, cancellationToken);
                return McpHelpers.Status("empty",
                    $"No statement CPU for {resolved.DisplayName} in the last {hours_back} hour(s): the plan cache collector wrote no interval with executions in the window. "
                    + "Procedure stats are not a substitute, because they count the same work.",
                    emptyNotice.IsUnavailable
                        ? new Dictionary<string, object?> { ["statements_window"] = planRoute.Disclosure }
                        : new Dictionary<string, object?>
                        {
                            ["statements_window"] = planRoute.Disclosure,
                            ["effective_start"] = emptyNotice.EffectiveStart,
                            ["window_truncated"] = emptyNotice.WindowTruncated,
                            ["truncation_note"] = emptyNotice.TruncationNote,
                        });
            }

            var notice = await CpuNoticeAsync(postgres, resolved.ServerName, planRoute.Hourly, start, windowEnd, emptyAnswer: false, cancellationToken);

            var serverCpuUs = page[0].ServerCpuUs;
            var procedures = await DarlingWhoDroveItReader.ReadProcedureCpuAsync(
                postgres, resolved.ServerId, start, windowEnd, page.Select(r => r.DatabaseName).ToArray(), procRoute.Hourly, procRoute.Ceiling, cancellationToken);
            var texts = await DarlingWhoDroveItReader.ReadStatementTextAsync(
                postgres, resolved.ServerId, start, windowEnd, page.Where(r => r.TopQueryHash != null).Select(r => r.TopQueryHash!).Distinct().ToArray(), cancellationToken);

            static double? Share(double part, double whole) => whole > 0 ? Math.Round(part / whole, 4) : null;

            var databases = page.Select(r =>
            {
                procedures.TryGetValue(r.DatabaseName, out var proc);
                string? text = r.TopQueryHash != null && texts.TryGetValue((r.DatabaseName, r.TopQueryHash), out var found) ? found : null;
                return new
                {
                    database_name = r.DatabaseName.Length == 0 ? null : r.DatabaseName,
                    cpu_ms = Math.Round(r.CpuUs / 1000.0, 1),
                    share = Share(r.CpuUs, serverCpuUs),
                    elapsed_ms = Math.Round(r.ElapsedUs / 1000.0, 1),
                    executions = r.Executions,
                    statements = r.Statements,
                    top_statement = r.TopQueryHash is null ? null : new
                    {
                        query_hash = r.TopQueryHash,
                        cpu_ms = Math.Round((r.TopCpuUs ?? 0) / 1000.0, 1),
                        share_of_database = Share(r.TopCpuUs ?? 0, r.CpuUs),
                        executions = r.TopExecutions,
                        query_text = McpHelpers.TruncateStatement(text, QueryTextPreviewLength),
                        query_text_truncated = text != null && text.Length > QueryTextPreviewLength,
                    },
                    procedure_cpu_ms = proc is null ? (double?)null : Math.Round(proc.CpuUs / 1000.0, 1),
                    top_procedure = proc?.TopProcedure is null ? null : new
                    {
                        name = proc.TopProcedure,
                        cpu_ms = Math.Round((proc.TopProcedureCpuUs ?? 0) / 1000.0, 1),
                    },
                };
            }).ToList();

            return DarlingMcpWindowNotice.Finish(JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                server_cpu_ms = Math.Round(serverCpuUs / 1000.0, 1),
                cpu_source = planRoute.Hourly ? "collect.query_stats_io_hourly (hourly rollup)" : "collect.query_stats",
                procedure_cpu_source = procRoute.Hourly ? "collect.procedure_stats_io_hourly (hourly rollup)" : "collect.procedure_stats",
                statements_window = planRoute.Disclosure,
                procedures_window = procRoute.Disclosure,
                overlap_note = "cpu_ms counts every statement the plan cache sampled, including the statements inside procedures. procedure_cpu_ms is the procedures' own total for the same work: read it beside cpu_ms, never added to it.",
                share_note = "share is a database's cpu_ms over server_cpu_ms, the CPU of every database in the window, so it does not change with top.",
                databases_returned = databases.Count,
                truncated = cut,
                databases,
            }, McpHelpers.JsonOptions), notice);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_cpu_by_database", ex);
        }
    }

    /* ───────────────────────── get_load_by_caller ───────────────────────── */

    internal const int DefaultStatements = 3;
    internal const int MaxStatements = 10;

    [McpServerTool(Name = "get_load_by_caller"), Description(
        "Groups the active-query snapshots of a window ending at as_of by caller (host_name, program_name, login_name), ranked by CPU, with requests, CPU, elapsed time, shares of the window and top statements. " +
        "Each request counts once, at the largest CPU any capture saw. Shares are of the window, not of the page. login_name and host_name are masked by mcp.dataDetail. <<GUIDE>> " +
        "A request is a session and its start (capture time minus its elapsed time, to the second). Its cpu_ms and elapsed_ms are the largest cumulative figures any capture saw, so a request seen in ten captures is one request. A request that started before the window or ran on after it counts only what the captures saw, and one shorter than the capture interval may not be seen at all. " +
        "requests counts the distinct requests, samples the snapshots (one per running request per capture), captures the distinct capture times. " +
        "Rows: host_name, program_name, login_name (null = the snapshot carried none), requests, samples, captures, cpu_ms, cpu_share, elapsed_ms, elapsed_share, sample_share, first_seen, last_seen, top_statements (query_hash, requests, samples, cpu_ms, query_text preview masked by mcp.dataDetail). WAITFOR shells are dropped, as get_active_queries drops them. " +
        "An empty answer says why: query_snapshots was not collected, or the window held no active request. top caps at 100 groups and truncated says more matched; statements per group default 3, at most 10. Reads the snapshots the collector wrote; it does not see a request that finished between two captures.")]
    public static Task<string> GetLoadByCaller(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of. Default 1.")] int hours_back = 1,
        [Description("Number of caller groups to list. Default 20, at most 100.")] int top = DefaultTop,
        [Description("Top statements listed per group. Default 3, at most 10.")] int statements = DefaultStatements,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetLoadByCallerCore(postgres, server_name, hours_back, top, statements, as_of, cancellationToken);

    internal static async Task<string> GetLoadByCallerCore(
        NpgsqlDataSource postgres, string? server_name, int hours_back, int top, int statements, string? as_of, CancellationToken cancellationToken)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        if (top > MaxTop)
            return McpHelpers.Refusal("top", $"top value '{top}' exceeds maximum of {MaxTop} for get_load_by_caller. Use a smaller value.");
        validation = McpHelpers.ValidateTop(statements, "statements");
        if (validation != null) return validation;
        if (statements > MaxStatements)
            return McpHelpers.Refusal("statements", $"statements value '{statements}' exceeds maximum of {MaxStatements} for get_load_by_caller. Use a smaller value.");

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var groups = await DarlingWhoDroveItReader.ReadLoadByCallerAsync(postgres, resolved.ServerId, start, windowEnd, top + 1, statements, cancellationToken);
            var (page, cut) = McpHelpers.BoundPage(groups, top);

            var notice = await DarlingMcpWindowNotice.ReadAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "query_snapshots", resolved.ServerName, start, windowEnd, cancellationToken),
                start, windowEnd, "query_snapshots", emptyAnswer: page.Count == 0, cancellationToken: cancellationToken);

            if (page.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_snapshots", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        $"No active-query snapshots for {resolved.DisplayName} in the last {hours_back} hour(s): nothing was running at a capture, or the collector did not run in the window.",
                        notice.AsHints());
            }

            static double? Share(double part, double whole) => whole > 0 ? Math.Round(part / whole, 4) : null;
            static string? Blank(string value) => value.Length == 0 ? null : value;
            var first = page[0];
            var callers = page.Select(g => new
            {
                host_name = Blank(g.HostName),
                program_name = Blank(g.ProgramName),
                login_name = Blank(g.LoginName),
                requests = g.Requests,
                samples = g.Samples,
                captures = g.Captures,
                cpu_ms = Math.Round(g.CpuMs, 1),
                cpu_share = Share(g.CpuMs, g.TotalCpuMs),
                elapsed_ms = Math.Round(g.ElapsedMs, 1),
                elapsed_share = Share(g.ElapsedMs, g.TotalElapsedMs),
                sample_share = Share(g.Samples, g.TotalSamples),
                first_seen = McpHelpers.FormatEffectiveStart(g.FirstSeen),
                last_seen = McpHelpers.FormatEffectiveStart(g.LastSeen),
                top_statements = g.Statements.Select(x => new
                {
                    query_hash = x.QueryHash,
                    requests = x.Requests,
                    samples = x.Samples,
                    cpu_ms = Math.Round(x.CpuMs, 1),
                    query_text = McpHelpers.TruncateStatement(x.QueryText, QueryTextPreviewLength),
                    query_text_truncated = x.QueryText != null && x.QueryText.Length > QueryTextPreviewLength,
                }).ToList(),
            }).ToList();

            var node = JsonSerializer.SerializeToNode(new
            {
                server = resolved.DisplayName,
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                source = "query_snapshots",
                total_requests = first.TotalRequests,
                total_samples = first.TotalSamples,
                total_cpu_ms = Math.Round(first.TotalCpuMs, 1),
                total_elapsed_ms = Math.Round(first.TotalElapsedMs, 1),
                sampling_note = "Each request counts once, by its largest cumulative cpu_ms and elapsed_ms over the captures that saw it (a request is a session and its start, to the second). A request that started before the window or ran on after it counts only what the captures saw, and one shorter than the capture interval may not be seen at all. Shares are of the window's total over every caller, not of the page.",
                callers_returned = callers.Count,
                truncated = cut,
                callers,
            }, McpHelpers.JsonOptions)!.AsObject();
            if (!notice.WindowTruncated)
            {
                node.Remove("effective_start");
                node.Remove("window_truncated");
                node.Remove("truncation_note");
            }

            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_load_by_caller", ex);
        }
    }

    /* ───────────────────────── get_query_pileups ───────────────────────── */

    internal const int DefaultMinSessions = 5;
    internal const int MinMinSessions = 2;
    internal const int DefaultPileups = 10;

    /// <summary>How far either side of a pile-up the nearest DMV blocking snapshot may be, in seconds. The blocking snapshot collector runs about every 30 seconds, so two cycles.</summary>
    internal const int BlockingToleranceSeconds = 60;

    [McpServerTool(Name = "get_query_pileups"), Description(
        "Lists the capture times when min_sessions or more sessions ran the same query_hash at once, most sessions first, with the wait types at that capture and the head blocker from the DMV blocking snapshot NEAREST that time. " +
        "head_blocker comes from a separate collector, so it is the nearest snapshot within 60 seconds either side (blocking_snapshot_offset_seconds says how near); null with a note means no blocking was recorded near the capture. " +
        "A session counts once per capture. <<GUIDE>> " +
        "Waits are session counts per wait_type at the capture itself (none = running, no wait). That collector writes rows only while something is blocked. " +
        "Rows: collection_time, query_hash, sessions, blocked_sessions (sessions with blocking_session_id set at the capture), max_wait_ms, waits [{wait_type, sessions}], query_text preview masked by mcp.dataDetail, head_blocker {spid, blocked_sessions (everything behind it, directly or not), wait_time_ms, login_name, host_name, program_name, database_name, query_text} and blocking_snapshot_time. " +
        "min_sessions default 5, at least 2; top caps at 100. An empty answer says why: no capture in the window had that many sessions on one statement, or query_snapshots was not collected. WAITFOR shells are dropped, as get_active_queries drops them. The pile-up is seen only at captures, so a pile-up that came and went between two captures is not listed.")]
    public static Task<string> GetQueryPileups(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of. Default 24.")] int hours_back = 24,
        [Description("Sessions running one query_hash at one capture for it to be listed. Default 5, at least 2.")] int min_sessions = DefaultMinSessions,
        [Description("Number of pile-ups to list. Default 10, at most 100.")] int top = DefaultPileups,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetQueryPileupsCore(postgres, server_name, hours_back, min_sessions, top, as_of, cancellationToken);

    internal static async Task<string> GetQueryPileupsCore(
        NpgsqlDataSource postgres, string? server_name, int hours_back, int min_sessions, int top, string? as_of, CancellationToken cancellationToken)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        if (top > MaxTop)
            return McpHelpers.Refusal("top", $"top value '{top}' exceeds maximum of {MaxTop} for get_query_pileups. Use a smaller value.");
        if (min_sessions < MinMinSessions)
            return McpHelpers.Refusal("min_sessions", $"Invalid min_sessions value '{min_sessions}'. A pile-up is at least {MinMinSessions} sessions on one statement.");

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var fetched = await DarlingWhoDroveItReader.ReadPileupsAsync(postgres, resolved.ServerId, start, windowEnd, min_sessions, top + 1, cancellationToken);
            var (page, cut) = McpHelpers.BoundPage(fetched, top);

            var notice = await DarlingMcpWindowNotice.ReadAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "query_snapshots", resolved.ServerName, start, windowEnd, cancellationToken),
                start, windowEnd, "query_snapshots", emptyAnswer: page.Count == 0, cancellationToken: cancellationToken);

            if (page.Count == 0)
            {
                var anySnapshots = notice.EffectiveStart is not null || notice.WindowTruncated;
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_snapshots", cancellationToken)
                    ?? McpHelpers.Status("empty",
                        $"No capture in the last {hours_back} hour(s) on {resolved.DisplayName} had {min_sessions} or more sessions running the same query_hash. "
                        + "A pile-up between two captures is not seen; a lower min_sessions or a longer window may find one.",
                        notice.AsHints());
            }

            /* The head blocker of each pile-up, from the DMV blocking snapshot nearest its time. */
            var blockingGate = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "dmv_blocking_snapshot", cancellationToken);
            Dictionary<DateTime, DateTime> nearest = [];
            var pairsBySnapshot = new Dictionary<DateTime, List<DarlingWhoDroveItReader.BlockingPair>>();
            if (blockingGate is null)
            {
                nearest = await DarlingWhoDroveItReader.ReadNearestBlockingSnapshotsAsync(
                    postgres, resolved.ServerId, page.Select(p => p.CollectionTime).Distinct().ToArray(), BlockingToleranceSeconds, cancellationToken);
                var pairs = await DarlingWhoDroveItReader.ReadBlockingPairsAsync(postgres, resolved.ServerId, nearest.Values.Distinct().ToArray(), cancellationToken);
                pairsBySnapshot = pairs.GroupBy(p => p.CollectionTime).ToDictionary(g => g.Key, g => g.ToList());
            }

            var pileups = page.Select(p =>
            {
                object? head = null;
                double? offset = null;
                string? snapshotTime = null;
                string? note = null;
                if (blockingGate is not null)
                {
                    note = "The DMV blocking snapshot is not collected on this server's engine, so there is no head blocker to name.";
                }
                else if (!nearest.TryGetValue(p.CollectionTime, out var at))
                {
                    note = $"No DMV blocking snapshot within {BlockingToleranceSeconds} seconds of this capture. That collector writes rows only while something is blocked, so no blocking was recorded near it.";
                }
                else
                {
                    snapshotTime = McpHelpers.FormatEffectiveStart(at);
                    offset = Math.Round((at - p.CollectionTime).TotalSeconds, 1);
                    head = HeadBlocker(pairsBySnapshot.GetValueOrDefault(at) ?? []);
                    if (head is null)
                    {
                        note = "The nearest blocking snapshot holds no head blocker (every blocker in it is itself blocked).";
                    }
                }

                return new
                {
                    collection_time = McpHelpers.FormatEffectiveStart(p.CollectionTime),
                    query_hash = p.QueryHash,
                    sessions = p.Sessions,
                    blocked_sessions = p.BlockedSessions,
                    max_wait_ms = p.MaxWaitMs,
                    waits = p.Waits.Select(w => new { wait_type = w.WaitType, sessions = w.Sessions }).ToList(),
                    query_text = McpHelpers.TruncateStatement(p.QueryText, QueryTextPreviewLength),
                    query_text_truncated = p.QueryText != null && p.QueryText.Length > QueryTextPreviewLength,
                    blocking_snapshot_time = snapshotTime,
                    blocking_snapshot_offset_seconds = offset,
                    head_blocker = head,
                    head_blocker_note = note,
                };
            }).ToList();

            var node = JsonSerializer.SerializeToNode(new
            {
                server = resolved.DisplayName,
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                min_sessions,
                source = "query_snapshots",
                blocking_source = "collect.dmv_blocking_snapshots",
                blocking_tolerance_seconds = BlockingToleranceSeconds,
                pileups_returned = pileups.Count,
                truncated = cut,
                pileups,
            }, McpHelpers.JsonOptions)!.AsObject();
            if (!notice.WindowTruncated)
            {
                node.Remove("effective_start");
                node.Remove("window_truncated");
                node.Remove("truncation_note");
            }

            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_pileups", ex);
        }
    }

    /// <summary>
    /// The head blocker of one blocking snapshot: a blocking spid that is not itself blocked in the snapshot. With several, the one with the most sessions
    /// behind it (directly or through a chain), then the longest wait, then the lowest spid, so the choice is the same every read.
    /// </summary>
    internal static object? HeadBlocker(IReadOnlyList<DarlingWhoDroveItReader.BlockingPair> pairs)
    {
        if (pairs.Count == 0)
        {
            return null;
        }

        var blocked = pairs.Select(p => p.BlockedSpid).ToHashSet();
        var heads = pairs.Where(p => p.BlockingSpid != 0 && !blocked.Contains(p.BlockingSpid)).GroupBy(p => p.BlockingSpid).ToList();
        if (heads.Count == 0)
        {
            return null;
        }

        var byBlocker = pairs.GroupBy(p => p.BlockingSpid).ToDictionary(g => g.Key, g => g.Select(p => p.BlockedSpid).Distinct().ToList());
        int Behind(int spid)
        {
            var seen = new HashSet<int>();
            var stack = new Stack<int>();
            stack.Push(spid);
            while (stack.Count > 0)
            {
                if (byBlocker.TryGetValue(stack.Pop(), out var next))
                {
                    foreach (var n in next)
                    {
                        if (seen.Add(n))
                        {
                            stack.Push(n);
                        }
                    }
                }
            }

            return seen.Count;
        }

        var best = heads
            .Select(g => (Spid: g.Key, Rows: g.ToList(), Behind: Behind(g.Key)))
            .OrderByDescending(h => h.Behind).ThenByDescending(h => h.Rows.Max(r => r.WaitTimeMs)).ThenBy(h => h.Spid)
            .First();
        var row = best.Rows.OrderByDescending(r => r.WaitTimeMs).First();
        return new
        {
            spid = best.Spid,
            blocked_sessions = best.Behind,
            wait_time_ms = best.Rows.Max(r => r.WaitTimeMs),
            login_name = row.BlockingLogin,
            host_name = row.BlockingHost,
            program_name = row.BlockingApp,
            database_name = row.Database,
            query_text = McpHelpers.TruncateStatement(row.BlockingSql, QueryTextPreviewLength),
        };
    }
}
