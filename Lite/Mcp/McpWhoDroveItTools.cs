using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Collectors;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #6114: the Lite twins of Darling's "who drove it" reads (#6072): <c>get_cpu_by_database</c> ranks databases by CPU,
/// <c>get_load_by_caller</c> groups active queries by host, program and login, and <c>get_query_pileups</c> lists the capture
/// times when many sessions ran one statement at once. Same names, parameters, ordering and output shape as Darling's, over
/// Lite's DuckDB tables. What Lite does not have is left out and the descriptions say so: no hourly rollup tier (so no
/// <c>statements_window</c> / <c>procedures_window</c>; <c>effective_start</c> and <c>window_truncated</c> disclose the window floor
/// as Lite's other reads do).
/// </summary>
[McpServerToolType]
public sealed class McpWhoDroveItTools
{
    internal const int DefaultTop = 20;
    internal const int MaxTop = 100;
    internal const int DefaultStatements = 3;
    internal const int MaxStatements = 10;
    internal const int DefaultMinSessions = 5;
    internal const int MinMinSessions = 2;
    internal const int DefaultPileups = 10;

    /// <summary>How far either side of a pile-up the nearest DMV blocking snapshot may be, in seconds: the window Darling uses.</summary>
    internal const int BlockingToleranceSeconds = 60;

    /// <summary>The query_text preview length, the one Darling's twins use.</summary>
    private const int QueryTextPreviewLength = 400;

    /* ───────────────────────── get_cpu_by_database ───────────────────────── */

    [McpServerTool(Name = "get_cpu_by_database"), Description(
        "Ranks databases by CPU over a window ending at as_of, each with its share of the server total and its heaviest statement. " +
        "cpu_ms is the plan cache's per-statement CPU (query_stats deltas, as get_top_queries_by_cpu sums them) per database; share is of the CPU of EVERY database, not of the page. " +
        "procedure_cpu_ms is the procedure stats' CPU, shown beside cpu_ms and NEVER added to it: a procedure's statements are already in cpu_ms. cpu_source and procedure_cpu_source name where each figure came from. <<GUIDE>> " +
        "Lite reads its raw query_stats and procedure_stats only (no hourly rollup), so effective_start and window_truncated say how far the window reached; Darling's statements_window and procedures_window are not here. " +
        "Rows: database_name, cpu_ms, share, elapsed_ms, executions, statements, top_statement (query_hash, cpu_ms, share_of_database, executions, query_text preview masked by mcp.dataDetail), procedure_cpu_ms, top_procedure. " +
        "An empty answer says why: the plan cache was not collected, or the window held no statement CPU. databases_returned/truncated: top caps at 100 and truncated says more databases matched. Deltas only, so a plan evicted between samples loses its CPU, the same limit get_top_queries_by_cpu has.")]
    public static async Task<string> GetCpuByDatabase(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of. Default 24.")] int hours_back = 24,
        [Description("Number of databases to list. Default 20, at most 100.")] int top = DefaultTop,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        if (top > MaxTop)
            return McpHelpers.Refusal("top", $"top value '{top}' exceeds maximum of {MaxTop} for get_cpu_by_database. Use a smaller value.");

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var fetched = await dataService.GetCpuByDatabaseAsync(resolved.ServerId, start, windowEnd, top + 1);
            var (page, cut) = McpHelpers.BoundPage(fetched, top);

            var notice = await McpQueryTools.WindowNoticeAsync(
                () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, resolved.ServerId, start, windowEnd),
                start, windowEnd, "query_stats", emptyAnswer: page.Count == 0);

            if (page.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_stats", awsRdsKind: AwsRdsKind.None)
                    ?? McpHelpers.Status("empty",
                        $"No statement CPU for {resolved.DisplayName} in the last {hours_back} hour(s): the plan cache collector wrote no interval with executions in the window. "
                        + "Procedure stats are not a substitute, because they count the same work.",
                        notice.AsHints());
            }

            var serverCpuUs = page[0].ServerCpuUs;
            static double? Share(double part, double whole) => whole > 0 ? Math.Round(part / whole, 4) : null;

            var databases = page.Select(r => new
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
                    query_text = McpHelpers.TruncateStatement(r.QueryText, QueryTextPreviewLength),
                    query_text_truncated = r.QueryText != null && r.QueryText.Length > QueryTextPreviewLength,
                },
                procedure_cpu_ms = r.ProcedureCpuUs is null ? (double?)null : Math.Round(r.ProcedureCpuUs.Value / 1000.0, 1),
                top_procedure = r.TopProcedure is null ? null : new
                {
                    name = r.TopProcedure,
                    cpu_ms = Math.Round((r.TopProcedureCpuUs ?? 0) / 1000.0, 1),
                },
            }).ToList();

            var node = JsonSerializer.SerializeToNode(new
            {
                server = resolved.DisplayName,
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                server_cpu_ms = Math.Round(serverCpuUs / 1000.0, 1),
                cpu_source = "query_stats",
                procedure_cpu_source = "procedure_stats",
                overlap_note = "cpu_ms counts every statement the plan cache sampled, including the statements inside procedures. procedure_cpu_ms is the procedures' own total for the same work: read it beside cpu_ms, never added to it.",
                share_note = "share is a database's cpu_ms over server_cpu_ms, the CPU of every database in the window, so it does not change with top.",
                databases_returned = databases.Count,
                truncated = cut,
                databases,
            }, McpHelpers.JsonOptions)!.AsObject();
            DropWindowKeysWhenWhole(node, notice);
            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_cpu_by_database", ex);
        }
    }

    /// <summary>Darling's twins carry the window-floor keys only when the floor cut the window; the same here.</summary>
    private static void DropWindowKeysWhenWhole(JsonObject node, McpQueryTools.McpWindowNotice notice)
    {
        if (!notice.WindowTruncated)
        {
            node.Remove("effective_start");
            node.Remove("window_truncated");
            node.Remove("truncation_note");
        }
    }

    /* ───────────────────────── get_load_by_caller ───────────────────────── */

    [McpServerTool(Name = "get_load_by_caller"), Description(
        "Groups the active-query snapshots of a window ending at as_of by caller (host_name, program_name, login_name), ranked by CPU, with requests, CPU, elapsed time, shares of the window and top statements. " +
        "Each request counts once, at the largest CPU any capture saw. Shares are of the window, not of the page. login_name and host_name are masked by mcp.dataDetail. <<GUIDE>> " +
        "A request is a session and its start (capture time minus its elapsed time, to the second). Its cpu_ms and elapsed_ms are the largest cumulative figures any capture saw, so a request seen in ten captures is one request. A request that started before the window or ran on after it counts only what the captures saw, and one shorter than the capture interval may not be seen at all. " +
        "requests counts the distinct requests, samples the snapshots (one per running request per capture), captures the distinct capture times. " +
        "Rows: host_name, program_name, login_name (null = the snapshot carried none), requests, samples, captures, cpu_ms, cpu_share, elapsed_ms, elapsed_share, sample_share, first_seen, last_seen, top_statements (query_hash, requests, samples, cpu_ms, query_text preview masked by mcp.dataDetail). WAITFOR shells are dropped, as get_active_queries drops them. " +
        "An empty answer says why: query_snapshots was not collected, or the window held no active request. top caps at 100 groups and truncated says more matched; statements per group default 3, at most 10. Reads the snapshots the collector wrote; it does not see a request that finished between two captures.")]
    public static async Task<string> GetLoadByCaller(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of. Default 1.")] int hours_back = 1,
        [Description("Number of caller groups to list. Default 20, at most 100.")] int top = DefaultTop,
        [Description("Top statements listed per group. Default 3, at most 10.")] int statements = DefaultStatements,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
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
            var groups = await dataService.GetLoadByCallerAsync(resolved.ServerId, start, windowEnd, top + 1, statements);
            var (page, cut) = McpHelpers.BoundPage(groups, top);

            var notice = await McpQueryTools.WindowNoticeAsync(
                () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QuerySnapshots, resolved.ServerId, start, windowEnd),
                start, windowEnd, "query_snapshots", emptyAnswer: page.Count == 0);

            if (page.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_snapshots", awsRdsKind: AwsRdsKind.None)
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
            DropWindowKeysWhenWhole(node, notice);
            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_load_by_caller", ex);
        }
    }

    /* ───────────────────────── get_query_pileups ───────────────────────── */

    [McpServerTool(Name = "get_query_pileups"), Description(
        "Lists the capture times when min_sessions or more sessions ran the same query_hash at once, most sessions first, with the wait types at that capture and the head blocker from the DMV blocking snapshot NEAREST that time. " +
        "head_blocker comes from a separate collector, so it is the nearest snapshot within 60 seconds either side (blocking_snapshot_offset_seconds says how near); null with a note means no blocking was recorded near the capture. " +
        "A session counts once per capture. <<GUIDE>> " +
        "Waits are session counts per wait_type at the capture itself (none = running, no wait). That collector (dmv_blocking_snapshots, the always-on blocking snapshot) writes rows only while something is blocked. " +
        "Rows: collection_time, query_hash, sessions, blocked_sessions (sessions with blocking_session_id set at the capture), max_wait_ms, waits [{wait_type, sessions}], query_text preview masked by mcp.dataDetail, head_blocker {spid, blocked_sessions (everything behind it, directly or not), wait_time_ms, login_name, host_name, program_name, database_name, query_text} and blocking_snapshot_time. " +
        "min_sessions default 5, at least 2; top caps at 100. An empty answer says why: no capture in the window had that many sessions on one statement, or query_snapshots was not collected. WAITFOR shells are dropped, as get_active_queries drops them. The pile-up is seen only at captures, so a pile-up that came and went between two captures is not listed.")]
    public static async Task<string> GetQueryPileups(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history ending at as_of. Default 24.")] int hours_back = 24,
        [Description("Sessions running one query_hash at one capture for it to be listed. Default 5, at least 2.")] int min_sessions = DefaultMinSessions,
        [Description("Number of pile-ups to list. Default 10, at most 100.")] int top = DefaultPileups,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        if (top > MaxTop)
            return McpHelpers.Refusal("top", $"top value '{top}' exceeds maximum of {MaxTop} for get_query_pileups. Use a smaller value.");
        if (min_sessions < MinMinSessions)
            return McpHelpers.Refusal("min_sessions", $"Invalid min_sessions value '{min_sessions}'. A pile-up is at least {MinMinSessions} sessions on one statement.");

        try
        {
            var start = windowEnd.AddHours(-hours_back);
            var fetched = await dataService.GetQueryPileupsAsync(resolved.ServerId, start, windowEnd, min_sessions, top + 1);
            var (page, cut) = McpHelpers.BoundPage(fetched, top);

            var notice = await McpQueryTools.WindowNoticeAsync(
                () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QuerySnapshots, resolved.ServerId, start, windowEnd),
                start, windowEnd, "query_snapshots", emptyAnswer: page.Count == 0);

            if (page.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_snapshots", awsRdsKind: AwsRdsKind.None)
                    ?? McpHelpers.Status("empty",
                        $"No capture in the last {hours_back} hour(s) on {resolved.DisplayName} had {min_sessions} or more sessions running the same query_hash. "
                        + "A pile-up between two captures is not seen; a lower min_sessions or a longer window may find one.",
                        notice.AsHints());
            }

            /* The head blocker of each pile-up, from the DMV blocking snapshot nearest its time. */
            var blockingGate = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "dmv_blocking_snapshot", awsRdsKind: AwsRdsKind.None);
            Dictionary<DateTime, DateTime> nearest = [];
            var pairsBySnapshot = new Dictionary<DateTime, List<LocalDataService.BlockingPair>>();
            if (blockingGate is null)
            {
                nearest = await dataService.GetNearestBlockingSnapshotsAsync(
                    resolved.ServerId, page.Select(p => p.CollectionTime).Distinct().ToArray(), BlockingToleranceSeconds);
                var pairs = await dataService.GetBlockingPairsAsync(resolved.ServerId, nearest.Values.Distinct().ToArray());
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
                blocking_source = "dmv_blocking_snapshots",
                blocking_tolerance_seconds = BlockingToleranceSeconds,
                pileups_returned = pileups.Count,
                truncated = cut,
                pileups,
            }, McpHelpers.JsonOptions)!.AsObject();
            DropWindowKeysWhenWhole(node, notice);
            return node.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_pileups", ex);
        }
    }

    /// <summary>
    /// The head blocker of one blocking snapshot: a blocking spid that is not itself blocked in the snapshot. With several, the one with the most sessions
    /// behind it (directly or through a chain), then the longest wait, then the lowest spid, so the choice is the same every read. The Darling twin's rule.
    /// </summary>
    internal static object? HeadBlocker(IReadOnlyList<LocalDataService.BlockingPair> pairs)
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
