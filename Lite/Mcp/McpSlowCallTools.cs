/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// <c>get_slow_mcp_calls</c> (#9423, #9424): the MCP calls Lite itself answered slowly, and the per-tool figures of every call since Lite
/// started. Darling has get_slow_reads and get_read_latency for the same job, with a different contract (statements, server_name, surface), and a shared tool name must carry a byte-identical description head on both editions (McpToolGuide), so Lite uses its own name and returns the slow rows and the per-tool figures in one answer.
/// Reads no monitored server.
/// </summary>
[McpServerToolType]
public sealed class McpSlowCallTools
{
    public const int DefaultHours = 24;
    public const int DefaultLimit = 20;

    internal const string Description =
        "Gets the monitoring tool's OWN slow MCP calls, newest first, and per-tool figures for every call since Lite started. Not a monitored server. " +
        "slow_calls: one row per call that took 2 seconds or more: tool; server_name (the server the reply names, else the server_name the call carried, null when it named none); " +
        "rows_returned (the reply's own count, else the length of the one array it holds, null when it carries neither); call_time_utc (when it started); outcome (ok, error, exception or cancelled); " +
        "total_ms (whole call); lock_wait_ms (time waiting for the local store's read lock, added up over every connection the call opened); " +
        "hot_ms and archive_ms (the part of total_ms spent outside and inside the archive span reads) and archive_files (archive files those reads opened), " +
        "which are null for a tool that keeps no hot and archive split (only get_query_store_top does), so null means not measured, not zero; " +
        "answered_from (get_object_usage only: chain, or fold and its short reason, as the reply's coverage says it, null for any other tool) and phase_split (get_object_usage only: the phases of the call in the order they ran, in words, null for any other tool); " +
        "response_bytes (size of the reply text the tool returned, before MCP's JSON-RPC encoding, so the body on the wire is larger); hours_back (the window the call asked for, null when it had none); " +
        "process_peak_working_set_bytes (highest working set of the whole Lite process read while the call ran) and duckdb_peak_memory_bytes (highest duckdb_memory() total read while the call ran, " +
        "null when the store lock was busy at every read), the same two figures the compaction history carries. Both are for the whole process, not the call alone: when calls overlap, " +
        "each one's figure includes the others' memory. Sampling starts 250 ms into a call and runs every 75 ms, so a rise shorter than that can be missed " +
        "and the first 250 ms has only the starting working-set reading. memory_source says in words where the two figures came from. No argument value is stored. " +
        "per_tool: for each tool called since Lite started, count, total_ms, mean_ms, max_ms, p50_ms, p95_ms and p99_ms (bucket upper bounds, never interpolated, capped at max_ms), " +
        "slow_calls, errors and slow_rows_unwritten. slow_rows_unwritten counts the slow calls whose row was not written to the local store after retries, or could not be built, " +
        "so they are in per_tool and missing from slow_calls. It is kept in memory since Lite started, like the rest of per_tool. Rows are kept 7 days, at most 1000. Optional tool filter: slow_calls and per_tool then hold only that tool. Empty slow_calls with slow_rows_unwritten 0 means no call was slow, not a failure. " +
        "truncated is true when the window held more slow calls than limit, so slow_calls is the newest limit of them. per_tool is never cut by limit.";

    [McpServerTool(Name = "get_slow_mcp_calls"), Description(Description)]
    public static async Task<string> GetSlowReads(
        LocalDataService dataService,
        McpCallTimingRecorder recorder,
        [Description("Hours of history for slow_calls. Default 24; max 168.")] int hours_back = DefaultHours,
        [Description("Optional: only this tool's slow_calls rows and per_tool entry.")] string? tool = null,
        [Description("Maximum slow_calls rows to return, newest first. Default 20. This is what bounds the page: read truncated to know whether the window held more.")] int limit = DefaultLimit)
    {
        var windowError = McpHelpers.ValidateWindow(hours_back, null, out _) ?? McpHelpers.ValidateTop(limit);
        if (windowError != null) return windowError;

        try
        {
            var since = DateTime.UtcNow.AddHours(-hours_back);
            var wanted = string.IsNullOrWhiteSpace(tool) ? null : tool.Trim();
            /* Fetch one past the limit so truncation is observed, not inferred from a full page (#3699). */
            var fetched = await dataService.GetMcpSlowCallsAsync(since, limit + 1, wanted);
            var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);
            var perTool = recorder.Snapshot()
                .Where(f => wanted is null || f.Tool == wanted)
                .Select(f => new
                {
                    tool = f.Tool,
                    count = f.Count,
                    total_ms = f.TotalMs,
                    mean_ms = f.Count == 0 ? 0 : f.TotalMs / f.Count,
                    max_ms = f.MaxMs,
                    p50_ms = f.P50Ms,
                    p95_ms = f.P95Ms,
                    p99_ms = f.P99Ms,
                    slow_calls = f.SlowCalls,
                    errors = f.Errors,
                    slow_rows_unwritten = f.SlowRowsUnwritten,
                });

            return JsonSerializer.Serialize(new
            {
                hours_back,
                slow_threshold_ms = (long)recorder.SlowThreshold.TotalMilliseconds,
                truncated,
                slow_calls = rows.Select(r => new
                {
                    tool = r.Tool,
                    call_time_utc = r.CallTimeUtc.ToString("o"),
                    outcome = r.Outcome,
                    server_name = r.ServerName,
                    rows_returned = r.RowsReturned,
                    total_ms = r.TotalMs,
                    lock_wait_ms = r.LockWaitMs,
                    hot_ms = r.HotMs,
                    archive_ms = r.ArchiveMs,
                    archive_files = r.ArchiveFiles,
                    answered_from = r.AnsweredFrom,
                    phase_split = r.PhaseSplit,
                    response_bytes = r.ResponseBytes,
                    hours_back = r.HoursBack,
                    process_peak_working_set_bytes = r.ProcessPeakWorkingSetBytes,
                    duckdb_peak_memory_bytes = r.DuckDbPeakMemoryBytes,
                    memory_source = r.MemorySource,
                }),
                per_tool = perTool,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_slow_mcp_calls", ex);
        }
    }
}
