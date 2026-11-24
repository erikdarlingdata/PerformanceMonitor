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
        "which are null for a tool that keeps no phase split (only get_query_store_top does), so null means not measured, not zero; " +
        "response_bytes (size of the text the client received); hours_back (the window the call asked for, null when it had none); " +
        "peak_working_set_bytes (highest process working set polled every 75 ms during the call) and duckdb_memory_bytes (DuckDB's memory total when the call ended, " +
        "null when the store lock was busy then); memory_source says where the two memory figures came from. No argument value is stored. " +
        "per_tool: for each tool called since Lite started, count, total_ms, mean_ms, max_ms, p50_ms, p95_ms and p99_ms (bucket upper bounds, never interpolated, capped at max_ms), " +
        "slow_calls and errors. Rows are kept 7 days, at most 1000. Optional tool filter on slow_calls. Empty slow_calls means no call was slow, not a failure.";

    [McpServerTool(Name = "get_slow_mcp_calls"), Description(Description)]
    public static async Task<string> GetSlowReads(
        LocalDataService dataService,
        McpCallTimingRecorder recorder,
        [Description("Hours of history for slow_calls. Default 24; max 168.")] int hours_back = DefaultHours,
        [Description("Optional: only slow_calls for this tool name.")] string? tool = null,
        [Description("Maximum slow_calls rows to return. Default 20.")] int limit = DefaultLimit)
    {
        var windowError = McpHelpers.ValidateWindow(hours_back, null, out _) ?? McpHelpers.ValidateTop(limit);
        if (windowError != null) return windowError;

        try
        {
            var since = DateTime.UtcNow.AddHours(-hours_back);
            var wanted = string.IsNullOrWhiteSpace(tool) ? null : tool.Trim();
            var rows = await dataService.GetMcpSlowCallsAsync(since, limit, wanted);
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
                });

            return JsonSerializer.Serialize(new
            {
                hours_back,
                slow_threshold_ms = (long)recorder.SlowThreshold.TotalMilliseconds,
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
                    response_bytes = r.ResponseBytes,
                    hours_back = r.HoursBack,
                    peak_working_set_bytes = r.PeakWorkingSetBytes,
                    duckdb_memory_bytes = r.DuckDbMemoryBytes,
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
