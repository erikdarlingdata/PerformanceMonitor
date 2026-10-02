/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for per-query OS resource usage, paired with the <c>pg_stat_kcache</c>-backed
/// <c>pg_kernel_stats</c> collector.
///
/// <para>
/// The other half of <c>get_pg_top_queries</c>. That tool reports ELAPSED time, which is CPU plus every
/// wait; this one reports the CPU inside it, split user and system, from the operating system's own
/// accounting rather than PostgreSQL's. A statement whose elapsed time is mostly CPU and one whose
/// elapsed time is mostly waiting are different problems with opposite fixes, and elapsed time alone
/// cannot separate them — which is why this pairs with <c>get_pg_wait_sampling</c> as naturally as it
/// does with the top-queries read.
/// </para>
///
/// <para>
/// <b>The byte counters mean something narrower than they look.</b> They are bytes that reached the
/// DEVICE, so zero means the page cache served the read, not that nothing was read — the one reading of
/// this data that turns a healthy server into a mystery. The description says so, because a caller who
/// takes them as logical I/O will conclude a hot, entirely-cached workload does no reads at all.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgKernelStatsTools
{
    [McpServerTool(Name = "get_pg_kernel_stats"), Description("pg_stat_kcache: per-query-shape OS CPU (user/system, ms) and device I/O. Pairs with get_pg_top_queries (ELAPSED = CPU + waiting) to split a slow statement into CPU-bound vs waiting. Bytes are DEVICE reads/writes: zero means cache-served, not unread. queryid joins get_pg_top_queries, get_pg_wait_sampling. Ranked by total CPU. PAGE BOUNDED BY limit: queries_returned/truncated. SHARES ARE OF THE WINDOW: pct_of_total_cpu divides by total_cpu_ms, not the page sum. status empty usually means just one collection so far - values are per-interval deltas needing a second to difference. <<GUIDE>> Gets per-query-shape OPERATING SYSTEM resource usage from the pg_stat_kcache extension: CPU split into user and system time, bytes that reached the storage device, and major page faults. Use this together with get_pg_top_queries, which reports ELAPSED time: elapsed is CPU plus waiting, so comparing the two tells you whether a slow statement is burning CPU or waiting on something, which is the first split any tuning question needs. The byte counters are DEVICE reads and writes, so a zero means the operating system page cache served the request rather than that no data was read - do not read them as logical I/O. queryid joins get_pg_top_queries and get_pg_wait_sampling. Rows are ranked by total CPU. THE PAGE IS BOUNDED BY limit: queries_returned is how many shapes you got, truncated says the window held more, and the rows are the heaviest so the ones past the cap burned less. SHARES ARE OF THE WINDOW, NOT OF THE PAGE: pct_of_total_cpu's denominator is total_cpu_ms, the WHOLE window's user + system CPU across every (database, query) series, computed in the same statement as the rows - so a three-row page does not sum to 100%, and the gap between returned_cpu_ms (what the page adds up to) and total_cpu_ms is the CPU the cap left out; returned_pct_of_total is that ratio stated once. Maximum query shapes to return, most CPU first. Default 20. This is what bounds the page - read truncated to know whether the window held more; the shares stay of the whole window whatever this is set to.")]
    public static async Task<string> GetPgKernelStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum query shapes to return, most CPU first. Default 20. This is what bounds the page - read truncated to know whether the window held more; the shares stay of the whole window.")] int limit = 20,
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
            /* #3541 A7: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal - and the window's CPU rides on the same statement, so the shares below have a
               denominator the cap cannot shrink. */
            var page = await DarlingPgKernelStatsReader.GetPgKernelStatsPageAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit + 1, cancellationToken);

            if (page.Rows.Count == 0)
            {
                /* pg_stat_kcache needs shared_preload_libraries and a restart, so "not installed" is the
                   likely answer and the precondition vocabulary names the fix. Ordered after the
                   capability check so a wrong-engine target is never told to install an extension. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_kernel_stats", cancellationToken)
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_kernel_stats", cancellationToken)
                    ?? McpHelpers.Status(
                        "empty",
                        $"No per-query OS resource usage for {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). These are per-interval deltas, so a single collection "
                        + "has nothing to difference against and the window fills on the second one.");
            }

            return BuildKernelStatsJson(resolved.ServerName, hours_back, page, limit);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_kernel_stats", ex);
        }
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store.
    ///
    /// <para><b>The denominator is the window's, not the page's</b> (#3541 A7). <paramref name="page"/> carries
    /// <c>WindowTotalCpuMs</c> from the same statement as its rows, and every <c>pct_of_total_cpu</c> divides by
    /// THAT. The previous shape divided by the sum of the rows fetched, so at <c>limit = 3</c> the three shares
    /// summed to 100% and read as "these three burned all the CPU". The page's own sum still travels as
    /// <c>returned_cpu_ms</c>; the ratio of the two is <c>returned_pct_of_total</c>.</para>
    /// </summary>
    internal static string BuildKernelStatsJson(
        string serverName,
        int hoursBack,
        DarlingPgKernelStatsReader.PgKernelStatsPage page,
        int limit)
    {
        var truncated = page.Rows.Count > limit;
        var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows;

        var windowTotalCpuMs = page.WindowTotalCpuMs;
        var returnedCpuMs = rows.Sum(r => r.TotalCpuMs);
        var resetInWindow = rows.Any(r => r.CounterReset);

        var queries = rows.Select(r => new
        {
            /* String, like every other queryid on this surface — a signed 64-bit value would round in
               a double-decoding JSON parser and produce an id that joins to nothing. */
            queryid = r.QueryId.ToString(CultureInfo.InvariantCulture),
            database_name = r.DatabaseName,
            cpu_ms = Math.Round(r.TotalCpuMs, 1),
            /* Kept split rather than only summed: system time dominated by kernel work is a different
               finding from user time dominated by the planner or by expression evaluation. */
            user_cpu_ms = Math.Round(r.ExecUserTimeMs, 1),
            system_cpu_ms = Math.Round(r.ExecSystemTimeMs, 1),
            /* Of the WINDOW's CPU, never of the page's — see the remarks. */
            pct_of_total_cpu = windowTotalCpuMs > 0 ? Math.Round(r.TotalCpuMs / windowTotalCpuMs * 100, 1) : 0,
            device_read_bytes = r.ExecReadBytes,
            device_write_bytes = r.ExecWriteBytes,
            /* Bytes AND megabytes, the convention get_pg_index_usage already follows: an agent wants
               the exact figure, a grid wants something readable, and deriving one from the other at
               the display layer is where rounding disagreements start. */
            device_read_mb = Math.Round(r.ExecReadBytes / 1024.0 / 1024.0, 1),
            device_write_mb = Math.Round(r.ExecWriteBytes / 1024.0 / 1024.0, 1),
            /* A major fault is a page read from disk to satisfy a memory access — the signal that the
               host is short of memory, which no PostgreSQL-side counter reports at all. */
            major_faults = r.MajorFaults,
            counter_reset = r.CounterReset,
        })
        .ToList();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            /* #3541 A3 dialect: the page described as a page. No time bounds — each row is one series
               differenced across the whole window, so there is no page reach to report, only a cap. */
            queries_returned = queries.Count,
            truncated,
            order = "cpu_ms_desc",
            /* The WINDOW's CPU, across every series — the denominator of every pct_of_total_cpu above.
               NOT the sum of the rows; that is returned_cpu_ms. */
            total_cpu_ms = Math.Round(windowTotalCpuMs, 1),
            returned_cpu_ms = Math.Round(returnedCpuMs, 1),
            returned_pct_of_total = windowTotalCpuMs > 0 ? Math.Round(returnedCpuMs / windowTotalCpuMs * 100, 1) : 0,
            note = "CPU is measured by the OPERATING SYSTEM, not by PostgreSQL. device_read_bytes and "
                 + "device_write_bytes count bytes that reached the device, so zero means the page "
                 + "cache served it rather than that nothing was read. total_cpu_ms is the WHOLE window's "
                 + "user + system CPU across every series, computed in the same statement as the rows; "
                 + "each row's pct_of_total_cpu divides by it, so the shares on a page do not sum to 100 "
                 + "unless the page is the whole window (truncated = false). returned_cpu_ms is what the "
                 + "rows returned add up to."
                 + (resetInWindow
                     ? " At least one series was RESET inside this window, so its figures cover only "
                       + "the time since the reset."
                     : string.Empty),
            queries,
        }, McpHelpers.JsonOptions);
    }
}
