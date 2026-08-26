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
    [McpServerTool(Name = "get_pg_kernel_stats"), Description("Gets per-query-shape OPERATING SYSTEM resource usage from the pg_stat_kcache extension: CPU split into user and system time, bytes that reached the storage device, and major page faults. Use this together with get_pg_top_queries, which reports ELAPSED time: elapsed is CPU plus waiting, so comparing the two tells you whether a slow statement is burning CPU or waiting on something, which is the first split any tuning question needs. The byte counters are DEVICE reads and writes, so a zero means the operating system page cache served the request rather than that no data was read - do not read them as logical I/O. queryid joins get_pg_top_queries and get_pg_wait_sampling. Rows are ranked by total CPU.")]
    public static async Task<string> GetPgKernelStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 20.")] int limit = 20,
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
            var rows = await DarlingPgKernelStatsReader.GetPgKernelStatsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, limit);

            if (rows.Count == 0)
            {
                /* pg_stat_kcache needs shared_preload_libraries and a restart, so "not installed" is the
                   likely answer and the precondition vocabulary names the fix. Ordered after the
                   capability check so a wrong-engine target is never told to install an extension. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_kernel_stats")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_kernel_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No per-query OS resource usage for {resolved.ServerName} in the last "
                        + $"{hours_back} hour(s). These are per-interval deltas, so a single collection "
                        + "has nothing to difference against and the window fills on the second one.");
            }

            var totalCpuMs = rows.Sum(r => r.TotalCpuMs);
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
                pct_of_total_cpu = totalCpuMs > 0 ? Math.Round(r.TotalCpuMs / totalCpuMs * 100, 1) : 0,
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
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_cpu_ms = Math.Round(totalCpuMs, 1),
                note = "CPU is measured by the OPERATING SYSTEM, not by PostgreSQL. device_read_bytes and "
                     + "device_write_bytes count bytes that reached the device, so zero means the page "
                     + "cache served it rather than that nothing was read."
                     + (resetInWindow
                         ? " At least one series was RESET inside this window, so its figures cover only "
                           + "the time since the reset."
                         : string.Empty),
                queries,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL kernel stats failed: {ex.Message}");
        }
    }
}
