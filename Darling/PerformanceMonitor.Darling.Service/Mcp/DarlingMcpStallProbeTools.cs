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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over the out-of-band stall samples (#2880) — the one surface in the product that reports what a
/// monitored instance was doing INSIDE the window where the sequential sweep records nothing.
///
/// <para>A new tool rather than a facet of <c>get_collection_health</c> or <c>get_collection_log</c>, because
/// those two answer "what did our collectors do" and this answers "what was the TARGET doing while one of
/// them could not finish". Folding it in would put a target-side sample under a collector-side heading, and
/// the denominator would be wrong: collection health counts runs, and this counts probes.</para>
///
/// <para>Fleet-wide by default and per-server on request. Fleet-wide is the useful default here for the
/// reason #2880's investigation turned on: abandonment is confined to 2 of 43 servers and the events never
/// coincide across servers, so the first question of any window is which servers appear in it at all.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStallProbeTools
{
    /// <summary>Default rows of sample detail. Small on purpose — the census is the summary and each row is read whole.</summary>
    public const int DefaultLimit = 50;

    /// <summary>
    /// Maximum days of history, the probe table's own retention. Asking beyond it would return a window the
    /// store cannot answer for.
    /// </summary>
    public const int MaxDaysBack = StallWaitProbeRunner.RetentionDays;

    [McpServerTool(Name = "get_collector_stall_probes"), Description(
        "Gets the out-of-band, server-wide wait samples this tool took while one of its OWN collectors was stalled mid-read on a monitored server. This is the answer to a question nothing else here can reach: collectors run strictly sequentially per server, so a collector stalled in its result-set drain is itself holding the sequence that waiting_tasks, dmv_blocking_snapshot and query_snapshots would run in — measured on one real stall, nothing was observed for four minutes, waiting_tasks ran 2.3 seconds after it cleared and returned zero rows, and about 20 collectors then completed inside 20 seconds as the backlog drained. When a server-scoped collector that declares a wall-clock budget is a quarter of the way through it and delivering under 1 MB/s, the service opens ONE additional connection, takes one sample of sys.dm_os_waiting_tasks and sys.dm_os_schedulers, stores it, and never retries. Each row carries BOTH halves: the client-side trigger evidence (how far into the budget it fired, how many rows and bytes had arrived, and the terminal silence — trigger_elapsed_ms minus trigger_last_read_ms, which was 0-3 ms on every stall measured, i.e. the stream was streaming SLOWLY and never went quiet) and the server-wide sample (waiting task count and distinct wait types across the WHOLE instance, the heaviest wait type with its totals, a summary line of the top five, and the scheduler aggregate: runnable tasks, work-queue length, pending disk IO, and the busiest single scheduler). Read the scheduler figures beside the waits: an instance producing rows 50x slowly with high runnable_tasks is scheduler pressure, with high pending_disk_io is storage, with a leading LCK_ wait is blocking, and with everything quiet is none of those and points off the instance entirely. waiting_task_count is the whole instance before the top-five cut, and scheduler_count is the sample's own DENOMINATOR — every live SQL Server reports at least one VISIBLE ONLINE scheduler, so a zero waiting_task_count beside a positive scheduler_count is a real all-clear rather than a probe that read nothing. The outcome census is always returned and is NOT filtered to successful samples, on purpose: whether a fresh connection can be obtained mid-stall has never been established, so CONNECT_FAILED and CONNECT_TIMED_OUT are findings in their own right and connect_ms on a SAMPLED row is the first measurement of it. connect_ms is time-to-usable-connection and not a claim about a login. Deliberately UNBANDED and untrended — a probe row is evidence about one moment, not a series with a healthy range. Takes no band, no thresholds; pass server_name to scope it, or leave it off for the fleet.")]
    public static async Task<string> GetCollectorStallProbes(
        NpgsqlDataSource postgres,
        [Description("Optional: server name or display name. Omit for the whole fleet.")] string? server_name = null,
        [Description("Days of history. Default 7; max 60 (the samples' own retention).")] int days_back = 7,
        [Description("Maximum sample rows to return. Default 50.")] int limit = DefaultLimit)
    {
        if (days_back <= 0 || days_back > MaxDaysBack)
        {
            return $"Invalid days_back value '{days_back}'. Must be a positive integer (1-{MaxDaysBack}).";
        }

        var validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        int? serverId = null;
        var scope = "(fleet)";

        if (!string.IsNullOrWhiteSpace(server_name))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
            if (error != null)
            {
                return error;
            }

            serverId = resolved.ServerId;
            scope = resolved.ServerName;
        }

        try
        {
            var since = DateTime.UtcNow.AddDays(-days_back);

            /* The census FIRST, because it is what makes an empty answer honest — get_pg_blocking's
               ordering, for its reason. */
            var census = await DarlingStallProbeReader.GetOutcomeCensusAsync(postgres, since, serverId);

            if (census.Count == 0)
            {
                return McpHelpers.Status(
                    "not_collected",
                    $"No stall probes in the last {days_back} day(s) for {scope}, so this window makes no claim "
                    + "about what any monitored instance was doing during a collector stall. That is the EXPECTED "
                    + "answer on a healthy fleet: a probe is only taken when a server-scoped collector that "
                    + "declares a wall-clock budget is a quarter of the way through it and still delivering under "
                    + $"{StallWaitProbePolicy.ThroughputFloorBytesPerSecond / 1024 / 1024} MB/s, which on the "
                    + "measured population is about 1% of the runs of two collectors on 2 of 43 servers. It is "
                    + "also permanently empty on a store below schema V112, and on any window predating the "
                    + "service build that carries the probe.");
            }

            var probes = await DarlingStallProbeReader.GetProbesAsync(postgres, since, serverId, limit);

            return JsonSerializer.Serialize(
                new
                {
                    scope,
                    days_back,
                    note = "A probe is one out-of-band sample taken from a SECOND connection while a collector "
                        + "was stalled mid-read; it is never retried and is abandoned at "
                        + $"{StallWaitProbePolicy.HardBudget.TotalSeconds:F0}s. terminal_silence_ms is "
                        + "trigger_elapsed_ms minus trigger_last_read_ms and is REPORTED, not what the probe "
                        + "fires on: the measured failure mode streams slowly with 0-3 ms of it, so a condition "
                        + "keyed on the reader going quiet would never fire on the real defect. "
                        + "waiting_task_count is the whole instance before the top-five cut; scheduler_count is "
                        + "the sample's denominator, so zero waits beside a positive count is a real all-clear.",
                    outcome_census = census.Select(c => new
                    {
                        outcome = c.Outcome,
                        probes = c.Probes,
                        servers = c.Servers,
                        max_connect_ms = c.MaxConnectMs,
                        max_query_ms = c.MaxQueryMs,
                    }),
                    probes = probes.Select(p => new
                    {
                        probe_time = p.ProbeTime.ToString("o", CultureInfo.InvariantCulture),
                        server_name = p.ServerName,
                        collector_name = p.CollectorName,
                        outcome = p.Outcome,
                        budget_ms = p.BudgetMs,
                        trigger_elapsed_ms = p.TriggerElapsedMs,
                        trigger_rows_read = p.TriggerRowsRead,
                        trigger_bytes_read = p.TriggerBytesRead,
                        trigger_mb_per_second = p.TriggerMbPerSecond,
                        terminal_silence_ms = p.TerminalSilenceMs,
                        connect_ms = p.ConnectMs,
                        query_ms = p.QueryMs,
                        waiting_task_count = p.WaitingTaskCount,
                        distinct_wait_types = p.DistinctWaitTypes,
                        top_wait_type = p.TopWaitType,
                        top_wait_total_ms = p.TopWaitTotalMs,
                        top_wait_max_ms = p.TopWaitMaxMs,
                        wait_summary = p.WaitSummary,
                        scheduler_count = p.SchedulerCount,
                        runnable_tasks = p.RunnableTasks,
                        work_queue_length = p.WorkQueueLength,
                        pending_disk_io = p.PendingDiskIo,
                        max_runnable_tasks = p.MaxRunnableTasks,
                        error_message = p.ErrorMessage,
                    }),
                },
                McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collector_stall_probes", ex);
        }
    }
}
