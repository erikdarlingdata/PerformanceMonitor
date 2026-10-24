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
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The collector-cost MCP surface (#2674) — the monitoring tool measuring ITSELF, so "which of our
/// collectors is a performance hog on the monitored servers" is a query, not a log scrape. Reads the
/// hourly aggregate the worker's <see cref="CollectorCostAccumulator"/> persists into
/// <c>collect.collector_cost</c>: the ranked fleet cost over a window, or one collector's daily trend for
/// spotting a regression against its own history. Store-scoped by nature, so it takes no
/// <c>server_name</c>.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCollectorCostTools
{
    public const int MaxDaysBack = CollectorCostAccumulator.RetentionDays;

    /// <summary>
    /// The one sentence that keeps this tool's headline number from being read as the monitored servers'
    /// fault (#3192). Emitted on BOTH shapes — the ranked list and the single-collector trend — from one
    /// constant rather than two copies, because the trend is the shape a regression investigation lands on
    /// and it is the one that had no caveat at all.
    ///
    /// <para>sql_ms comes from <c>CollectorRunResult.SqlMs</c>, which on the enumerated path is the driver's
    /// per-item stopwatch around the whole <c>readItem</c> closure — and for <c>query_store</c> that closure
    /// probes and writes the STORE. This series cannot be corrected for it: <see cref="CollectorCostAccumulator"/>
    /// sums the blended figure in memory and flushes an hourly total, so there is no phase split here to
    /// subtract and no source table to re-aggregate from. Naming the caveat is the whole of what this
    /// surface can honestly do; <c>get_collection_log.sql_store_ms</c> is where the attribution lives.</para>
    /// </summary>
    internal const string StoreProbeCaveat =
        "On query_store, part of sql_ms is the MONITORING STORE's own plan/text probe and write, not the "
        + "monitored server: those fetches run inside the same per-item stopwatch, and where this was "
        + "measured the probe was 55.4% of plan_fetch and 80.6% of text_fetch. This series carries no phase split, so "
        + "read get_collection_log's sql_store_ms per run before concluding a target is slow.";

    [McpServerTool(Name = "get_collector_cost"), Description(
        "Gets the monitoring tool's OWN per-collector cost on monitored servers — measuring itself, not server "
        + "health; no server_name. Default: ranked fleet list, total_sql_ms desc; collector_name switches to "
        + "that one collector's daily trend. sql_ms is a DURATION (waits included), not CPU; max_sql_ms is the "
        + "worst single execution. On query_store, sql_ms includes the STORE's own probe/write, "
        + "55-81% of the figure here, with no phase split — never read it as server-slowness; "
        + "get_collection_log.sql_store_ms has the per-run split. Empty means no run in the window, not a "
        + "failure. <<GUIDE>> "
        + "Gets the monitoring tool's OWN per-collector cost ON the monitored servers — which of THIS tool's collectors are the most expensive to run, so a hog shows on a dashboard instead of a log scrape. This is the tool measuring itself, NOT a monitored SQL Server. The service records an hourly aggregate per (server, collector): run count, total and average query duration in ms (sql_ms is a DURATION that includes waits, not pure CPU), the WORST single execution in the window (max_sql_ms — the tail is how a collector sticks out on a target), store-write time, rows collected, and how many servers ran it. Returns the ranked fleet list, most expensive by total_sql_ms first. Pass collector_name to get that ONE collector's daily trend instead (summed cost and the day's worst execution), for spotting a regression against its own history. Read sql_ms with care: it is NOT purely target-side on the collectors that fetch plan XML or statement text (query_store). Those fetches run inside the driver's per-item SQL stopwatch and each one round-trips the MONITORING STORE to decide what content is already held before writing back what came off the target, so the store's probe and write land in this figure. Where this was measured, the store probe is the largest single term in both fetches — 55.4% of plan_fetch and 80.6% of text_fetch. This series carries NO phase split (it is an hourly total per server and collector, nothing more), so the attribution cannot be recovered here at all: use get_collection_log, whose sql_store_ms names the store share per run. Do NOT read a large total_sql_ms or max_sql_ms on query_store as evidence that the monitored servers are slow. NETWORK COST (#6629): each collector also carries bytes_received_per_day and bytes_sent_per_day, the bytes it received from and sent to the monitored SQL Server, taken from SqlClient's connection statistics for every run and summed over a fan-out's per-database connections, then divided by the time measured: from the collector's first run in the window that has a figure to now, at least one hour and at most the window. bytes_measured_hours says how long that was, so a figure over 2 hours (a young store, or one just upgraded) is a 2-hour rate, not a 7-day one. The window is days_back cut to 7, the longest the hourly rollup keeps (network_bytes.window_days says what it was), and the figure is summed over ALL servers, so a collector that is cheap on one and heavy on another reads as the mix; get_collection_log has the per-run, per-server figures. network_bytes.top_collectors_by_bytes_received_per_day names the five collectors that pull the most. A collector whose runs recorded no figure reads bytes_status \"not measured\" with null bytes, never 0: a PostgreSQL target is not measured, nor are runs from before this was recorded or runs whose connection never opened. Use it to choose which collector to re-cadence or turn off before putting a monitoring host across a WAN link.")]
    public static async Task<string> GetCollectorCost(
        NpgsqlDataSource postgres,
        [Description("Days of history to summarize. Default 7; max 90 (the series' own retention).")] int days_back = 7,
        [Description("Optional: a collector name (e.g. query_store) to return its daily trend instead of the ranked fleet list.")] string? collector_name = null,
        CancellationToken cancellationToken = default)
    {
        /* #3653: the shared day-grained refusal, in ValidateHoursBack's sentence; the ceiling stays this
           tool's (the series' own retention). */
        var daysError = DarlingReadWindow.ValidateDaysBack(days_back, MaxDaysBack);
        if (daysError != null)
        {
            return daysError;
        }

        var since = DateTime.UtcNow.AddDays(-days_back);

        try
        {
            if (!string.IsNullOrWhiteSpace(collector_name))
            {
                var trend = await DarlingCollectorCostReader.GetTrendAsync(postgres, collector_name.Trim(), since, cancellationToken);
                if (trend.Count == 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No cost recorded for collector '{collector_name.Trim()}' in the last {days_back} day(s). " +
                        "The service records an hourly aggregate; a collector that has not run in the window has no row.");
                }

                var bytesDays = DarlingCollectorBytes.WindowDays(days_back);
                var trendBytes = await DarlingCollectorBytes.ReadAsync(postgres, null, DateTime.UtcNow, bytesDays, cancellationToken);
                return DarlingCollectorBytes.MergeSingle(JsonSerializer.Serialize(new
                {
                    collector_name = collector_name.Trim(),
                    days_back,
                    note = "sql_ms is query DURATION (includes waits), not pure CPU. max_sql_ms is the day's worst single execution. "
                        + StoreProbeCaveat,
                    trend = trend.Select(p => new
                    {
                        day = p.Day,
                        run_count = p.RunCount,
                        total_sql_ms = p.TotalSqlMs,
                        avg_sql_ms = p.RunCount > 0 ? p.TotalSqlMs / p.RunCount : 0,
                        max_sql_ms = p.MaxSqlMs
                    })
                }), trendBytes, collector_name.Trim(), bytesDays);
            }

            var top = await DarlingCollectorCostReader.GetTopAsync(postgres, since, cancellationToken);
            if (top.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No collector cost recorded yet. The service records an hourly aggregate per (server, collector); " +
                    "the first lands within an hour of starting on a store at schema V105 or later.");
            }

            var costBytesDays = DarlingCollectorBytes.WindowDays(days_back);
            var costBytes = await DarlingCollectorBytes.ReadAsync(postgres, null, DateTime.UtcNow, costBytesDays, cancellationToken);
            return DarlingCollectorBytes.Merge(JsonSerializer.Serialize(new
            {
                days_back,
                note = "The tool's OWN cost on the monitored servers. sql_ms is query DURATION (includes waits), not pure CPU. max_sql_ms is the worst single execution in the window — the tail that makes a collector stick out on a target. "
                    + StoreProbeCaveat,
                collectors = top.Select(r => new
                {
                    collector_name = r.CollectorName,
                    run_count = r.RunCount,
                    total_sql_ms = r.TotalSqlMs,
                    avg_sql_ms = r.AvgSqlMs,
                    max_sql_ms = r.MaxSqlMs,
                    total_storage_ms = r.TotalStorageMs,
                    total_rows = r.TotalRows,
                    server_count = r.ServerCount
                })
            }), costBytes, costBytesDays, "collector_name", explicitStatus: true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collector_cost", ex);
        }
    }
}
