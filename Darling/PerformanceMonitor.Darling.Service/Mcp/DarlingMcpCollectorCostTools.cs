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

    [McpServerTool(Name = "get_collector_cost"), Description(
        "Gets the monitoring tool's OWN per-collector cost ON the monitored servers — which of THIS tool's collectors are the most expensive to run, so a hog shows on a dashboard instead of a log scrape. This is the tool measuring itself, NOT a monitored SQL Server. The service records an hourly aggregate per (server, collector): run count, total and average target-side query duration in ms (sql_ms is a DURATION that includes waits, not pure CPU), the WORST single execution in the window (max_sql_ms — the tail is how a collector sticks out on a target), store-write time, rows collected, and how many servers ran it. Returns the ranked fleet list, most expensive by total_sql_ms first. Pass collector_name to get that ONE collector's daily trend instead (summed cost and the day's worst execution), for spotting a regression against its own history.")]
    public static async Task<string> GetCollectorCost(
        NpgsqlDataSource postgres,
        [Description("Days of history to summarize. Default 7; max 90 (the series' own retention).")] int days_back = 7,
        [Description("Optional: a collector name (e.g. query_store) to return its daily trend instead of the ranked fleet list.")] string? collector_name = null)
    {
        if (days_back <= 0 || days_back > MaxDaysBack)
        {
            return $"Invalid days_back value '{days_back}'. Must be a positive integer (1-{MaxDaysBack}).";
        }

        var since = DateTime.UtcNow.AddDays(-days_back);

        try
        {
            if (!string.IsNullOrWhiteSpace(collector_name))
            {
                var trend = await DarlingCollectorCostReader.GetTrendAsync(postgres, collector_name.Trim(), since);
                if (trend.Count == 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No cost recorded for collector '{collector_name.Trim()}' in the last {days_back} day(s). " +
                        "The service records an hourly aggregate; a collector that has not run in the window has no row.");
                }

                return JsonSerializer.Serialize(new
                {
                    collector_name = collector_name.Trim(),
                    days_back,
                    note = "sql_ms is target-side query DURATION (includes waits), not pure CPU. max_sql_ms is the day's worst single execution.",
                    trend = trend.Select(p => new
                    {
                        day = p.Day,
                        run_count = p.RunCount,
                        total_sql_ms = p.TotalSqlMs,
                        avg_sql_ms = p.RunCount > 0 ? p.TotalSqlMs / p.RunCount : 0,
                        max_sql_ms = p.MaxSqlMs
                    })
                });
            }

            var top = await DarlingCollectorCostReader.GetTopAsync(postgres, since);
            if (top.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No collector cost recorded yet. The service records an hourly aggregate per (server, collector); " +
                    "the first lands within an hour of starting on a store at schema V105 or later.");
            }

            return JsonSerializer.Serialize(new
            {
                days_back,
                note = "The tool's OWN cost on the monitored servers. sql_ms is target-side query DURATION (includes waits), not pure CPU. max_sql_ms is the worst single execution in the window — the tail that makes a collector stick out on a target.",
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
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Failed to read collector cost: {ex.Message}");
        }
    }
}
