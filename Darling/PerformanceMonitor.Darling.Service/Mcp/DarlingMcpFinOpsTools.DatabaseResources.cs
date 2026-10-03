/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string DatabaseResourcesView = "database_resources";

    internal const string DatabaseResourcesViewLine =
        "database_resources: CPU, reads, writes and I/O per database.";

    internal const string DatabaseResourcesViewGuide =
        "database_resources gives one row per database: CPU ms, logical and physical reads, logical writes and executions from the query-stats window, and read and write MB and stall ms from the file-I/O window. cpu_share_pct and io_share_pct are the database's share of the server's CPU and I/O, rounded to 0.01. Rows are ordered by CPU, highest first, then capped at limit; database_count is the number of databases before the cap and truncated says whether rows were cut. Older windows read the hourly or daily rollups, chosen by the same rule as the desktop viewer. top_by_total and top_by_avg rank databases by total CPU and by CPU per execution, from the query-grain window, capped at limit; a database with no executions is left out of top_by_avg, and an unattributed one is left out of top_by_total. No cost fields.";

    private static async Task<string> ReadDatabaseResourcesAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit, CancellationToken ct)
    {
        /* The cutoff is taken before the rollup probe, the viewer's order. */
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, ct);
        var rows = await DarlingFinOpsDatabaseResourcesReader.GetDatabaseResourceUsageAsync(
            postgres, resolved.ServerId, rollups, coverage, cutoff, McpCommandDeadlines.ReadSeconds, ct);
        if (rows.Count == 0)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", ct)
                ?? McpHelpers.Status("empty",
                    $"No query statistics or file I/O were collected for this server in the last {hoursBack} hours, so there is no per-database usage to show.");
        }

        var (byTotal, byAvg) = await DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync(
            postgres, resolved.ServerId, rollups, coverage, cutoff, McpCommandDeadlines.ReadSeconds, limit, ct);

        return JsonSerializer.Serialize(new
        {
            server = resolved.ServerName,
            view = DatabaseResourcesView,
            hours_back = hoursBack,
            database_count = rows.Count,
            truncated = rows.Count > limit,
            rows = rows.OrderByDescending(r => r.CpuTimeMs).Take(limit).Select(DatabaseResourcesRow).ToList(),
            top_by_total = byTotal.Select(TopByTotalRow).ToList(),
            top_by_avg = byAvg.Select(TopByAvgRow).ToList(),
        }, McpHelpers.JsonOptions);
    }

    /// <summary>One database-resources row in the wire shape: snake_case keys, no cost fields.</summary>
    internal static object DatabaseResourcesRow(DatabaseResourceUsage r) => new
    {
        database_name = r.DatabaseName,
        cpu_time_ms = r.CpuTimeMs,
        logical_reads = r.LogicalReads,
        physical_reads = r.PhysicalReads,
        logical_writes = r.LogicalWrites,
        execution_count = r.ExecutionCount,
        io_read_mb = r.IoReadMb,
        io_write_mb = r.IoWriteMb,
        io_stall_ms = r.IoStallMs,
        cpu_share_pct = r.PctCpuShare,
        io_share_pct = r.PctIoShare,
    };

    /// <summary>One top-by-total-CPU row in the wire shape.</summary>
    internal static object TopByTotalRow(TopResourceConsumer r) => new
    {
        database_name = r.DatabaseName,
        cpu_time_ms = r.CpuTimeMs,
        execution_count = r.ExecutionCount,
        io_total_mb = r.IoTotalMb,
        cpu_share_pct = r.PctCpu,
        io_share_pct = r.PctIo,
    };

    /// <summary>One top-by-CPU-per-execution row in the wire shape; <c>CpuTimeMs</c> holds the average on these rows.</summary>
    internal static object TopByAvgRow(TopResourceConsumer r) => new
    {
        database_name = r.DatabaseName,
        avg_cpu_ms = r.CpuTimeMs,
        execution_count = r.ExecutionCount,
        total_cpu_ms = r.TotalCpuTimeMs,
        io_total_mb = r.IoTotalMb,
        avg_io_mb = r.AvgIoMb,
    };
}
