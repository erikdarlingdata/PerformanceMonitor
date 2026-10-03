/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
    internal const string ApplicationConnectionsView = "application_connections";

    internal const string ApplicationConnectionsViewLine =
        "application_connections: connections and work per application.";

    internal const string ApplicationConnectionsViewGuide =
        "application_connections gives one row per client program over the window: average and peak connections, running, sleeping and dormant counts (each average and peak), CPU ms, reads, writes and logical reads (each average and peak), sample_count, and first_seen_utc and last_seen_utc. A program with no name is listed with an empty application_name; a NULL and an empty name can show as two rows. The CPU, reads, writes and logical-reads columns read 0 until the session collector has filled them. Averages are whole numbers. rows lists every application up to 500, ordered by peak connections, then average connections, then name, then first and last seen and sample_count; application_count is the number of applications and truncated says when there were more. hours_back is honoured and defaults to 24, which is the window the desktop's Application Connections tab reads. limit does not apply: a limit other than 10 is refused. Times are UTC and end in Z. No cost fields.";

    /// <summary>The fixed ceiling on <c>rows</c>.</summary>
    internal const int MaxApplicationRows = 500;

    /// <summary>The application ordering: peak connections, then average connections, then name, then first seen, last seen and sample_count, so the order is total and a cut never depends on SQL order.</summary>
    internal static List<ApplicationConnectionUsage> OrderApplicationRows(IEnumerable<ApplicationConnectionUsage> rows) =>
        rows.OrderByDescending(r => r.MaxConnections)
            .ThenByDescending(r => r.AvgConnections)
            .ThenBy(r => r.ApplicationName, StringComparer.Ordinal)
            .ThenBy(r => r.FirstSeenUtc)
            .ThenBy(r => r.LastSeenUtc)
            .ThenBy(r => r.SampleCount)
            .ToList();

    private static async Task<string> ReadApplicationConnectionsAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit, CancellationToken ct)
    {
        if (limit != DefaultLimit)
            return McpHelpers.Refusal("limit",
                $"Invalid limit value '{limit}': view {ApplicationConnectionsView} lists every application up to {MaxApplicationRows}; limit applies to the views with a top-N list. Omit it or pass {DefaultLimit}.");

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var rows = await DarlingFinOpsApplicationConnectionsReader.GetApplicationConnectionsAsync(
            postgres, resolved.ServerId, cutoff, McpCommandDeadlines.ReadSeconds, ct);
        if (rows.Count == 0)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "session_stats", ct)
                ?? McpHelpers.Status("empty",
                    $"No session statistics were collected for this server in the last {hoursBack} hours, so there is no per-application connection data to show.");
        }

        var ordered = OrderApplicationRows(rows);
        return JsonSerializer.Serialize(new
        {
            server = resolved.ServerName,
            view = ApplicationConnectionsView,
            hours_back = hoursBack,
            application_count = ordered.Count,
            truncated = ordered.Count > MaxApplicationRows,
            rows = ordered.Take(MaxApplicationRows).Select(ApplicationConnectionsRow).ToList(),
        }, McpHelpers.JsonOptions);
    }

    /// <summary>One application-connections row in the wire shape: snake_case keys, UTC times, no cost fields.</summary>
    internal static object ApplicationConnectionsRow(ApplicationConnectionUsage r) => new
    {
        application_name = r.ApplicationName,
        avg_connections = r.AvgConnections,
        max_connections = r.MaxConnections,
        avg_running = r.AvgRunning,
        max_running = r.MaxRunning,
        avg_sleeping = r.AvgSleeping,
        max_sleeping = r.MaxSleeping,
        avg_dormant = r.AvgDormant,
        max_dormant = r.MaxDormant,
        avg_cpu_time_ms = r.AvgCpuTimeMs,
        max_cpu_time_ms = r.MaxCpuTimeMs,
        avg_reads = r.AvgReads,
        max_reads = r.MaxReads,
        avg_writes = r.AvgWrites,
        max_writes = r.MaxWrites,
        avg_logical_reads = r.AvgLogicalReads,
        max_logical_reads = r.MaxLogicalReads,
        sample_count = r.SampleCount,
        first_seen_utc = McpHelpers.FormatEffectiveStart(r.FirstSeenUtc),
        last_seen_utc = McpHelpers.FormatEffectiveStart(r.LastSeenUtc),
    };
}
