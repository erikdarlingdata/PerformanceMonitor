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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for instance-level PostgreSQL/Aurora CPU (#2719/#2629), paired with the
/// <c>pg_cpu_utilization</c> collector. Mirrors <c>DarlingMcpDataTools.GetCpuUtilization</c>'s shape (a
/// 1-minute-bucketed time series) rather than <c>get_pg_kernel_stats</c>'s per-query ranking — the two
/// collectors answer different questions, and this one is a gauge over time like SQL Server's own CPU read,
/// not a ranked list.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgCpuUtilizationTools
{
    [McpServerTool(Name = "get_pg_cpu_utilization"), Description("Gets instance-level CPU utilization over time for a PostgreSQL/Aurora target, from AWS Performance Insights' os.cpuUtilization.total.avg. Data is downsampled to 1-minute averages. Aurora and RDS only - self-hosted PostgreSQL has no instance-level CPU source and this returns empty for one.")]
    public static async Task<string> GetPgCpuUtilization(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingPgCpuUtilizationReader.GetHistoryAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd);

            if (rows.Count == 0)
            {
                /* Not-collected first: a self-hosted target has no route at all (see
                   PgCpuUtilizationCollector's doc comment), so that is the likelier and more actionable
                   answer than a bare "no data in window". */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_cpu_utilization")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No CPU utilization data for {resolved.ServerName} in the last {hours_back} hour(s).");
            }

            var bucketed = rows
                .GroupBy(r => new DateTime(r.SampleTimeUtc.Year, r.SampleTimeUtc.Month, r.SampleTimeUtc.Day,
                    r.SampleTimeUtc.Hour, r.SampleTimeUtc.Minute, 0, r.SampleTimeUtc.Kind))
                .OrderBy(g => g.Key)
                .Select(g => new
                {
                    sample_time = g.Key.ToString("o"),
                    cpu_percent = Math.Round(g.Average(r => r.CpuPercent), 1),
                    samples_in_bucket = g.Count(),
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                note = "os.cpuUtilization.total.avg from AWS Performance Insights - true OS-level utilization, "
                     + "not CloudWatch's capacity-relative CPUUtilization metric.",
                samples = bucketed,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL CPU utilization failed: {ex.Message}");
        }
    }
}
