/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The fleet-overview MCP tool (#1562) — the signature cross-server roll-up Darling's single central store
/// enables and neither the Dashboard nor Lite can do (each monitors one server per database). It reads through
/// the SHARED <see cref="DarlingFleetReader"/> — the SAME reader (and the SAME <c>ServerHealthClassifier</c>
/// banding) that powers the web dashboard's <c>/api/fleet</c> and the WPF viewer's Overview — so an MCP client,
/// a browser, and the desktop app all see one consistently-banded fleet. A STORED read (no live
/// monitored-server hit).
///
/// <para>This is ADDITIVE alongside get_server_summary (the Lite-parity one-shot per-server check, unchanged):
/// get_fleet_overview returns EVERY enabled server's pre-banded card plus the fleet rollup (band counts, the
/// cross-server blocking / deadlock totals, and the worst-first "needs attention" ranking) in one call.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFleetTools
{
    [McpServerTool(Name = "get_fleet_overview"), Description(
        "Gets the whole fleet's health at a glance: one pre-banded card per monitored server (CPU, memory, " +
        "blocking, deadlocks, worker threads, and collector health, each with a Healthy/Warning/Critical band), " +
        "plus a rollup — counts by band, cross-server blocking and deadlock totals, and a worst-first 'needs " +
        "attention' ranking. Use this first to decide which server to drill into, then call the per-server tools. " +
        "This cross-server view is unique to the central store.")]
    public static async Task<string> GetFleetOverview(
        NpgsqlDataSource postgres,
        [Description("Hours of blocking/deadlock history the per-server cards and fleet totals window over. Default 1.")] int hours_back = 1)
    {
        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-hours_back), now, now);

            if (result.TotalServers == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No servers are registered yet. The service registers each monitored server on its first successful connection.");
            }

            return JsonSerializer.Serialize(result, DarlingFleetReader.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_fleet_overview", ex);
        }
    }
}
