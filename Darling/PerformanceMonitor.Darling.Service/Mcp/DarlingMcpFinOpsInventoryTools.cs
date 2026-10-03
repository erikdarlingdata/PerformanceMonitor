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
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The FinOps Server Inventory as one fleet-wide MCP tool (#4843): one row per server that has a collected
/// properties snapshot, with the same figures the desktop viewer's Server Inventory grid shows. Every figure comes
/// from <see cref="FinOpsInventoryFigures"/> and <see cref="FinOpsUtilizationFigures"/>; this class only shapes them.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpFinOpsInventoryTools
{
    /// <summary>The views <c>get_finops_inventory</c> accepts.</summary>
    internal static readonly string[] Views = ["server_inventory"];

    internal const int DefaultLimit = 33;
    internal const int MaxLimit = 200;

    private const string InventoryGuide =
        " server_inventory: health_score 0-100 weights CPU 40%, memory 30% and storage 30%, using the 24-hour average CPU; memory and storage are fixed defaults (80 and 100), and with no CPU sample the CPU term is left out. health_band is good at 80 and above, fair at 60 and above, else poor. annual_cost_usd is monthly × 12; 0 means no budget is set. license_warning flags Standard edition over 24 CPUs or 128 GB. On Azure SQL Database, memory and sockets are the host's and are null. sqlserver_start_time_local is the server's own clock, not UTC. Servers without a collected properties snapshot are not listed. truncated means total_servers > servers_returned; raise limit (max 200).";

    [McpServerTool(Name = "get_finops_inventory"), Description(
        "FinOps Server Inventory for the whole fleet: one row per server with a collected properties snapshot. Fixed windows: CPU over the last 24 hours, idle databases over the last 7 days; times UTC; no server_name, hours_back or as_of. Views: server_inventory. An unknown view is refused with the valid list. <<GUIDE>>" + InventoryGuide)]
    public static async Task<string> GetFinOpsInventory(
        NpgsqlDataSource postgres,
        [Description("Which view to read. Valid: server_inventory.")] string view,
        [Description("Most servers returned (1-200, default 33).")] int limit = DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        /* An abandoned request answers nothing, not even a refusal. */
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(view))
            return McpHelpers.Refusal("view", $"view is required. Valid views: {string.Join(", ", Views)}.");
        var normalized = view.Trim().ToLowerInvariant();
        if (!Views.Contains(normalized))
            return McpHelpers.Refusal("view", $"Invalid view value '{view}'. Valid views: {string.Join(", ", Views)}.");
        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;
        if (limit > MaxLimit)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}'. Must be an integer from 1 to {MaxLimit}.");

        try
        {
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
            var inventory = await DarlingFinOpsInventoryReader.GetServerInventoryAsync(postgres, McpCommandDeadlines.ReadSeconds, cancellationToken);
            if (inventory.Count == 0)
            {
                return McpHelpers.Status("empty",
                    "No registered server has a collected server_properties snapshot yet, so there is no inventory to show.");
            }

            var metrics = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(
                postgres, rollups, coverage, McpCommandDeadlines.ReadSeconds, cancellationToken: cancellationToken);

            var ordered = inventory
                .OrderByDescending(s => s.IsEnabled)
                .ThenBy(s => s.ServerName, StringComparer.Ordinal)
                .ThenBy(s => s.ServerId)
                .ToList();
            var rows = ordered.Take(limit).Select(s => InventoryRow(s, metrics.TryGetValue(s.ServerId, out var overlay) ? overlay : default)).ToList();

            return JsonSerializer.Serialize(new
            {
                view = normalized,
                cpu_window_hours = 24,
                idle_window_days = 7,
                total_servers = ordered.Count,
                servers_returned = rows.Count,
                truncated = ordered.Count > rows.Count,
                servers = rows,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_finops_inventory", ex);
        }
    }

    /// <summary>One server in the wire shape: snake_case keys, every figure from the Storage helpers, no internal id.</summary>
    internal static object InventoryRow(ServerInventoryDto s, ServerMetricsDto m)
    {
        var note = FinOpsInventoryFigures.HardwareNote(s.EngineEdition, s.HardwareUnavailableReason);
        var score = FinOpsInventoryFigures.HealthScore(m.AvgCpuPct);
        var annual = FinOpsCost.Annual(s.MonthlyCost);
        return new
        {
            server = s.ServerName,
            monitoring = s.IsEnabled ? "active" : "stopped",
            edition = s.Edition,
            engine_edition = s.EngineEdition,
            product_version = s.ProductVersion,
            host_os_version = string.IsNullOrEmpty(s.HostOsVersion) ? null : s.HostOsVersion,
            cpu_count = s.CpuCount == 0 && note != null ? (int?)null : s.CpuCount,
            physical_memory_mb = FinOpsInventoryFigures.PhysicalMemoryMb(s.EngineEdition, s.PhysicalMemoryMb),
            socket_count = FinOpsInventoryFigures.SocketCount(s.EngineEdition, s.SocketCount),
            cores_per_socket = FinOpsInventoryFigures.CoresPerSocket(s.EngineEdition, s.CoresPerSocket),
            hardware_note = note,
            provisioning_status = m.ProvisioningStatus,
            avg_cpu_pct = m.AvgCpuPct is decimal cpu ? Math.Round(cpu, 1, MidpointRounding.AwayFromZero) : (decimal?)null,
            storage_total_gb = m.StorageTotalGb is decimal gb ? Math.Round(gb, 1, MidpointRounding.AwayFromZero) : (decimal?)null,
            idle_db_count = m.IdleDbCount,
            health_score = score,
            health_band = FinOpsUtilizationFigures.HealthBand(score),
            monthly_cost_usd = s.MonthlyCost,
            annual_cost_usd = annual,
            license_warning = FinOpsInventoryFigures.LicenseWarning(s.Edition, s.EngineEdition, s.CpuCount, s.PhysicalMemoryMb),
            is_hadr_enabled = s.IsHadrEnabled,
            is_clustered = s.IsClustered,
            ag_replica_role = s.AgReplicaRole,
            sqlserver_start_time_local = s.SqlServerStartTime is DateTime start
                ? start.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) : null,
            inventory_as_of = Utc(s.InventoryAsOfUtc),
            last_collected = Utc(s.LastCollectedUtc),
        };
    }

    private static string? Utc(DateTime? t) => t is DateTime v
        ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)
        : null;
}
