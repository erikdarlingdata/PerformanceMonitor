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
using System.Text.Json.Serialization;
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

    internal const int DefaultLimit = 34;

    /// <summary>The short <c>hardware_note</c> an Azure SQL Database row carries; <c>hardware_note_legend</c> spells it out once.</summary>
    internal const string HostScopedNoteCode = "host_scoped";

    /// <summary>The shared wire options with null fields left out of a row.</summary>
    internal static readonly JsonSerializerOptions WireOptions = new(McpHelpers.JsonOptions)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
    internal const int MaxLimit = 200;

    private const string InventoryGuide =
        " server_inventory: health_score 0-100 weights CPU 40%, memory 30% and storage 30%, using the 24-hour average CPU; memory and storage are fixed defaults (80 and 100), and with no CPU sample the CPU term is left out. health_band is good at 80 and above, fair at 60 and above, else poor. annual_cost_usd is monthly × 12; 0 means no budget is set. license_warning flags Standard edition over 24 CPUs or 128 GB. On Azure SQL Database, memory and sockets are the host's and are null. sqlserver_start_time_local is the server's own clock, not UTC. Servers without a collected properties snapshot are not listed. A field with no value is left out of its row. hardware_note host_scoped is spelled out in hardware_note_legend. truncated means total_servers > servers_returned; raise limit (max 200).";

    [McpServerTool(Name = "get_finops_inventory"), Description(
        "FinOps Server Inventory for the whole fleet: one row per server with a collected properties snapshot. Fixed windows: CPU over the last 24 hours, idle databases over the last 7 days; times UTC except sqlserver_start_time_local (the server's own clock); no server_name, hours_back or as_of. Views: server_inventory. An unknown view is refused with the valid list. <<GUIDE>>" + InventoryGuide)]
    public static async Task<string> GetFinOpsInventory(
        NpgsqlDataSource postgres,
        [Description("Which view to read. Valid: server_inventory.")] string view,
        [Description("Most servers returned (1-200, default 34).")] int limit = DefaultLimit,
        CancellationToken cancellationToken = default)
    {
        /* An abandoned request answers nothing, not even a refusal. */
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(view))
            return McpHelpers.Refusal("view", $"view is required. Valid views: {string.Join(", ", Views)}.");
        var normalized = view.Trim().ToLowerInvariant();
        if (!Views.Contains(normalized))
            return McpHelpers.Refusal("view", $"Invalid view value '{view}'. Valid views: {string.Join(", ", Views)}.");
        if (limit < 1 || limit > MaxLimit)
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
            return JsonSerializer.Serialize(Envelope(normalized, ordered.Count, ordered.Take(limit).ToList(), metrics), WireOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_finops_inventory", ex);
        }
    }

    /// <summary>The response envelope for one page of servers; the legend is present only when a row uses the code.</summary>
    internal static object Envelope(string view, int total, System.Collections.Generic.IReadOnlyList<ServerInventoryDto> page,
        System.Collections.Generic.IReadOnlyDictionary<int, ServerMetricsDto> metrics)
    {
        var rows = page.Select(s => InventoryRow(s, metrics.TryGetValue(s.ServerId, out var overlay) ? overlay : default)).ToList();
        var legend = page.Any(s => NoteFor(s) == HostScopedNoteCode)
            ? new System.Collections.Generic.Dictionary<string, string> { [HostScopedNoteCode] = ServerHardwareScope.InventoryHardwareNote }
            : null;
        return new
        {
            view,
            cpu_window_hours = 24,
            idle_window_days = 7,
            total_servers = total,
            servers_returned = rows.Count,
            truncated = total > rows.Count,
            hardware_note_legend = legend,
            servers = rows,
        };
    }

    /// <summary>One server in the wire shape: snake_case keys, every figure from the Storage helpers, no internal id.</summary>
    internal static object InventoryRow(ServerInventoryDto s, ServerMetricsDto m)
    {
        var note = NoteFor(s);
        var denied = s.HardwareUnavailableReason != null;
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
            cpu_count = denied ? (int?)null : s.CpuCount,
            physical_memory_mb = denied ? null : FinOpsInventoryFigures.PhysicalMemoryMb(s.EngineEdition, s.PhysicalMemoryMb),
            socket_count = denied ? null : FinOpsInventoryFigures.SocketCount(s.EngineEdition, s.SocketCount),
            cores_per_socket = denied ? null : FinOpsInventoryFigures.CoresPerSocket(s.EngineEdition, s.CoresPerSocket),
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

    /// <summary>The reader's own note, else the short host-scoped code, else null.</summary>
    private static string? NoteFor(ServerInventoryDto s) =>
        s.HardwareUnavailableReason ?? (FinOpsInventoryFigures.HardwareNote(s.EngineEdition, null) != null ? HostScopedNoteCode : null);

    private static string? Utc(DateTime? t) => t is DateTime v
        ? DateTime.SpecifyKind(v, DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)
        : null;
}
