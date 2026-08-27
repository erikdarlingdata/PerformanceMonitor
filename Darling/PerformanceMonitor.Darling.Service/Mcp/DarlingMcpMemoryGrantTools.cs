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

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The memory-grant MCP tools — get_resource_semaphore, get_memory_grants — served over Darling's Postgres
/// store, both reading the LATEST <c>memory_grant_stats</c> snapshot through
/// <see cref="DarlingMemoryGrantReader"/> (STORED reads, no live monitored-server hit). The two names are two
/// lenses on the one collector table, so Darling hosts BOTH: get_resource_semaphore is the Dashboard's
/// semaphore/ceiling shape (per resource semaphore, with the workspace-memory target/max-target ceiling);
/// get_memory_grants is Lite's per-pool grant-detail shape. A client familiar with either SKU finds its tool.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpMemoryGrantTools
{
    [McpServerTool(Name = "get_resource_semaphore"), Description("Gets resource semaphore statistics showing granted vs available workspace memory against the target/max-target ceiling, waiter counts, and timeout/forced grant pressure indicators. High waiter counts or rising timeout/forced deltas indicate memory grant pressure affecting query performance.")]
    public static async Task<string> GetResourceSemaphore(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingMemoryGrantReader.GetResourceSemaphoreLatestAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_grant_stats")
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");

            var grants = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                resource_semaphore_id = r.ResourceSemaphoreId,
                pool_id = r.PoolId,
                target_memory_mb = r.TargetMemoryMb,
                max_target_memory_mb = r.MaxTargetMemoryMb,
                total_memory_mb = r.TotalMemoryMb,
                available_memory_mb = r.AvailableMemoryMb,
                granted_memory_mb = r.GrantedMemoryMb,
                used_memory_mb = r.UsedMemoryMb,
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count = r.TimeoutErrorCount,
                forced_grant_count = r.ForcedGrantCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                grants
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_resource_semaphore", ex);
        }
    }

    [McpServerTool(Name = "get_memory_grants"), Description("Gets resource semaphore statistics showing granted vs available workspace memory per resource pool, waiter counts, and timeout/forced grant deltas. High waiter counts or rising timeout deltas indicate memory grant pressure affecting query performance.")]
    public static async Task<string> GetMemoryGrants(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingMemoryGrantReader.GetMemoryGrantsLatestAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_grant_stats")
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");

            var grants = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                pool_id = r.PoolId,
                available_memory_mb = Math.Round(r.AvailableMemoryMb, 2),
                granted_memory_mb = Math.Round(r.GrantedMemoryMb, 2),
                used_memory_mb = Math.Round(r.UsedMemoryMb, 2),
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                grants
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_grants", ex);
        }
    }

    [McpServerTool(Name = "get_memory_pressure_events"), Description(@"Gets memory pressure notifications from the RING_BUFFER_RESOURCE_MONITOR ring buffer (same source as sp_pressuredetector). Returns RESOURCE_MEMPHYSICAL_LOW, RESOURCE_MEMVIRTUAL_LOW, RESOURCE_MEMPHYSICAL_HIGH, and RESOURCE_MEM_STEADY notifications with indicator values.

Indicator scale (applies to both memory_indicators_process and memory_indicators_system):
  0-1 = normal, no pressure
  2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)
  3+  = severe pressure (aggressive buffer pool / plan cache eviction)

memory_indicators_process = SQL Server process itself is under memory pressure (workload-induced).
memory_indicators_system  = Windows is signaling low memory system-wide (could be other tenants on the box).

Not available on Azure SQL DB (ring buffer not exposed).")]
    public static async Task<string> GetMemoryPressureEvents(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingMemoryGrantReader.GetMemoryPressureEventsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_pressure_events")
                    ?? McpHelpers.Status("empty", "No memory pressure events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                events = rows.Select(r => new
                {
                    sample_time = r.SampleTime.ToString("o"),
                    memory_notification = r.MemoryNotification,
                    memory_indicators_process = r.MemoryIndicatorsProcess,
                    memory_indicators_system = r.MemoryIndicatorsSystem
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_pressure_events", ex);
        }
    }
}
