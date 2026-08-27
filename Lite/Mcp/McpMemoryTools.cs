using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpMemoryTools
{
    [McpServerTool(Name = "get_memory_stats"), Description("Gets the latest memory statistics snapshot: physical memory, buffer pool size, plan cache size, memory utilization %, and SQL Server memory model. Use this for a quick memory health check; use get_memory_clerks to see detailed breakdown by component.")]
    public static async Task<string> GetMemoryStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var stats = await dataService.GetLatestMemoryStatsAsync(resolved.ServerId);
            if (stats == null)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_stats")
                    ?? McpHelpers.Status("unavailable", "No memory stats available.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                collection_time = stats.CollectionTime.ToString("o"),
                total_physical_memory_mb = stats.TotalPhysicalMemoryMb,
                available_physical_memory_mb = stats.AvailablePhysicalMemoryMb,
                memory_utilization_pct = Math.Round(stats.MemoryUtilizationPercent, 1),
                system_memory_state = stats.SystemMemoryState,
                sql_memory_model = stats.SqlMemoryModel,
                target_server_memory_mb = stats.TargetServerMemoryMb,
                total_server_memory_mb = stats.TotalServerMemoryMb,
                buffer_pool_mb = stats.BufferPoolMb,
                plan_cache_mb = stats.PlanCacheMb
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_stats", ex);
        }
    }

    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage trend over time: total server memory, target memory, buffer pool, plan cache, and granted memory. Useful for identifying memory growth patterns or pressure periods.")]
    public static async Task<string> GetMemoryTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var points = await dataService.GetMemoryTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (points.Count == 0)
            {
                /*
                    A bare empty array here told an MCP client nothing at all -- and Darling's twin already
                    returned a status envelope, so the same tool name gave two different answers depending
                    on which SKU it was pointed at. Both now make the same distinction in the same words: a
                    server that collected fine and was quiet in THIS window wants the window widened, while
                    a server the collector has never touched wants somebody to go look at collection, and
                    widening will never fill it. Probed only here, against the SAME source the trend read.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await dataService.HasAnyMemoryStatAsync(resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No memory samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected memory stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No memory stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the memory_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_memory_stats will be equally empty until it does.");
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                total_server_memory_mb = p.TotalServerMemoryMb,
                target_server_memory_mb = p.TargetServerMemoryMb,
                buffer_pool_mb = p.BufferPoolMb,
                plan_cache_mb = p.PlanCacheMb,
                total_granted_mb = p.TotalGrantedMb
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_trend", ex);
        }
    }

    [McpServerTool(Name = "get_memory_clerks"), Description("Gets the top memory consumers by memory clerk type — shows which SQL Server components are using the most memory.")]
    public static async Task<string> GetMemoryClerks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestMemoryClerksAsync(resolved.ServerId);

            if (rows.Count == 0)
                /*
                    ONE branch here, deliberately, and it is the reason this read gets no existence probe.
                    The read is "every clerk at MAX(collection_time)", so zero rows back is logically the
                    same statement as zero rows in the table — any probe against that source would agree
                    with the read by construction. What the caller needs told is that an empty clerk list is
                    NEVER a quiet period, because on a live SQL Server it cannot be. Same words as Darling's
                    twin.
                */
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_clerks")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No memory-clerk snapshot is available for {resolved.ServerName}. This read returns the LATEST snapshot rather than a window, so an empty result is never a quiet period — a live SQL Server always has memory clerks. It means nothing the memory_clerks collector stored is still retained, either because it has not run for this server or because its rows have aged out. Check get_collection_health and get_collection_log for the memory_clerks collector.");

            var result = rows.Select(r => new
            {
                clerk_type = r.ClerkType,
                memory_mb = Math.Round(r.MemoryMb, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                clerks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_clerks", ex);
        }
    }

    [McpServerTool(Name = "get_memory_pressure_events"), Description(@"Gets memory pressure notifications from the RING_BUFFER_RESOURCE_MONITOR ring buffer (same source as sp_pressuredetector). Returns RESOURCE_MEMPHYSICAL_LOW, RESOURCE_MEMVIRTUAL_LOW, RESOURCE_MEMPHYSICAL_HIGH, and RESOURCE_MEM_STEADY notifications with indicator values.

Indicator scale (applies to both memory_indicators_process and memory_indicators_system):
  0-1 = normal, no pressure
  2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)
  3+  = severe pressure (aggressive buffer pool / plan cache eviction)

memory_indicators_process = SQL Server process itself is under memory pressure (workload-induced).
memory_indicators_system  = Windows is signaling low memory system-wide (could be other tenants on the box).

Not available on Azure SQL DB (ring buffer not exposed). For actionable interpretation and suggested follow-up tools, see the 'Interpreting Memory Pressure Events' section of the server instructions.")]
    public static async Task<string> GetMemoryPressureEvents(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetMemoryPressureEventsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_pressure_events")
                    ?? McpHelpers.Status("empty", "No memory pressure events found in the requested time range.");
            }

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

    [McpServerTool(Name = "get_resource_semaphore"), Description("Gets resource semaphore statistics from the latest snapshot: granted vs available workspace memory against the target/max-target ceiling, per resource semaphore, with waiter counts and cumulative + per-interval timeout/forced-grant pressure indicators. High waiter counts or rising timeout/forced deltas indicate memory grant pressure affecting query performance.")]
    public static async Task<string> GetResourceSemaphore(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetResourceSemaphoreSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_grant_stats")
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");
            }

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                resource_semaphore_id = r.ResourceSemaphoreId,
                pool_id = r.PoolId,
                target_memory_mb = Math.Round(r.TargetMemoryMb, 2),
                max_target_memory_mb = Math.Round(r.MaxTargetMemoryMb, 2),
                total_memory_mb = Math.Round(r.TotalMemoryMb, 2),
                available_memory_mb = Math.Round(r.AvailableMemoryMb, 2),
                granted_memory_mb = Math.Round(r.GrantedMemoryMb, 2),
                used_memory_mb = Math.Round(r.UsedMemoryMb, 2),
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
                grants = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_resource_semaphore", ex);
        }
    }

    [McpServerTool(Name = "get_memory_grants"), Description("Gets resource semaphore statistics showing granted vs available workspace memory per resource pool, waiter counts, and timeout/forced grant deltas. High waiter counts or rising timeout deltas indicate memory grant pressure affecting query performance.")]
    public static async Task<string> GetMemoryGrants(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetMemoryGrantChartDataAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_grant_stats")
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");
            }

            /* Return latest snapshot */
            var latestTime = rows.Max(r => r.CollectionTime);
            var latest = rows.Where(r => r.CollectionTime == latestTime);

            var result = latest.Select(r => new
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
                grants = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_grants", ex);
        }
    }
}
