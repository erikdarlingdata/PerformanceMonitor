using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpMemoryTools
{
    [McpServerTool(Name = "get_memory_stats"), Description("Gets the latest memory statistics snapshot showing buffer pool size, plan cache size, physical memory utilization, and pressure warnings. Use this first for a quick memory health check, then get_memory_clerks or get_memory_grants for deeper analysis.")]
    public static async Task<string> GetMemoryStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Value.Service.GetMemoryStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return "No memory stats available.";
            }

            /* Return only the latest snapshot */
            var stats = rows.OrderByDescending(r => r.CollectionTime).FirstOrDefault();
            if (stats == null)
            {
                return "No memory stats available.";
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                collection_time = stats.CollectionTime.ToString("o"),
                buffer_pool_mb = stats.BufferPoolMb,
                plan_cache_mb = stats.PlanCacheMb,
                other_memory_mb = stats.OtherMemoryMb,
                total_memory_mb = stats.TotalMemoryMb,
                physical_memory_in_use_mb = stats.PhysicalMemoryInUseMb,
                available_physical_memory_mb = stats.AvailablePhysicalMemoryMb,
                memory_utilization_pct = stats.MemoryUtilizationPercentage,
                buffer_pool_pressure_warning = stats.BufferPoolPressureWarning,
                plan_cache_pressure_warning = stats.PlanCachePressureWarning
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_stats", ex);
        }
    }

    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage trend over time showing buffer pool, plan cache, total memory, and utilization percentage. Use to identify memory growth patterns or confirm whether memory pressure is new, worsening, or steady-state.")]
    public static async Task<string> GetMemoryTrend(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Value.Service.GetMemoryStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return "No memory trend data available.";
            }

            var result = rows.Select(r => new
            {
                time = r.CollectionTime.ToString("o"),
                buffer_pool_mb = r.BufferPoolMb,
                plan_cache_mb = r.PlanCacheMb,
                total_memory_mb = r.TotalMemoryMb,
                memory_utilization_pct = r.MemoryUtilizationPercentage
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_trend", ex);
        }
    }

    [McpServerTool(Name = "get_memory_clerks"), Description("Gets the top memory consumers by memory clerk type showing which SQL Server components are using the most memory. Useful for identifying unexpected memory consumers (e.g., MEMORYCLERK_SQLBUFFERPOOL, CACHESTORE_SQLCP for plan cache).")]
    public static async Task<string> GetMemoryClerks(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Value.Service.GetMemoryClerksAsync(hours_back);
            if (rows.Count == 0)
            {
                return "No memory clerk data available.";
            }

            /* Return latest snapshot only */
            var latestTime = rows.Max(r => r.CollectionTime);
            var latest = rows.Where(r => r.CollectionTime == latestTime);

            var result = latest.Select(r => new
            {
                clerk_type = r.ClerkType,
                memory_mb = Math.Round((double)r.PagesMb, 2),
                percent_of_total = r.PercentOfTotal,
                concern_level = r.ConcernLevel,
                description = r.ClerkDescription
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                clerks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_clerks", ex);
        }
    }

    [McpServerTool(Name = "get_resource_semaphore"), Description("Gets resource semaphore statistics showing granted vs available workspace memory, waiter counts, and pressure indicators. High waiter counts or RESOURCE_SEMAPHORE waits indicate memory grant pressure affecting query performance.")]
    public static async Task<string> GetMemoryGrants(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Value.Service.GetMemoryGrantStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return "No memory grant data available.";
            }

            /* Return latest snapshot */
            var latestTime = rows.Max(r => r.CollectionTime);
            var latest = rows.Where(r => r.CollectionTime == latestTime);

            var result = latest.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                resource_semaphore_id = r.ResourceSemaphoreId,
                target_memory_mb = r.TargetMemoryMb,
                total_memory_mb = r.TotalMemoryMb,
                available_memory_mb = r.AvailableMemoryMb,
                granted_memory_mb = r.GrantedMemoryMb,
                used_memory_mb = r.UsedMemoryMb,
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count = r.TimeoutErrorCount,
                forced_grant_count = r.ForcedGrantCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta,
                sample_interval_seconds = r.SampleIntervalSeconds
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                grants = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_grants", ex);
        }
    }
}
