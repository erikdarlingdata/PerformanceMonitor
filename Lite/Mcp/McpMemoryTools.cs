using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;

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
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var stats = await dataService.GetLatestMemoryStatsAsync(resolved.Value.ServerId);
            if (stats == null)
            {
                return "No memory stats available.";
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
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
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var points = await dataService.GetMemoryTrendAsync(resolved.Value.ServerId, hours_back);
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

    [McpServerTool(Name = "get_memory_clerks"), Description("Gets the top memory consumers by memory clerk type — shows which SQL Server components are using the most memory.")]
    public static async Task<string> GetMemoryClerks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var rows = await dataService.GetLatestMemoryClerksAsync(resolved.Value.ServerId);
            var result = rows.Select(r => new
            {
                clerk_type = r.ClerkType,
                memory_mb = Math.Round(r.MemoryMb, 2)
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

    [McpServerTool(Name = "get_memory_grants"), Description("Gets resource semaphore statistics showing granted vs available workspace memory per resource pool, waiter counts, and timeout/forced grant deltas. High waiter counts or rising timeout deltas indicate memory grant pressure affecting query performance.")]
    public static async Task<string> GetMemoryGrants(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetMemoryGrantChartDataAsync(resolved.Value.ServerId, hours_back);
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
