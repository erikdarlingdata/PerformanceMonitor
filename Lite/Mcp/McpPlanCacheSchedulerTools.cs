using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The plan-cache + CPU-scheduler snapshot MCP tools — get_plan_cache_bloat, get_cpu_scheduler_pressure —
/// served over Lite's DuckDB store. get_plan_cache_bloat folds the true (uncapped) plan-count summary
/// (GetPlanCacheSummaryAsync) with the per-(cacheobjtype,objtype) composition breakdown (GetPlanCacheSnapshotAsync)
/// and the shared ClassifyPlanCacheBloat banding; get_cpu_scheduler_pressure returns the latest
/// cpu_scheduler_stats snapshot (GetCpuSchedulerSnapshotAsync) with the collector's warning flags. STORED
/// reads, no live monitored-server hit.
/// </summary>
[McpServerToolType]
public sealed class McpPlanCacheSchedulerTools
{
    [McpServerTool(Name = "get_plan_cache_bloat"), Description("Gets plan cache composition showing single-use vs multi-use plans per cache/object type, with a bloat-level classification. High single-use plan counts indicate ad-hoc query bloat consuming buffer pool memory. Consider enabling 'optimize for ad hoc workloads' or Forced Parameterization.")]
    public static async Task<string> GetPlanCacheBloat(
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

            var summary = await dataService.GetPlanCacheSummaryAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            var cacheTypes = await dataService.GetPlanCacheSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (summary.TotalPlans == 0 && cacheTypes.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "plan_cache_stats")
                    ?? McpHelpers.Status("unavailable", "No plan cache statistics available in the requested time range.");

            var bloat = LocalDataService.ClassifyPlanCacheBloat(summary.TotalPlans, summary.SingleUsePlans);
            var singleUsePercent = summary.TotalPlans > 0
                ? Math.Round(100.0 * summary.SingleUsePlans / summary.TotalPlans, 1)
                : 0;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                summary = new
                {
                    total_plans = summary.TotalPlans,
                    single_use_plans = summary.SingleUsePlans,
                    single_use_percent = singleUsePercent,
                    oldest_plan_create_time = summary.OldestPlanCreateTime?.ToString("o"),
                    bloat_level = bloat.Level,
                    bloat_recommendation = bloat.Recommendation
                },
                cache_types = cacheTypes.Select(r => new
                {
                    cache_type = r.Cacheobjtype,
                    object_type = r.Objtype,
                    total_plans = r.TotalPlans,
                    total_size_mb = r.TotalSizeMb,
                    single_use_plans = r.SingleUsePlans,
                    single_use_size_mb = r.SingleUseSizeMb,
                    multi_use_plans = r.MultiUsePlans,
                    multi_use_size_mb = r.MultiUseSizeMb,
                    avg_use_count = Math.Round(r.AvgUseCount, 2),
                    avg_size_kb = r.AvgSizeKb
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_cache_bloat", ex);
        }
    }

    [McpServerTool(Name = "get_cpu_scheduler_pressure"), Description("Gets CPU scheduler pressure from the latest snapshot: runnable task queue depth, worker thread utilization, queued/blocked requests, and the collector's pressure warning flags. Shows whether the server has enough worker threads and if tasks are queuing for CPU time.")]
    public static async Task<string> GetCpuSchedulerPressure(
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

            var item = await dataService.GetCpuSchedulerSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (item == null)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "cpu_scheduler_stats")
                    ?? McpHelpers.Status("unavailable", "No CPU scheduler data available. The scheduler collector may not have run yet.");

            var workerUtilizationPercent = item.MaxWorkersCount > 0
                ? Math.Round(item.TotalCurrentWorkersCount * 100.0 / item.MaxWorkersCount, 2)
                : 0;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                collection_time = item.CollectionTime.ToString("o"),
                schedulers = item.SchedulerCount,
                cpu_count = item.CpuCount,
                runnable_tasks = item.TotalRunnableTasksCount,
                avg_runnable_per_scheduler = Math.Round(item.AvgRunnableTasksCount, 2),
                workers = item.TotalCurrentWorkersCount,
                max_workers = item.MaxWorkersCount,
                worker_utilization_percent = workerUtilizationPercent,
                runnable_percent = item.RunnablePercent.HasValue ? Math.Round(item.RunnablePercent.Value, 2) : (double?)null,
                active_requests = item.TotalActiveRequestCount,
                queued_requests = item.TotalQueuedRequestCount,
                blocked_tasks = item.TotalBlockedTaskCount,
                system_memory_state = item.SystemMemoryStateDesc,
                warnings = new
                {
                    worker_thread_exhaustion = item.WorkerThreadExhaustionWarning,
                    runnable_tasks = item.RunnableTasksWarning,
                    blocked_tasks = item.BlockedTasksWarning,
                    queued_requests = item.QueuedRequestsWarning,
                    physical_memory_pressure = item.PhysicalMemoryPressureWarning,
                    offline_cpu = item.OfflineCpuWarning
                }
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_scheduler_pressure", ex);
        }
    }
}
