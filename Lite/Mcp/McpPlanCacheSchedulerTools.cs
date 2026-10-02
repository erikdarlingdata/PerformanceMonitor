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
    [McpServerTool(Name = "get_plan_cache_bloat"), Description("Gets plan cache composition showing single-use vs multi-use plans per cache/object type, with a bloat-level classification. High single-use plan counts indicate ad-hoc query bloat consuming buffer pool memory. Consider enabling 'optimize for ad hoc workloads' or Forced Parameterization. LATEST IS A TIME: this is the newest plan-cache snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant the snapshot was collected and age_seconds its distance from the window's end.")]
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
            /* The summary's stamp is null exactly when the window held no snapshot, which is the same state
               the (0 plans, no groups) test below names — one branch, so a stamped payload always has rows. */
            if (summary.CollectionTime is not DateTime capturedAt || (summary.TotalPlans == 0 && cacheTypes.Count == 0))
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "plan_cache_stats")
                    ?? McpHelpers.Status("unavailable", "No plan cache statistics available in the requested time range.");

            var bloat = LocalDataService.ClassifyPlanCacheBloat(summary.TotalPlans, summary.SingleUsePlans);
            var singleUsePercent = summary.TotalPlans > 0
                ? Math.Round(100.0 * summary.SingleUsePlans / summary.TotalPlans, 1)
                : 0;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = capturedAt.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(capturedAt, windowEnd),
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

    /// <summary>
    /// get_cpu_scheduler_pressure's description, VERBATIM the text Darling's twin carries (#3541 A10): the same
    /// tool name described two ways on two servers was half of the drift that lane closed, and a shared const
    /// cannot be shared across the two assemblies, so the cross-SKU description census pins the two strings
    /// equal instead. Change one, change both.
    /// </summary>
    internal const string CpuSchedulerPressureDescription =
        "Gets CPU scheduler pressure from the latest snapshot: runnable task queue depth, worker thread utilization, queued/blocked requests, the collector's pressure warning flags, and the banded pressure_level verdict with its recommendation. Shows whether the server has enough worker threads and if tasks are queuing for CPU time. LATEST IS A TIME: this is the newest scheduler snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end; the verdict is that instant's, so read age_seconds before reading pressure_level as current.";

    [McpServerTool(Name = "get_cpu_scheduler_pressure"), Description(CpuSchedulerPressureDescription)]
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
                    ?? McpHelpers.Status("unavailable", "No CPU scheduler snapshot in the requested time range. The scheduler collector may not have run yet, or its newest snapshot is older than hours_back.");

            var workerUtilizationPercent = item.MaxWorkersCount > 0
                ? Math.Round(item.TotalCurrentWorkersCount * 100.0 / item.MaxWorkersCount, 2)
                : 0;

            /* #3541 A10: the verdict Darling's twin has always published, from the SHARED banding
               (install/47's report.cpu_scheduler_pressure CASE, the one the CPU Scheduler tab renders) — the
               same tool name answered with a verdict on one SKU and without one on the other. */
            var pressure = CpuSchedulerMetrics.ClassifyCpuPressure(item);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = item.CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(item.CollectionTime, windowEnd),
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
                pressure_level = pressure.Level,
                recommendation = pressure.Recommendation,
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
