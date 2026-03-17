using System;
using System.ComponentModel;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpSchedulerTools
{
    [McpServerTool(Name = "get_cpu_scheduler_pressure"), Description("Gets CPU scheduler pressure: runnable task queue depth, worker thread utilization, and pressure warnings. Shows whether the server has enough worker threads and if tasks are queuing for CPU time.")]
    public static async Task<string> GetCpuSchedulerPressure(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var item = await resolved.Value.Service.GetCpuPressureAsync();
            if (item == null)
                return "No CPU scheduler data available. The scheduler collector may not have run yet.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                collection_time = item.CollectionTime.ToString("o"),
                schedulers = item.TotalSchedulers,
                runnable_tasks = item.TotalRunnableTasks,
                avg_runnable_per_scheduler = item.AvgRunnableTasksPerScheduler,
                workers = item.TotalWorkers,
                max_workers = item.MaxWorkers,
                worker_utilization_percent = item.WorkerUtilizationPercent,
                runnable_percent = item.RunnablePercent,
                queued_requests = item.TotalQueuedRequests,
                active_requests = item.TotalActiveRequests,
                pressure_level = item.PressureLevel,
                recommendation = item.Recommendation,
                warnings = new
                {
                    worker_thread_exhaustion = item.WorkerThreadExhaustionWarning,
                    runnable_tasks = item.RunnableTasksWarning,
                    blocked_tasks = item.BlockedTasksWarning,
                    queued_requests = item.QueuedRequestsWarning,
                    physical_memory_pressure = item.PhysicalMemoryPressureWarning
                }
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_scheduler_pressure", ex);
        }
    }
}
