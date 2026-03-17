using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpLatchSpinlockTools
{
    [McpServerTool(Name = "get_latch_stats"), Description("Gets top latch contention by class. Shows latch waits, wait time, and per-second rates. High LATCH_EX on ACCESS_METHODS_DATASET_PARENT or FGCB_ADD_REMOVE indicates TempDB allocation contention.")]
    public static async Task<string> GetLatchStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description("Number of top latch classes to return. Default 10.")] int top = 10)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetLatchStatsTopNAsync(top, hours_back);
            if (rows.Count == 0)
                return "No latch statistics available in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                latch_count = rows.Count,
                latches = rows.Select(r => new
                {
                    latch_class = r.LatchClass,
                    waiting_requests_count = r.WaitingRequestsCount,
                    wait_time_ms = r.WaitTimeMs,
                    max_wait_time_ms = r.MaxWaitTimeMs,
                    delta_waiting_requests = r.WaitingRequestsCountDelta,
                    delta_wait_time_ms = r.WaitTimeMsDelta,
                    waits_per_second = r.WaitingRequestsCountPerSecond,
                    wait_ms_per_second = r.WaitTimeMsPerSecond,
                    avg_wait_ms_per_request = r.AvgWaitMsPerRequest,
                    severity = string.IsNullOrEmpty(r.Severity) ? null : r.Severity,
                    recommendation = string.IsNullOrEmpty(r.Recommendation) ? null : r.Recommendation,
                    collection_time = r.CollectionTime.ToString("o")
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_latch_stats", ex);
        }
    }

    [McpServerTool(Name = "get_spinlock_stats"), Description("Gets top spinlock contention. Shows collisions, spins, backoffs, and per-second rates. High spinlock contention indicates CPU-bound internal contention that doesn't appear in wait stats.")]
    public static async Task<string> GetSpinlockStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description("Number of top spinlocks to return. Default 10.")] int top = 10)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetSpinlockStatsTopNAsync(top, hours_back);
            if (rows.Count == 0)
                return "No spinlock statistics available in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                spinlock_count = rows.Count,
                spinlocks = rows.Select(r => new
                {
                    spinlock_name = r.SpinlockName,
                    collisions = r.Collisions,
                    spins = r.Spins,
                    spins_per_collision = r.SpinsPerCollision,
                    sleep_time = r.SleepTime,
                    backoffs = r.Backoffs,
                    delta_collisions = r.CollisionsDelta,
                    delta_spins = r.SpinsDelta,
                    delta_backoffs = r.BackoffsDelta,
                    collisions_per_second = r.CollisionsPerSecond,
                    spins_per_second = r.SpinsPerSecond,
                    collection_time = r.CollectionTime.ToString("o")
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_spinlock_stats", ex);
        }
    }
}
