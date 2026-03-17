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

            // Service returns all snapshots for top N classes (for UI charting).
            // For MCP, return only the latest snapshot per class with aggregated deltas.
            var latestPerClass = rows
                .GroupBy(r => r.LatchClass)
                .Select(g =>
                {
                    var latest = g.OrderByDescending(r => r.CollectionTime).First();
                    var totalDeltaWaitMs = g.Sum(r => r.WaitTimeMsDelta ?? 0);
                    var totalDeltaRequests = g.Sum(r => r.WaitingRequestsCountDelta ?? 0);
                    return new
                    {
                        latch_class = latest.LatchClass,
                        total_delta_wait_time_ms = totalDeltaWaitMs,
                        total_delta_waiting_requests = totalDeltaRequests,
                        avg_wait_ms_per_request = totalDeltaRequests > 0
                            ? Math.Round((double)totalDeltaWaitMs / totalDeltaRequests, 2)
                            : (double?)null,
                        waits_per_second = latest.WaitingRequestsCountPerSecond,
                        wait_ms_per_second = latest.WaitTimeMsPerSecond,
                        severity = string.IsNullOrEmpty(latest.Severity) ? null : latest.Severity,
                        description = string.IsNullOrEmpty(latest.LatchDescription) ? null : latest.LatchDescription,
                        recommendation = string.IsNullOrEmpty(latest.Recommendation) ? null : latest.Recommendation,
                        latest_collection_time = latest.CollectionTime.ToString("o")
                    };
                })
                .OrderByDescending(r => r.total_delta_wait_time_ms)
                .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                latch_count = latestPerClass.Count,
                latches = latestPerClass
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

            // Aggregate to one row per spinlock class with totals over the period
            var latestPerClass = rows
                .GroupBy(r => r.SpinlockName)
                .Select(g =>
                {
                    var latest = g.OrderByDescending(r => r.CollectionTime).First();
                    var totalDeltaCollisions = g.Sum(r => r.CollisionsDelta ?? 0);
                    var totalDeltaSpins = g.Sum(r => r.SpinsDelta ?? 0);
                    var totalDeltaBackoffs = g.Sum(r => r.BackoffsDelta ?? 0);
                    return new
                    {
                        spinlock_name = latest.SpinlockName,
                        total_delta_collisions = totalDeltaCollisions,
                        total_delta_spins = totalDeltaSpins,
                        total_delta_backoffs = totalDeltaBackoffs,
                        spins_per_collision = totalDeltaCollisions > 0
                            ? Math.Round((double)totalDeltaSpins / totalDeltaCollisions, 1)
                            : (double?)null,
                        collisions_per_second = latest.CollisionsPerSecond,
                        spins_per_second = latest.SpinsPerSecond,
                        description = string.IsNullOrEmpty(latest.SpinlockDescription) ? null : latest.SpinlockDescription,
                        latest_collection_time = latest.CollectionTime.ToString("o")
                    };
                })
                .OrderByDescending(r => r.total_delta_collisions)
                .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                spinlock_count = latestPerClass.Count,
                spinlocks = latestPerClass
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_spinlock_stats", ex);
        }
    }
}
