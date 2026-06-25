using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpWaitTools
{
    [McpServerTool(Name = "get_wait_stats"), Description("Gets the top SQL Server wait types from the last hour. Wait stats reveal what SQL Server spends time waiting on — high signal waits indicate CPU pressure, high resource waits indicate I/O or lock contention. Use this first to identify the dominant wait category, then use get_wait_trend for historical patterns.")]
    public static async Task<string> GetWaitStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Maximum rows to return. Default 20.")] int limit = 20)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        try
        {
            var rows = await resolved.Value.Service.GetWaitStatsAsync();
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No wait stats data available.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                wait_type = r.WaitType,
                wait_time_ms = r.WaitTimeMs,
                wait_time_sec = r.WaitTimeSec,
                waiting_tasks = r.WaitingTasks,
                signal_wait_ms = r.SignalWaitMs,
                resource_wait_ms = r.ResourceWaitMs,
                avg_wait_ms_per_task = r.AvgWaitMsPerTask,
                last_seen = r.LastSeen.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                waits = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_stats", ex);
        }
    }

    [McpServerTool(Name = "get_wait_trend"), Description("Gets a time-series trend of the top N wait types over time. Automatically selects the most significant wait types. Useful for identifying whether wait issues are new, worsening, or steady-state.")]
    public static async Task<string> GetWaitTrend(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Top N wait types to include. Default 5.")] int top_wait_types = 5)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        var hoursError = McpHelpers.ValidateHoursBack(hours_back);
        if (hoursError != null) return hoursError;

        if (top_wait_types <= 0 || top_wait_types > 20)
            return $"Invalid top_wait_types value '{top_wait_types}'. Must be between 1 and 20.";

        try
        {
            var points = await resolved.Value.Service.GetWaitStatsDataAsync(hours_back, topWaitTypes: top_wait_types);
            if (points.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No wait stats trend data available.");
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                wait_type = p.WaitType,
                wait_time_ms_per_second = p.WaitTimeMsPerSecond,
                signal_wait_time_ms_per_second = p.SignalWaitTimeMsPerSecond
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
            return McpHelpers.FormatError("get_wait_trend", ex);
        }
    }
}
