using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpWaitTools
{
    [McpServerTool(Name = "get_wait_stats"), Description("Gets the top SQL Server wait types aggregated over a time period. Wait stats reveal what SQL Server spends time waiting on — high signal waits indicate CPU pressure, high resource waits indicate I/O or lock contention. Use this first to identify the dominant wait category, then drill into specific tools based on the wait type.")]
    public static async Task<string> GetWaitStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 20.")] int limit = 20)
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

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await dataService.GetWaitStatsAsync(resolved.Value.ServerId, hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No wait stats data available for the specified time range.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                wait_type = r.WaitType,
                total_wait_time_ms = r.TotalWaitTimeMs,
                total_signal_wait_ms = r.TotalSignalWaitTimeMs,
                resource_wait_ms = r.ResourceWaitTimeMs,
                waiting_tasks = r.TotalWaitingTasks,
                signal_wait_pct = Math.Round(r.SignalWaitPercent, 1)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                waits = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_stats", ex);
        }
    }

    [McpServerTool(Name = "get_wait_types"), Description("Lists the distinct wait types observed on a server in the given time period. Useful for discovering what wait types to drill into with get_wait_trend.")]
    public static async Task<string> GetWaitTypes(
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

            var types = await dataService.GetDistinctWaitTypesAsync(resolved.Value.ServerId, hours_back);
            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                wait_types = types
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_types", ex);
        }
    }

    [McpServerTool(Name = "get_wait_trend"), Description("Gets a time-series trend for a specific wait type, showing how wait time changes over time. Use get_wait_types first to discover available wait types.")]
    public static async Task<string> GetWaitTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The exact wait type name, e.g. CXPACKET, PAGEIOLATCH_SH.")] string wait_type,
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

            var points = await dataService.GetWaitStatsTrendAsync(resolved.Value.ServerId, wait_type, hours_back);
            if (points.Count == 0)
            {
                /* Same shape as get_perfmon_trend: tell the caller whether the wait type is just
                   unknown here vs. nothing collected at all, and hand back the ones that do have data. */
                var collected = await dataService.GetDistinctWaitTypesAsync(resolved.Value.ServerId, hours_back);
                if (collected.Count == 0)
                    return McpHelpers.Status(
                        "unavailable",
                        $"No trend data for wait type '{wait_type}'. No wait stats have been collected for this server in the last {hours_back}h yet " +
                        "(the collector may not have run, or delta wait stats need a second collection cycle).");

                return McpHelpers.Status(
                    "not_collected",
                    $"No trend data for wait type '{wait_type}'. It may not be a wait type observed on this server in this window — see hints.collected_wait_types for the {collected.Count} that have data.",
                    new { collected_wait_types = collected });
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                wait_time_ms_per_second = p.WaitTimeMsPerSecond,
                signal_wait_time_ms_per_second = p.SignalWaitTimeMsPerSecond
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                wait_type,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_trend", ex);
        }
    }

    [McpServerTool(Name = "get_waiting_tasks"), Description("Gets recently captured waiting tasks — queries that were actively waiting on a resource at collection time. Shows session ID, wait type, duration, blocking session, and database. Complements get_wait_stats by showing individual waiting queries rather than aggregated stats.")]
    public static async Task<string> GetWaitingTasks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description("Maximum rows. Default 30.")] int limit = 30)
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

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await dataService.GetWaitingTasksAsync(resolved.Value.ServerId, hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", "No waiting tasks found.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                session_id = r.SessionId,
                wait_type = r.WaitType,
                wait_duration_ms = r.WaitDurationMs,
                blocking_session_id = r.BlockingSessionId,
                database_name = r.DatabaseName,
                resource_description = r.ResourceDescription,
                collection_time = r.CollectionTime.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                tasks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_waiting_tasks", ex);
        }
    }
}
