using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpPerfmonTools
{
    [McpServerTool(Name = "get_perfmon_stats"), Description("Gets the latest performance counter values including batch requests/sec, compilations/sec, recompilations/sec, and other key SQL Server metrics. Provides throughput context that helps distinguish a busy server from a sick one. Use counter_name or instance_name to filter results.")]
    public static async Task<string> GetPerfmonStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Filter to a specific counter name, e.g. 'Batch Requests/sec'. Partial match.")] string? counter_name = null,
        [Description("Filter to a specific instance name, e.g. a database name. Partial match.")] string? instance_name = null)
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

            var rows = await resolved.Value.Service.GetPerfmonStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No perfmon stats available.");
            }

            /* Return latest snapshot */
            var latestTime = rows.Max(r => r.CollectionTime);
            IEnumerable<PerfmonStatsItem> latest = rows.Where(r => r.CollectionTime == latestTime);

            if (!string.IsNullOrEmpty(counter_name))
                latest = latest.Where(r => r.CounterName != null && r.CounterName.Contains(counter_name, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(instance_name))
                latest = latest.Where(r => r.InstanceName != null && r.InstanceName.Contains(instance_name, StringComparison.OrdinalIgnoreCase));

            var result = latest.Select(r => new
            {
                object_name = r.ObjectName,
                counter_name = r.CounterName,
                instance_name = r.InstanceName,
                value = r.CntrValue,
                delta_value = r.CntrValueDelta,
                per_second = r.CntrValuePerSecond
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                counters = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_stats", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_trend"), Description("Gets a time-series trend of performance counter values over time. Use get_perfmon_stats first to discover available counter names. Always filter by counter_name and/or instance_name to avoid overwhelming output.")]
    public static async Task<string> GetPerfmonTrend(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Filter to a specific counter name, e.g. 'Batch Requests/sec'. Partial match.")] string? counter_name = null,
        [Description("Filter to a specific instance name, e.g. a database name. Partial match.")] string? instance_name = null)
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

            var rows = await resolved.Value.Service.GetPerfmonStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No perfmon trend data available.");
            }

            IEnumerable<PerfmonStatsItem> filtered = rows;
            if (!string.IsNullOrEmpty(counter_name))
                filtered = filtered.Where(r => r.CounterName != null && r.CounterName.Contains(counter_name, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(instance_name))
                filtered = filtered.Where(r => r.InstanceName != null && r.InstanceName.Contains(instance_name, StringComparison.OrdinalIgnoreCase));

            var result = filtered.Select(r => new
            {
                time = r.CollectionTime.ToString("o"),
                object_name = r.ObjectName,
                counter_name = r.CounterName,
                instance_name = r.InstanceName,
                value = r.CntrValue,
                delta_value = r.CntrValueDelta,
                per_second = r.CntrValuePerSecond
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
            return McpHelpers.FormatError("get_perfmon_trend", ex);
        }
    }
}
