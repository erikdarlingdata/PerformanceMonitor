using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpHealthParserTools
{
    [McpServerTool(Name = "get_health_parser_system_health"), Description("Gets parsed system_health extended event data: overall health indicators captured by sp_HealthParser.")]
    public static async Task<string> GetSystemHealth(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserSystemHealthAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No system health data found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                total_entries = rows.Count,
                shown = Math.Min(rows.Count, limit),
                entries = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_system_health", ex); }
    }

    [McpServerTool(Name = "get_health_parser_severe_errors"), Description("Gets severe errors from system_health: stack dumps, non-yielding schedulers, and other critical SQL Server events.")]
    public static async Task<string> GetSevereErrors(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserSevereErrorsAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No severe errors found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                error_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                errors = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_severe_errors", ex); }
    }

    [McpServerTool(Name = "get_health_parser_io_issues"), Description("Gets I/O-related issues from system_health: 15-second I/O warnings, long I/O requests, and stalled I/O subsystems.")]
    public static async Task<string> GetIOIssues(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserIOIssuesAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No I/O issues found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                issue_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                issues = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_io_issues", ex); }
    }

    [McpServerTool(Name = "get_health_parser_scheduler_issues"), Description("Gets scheduler issues from system_health: non-yielding schedulers, deadlocked schedulers, and scheduler monitor events.")]
    public static async Task<string> GetSchedulerIssues(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserSchedulerIssuesAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No scheduler issues found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                issue_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                issues = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_scheduler_issues", ex); }
    }

    [McpServerTool(Name = "get_health_parser_memory_conditions"), Description("Gets memory condition events from system_health: low memory notifications, memory broker adjustments, and memory pressure indicators.")]
    public static async Task<string> GetMemoryConditions(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserMemoryConditionsAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No memory condition events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_memory_conditions", ex); }
    }

    [McpServerTool(Name = "get_health_parser_cpu_tasks"), Description("Gets CPU task events from system_health: long-running CPU-bound tasks, high CPU worker threads, and process utilization snapshots.")]
    public static async Task<string> GetCPUTasks(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserCPUTasksAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No CPU task events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_cpu_tasks", ex); }
    }

    [McpServerTool(Name = "get_health_parser_memory_broker"), Description("Gets memory broker events from system_health: cache shrink/grow notifications, memory clerk adjustments, and broker-mediated memory redistribution.")]
    public static async Task<string> GetMemoryBroker(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserMemoryBrokerAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No memory broker events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_memory_broker", ex); }
    }

    [McpServerTool(Name = "get_health_parser_memory_node_oom"), Description("Gets memory node OOM events from system_health: out-of-memory conditions on specific NUMA nodes.")]
    public static async Task<string> GetMemoryNodeOOM(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await resolved.Value.Service.GetHealthParserMemoryNodeOOMAsync(hours_back);
            if (rows.Count == 0) return McpHelpers.Status("empty", "No memory node OOM events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => SerializeHealthItem(r))
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_memory_node_oom", ex); }
    }

    /// <summary>
    /// Generic serializer for HealthParser items. All HealthParser models share
    /// similar structure — uses reflection-free duck typing via dynamic.
    /// </summary>
    private static object SerializeHealthItem(object item)
    {
        // All HealthParser items share a CollectionTime property and varying detail columns.
        // Serialize the full object and let JSON handle the properties.
        return item;
    }
}
