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
public sealed class McpBlockingTools
{
    [McpServerTool(Name = "get_blocking"), Description("Gets blocking events captured by the blocked process report extended event. Shows the blocking chain, wait types, wait times, and query text for both blocker and blocked sessions. Use this first for a quick overview, then use get_blocked_process_xml for deep analysis of prolonged blocking.")]
    public static async Task<string> GetBlocking(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 30.")] int limit = 30)
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

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Value.Service.GetBlockingEventsAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", "No blocking events found in the specified time range.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                event_time = r.EventTime?.ToString("o"),
                database_name = r.DatabaseName,
                contentious_object = r.ContentiousObject,
                activity = r.Activity,
                blocking_tree = r.BlockingTree,
                spid = r.Spid,
                wait_time_ms = r.WaitTimeMs,
                wait_time_sec = r.WaitTimeSec,
                status = r.Status,
                isolation_level = r.IsolationLevel,
                lock_mode = r.LockMode,
                wait_resource = r.WaitResource,
                transaction_count = r.TransactionCount,
                transaction_name = r.TransactionName,
                login_name = r.LoginName,
                host_name = r.HostName,
                client_app = r.ClientApp,
                priority = r.Priority,
                log_used = r.LogUsed,
                query_text = McpHelpers.Truncate(r.QueryText, 2000)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                total_events = rows.Count,
                events = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking", ex);
        }
    }

    [McpServerTool(Name = "get_deadlocks"), Description("Gets deadlock events captured from extended events. Shows victim process info, lock modes, isolation levels, and query text. Deadlocks occur when two or more sessions permanently block each other. Use get_deadlock_detail for the full deadlock graph XML for deep analysis.")]
    public static async Task<string> GetDeadlocks(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 20.")] int limit = 20)
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

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Value.Service.GetDeadlocksAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", "No deadlocks found in the specified time range.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                event_date = r.EventDate?.ToString("o"),
                database_name = r.DatabaseName,
                deadlock_type = r.DeadlockType,
                deadlock_group = r.DeadlockGroup,
                spid = r.Spid,
                query = McpHelpers.Truncate(r.Query, 2000),
                object_names = r.ObjectNames,
                isolation_level = r.IsolationLevel,
                owner_mode = r.OwnerMode,
                waiter_mode = r.WaiterMode,
                lock_mode = r.LockMode,
                wait_time_ms = r.WaitTime,
                wait_resource = r.WaitResource,
                login_name = r.LoginName,
                host_name = r.HostName,
                client_app = r.ClientApp,
                status = r.Status,
                priority = r.Priority,
                transaction_name = r.TransactionName,
                last_tran_started = r.LastTranStarted?.ToString("o"),
                last_batch_started = r.LastBatchStarted?.ToString("o"),
                last_batch_completed = r.LastBatchCompleted?.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                total_deadlocks = rows.Count,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlocks", ex);
        }
    }

    [McpServerTool(Name = "get_deadlock_detail"), Description("Gets the full deadlock graph XML for deep analysis. Returns raw XML that can be parsed to identify lock resources, process details, and deadlock chains. Use after get_deadlocks identifies a deadlock worth investigating.")]
    public static async Task<string> GetDeadlockDetail(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum deadlocks to return. Default 5.")] int limit = 5)
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

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Value.Service.GetDeadlocksAsync(hours_back);
            var withXml = rows.Where(r => !string.IsNullOrEmpty(r.DeadlockGraph)).Take(limit).ToList();
            if (withXml.Count == 0)
            {
                return McpHelpers.Status("empty", "No deadlock XML available in the specified time range.");
            }

            var result = withXml.Select(r => new
            {
                event_date = r.EventDate?.ToString("o"),
                database_name = r.DatabaseName,
                deadlock_type = r.DeadlockType,
                deadlock_graph_xml = r.DeadlockGraph
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlock_detail", ex);
        }
    }

    [McpServerTool(Name = "get_blocked_process_xml"), Description("Gets the raw blocked process report XML for deep analysis. Contains full detail about both the blocked and blocking sessions including isolation levels, transaction names, and complete query text. Use after get_blocking identifies blocking worth investigating.")]
    public static async Task<string> GetBlockedProcessXml(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum reports to return. Default 5.")] int limit = 5)
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

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Value.Service.GetBlockingEventsAsync(hours_back);
            var withXml = rows.Where(r => !string.IsNullOrEmpty(r.BlockedProcessReportXml)).Take(limit).ToList();
            if (withXml.Count == 0)
            {
                return McpHelpers.Status("empty", "No blocked process report XML available in the specified time range.");
            }

            var result = withXml.Select(r => new
            {
                event_time = r.EventTime?.ToString("o"),
                database_name = r.DatabaseName,
                spid = r.Spid,
                wait_time_ms = r.WaitTimeMs,
                blocked_process_report_xml = r.BlockedProcessReportXml
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                reports = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocked_process_xml", ex);
        }
    }

    [McpServerTool(Name = "get_blocking_deadlock_stats"), Description("Gets aggregated blocking and deadlock statistics over time showing event counts, durations, and patterns. Useful for identifying trends and determining if blocking/deadlock issues are new, worsening, or steady-state.")]
    public static async Task<string> GetBlockingDeadlockStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
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

            var rows = await resolved.Value.Service.GetBlockingDeadlockStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No blocking/deadlock statistics available.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                stats = rows
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking_deadlock_stats", ex);
        }
    }
}
