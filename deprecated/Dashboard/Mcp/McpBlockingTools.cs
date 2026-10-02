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
    /* #3653 (#3594's class): the blocking and deadlock readers cap at DatabaseService.EventGridCap rows
       before the tool applies `limit`, and the tools used to publish that capped count under a `total_*`
       name — a 100-row page of a 5,000-event window read as the window. The page now says
       what bounded it. Truncation is REPORTED where it is known and null where it is not: rows in hand
       beyond `limit` were dropped here (true); a reader that came back under its own cap saw the whole
       window, so nothing beyond the page exists (false); a reader that filled to its cap may have left
       rows behind that this tool never saw, and a window holding exactly the cap is indistinguishable
       from a busier one (null — the cap is published beside it as `limit_applied_by_reader`). The
       Lite/Darling dialect fetches `limit + 1` through a parameterised cap; these readers serve the WPF
       grids at a fixed TOP and are not rebuilt on a frozen twin. */
    internal static bool? PageTruncated(int readerRows, int limit)
    {
        if (readerRows > limit) return true;
        if (readerRows < DatabaseService.EventGridCap) return false;
        return null;
    }

    [McpServerTool(Name = "get_blocking"), Description("Gets blocking events captured by the blocked process report extended event. Shows the blocking chain, wait types, wait times, and query text for both blocker and blocked sessions. Use this first for a quick overview, then use get_blocked_process_xml for deep analysis of prolonged blocking. events_returned is the page size, not the window's total; truncated is true when rows beyond the page were dropped, false when the whole window fit, and null when the reader filled to its own cap (limit_applied_by_reader) so rows beyond it are unknowable from this tool.")]
    public static async Task<string> GetBlocking(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 30. The reader itself never returns more than limit_applied_by_reader rows, so a limit above it cannot widen the page; see truncated.")] int limit = 30)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Service.GetBlockingEventsAsync(hours_back);
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
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                events_returned = result.Count,
                truncated = PageTruncated(rows.Count, limit),
                limit_applied_by_reader = DatabaseService.EventGridCap,
                events = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking", ex);
        }
    }

    [McpServerTool(Name = "get_deadlocks"), Description("Gets deadlock events captured from extended events. Shows victim process info, lock modes, isolation levels, and query text. Deadlocks occur when two or more sessions permanently block each other. Use get_deadlock_detail for the full deadlock graph XML for deep analysis. deadlocks_returned is the page size, not the window's total; truncated is true when rows beyond the page were dropped, false when the whole window fit, and null when the reader filled to its own cap (limit_applied_by_reader) so rows beyond it are unknowable from this tool.")]
    public static async Task<string> GetDeadlocks(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 20. The reader itself never returns more than limit_applied_by_reader rows, so a limit above it cannot widen the page; see truncated.")] int limit = 20)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Service.GetDeadlocksAsync(hours_back);
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
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                deadlocks_returned = result.Count,
                truncated = PageTruncated(rows.Count, limit),
                limit_applied_by_reader = DatabaseService.EventGridCap,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Service.GetDeadlocksAsync(hours_back);
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
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var rows = await resolved.Service.GetBlockingEventsAsync(hours_back);
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
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Service.GetBlockingDeadlockStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No blocking/deadlock statistics available.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
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
