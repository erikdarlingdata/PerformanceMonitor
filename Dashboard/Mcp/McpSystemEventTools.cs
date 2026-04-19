using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpSystemEventTools
{
    [McpServerTool(Name = "get_default_trace_events"), Description("Gets system events from the default trace: auto-growth, auto-shrink, configuration changes, database creation/deletion, memory errors, and other server-level events.")]
    public static async Task<string> GetDefaultTraceEvents(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of events to return. Default 100.")] int limit = 100)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetDefaultTraceEventsAsync(hours_back);
            if (rows.Count == 0)
                return "No default trace events found in the requested time range.";

            var result = rows.Take(limit).Select(r => new
            {
                event_time = r.EventTime.ToString("o"),
                event_name = r.EventName,
                event_class = r.EventClass,
                database_name = r.DatabaseName,
                object_name = r.ObjectName,
                login_name = r.LoginName,
                host_name = r.HostName,
                application_name = r.ApplicationName,
                spid = r.Spid,
                filename = r.Filename,
                text_data = McpHelpers.Truncate(r.TextData, 2000),
                error_number = r.ErrorNumber
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                total_events = rows.Count,
                shown = result.Count,
                events = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_default_trace_events", ex);
        }
    }

    [McpServerTool(Name = "get_trace_analysis"), Description("Gets processed SQL Trace data showing long-running queries and expensive operations captured by the default trace. Includes duration, CPU, reads, writes, and query text.")]
    public static async Task<string> GetTraceAnalysis(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries to return. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetTraceAnalysisAsync(hours_back);
            if (rows.Count == 0)
                return "No trace analysis data found in the requested time range.";

            var result = rows.Take(limit).Select(r => new
            {
                event_name = r.EventName,
                database_name = r.DatabaseName,
                start_time = r.StartTime?.ToString("o"),
                end_time = r.EndTime?.ToString("o"),
                duration_ms = r.DurationMs,
                cpu_ms = r.CpuMs,
                reads = r.Reads,
                writes = r.Writes,
                row_counts = r.RowCounts,
                login_name = r.LoginName,
                host_name = r.HostName,
                application_name = r.ApplicationName,
                spid = r.Spid,
                sql_text = McpHelpers.Truncate(r.SqlText, 2000)
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                total_entries = rows.Count,
                shown = result.Count,
                entries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_trace_analysis", ex);
        }
    }

    [McpServerTool(Name = "get_memory_pressure_events"), Description(@"Gets memory pressure notifications from the RING_BUFFER_RESOURCE_MONITOR ring buffer (same source as sp_pressuredetector). Returns RESOURCE_MEMPHYSICAL_LOW, RESOURCE_MEMVIRTUAL_LOW, RESOURCE_MEMPHYSICAL_HIGH, and RESOURCE_MEM_STEADY notifications with indicator values.

Indicator scale (applies to both memory_indicators_process and memory_indicators_system):
  0-1 = normal, no pressure
  2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)
  3+  = severe pressure (aggressive buffer pool / plan cache eviction)

memory_indicators_process = SQL Server process itself is under memory pressure (workload-induced).
memory_indicators_system  = Windows is signaling low memory system-wide (could be other tenants on the box).

For actionable interpretation and suggested follow-up tools, see the 'Interpreting Memory Pressure Events' section of the server instructions.")]
    public static async Task<string> GetMemoryPressureEvents(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetMemoryPressureEventsAsync(hours_back);
            if (rows.Count == 0)
                return "No memory pressure events found in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                event_count = rows.Count,
                events = rows.Select(r => new
                {
                    sample_time = r.SampleTime.ToString("o"),
                    notification = r.MemoryNotification,
                    indicators_process = r.MemoryIndicatorsProcess,
                    indicators_system = r.MemoryIndicatorsSystem,
                    severity = r.Severity
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_pressure_events", ex);
        }
    }
}
