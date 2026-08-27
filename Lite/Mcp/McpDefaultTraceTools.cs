using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The Default Trace MCP tool — get_default_trace_events — served over Lite's DuckDB store. Wraps the
/// existing GetDefaultTraceEventsAsync reader, which returns the SIGNIFICANT set from the built-in Default
/// Trace (gated through the shared PerformanceMonitor.Common.DefaultTraceEventSignificance, the same gate the
/// viewer's System Events surface uses), each row tagged with its category. STORED read, no live hit.
/// Config-change events are intentionally NOT here — use get_server_config_changes /
/// get_database_config_changes / get_trace_flag_changes for those.
/// </summary>
[McpServerToolType]
public sealed class McpDefaultTraceTools
{
    [McpServerTool(Name = "get_default_trace_events"), Description("Gets significant server events captured by the built-in Default Trace (stored, read-only): data/log file auto-grow/shrink STALLS (over 1 second), severe ErrorLog writes (severity >= 16), schema DDL (object create/alter/delete), security audits (audit-change / DBCC / alter-trace), and Server Memory Change. Each event is tagged with a category. NOTE: configuration-change events are excluded here — use get_server_config_changes / get_database_config_changes / get_trace_flag_changes for those. Not available on Azure SQL Database (no default trace there).")]
    public static async Task<string> GetDefaultTraceEvents(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of events to return. Default 100.")] int limit = 100,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetDefaultTraceEventsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "default_trace_events")
                    ?? McpHelpers.Status("empty", "No significant default trace events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_events = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTimeUtc?.ToString("o"),
                    category = r.Category,
                    event_name = r.EventName,
                    database_name = r.DatabaseName,
                    object_name = r.ObjectName,
                    login_name = r.LoginName,
                    host_name = r.HostName,
                    application_name = r.ApplicationName,
                    spid = r.Spid,
                    duration_ms = r.DurationMs,
                    growth_mb = r.GrowthMb,
                    error_number = r.ErrorNumber,
                    severity = r.Severity,
                    text_data = McpHelpers.Truncate(r.TextData, 2000)
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_default_trace_events", ex);
        }
    }
}
