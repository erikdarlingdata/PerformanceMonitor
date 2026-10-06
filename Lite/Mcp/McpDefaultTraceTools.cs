using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis.Baselines;
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
    /// <summary>
    /// #4966: the window notice. The window is the one the read took (<paramref name="windowEnd"/> minus
    /// <paramref name="hoursBack"/>, the same start <c>GetTimeRange</c> gives for an <c>as_of</c> anchor). The probe takes the
    /// same server clock as the read: the Default Trace stores its <c>StartTime</c> in the monitored server's local time, and
    /// the probe converts each candidate row to UTC through that clock (#4989), so the floor it names is UTC like every other
    /// <c>effective_start</c>. The plain form, not the event-time one: the probe reads <c>event_time</c>, the column the read
    /// windows on, so no row a page shows is older than its floor. A data answer over a window of 90 minutes or less starts
    /// no probe; an empty one always does.
    /// </summary>
    private static Task<McpQueryTools.McpWindowNotice> WindowNoticeAsync(
        LocalDataService dataService, int serverId, int hoursBack, DateTime windowEnd, ServerClock serverClock, bool emptyAnswer = false)
    {
        var requestedStart = windowEnd.AddHours(-hoursBack);
        return McpQueryTools.WindowNoticeAsync(
            () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.DefaultTraceEvents, serverId, requestedStart, windowEnd, serverClock),
            requestedStart, windowEnd, "default_trace_events", emptyAnswer: emptyAnswer);
    }

    [McpServerTool(Name = "get_default_trace_events"), Description("Gets significant server events from the built-in Default Trace: file auto-grow/shrink stalls over 1 second, ErrorLog writes at severity 16+ (a null severity also counts), schema DDL, security audits, and Server Memory Change, each tagged with category, over an event_time window ending at as_of, newest first. Config-change events are excluded: use get_server_config_changes / get_database_config_changes / get_trace_flag_changes instead. Empty: nothing significant in the window, or nothing collected in it; not_collected means this engine has no default trace (Azure SQL Database). <<GUIDE>> Gets significant server events captured by the built-in Default Trace (stored, read-only): data/log file auto-grow/shrink STALLS (over 1 second), severe ErrorLog writes (severity >= 16), schema DDL (object create/alter/delete), security audits (audit-change / DBCC / alter-trace), and Server Memory Change. Each event is tagged with a category. event_time is UTC, the same frame as as_of (the Default Trace stores its StartTime in the monitored server's local clock; this read de-skews it). NOTE: configuration-change events are excluded here — use get_server_config_changes / get_database_config_changes / get_trace_flag_changes for those. Not available on Azure SQL Database (no default trace there).")]
    public static async Task<string> GetDefaultTraceEvents(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of events to return. Default 100.")] int limit = 100,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            /* Default Trace event_time is THIS server's local wall clock, so the window — and the de-skew
               that puts each returned event_time back into UTC — need THIS server's clock, not the desktop
               tab's. See McpServerLocalWindow. */
            var serverClock = await McpServerLocalWindow.ClockForAsync(dataService, resolved.ServerId);

            /* #5244: database_name appended LAST (H1). The reader filters in SQL, before the limit below, so total_events and the page
               are the chosen database's; a blank is "no filter". Events with no database (server-level ones) are in no chosen database. */
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name;
            var rows = await dataService.GetDefaultTraceEventsAsync(
                resolved.ServerId, hours_back, databaseNames: database is null ? null : new[] { database }, asOfUtc: windowEnd, serverClock: serverClock);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "default_trace_events")
                    ?? McpHelpers.Status("empty",
                        "No significant default trace events found in the requested time range" + (database is null ? "." : " for database " + database + "."),
                        (await WindowNoticeAsync(dataService, resolved.ServerId, hours_back, windowEnd, serverClock, emptyAnswer: true)).AsHints());

            var notice = await WindowNoticeAsync(dataService, resolved.ServerId, hours_back, windowEnd, serverClock);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #4966: where this server's default trace data starts for the window, always present (false and null
                   when the store covered it). No effective_hours_back: the payload's total_events/shown cap is a page. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
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
