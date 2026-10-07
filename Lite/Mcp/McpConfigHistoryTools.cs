using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The configuration change-history MCP tools — get_server_config_changes, get_database_config_changes,
/// get_trace_flag_changes — served over Lite's DuckDB store. Each wraps the existing change reader, which
/// diffs the store's append-only config snapshots via the shared PerformanceMonitor.Common.ConfigChangeDiff.
/// STORED reads, no live monitored-server hit. Config is captured ON CONNECT (not on a fixed schedule), so
/// change granularity equals the connect/restart cadence and at least two snapshots are needed before a
/// change can be detected — the empty result explains this rather than silently returning nothing.
///
/// <para>#4966: each tool says where its data starts (<c>effective_start</c>, <c>window_truncated</c>,
/// <c>truncation_note</c>), from the coverage probe (<see cref="LocalDataService.GetConfigSnapshotCoverageFloorAsync"/>) over
/// the snapshot view the diff reads (<c>v_server_config</c>, <c>v_database_config</c>, <c>v_trace_flags</c>, on <c>capture_time</c>),
/// which also counts the snapshot before the window that the diff keeps as its baseline.
/// A change is stamped with the capture time of the snapshot that showed it, so a first run cannot store a change from
/// before itself: the oldest change can never be older than the probe's floor, and the plain notice
/// (<see cref="McpQueryTools.WindowNoticeAsync"/>) is the right one, not the event-time form.</para>
/// </summary>
[McpServerToolType]
public sealed class McpConfigHistoryTools
{
    /// <summary>
    /// #4966: the window notice of one config-change tool. The window is the one the read took
    /// (<c>windowEnd</c> minus <c>hours_back</c>, which is what <c>GetTimeRange</c> gives for an <c>as_of</c> anchor).
    /// A data answer over a window of 90 minutes or less starts no probe; an empty one always does.
    /// </summary>
    private static Task<McpQueryTools.McpWindowNotice> WindowNoticeAsync(
        LocalDataService dataService, QueryWindowRelation relation, int serverId, int hoursBack, DateTime windowEnd, string table, bool emptyAnswer = false)
    {
        var requestedStart = windowEnd.AddHours(-hoursBack);
        return McpQueryTools.WindowNoticeAsync(
            () => dataService.GetConfigSnapshotCoverageFloorAsync(relation, serverId, requestedStart, windowEnd),
            requestedStart, windowEnd, table, emptyAnswer: emptyAnswer);
    }

    [McpServerTool(Name = "get_server_config_changes"), Description("Gets server configuration change history by diffing sp_configure (sys.configurations) snapshots. Shows which settings changed, their old vs new configured/in-use values, whether the change requires a restart, and dynamic/advanced flags. NOTE: config is captured on server connect (not on a fixed schedule), so changes are detected between connect snapshots and need at least two.")]
    public static async Task<string> GetServerConfigChanges(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetServerConfigChangesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "server_config")
                    ?? McpHelpers.Status("empty",
                        $"No server configuration changes detected in the last {hours_back}h. Config is captured on connect, so at least two snapshots are needed to detect a change.",
                        (await WindowNoticeAsync(dataService, QueryWindowRelation.ServerConfig, resolved.ServerId, hours_back, windowEnd, "server_config", emptyAnswer: true)).AsHints());

            var notice = await WindowNoticeAsync(dataService, QueryWindowRelation.ServerConfig, resolved.ServerId, hours_back, windowEnd, "server_config");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #4966: where this server's config snapshots start for the window, always present (false and null when
                   the store covered it). No effective_hours_back, as on the other window-floor payloads. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                change_count = rows.Count,
                changes = rows.Select(r => new
                {
                    change_time = r.ChangeTime.ToString("o"),
                    configuration_name = r.ConfigurationName,
                    old_value_configured = r.OldValueConfigured,
                    new_value_configured = r.NewValueConfigured,
                    old_value_in_use = r.OldValueInUse,
                    new_value_in_use = r.NewValueInUse,
                    requires_restart = r.RequiresRestartDisplay,
                    is_dynamic = r.DynamicDisplay,
                    is_advanced = r.AdvancedDisplay,
                    change_description = r.ChangeDescription
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_config_changes", ex);
        }
    }

    [McpServerTool(Name = "get_database_config_changes"), Description("Gets database configuration change history by diffing sys.databases snapshots. Shows which database settings changed (recovery model, RCSI, compatibility level, etc.) with old and new values; setting_name is the underlying column name. NOTE: config is captured on server connect (not on a fixed schedule), so changes are detected between connect snapshots and need at least two.")]
    public static async Task<string> GetDatabaseConfigChanges(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetDatabaseConfigChangesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "database_config")
                    ?? McpHelpers.Status("empty",
                        $"No database configuration changes detected in the last {hours_back}h. Config is captured on connect, so at least two snapshots are needed to detect a change.",
                        (await WindowNoticeAsync(dataService, QueryWindowRelation.DatabaseConfig, resolved.ServerId, hours_back, windowEnd, "database_config", emptyAnswer: true)).AsHints());

            var notice = await WindowNoticeAsync(dataService, QueryWindowRelation.DatabaseConfig, resolved.ServerId, hours_back, windowEnd, "database_config");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #4966: where this server's config snapshots start for the window, always present (false and null when
                   the store covered it). No effective_hours_back, as on the other window-floor payloads. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                change_count = rows.Count,
                changes = rows.Select(r => new
                {
                    change_time = r.ChangeTime.ToString("o"),
                    database_name = r.DatabaseName,
                    setting_name = r.SettingName,
                    old_value = r.OldValue,
                    new_value = r.NewValue,
                    change_description = r.ChangeDescription
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_config_changes", ex);
        }
    }

    [McpServerTool(Name = "get_trace_flag_changes"), Description("Gets trace flag change history by diffing DBCC TRACESTATUS snapshots. Shows which trace flags were enabled or disabled, with scope (global/session) and the change time. NOTE: config is captured on server connect (not on a fixed schedule), so changes are detected between connect snapshots and need at least two.")]
    public static async Task<string> GetTraceFlagChanges(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetTraceFlagChangesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "trace_flags")
                    ?? McpHelpers.Status("empty",
                        $"No trace flag changes detected in the last {hours_back}h. Config is captured on connect, so at least two snapshots are needed to detect a change.",
                        (await WindowNoticeAsync(dataService, QueryWindowRelation.TraceFlags, resolved.ServerId, hours_back, windowEnd, "trace_flags", emptyAnswer: true)).AsHints());

            var notice = await WindowNoticeAsync(dataService, QueryWindowRelation.TraceFlags, resolved.ServerId, hours_back, windowEnd, "trace_flags");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #4966: where this server's config snapshots start for the window, always present (false and null when
                   the store covered it). No effective_hours_back, as on the other window-floor payloads. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                change_count = rows.Count,
                changes = rows.Select(r => new
                {
                    change_time = r.ChangeTime.ToString("o"),
                    trace_flag = r.TraceFlag,
                    previous_status = r.PreviousStatusDisplay,
                    new_status = r.NewStatusDisplay,
                    scope = r.Scope,
                    is_global = r.GlobalDisplay,
                    is_session = r.SessionDisplay,
                    change_description = r.ChangeDescription
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_trace_flag_changes", ex);
        }
    }
}
