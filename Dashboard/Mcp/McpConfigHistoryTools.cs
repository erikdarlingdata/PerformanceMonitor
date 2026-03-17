using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpConfigHistoryTools
{
    [McpServerTool(Name = "get_server_config_changes"), Description("Gets server configuration change history. Shows which sp_configure settings changed, old vs new values, and whether a restart is required. Use to detect recent configuration drift.")]
    public static async Task<string> GetServerConfigChanges(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetServerConfigChangesAsync(hours_back);
            if (rows.Count == 0)
                return "No server configuration changes found in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                change_count = rows.Count,
                changes = rows.Select(r => new
                {
                    change_time = r.ChangeTime.ToString("o"),
                    configuration_name = r.ConfigurationName,
                    old_value_configured = r.OldValueConfigured,
                    new_value_configured = r.NewValueConfigured,
                    old_value_in_use = r.OldValueInUse,
                    new_value_in_use = r.NewValueInUse,
                    requires_restart = r.RequiresRestart,
                    is_dynamic = r.IsDynamic,
                    description = r.Description
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_config_changes", ex);
        }
    }

    [McpServerTool(Name = "get_database_config_changes"), Description("Gets database configuration change history. Shows which database settings changed (recovery model, RCSI, compatibility level, etc.), with old and new values.")]
    public static async Task<string> GetDatabaseConfigChanges(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetDatabaseConfigChangesAsync(hours_back);
            if (rows.Count == 0)
                return "No database configuration changes found in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                change_count = rows.Count,
                changes = rows.Select(r => new
                {
                    change_time = r.ChangeTime.ToString("o"),
                    database_name = r.DatabaseName,
                    setting_type = r.SettingType,
                    setting_name = r.SettingName,
                    old_value = r.OldValue,
                    new_value = r.NewValue,
                    description = r.ChangeDescription
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_config_changes", ex);
        }
    }

    [McpServerTool(Name = "get_trace_flag_changes"), Description("Gets trace flag change history. Shows which trace flags were enabled or disabled, with scope (global/session) and timestamps.")]
    public static async Task<string> GetTraceFlagChanges(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetTraceFlagChangesAsync(hours_back);
            if (rows.Count == 0)
                return "No trace flag changes found in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                change_count = rows.Count,
                changes = rows.Select(r => new
                {
                    change_time = r.ChangeTime.ToString("o"),
                    trace_flag = r.TraceFlag,
                    previous_status = r.PreviousStatus,
                    new_status = r.NewStatus,
                    scope = r.Scope,
                    is_global = r.IsGlobal,
                    description = r.ChangeDescription
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_trace_flag_changes", ex);
        }
    }
}
