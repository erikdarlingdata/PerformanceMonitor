using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpConfigTools
{
    [McpServerTool(Name = "get_server_config"), Description("Gets the current SQL Server instance configuration (sys.configurations). Shows all sp_configure settings with configured and in-use values. Useful for checking CTFP, MAXDOP, max memory, and other instance-level settings.")]
    public static async Task<string> GetServerConfig(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await dataService.GetLatestServerConfigAsync(resolved.Value.ServerId);
            if (rows.Count == 0)
                return "No server configuration data available. The config collector may not have run yet.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                setting_count = rows.Count,
                settings = rows.Select(r => new
                {
                    name = r.ConfigurationName,
                    value_configured = r.ValueConfigured,
                    value_in_use = r.ValueInUse,
                    values_match = r.ValuesMatch,
                    is_dynamic = r.IsDynamic,
                    is_advanced = r.IsAdvanced
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_config", ex);
        }
    }

    [McpServerTool(Name = "get_database_config"), Description("Gets database-level configuration for all databases (sys.databases). Shows recovery model, RCSI, auto-shrink, auto-close, Query Store, compatibility level, page verify, and other settings. Critical for identifying misconfigured databases.")]
    public static async Task<string> GetDatabaseConfig(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific database. Omit for all databases.")] string? database_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await dataService.GetLatestDatabaseConfigAsync(resolved.Value.ServerId);
            if (rows.Count == 0)
                return "No database configuration data available. The config collector may not have run yet.";

            IEnumerable<DatabaseConfigRow> filtered = rows;
            if (!string.IsNullOrEmpty(database_name))
                filtered = filtered.Where(r => r.DatabaseName.Equals(database_name, StringComparison.OrdinalIgnoreCase));

            var result = filtered.Select(r => new
            {
                database_name = r.DatabaseName,
                state = r.StateDesc,
                compatibility_level = r.CompatibilityLevel,
                recovery_model = r.RecoveryModel,
                rcsi = r.IsRcsiOn,
                snapshot_isolation = r.SnapshotIsolationState,
                auto_close = r.IsAutoCloseOn,
                auto_shrink = r.IsAutoShrinkOn,
                auto_create_stats = r.IsAutoCreateStatsOn,
                auto_update_stats = r.IsAutoUpdateStatsOn,
                auto_update_stats_async = r.IsAutoUpdateStatsAsyncOn,
                query_store = r.IsQueryStoreOn,
                page_verify = r.PageVerifyOption,
                parameterization_forced = r.IsParameterizationForced,
                delayed_durability = r.DelayedDurability,
                target_recovery_time_seconds = r.TargetRecoveryTimeSeconds,
                encrypted = r.IsEncrypted,
                accelerated_database_recovery = r.IsAcceleratedDatabaseRecoveryOn,
                optimized_locking = r.IsOptimizedLockingOn,
                log_reuse_wait = r.LogReuseWaitDesc
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                database_count = result.Count,
                databases = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_config", ex);
        }
    }

    [McpServerTool(Name = "get_database_scoped_config"), Description("Gets database-scoped configuration settings (sys.database_scoped_configurations). Shows MAXDOP, legacy CE, parameter sniffing, and other per-database settings.")]
    public static async Task<string> GetDatabaseScopedConfig(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific database. Omit for all databases.")] string? database_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await dataService.GetLatestDatabaseScopedConfigAsync(resolved.Value.ServerId);
            if (rows.Count == 0)
                return "No database-scoped configuration data available. The config collector may not have run yet.";

            IEnumerable<DatabaseScopedConfigRow> filtered = rows;
            if (!string.IsNullOrEmpty(database_name))
                filtered = filtered.Where(r => r.DatabaseName.Equals(database_name, StringComparison.OrdinalIgnoreCase));

            var grouped = filtered
                .GroupBy(r => r.DatabaseName)
                .Select(g => new
                {
                    database_name = g.Key,
                    settings = g.Select(r => new
                    {
                        name = r.ConfigurationName,
                        value = r.Value,
                        value_for_secondary = string.IsNullOrEmpty(r.ValueForSecondary) ? null : r.ValueForSecondary
                    })
                }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                database_count = grouped.Count,
                databases = grouped
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_scoped_config", ex);
        }
    }

    [McpServerTool(Name = "get_trace_flags"), Description("Gets active trace flags on the SQL Server instance. Shows flag number, enabled status, and whether the flag is global or session-scoped.")]
    public static async Task<string> GetTraceFlags(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await dataService.GetLatestTraceFlagsAsync(resolved.Value.ServerId);
            if (rows.Count == 0)
                return "No trace flags found (none enabled, or the config collector has not run yet).";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                trace_flag_count = rows.Count,
                trace_flags = rows.Select(r => new
                {
                    trace_flag = r.TraceFlag,
                    enabled = r.Status,
                    is_global = r.IsGlobal,
                    is_session = r.IsSession
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_trace_flags", ex);
        }
    }
}
