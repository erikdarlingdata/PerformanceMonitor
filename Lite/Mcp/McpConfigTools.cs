using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestServerConfigAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "server_config")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No server configuration data available. The config collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestDatabaseConfigAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "database_config")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No database configuration data available. The config collector may not have run yet.");

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
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestDatabaseScopedConfigAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "database_scoped_config")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No database-scoped configuration data available. The config collector may not have run yet.");

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
                server = resolved.ServerName,
                database_count = grouped.Count,
                databases = grouped
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_scoped_config", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_health"), Description("Gets per-database Query Store health (sys.database_query_store_options): actual vs desired state, readonly_reason (decoded), storage used vs cap, cleanup mode and thresholds, and the runtime-stats interval length. The classic silent failure is desired READ_WRITE with actual READ_ONLY after the storage cap hit — check this when Query Store data looks stale or missing. Collected hourly; OFF is recorded as OFF (an absent database means not collected, never off).")]
    public static async Task<string> GetQueryStoreHealth(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific database. Omit for all databases.")] string? database_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestQueryStoreHealthAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_store_health")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No Query Store health data available. The query_store_health collector runs hourly (SQL Server 2016+); a server with no rows either predates Query Store or has not completed a cycle yet.");

            IEnumerable<QueryStoreHealthRow> filtered = rows;
            if (!string.IsNullOrEmpty(database_name))
                filtered = filtered.Where(r => r.DatabaseName.Equals(database_name, StringComparison.OrdinalIgnoreCase));

            var result = filtered.Select(r => new
            {
                database_name = r.DatabaseName,
                actual_state = r.ActualState,
                desired_state = r.DesiredState,
                /* The condition this collector exists to surface, pre-folded so a client cannot miss it. */
                state_matches_desired = string.Equals(r.ActualState, r.DesiredState, StringComparison.OrdinalIgnoreCase),
                readonly_reason = r.ReadonlyReason,
                readonly_reason_decoded = r.ReadonlyReason == 0 ? null : QueryStoreReadonlyReason.Decode(r.ReadonlyReason),
                current_storage_size_mb = r.CurrentStorageMb,
                max_storage_size_mb = r.MaxStorageMb,
                pct_of_cap = r.MaxStorageMb > 0 ? Math.Round(100.0 * r.CurrentStorageMb / r.MaxStorageMb, 1) : (double?)null,
                size_based_cleanup_mode = string.IsNullOrEmpty(r.SizeBasedCleanupMode) ? null : r.SizeBasedCleanupMode,
                stale_query_threshold_days = r.StaleQueryThresholdDays,
                max_plans_per_query = r.MaxPlansPerQuery,
                interval_length_minutes = r.IntervalLengthMinutes,
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                database_count = result.Count,
                databases = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_health", ex);
        }
    }

    [McpServerTool(Name = "get_trace_flags"), Description("Gets active trace flags on the SQL Server instance. Shows flag number, enabled status, and whether the flag is global or session-scoped.")]
    public static async Task<string> GetTraceFlags(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestTraceFlagsAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "trace_flags")
                    ?? McpHelpers.Status("empty", "No trace flags found (none enabled, or the config collector has not run yet).");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
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
