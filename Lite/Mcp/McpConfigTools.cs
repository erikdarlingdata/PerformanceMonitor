using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpConfigTools
{
    [McpServerTool(Name = "get_server_config"), Description("Gets the current SQL Server instance configuration (sys.configurations). Shows all sp_configure settings with configured and in-use values. Useful for checking CTFP, MAXDOP, max memory, and other instance-level settings right now (unlike get_server_config_changes, which shows only what changed between connect snapshots). LATEST IS A TIME: configuration is captured when the collector CONNECTS, not on a schedule, so 'current' here means 'as of the last capture' - captured_at is that instant, and a value can be days old on a server the monitor has stayed connected to.")]
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
                /* #3541 A10: the connect-time capture this "current" configuration is as of. */
                captured_at = rows[0].CaptureTime.ToString("o"),
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

    [McpServerTool(Name = "get_database_config"), Description("Gets database-level configuration for all databases (sys.databases). Shows recovery model, RCSI, auto-shrink, auto-close, Query Store, compatibility level, page verify, and other settings. Critical for identifying misconfigured databases. LATEST IS A TIME: captured when the collector connects, not on a schedule - captured_at is the instant these settings are as of, and a database created or altered since is not reflected until the next connect.")]
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

            /* Taken from the unfiltered snapshot, so a database_name that matches nothing still says when. */
            var capturedAt = rows[0].CaptureTime;

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
                captured_at = capturedAt.ToString("o"),
                database_count = result.Count,
                databases = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_config", ex);
        }
    }

    [McpServerTool(Name = "get_database_scoped_config"), Description("Gets database-scoped configuration settings (sys.database_scoped_configurations). Shows MAXDOP, legacy CE, parameter sniffing, and other per-database settings. LATEST IS A TIME: captured when the collector connects, not on a schedule - captured_at is the instant these settings are as of.")]
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

            var capturedAt = rows[0].CaptureTime;

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
                captured_at = capturedAt.ToString("o"),
                database_count = grouped.Count,
                databases = grouped
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_scoped_config", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_health"), Description("Per-database Query Store health: actual/desired state (READ_WRITE->READ_ONLY after the storage cap = the classic failure). query_capture_mode (churn knob): ALL (2016/17 default) churns most; AUTO (2019+ default) skips minor ones; CUSTOM tunes AUTO; NONE stops new capture. wait_stats_capture_mode: ON default, OFF empties per-query waits. null on either: predates the rung, or pre-2017 engine for wait_stats - never OFF. No verdict rendered. No server rows = unavailable; an unmatched database_name answers database_count 0. LATEST IS A TIME: captured_at is the newest hourly capture. <<GUIDE>> Gets per-database Query Store health (sys.database_query_store_options): actual vs desired state, readonly_reason (decoded), storage used vs cap, cleanup mode and thresholds, the runtime-stats interval length, and — the two trailing fields on every row since V137 / Lite v64 — query_capture_mode and wait_stats_capture_mode. The classic silent failure is desired READ_WRITE with actual READ_ONLY after the storage cap hit — check this when Query Store data looks stale or missing. CAPTURE MODE IS THE PLAN-CHURN KNOB: query_capture_mode is the one option on this row that names a plan-churn factory. ALL captures every query the engine compiles, one-off ad hoc statements included — on an ad hoc workload each distinct text is a new query with a new plan, so the store fills toward max_storage_size_mb, size-based cleanup cycles, and the READ_ONLY cap hit that readonly_reason decodes follows; ALL was the engine default on SQL Server 2016 and 2017. AUTO skips insignificant queries (the engine's own thresholds over a day: fewer than 30 executions, under 1 s of compile CPU and under 100 ms of execution CPU) and has been the default since SQL Server 2019 and on Azure SQL Database. CUSTOM (2019+) is AUTO with operator-set thresholds — the capture_policy_* knobs, which this row does not collect, so CUSTOM here says the thresholds were tuned, not to what. NONE stops capturing NEW queries while the store keeps collecting compile and runtime statistics for the ones it already holds. wait_stats_capture_mode ON (the default) records per-plan wait statistics into every runtime-stats interval, at a per-execution bookkeeping cost and more store bytes per interval; OFF saves both and leaves the store's per-query wait view empty. Both are the DMV's *_desc spelling verbatim. null means the row predates the V137 rung or, for wait_stats_capture_mode, the engine is older than SQL Server 2017 (the column does not exist there) — never OFF. Consumed by the Viewer's Query Store grid and, next, by get_query_store_clutter as its churn × ALL 'switch to AUTO' arm; this tool reports the modes and renders no verdict on them. Collected hourly; OFF is recorded as OFF (an absent database means not collected, never off). LATEST IS A TIME: this is the newest hourly capture, and captured_at is its instant.")]
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

            var capturedAt = rows[0].CaptureTime;

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
                /* v64 (#3796): the two capture modes, TRAILING and in the collector's order, so a client that
                   indexed the row by position before the rung still finds its ten fields where they were. The
                   DMV's *_desc spelling verbatim, and null is published as null rather than coalesced: it is
                   the pre-rung row or the 2016 engine (wait stats), a real state the description spells out,
                   and the same key shape both SKUs emit. No verdict on the value — that is #3797's. */
                query_capture_mode = r.QueryCaptureMode,
                wait_stats_capture_mode = r.WaitStatsCaptureMode,
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = capturedAt.ToString("o"),
                database_count = result.Count,
                databases = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_health", ex);
        }
    }

    [McpServerTool(Name = "get_trace_flags"), Description("Gets active trace flags on the SQL Server instance. Shows flag number, enabled status, and whether the flag is global or session-scoped. LATEST IS A TIME: captured when the collector connects, not on a schedule - captured_at is the instant these flags are as of; a flag turned on or off since is not reflected until the next connect.")]
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
                captured_at = rows[0].CaptureTime.ToString("o"),
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
