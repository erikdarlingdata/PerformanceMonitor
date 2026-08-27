/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The current-config snapshot MCP tools — get_server_config, get_database_config, get_trace_flags — served
/// over Darling's Postgres store, the same names Lite exposes. Where the sibling
/// <see cref="DarlingMcpConfigHistoryTools"/> diffs config snapshots into a CHANGE history (empty on a stable
/// server), these answer "what is it set to RIGHT NOW" — the most recent capture per server, via the
/// <see cref="DarlingCurrentConfigReader"/> latest-snapshot reads (<c>MAX(capture_time)</c> over the config
/// passthrough views). All STORED reads (no live monitored-server hit); each tool body mirrors Lite's
/// <c>McpConfigTools</c> field-for-field so an MCP client sees one consistent product across the SKUs.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpConfigTools
{
    [McpServerTool(Name = "get_server_config"), Description("Gets the current SQL Server instance configuration (sys.configurations). Shows all sp_configure settings with configured and in-use values. Useful for checking CTFP, MAXDOP, max memory, and other instance-level settings right now (unlike get_server_config_changes, which shows only what changed between connect snapshots).")]
    public static async Task<string> GetServerConfig(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingCurrentConfigReader.GetLatestServerConfigAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "server_config")
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
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific database. Omit for all databases.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingCurrentConfigReader.GetLatestDatabaseConfigAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "database_config")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No database configuration data available. The config collector may not have run yet.");

            IEnumerable<DarlingCurrentConfigReader.DatabaseConfigReadRow> filtered = rows;
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

    [McpServerTool(Name = "get_trace_flags"), Description("Gets active trace flags on the SQL Server instance. Shows flag number, enabled status, and whether the flag is global or session-scoped.")]
    public static async Task<string> GetTraceFlags(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingCurrentConfigReader.GetLatestTraceFlagsAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "trace_flags")
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
