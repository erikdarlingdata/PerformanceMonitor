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
/// The config / trace-flag diagnostic-depth MCP tools — get_server_config_changes,
/// get_database_config_changes, get_trace_flag_changes (the Dashboard's change-history names) and the two
/// Lite latest-snapshot names, get_database_scoped_config and get_query_store_health — served over Darling's
/// Postgres store. The three change tools diff the store's append-only config snapshots (the Dashboard reads
/// pre-materialized <c>report.*_changes</c> tables that Darling does not have, so Darling computes the diff
/// from the raw snapshot history via <see cref="DarlingConfigHistoryReader"/>); the latest-snapshot tools
/// port Lite's over the viewer's reads. All are STORED reads (no live monitored-server hit).
///
/// <para>
/// Each change tool's Description states the two honest caveats plainly (they are NOT silently dropped):
/// (1) config is captured ON CONNECT only, so change granularity equals the connect/restart cadence and a
/// stable deployment may show no changes until the next connect; (2) the tools emit only the values Darling
/// collects — the Dashboard's <c>requires_restart</c> / <c>description</c> / <c>setting_type</c> /
/// generated <c>change_description</c> enrichment is not collected here and is omitted.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpConfigHistoryTools
{
    [McpServerTool(Name = "get_server_config_changes"), Description("Gets server configuration change history by diffing sp_configure (sys.configurations) snapshots. Shows which settings changed and their old vs new configured/in-use values. NOTE: this edition captures config on server connect (not on a fixed schedule), so changes are detected between connect snapshots and need at least two; requires_restart and the setting description are not collected and are omitted.")]
    public static async Task<string> GetServerConfigChanges(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var windowEndNaive = NaiveUtc(windowEnd);
            var windowStart = windowEndNaive.AddHours(-hours_back);
            var snapshots = await DarlingConfigHistoryReader.GetServerConfigSnapshotsAsync(postgres, resolved.ServerId);
            /* Unanchored, the tool still reads the full history and only lower-bounds — see UpperEdge, which
               keeps DateTime.MaxValue as the (no-op) upper edge so the shared both-edges diff reproduces the
               prior behaviour exactly. An as_of anchor is what closes the upper edge. */
            var changes = ConfigChangeDiff.DiffServerConfigChanges(snapshots, windowStart, UpperEdge(as_of, windowEndNaive));
            if (changes.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "server_config")
                    ?? NoChanges(resolved.ServerName, hours_back, DistinctCaptures(snapshots.Select(s => s.CaptureTime)));

            var result = changes.Select(c => new
            {
                change_time = c.ChangeTime.ToString("o"),
                configuration_name = c.ConfigurationName,
                old_value_configured = c.OldValueConfigured,
                new_value_configured = c.NewValueConfigured,
                old_value_in_use = c.OldValueInUse,
                new_value_in_use = c.NewValueInUse,
                is_dynamic = c.IsDynamic,
                is_advanced = c.IsAdvanced
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                change_count = changes.Count,
                changes = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_config_changes", ex);
        }
    }

    [McpServerTool(Name = "get_database_config_changes"), Description("Gets database configuration change history by diffing sys.databases snapshots. Shows which database settings changed (recovery model, RCSI, compatibility level, etc.) with old and new values; setting_name is the underlying column name. NOTE: this edition captures config on server connect (not on a fixed schedule), so changes are detected between connect snapshots and need at least two; the setting category/description narrative is not collected and is omitted.")]
    public static async Task<string> GetDatabaseConfigChanges(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var windowEndNaive = NaiveUtc(windowEnd);
            var windowStart = windowEndNaive.AddHours(-hours_back);
            var snapshots = await DarlingConfigHistoryReader.GetDatabaseConfigSnapshotsAsync(postgres, resolved.ServerId);
            var changes = ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, windowStart, UpperEdge(as_of, windowEndNaive));
            if (changes.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "database_config")
                    ?? NoChanges(resolved.ServerName, hours_back, DistinctCaptures(snapshots.Select(s => s.CaptureTime)));

            var result = changes.Select(c => new
            {
                change_time = c.ChangeTime.ToString("o"),
                database_name = c.DatabaseName,
                setting_name = c.SettingName,
                old_value = c.OldValue,
                new_value = c.NewValue
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                change_count = changes.Count,
                changes = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_database_config_changes", ex);
        }
    }

    [McpServerTool(Name = "get_trace_flag_changes"), Description("Gets trace flag change history by diffing DBCC TRACESTATUS snapshots. Shows which trace flags were enabled or disabled (change_type), with scope (global/session) and the change time. NOTE: this edition captures config on server connect (not on a fixed schedule), so changes are detected between connect snapshots and need at least two.")]
    public static async Task<string> GetTraceFlagChanges(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 168 (7 days).")] int hours_back = 168,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var windowEndNaive = NaiveUtc(windowEnd);
            var windowStart = windowEndNaive.AddHours(-hours_back);
            var snapshots = await DarlingConfigHistoryReader.GetTraceFlagSnapshotsAsync(postgres, resolved.ServerId);
            var changes = ConfigChangeDiff.DiffTraceFlagChanges(snapshots, windowStart, UpperEdge(as_of, windowEndNaive));
            if (changes.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "trace_flags")
                    ?? NoChanges(resolved.ServerName, hours_back, DistinctCaptures(snapshots.Select(s => s.CaptureTime)));

            var result = changes.Select(c => new
            {
                change_time = c.ChangeTime.ToString("o"),
                trace_flag = c.TraceFlag,
                change_type = c.ChangeType,
                previous_status = c.PreviousStatus,
                new_status = c.NewStatus,
                scope = Scope(c.IsGlobal, c.IsSession),
                is_global = c.IsGlobal,
                is_session = c.IsSession
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                change_count = changes.Count,
                changes = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_trace_flag_changes", ex);
        }
    }

    [McpServerTool(Name = "get_database_scoped_config"), Description("Gets database-scoped configuration settings (sys.database_scoped_configurations). Shows MAXDOP, legacy CE, parameter sniffing, and other per-database settings.")]
    public static async Task<string> GetDatabaseScopedConfig(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific database. Omit for all databases.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingConfigHistoryReader.GetLatestDatabaseScopedConfigAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "database_scoped_config")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No database-scoped configuration data available. The config collector may not have run yet.");

            IEnumerable<DarlingConfigHistoryReader.DatabaseScopedConfigReadRow> filtered = rows;
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
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific database. Omit for all databases.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingConfigHistoryReader.GetLatestQueryStoreHealthAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store_health")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No Query Store health data available. The query_store_health collector runs hourly (SQL Server 2016+); a server with no rows either predates Query Store or has not completed a cycle yet.");

            IEnumerable<DarlingConfigHistoryReader.QueryStoreHealthReadRow> filtered = rows;
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

    /// <summary>The number of distinct config CAPTURES (capture_time values) among the snapshot rows — the
    /// reads return one row per setting/database/flag per capture, so a raw row count would never hit the
    /// "fewer than two snapshots" branch on a freshly-connected server.</summary>
    private static int DistinctCaptures(IEnumerable<DateTime> captureTimes) =>
        captureTimes.Distinct().Count();

    /// <summary>The "empty" miss for a change tool — distinguishes "no snapshots collected yet" from "snapshots
    /// exist but nothing changed", so a caller understands why the history is empty (the on-connect cadence).</summary>
    private static string NoChanges(string serverName, int hoursBack, int snapshotCount) =>
        McpHelpers.Status(
            "empty",
            snapshotCount <= 1
                ? "No configuration change history yet: fewer than two config snapshots have been captured for this server. Config is captured when the service connects to the server, so changes appear once a second connect snapshot exists."
                : $"No configuration changes detected in the last {hoursBack}h across the captured snapshots.",
            new { server = serverName, snapshot_count = snapshotCount });

    /// <summary>Global / Session / Global+Session scope label from the two flags.</summary>
    private static string Scope(bool? isGlobal, bool? isSession) =>
        (isGlobal == true, isSession == true) switch
        {
            (true, true) => "Global+Session",
            (true, false) => "Global",
            (false, true) => "Session",
            _ => "",
        };

    private static DateTime NaiveUtc(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>
    /// The diff's upper edge. With no <c>as_of</c> the read stays open-ended (<see cref="DateTime.MaxValue"/>)
    /// — byte-for-byte the pre-#2495 behaviour, which is the whole compatibility contract of that change; an
    /// anchored read bounds at the anchor, because that is the point of sending one.
    ///
    /// <para>Lite's twin computes <c>now</c> for the same unanchored case rather than an open edge, and the
    /// two are NOT reconciled here on purpose. <c>capture_time</c> is stamped <c>DateTime.UtcNow</c> by the
    /// collector in the same process that later reads it, so a snapshot can never carry a timestamp after
    /// the read's own <c>now</c> — the two edges cannot select different rows, and unifying them would be an
    /// unrelated behaviour change to two shipped reads inside a change that promises not to make one.</para>
    /// </summary>
    private static DateTime UpperEdge(string? asOf, DateTime anchorNaiveUtc) =>
        string.IsNullOrWhiteSpace(asOf) ? DateTime.MaxValue : anchorNaiveUtc;
}
