/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;

namespace PerformanceMonitorLite.Services;

/*
 * The Configuration Changes tab reads (Tier-1 parity) — the Lite (DuckDB) port of the Darling viewer's
 * ViewerDataService.ConfigChanges reads. There is NO change table: server-config / database-config /
 * trace-flag DRIFT is COMPUTED by diffing the store's APPEND-ONLY config SNAPSHOT views
 * (v_server_config / v_database_config / v_trace_flags) in pure C#. The diff + the change/snapshot records
 * live ONCE in PerformanceMonitor.Common.ConfigChangeDiff (shared by the viewer and the headless MCP host) —
 * this reader only fetches the ordered snapshots from DuckDB, calls the shared diff, and wraps each change in
 * a Lite display row that adds ServerTimeHelper time rendering. Lite already surfaces the same snapshots on
 * its Configuration tab (LocalDataService.Config.cs), so the data exists; only this diff-derived view is new.
 *
 * WINDOWING (mirrors Darling): the snapshot read is upper-bounded at the window END (capture_time <= endUtc)
 * but NOT lower-bounded, so the snapshot immediately BEFORE the window is kept as the diff baseline and a
 * change landing on the first in-window snapshot is still detected; the shared diff then applies BOTH edges
 * to the computed change_time. capture_time is stamped DateTime.UtcNow by the collector (like collection_time),
 * so the read windows on the UTC bounds from GetTimeRange and each row's change_time renders through
 * ServerTimeHelper.FormatServerTime (naive-UTC -> display) exactly like the other UTC-stamped grids.
 *
 * SHAPE CAVEATS (surfaced honestly): the config collectors run ON CONNECT only, so change granularity equals
 * the connect/restart cadence and a fresh / stably-connected server may hold a single snapshot (then there is
 * no change yet — the diff needs >= 2 snapshots). requires_restart is DERIVED (is_dynamic = 0 AND
 * value_configured != value_in_use), matching the Dashboard view; the Dashboard's uncollected enrichment
 * (server-config description, database-config setting_type) is OMITTED, not fabricated.
 */

/// <summary>Shared render of a config change_time (naive-UTC, capture_time) for the Configuration Changes
/// grids — the same ServerTimeHelper.FormatServerTime the System Events / blocking grids use.</summary>
internal static class ConfigChangeRowFormat
{
    public static string Local(DateTime utc) => ServerTimeHelper.FormatServerTime(utc, "yyyy-MM-dd HH:mm:ss");
}

/// <summary>One server-configuration change (Server Config Changes sub-tab). Thin display wrapper over the
/// shared <see cref="ConfigChangeDiff.ServerConfigChange"/>; only <see cref="ChangeTimeLocal"/> is Lite-specific
/// (the derived requires_restart / dynamic / advanced / change-description columns come straight from the
/// shared record).</summary>
public sealed class ServerConfigChangeRow(ConfigChangeDiff.ServerConfigChange change)
{
    public string ChangeTimeLocal => ConfigChangeRowFormat.Local(change.ChangeTime);
    /// <summary>Raw naive-UTC change time (capture_time) for the MCP layer's ISO output.</summary>
    public DateTime ChangeTime => change.ChangeTime;
    public string ConfigurationName => change.ConfigurationName;
    public long? OldValueConfigured => change.OldValueConfigured;
    public long? NewValueConfigured => change.NewValueConfigured;
    public long? OldValueInUse => change.OldValueInUse;
    public long? NewValueInUse => change.NewValueInUse;
    public string RequiresRestartDisplay => change.RequiresRestartDisplay;
    public string DynamicDisplay => change.DynamicDisplay;
    public string AdvancedDisplay => change.AdvancedDisplay;
    public string ChangeDescription => change.ChangeDescription;
}

/// <summary>One database-configuration change (Database Config Changes sub-tab). Thin display wrapper over the
/// shared <see cref="ConfigChangeDiff.DatabaseConfigChange"/>; setting_name is the literal column name (the
/// WIDE store has no setting_type).</summary>
public sealed class DatabaseConfigChangeRow(ConfigChangeDiff.DatabaseConfigChange change)
{
    public string ChangeTimeLocal => ConfigChangeRowFormat.Local(change.ChangeTime);
    /// <summary>Raw naive-UTC change time (capture_time) for the MCP layer's ISO output.</summary>
    public DateTime ChangeTime => change.ChangeTime;
    public string DatabaseName => change.DatabaseName;
    public string SettingName => change.SettingName;
    public string? OldValue => change.OldValue;
    public string? NewValue => change.NewValue;
    public string ChangeDescription => change.ChangeDescription;
}

/// <summary>One trace-flag change (Trace Flag Changes sub-tab). Thin display wrapper over the shared
/// <see cref="ConfigChangeDiff.TraceFlagChange"/> (enabled / disabled / modified set-diff outcome).</summary>
public sealed class TraceFlagChangeRow(ConfigChangeDiff.TraceFlagChange change)
{
    public string ChangeTimeLocal => ConfigChangeRowFormat.Local(change.ChangeTime);
    /// <summary>Raw naive-UTC change time (capture_time) for the MCP layer's ISO output.</summary>
    public DateTime ChangeTime => change.ChangeTime;
    public int TraceFlag => change.TraceFlag;
    public string PreviousStatusDisplay => change.PreviousStatusDisplay;
    public string NewStatusDisplay => change.NewStatusDisplay;
    public string Scope => change.Scope;
    public string GlobalDisplay => change.GlobalDisplay;
    public string SessionDisplay => change.SessionDisplay;
    public string ChangeDescription => change.ChangeDescription;
}

public partial class LocalDataService
{
    /// <summary>Server-configuration changes over the tab window, newest first. Reads every sys.configurations
    /// snapshot up to the window end (keeping the pre-window baseline), then diffs per configuration_name via
    /// the shared <see cref="ConfigChangeDiff"/>.</summary>
    public async Task<List<ServerConfigChangeRow>> GetServerConfigChangesAsync(
        int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetServerConfigChangesAsync", "v_server_config snapshot diff");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var snapshots = await ReadServerConfigSnapshotsAsync(serverId, endTime);
        return ConfigChangeDiff.DiffServerConfigChanges(snapshots, startTime, endTime)
            .Select(c => new ServerConfigChangeRow(c))
            .ToList();
    }

    /// <summary>Database-configuration changes over the tab window, newest first. Reads every sys.databases
    /// snapshot up to the window end then UNPIVOTs the wide row per (database, setting) whose value moved via
    /// the shared <see cref="ConfigChangeDiff"/>. #1319: the global database filter is pushed into SQL.</summary>
    public async Task<List<DatabaseConfigChangeRow>> GetDatabaseConfigChangesAsync(
        int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetDatabaseConfigChangesAsync", "v_database_config snapshot diff");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var snapshots = await ReadDatabaseConfigSnapshotsAsync(serverId, endTime, databaseNames);
        return ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, startTime, endTime)
            .Select(c => new DatabaseConfigChangeRow(c))
            .ToList();
    }

    /// <summary>Trace-flag changes over the tab window, newest first. Reads every trace-flag snapshot up to the
    /// window end then set-diffs consecutive captures via the shared <see cref="ConfigChangeDiff"/> (flag
    /// appears = enabled, disappears = disabled, status/scope moves = modified).</summary>
    public async Task<List<TraceFlagChangeRow>> GetTraceFlagChangesAsync(
        int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetTraceFlagChangesAsync", "v_trace_flags snapshot diff");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var snapshots = await ReadTraceFlagSnapshotsAsync(serverId, endTime);
        return ConfigChangeDiff.DiffTraceFlagChanges(snapshots, startTime, endTime)
            .Select(c => new TraceFlagChangeRow(c))
            .ToList();
    }

    private async Task<List<ConfigChangeDiff.ServerConfigSnapshot>> ReadServerConfigSnapshotsAsync(int serverId, DateTime endUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT capture_time, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced
FROM v_server_config
WHERE server_id = $1
AND   capture_time <= $2
ORDER BY configuration_name, capture_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });

        var rows = new List<ConfigChangeDiff.ServerConfigSnapshot>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ConfigChangeDiff.ServerConfigSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? (long?)null : ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? (long?)null : ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? (bool?)null : reader.GetBoolean(4),
                reader.IsDBNull(5) ? (bool?)null : reader.GetBoolean(5)));
        }

        return rows;
    }

    private async Task<List<ConfigChangeDiff.DatabaseConfigSnapshot>> ReadDatabaseConfigSnapshotsAsync(
        int serverId, DateTime endUtc, IReadOnlyList<string>? databaseNames)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 3, out var dbValues);
        /* CAST every setting to VARCHAR so the wide row diffs uniformly by text (matching the Postgres ::text
           casts); the SELECT order is ConfigChangeDiff.DatabaseConfigChangeSettingNames, walked positionally. */
        command.CommandText = @"
SELECT capture_time, database_name,
       CAST(state_desc AS VARCHAR), CAST(compatibility_level AS VARCHAR), CAST(collation_name AS VARCHAR),
       CAST(recovery_model AS VARCHAR), CAST(is_read_only AS VARCHAR), CAST(is_auto_close_on AS VARCHAR),
       CAST(is_auto_shrink_on AS VARCHAR), CAST(is_auto_create_stats_on AS VARCHAR),
       CAST(is_auto_update_stats_on AS VARCHAR), CAST(is_auto_update_stats_async_on AS VARCHAR),
       CAST(is_read_committed_snapshot_on AS VARCHAR), CAST(snapshot_isolation_state AS VARCHAR),
       CAST(is_parameterization_forced AS VARCHAR), CAST(is_query_store_on AS VARCHAR),
       CAST(is_encrypted AS VARCHAR), CAST(is_trustworthy_on AS VARCHAR), CAST(is_db_chaining_on AS VARCHAR),
       CAST(is_broker_enabled AS VARCHAR), CAST(is_cdc_enabled AS VARCHAR),
       CAST(is_mixed_page_allocation_on AS VARCHAR), CAST(log_reuse_wait_desc AS VARCHAR),
       CAST(page_verify_option AS VARCHAR), CAST(target_recovery_time_seconds AS VARCHAR),
       CAST(delayed_durability AS VARCHAR), CAST(is_accelerated_database_recovery_on AS VARCHAR),
       CAST(is_memory_optimized_enabled AS VARCHAR), CAST(is_optimized_locking_on AS VARCHAR)
FROM v_database_config
WHERE server_id = $1
AND   capture_time <= $2" + dbClause + @"
ORDER BY database_name, capture_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var rows = new List<ConfigChangeDiff.DatabaseConfigSnapshot>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
            for (var i = 0; i < values.Length; i++)
            {
                var ordinal = i + 2; /* capture_time, database_name precede the settings */
                values[i] = reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
            }

            rows.Add(new ConfigChangeDiff.DatabaseConfigSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                values));
        }

        return rows;
    }

    private async Task<List<ConfigChangeDiff.TraceFlagSnapshot>> ReadTraceFlagSnapshotsAsync(int serverId, DateTime endUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT capture_time, trace_flag, status, is_global, is_session
FROM v_trace_flags
WHERE server_id = $1
AND   capture_time <= $2
ORDER BY capture_time, trace_flag";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });

        var rows = new List<ConfigChangeDiff.TraceFlagSnapshot>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ConfigChangeDiff.TraceFlagSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? (bool?)null : reader.GetBoolean(2),
                reader.IsDBNull(3) ? (bool?)null : reader.GetBoolean(3),
                reader.IsDBNull(4) ? (bool?)null : reader.GetBoolean(4)));
        }

        return rows;
    }
}
