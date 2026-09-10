/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Configuration Changes tab's three change histories (Dashboard's <c>ConfigChangesContent</c> ported to
/// the viewer): server-config, database-config and trace-flag DRIFT over time. The Dashboard reads
/// pre-materialized <c>report.*_configuration_changes</c> / <c>report.trace_flag_changes</c> views (LAG-diff
/// over a persisted config history); Darling has no such views, so the change history is computed in C# by
/// diffing the store's APPEND-ONLY config SNAPSHOTS (<c>v_server_config</c> / <c>v_database_config</c> /
/// <c>v_trace_flags</c>, every setting written every capture, keyed by <c>capture_time</c>). The pure diff +
/// the change/snapshot records + the derived columns live ONCE in
/// <see cref="PerformanceMonitor.Common.ConfigChangeDiff"/> (shared by the headless MCP host and Lite — no
/// hand-mirrored twin); this class owns only the viewer-side reads and the display-row wrappers that add
/// Darling's timezone rendering.
///
/// <para>
/// WINDOWING — the read is upper-bounded at the window end (<c>capture_time &lt;= $2</c>) and BOTH window edges
/// are applied to the computed <c>change_time</c> in <see cref="ConfigChangeDiff"/> (NOT to the snapshot read).
/// This deliberately keeps the snapshot immediately BEFORE the window as the diff baseline, so a change landing
/// on the first in-window snapshot is still detected — mirroring the Dashboard, whose <c>report.*_changes</c>
/// views LAG over the full history and then filter <c>change_time &gt;= @from AND change_time &lt;= @to</c>.
/// Strictly lower-bounding the snapshot read would drop that baseline and under-report the left-edge change.
/// </para>
///
/// <para>
/// COLLECTION-CADENCE + SHAPE CAVEATS (surfaced honestly, never silently dropped): the config collectors run ON
/// CONNECT only (<c>FrequencyMinutes = 0</c>), so change granularity equals the service's connect/restart
/// cadence, and a fresh / stably-connected server may hold a single snapshot — then there is no change yet (the
/// diff needs &gt;= 2 snapshots). The change grids emit ONLY collected values; the Dashboard enrichment columns
/// Darling does not collect are OMITTED (not fabricated): server-config <c>description</c> (the setting's
/// human blurb) and database-config <c>setting_type</c> (the store is WIDE — <c>setting_name</c> is the literal
/// column name). <c>requires_restart</c> is NOT one of the gaps: it is derivable from collected columns
/// (<c>is_dynamic = false AND value_configured != value_in_use</c>, the Dashboard view's own definition), so it
/// is kept as a derived column. The per-row change narrative is DERIVED from the values (allowed), matching
/// the Dashboard view's wording.
/// </para>
/// </summary>
public sealed partial class ViewerDataService
{
    /* ─────────────────────────── snapshot-read SQL (upper-bound at window end) ─────────────────────────── */

    /// <summary>Every sys.configurations snapshot for a server up to the window end, oldest-first per name —
    /// the diff walks consecutive captures per configuration_name (every config row is present in every
    /// snapshot, so a per-name walk suffices). $1 server_id, $2 window end (naive UTC).</summary>
    public const string ServerConfigChangesSnapshotsSql = """
        SELECT
            capture_time,
            configuration_name,
            value_configured,
            value_in_use,
            is_dynamic,
            is_advanced
        FROM v_server_config
        WHERE server_id = $1
        AND   capture_time <= $2
        ORDER BY configuration_name, capture_time
        """;

    /// <summary>Every sys.databases snapshot for a server up to the window end, with the 27 setting columns
    /// CAST to text for a uniform value-diff. $1 server_id, $2 window end (naive UTC). The SELECT order is
    /// <see cref="ConfigChangeDiff.DatabaseConfigChangeSettingNames"/> (load-bearing — the diff walks the wide
    /// row positionally).</summary>
    public const string DatabaseConfigChangesSnapshotsSql = """
        SELECT
            capture_time,
            database_name,
            state_desc,
            compatibility_level::text,
            collation_name,
            recovery_model,
            is_read_only::text,
            is_auto_close_on::text,
            is_auto_shrink_on::text,
            is_auto_create_stats_on::text,
            is_auto_update_stats_on::text,
            is_auto_update_stats_async_on::text,
            is_read_committed_snapshot_on::text,
            snapshot_isolation_state,
            is_parameterization_forced::text,
            is_query_store_on::text,
            is_encrypted::text,
            is_trustworthy_on::text,
            is_db_chaining_on::text,
            is_broker_enabled::text,
            is_cdc_enabled::text,
            is_mixed_page_allocation_on::text,
            log_reuse_wait_desc,
            page_verify_option,
            target_recovery_time_seconds::text,
            delayed_durability,
            is_accelerated_database_recovery_on::text,
            is_memory_optimized_enabled::text,
            is_optimized_locking_on::text
        FROM v_database_config
        WHERE server_id = $1
        AND   capture_time <= $2
        AND   ($3::text[] IS NULL OR database_name = ANY($3))
        ORDER BY database_name, capture_time
        """;

    /// <summary>Every trace-flag snapshot for a server up to the window end (a row exists only while a flag is
    /// enabled), oldest-first — the diff set-diffs consecutive captures. $1 server_id, $2 window end (naive UTC).</summary>
    public const string TraceFlagChangesSnapshotsSql = """
        SELECT
            capture_time,
            trace_flag,
            status,
            is_global,
            is_session
        FROM v_trace_flags
        WHERE server_id = $1
        AND   capture_time <= $2
        ORDER BY capture_time, trace_flag
        """;

    /* ─────────────────────────── public read + diff (composes the read with the shared ConfigChangeDiff) ─────────────────────────── */

    /// <summary>Server-configuration changes in the window [startUtc, endUtc]. Reads every snapshot up to
    /// endUtc (so the pre-window baseline is kept) then diffs per configuration_name via the shared
    /// <see cref="ConfigChangeDiff"/>, wrapping each change in a display row that adds Darling's timezone render.</summary>
    public async Task<List<ServerConfigChangeRow>> GetServerConfigChangesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var snapshots = await ReadServerConfigSnapshotsAsync(serverId, endUtc, cancellationToken);
        return ConfigChangeDiff.DiffServerConfigChanges(snapshots, startUtc, endUtc)
            .Select(c => new ServerConfigChangeRow(c))
            .ToList();
    }

    /// <summary>Database-configuration changes in the window [startUtc, endUtc]. Reads every snapshot up to
    /// endUtc then UNPIVOTs the wide row per (database, setting) whose value moved via the shared
    /// <see cref="ConfigChangeDiff"/>.</summary>
    public async Task<List<DatabaseConfigChangeRow>> GetDatabaseConfigChangesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var snapshots = await ReadDatabaseConfigSnapshotsAsync(serverId, endUtc, databaseNames, cancellationToken);
        return ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, startUtc, endUtc)
            .Select(c => new DatabaseConfigChangeRow(c))
            .ToList();
    }

    /// <summary>Trace-flag changes in the window [startUtc, endUtc]. Reads every snapshot up to endUtc then
    /// set-diffs consecutive captures via the shared <see cref="ConfigChangeDiff"/> (flag appears = enabled,
    /// disappears = disabled, status/scope moves = modified).</summary>
    public async Task<List<TraceFlagChangeRow>> GetTraceFlagChangesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var snapshots = await ReadTraceFlagSnapshotsAsync(serverId, endUtc, cancellationToken);
        return ConfigChangeDiff.DiffTraceFlagChanges(snapshots, startUtc, endUtc)
            .Select(c => new TraceFlagChangeRow(c))
            .ToList();
    }

    private async Task<List<ConfigChangeDiff.ServerConfigSnapshot>> ReadServerConfigSnapshotsAsync(
        int serverId, DateTime endUtc, CancellationToken cancellationToken)
    {
        var rows = new List<ConfigChangeDiff.ServerConfigSnapshot>();
        await using var command = _dataSource.CreateCommand(ServerConfigChangesSnapshotsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerIdAndWindowEnd(command, serverId, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ConfigChangeDiff.ServerConfigSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetBoolean(4),
                reader.IsDBNull(5) ? null : reader.GetBoolean(5)));
        }

        return rows;
    }

    private async Task<List<ConfigChangeDiff.DatabaseConfigSnapshot>> ReadDatabaseConfigSnapshotsAsync(
        int serverId, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        var rows = new List<ConfigChangeDiff.DatabaseConfigSnapshot>();
        await using var command = _dataSource.CreateCommand(DatabaseConfigChangesSnapshotsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerIdAndWindowEnd(command, serverId, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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

    private async Task<List<ConfigChangeDiff.TraceFlagSnapshot>> ReadTraceFlagSnapshotsAsync(
        int serverId, DateTime endUtc, CancellationToken cancellationToken)
    {
        var rows = new List<ConfigChangeDiff.TraceFlagSnapshot>();
        await using var command = _dataSource.CreateCommand(TraceFlagChangesSnapshotsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerIdAndWindowEnd(command, serverId, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ConfigChangeDiff.TraceFlagSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetBoolean(4)));
        }

        return rows;
    }

    /// <summary>$1 server_id, $2 window end re-stamped naive-UTC (the store is naive-UTC, matching the other
    /// windowed viewer reads — see ViewerDataService.SystemEvents.cs).</summary>
    private static void AddServerIdAndWindowEnd(NpgsqlCommand command, int serverId, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
    }
}

/* ─────────────────────────── grid row view-models — thin DISPLAY WRAPPERS over the shared change records:
   they add ONLY the timezone-rendered ChangeTimeDisplay (Darling's ViewerTimeHelper, applied at bind time on
   the UI thread) and bind the shared pure-derived columns. No diff logic here — that lives in ConfigChangeDiff. ─────────────────────────── */

/// <summary>One server-configuration change (Server Config Changes grid). Wraps
/// <see cref="ConfigChangeDiff.ServerConfigChange"/>; only <see cref="ChangeTimeDisplay"/> is viewer-specific.</summary>
public sealed class ServerConfigChangeRow(ConfigChangeDiff.ServerConfigChange change)
{
    public DateTime ChangeTime => change.ChangeTime;
    public string ConfigurationName => change.ConfigurationName;
    public long? OldValueConfigured => change.OldValueConfigured;
    public long? NewValueConfigured => change.NewValueConfigured;
    public long? OldValueInUse => change.OldValueInUse;
    public long? NewValueInUse => change.NewValueInUse;
    public bool? IsDynamic => change.IsDynamic;
    public bool? IsAdvanced => change.IsAdvanced;

    public string ChangeTimeDisplay => ViewerTimeHelper.ForDisplay(change.ChangeTime).ToString("yyyy-MM-dd HH:mm:ss");
    public string DynamicDisplay => change.DynamicDisplay;
    public string AdvancedDisplay => change.AdvancedDisplay;
    public bool RequiresRestart => change.RequiresRestart;
    public string RequiresRestartDisplay => change.RequiresRestartDisplay;
    public string ChangeDescription => change.ChangeDescription;
}

/// <summary>One database-configuration change (Database Config Changes grid). Wraps
/// <see cref="ConfigChangeDiff.DatabaseConfigChange"/>; only <see cref="ChangeTimeDisplay"/> is viewer-specific.</summary>
public sealed class DatabaseConfigChangeRow(ConfigChangeDiff.DatabaseConfigChange change)
{
    public DateTime ChangeTime => change.ChangeTime;
    public string DatabaseName => change.DatabaseName;
    public string SettingName => change.SettingName;
    public string? OldValue => change.OldValue;
    public string? NewValue => change.NewValue;

    public string ChangeTimeDisplay => ViewerTimeHelper.ForDisplay(change.ChangeTime).ToString("yyyy-MM-dd HH:mm:ss");
    public string ChangeDescription => change.ChangeDescription;
}

/// <summary>One trace-flag change (Trace Flag Changes grid). Wraps
/// <see cref="ConfigChangeDiff.TraceFlagChange"/>; only <see cref="ChangeTimeDisplay"/> is viewer-specific.</summary>
public sealed class TraceFlagChangeRow(ConfigChangeDiff.TraceFlagChange change)
{
    public DateTime ChangeTime => change.ChangeTime;
    public int TraceFlag => change.TraceFlag;
    public bool? PreviousStatus => change.PreviousStatus;
    public bool? NewStatus => change.NewStatus;
    public bool? IsGlobal => change.IsGlobal;
    public bool? IsSession => change.IsSession;

    /// <summary>enabled / disabled / modified (the set-diff outcome).</summary>
    public string ChangeType => change.ChangeType;

    public string ChangeTimeDisplay => ViewerTimeHelper.ForDisplay(change.ChangeTime).ToString("yyyy-MM-dd HH:mm:ss");
    public string PreviousStatusDisplay => change.PreviousStatusDisplay;
    public string NewStatusDisplay => change.NewStatusDisplay;
    public string GlobalDisplay => change.GlobalDisplay;
    public string SessionDisplay => change.SessionDisplay;
    public string Scope => change.Scope;
    public string ChangeDescription => change.ChangeDescription;
}
