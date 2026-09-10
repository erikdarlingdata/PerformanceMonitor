/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the config / trace-flag MCP tools (<see cref="DarlingMcpConfigHistoryTools"/>).
/// The store keeps config as APPEND-ONLY snapshots keyed by <c>capture_time</c> (the collector writes every
/// setting every run, no dedup), so a change history genuinely exists — the three <c>*_changes</c> tools
/// fetch the ordered snapshots here and diff them via the shared
/// <see cref="PerformanceMonitor.Common.ConfigChangeDiff"/> (one source, also used by the viewer and Lite — no
/// hand-mirrored twin). The diff runs in C# (not SQL) so it is unit-testable without a live Postgres and so
/// trace-flag enable/disable (a flag row appearing/disappearing between snapshots) is handled correctly; the
/// reads just fetch the ordered snapshots. get_database_scoped_config reads the latest snapshot directly (the
/// viewer's <c>DatabaseScopedConfigSql</c>). All are STORED reads (no live hit).
///
/// <para>
/// COLLECTION-CADENCE + SHAPE CAVEATS (surfaced in the tool descriptions, the MCP instructions, and the
/// READMEs, never silently dropped): the config collectors run ON CONNECT only (<c>FrequencyMinutes = 0</c>),
/// so change granularity equals the service's connect/restart cadence, and a stable always-connected
/// deployment may hold a single snapshot (→ no changes yet). The change tools emit ONLY collected values —
/// the Dashboard's enrichment columns that Darling does not collect are omitted: server-config
/// <c>requires_restart</c> / <c>description</c>, database-config <c>setting_type</c> (the store is WIDE, so
/// <c>setting_name</c> is the literal column name), and the generated <c>change_description</c> narrative.
/// </para>
/// </summary>
internal static class DarlingConfigHistoryReader
{
    /* ─────────────────────────── database scoped config snapshot row (not part of the change diff) ─────────────────────────── */

    public sealed record DatabaseScopedConfigReadRow(
        string DatabaseName, string ConfigurationName, string? Value, string? ValueForSecondary);

    /* ─────────────────────────── query store health snapshot row (not part of the change diff) ─────────────────────────── */

    /* String fields coalesce DBNull to "" — the same defaults as both viewers' QueryStoreHealthRow —
       so the two SKUs' MCP tools serialize identical JSON even in the never-observed null case. */
    public sealed record QueryStoreHealthReadRow(
        string DatabaseName, string ActualState, string DesiredState, int ReadonlyReason,
        long CurrentStorageMb, long MaxStorageMb, string SizeBasedCleanupMode,
        long StaleQueryThresholdDays, long MaxPlansPerQuery, long IntervalLengthMinutes);

    /* ─────────────────────────── server config snapshots ─────────────────────────── */

    /// <summary>Every sys.configurations snapshot for a server, oldest-first — the change tool diffs
    /// consecutive captures per configuration_name (rows are always all-present, so a per-name walk suffices).
    /// $1 server_id.</summary>
    public const string ServerConfigSnapshotsSql = """
        SELECT
            capture_time,
            configuration_name,
            value_configured,
            value_in_use,
            is_dynamic,
            is_advanced
        FROM v_server_config
        WHERE server_id = $1
        ORDER BY configuration_name, capture_time
        """;

    public static async Task<List<ConfigChangeDiff.ServerConfigSnapshot>> GetServerConfigSnapshotsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<ConfigChangeDiff.ServerConfigSnapshot>();
        await using var command = postgres.CreateCommand(ServerConfigSnapshotsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
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

    /* ─────────────────────────── database config snapshots (WIDE) ─────────────────────────── */

    /// <summary>Every sys.databases snapshot for a server, with the 27 setting columns CAST to text for a
    /// uniform value-diff. $1 server_id. The SELECT order is
    /// <see cref="ConfigChangeDiff.DatabaseConfigChangeSettingNames"/>.</summary>
    public const string DatabaseConfigSnapshotsSql = """
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
        ORDER BY database_name, capture_time
        """;

    public static async Task<List<ConfigChangeDiff.DatabaseConfigSnapshot>> GetDatabaseConfigSnapshotsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<ConfigChangeDiff.DatabaseConfigSnapshot>();
        await using var command = postgres.CreateCommand(DatabaseConfigSnapshotsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
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

    /* ─────────────────────────── trace flag snapshots ─────────────────────────── */

    /// <summary>Every trace-flag snapshot for a server (a row exists only while a flag is enabled), oldest
    /// first — the change tool set-diffs consecutive captures so a flag appearing = enabled, disappearing =
    /// disabled. $1 server_id.</summary>
    public const string TraceFlagSnapshotsSql = """
        SELECT
            capture_time,
            trace_flag,
            status,
            is_global,
            is_session
        FROM v_trace_flags
        WHERE server_id = $1
        ORDER BY capture_time, trace_flag
        """;

    public static async Task<List<ConfigChangeDiff.TraceFlagSnapshot>> GetTraceFlagSnapshotsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<ConfigChangeDiff.TraceFlagSnapshot>();
        await using var command = postgres.CreateCommand(TraceFlagSnapshotsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
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

    /* ─────────────────────────── database scoped config (latest snapshot) ─────────────────────────── */

    /// <summary>The latest sys.database_scoped_configurations snapshot — the viewer's
    /// <c>DatabaseScopedConfigSql</c>. $1 server_id.</summary>
    public const string DatabaseScopedConfigSql = """
        SELECT database_name, configuration_name, value, value_for_secondary
        FROM v_database_scoped_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_database_scoped_config WHERE server_id = $1)
        ORDER BY database_name, configuration_name
        """;

    public static async Task<List<DatabaseScopedConfigReadRow>> GetLatestDatabaseScopedConfigAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<DatabaseScopedConfigReadRow>();
        await using var command = postgres.CreateCommand(DatabaseScopedConfigSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DatabaseScopedConfigReadRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return rows;
    }

    /* ─────────────────────────── query store health (latest snapshot) ─────────────────────────── */

    /// <summary>The latest sys.database_query_store_options snapshot per database — the viewer's
    /// <c>QueryStoreHealthSql</c> minus its grid database filter (the tool filters in memory, like the
    /// scoped-config read above). Unlike the config-family reads this table is HOURLY, not on-connect,
    /// so "latest" here is at most an hour old on a healthy schedule. $1 server_id.</summary>
    public const string QueryStoreHealthSql = """
        SELECT database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes
        FROM v_query_store_health
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_query_store_health WHERE server_id = $1)
        ORDER BY database_name
        """;

    public static async Task<List<QueryStoreHealthReadRow>> GetLatestQueryStoreHealthAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<QueryStoreHealthReadRow>();
        await using var command = postgres.CreateCommand(QueryStoreHealthSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new QueryStoreHealthReadRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? 0L : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0L : reader.GetInt64(5),
                reader.IsDBNull(6) ? "" : reader.GetString(6),
                reader.IsDBNull(7) ? 0L : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0L : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0L : reader.GetInt64(9)));
        }

        return rows;
    }
}
