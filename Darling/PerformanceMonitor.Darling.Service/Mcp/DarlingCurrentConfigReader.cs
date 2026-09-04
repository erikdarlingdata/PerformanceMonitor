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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side latest-snapshot reads for the current-config MCP tools (<see cref="DarlingMcpConfigTools"/>) —
/// get_server_config / get_database_config / get_trace_flags. Where the sibling
/// <see cref="DarlingConfigHistoryReader"/> DIFFS the append-only config snapshots into a change history,
/// these return the MOST RECENT capture for one server via <c>capture_time = (SELECT MAX(capture_time) ...)</c>
/// against the config passthrough views — the "what is it set to right now" read the <c>*_changes</c> diff
/// tools cannot answer on a stable server (an unchanged deployment yields no diffs). The SQL is reproduced
/// verbatim from the viewer's Configuration-tab reads (<c>ViewerDataService.Config.cs</c>), which are Lite's
/// <c>LocalDataService.Config.cs</c> ported to Postgres; the database-config SELECT's 28-column order is
/// load-bearing (the reader maps it by incrementing ordinal, exactly like the viewer/Lite). All STORED reads
/// (no live monitored-server hit); public-const SQL so Darling.Tests pin the dialect + columns without a
/// live Postgres.
/// </summary>
internal static class DarlingCurrentConfigReader
{
    /* ─────────────────────────── server config (sys.configurations) ─────────────────────────── */

    public sealed record ServerConfigReadRow(
        string ConfigurationName, long ValueConfigured, long ValueInUse, bool IsDynamic, bool IsAdvanced)
    {
        public bool ValuesMatch => ValueConfigured == ValueInUse;
    }

    /// <summary>Latest sys.configurations snapshot for one server — the viewer's <c>ServerConfigSql</c>.
    /// $1 server_id.</summary>
    public const string ServerConfigSql = """
        SELECT configuration_name, value_configured, value_in_use, is_dynamic, is_advanced
        FROM v_server_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        ORDER BY configuration_name
        """;

    public static async Task<List<ServerConfigReadRow>> GetLatestServerConfigAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<ServerConfigReadRow>();
        await using var command = postgres.CreateCommand(ServerConfigSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ServerConfigReadRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                !reader.IsDBNull(4) && reader.GetBoolean(4)));
        }

        return rows;
    }

    /* ─────────────────────────── database config (sys.databases) ─────────────────────────── */

    public sealed record DatabaseConfigReadRow(
        string DatabaseName, string StateDesc, int CompatibilityLevel, string CollationName, string RecoveryModel,
        bool IsReadOnly, bool IsAutoCloseOn, bool IsAutoShrinkOn, bool IsAutoCreateStatsOn, bool IsAutoUpdateStatsOn,
        bool IsAutoUpdateStatsAsyncOn, bool IsRcsiOn, string SnapshotIsolationState, bool IsParameterizationForced,
        bool IsQueryStoreOn, bool IsEncrypted, bool IsTrustworthyOn, bool IsDbChainingOn, bool IsBrokerEnabled,
        bool IsCdcEnabled, bool IsMixedPageAllocationOn, string LogReuseWaitDesc, string PageVerifyOption,
        int TargetRecoveryTimeSeconds, string DelayedDurability, bool IsAcceleratedDatabaseRecoveryOn,
        bool IsMemoryOptimizedEnabled, bool IsOptimizedLockingOn);

    /* 28 columns in the viewer's / Lite's exact SELECT order — the reader below maps them by incrementing
       ordinal, so this list's order is load-bearing and must stay byte-identical. */
    public const string DatabaseConfigSql = """
        SELECT database_name, state_desc, compatibility_level, collation_name, recovery_model,
               is_read_only, is_auto_close_on, is_auto_shrink_on,
               is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
               is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced,
               is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
               is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on,
               log_reuse_wait_desc, page_verify_option, target_recovery_time_seconds, delayed_durability,
               is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on
        FROM v_database_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)
        ORDER BY database_name
        """;

    public static async Task<List<DatabaseConfigReadRow>> GetLatestDatabaseConfigAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<DatabaseConfigReadRow>();
        await using var command = postgres.CreateCommand(DatabaseConfigSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* Incrementing-ordinal mapping copied from the viewer/Lite so the 28-column order in
               DatabaseConfigSql binds to the same fields — see the byte-identical note above. */
            var ordinal = 0;
            rows.Add(new DatabaseConfigReadRow(
                reader.IsDBNull(ordinal) ? "" : reader.GetString(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                reader.IsDBNull(++ordinal) ? 0 : reader.GetInt32(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                reader.IsDBNull(++ordinal) ? 0 : reader.GetInt32(ordinal),
                reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal)));
        }

        return rows;
    }

    /* ─────────────────────────── trace flags (DBCC TRACESTATUS) ─────────────────────────── */

    public sealed record TraceFlagReadRow(int TraceFlag, bool Status, bool IsGlobal, bool IsSession);

    /// <summary>Latest trace-flags snapshot for one server — the viewer's <c>TraceFlagsSql</c>. $1 server_id.
    /// A row exists only while a flag is enabled, so an empty result means no active flags at the last capture.</summary>
    public const string TraceFlagsSql = """
        SELECT trace_flag, status, is_global, is_session
        FROM v_trace_flags
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1)
        ORDER BY trace_flag
        """;

    public static async Task<List<TraceFlagReadRow>> GetLatestTraceFlagsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<TraceFlagReadRow>();
        await using var command = postgres.CreateCommand(TraceFlagsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new TraceFlagReadRow(
                reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                !reader.IsDBNull(1) && reader.GetBoolean(1),
                !reader.IsDBNull(2) && reader.GetBoolean(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3)));
        }

        return rows;
    }
}
