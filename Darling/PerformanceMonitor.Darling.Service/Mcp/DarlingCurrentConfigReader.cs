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

    /// <summary>Latest sys.configurations snapshot for one server — the viewer's <c>ServerConfigSql</c> plus
    /// the trailing <c>capture_time</c> (#3541 A10): config is captured ON CONNECT, so the "current" value this
    /// read serves can be as old as the last successful connect, and the tool must be able to say so.
    /// $1 server_id.</summary>
    public const string ServerConfigSql = """
        SELECT configuration_name, value_configured, value_in_use, is_dynamic, is_advanced, capture_time
        FROM v_server_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        ORDER BY configuration_name
        """;

    public static async Task<LatestSnapshot<ServerConfigReadRow>> GetLatestServerConfigAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<ServerConfigReadRow>();
        DateTime? capturedAt = null;
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
            capturedAt ??= reader.GetDateTime(5);
        }

        return new LatestSnapshot<ServerConfigReadRow>(capturedAt, rows);
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
       ordinal, so this list's order is load-bearing and must stay byte-identical. capture_time is APPENDED
       as a 29th column (#3541 A10) and read by explicit ordinal 28, so the 28-column mapping is untouched. */
    public const string DatabaseConfigSql = """
        SELECT database_name, state_desc, compatibility_level, collation_name, recovery_model,
               is_read_only, is_auto_close_on, is_auto_shrink_on,
               is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
               is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced,
               is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
               is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on,
               log_reuse_wait_desc, page_verify_option, target_recovery_time_seconds, delayed_durability,
               is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on,
               capture_time
        FROM v_database_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)
        ORDER BY database_name
        """;

    /// <summary>The ordinal of the appended <c>capture_time</c> column in <see cref="DatabaseConfigSql"/> — one
    /// past the 28-column block the incrementing mapping consumes.</summary>
    private const int DatabaseConfigCaptureTimeOrdinal = 28;

    public static async Task<LatestSnapshot<DatabaseConfigReadRow>> GetLatestDatabaseConfigAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<DatabaseConfigReadRow>();
        DateTime? capturedAt = null;
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
            capturedAt ??= reader.GetDateTime(DatabaseConfigCaptureTimeOrdinal);
        }

        return new LatestSnapshot<DatabaseConfigReadRow>(capturedAt, rows);
    }

    /* ─────────────────────────── trace flags (DBCC TRACESTATUS) ─────────────────────────── */

    public sealed record TraceFlagReadRow(int TraceFlag, bool Status, bool IsGlobal, bool IsSession);

    /// <summary>Latest trace-flags snapshot for one server — the viewer's <c>TraceFlagsSql</c> plus the trailing
    /// <c>capture_time</c> (#3541 A10). $1 server_id. A row exists only while a flag is enabled, so an empty
    /// result means no active flags at the last capture — and, having no row, no stamp either.
    ///
    /// <para><b>#3999: anchored on the collector's newest SUCCESSFUL run, not the newest row.</b> A capture
    /// that finds every flag off writes ZERO rows (<c>DBCC TRACESTATUS(-1)</c> only ever lists flags that are
    /// ON), so plain <c>capture_time = MAX(capture_time)</c> cannot see that capture at all and silently falls
    /// back to an older capture that still had a flag on — reporting a flag enabled days or weeks after it (and
    /// every other flag) was turned off. The second <c>AND</c> below closes that: it compares the newest row's
    /// own timestamp against the newest SUCCESS this collector logged in <c>v_collection_log</c>, and if that
    /// successful run is NEWER than the newest row, the run that ran most recently found nothing on, so the
    /// whole predicate goes false and the read reports no flags — exact, rather than a stale fallback. The
    /// <c>COALESCE</c> floor only matters when collection_log's retention has aged past this collector's
    /// oldest trace_flags row (or nothing has run yet), and defaults to the pre-fix reading rather than
    /// wrongly suppressing a real row it cannot corroborate.</para>
    /// </summary>
    public const string TraceFlagsSql = """
        SELECT trace_flag, status, is_global, is_session, capture_time
        FROM v_trace_flags
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1)
        AND   (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1) >= COALESCE(
                  (SELECT MAX(collection_time) FROM v_collection_log
                   WHERE server_id = $1 AND collector_name = 'trace_flags' AND status = 'SUCCESS'),
                  TIMESTAMP '1900-01-01')
        ORDER BY trace_flag
        """;

    public static async Task<LatestSnapshot<TraceFlagReadRow>> GetLatestTraceFlagsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<TraceFlagReadRow>();
        DateTime? capturedAt = null;
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
            capturedAt ??= reader.GetDateTime(4);
        }

        return new LatestSnapshot<TraceFlagReadRow>(capturedAt, rows);
    }
}
