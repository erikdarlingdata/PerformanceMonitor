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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Configuration tab's four latest-snapshot reads (W1g viewer copy-parity) — Lite's
/// <c>LocalDataService.Config.cs</c> ported byte-for-byte to Postgres. Each read pulls the most
/// recent capture for one server via <c>capture_time = (SELECT MAX(capture_time) ...)</c> against
/// the config passthrough views (<c>v_server_config</c> / <c>v_database_config</c> /
/// <c>v_database_scoped_config</c> / <c>v_trace_flags</c>, all V4/V5 twins of Lite's DuckDB views).
/// The SELECT column lists — and the database-config reader's incrementing-ordinal mapping — stay
/// identical to Lite so the 28-column database-config grid binds the same properties.
/// Only <c>server_id</c> ($1) is parameterized; there is no timestamp parameter (the snapshot is
/// chosen in SQL), so no naive-UTC handling is needed here. The row classes port as-is from Lite
/// (the display properties are pure C#), so the copied XAML column bindings resolve unchanged.
/// </summary>
public sealed partial class ViewerDataService
{
    public const string ServerConfigSql = """
        SELECT configuration_name, value_configured, value_in_use, is_dynamic, is_advanced
        FROM v_server_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        ORDER BY configuration_name
        """;

    /* 28 columns in Lite's exact SELECT order — the database-config reader below maps them by
       incrementing ordinal, so this list's order is load-bearing and must stay byte-identical. */
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
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        ORDER BY database_name
        """;

    public const string DatabaseScopedConfigSql = """
        SELECT database_name, configuration_name, value, value_for_secondary
        FROM v_database_scoped_config
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_database_scoped_config WHERE server_id = $1)
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        ORDER BY database_name, configuration_name
        """;


    /* V137 (#3796): the two capture modes trail the original ten, in the collector's payload order; the
       rung appended them to the table and the passthrough view is SELECT *, so a V137 store answers both.
       They are the two columns of the eight the rung added that this viewer reads, and the reason the
       connect-time gate in ViewerDataService.cs holds at V137: a store below it has no such column and this
       read would throw on the Query Store grid. */
    public const string QueryStoreHealthSql = """
        SELECT database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode
        FROM v_query_store_health
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_query_store_health WHERE server_id = $1)
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        ORDER BY database_name
        """;

    /// <summary>
    /// #3999: anchored on the trace_flags collector's newest SUCCESSFUL run, not the newest row. A capture
    /// that finds every flag off writes ZERO rows (<c>DBCC TRACESTATUS(-1)</c> only lists flags that are ON),
    /// so a plain <c>capture_time = MAX(capture_time)</c> falls back to an older capture that still had a
    /// flag on. The second <c>AND</c> compares the newest row's timestamp against the newest SUCCESS this
    /// collector logged in <c>v_collection_log</c>; if that run is newer, it found nothing on, and the whole
    /// predicate goes false so the read reports no flags rather than a stale one. The <c>COALESCE</c> floor
    /// only matters once collection_log's retention has aged past this collector's oldest trace_flags row,
    /// and defaults to the pre-fix reading rather than wrongly suppressing a row it cannot corroborate.
    /// </summary>
    public const string TraceFlagsSql = """
        SELECT trace_flag, status, is_global, is_session
        FROM v_trace_flags
        WHERE server_id = $1
        AND   capture_time = (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1)
        AND   (SELECT MAX(capture_time) FROM v_trace_flags WHERE server_id = $1) >= COALESCE(
                  (SELECT MAX(collection_time) FROM v_collection_log
                   WHERE server_id = $1 AND collector_name = 'trace_flags' AND status = 'SUCCESS'),
                  TIMESTAMP '1900-01-01')
        ORDER BY trace_flag
        """;

    /// <summary>Latest sys.configurations snapshot for one server (Server Configuration grid).</summary>
    public async Task<List<ServerConfigRow>> GetLatestServerConfigAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<ServerConfigRow>();

        await using var command = _dataSource.CreateCommand(ServerConfigSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ServerConfigRow
            {
                ConfigurationName = reader.GetString(0),
                ValueConfigured = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                ValueInUse = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                IsDynamic = !reader.IsDBNull(3) && reader.GetBoolean(3),
                IsAdvanced = !reader.IsDBNull(4) && reader.GetBoolean(4)
            });
        }

        return items;
    }

    /// <summary>Latest sys.databases snapshot for one server (Database Configuration grid).</summary>
    public async Task<List<DatabaseConfigRow>> GetLatestDatabaseConfigAsync(int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<DatabaseConfigRow>();

        await using var command = _dataSource.CreateCommand(DatabaseConfigSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* Incrementing-ordinal mapping copied verbatim from Lite so the 28-column order in
               DatabaseConfigSql binds to the same properties — see the SELECT's byte-identical note. */
            var ordinal = 0;
            items.Add(new DatabaseConfigRow
            {
                DatabaseName = reader.GetString(ordinal++),
                StateDesc = reader.IsDBNull(ordinal) ? "" : reader.GetString(ordinal),
                CompatibilityLevel = reader.IsDBNull(++ordinal) ? 0 : reader.GetInt32(ordinal),
                CollationName = reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                RecoveryModel = reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                IsReadOnly = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsAutoCloseOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsAutoShrinkOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsAutoCreateStatsOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsAutoUpdateStatsOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsAutoUpdateStatsAsyncOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsRcsiOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                SnapshotIsolationState = reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                IsParameterizationForced = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsQueryStoreOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsEncrypted = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsTrustworthyOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsDbChainingOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsBrokerEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsCdcEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsMixedPageAllocationOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                LogReuseWaitDesc = reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                PageVerifyOption = reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                TargetRecoveryTimeSeconds = reader.IsDBNull(++ordinal) ? 0 : reader.GetInt32(ordinal),
                DelayedDurability = reader.IsDBNull(++ordinal) ? "" : reader.GetString(ordinal),
                IsAcceleratedDatabaseRecoveryOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsMemoryOptimizedEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                IsOptimizedLockingOn = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
            });
        }

        return items;
    }

    /// <summary>Latest database-scoped configuration snapshot for one server (Scoped Configuration grid).</summary>
    public async Task<List<DatabaseScopedConfigRow>> GetLatestDatabaseScopedConfigAsync(int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<DatabaseScopedConfigRow>();

        await using var command = _dataSource.CreateCommand(DatabaseScopedConfigSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseScopedConfigRow
            {
                DatabaseName = reader.GetString(0),
                ConfigurationName = reader.GetString(1),
                Value = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ValueForSecondary = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }

        return items;
    }


    /// <summary>Latest per-database Query Store health snapshot for one server (Query Store grid, #2319).</summary>
    public async Task<List<QueryStoreHealthRow>> GetLatestQueryStoreHealthAsync(int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<QueryStoreHealthRow>();

        await using var command = _dataSource.CreateCommand(QueryStoreHealthSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryStoreHealthRow
            {
                DatabaseName = reader.GetString(0),
                ActualState = reader.IsDBNull(1) ? "" : reader.GetString(1),
                DesiredState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ReadonlyReason = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                CurrentStorageMb = reader.IsDBNull(4) ? 0L : reader.GetInt64(4),
                MaxStorageMb = reader.IsDBNull(5) ? 0L : reader.GetInt64(5),
                SizeBasedCleanupMode = reader.IsDBNull(6) ? "" : reader.GetString(6),
                StaleQueryThresholdDays = reader.IsDBNull(7) ? 0L : reader.GetInt64(7),
                MaxPlansPerQuery = reader.IsDBNull(8) ? 0L : reader.GetInt64(8),
                IntervalLengthMinutes = reader.IsDBNull(9) ? 0L : reader.GetInt64(9),
                /* V137 (#3796): NULL stays null — the row predates the rung, or the engine is 2016 (wait stats) —
                   and the display properties render it as the grid's absence glyph rather than as "". */
                QueryCaptureMode = reader.IsDBNull(10) ? null : reader.GetString(10),
                WaitStatsCaptureMode = reader.IsDBNull(11) ? null : reader.GetString(11),
            });
        }

        return items;
    }

    /// <summary>Latest trace-flags snapshot for one server (Trace Flags grid).</summary>
    public async Task<List<TraceFlagRow>> GetLatestTraceFlagsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<TraceFlagRow>();

        await using var command = _dataSource.CreateCommand(TraceFlagsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new TraceFlagRow
            {
                TraceFlag = reader.GetInt32(0),
                Status = !reader.IsDBNull(1) && reader.GetBoolean(1),
                IsGlobal = !reader.IsDBNull(2) && reader.GetBoolean(2),
                IsSession = !reader.IsDBNull(3) && reader.GetBoolean(3)
            });
        }

        return items;
    }
}

/* The four row classes port as-is from Lite's LocalDataService.Config.cs — the display properties
   (bool -> "Yes"/"No", etc.) are pure C#, so the copied XAML column bindings resolve unchanged. */

public class ServerConfigRow
{
    public string ConfigurationName { get; set; } = "";
    public long ValueConfigured { get; set; }
    public long ValueInUse { get; set; }
    public bool IsDynamic { get; set; }
    public bool IsAdvanced { get; set; }
    public string DynamicDisplay => IsDynamic ? "Yes" : "No";
    public string AdvancedDisplay => IsAdvanced ? "Yes" : "No";
    public bool ValuesMatch => ValueConfigured == ValueInUse;
}

public class DatabaseConfigRow
{
    public string DatabaseName { get; set; } = "";
    public string StateDesc { get; set; } = "";
    public int CompatibilityLevel { get; set; }
    public string CollationName { get; set; } = "";
    public string RecoveryModel { get; set; } = "";
    public bool IsReadOnly { get; set; }
    public bool IsAutoCloseOn { get; set; }
    public bool IsAutoShrinkOn { get; set; }
    public bool IsAutoCreateStatsOn { get; set; }
    public bool IsAutoUpdateStatsOn { get; set; }
    public bool IsAutoUpdateStatsAsyncOn { get; set; }
    public bool IsRcsiOn { get; set; }
    public string SnapshotIsolationState { get; set; } = "";
    public bool IsParameterizationForced { get; set; }
    public bool IsQueryStoreOn { get; set; }
    public bool IsEncrypted { get; set; }
    public bool IsTrustworthyOn { get; set; }
    public bool IsDbChainingOn { get; set; }
    public bool IsBrokerEnabled { get; set; }
    public bool IsCdcEnabled { get; set; }
    public bool IsMixedPageAllocationOn { get; set; }
    public string LogReuseWaitDesc { get; set; } = "";
    public string PageVerifyOption { get; set; } = "";
    public int TargetRecoveryTimeSeconds { get; set; }
    public string DelayedDurability { get; set; } = "";
    public bool IsAcceleratedDatabaseRecoveryOn { get; set; }
    public bool IsMemoryOptimizedEnabled { get; set; }
    public bool IsOptimizedLockingOn { get; set; }

    /* Display properties for DataGrid (bool -> Yes/No) */
    public string ReadOnlyDisplay => IsReadOnly ? "Yes" : "No";
    public string AutoCloseDisplay => IsAutoCloseOn ? "Yes" : "No";
    public string AutoShrinkDisplay => IsAutoShrinkOn ? "Yes" : "No";
    public string AutoCreateStatsDisplay => IsAutoCreateStatsOn ? "Yes" : "No";
    public string AutoUpdateStatsDisplay => IsAutoUpdateStatsOn ? "Yes" : "No";
    public string AutoUpdateStatsAsyncDisplay => IsAutoUpdateStatsAsyncOn ? "Yes" : "No";
    public string RcsiDisplay => IsRcsiOn ? "Yes" : "No";
    public string ParameterizationForcedDisplay => IsParameterizationForced ? "Yes" : "No";
    public string QueryStoreDisplay => IsQueryStoreOn ? "Yes" : "No";
    public string EncryptedDisplay => IsEncrypted ? "Yes" : "No";
    public string TrustworthyDisplay => IsTrustworthyOn ? "Yes" : "No";
    public string DbChainingDisplay => IsDbChainingOn ? "Yes" : "No";
    public string BrokerEnabledDisplay => IsBrokerEnabled ? "Yes" : "No";
    public string CdcEnabledDisplay => IsCdcEnabled ? "Yes" : "No";
    public string MixedPageAllocationDisplay => IsMixedPageAllocationOn ? "Yes" : "No";
    public string AdrDisplay => IsAcceleratedDatabaseRecoveryOn ? "Yes" : "No";
    public string MemoryOptimizedDisplay => IsMemoryOptimizedEnabled ? "Yes" : "No";
    public string OptimizedLockingDisplay => IsOptimizedLockingOn ? "Yes" : "No";
}


/// <summary>
/// One database's Query Store health row (#2319) — the latest collected
/// sys.database_query_store_options snapshot. <see cref="StateDisplay"/> folds the classic silent
/// failure into one glanceable cell: actual and desired agreeing shows one state; disagreeing shows
/// both, because desired READ_WRITE with actual READ_ONLY is precisely the condition this collector
/// exists to surface. <see cref="ReadonlyReasonDisplay"/> decodes the bitmask values an operator
/// actually meets; unknown bits fall back to the raw number rather than guessing.
///
/// <para>V137 (#3796) added the two capture modes as the row's trailing pair. <see cref="QueryCaptureMode"/>
/// is the one option on this row that names a plan-churn factory: <c>ALL</c> captures every query the engine
/// compiles, one-off ad hoc statements included, so on an ad hoc workload each distinct text is a new query
/// with a new plan and the store fills toward its cap; <c>AUTO</c> (the engine default since 2019) skips
/// insignificant queries; <c>CUSTOM</c> (2019+) is <c>AUTO</c> with operator-set thresholds; <c>NONE</c> stops
/// capturing new queries. <see cref="WaitStatsCaptureMode"/> <c>ON</c> / <c>OFF</c> is whether per-plan wait
/// statistics are recorded into every runtime interval. Both are the DMV's <c>*_desc</c> spelling verbatim and
/// both are nullable, because NULL is a real state here — the row predates the rung, or (wait stats) the engine
/// is SQL Server 2016, where the column does not exist — and the two <c>*Display</c> properties render it as the
/// grid's absence glyph rather than as a blank that reads like a value.</para>
/// </summary>
public class QueryStoreHealthRow
{
    public string DatabaseName { get; set; } = "";
    public string ActualState { get; set; } = "";
    public string DesiredState { get; set; } = "";
    public int ReadonlyReason { get; set; }
    public long CurrentStorageMb { get; set; }
    public long MaxStorageMb { get; set; }
    public string SizeBasedCleanupMode { get; set; } = "";
    public long StaleQueryThresholdDays { get; set; }
    public long MaxPlansPerQuery { get; set; }
    public long IntervalLengthMinutes { get; set; }

    /// <summary>V137 (#3796): <c>query_capture_mode_desc</c> verbatim — <c>ALL</c> / <c>AUTO</c> / <c>CUSTOM</c> /
    /// <c>NONE</c>; null on a pre-rung row.</summary>
    public string? QueryCaptureMode { get; set; }

    /// <summary>V137 (#3796): <c>wait_stats_capture_mode_desc</c> verbatim — <c>ON</c> / <c>OFF</c>; null on a
    /// pre-rung row or a 2016 engine, and that null means "the engine cannot say", never <c>OFF</c>.</summary>
    public string? WaitStatsCaptureMode { get; set; }

    /// <summary>The Capture Mode cell: the mode verbatim, or the absence glyph for a null (pre-rung row).</summary>
    public string CaptureModeDisplay => QueryCaptureMode ?? "—";

    /// <summary>The Wait Stats Capture cell: <c>ON</c> / <c>OFF</c> verbatim, or the absence glyph for a null
    /// (pre-rung row, or a 2016 engine that has no such option — not <c>OFF</c>).</summary>
    public string WaitStatsCaptureModeDisplay => WaitStatsCaptureMode ?? "—";

    public string StateDisplay =>
        string.Equals(ActualState, DesiredState, StringComparison.OrdinalIgnoreCase)
            ? ActualState
            : $"{ActualState} (wanted {DesiredState})";

    /// <summary>Percent of the storage cap in use; blank when the cap is 0 (unlimited/unknown).</summary>
    public string PercentOfCapDisplay =>
        MaxStorageMb > 0 ? $"{100.0 * CurrentStorageMb / MaxStorageMb:F0}%" : "";

    /// <summary>The shared bit-by-bit decode — one label table for every surface that shows this value.</summary>
    public string ReadonlyReasonDisplay => PerformanceMonitor.Common.QueryStoreReadonlyReason.Decode(ReadonlyReason);
}

public class DatabaseScopedConfigRow
{
    public string DatabaseName { get; set; } = "";
    public string ConfigurationName { get; set; } = "";
    public string Value { get; set; } = "";
    public string ValueForSecondary { get; set; } = "";
}

public class TraceFlagRow
{
    public int TraceFlag { get; set; }
    public bool Status { get; set; }
    public bool IsGlobal { get; set; }
    public bool IsSession { get; set; }
    public string StatusDisplay => Status ? "Enabled" : "Disabled";
    public string GlobalDisplay => IsGlobal ? "Yes" : "No";
    public string SessionDisplay => IsSession ? "Yes" : "No";
}
