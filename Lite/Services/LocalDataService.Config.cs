/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest server configuration snapshot (sys.configurations).
    /// </summary>
    public async Task<List<ServerConfigRow>> GetLatestServerConfigAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT configuration_name, value_configured, value_in_use, is_dynamic, is_advanced
FROM server_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM server_config WHERE server_id = $1)
ORDER BY configuration_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<ServerConfigRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ServerConfigRow
            {
                ConfigurationName = reader.GetString(0),
                ValueConfigured = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                ValueInUse = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                IsDynamic = !reader.IsDBNull(3) && reader.GetBoolean(3),
                IsAdvanced = !reader.IsDBNull(4) && reader.GetBoolean(4)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest database configuration snapshot (sys.databases).
    /// </summary>
    public async Task<List<DatabaseConfigRow>> GetLatestDatabaseConfigAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT database_name, state_desc, compatibility_level, collation_name, recovery_model,
       is_read_only, is_auto_close_on, is_auto_shrink_on,
       is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
       is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced,
       is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
       is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on,
       log_reuse_wait_desc, page_verify_option, target_recovery_time_seconds, delayed_durability,
       is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on
FROM database_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM database_config WHERE server_id = $1)
ORDER BY database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseConfigRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
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

    /// <summary>
    /// Gets the latest database-scoped configuration snapshot.
    /// </summary>
    public async Task<List<DatabaseScopedConfigRow>> GetLatestDatabaseScopedConfigAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT database_name, configuration_name, value, value_for_secondary
FROM database_scoped_config
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM database_scoped_config WHERE server_id = $1)
ORDER BY database_name, configuration_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseScopedConfigRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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

    /// <summary>
    /// Gets the latest trace flags snapshot.
    /// </summary>
    public async Task<List<TraceFlagRow>> GetLatestTraceFlagsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT trace_flag, status, is_global, is_session
FROM trace_flags
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM trace_flags WHERE server_id = $1)
ORDER BY trace_flag";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<TraceFlagRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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

    /* Display properties for DataGrid (bool → Yes/No) */
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
