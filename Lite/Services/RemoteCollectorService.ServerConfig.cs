/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Collects server configuration from sys.configurations. On-load only, not scheduled.
    /// </summary>
    private async Task<int> CollectServerConfigAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    configuration_name = c.name,
    value_configured = CONVERT(bigint, c.value),
    value_in_use = CONVERT(bigint, c.value_in_use),
    is_dynamic = c.is_dynamic,
    is_advanced = c.is_advanced
FROM sys.configurations AS c
ORDER BY c.name
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var captureTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        /* Read all rows from SQL Server first to avoid holding appender open during SQL reads */
        var rows = new List<(string Name, long ValueConfigured, long ValueInUse, bool IsDynamic, bool IsAdvanced)>();

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetBoolean(3),
                reader.GetBoolean(4)));
        }
        sqlSw.Stop();

        /* Write to DuckDB using appender */
        var duckSw = Stopwatch.StartNew();
        using var duckConnection = _duckDb.CreateConnection();
        await duckConnection.OpenAsync(cancellationToken);

        using var appender = duckConnection.CreateAppender("server_config");
        foreach (var r in rows)
        {
            var row = appender.CreateRow();
            row.AppendValue(GenerateCollectionId())
               .AppendValue(captureTime)
               .AppendValue(serverId)
               .AppendValue(server.ServerName)
               .AppendValue(r.Name)
               .AppendValue(r.ValueConfigured)
               .AppendValue(r.ValueInUse)
               .AppendValue(r.IsDynamic)
               .AppendValue(r.IsAdvanced)
               .EndRow();
            rowsCollected++;
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} server config rows for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }

    /// <summary>
    /// Collects database configuration from sys.databases. On-load only, not scheduled.
    /// Version-gated columns are conditionally included based on the server's major version.
    /// </summary>
    private async Task<int> CollectDatabaseConfigAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        var majorVersion = serverStatus.SqlMajorVersion;

        /* Base columns available on all supported versions (2016+) */
        var selectColumns = @"
    database_name = d.name,
    state_desc = d.state_desc,
    compatibility_level = d.compatibility_level,
    collation_name = d.collation_name,
    recovery_model = d.recovery_model_desc,
    is_read_only = d.is_read_only,
    is_auto_close_on = d.is_auto_close_on,
    is_auto_shrink_on = d.is_auto_shrink_on,
    is_auto_create_stats_on = d.is_auto_create_stats_on,
    is_auto_update_stats_on = d.is_auto_update_stats_on,
    is_auto_update_stats_async_on = d.is_auto_update_stats_async_on,
    is_read_committed_snapshot_on = d.is_read_committed_snapshot_on,
    snapshot_isolation_state = d.snapshot_isolation_state_desc,
    is_parameterization_forced = d.is_parameterization_forced,
    is_query_store_on = d.is_query_store_on,
    is_encrypted = d.is_encrypted,
    is_trustworthy_on = d.is_trustworthy_on,
    is_db_chaining_on = d.is_db_chaining_on,
    is_broker_enabled = d.is_broker_enabled,
    is_cdc_enabled = d.is_cdc_enabled,
    is_mixed_page_allocation_on = d.is_mixed_page_allocation_on,
    log_reuse_wait_desc = d.log_reuse_wait_desc,
    page_verify_option = d.page_verify_option_desc,
    target_recovery_time_seconds = d.target_recovery_time_in_seconds,
    delayed_durability = d.delayed_durability_desc";

        /* SQL Server 2019+ (major version 15), or Azure SQL DB/MI which always have these */
        var isAzure = serverStatus.SqlEngineEdition == 5 || serverStatus.SqlEngineEdition == 8;
        var has2019Columns = majorVersion >= 15 || majorVersion == 0 || isAzure;
        if (has2019Columns)
        {
            selectColumns += @",
    is_accelerated_database_recovery_on = d.is_accelerated_database_recovery_on,
    is_memory_optimized_enabled = d.is_memory_optimized_enabled";
        }

        /* SQL Server 2025+ (major version 17) */
        var has2025Columns = majorVersion >= 17;
        if (has2025Columns)
        {
            selectColumns += @",
    is_optimized_locking_on = d.is_optimized_locking_on";
        }

        var query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
{selectColumns}
FROM sys.databases AS d
WHERE (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
ORDER BY d.name
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var captureTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var rows = new List<DatabaseConfigCollected>();

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var ordinal = 0;
            var r = new DatabaseConfigCollected
            {
                DbName = reader.GetString(ordinal++),
                StateDesc = reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal),
                CompatLevel = reader.IsDBNull(++ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal)),
                CollationName = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                RecoveryModel = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                IsReadOnly = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoClose = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoShrink = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoCreateStats = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoUpdateStats = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoUpdateStatsAsync = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                Rcsi = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                SnapshotIsolation = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                ParameterizationForced = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                QueryStore = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                Encrypted = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                Trustworthy = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                DbChaining = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                BrokerEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                CdcEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                MixedPageAllocation = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                LogReuseWait = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                PageVerify = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                TargetRecovery = reader.IsDBNull(++ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal)),
                DelayedDurability = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
            };

            if (has2019Columns)
            {
                r.AcceleratedDatabaseRecovery = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal);
                r.MemoryOptimized = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal);
            }

            if (has2025Columns)
            {
                r.OptimizedLocking = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal);
            }

            rows.Add(r);
        }
        sqlSw.Stop();

        var duckSw = Stopwatch.StartNew();
        using var duckConnection = _duckDb.CreateConnection();
        await duckConnection.OpenAsync(cancellationToken);

        using var appender = duckConnection.CreateAppender("database_config");
        foreach (var r in rows)
        {
            var row = appender.CreateRow();
            row.AppendValue(GenerateCollectionId())
               .AppendValue(captureTime)
               .AppendValue(serverId)
               .AppendValue(server.ServerName)
               .AppendValue(r.DbName)
               .AppendValue(r.StateDesc)
               .AppendValue(r.CompatLevel)
               .AppendValue(r.CollationName)
               .AppendValue(r.RecoveryModel)
               .AppendValue(r.IsReadOnly)
               .AppendValue(r.AutoClose)
               .AppendValue(r.AutoShrink)
               .AppendValue(r.AutoCreateStats)
               .AppendValue(r.AutoUpdateStats)
               .AppendValue(r.AutoUpdateStatsAsync)
               .AppendValue(r.Rcsi)
               .AppendValue(r.SnapshotIsolation)
               .AppendValue(r.ParameterizationForced)
               .AppendValue(r.QueryStore)
               .AppendValue(r.Encrypted)
               .AppendValue(r.Trustworthy)
               .AppendValue(r.DbChaining)
               .AppendValue(r.BrokerEnabled)
               .AppendValue(r.CdcEnabled)
               .AppendValue(r.MixedPageAllocation)
               .AppendValue(r.LogReuseWait)
               .AppendValue(r.PageVerify)
               .AppendValue(r.TargetRecovery)
               .AppendValue(r.DelayedDurability);

            /* Version-gated columns: write value if collected, NULL otherwise */
            if (r.AcceleratedDatabaseRecovery.HasValue)
                row.AppendValue(r.AcceleratedDatabaseRecovery.Value);
            else
                row.AppendNullValue();

            if (r.MemoryOptimized.HasValue)
                row.AppendValue(r.MemoryOptimized.Value);
            else
                row.AppendNullValue();

            if (r.OptimizedLocking.HasValue)
                row.AppendValue(r.OptimizedLocking.Value);
            else
                row.AppendNullValue();

            row.EndRow();
            rowsCollected++;
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} database config rows for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }

    private class DatabaseConfigCollected
    {
        public string DbName { get; set; } = "";
        public string? StateDesc { get; set; }
        public int CompatLevel { get; set; }
        public string? CollationName { get; set; }
        public string? RecoveryModel { get; set; }
        public bool IsReadOnly { get; set; }
        public bool AutoClose { get; set; }
        public bool AutoShrink { get; set; }
        public bool AutoCreateStats { get; set; }
        public bool AutoUpdateStats { get; set; }
        public bool AutoUpdateStatsAsync { get; set; }
        public bool Rcsi { get; set; }
        public string? SnapshotIsolation { get; set; }
        public bool ParameterizationForced { get; set; }
        public bool QueryStore { get; set; }
        public bool Encrypted { get; set; }
        public bool Trustworthy { get; set; }
        public bool DbChaining { get; set; }
        public bool BrokerEnabled { get; set; }
        public bool CdcEnabled { get; set; }
        public bool MixedPageAllocation { get; set; }
        public string? LogReuseWait { get; set; }
        public string? PageVerify { get; set; }
        public int TargetRecovery { get; set; }
        public string? DelayedDurability { get; set; }
        public bool? AcceleratedDatabaseRecovery { get; set; }
        public bool? MemoryOptimized { get; set; }
        public bool? OptimizedLocking { get; set; }
    }

    /// <summary>
    /// Collects database-scoped configurations from sys.database_scoped_configurations
    /// for each online user database. On-load only, not scheduled.
    /// </summary>
    private async Task<int> CollectDatabaseScopedConfigAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string dbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name
FROM sys.databases AS d
WHERE (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
AND   d.state_desc = N'ONLINE'
ORDER BY d.name
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var captureTime = DateTime.UtcNow;
        var totalRows = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);

        /* Get list of databases */
        var databases = new List<string>();
        using (var dbCommand = new SqlCommand(dbQuery, sqlConnection))
        {
            dbCommand.CommandTimeout = CommandTimeoutSeconds;
            using var dbReader = await dbCommand.ExecuteReaderAsync(cancellationToken);
            while (await dbReader.ReadAsync(cancellationToken))
            {
                databases.Add(dbReader.GetString(0));
            }
        }

        if (databases.Count == 0)
        {
            return 0;
        }

        /* Collect all scoped configs from SQL Server first */
        var scopedRows = new List<(string DbName, string ConfigName, string? Value, string? ValueForSecondary)>();

        foreach (var dbName in databases)
        {
            try
            {
                /* Use [dbname].sys.sp_executesql to run in database context (Azure SQL DB compatible) */
                var scopedQuery = $@"
EXECUTE [{dbName.Replace("]", "]]")}].sys.sp_executesql
    N'SELECT
         configuration_name = dsc.name,
         value = CONVERT(nvarchar(256), dsc.value),
         value_for_secondary = CONVERT(nvarchar(256), dsc.value_for_secondary)
     FROM sys.database_scoped_configurations AS dsc
     ORDER BY dsc.name
     OPTION(RECOMPILE);'";

                using var cmd = new SqlCommand(scopedQuery, sqlConnection);
                cmd.CommandTimeout = CommandTimeoutSeconds;

                using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    scopedRows.Add((
                        dbName,
                        reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2)));
                }
            }
            catch (SqlException ex)
            {
                _logger?.LogWarning("Failed to collect scoped config from [{Database}] on '{Server}': {Message}",
                    dbName, server.DisplayName, ex.Message);
            }
        }

        sqlSw.Stop();

        /* Write to DuckDB using appender */
        var duckSw = Stopwatch.StartNew();
        using var duckConnection = _duckDb.CreateConnection();
        await duckConnection.OpenAsync(cancellationToken);

        using var appender = duckConnection.CreateAppender("database_scoped_config");
        foreach (var (dbName, configName, value, valueForSecondary) in scopedRows)
        {
            var row = appender.CreateRow();
            row.AppendValue(GenerateCollectionId())
               .AppendValue(captureTime)
               .AppendValue(serverId)
               .AppendValue(server.ServerName)
               .AppendValue(dbName)
               .AppendValue(configName)
               .AppendValue(value)
               .AppendValue(valueForSecondary)
               .EndRow();
            totalRows++;
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} database scoped config rows across {DbCount} databases for server '{Server}'",
            totalRows, databases.Count, server.DisplayName);
        return totalRows;
    }

    /// <summary>
    /// Collects active trace flags via DBCC TRACESTATUS(-1). On-load only, not scheduled.
    /// Wrapped in try/catch — fails gracefully if caller lacks DBCC permissions.
    /// </summary>
    private async Task<int> CollectTraceFlagsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

CREATE TABLE
    #trace_flags
(
    trace_flag integer NOT NULL,
    status bit NOT NULL,
    is_global bit NOT NULL,
    is_session bit NOT NULL
);

INSERT
    #trace_flags
(
    trace_flag,
    status,
    is_global,
    is_session
)
EXECUTE(N'DBCC TRACESTATUS(-1) WITH NO_INFOMSGS;');

SELECT
    tf.trace_flag,
    tf.status,
    tf.is_global,
    tf.is_session
FROM #trace_flags AS tf
ORDER BY tf.trace_flag
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var captureTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        try
        {
            var rows = new List<(int TraceFlag, bool Status, bool IsGlobal, bool IsSession)>();

            var sqlSw = Stopwatch.StartNew();
            using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
            using var command = new SqlCommand(query, sqlConnection);
            command.CommandTimeout = CommandTimeoutSeconds;

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add((
                    reader.GetInt32(0),
                    reader.GetBoolean(1),
                    reader.GetBoolean(2),
                    reader.GetBoolean(3)));
            }
            sqlSw.Stop();

            var duckSw = Stopwatch.StartNew();
            using var duckConnection = _duckDb.CreateConnection();
            await duckConnection.OpenAsync(cancellationToken);

            using var appender = duckConnection.CreateAppender("trace_flags");
            foreach (var r in rows)
            {
                var row = appender.CreateRow();
                row.AppendValue(GenerateCollectionId())
                   .AppendValue(captureTime)
                   .AppendValue(serverId)
                   .AppendValue(server.ServerName)
                   .AppendValue(r.TraceFlag)
                   .AppendValue(r.Status)
                   .AppendValue(r.IsGlobal)
                   .AppendValue(r.IsSession)
                   .EndRow();
                rowsCollected++;
            }

            duckSw.Stop();
            _lastSqlMs = sqlSw.ElapsedMilliseconds;
            _lastDuckDbMs = duckSw.ElapsedMilliseconds;

            _logger?.LogDebug("Collected {RowCount} trace flag rows for server '{Server}'", rowsCollected, server.DisplayName);
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning("Failed to collect trace flags on '{Server}' (may lack DBCC permissions): {Message}",
                server.DisplayName, ex.Message);
        }

        return rowsCollected;
    }
}
