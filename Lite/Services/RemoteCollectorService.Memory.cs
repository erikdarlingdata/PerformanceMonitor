/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
    /// Collects memory statistics from sys.dm_os_sys_memory and performance counters.
    /// </summary>
    private async Task<int> CollectMemoryStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        /* Azure SQL DB (edition 5) doesn't have sys.dm_os_sys_memory or sql_memory_model_desc.
           Use sys.dm_os_sys_info committed_target_kb/committed_kb as approximations.
           Azure MI (edition 8) HAS dm_os_sys_memory, sql_memory_model_desc, and behaves like on-prem. */
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        string query = isAzureSqlDb
            ? @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    total_physical_memory_mb = CONVERT(decimal(18,2), osi.committed_target_kb / 1024.0),
    available_physical_memory_mb = CONVERT(decimal(18,2), (osi.committed_target_kb - osi.committed_kb) / 1024.0),
    total_page_file_mb = CONVERT(decimal(18,2), 0),
    available_page_file_mb = CONVERT(decimal(18,2), 0),
    system_memory_state = N'Available',
    sql_memory_model = N'N/A',
    target_server_memory_mb = CONVERT(decimal(18,2), pc_target.cntr_value / 1024.0),
    total_server_memory_mb = CONVERT(decimal(18,2), pc_total.cntr_value / 1024.0),
    buffer_pool_mb = CONVERT(decimal(18,2), pc_buffer.cntr_value / 1024.0),
    plan_cache_mb = CONVERT(decimal(18,2), pc_plan.cntr_value * 8.0 / 1024.0)
FROM sys.dm_os_sys_info AS osi
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Target Server Memory (KB)'
) AS pc_target
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Total Server Memory (KB)'
) AS pc_total
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Database Cache Memory (KB)'
) AS pc_buffer
CROSS JOIN
(
    SELECT cntr_value = SUM(cntr_value)
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Cache Pages'
      AND object_name LIKE N'%:Plan Cache%'
) AS pc_plan
OPTION(RECOMPILE);"
            : @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    total_physical_memory_mb = CONVERT(decimal(18,2), osm.total_physical_memory_kb / 1024.0),
    available_physical_memory_mb = CONVERT(decimal(18,2), osm.available_physical_memory_kb / 1024.0),
    total_page_file_mb = CONVERT(decimal(18,2), osm.total_page_file_kb / 1024.0),
    available_page_file_mb = CONVERT(decimal(18,2), osm.available_page_file_kb / 1024.0),
    system_memory_state = osm.system_memory_state_desc,
    sql_memory_model = osi.sql_memory_model_desc,
    target_server_memory_mb = CONVERT(decimal(18,2), pc_target.cntr_value / 1024.0),
    total_server_memory_mb = CONVERT(decimal(18,2), pc_total.cntr_value / 1024.0),
    buffer_pool_mb = CONVERT(decimal(18,2), pc_buffer.cntr_value / 1024.0),
    plan_cache_mb = CONVERT(decimal(18,2), pc_plan.cntr_value * 8.0 / 1024.0)
FROM sys.dm_os_sys_memory AS osm
CROSS JOIN sys.dm_os_sys_info AS osi
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Target Server Memory (KB)'
) AS pc_target
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Total Server Memory (KB)'
) AS pc_total
CROSS JOIN
(
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Database Cache Memory (KB)'
) AS pc_buffer
CROSS JOIN
(
    SELECT cntr_value = SUM(cntr_value)
    FROM sys.dm_os_performance_counters
    WHERE counter_name = N'Cache Pages'
      AND object_name LIKE N'%:Plan Cache%'
) AS pc_plan
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return 0;
        }

        var totalPhysicalMb = reader.IsDBNull(0) ? 0m : reader.GetDecimal(0);
        var availablePhysicalMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
        var totalPageFileMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2);
        var availablePageFileMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3);
        var systemMemoryState = reader.IsDBNull(4) ? "Unknown" : reader.GetString(4);
        var sqlMemoryModel = reader.IsDBNull(5) ? "Unknown" : reader.GetString(5);
        var targetServerMemoryMb = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6);
        var totalServerMemoryMb = reader.IsDBNull(7) ? 0m : reader.GetDecimal(7);
        var bufferPoolMb = reader.IsDBNull(8) ? 0m : reader.GetDecimal(8);
        var planCacheMb = reader.IsDBNull(9) ? 0m : reader.GetDecimal(9);
        sqlSw.Stop();

        /* Insert into DuckDB using Appender */
        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("memory_stats"))
            {
                var row = appender.CreateRow();
                row.AppendValue(GenerateCollectionId())
                   .AppendValue(collectionTime)
                   .AppendValue(serverId)
                   .AppendValue(server.ServerName)
                   .AppendValue(totalPhysicalMb)
                   .AppendValue(availablePhysicalMb)
                   .AppendValue(totalPageFileMb)
                   .AppendValue(availablePageFileMb)
                   .AppendValue(systemMemoryState)
                   .AppendValue(sqlMemoryModel)
                   .AppendValue(targetServerMemoryMb)
                   .AppendValue(totalServerMemoryMb)
                   .AppendValue(bufferPoolMb)
                   .AppendValue(planCacheMb)
                   .EndRow();
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected memory stats for server '{Server}'", server.DisplayName);
        return 1;
    }

    /// <summary>
    /// Collects top memory clerks from sys.dm_os_memory_clerks.
    /// </summary>
    private async Task<int> CollectMemoryClerksAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (25)
    clerk_type = mc.type,
    memory_mb = CONVERT(decimal(18,2), SUM(mc.pages_kb) / 1024.0)
FROM sys.dm_os_memory_clerks AS mc
GROUP BY
    mc.type
HAVING
    SUM(mc.pages_kb) > 1024
ORDER BY
    SUM(mc.pages_kb) DESC
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        sqlSw.Stop();

        /* Insert into DuckDB */
        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("memory_clerks"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(reader.GetString(0))
                       .AppendValue(reader.GetDecimal(1))
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} memory clerks for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
