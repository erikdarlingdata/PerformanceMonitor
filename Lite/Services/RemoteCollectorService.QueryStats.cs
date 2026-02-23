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
    /// Collects query statistics from sys.dm_exec_query_stats.
    /// </summary>
    private async Task<int> CollectQueryStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        /* On Azure SQL DB, dm_exec_plan_attributes reports dbid=1 (master) for ALL queries,
           so the standard INNER JOIN + NOT IN filter excludes everything.
           Use a simplified query that skips plan_attributes entirely — there's only one user database. */
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        const string standardQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (200)
    database_name = d.name,
    query_hash = CONVERT(varchar(64), qs.query_hash, 1),
    query_plan_hash = CONVERT(varchar(64), qs.query_plan_hash, 1),
    execution_count = qs.execution_count,
    total_worker_time = qs.total_worker_time,
    total_elapsed_time = qs.total_elapsed_time,
    total_logical_reads = qs.total_logical_reads,
    total_logical_writes = qs.total_logical_writes,
    total_physical_reads = qs.total_physical_reads,
    total_rows = qs.total_rows,
    total_spills = qs.total_spills,
    min_worker_time = qs.min_worker_time,
    max_worker_time = qs.max_worker_time,
    min_elapsed_time = qs.min_elapsed_time,
    max_elapsed_time = qs.max_elapsed_time,
    min_dop = qs.min_dop,
    max_dop = qs.max_dop,
    sql_handle = CONVERT(varchar(64), qs.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), qs.plan_handle, 1),
    query_text =
        CASE
            WHEN qs.statement_start_offset = 0
            AND  qs.statement_end_offset = -1
            THEN st.text
            ELSE
                SUBSTRING
                (
                    st.text,
                    (qs.statement_start_offset / 2) + 1,
                    (
                        CASE
                            WHEN qs.statement_end_offset = -1
                            THEN DATALENGTH(st.text)
                            ELSE qs.statement_end_offset
                        END - qs.statement_start_offset
                    ) / 2 + 1
                )
        END
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
    WHERE pa.attribute = N'dbid'
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE pa.dbid NOT IN (1, 2, 3, 4, 32761, 32767, ISNULL(DB_ID(N'PerformanceMonitor'), 0))
AND   qs.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

        /* Azure SQL DB: skip plan_attributes, use DB_NAME() for the single database context */
        const string azureSqlDbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (200)
    database_name = DB_NAME(),
    query_hash = CONVERT(varchar(64), qs.query_hash, 1),
    query_plan_hash = CONVERT(varchar(64), qs.query_plan_hash, 1),
    execution_count = qs.execution_count,
    total_worker_time = qs.total_worker_time,
    total_elapsed_time = qs.total_elapsed_time,
    total_logical_reads = qs.total_logical_reads,
    total_logical_writes = qs.total_logical_writes,
    total_physical_reads = qs.total_physical_reads,
    total_rows = qs.total_rows,
    total_spills = qs.total_spills,
    min_worker_time = qs.min_worker_time,
    max_worker_time = qs.max_worker_time,
    min_elapsed_time = qs.min_elapsed_time,
    max_elapsed_time = qs.max_elapsed_time,
    min_dop = qs.min_dop,
    max_dop = qs.max_dop,
    sql_handle = CONVERT(varchar(64), qs.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), qs.plan_handle, 1),
    query_text =
        CASE
            WHEN qs.statement_start_offset = 0
            AND  qs.statement_end_offset = -1
            THEN st.text
            ELSE
                SUBSTRING
                (
                    st.text,
                    (qs.statement_start_offset / 2) + 1,
                    (
                        CASE
                            WHEN qs.statement_end_offset = -1
                            THEN DATALENGTH(st.text)
                            ELSE qs.statement_end_offset
                        END - qs.statement_start_offset
                    ) / 2 + 1
                )
        END
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE qs.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

        string query = isAzureSqlDb ? azureSqlDbQuery : standardQuery;

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

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("query_stats"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var queryHash = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    var executionCount = reader.IsDBNull(3) ? 0L : reader.GetInt64(3);
                    var totalWorkerTime = reader.IsDBNull(4) ? 0L : reader.GetInt64(4);
                    var totalElapsedTime = reader.IsDBNull(5) ? 0L : reader.GetInt64(5);
                    var totalLogicalReads = reader.IsDBNull(6) ? 0L : reader.GetInt64(6);
                    var totalLogicalWrites = reader.IsDBNull(7) ? 0L : reader.GetInt64(7);
                    var totalPhysicalReads = reader.IsDBNull(8) ? 0L : reader.GetInt64(8);
                    var totalRows = reader.IsDBNull(9) ? 0L : reader.GetInt64(9);
                    var totalSpills = reader.IsDBNull(10) ? 0L : reader.GetInt64(10);
                    var minWorkerTime = reader.IsDBNull(11) ? 0L : reader.GetInt64(11);
                    var maxWorkerTime = reader.IsDBNull(12) ? 0L : reader.GetInt64(12);
                    var minElapsedTime = reader.IsDBNull(13) ? 0L : reader.GetInt64(13);
                    var maxElapsedTime = reader.IsDBNull(14) ? 0L : reader.GetInt64(14);
                    var minDop = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15));
                    var maxDop = reader.IsDBNull(16) ? 0 : Convert.ToInt32(reader.GetValue(16));
                    var sqlHandle = reader.IsDBNull(17) ? (string?)null : reader.GetString(17);
                    var planHandle = reader.IsDBNull(18) ? (string?)null : reader.GetString(18);

                    /* Delta calculations based on query_hash */
                    var deltaExecCount = _deltaCalculator.CalculateDelta(serverId, "query_stats_exec", queryHash, executionCount);
                    var deltaWorkerTime = _deltaCalculator.CalculateDelta(serverId, "query_stats_worker", queryHash, totalWorkerTime);
                    var deltaElapsedTime = _deltaCalculator.CalculateDelta(serverId, "query_stats_elapsed", queryHash, totalElapsedTime);
                    var deltaLogicalReads = _deltaCalculator.CalculateDelta(serverId, "query_stats_reads", queryHash, totalLogicalReads);
                    var deltaLogicalWrites = _deltaCalculator.CalculateDelta(serverId, "query_stats_writes", queryHash, totalLogicalWrites);
                    var deltaPhysicalReads = _deltaCalculator.CalculateDelta(serverId, "query_stats_phys_reads", queryHash, totalPhysicalReads);
                    var deltaRows = _deltaCalculator.CalculateDelta(serverId, "query_stats_rows", queryHash, totalRows);
                    var deltaSpills = _deltaCalculator.CalculateDelta(serverId, "query_stats_spills", queryHash, totalSpills);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(reader.IsDBNull(0) ? (string?)null : reader.GetString(0))
                       .AppendValue(queryHash)
                       .AppendValue(reader.IsDBNull(2) ? (string?)null : reader.GetString(2))
                       .AppendValue(executionCount)
                       .AppendValue(totalWorkerTime)
                       .AppendValue(totalElapsedTime)
                       .AppendValue(totalLogicalReads)
                       .AppendValue(totalLogicalWrites)
                       .AppendValue(totalPhysicalReads)
                       .AppendValue(totalRows)
                       .AppendValue(totalSpills)
                       .AppendValue(minWorkerTime)
                       .AppendValue(maxWorkerTime)
                       .AppendValue(minElapsedTime)
                       .AppendValue(maxElapsedTime)
                       .AppendValue(minDop)
                       .AppendValue(maxDop)
                       .AppendValue(reader.IsDBNull(19) ? (string?)null : reader.GetString(19))
                       .AppendValue((string?)null) /* query plans retrieved on-demand */
                       .AppendValue(sqlHandle)
                       .AppendValue(planHandle)
                       .AppendValue(deltaExecCount)
                       .AppendValue(deltaWorkerTime)
                       .AppendValue(deltaElapsedTime)
                       .AppendValue(deltaLogicalReads)
                       .AppendValue(deltaLogicalWrites)
                       .AppendValue(deltaPhysicalReads)
                       .AppendValue(deltaRows)
                       .AppendValue(deltaSpills)
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} query stats for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
