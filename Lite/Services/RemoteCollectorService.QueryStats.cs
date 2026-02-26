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

SELECT /* PerformanceMonitorLite */ TOP (200)
    database_name = d.name,
    query_hash = CONVERT(varchar(64), qs.query_hash, 1),
    query_plan_hash = CONVERT(varchar(64), qs.query_plan_hash, 1),
    creation_time = qs.creation_time,
    last_execution_time = qs.last_execution_time,
    execution_count = qs.execution_count,
    total_worker_time = qs.total_worker_time,
    total_elapsed_time = qs.total_elapsed_time,
    total_logical_reads = qs.total_logical_reads,
    total_logical_writes = qs.total_logical_writes,
    total_physical_reads = qs.total_physical_reads,
    total_clr_time = qs.total_clr_time,
    total_rows = qs.total_rows,
    total_spills = qs.total_spills,
    min_worker_time = qs.min_worker_time,
    max_worker_time = qs.max_worker_time,
    min_elapsed_time = qs.min_elapsed_time,
    max_elapsed_time = qs.max_elapsed_time,
    min_physical_reads = qs.min_physical_reads,
    max_physical_reads = qs.max_physical_reads,
    min_rows = qs.min_rows,
    max_rows = qs.max_rows,
    min_dop = qs.min_dop,
    max_dop = qs.max_dop,
    min_grant_kb = qs.min_grant_kb,
    max_grant_kb = qs.max_grant_kb,
    min_used_grant_kb = qs.min_used_grant_kb,
    max_used_grant_kb = qs.max_used_grant_kb,
    min_ideal_grant_kb = qs.min_ideal_grant_kb,
    max_ideal_grant_kb = qs.max_ideal_grant_kb,
    min_reserved_threads = qs.min_reserved_threads,
    max_reserved_threads = qs.max_reserved_threads,
    min_used_threads = qs.min_used_threads,
    max_used_threads = qs.max_used_threads,
    min_spills = qs.min_spills,
    max_spills = qs.max_spills,
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
AND   st.text NOT LIKE N'%PerformanceMonitorLite%'
AND   qs.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

        /* Azure SQL DB: skip plan_attributes, use DB_NAME() for the single database context */
        const string azureSqlDbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */ TOP (200)
    database_name = DB_NAME(),
    query_hash = CONVERT(varchar(64), qs.query_hash, 1),
    query_plan_hash = CONVERT(varchar(64), qs.query_plan_hash, 1),
    creation_time = qs.creation_time,
    last_execution_time = qs.last_execution_time,
    execution_count = qs.execution_count,
    total_worker_time = qs.total_worker_time,
    total_elapsed_time = qs.total_elapsed_time,
    total_logical_reads = qs.total_logical_reads,
    total_logical_writes = qs.total_logical_writes,
    total_physical_reads = qs.total_physical_reads,
    total_clr_time = qs.total_clr_time,
    total_rows = qs.total_rows,
    total_spills = qs.total_spills,
    min_worker_time = qs.min_worker_time,
    max_worker_time = qs.max_worker_time,
    min_elapsed_time = qs.min_elapsed_time,
    max_elapsed_time = qs.max_elapsed_time,
    min_physical_reads = qs.min_physical_reads,
    max_physical_reads = qs.max_physical_reads,
    min_rows = qs.min_rows,
    max_rows = qs.max_rows,
    min_dop = qs.min_dop,
    max_dop = qs.max_dop,
    min_grant_kb = qs.min_grant_kb,
    max_grant_kb = qs.max_grant_kb,
    min_used_grant_kb = qs.min_used_grant_kb,
    max_used_grant_kb = qs.max_used_grant_kb,
    min_ideal_grant_kb = qs.min_ideal_grant_kb,
    max_ideal_grant_kb = qs.max_ideal_grant_kb,
    min_reserved_threads = qs.min_reserved_threads,
    max_reserved_threads = qs.max_reserved_threads,
    min_used_threads = qs.min_used_threads,
    max_used_threads = qs.max_used_threads,
    min_spills = qs.min_spills,
    max_spills = qs.max_spills,
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
WHERE st.text NOT LIKE N'%PerformanceMonitorLite%'
AND   qs.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
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
                    /* Reader ordinals match SELECT column order:
                       0=database_name, 1=query_hash, 2=query_plan_hash,
                       3=creation_time, 4=last_execution_time,
                       5=execution_count, 6=total_worker_time, 7=total_elapsed_time,
                       8=total_logical_reads, 9=total_logical_writes, 10=total_physical_reads,
                       11=total_clr_time, 12=total_rows, 13=total_spills,
                       14=min_worker_time, 15=max_worker_time, 16=min_elapsed_time, 17=max_elapsed_time,
                       18=min_physical_reads, 19=max_physical_reads, 20=min_rows, 21=max_rows,
                       22=min_dop, 23=max_dop,
                       24=min_grant_kb, 25=max_grant_kb, 26=min_used_grant_kb, 27=max_used_grant_kb,
                       28=min_ideal_grant_kb, 29=max_ideal_grant_kb,
                       30=min_reserved_threads, 31=max_reserved_threads, 32=min_used_threads, 33=max_used_threads,
                       34=min_spills, 35=max_spills,
                       36=sql_handle, 37=plan_handle, 38=query_text */
                    var queryHash = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    var creationTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
                    var lastExecTime = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4);
                    var executionCount = reader.IsDBNull(5) ? 0L : reader.GetInt64(5);
                    var totalWorkerTime = reader.IsDBNull(6) ? 0L : reader.GetInt64(6);
                    var totalElapsedTime = reader.IsDBNull(7) ? 0L : reader.GetInt64(7);
                    var totalLogicalReads = reader.IsDBNull(8) ? 0L : reader.GetInt64(8);
                    var totalLogicalWrites = reader.IsDBNull(9) ? 0L : reader.GetInt64(9);
                    var totalPhysicalReads = reader.IsDBNull(10) ? 0L : reader.GetInt64(10);
                    var totalClrTime = reader.IsDBNull(11) ? 0L : reader.GetInt64(11);
                    var totalRows = reader.IsDBNull(12) ? 0L : reader.GetInt64(12);
                    var totalSpills = reader.IsDBNull(13) ? 0L : reader.GetInt64(13);
                    var sqlHandle = reader.IsDBNull(36) ? (string?)null : reader.GetString(36);
                    var planHandle = reader.IsDBNull(37) ? (string?)null : reader.GetString(37);

                    /* Delta calculations keyed by plan_handle to prevent cross-contamination
                       when multiple plans exist for the same query_hash */
                    var deltaKey = planHandle ?? queryHash;
                    var deltaExecCount = _deltaCalculator.CalculateDelta(serverId, "query_stats_exec", deltaKey, executionCount);
                    var deltaWorkerTime = _deltaCalculator.CalculateDelta(serverId, "query_stats_worker", deltaKey, totalWorkerTime);
                    var deltaElapsedTime = _deltaCalculator.CalculateDelta(serverId, "query_stats_elapsed", deltaKey, totalElapsedTime);
                    var deltaLogicalReads = _deltaCalculator.CalculateDelta(serverId, "query_stats_reads", deltaKey, totalLogicalReads);
                    var deltaLogicalWrites = _deltaCalculator.CalculateDelta(serverId, "query_stats_writes", deltaKey, totalLogicalWrites);
                    var deltaPhysicalReads = _deltaCalculator.CalculateDelta(serverId, "query_stats_phys_reads", deltaKey, totalPhysicalReads);
                    var deltaRows = _deltaCalculator.CalculateDelta(serverId, "query_stats_rows", deltaKey, totalRows);
                    var deltaSpills = _deltaCalculator.CalculateDelta(serverId, "query_stats_spills", deltaKey, totalSpills);

                    /* Appender column order must match DuckDB table definition exactly */
                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())                                                 /* collection_id */
                       .AppendValue(collectionTime)                                                         /* collection_time */
                       .AppendValue(serverId)                                                               /* server_id */
                       .AppendValue(server.ServerName)                                                      /* server_name */
                       .AppendValue(reader.IsDBNull(0) ? (string?)null : reader.GetString(0))               /* database_name */
                       .AppendValue(queryHash)                                                              /* query_hash */
                       .AppendValue(reader.IsDBNull(2) ? (string?)null : reader.GetString(2))               /* query_plan_hash */
                       .AppendValue(creationTime)                                                           /* creation_time */
                       .AppendValue(lastExecTime)                                                           /* last_execution_time */
                       .AppendValue(executionCount)                                                         /* execution_count */
                       .AppendValue(totalWorkerTime)                                                        /* total_worker_time */
                       .AppendValue(totalElapsedTime)                                                       /* total_elapsed_time */
                       .AppendValue(totalLogicalReads)                                                      /* total_logical_reads */
                       .AppendValue(totalLogicalWrites)                                                     /* total_logical_writes */
                       .AppendValue(totalPhysicalReads)                                                     /* total_physical_reads */
                       .AppendValue(totalClrTime)                                                           /* total_clr_time */
                       .AppendValue(totalRows)                                                              /* total_rows */
                       .AppendValue(totalSpills)                                                            /* total_spills */
                       .AppendValue(reader.IsDBNull(14) ? 0L : reader.GetInt64(14))                         /* min_worker_time */
                       .AppendValue(reader.IsDBNull(15) ? 0L : reader.GetInt64(15))                         /* max_worker_time */
                       .AppendValue(reader.IsDBNull(16) ? 0L : reader.GetInt64(16))                         /* min_elapsed_time */
                       .AppendValue(reader.IsDBNull(17) ? 0L : reader.GetInt64(17))                         /* max_elapsed_time */
                       .AppendValue(reader.IsDBNull(18) ? 0L : reader.GetInt64(18))                         /* min_physical_reads */
                       .AppendValue(reader.IsDBNull(19) ? 0L : reader.GetInt64(19))                         /* max_physical_reads */
                       .AppendValue(reader.IsDBNull(20) ? 0L : reader.GetInt64(20))                         /* min_rows */
                       .AppendValue(reader.IsDBNull(21) ? 0L : reader.GetInt64(21))                         /* max_rows */
                       .AppendValue(reader.IsDBNull(22) ? 0 : Convert.ToInt32(reader.GetValue(22)))         /* min_dop */
                       .AppendValue(reader.IsDBNull(23) ? 0 : Convert.ToInt32(reader.GetValue(23)))         /* max_dop */
                       .AppendValue(reader.IsDBNull(24) ? 0L : reader.GetInt64(24))                         /* min_grant_kb */
                       .AppendValue(reader.IsDBNull(25) ? 0L : reader.GetInt64(25))                         /* max_grant_kb */
                       .AppendValue(reader.IsDBNull(26) ? 0L : reader.GetInt64(26))                         /* min_used_grant_kb */
                       .AppendValue(reader.IsDBNull(27) ? 0L : reader.GetInt64(27))                         /* max_used_grant_kb */
                       .AppendValue(reader.IsDBNull(28) ? 0L : reader.GetInt64(28))                         /* min_ideal_grant_kb */
                       .AppendValue(reader.IsDBNull(29) ? 0L : reader.GetInt64(29))                         /* max_ideal_grant_kb */
                       .AppendValue(reader.IsDBNull(30) ? 0L : reader.GetInt64(30))                         /* min_reserved_threads */
                       .AppendValue(reader.IsDBNull(31) ? 0L : reader.GetInt64(31))                         /* max_reserved_threads */
                       .AppendValue(reader.IsDBNull(32) ? 0L : reader.GetInt64(32))                         /* min_used_threads */
                       .AppendValue(reader.IsDBNull(33) ? 0L : reader.GetInt64(33))                         /* max_used_threads */
                       .AppendValue(reader.IsDBNull(34) ? 0L : reader.GetInt64(34))                         /* min_spills */
                       .AppendValue(reader.IsDBNull(35) ? 0L : reader.GetInt64(35))                         /* max_spills */
                       .AppendValue(reader.IsDBNull(38) ? (string?)null : reader.GetString(38))             /* query_text */
                       .AppendValue((string?)null)                                                          /* query_plan_xml — retrieved on-demand */
                       .AppendValue(sqlHandle)                                                              /* sql_handle */
                       .AppendValue(planHandle)                                                             /* plan_handle */
                       .AppendValue(deltaExecCount)                                                         /* delta_execution_count */
                       .AppendValue(deltaWorkerTime)                                                        /* delta_worker_time */
                       .AppendValue(deltaElapsedTime)                                                       /* delta_elapsed_time */
                       .AppendValue(deltaLogicalReads)                                                      /* delta_logical_reads */
                       .AppendValue(deltaLogicalWrites)                                                     /* delta_logical_writes */
                       .AppendValue(deltaPhysicalReads)                                                     /* delta_physical_reads */
                       .AppendValue(deltaRows)                                                              /* delta_rows */
                       .AppendValue(deltaSpills)                                                            /* delta_spills */
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
