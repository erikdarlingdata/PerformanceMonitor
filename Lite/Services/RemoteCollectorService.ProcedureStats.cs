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
    /// Collects procedure statistics from sys.dm_exec_procedure_stats.
    /// </summary>
    private async Task<int> CollectProcedureStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        /* On Azure SQL DB, dm_exec_plan_attributes reports dbid=1 (master) for ALL plans,
           so the standard NOT IN filter excludes everything. Use a simplified query. */
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        /* total_spills/min_spills/max_spills exist in dm_exec_procedure_stats and dm_exec_trigger_stats
           on all supported versions, but do NOT exist in dm_exec_function_stats on any version.
           Use dynamic SQL to handle this. */
        const string standardQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @spills_cols nvarchar(400) = N'total_spills = ISNULL(s.total_spills, 0), min_spills = ISNULL(s.min_spills, 0), max_spills = ISNULL(s.max_spills, 0),',
    @fn_spills_cols nvarchar(400) = N'total_spills = CONVERT(bigint, 0), min_spills = CONVERT(bigint, 0), max_spills = CONVERT(bigint, 0),',
    @sql nvarchar(max);

SET @sql = CAST(N'
SELECT /* PerformanceMonitorLite */ TOP (150) * FROM (
SELECT
    database_name = d.name,
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N''PROCEDURE'',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    ' AS nvarchar(max)) + @spills_cols + N'
    sql_handle = CONVERT(varchar(64), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), s.plan_handle, 1)
FROM sys.dm_exec_procedure_stats AS s
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(s.plan_handle) AS pa
    WHERE pa.attribute = N''dbid''
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE d.state = 0
AND   pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())

UNION ALL

SELECT
    database_name = d.name,
    schema_name = ISNULL(OBJECT_SCHEMA_NAME(s.object_id, s.database_id), N''dbo''),
    object_name = COALESCE(
        OBJECT_NAME(s.object_id, s.database_id),
        CASE
            WHEN CHARINDEX(N''CREATE TRIGGER'', st.text) > 0
            THEN LTRIM(RTRIM(REPLACE(REPLACE(
                SUBSTRING(
                    st.text,
                    CHARINDEX(N''CREATE TRIGGER'', st.text) + 15,
                    CHARINDEX(N'' ON '', st.text + N'' ON '') - CHARINDEX(N''CREATE TRIGGER'', st.text) - 15
                ), N''['', N''''), N'']'', N'''')))
            ELSE N''trigger_'' + CONVERT(nvarchar(20), s.object_id)
        END
    ),
    object_type = N''TRIGGER'',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    ' + @spills_cols + CAST(N'
    sql_handle = CONVERT(varchar(64), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), s.plan_handle, 1)
FROM sys.dm_exec_trigger_stats AS s
CROSS APPLY sys.dm_exec_sql_text(s.sql_handle) AS st
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(s.plan_handle) AS pa
    WHERE pa.attribute = N''dbid''
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE d.state = 0
AND   pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())

UNION ALL

SELECT
    database_name = d.name,
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N''FUNCTION'',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    ' AS nvarchar(max)) + @fn_spills_cols + CAST(N'
    sql_handle = CONVERT(varchar(64), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), s.plan_handle, 1)
FROM sys.dm_exec_function_stats AS s
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(s.plan_handle) AS pa
    WHERE pa.attribute = N''dbid''
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE d.state = 0
AND   pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
) AS combined
ORDER BY total_elapsed_time DESC
OPTION(RECOMPILE);' AS nvarchar(max));

EXECUTE sys.sp_executesql @sql;";

        /* Azure SQL DB: skip plan_attributes (reports dbid=1 for all plans), use DB_NAME() directly.
           No trigger stats or function stats — Azure SQL DB scope is single-database. */
        const string azureSqlDbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */ TOP (150)
    database_name = DB_NAME(),
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N'PROCEDURE',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    total_spills = ISNULL(s.total_spills, 0),
    min_spills = ISNULL(s.min_spills, 0),
    max_spills = ISNULL(s.max_spills, 0),
    sql_handle = CONVERT(varchar(64), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), s.plan_handle, 1)
FROM sys.dm_exec_procedure_stats AS s
WHERE s.database_id = DB_ID()
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
ORDER BY s.total_elapsed_time DESC
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

            using (var appender = duckConnection.CreateAppender("procedure_stats"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    /* Reader ordinals match SELECT column order:
                       0=database_name, 1=schema_name, 2=object_name, 3=object_type,
                       4=cached_time, 5=last_execution_time,
                       6=execution_count, 7=total_worker_time, 8=total_elapsed_time,
                       9=total_logical_reads, 10=total_physical_reads, 11=total_logical_writes,
                       12=min_worker_time, 13=max_worker_time, 14=min_elapsed_time, 15=max_elapsed_time,
                       16=min_logical_reads, 17=max_logical_reads, 18=min_physical_reads, 19=max_physical_reads,
                       20=min_logical_writes, 21=max_logical_writes,
                       22=total_spills, 23=min_spills, 24=max_spills,
                       25=sql_handle, 26=plan_handle */
                    var dbName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                    var schemaName = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    var objectName = reader.IsDBNull(2) ? "" : reader.GetString(2);
                    var objectType = reader.IsDBNull(3) ? "" : reader.GetString(3);
                    var cachedTime = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4);
                    var lastExecTime = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
                    var execCount = reader.GetInt64(6);
                    var workerTime = reader.GetInt64(7);
                    var elapsedTime = reader.GetInt64(8);
                    var logicalReads = reader.GetInt64(9);
                    var physicalReads = reader.GetInt64(10);
                    var logicalWrites = reader.GetInt64(11);
                    var sqlHandle = reader.IsDBNull(25) ? (string?)null : reader.GetString(25);
                    var planHandle = reader.IsDBNull(26) ? (string?)null : reader.GetString(26);

                    /* Delta key: plan_handle to prevent cross-contamination
                       when multiple plans exist for the same object */
                    var deltaKey = planHandle ?? $"{dbName}.{schemaName}.{objectName}";
                    var deltaExec = _deltaCalculator.CalculateDelta(serverId, "proc_stats_exec", deltaKey, execCount);
                    var deltaWorker = _deltaCalculator.CalculateDelta(serverId, "proc_stats_worker", deltaKey, workerTime);
                    var deltaElapsed = _deltaCalculator.CalculateDelta(serverId, "proc_stats_elapsed", deltaKey, elapsedTime);
                    var deltaReads = _deltaCalculator.CalculateDelta(serverId, "proc_stats_reads", deltaKey, logicalReads);
                    var deltaWrites = _deltaCalculator.CalculateDelta(serverId, "proc_stats_writes", deltaKey, logicalWrites);
                    var deltaPhysReads = _deltaCalculator.CalculateDelta(serverId, "proc_stats_phys_reads", deltaKey, physicalReads);

                    /* Appender column order must match DuckDB table definition exactly */
                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())                                     /* collection_id */
                       .AppendValue(collectionTime)                                             /* collection_time */
                       .AppendValue(serverId)                                                   /* server_id */
                       .AppendValue(server.ServerName)                                          /* server_name */
                       .AppendValue(dbName)                                                     /* database_name */
                       .AppendValue(schemaName)                                                 /* schema_name */
                       .AppendValue(objectName)                                                 /* object_name */
                       .AppendValue(objectType)                                                 /* object_type */
                       .AppendValue(cachedTime)                                                 /* cached_time */
                       .AppendValue(lastExecTime)                                               /* last_execution_time */
                       .AppendValue(execCount)                                                  /* execution_count */
                       .AppendValue(workerTime)                                                 /* total_worker_time */
                       .AppendValue(elapsedTime)                                                /* total_elapsed_time */
                       .AppendValue(logicalReads)                                               /* total_logical_reads */
                       .AppendValue(physicalReads)                                              /* total_physical_reads */
                       .AppendValue(logicalWrites)                                              /* total_logical_writes */
                       .AppendValue(reader.IsDBNull(12) ? 0L : reader.GetInt64(12))             /* min_worker_time */
                       .AppendValue(reader.IsDBNull(13) ? 0L : reader.GetInt64(13))             /* max_worker_time */
                       .AppendValue(reader.IsDBNull(14) ? 0L : reader.GetInt64(14))             /* min_elapsed_time */
                       .AppendValue(reader.IsDBNull(15) ? 0L : reader.GetInt64(15))             /* max_elapsed_time */
                       .AppendValue(reader.IsDBNull(16) ? 0L : reader.GetInt64(16))             /* min_logical_reads */
                       .AppendValue(reader.IsDBNull(17) ? 0L : reader.GetInt64(17))             /* max_logical_reads */
                       .AppendValue(reader.IsDBNull(18) ? 0L : reader.GetInt64(18))             /* min_physical_reads */
                       .AppendValue(reader.IsDBNull(19) ? 0L : reader.GetInt64(19))             /* max_physical_reads */
                       .AppendValue(reader.IsDBNull(20) ? 0L : reader.GetInt64(20))             /* min_logical_writes */
                       .AppendValue(reader.IsDBNull(21) ? 0L : reader.GetInt64(21))             /* max_logical_writes */
                       .AppendValue(reader.GetInt64(22))                                        /* total_spills */
                       .AppendValue(reader.GetInt64(23))                                        /* min_spills */
                       .AppendValue(reader.GetInt64(24))                                        /* max_spills */
                       .AppendValue(sqlHandle)                                                  /* sql_handle */
                       .AppendValue(planHandle)                                                 /* plan_handle */
                       .AppendValue(deltaExec)                                                  /* delta_execution_count */
                       .AppendValue(deltaWorker)                                                /* delta_worker_time */
                       .AppendValue(deltaElapsed)                                               /* delta_elapsed_time */
                       .AppendValue(deltaReads)                                                 /* delta_logical_reads */
                       .AppendValue(deltaWrites)                                                /* delta_logical_writes */
                       .AppendValue(deltaPhysReads)                                             /* delta_physical_reads */
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} procedure stats for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
