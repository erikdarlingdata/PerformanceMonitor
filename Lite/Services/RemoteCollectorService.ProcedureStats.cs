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

        /* total_spills exists in dm_exec_procedure_stats and dm_exec_trigger_stats on all supported versions,
           but does NOT exist in dm_exec_function_stats on any version. Use dynamic SQL to handle this. */
        const string standardQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @spills_col nvarchar(200) = N'total_spills = ISNULL(s.total_spills, 0),',
    @fn_spills_col nvarchar(200) = N'total_spills = CONVERT(bigint, 0),',
    @sql nvarchar(max);

SET @sql = CAST(N'
SELECT TOP (150) * FROM (
SELECT
    database_name = d.name,
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N''PROCEDURE'',
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
    ' AS nvarchar(max)) + @spills_col + N'
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
LEFT JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))

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
    ' + @spills_col + CAST(N'
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
LEFT JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))

UNION ALL

SELECT
    database_name = d.name,
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N''FUNCTION'',
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
    ' AS nvarchar(max)) + @fn_spills_col + CAST(N'
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
LEFT JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
) AS combined
ORDER BY total_elapsed_time DESC
OPTION(RECOMPILE);' AS nvarchar(max));

EXECUTE sys.sp_executesql @sql;";

        /* Azure SQL DB: skip plan_attributes (reports dbid=1 for all plans), use DB_NAME() directly.
           No trigger stats or function stats — Azure SQL DB scope is single-database. */
        const string azureSqlDbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (150)
    database_name = DB_NAME(),
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N'PROCEDURE',
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
    total_spills = ISNULL(s.total_spills, 0),
    sql_handle = CONVERT(varchar(64), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(64), s.plan_handle, 1)
FROM sys.dm_exec_procedure_stats AS s
WHERE s.database_id = DB_ID()
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
                    var dbName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                    var schemaName = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    var objectName = reader.IsDBNull(2) ? "" : reader.GetString(2);
                    var objectType = reader.IsDBNull(3) ? "" : reader.GetString(3);
                    var execCount = reader.GetInt64(4);
                    var workerTime = reader.GetInt64(5);
                    var elapsedTime = reader.GetInt64(6);
                    var logicalReads = reader.GetInt64(7);
                    var physicalReads = reader.GetInt64(8);
                    var logicalWrites = reader.GetInt64(9);
                    var minWorkerTime = reader.GetInt64(10);
                    var maxWorkerTime = reader.GetInt64(11);
                    var minElapsedTime = reader.GetInt64(12);
                    var maxElapsedTime = reader.GetInt64(13);
                    var totalSpills = reader.GetInt64(14);
                    var sqlHandle = reader.IsDBNull(15) ? (string?)null : reader.GetString(15);
                    var planHandle = reader.IsDBNull(16) ? (string?)null : reader.GetString(16);

                    /* Delta key: database.schema.object */
                    var deltaKey = $"{dbName}.{schemaName}.{objectName}";
                    var deltaExec = _deltaCalculator.CalculateDelta(serverId, "proc_stats_exec", deltaKey, execCount);
                    var deltaWorker = _deltaCalculator.CalculateDelta(serverId, "proc_stats_worker", deltaKey, workerTime);
                    var deltaElapsed = _deltaCalculator.CalculateDelta(serverId, "proc_stats_elapsed", deltaKey, elapsedTime);
                    var deltaReads = _deltaCalculator.CalculateDelta(serverId, "proc_stats_reads", deltaKey, logicalReads);
                    var deltaWrites = _deltaCalculator.CalculateDelta(serverId, "proc_stats_writes", deltaKey, logicalWrites);
                    var deltaPhysReads = _deltaCalculator.CalculateDelta(serverId, "proc_stats_phys_reads", deltaKey, physicalReads);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(dbName)
                       .AppendValue(schemaName)
                       .AppendValue(objectName)
                       .AppendValue(objectType)
                       .AppendValue(execCount)
                       .AppendValue(workerTime)
                       .AppendValue(elapsedTime)
                       .AppendValue(logicalReads)
                       .AppendValue(physicalReads)
                       .AppendValue(logicalWrites)
                       .AppendValue(minWorkerTime)
                       .AppendValue(maxWorkerTime)
                       .AppendValue(minElapsedTime)
                       .AppendValue(maxElapsedTime)
                       .AppendValue(totalSpills)
                       .AppendValue(sqlHandle)
                       .AppendValue(planHandle)
                       .AppendValue(deltaExec)
                       .AppendValue(deltaWorker)
                       .AppendValue(deltaElapsed)
                       .AppendValue(deltaReads)
                       .AppendValue(deltaWrites)
                       .AppendValue(deltaPhysReads)
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
