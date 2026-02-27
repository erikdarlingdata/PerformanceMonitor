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
    /// Collects Query Store data from databases that have it enabled.
    /// Matches Dashboard column parity with version-gated columns for SQL 2017+ and 2022+.
    /// </summary>
    private async Task<int> CollectQueryStoreAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        /* First, get databases with Query Store actually enabled.
           Uses sys.database_query_store_options.actual_state instead of
           sys.databases.is_query_store_on, which can be out of sync on Azure SQL DB. */
        const string dbQuery = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @result TABLE (name sysname);

DECLARE
    @db sysname,
    @sql NVARCHAR(500);

DECLARE db_check CURSOR LOCAL FAST_FORWARD FOR
    SELECT /* PerformanceMonitorLite */
        d.name
    FROM sys.databases AS d
    WHERE d.database_id > 4
    AND   d.database_id < 32761
    AND   d.state_desc = N'ONLINE'
    AND   d.name <> N'PerformanceMonitor'
    AND   d.database_id NOT IN
          (
              SELECT
                  d2.database_id
              FROM sys.databases AS d2
              JOIN sys.availability_replicas AS r
                ON d2.replica_id = r.replica_id
              WHERE NOT EXISTS
                    (
                        SELECT
                            1/0
                        FROM sys.dm_hadr_availability_group_states AS s
                        WHERE s.primary_replica = r.replica_server_name
                    )
              AND   r.secondary_role_allow_connections_desc = N'READ_ONLY'
              AND   r.replica_server_name = @@SERVERNAME
          )
    OPTION(RECOMPILE);

OPEN db_check;

FETCH NEXT
FROM db_check
INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        SET @sql =
            N'USE ' + QUOTENAME(@db) + N';
            SELECT ' + QUOTENAME(@db, '''') + N'
            WHERE EXISTS
            (
                SELECT
                    1
                FROM sys.database_query_store_options
                WHERE actual_state > 0
            );';

        INSERT @result (name)
        EXEC(@sql);
    END TRY
    BEGIN CATCH
    END CATCH;

    FETCH NEXT
    FROM db_check
    INTO @db;
END;

CLOSE db_check;
DEALLOCATE db_check;

SELECT
    name
FROM @result
ORDER BY
    name;";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var totalRows = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        /* Incremental: only fetch runtime_stats intervals newer than what we already have */
        var lastCollectedTime = await GetLastCollectedTimeAsync(
            serverId, "query_store_stats", "last_execution_time", cancellationToken);
        var cutoffTime = lastCollectedTime ?? DateTime.UtcNow.AddMinutes(-60);

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);

        /* Get list of QS-enabled databases */
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
            sqlSw.Stop();
            _lastSqlMs = sqlSw.ElapsedMilliseconds;
            return 0;
        }

        /* Detect server version for version-gated columns.
           @isNew = true for SQL Server 2017+ (product version > 13) or Azure SQL DB/MI (engine 5/8).
           Controls: avg_num_physical_io_reads, avg_log_bytes_used, avg_tempdb_space_used, plan_forcing_type_desc.
           @hasPlanType = true for SQL Server 2022+ (product version >= 16).
           Controls: plan_type_desc. */
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        int productVersion = 13; /* default to SQL 2016 */
        try
        {
            using var versionCmd = new SqlCommand(
                "SELECT CONVERT(integer, PARSENAME(CONVERT(sysname, SERVERPROPERTY('PRODUCTVERSION')), 4))",
                sqlConnection);
            versionCmd.CommandTimeout = 10;
            var versionResult = await versionCmd.ExecuteScalarAsync(cancellationToken);
            if (versionResult != null && versionResult != DBNull.Value)
                productVersion = Convert.ToInt32(versionResult);
        }
        catch
        {
            /* Fall back to 13 (SQL 2016) if version detection fails */
        }

        bool isNew = productVersion > 13 || serverStatus.SqlEngineEdition == 5 || serverStatus.SqlEngineEdition == 8;
        bool hasPlanType = productVersion >= 16;

        /* Build version-conditional column fragments for the Query Store query.
           These are injected into the sp_executesql parameter string — no single quotes needed. */
        string numPhysIoReadsCols = isNew
            ? "qsrs.avg_num_physical_io_reads, qsrs.min_num_physical_io_reads, qsrs.max_num_physical_io_reads,"
            : "avg_num_physical_io_reads = NULL, min_num_physical_io_reads = NULL, max_num_physical_io_reads = NULL,";

        string logBytesCols = isNew
            ? "avg_log_bytes_used = qsrs.avg_log_bytes_used, min_log_bytes_used = qsrs.min_log_bytes_used, max_log_bytes_used = qsrs.max_log_bytes_used,"
            : "avg_log_bytes_used = NULL, min_log_bytes_used = NULL, max_log_bytes_used = NULL,";

        string tempdbCols = isNew
            ? "avg_tempdb_space_used = qsrs.avg_tempdb_space_used, min_tempdb_space_used = qsrs.min_tempdb_space_used, max_tempdb_space_used = qsrs.max_tempdb_space_used,"
            : "avg_tempdb_space_used = NULL, min_tempdb_space_used = NULL, max_tempdb_space_used = NULL,";

        string planForcingCol = isNew
            ? "plan_forcing_type = qsp.plan_forcing_type_desc,"
            : "plan_forcing_type = NULL,";

        string planTypeCol = hasPlanType
            ? "plan_type = qsp.plan_type_desc,"
            : "plan_type = NULL,";

        var duckSw = new Stopwatch();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            /* For each database, collect new query store intervals since last collection */
            foreach (var dbName in databases)
            {
                try
                {
                    var escapedDbName = dbName.Replace("]", "]]");
                    var qsQuery = $@"
EXECUTE [{escapedDbName}].sys.sp_executesql
    N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

     SELECT /* PerformanceMonitorLite */
         query_id = qsq.query_id,
         plan_id = qsp.plan_id,
         execution_type_desc = qsrs.execution_type_desc,
         first_execution_time = qsrs.first_execution_time,
         last_execution_time = qsrs.last_execution_time,
         module_name =
             CASE
                 WHEN qsq.object_id = 0
                 THEN N''Adhoc''
                 ELSE COALESCE(
                     OBJECT_SCHEMA_NAME(qsq.object_id) + N''.'' + OBJECT_NAME(qsq.object_id),
                     N''Unknown'')
             END,
         query_sql_text = qst.query_sql_text,
         query_hash = CONVERT(varchar(64), qsq.query_hash, 1),
         count_executions = qsrs.count_executions,
         avg_duration = qsrs.avg_duration,
         min_duration = qsrs.min_duration,
         max_duration = qsrs.max_duration,
         avg_cpu_time = qsrs.avg_cpu_time,
         min_cpu_time = qsrs.min_cpu_time,
         max_cpu_time = qsrs.max_cpu_time,
         avg_logical_io_reads = qsrs.avg_logical_io_reads,
         min_logical_io_reads = qsrs.min_logical_io_reads,
         max_logical_io_reads = qsrs.max_logical_io_reads,
         avg_logical_io_writes = qsrs.avg_logical_io_writes,
         min_logical_io_writes = qsrs.min_logical_io_writes,
         max_logical_io_writes = qsrs.max_logical_io_writes,
         avg_physical_io_reads = qsrs.avg_physical_io_reads,
         min_physical_io_reads = qsrs.min_physical_io_reads,
         max_physical_io_reads = qsrs.max_physical_io_reads,
         avg_clr_time = qsrs.avg_clr_time,
         min_clr_time = qsrs.min_clr_time,
         max_clr_time = qsrs.max_clr_time,
         min_dop = qsrs.min_dop,
         max_dop = qsrs.max_dop,
         avg_query_max_used_memory = qsrs.avg_query_max_used_memory,
         min_query_max_used_memory = qsrs.min_query_max_used_memory,
         max_query_max_used_memory = qsrs.max_query_max_used_memory,
         avg_rowcount = qsrs.avg_rowcount,
         min_rowcount = qsrs.min_rowcount,
         max_rowcount = qsrs.max_rowcount,
         {numPhysIoReadsCols}
         {logBytesCols}
         {tempdbCols}
         {planTypeCol}
         {planForcingCol}
         is_forced_plan = qsp.is_forced_plan,
         force_failure_count = qsp.force_failure_count,
         last_force_failure_reason = qsp.last_force_failure_reason_desc,
         compatibility_level = qsp.compatibility_level,
         query_plan_text = CONVERT(nvarchar(max), qsp.query_plan),
         query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1)
     FROM sys.query_store_runtime_stats AS qsrs
     JOIN sys.query_store_plan AS qsp
       ON qsp.plan_id = qsrs.plan_id
     JOIN sys.query_store_query AS qsq
       ON qsq.query_id = qsp.query_id
     JOIN sys.query_store_query_text AS qst
       ON qst.query_text_id = qsq.query_text_id
     WHERE qsrs.last_execution_time > @cutoff_time
     AND   qst.query_sql_text NOT LIKE N''%PerformanceMonitorLite%''
     OPTION(RECOMPILE);',
    N'@cutoff_time datetime2(7)',
    @cutoff_time;";

                    sqlSw.Start();
                    using var qsCommand = new SqlCommand(qsQuery, sqlConnection);
                    qsCommand.CommandTimeout = CommandTimeoutSeconds;
                    qsCommand.Parameters.Add(new SqlParameter("@cutoff_time", System.Data.SqlDbType.DateTime2) { Value = cutoffTime });

                    using var reader = await qsCommand.ExecuteReaderAsync(cancellationToken);
                    sqlSw.Stop();

                    duckSw.Start();

                    using (var appender = duckConnection.CreateAppender("query_store_stats"))
                    {
                        while (await reader.ReadAsync(cancellationToken))
                        {
                            /* Reader ordinals match SELECT column order:
                               0=query_id, 1=plan_id, 2=execution_type_desc,
                               3=first_execution_time (dto), 4=last_execution_time (dto),
                               5=module_name, 6=query_sql_text, 7=query_hash, 8=count_executions,
                               9=avg_duration, 10=min_duration, 11=max_duration,
                               12=avg_cpu_time, 13=min_cpu_time, 14=max_cpu_time,
                               15=avg_logical_io_reads, 16=min_logical_io_reads, 17=max_logical_io_reads,
                               18=avg_logical_io_writes, 19=min_logical_io_writes, 20=max_logical_io_writes,
                               21=avg_physical_io_reads, 22=min_physical_io_reads, 23=max_physical_io_reads,
                               24=avg_clr_time, 25=min_clr_time, 26=max_clr_time,
                               27=min_dop, 28=max_dop,
                               29=avg_query_max_used_memory, 30=min_query_max_used_memory, 31=max_query_max_used_memory,
                               32=avg_rowcount, 33=min_rowcount, 34=max_rowcount,
                               35=avg_num_physical_io_reads, 36=min_num_physical_io_reads, 37=max_num_physical_io_reads,
                               38=avg_log_bytes_used, 39=min_log_bytes_used, 40=max_log_bytes_used,
                               41=avg_tempdb_space_used, 42=min_tempdb_space_used, 43=max_tempdb_space_used,
                               44=plan_type, 45=plan_forcing_type,
                               46=is_forced_plan, 47=force_failure_count, 48=last_force_failure_reason,
                               49=compatibility_level, 50=query_plan_text, 51=query_plan_hash */

                            var row = appender.CreateRow();
                            row.AppendValue(GenerateCollectionId())                                                             /* collection_id */
                               .AppendValue(collectionTime)                                                                     /* collection_time */
                               .AppendValue(serverId)                                                                           /* server_id */
                               .AppendValue(server.ServerName)                                                                  /* server_name */
                               .AppendValue(dbName)                                                                             /* database_name */
                               .AppendValue(reader.GetInt64(0))                                                                 /* query_id */
                               .AppendValue(reader.GetInt64(1))                                                                 /* plan_id */
                               .AppendValue(reader.IsDBNull(2) ? (string?)null : reader.GetString(2))                           /* execution_type_desc */
                               .AppendValue(reader.IsDBNull(3) ? (DateTime?)null : ((DateTimeOffset)reader.GetValue(3)).UtcDateTime) /* first_execution_time */
                               .AppendValue(reader.IsDBNull(4) ? (DateTime?)null : ((DateTimeOffset)reader.GetValue(4)).UtcDateTime) /* last_execution_time */
                               .AppendValue(reader.IsDBNull(5) ? (string?)null : reader.GetString(5))                           /* module_name */
                               .AppendValue(reader.IsDBNull(6) ? (string?)null : reader.GetString(6))                           /* query_text */
                               .AppendValue(reader.IsDBNull(7) ? (string?)null : reader.GetString(7))                           /* query_hash */
                               .AppendValue(reader.GetInt64(8))                                                                 /* execution_count */
                               .AppendValue(ReadNullableInt64(reader, 9))                                                       /* avg_duration_us */
                               .AppendValue(ReadNullableInt64(reader, 10))                                                      /* min_duration_us */
                               .AppendValue(ReadNullableInt64(reader, 11))                                                      /* max_duration_us */
                               .AppendValue(ReadNullableInt64(reader, 12))                                                      /* avg_cpu_time_us */
                               .AppendValue(ReadNullableInt64(reader, 13))                                                      /* min_cpu_time_us */
                               .AppendValue(ReadNullableInt64(reader, 14))                                                      /* max_cpu_time_us */
                               .AppendValue(ReadNullableInt64(reader, 15))                                                      /* avg_logical_io_reads */
                               .AppendValue(ReadNullableInt64(reader, 16))                                                      /* min_logical_io_reads */
                               .AppendValue(ReadNullableInt64(reader, 17))                                                      /* max_logical_io_reads */
                               .AppendValue(ReadNullableInt64(reader, 18))                                                      /* avg_logical_io_writes */
                               .AppendValue(ReadNullableInt64(reader, 19))                                                      /* min_logical_io_writes */
                               .AppendValue(ReadNullableInt64(reader, 20))                                                      /* max_logical_io_writes */
                               .AppendValue(ReadNullableInt64(reader, 21))                                                      /* avg_physical_io_reads */
                               .AppendValue(ReadNullableInt64(reader, 22))                                                      /* min_physical_io_reads */
                               .AppendValue(ReadNullableInt64(reader, 23))                                                      /* max_physical_io_reads */
                               .AppendValue(ReadNullableInt64(reader, 24))                                                      /* avg_clr_time_us */
                               .AppendValue(ReadNullableInt64(reader, 25))                                                      /* min_clr_time_us */
                               .AppendValue(ReadNullableInt64(reader, 26))                                                      /* max_clr_time_us */
                               .AppendValue(ReadNullableInt64(reader, 27))                                                      /* min_dop */
                               .AppendValue(ReadNullableInt64(reader, 28))                                                      /* max_dop */
                               .AppendValue(ReadNullableInt64(reader, 29))                                                      /* avg_query_max_used_memory */
                               .AppendValue(ReadNullableInt64(reader, 30))                                                      /* min_query_max_used_memory */
                               .AppendValue(ReadNullableInt64(reader, 31))                                                      /* max_query_max_used_memory */
                               .AppendValue(ReadNullableInt64(reader, 32))                                                      /* avg_rowcount */
                               .AppendValue(ReadNullableInt64(reader, 33))                                                      /* min_rowcount */
                               .AppendValue(ReadNullableInt64(reader, 34))                                                      /* max_rowcount */
                               .AppendValue(ReadNullableInt64(reader, 35))                                                      /* avg_num_physical_io_reads (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 36))                                                      /* min_num_physical_io_reads (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 37))                                                      /* max_num_physical_io_reads (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 38))                                                      /* avg_log_bytes_used (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 39))                                                      /* min_log_bytes_used (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 40))                                                      /* max_log_bytes_used (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 41))                                                      /* avg_tempdb_space_used (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 42))                                                      /* min_tempdb_space_used (2017+) */
                               .AppendValue(ReadNullableInt64(reader, 43))                                                      /* max_tempdb_space_used (2017+) */
                               .AppendValue(reader.IsDBNull(44) ? (string?)null : reader.GetString(44))                         /* plan_type (2022+) */
                               .AppendValue(reader.IsDBNull(45) ? (string?)null : reader.GetString(45))                         /* plan_forcing_type (2017+) */
                               .AppendValue(!reader.IsDBNull(46) && reader.GetBoolean(46))                                      /* is_forced_plan */
                               .AppendValue(reader.IsDBNull(47) ? 0L : reader.GetInt64(47))                                     /* force_failure_count */
                               .AppendValue(reader.IsDBNull(48) ? (string?)null : reader.GetString(48))                         /* last_force_failure_reason */
                               .AppendValue(reader.IsDBNull(49) ? 0 : Convert.ToInt32(reader.GetValue(49)))                     /* compatibility_level */
                               .AppendValue(reader.IsDBNull(50) ? (string?)null : reader.GetString(50))                         /* query_plan_text */
                               .AppendValue(reader.IsDBNull(51) ? (string?)null : reader.GetString(51))                         /* query_plan_hash */
                               .EndRow();

                            totalRows++;
                        }
                    }

                    duckSw.Stop();
                }
                catch (SqlException ex)
                {
                    sqlSw.Stop();
                    duckSw.Stop();
                    _logger?.LogWarning("Failed to collect Query Store data from [{Database}] on '{Server}': {Message}",
                        dbName, server.DisplayName, ex.Message);
                }
            }
        }

        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} Query Store rows across {DbCount} databases for server '{Server}'",
            totalRows, databases.Count, server.DisplayName);
        return totalRows;
    }

    /// <summary>
    /// Helper to read a nullable int64 from a SqlDataReader, converting float/decimal Query Store values to long.
    /// Query Store runtime_stats columns are stored as float in the catalog but represent integer-scale values.
    /// </summary>
    private static long ReadNullableInt64(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return 0L;
        var value = reader.GetValue(ordinal);
        return value switch
        {
            long l => l,
            int i => i,
            short s => s,
            decimal d => (long)d,
            double dbl => (long)dbl,
            float f => (long)f,
            _ => Convert.ToInt64(value)
        };
    }
}
