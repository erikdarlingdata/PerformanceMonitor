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
    /// </summary>
    private async Task<int> CollectQueryStoreAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        /* First, get databases with Query Store enabled */
        const string dbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name
FROM sys.databases AS d
WHERE d.is_query_store_on = 1
AND   d.database_id > 4
AND   d.database_id < 32761
AND   d.state_desc = N'ONLINE'
AND   d.name <> N'PerformanceMonitor'
ORDER BY d.name
OPTION(RECOMPILE);";

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

        var duckSw = new Stopwatch();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            /* For each database, collect new query store intervals since last collection */
            foreach (var dbName in databases)
            {
                try
                {
                    var qsQuery = $@"
EXECUTE [{dbName.Replace("]", "]]")}].sys.sp_executesql
    N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

     SELECT
         query_id = qsq.query_id,
         plan_id = qsp.plan_id,
         query_text = qst.query_sql_text,
         query_hash = CONVERT(varchar(64), qsq.query_hash, 1),
         execution_count = qsrs.count_executions,
         avg_duration_ms = CONVERT(decimal(18,2), qsrs.avg_duration / 1000.0),
         avg_cpu_time_ms = CONVERT(decimal(18,2), qsrs.avg_cpu_time / 1000.0),
         avg_logical_reads = CONVERT(decimal(18,2), qsrs.avg_logical_io_reads),
         avg_logical_writes = CONVERT(decimal(18,2), qsrs.avg_logical_io_writes),
         avg_physical_reads = CONVERT(decimal(18,2), qsrs.avg_physical_io_reads),
         avg_rowcount = CONVERT(decimal(18,2), qsrs.avg_rowcount),
         last_execution_time = qsrs.last_execution_time,
         query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1)
     FROM sys.query_store_runtime_stats AS qsrs
     JOIN sys.query_store_plan AS qsp
       ON qsp.plan_id = qsrs.plan_id
     JOIN sys.query_store_query AS qsq
       ON qsq.query_id = qsp.query_id
     JOIN sys.query_store_query_text AS qst
       ON qst.query_text_id = qsq.query_text_id
     WHERE qsrs.last_execution_time > @cutoff_time
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
                            var row = appender.CreateRow();
                            row.AppendValue(GenerateCollectionId())
                               .AppendValue(collectionTime)
                               .AppendValue(serverId)
                               .AppendValue(server.ServerName)
                               .AppendValue(dbName)
                               .AppendValue(reader.GetInt64(0))                                                         /* query_id */
                               .AppendValue(reader.GetInt64(1))                                                         /* plan_id */
                               .AppendValue(reader.IsDBNull(2) ? (string?)null : reader.GetString(2))                   /* query_text */
                               .AppendValue(reader.IsDBNull(3) ? (string?)null : reader.GetString(3))                   /* query_hash */
                               .AppendValue(reader.GetInt64(4))                                                         /* execution_count */
                               .AppendValue(reader.IsDBNull(5) ? 0m : reader.GetDecimal(5))                              /* avg_duration_ms */
                               .AppendValue(reader.IsDBNull(6) ? 0m : reader.GetDecimal(6))                              /* avg_cpu_time_ms */
                               .AppendValue(reader.IsDBNull(7) ? 0m : reader.GetDecimal(7))                              /* avg_logical_reads */
                               .AppendValue(reader.IsDBNull(8) ? 0m : reader.GetDecimal(8))                              /* avg_logical_writes */
                               .AppendValue(reader.IsDBNull(9) ? 0m : reader.GetDecimal(9))                              /* avg_physical_reads */
                               .AppendValue(reader.IsDBNull(10) ? 0m : reader.GetDecimal(10))                            /* avg_rowcount */
                               .AppendValue(reader.IsDBNull(11) ? (DateTime?)null : ((DateTimeOffset)reader.GetValue(11)).UtcDateTime)
                               .AppendValue(reader.IsDBNull(12) ? (string?)null : reader.GetString(12))
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
}
