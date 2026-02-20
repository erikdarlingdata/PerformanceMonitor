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
    /// Collects CPU utilization from the ring buffer (on-prem, MI, RDS)
    /// or sys.dm_db_resource_stats (Azure SQL DB).
    /// </summary>
    private async Task<int> CollectCpuUtilizationAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        /* Azure SQL DB: ring buffer is empty, use dm_db_resource_stats instead.
           Returns avg_cpu_percent sampled every 15 seconds, retained for 1 hour.
           No "other process" concept in Azure SQL DB — isolated environment. */
        const string azureSqlDbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (60)
    sample_time = drs.end_time,
    sqlserver_cpu_utilization = CONVERT(integer, drs.avg_cpu_percent),
    other_process_cpu_utilization = 0
FROM sys.dm_db_resource_stats AS drs
ORDER BY
    drs.end_time DESC
OPTION(RECOMPILE);";

        /* On-prem, MI, RDS: use ring buffer scheduler monitor */
        const string ringBufferQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @ms_ticks bigint,
    @start_time datetime2(7);

SELECT
    @ms_ticks = dosi.ms_ticks,
    @start_time = dosi.sqlserver_start_time
FROM sys.dm_os_sys_info AS dosi;

SELECT TOP (60)
    sample_time = DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSDATETIME()),
    sqlserver_cpu_utilization = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer'),
    other_process_cpu_utilization =
        CASE
            WHEN (100 - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
                      - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer')) < 0
            THEN 0
            ELSE 100 - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
                     - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer')
        END
FROM
(
    SELECT
        dorb.timestamp,
        record = CONVERT(xml, dorb.record)
    FROM sys.dm_os_ring_buffers AS dorb
    WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
) AS t
ORDER BY t.timestamp DESC
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        /* Get the most recent sample_time we already have, to skip duplicates.
           Ring buffer always returns TOP 60 (computed sample_time can't be filtered server-side).
           For Azure SQL DB, we push the filter into the SQL query since end_time is a real column. */
        var lastSampleTime = await GetLastCollectedTimeAsync(
            serverId, "cpu_utilization_stats", "sample_time", cancellationToken);

        string query;
        if (isAzureSqlDb && lastSampleTime.HasValue)
        {
            /* Azure SQL DB: filter server-side since end_time is a real column */
            query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    sample_time = drs.end_time,
    sqlserver_cpu_utilization = CONVERT(integer, drs.avg_cpu_percent),
    other_process_cpu_utilization = 0
FROM sys.dm_db_resource_stats AS drs
WHERE drs.end_time > @last_sample_time
ORDER BY
    drs.end_time DESC
OPTION(RECOMPILE);";
        }
        else
        {
            query = isAzureSqlDb ? azureSqlDbQuery : ringBufferQuery;
        }

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        if (isAzureSqlDb && lastSampleTime.HasValue)
        {
            command.Parameters.Add(new SqlParameter("@last_sample_time", System.Data.SqlDbType.DateTime2) { Value = lastSampleTime.Value });
        }

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        sqlSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;

        /* Insert into DuckDB using Appender for bulk performance */
        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("cpu_utilization_stats"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var sampleTime = reader.GetDateTime(0);

                    /* Client-side dedup for ring buffer (computed sample_time can't be filtered in SQL) */
                    if (!isAzureSqlDb && lastSampleTime.HasValue && sampleTime <= lastSampleTime.Value)
                        continue;

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(sampleTime)
                       .AppendValue(reader.IsDBNull(1) ? 0 : reader.GetInt32(1))
                       .AppendValue(reader.IsDBNull(2) ? 0 : reader.GetInt32(2))
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} CPU utilization samples for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
