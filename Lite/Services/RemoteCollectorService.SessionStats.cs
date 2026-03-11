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
    /// Collects per-application session statistics from sys.dm_exec_sessions.
    /// Groups by program_name to track connection counts, status breakdown,
    /// and cumulative resource usage per application.
    /// </summary>
    private async Task<int> CollectSessionStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    program_name =
        ISNULL(des.program_name, N''),
    connection_count =
        COUNT_BIG(*),
    running_count =
        SUM
        (
            CASE
                WHEN des.status = N'running'
                THEN 1
                ELSE 0
            END
        ),
    sleeping_count =
        SUM
        (
            CASE
                WHEN des.status = N'sleeping'
                THEN 1
                ELSE 0
            END
        ),
    dormant_count =
        SUM
        (
            CASE
                WHEN des.status = N'dormant'
                THEN 1
                ELSE 0
            END
        ),
    total_cpu_time_ms =
        SUM(des.cpu_time),
    total_reads =
        SUM(des.reads),
    total_writes =
        SUM(des.writes),
    total_logical_reads =
        SUM(des.logical_reads)
FROM sys.dm_exec_sessions AS des
WHERE des.session_id > 50
AND   des.is_user_process = 1
AND   des.program_name IS NOT NULL
AND   des.program_name <> N''
AND   des.program_name NOT LIKE N'PerformanceMonitor%'
GROUP BY
    des.program_name
ORDER BY
    COUNT_BIG(*) DESC
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
        _lastSqlMs = sqlSw.ElapsedMilliseconds;

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("session_stats"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var programName = reader.GetString(0);
                    var connectionCount = Convert.ToInt32(reader.GetValue(1));
                    var runningCount = Convert.ToInt32(reader.GetValue(2));
                    var sleepingCount = Convert.ToInt32(reader.GetValue(3));
                    var dormantCount = Convert.ToInt32(reader.GetValue(4));
                    long? totalCpuTimeMs = reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5));
                    long? totalReads = reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6));
                    long? totalWrites = reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7));
                    long? totalLogicalReads = reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetValue(8));

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(GetServerNameForStorage(server))
                       .AppendValue(programName)
                       .AppendValue(connectionCount)
                       .AppendValue(runningCount)
                       .AppendValue(sleepingCount)
                       .AppendValue(dormantCount)
                       .AppendValue(totalCpuTimeMs)
                       .AppendValue(totalReads)
                       .AppendValue(totalWrites)
                       .AppendValue(totalLogicalReads)
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} session stats rows for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
