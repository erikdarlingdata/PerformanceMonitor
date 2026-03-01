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
    /// Collects point-in-time waiting task information from sys.dm_os_waiting_tasks.
    /// </summary>
    private async Task<int> CollectWaitingTasksAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    session_id = wt.session_id,
    wait_type = wt.wait_type,
    wait_duration_ms = wt.wait_duration_ms,
    blocking_session_id = wt.blocking_session_id,
    database_name = d.name
FROM sys.dm_os_waiting_tasks AS wt
LEFT JOIN sys.dm_exec_requests AS er
  ON er.session_id = wt.session_id
LEFT JOIN sys.databases AS d
  ON d.database_id = er.database_id
WHERE wt.session_id >= 50
AND   wt.session_id <> @@SPID
AND   wt.wait_type IS NOT NULL
AND   er.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
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

            using (var appender = duckConnection.CreateAppender("waiting_tasks"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    /* session_id and blocking_session_id are smallint in sys.dm_os_waiting_tasks */
                    var sessionId = reader.IsDBNull(0) ? 0 : reader.GetInt16(0);
                    var waitType = reader.IsDBNull(1) ? null : reader.GetString(1);
                    var waitDurationMs = reader.IsDBNull(2) ? 0L : reader.GetInt64(2);
                    var blockingSessionId = reader.IsDBNull(3) ? (short?)null : reader.GetInt16(3);
                    var databaseName = reader.IsDBNull(4) ? null : reader.GetString(4);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue((int)sessionId)
                       .AppendValue(waitType)
                       .AppendValue(waitDurationMs)
                       .AppendValue(blockingSessionId.HasValue ? (int?)blockingSessionId.Value : null)
                       .AppendValue((string?)null) /* resource_description — no longer collected */
                       .AppendValue(databaseName)
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} waiting task records for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
