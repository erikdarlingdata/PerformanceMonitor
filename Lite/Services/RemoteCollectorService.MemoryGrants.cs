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
    /// Collects memory grant statistics from sys.dm_exec_query_memory_grants.
    /// </summary>
    private async Task<int> CollectMemoryGrantStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    session_id = mg.session_id,
    database_name = DB_NAME(st.dbid),
    query_text = LEFT(st.text, 4000),
    requested_memory_mb = CONVERT(decimal(18,2), mg.requested_memory_kb / 1024.0),
    granted_memory_mb = CONVERT(decimal(18,2), ISNULL(mg.granted_memory_kb, 0) / 1024.0),
    used_memory_mb = CONVERT(decimal(18,2), ISNULL(mg.used_memory_kb, 0) / 1024.0),
    max_used_memory_mb = CONVERT(decimal(18,2), ISNULL(mg.max_used_memory_kb, 0) / 1024.0),
    ideal_memory_mb = CONVERT(decimal(18,2), mg.ideal_memory_kb / 1024.0),
    required_memory_mb = CONVERT(decimal(18,2), mg.required_memory_kb / 1024.0),
    wait_time_ms = mg.wait_time_ms,
    is_small_grant = mg.is_small,
    dop = mg.dop,
    query_cost = mg.query_cost
FROM sys.dm_exec_query_memory_grants AS mg
OUTER APPLY sys.dm_exec_sql_text(mg.sql_handle) AS st
WHERE mg.session_id <> @@SPID
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var rows = new List<(int SessionId, string? DatabaseName, string? QueryText,
            decimal RequestedMb, decimal GrantedMb, decimal UsedMb, decimal MaxUsedMb,
            decimal IdealMb, decimal RequiredMb, long WaitTimeMs, bool IsSmall,
            int Dop, decimal QueryCost)>();

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                Convert.ToInt32(reader.GetValue(0)),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
                reader.IsDBNull(8) ? 0m : reader.GetDecimal(8),
                reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                reader.IsDBNull(10) ? false : Convert.ToBoolean(reader.GetValue(10)),
                reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11)),
                reader.IsDBNull(12) ? 0m : SafeToDecimal(reader.GetValue(12))));
        }
        sqlSw.Stop();

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("memory_grant_stats"))
            {
                foreach (var r in rows)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(r.SessionId)
                       .AppendValue(r.DatabaseName)
                       .AppendValue(r.QueryText)
                       .AppendValue(r.RequestedMb)
                       .AppendValue(r.GrantedMb)
                       .AppendValue(r.UsedMb)
                       .AppendValue(r.MaxUsedMb)
                       .AppendValue(r.IdealMb)
                       .AppendValue(r.RequiredMb)
                       .AppendValue(r.WaitTimeMs)
                       .AppendValue(r.IsSmall)
                       .AppendValue(r.Dop)
                       .AppendValue(r.QueryCost)
                       .EndRow();
                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} memory grant records for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
