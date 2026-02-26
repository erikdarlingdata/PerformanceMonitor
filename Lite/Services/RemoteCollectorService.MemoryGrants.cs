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
    /// Collects memory grant statistics from sys.dm_exec_query_resource_semaphores.
    /// Uses the same DMV as Dashboard for parity.
    /// </summary>
    private async Task<int> CollectMemoryGrantStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    resource_semaphore_id = deqrs.resource_semaphore_id,
    pool_id = deqrs.pool_id,
    target_memory_mb = CONVERT(decimal(18,2), deqrs.target_memory_kb / 1024.0),
    max_target_memory_mb = CONVERT(decimal(18,2), deqrs.max_target_memory_kb / 1024.0),
    total_memory_mb = CONVERT(decimal(18,2), deqrs.total_memory_kb / 1024.0),
    available_memory_mb = CONVERT(decimal(18,2), deqrs.available_memory_kb / 1024.0),
    granted_memory_mb = CONVERT(decimal(18,2), ISNULL(deqrs.granted_memory_kb, 0) / 1024.0),
    used_memory_mb = CONVERT(decimal(18,2), ISNULL(deqrs.used_memory_kb, 0) / 1024.0),
    grantee_count = deqrs.grantee_count,
    waiter_count = deqrs.waiter_count,
    timeout_error_count = ISNULL(deqrs.timeout_error_count, 0),
    forced_grant_count = ISNULL(deqrs.forced_grant_count, 0)
FROM sys.dm_exec_query_resource_semaphores AS deqrs
WHERE deqrs.max_target_memory_kb IS NOT NULL
OPTION(RECOMPILE);";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var rows = new List<(short ResourceSemaphoreId, int PoolId,
            decimal TargetMb, decimal MaxTargetMb, decimal TotalMb, decimal AvailableMb,
            decimal GrantedMb, decimal UsedMb,
            int GranteeCount, int WaiterCount, long TimeoutErrorCount, long ForcedGrantCount)>();

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                Convert.ToInt16(reader.GetValue(0)),
                Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
                reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11))));
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
                    var deltaKey = $"{r.PoolId}_{r.ResourceSemaphoreId}";
                    var deltaTimeouts = _deltaCalculator.CalculateDelta(serverId, "memory_grants_timeouts", deltaKey, r.TimeoutErrorCount);
                    var deltaForced = _deltaCalculator.CalculateDelta(serverId, "memory_grants_forced", deltaKey, r.ForcedGrantCount);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(r.ResourceSemaphoreId)
                       .AppendValue(r.PoolId)
                       .AppendValue(r.TargetMb)
                       .AppendValue(r.MaxTargetMb)
                       .AppendValue(r.TotalMb)
                       .AppendValue(r.AvailableMb)
                       .AppendValue(r.GrantedMb)
                       .AppendValue(r.UsedMb)
                       .AppendValue(r.GranteeCount)
                       .AppendValue(r.WaiterCount)
                       .AppendValue(r.TimeoutErrorCount)
                       .AppendValue(r.ForcedGrantCount)
                       .AppendValue(deltaTimeouts)
                       .AppendValue(deltaForced)
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
