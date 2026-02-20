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
    /// Collects TempDB space usage statistics.
    /// </summary>
    private async Task<int> CollectTempDbStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    user_object_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.user_object_reserved_page_count) * 8 / 1024.0),
    internal_object_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.internal_object_reserved_page_count) * 8 / 1024.0),
    version_store_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.version_store_reserved_page_count) * 8 / 1024.0),
    total_reserved_mb = CONVERT(decimal(18,2), SUM(dsu.user_object_reserved_page_count + dsu.internal_object_reserved_page_count + dsu.version_store_reserved_page_count) * 8 / 1024.0),
    unallocated_mb = CONVERT(decimal(18,2), SUM(dsu.unallocated_extent_page_count) * 8 / 1024.0)
FROM tempdb.sys.dm_db_file_space_usage AS dsu;

SELECT TOP (1)
    session_id = ssu.session_id,
    tempdb_mb = CONVERT(decimal(18,2), (ssu.user_objects_alloc_page_count + ssu.internal_objects_alloc_page_count) * 8 / 1024.0),
    total_sessions = (SELECT COUNT_BIG(*) FROM sys.dm_db_session_space_usage WHERE user_objects_alloc_page_count + internal_objects_alloc_page_count > 0)
FROM sys.dm_db_session_space_usage AS ssu
ORDER BY (ssu.user_objects_alloc_page_count + ssu.internal_objects_alloc_page_count) DESC;";

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        decimal userObjMb = 0, internalObjMb = 0, versionStoreMb = 0, totalReservedMb = 0, unallocatedMb = 0;
        int topSessionId = 0, totalSessions = 0;
        decimal topSessionMb = 0;

        if (await reader.ReadAsync(cancellationToken))
        {
            userObjMb = reader.IsDBNull(0) ? 0m : reader.GetDecimal(0);
            internalObjMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
            versionStoreMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2);
            totalReservedMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3);
            unallocatedMb = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4);
        }

        if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
        {
            topSessionId = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
            topSessionMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1);
            totalSessions = reader.IsDBNull(2) ? 0 : (int)reader.GetInt64(2);
        }
        sqlSw.Stop();

        /* Insert into DuckDB using Appender */
        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("tempdb_stats"))
            {
                var row = appender.CreateRow();
                row.AppendValue(GenerateCollectionId())
                   .AppendValue(collectionTime)
                   .AppendValue(serverId)
                   .AppendValue(server.ServerName)
                   .AppendValue(userObjMb)
                   .AppendValue(internalObjMb)
                   .AppendValue(versionStoreMb)
                   .AppendValue(totalReservedMb)
                   .AppendValue(unallocatedMb)
                   .AppendValue(totalSessions)
                   .AppendValue(topSessionId)
                   .AppendValue(topSessionMb)
                   .EndRow();
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected TempDB stats for server '{Server}'", server.DisplayName);
        return 1;
    }
}
