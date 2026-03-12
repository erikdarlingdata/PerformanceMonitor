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
    /// Collects server edition, version, CPU/memory hardware metadata for
    /// license audit and FinOps cost attribution. On-load only collector.
    /// </summary>
    private async Task<int> CollectServerPropertiesAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus?.SqlEngineEdition == 5;

        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    server_name =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')),
    edition =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition')),
    product_version =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
    product_level =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductLevel')),
    product_update_level =
        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductUpdateLevel')),
    engine_edition =
        CONVERT(int, SERVERPROPERTY(N'EngineEdition')),
    cpu_count =
        osi.cpu_count,
    hyperthread_ratio =
        osi.hyperthread_ratio,
    physical_memory_mb =
        osi.physical_memory_kb / 1024,
    socket_count =
        osi.socket_count,
    cores_per_socket =
        osi.cores_per_socket,
    is_hadr_enabled =
        CONVERT(bit, SERVERPROPERTY(N'IsHadrEnabled')),
    is_clustered =
        CONVERT(bit, SERVERPROPERTY(N'IsClustered')),
    service_objective =
        CASE
            WHEN CONVERT(int, SERVERPROPERTY(N'EngineEdition')) = 5
            THEN CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), N'ServiceObjective'))
            ELSE NULL
        END
FROM sys.dm_os_sys_info AS osi
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
        if (await reader.ReadAsync(cancellationToken))
        {
            var serverName = reader.GetString(0);
            var edition = reader.GetString(1);
            var productVersion = reader.GetString(2);
            var productLevel = reader.GetString(3);
            var productUpdateLevel = reader.IsDBNull(4) ? null : reader.GetString(4);
            var engineEdition = reader.GetInt32(5);
            var cpuCount = reader.GetInt32(6);
            var hyperthreadRatio = reader.GetInt32(7);
            var physicalMemoryMb = reader.GetInt64(8);
            int? socketCount = reader.IsDBNull(9) ? null : reader.GetInt32(9);
            int? coresPerSocket = reader.IsDBNull(10) ? null : reader.GetInt32(10);
            bool? isHadrEnabled = reader.IsDBNull(11) ? null : reader.GetBoolean(11);
            bool? isClustered = reader.IsDBNull(12) ? null : reader.GetBoolean(12);
            var serviceObjective = reader.IsDBNull(13) ? null : reader.GetString(13);

            sqlSw.Stop();

            var duckSw = Stopwatch.StartNew();

            using (var duckConnection = _duckDb.CreateConnection())
            {
                await duckConnection.OpenAsync(cancellationToken);

                using (var appender = duckConnection.CreateAppender("server_properties"))
                {
                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(GetServerNameForStorage(server))
                       .AppendValue(edition)
                       .AppendValue(productVersion)
                       .AppendValue(productLevel)
                       .AppendValue(productUpdateLevel)
                       .AppendValue(engineEdition)
                       .AppendValue(cpuCount)
                       .AppendValue(hyperthreadRatio)
                       .AppendValue(physicalMemoryMb)
                       .AppendValue(socketCount)
                       .AppendValue(coresPerSocket)
                       .AppendValue(isHadrEnabled)
                       .AppendValue(isClustered)
                       .AppendValue((string?)null) // enterprise_features — not collected in Lite (requires cross-database cursor)
                       .AppendValue(serviceObjective)
                       .EndRow();
                    rowsCollected++;
                }
            }

            duckSw.Stop();
            _lastDuckDbMs = duckSw.ElapsedMilliseconds;
        }
        else
        {
            sqlSw.Stop();
        }

        _lastSqlMs = sqlSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} server properties row(s) for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
