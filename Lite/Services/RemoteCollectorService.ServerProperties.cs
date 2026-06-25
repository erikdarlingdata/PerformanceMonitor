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
    // RAM floor below which LPIM-off is not worth flagging (shared rule with the Dashboard
    // SqlServerFactCollector). On a small buffer pool the OS paging SQL out is not the practical
    // risk it is on a large dedicated host.
    private const long LpimAdvisoryMinPhysicalMemoryMb = 32 * 1024;

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
        /* Azure SQL DB reports the legacy 'SQL Azure' for SERVERPROPERTY('Edition');
           store the actual product name + service tier instead. */
        CASE
            WHEN CONVERT(int, SERVERPROPERTY(N'EngineEdition')) = 5
            THEN N'Azure SQL Database'
                 + ISNULL(N' (' +
                     CASE CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), N'Edition'))
                         WHEN N'GeneralPurpose'   THEN N'General Purpose'
                         WHEN N'BusinessCritical' THEN N'Business Critical'
                         ELSE CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), N'Edition'))
                     END + N')', N'')
            ELSE CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition'))
        END,
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

            /* For Azure SQL DB, sys.dm_os_sys_info.cpu_count returns the compute node's
               total cores, not the per-database vCore allocation. Parse the actual vCore
               count from the service_objective string (e.g. "HS_Gen5_14" → 14). */
            int? vcoreCount = null;
            if (isAzureSqlDb && !string.IsNullOrEmpty(serviceObjective))
            {
                vcoreCount = ParseVcoreFromServiceObjective(serviceObjective);
            }

            /* WS5 server-health values (LPIM / IFI / memory dumps). Read best-effort on the same
               connection — version-gated and Azure-unavailable DMVs are guarded inside the query
               and any failure leaves the values NULL rather than failing the collector. */
            var (lockPagesInMemory, instantFileInit, memoryDumpCount) =
                await ReadServerHealthAsync(sqlConnection, isAzureSqlDb, cancellationToken);

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
                       .AppendValue(vcoreCount)
                       .AppendValue(lockPagesInMemory)
                       .AppendValue(instantFileInit)
                       .AppendValue(memoryDumpCount)
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

    /// <summary>
    /// Reads the WS5 server-health values — Lock Pages in Memory, Instant File Initialization,
    /// and the SQL Server memory-dump count — best-effort from a single guarded batch. Each value
    /// is left NULL when its source is unavailable (Azure SQL DB, older builds, or a missing
    /// column/DMV), and any failure is swallowed so this never fails the server-properties
    /// collection. Mirrors the defensive logic in install/53_collect_server_properties.sql.
    /// </summary>
    private async Task<(bool? lpim, bool? ifi, int? dumpCount)> ReadServerHealthAsync(
        SqlConnection sqlConnection, bool isAzureSqlDb, CancellationToken cancellationToken)
    {
        if (isAzureSqlDb)
        {
            /* None of these sources are meaningful/available on Azure SQL DB. */
            return (null, null, null);
        }

        /* LPIM: sql_memory_model_desc (LOCK_PAGES / LARGE_PAGES => on; CONVENTIONAL => off).
           IFI: sys.dm_server_services.instant_file_initialization_enabled ('Y'/'N'), guarded on
                DMV + column existence and read via dynamic SQL so older builds still compile.
           Dumps: COUNT from sys.dm_server_memory_dumps, guarded on DMV existence.
           Each branch is wrapped in TRY/CATCH inside the batch so one missing source does not
           prevent reading the others. */
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @lpim bit = NULL,
    @ifi bit = NULL,
    @dumps integer = NULL;

BEGIN TRY
    SELECT
        @lpim =
            CASE
                WHEN osi.sql_memory_model IN (2, 3)
                THEN CONVERT(bit, 1)
                ELSE CONVERT(bit, 0)
            END
    FROM sys.dm_os_sys_info AS osi;
END TRY
BEGIN CATCH
    SET @lpim = NULL;
END CATCH;

IF OBJECT_ID(N'sys.dm_server_services', N'V') IS NOT NULL
AND EXISTS
(
    SELECT
        1/0
    FROM sys.system_columns AS sc
    WHERE sc.object_id = OBJECT_ID(N'sys.dm_server_services')
    AND   sc.name = N'instant_file_initialization_enabled'
)
BEGIN
    BEGIN TRY
        DECLARE
            @ifi_sql nvarchar(max) =
                N'
        SELECT TOP (1)
            @ifi_out =
                CASE
                    WHEN ss.instant_file_initialization_enabled = N''Y''
                    THEN CONVERT(bit, 1)
                    WHEN ss.instant_file_initialization_enabled = N''N''
                    THEN CONVERT(bit, 0)
                    ELSE NULL
                END
        FROM sys.dm_server_services AS ss
        WHERE ss.servicename LIKE N''SQL Server (%'';';

        EXECUTE sys.sp_executesql
            @ifi_sql,
          N'@ifi_out bit OUTPUT',
            @ifi_out = @ifi OUTPUT;
    END TRY
    BEGIN CATCH
        SET @ifi = NULL;
    END CATCH;
END;

IF OBJECT_ID(N'sys.dm_server_memory_dumps', N'V') IS NOT NULL
BEGIN
    BEGIN TRY
        SELECT
            @dumps = COUNT_BIG(*)
        FROM sys.dm_server_memory_dumps AS smd;
    END TRY
    BEGIN CATCH
        SET @dumps = NULL;
    END CATCH;
END;

SELECT
    lock_pages_in_memory = @lpim,
    instant_file_initialization_enabled = @ifi,
    memory_dump_count = @dumps;";

        try
        {
            using var command = new SqlCommand(query, sqlConnection);
            command.CommandTimeout = CommandTimeoutSeconds;

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return (null, null, null);
            }

            bool? lpim = reader.IsDBNull(0) ? (bool?)null : reader.GetBoolean(0);
            bool? ifi = reader.IsDBNull(1) ? (bool?)null : reader.GetBoolean(1);
            int? dumpCount = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2);
            return (lpim, ifi, dumpCount);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "ReadServerHealthAsync failed; leaving server-health values NULL");
            return (null, null, null);
        }
    }

    /// <summary>
    /// Parses the vCore count from an Azure SQL DB service objective string.
    /// vCore tiers follow the pattern {Tier}_{Gen}_{VcoreCount}
    /// (e.g. "HS_Gen5_14", "GP_Gen5_6", "BC_Gen5_8", "GP_S_Gen5_2").
    /// DTU tiers (e.g. "P1", "S0") and elastic pools return null.
    /// </summary>
    internal static int? ParseVcoreFromServiceObjective(string serviceObjective)
    {
        var parts = serviceObjective.Split('_');
        if (parts.Length >= 3 && int.TryParse(parts[^1], out var vcores) && vcores > 0)
        {
            return vcores;
        }

        return null;
    }
}
