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
    /// Collects file I/O statistics from sys.dm_io_virtual_file_stats.
    /// </summary>
    private async Task<int> CollectFileIoStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        /* Azure SQL DB (edition 5) doesn't have sys.master_files; use sys.database_files instead.
           dm_io_virtual_file_stats on Azure SQL DB is scoped to the current database only.
           Azure MI (edition 8) HAS sys.master_files and behaves like on-prem. */
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        string query = isAzureSqlDb
            ? @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name = DB_NAME(),
    file_name = df.name,
    file_type = df.type_desc,
    physical_name = df.physical_name,
    size_mb = CONVERT(decimal(18,2), vfs.size_on_disk_bytes / 1048576.0),
    num_of_reads = vfs.num_of_reads,
    num_of_writes = vfs.num_of_writes,
    read_bytes = vfs.num_of_bytes_read,
    write_bytes = vfs.num_of_bytes_written,
    io_stall_read_ms = vfs.io_stall_read_ms,
    io_stall_write_ms = vfs.io_stall_write_ms,
    database_id = vfs.database_id,
    file_id = vfs.file_id
FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL) AS vfs
LEFT JOIN sys.database_files AS df
  ON df.file_id = vfs.file_id
OPTION(RECOMPILE);"
            : @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name = DB_NAME(vfs.database_id),
    file_name = mf.name,
    file_type = mf.type_desc,
    physical_name = mf.physical_name,
    size_mb = CONVERT(decimal(18,2), vfs.size_on_disk_bytes / 1048576.0),
    num_of_reads = vfs.num_of_reads,
    num_of_writes = vfs.num_of_writes,
    read_bytes = vfs.num_of_bytes_read,
    write_bytes = vfs.num_of_bytes_written,
    io_stall_read_ms = vfs.io_stall_read_ms,
    io_stall_write_ms = vfs.io_stall_write_ms,
    database_id = vfs.database_id,
    file_id = vfs.file_id
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
LEFT JOIN sys.master_files AS mf
  ON  mf.database_id = vfs.database_id
  AND mf.file_id = vfs.file_id
WHERE (vfs.database_id > 4 OR vfs.database_id = 2)
AND   vfs.database_id < 32761
AND   vfs.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
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

        /* Collect all rows first */
        var fileStats = new List<(
            string DatabaseName, string FileName, string FileType, string PhysicalName,
            decimal SizeMb, long NumOfReads, long NumOfWrites, long ReadBytes, long WriteBytes,
            long IoStallReadMs, long IoStallWriteMs, int DatabaseId, int FileId)>();

        while (await reader.ReadAsync(cancellationToken))
        {
            fileStats.Add((
                DatabaseName: reader.IsDBNull(0) ? "Unknown" : reader.GetString(0),
                FileName: reader.IsDBNull(1) ? "Unknown" : reader.GetString(1),
                FileType: reader.IsDBNull(2) ? "Unknown" : reader.GetString(2),
                PhysicalName: reader.IsDBNull(3) ? "" : reader.GetString(3),
                SizeMb: reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                NumOfReads: reader.IsDBNull(5) ? 0L : reader.GetInt64(5),
                NumOfWrites: reader.IsDBNull(6) ? 0L : reader.GetInt64(6),
                ReadBytes: reader.IsDBNull(7) ? 0L : reader.GetInt64(7),
                WriteBytes: reader.IsDBNull(8) ? 0L : reader.GetInt64(8),
                IoStallReadMs: reader.IsDBNull(9) ? 0L : reader.GetInt64(9),
                IoStallWriteMs: reader.IsDBNull(10) ? 0L : reader.GetInt64(10),
                DatabaseId: reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11)),
                FileId: reader.IsDBNull(12) ? 0 : Convert.ToInt32(reader.GetValue(12))
            ));
        }
        sqlSw.Stop();

        /* Insert into DuckDB with delta calculations */
        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("file_io_stats"))
            {
                foreach (var stat in fileStats)
                {
                    var deltaKey = $"{stat.DatabaseId}_{stat.FileId}";
                    var deltaReads = _deltaCalculator.CalculateDelta(serverId, "file_io_reads", deltaKey, stat.NumOfReads);
                    var deltaWrites = _deltaCalculator.CalculateDelta(serverId, "file_io_writes", deltaKey, stat.NumOfWrites);
                    var deltaReadBytes = _deltaCalculator.CalculateDelta(serverId, "file_io_read_bytes", deltaKey, stat.ReadBytes);
                    var deltaWriteBytes = _deltaCalculator.CalculateDelta(serverId, "file_io_write_bytes", deltaKey, stat.WriteBytes);
                    var deltaStallReadMs = _deltaCalculator.CalculateDelta(serverId, "file_io_stall_read", deltaKey, stat.IoStallReadMs);
                    var deltaStallWriteMs = _deltaCalculator.CalculateDelta(serverId, "file_io_stall_write", deltaKey, stat.IoStallWriteMs);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(stat.DatabaseName)
                       .AppendValue(stat.FileName)
                       .AppendValue(stat.FileType)
                       .AppendValue(stat.PhysicalName)
                       .AppendValue(stat.SizeMb)
                       .AppendValue(stat.NumOfReads)
                       .AppendValue(stat.NumOfWrites)
                       .AppendValue(stat.ReadBytes)
                       .AppendValue(stat.WriteBytes)
                       .AppendValue(stat.IoStallReadMs)
                       .AppendValue(stat.IoStallWriteMs)
                       .AppendValue(deltaReads)
                       .AppendValue(deltaWrites)
                       .AppendValue(deltaReadBytes)
                       .AppendValue(deltaWriteBytes)
                       .AppendValue(deltaStallReadMs)
                       .AppendValue(deltaStallWriteMs)
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} file I/O stats for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
