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
    /// Collects per-file database sizes for growth trending and capacity planning.
    /// On-prem: queries sys.master_files + sys.databases + dm_os_volume_stats for file and drive context.
    /// Azure SQL DB: queries sys.database_files for the single database (no volume stats available).
    /// </summary>
    private async Task<int> CollectDatabaseSizeStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus?.SqlEngineEdition == 5;

        const string onPremQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

CREATE TABLE #file_space
(
    database_id int NOT NULL,
    file_id int NOT NULL,
    used_size_mb decimal(19,2) NULL
);

DECLARE
    @sql nvarchar(MAX) = N'';

SELECT
    @sql += N'
USE ' + QUOTENAME(d.name) + N';
INSERT #file_space (database_id, file_id, used_size_mb)
SELECT
    DB_ID(),
    df.file_id,
    CONVERT(decimal(19,2), FILEPROPERTY(df.name, N''SpaceUsed'') * 8.0 / 1024.0)
FROM sys.database_files AS df;
'
FROM sys.databases AS d
WHERE d.state_desc = N'ONLINE'
AND   d.database_id > 0
ORDER BY
    d.name;

EXEC sys.sp_executesql @sql;

SELECT
    database_name = d.name,
    database_id = d.database_id,
    file_id = mf.file_id,
    file_type_desc = mf.type_desc,
    file_name = mf.name,
    physical_name = mf.physical_name,
    total_size_mb =
        CONVERT(decimal(19,2), mf.size * 8.0 / 1024.0),
    used_size_mb =
        fs.used_size_mb,
    auto_growth_mb =
        CASE
            WHEN mf.is_percent_growth = 1
            THEN CONVERT(decimal(19,2), NULL)
            ELSE CONVERT(decimal(19,2), mf.growth * 8.0 / 1024.0)
        END,
    max_size_mb =
        CASE
            WHEN mf.max_size = -1
            THEN CONVERT(decimal(19,2), -1)
            WHEN mf.max_size = 268435456
            THEN CONVERT(decimal(19,2), 2097152)
            ELSE CONVERT(decimal(19,2), mf.max_size * 8.0 / 1024.0)
        END,
    recovery_model_desc =
        d.recovery_model_desc,
    compatibility_level =
        CONVERT(int, d.compatibility_level),
    state_desc =
        d.state_desc,
    volume_mount_point =
        RTRIM(vs.volume_mount_point),
    volume_total_mb =
        CONVERT(decimal(19,2), vs.total_bytes / 1048576.0),
    volume_free_mb =
        CONVERT(decimal(19,2), vs.available_bytes / 1048576.0)
FROM sys.master_files AS mf
JOIN sys.databases AS d
  ON d.database_id = mf.database_id
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
LEFT JOIN #file_space AS fs
  ON  fs.database_id = mf.database_id
  AND fs.file_id = mf.file_id
WHERE d.state_desc = N'ONLINE'
ORDER BY
    d.name,
    mf.file_id
OPTION(RECOMPILE);";

        const string azureSqlDbQuery = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name = DB_NAME(),
    database_id = DB_ID(),
    file_id = df.file_id,
    file_type_desc = df.type_desc,
    file_name = df.name,
    physical_name = df.physical_name,
    total_size_mb =
        CONVERT(decimal(19,2), df.size * 8.0 / 1024.0),
    used_size_mb =
        CONVERT(decimal(19,2), FILEPROPERTY(df.name, N'SpaceUsed') * 8.0 / 1024.0),
    auto_growth_mb =
        CASE
            WHEN df.is_percent_growth = 1
            THEN CONVERT(decimal(19,2), NULL)
            ELSE CONVERT(decimal(19,2), df.growth * 8.0 / 1024.0)
        END,
    max_size_mb =
        CASE
            WHEN df.max_size = -1
            THEN CONVERT(decimal(19,2), -1)
            WHEN df.max_size = 268435456
            THEN CONVERT(decimal(19,2), 2097152)
            ELSE CONVERT(decimal(19,2), df.max_size * 8.0 / 1024.0)
        END,
    recovery_model_desc =
        CONVERT(nvarchar(12), DATABASEPROPERTYEX(DB_NAME(), N'Recovery')),
    compatibility_level =
        CONVERT(int, NULL),
    state_desc =
        N'ONLINE',
    volume_mount_point =
        CONVERT(nvarchar(256), NULL),
    volume_total_mb =
        CONVERT(decimal(19,2), NULL),
    volume_free_mb =
        CONVERT(decimal(19,2), NULL)
FROM sys.database_files AS df
ORDER BY
    df.file_id
OPTION(RECOMPILE);";

        string query = isAzureSqlDb ? azureSqlDbQuery : onPremQuery;

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var rows = new List<(string DatabaseName, int DatabaseId, int FileId, string FileTypeDesc,
            string FileName, string PhysicalName, decimal TotalSizeMb, decimal? UsedSizeMb,
            decimal? AutoGrowthMb, decimal? MaxSizeMb, string? RecoveryModel,
            int? CompatibilityLevel, string? StateDesc, string? VolumeMountPoint,
            decimal? VolumeTotalMb, decimal? VolumeFreeMb)>();

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                reader.GetString(0),
                reader.GetInt32(1),
                reader.GetInt32(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.GetDecimal(6),
                reader.IsDBNull(7) ? null : reader.GetDecimal(7),
                reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? null : reader.GetInt32(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetDecimal(14),
                reader.IsDBNull(15) ? null : reader.GetDecimal(15)));
        }
        sqlSw.Stop();

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("database_size_stats"))
            {
                foreach (var r in rows)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(r.DatabaseName)
                       .AppendValue(r.DatabaseId)
                       .AppendValue(r.FileId)
                       .AppendValue(r.FileTypeDesc)
                       .AppendValue(r.FileName)
                       .AppendValue(r.PhysicalName)
                       .AppendValue(r.TotalSizeMb)
                       .AppendValue(r.UsedSizeMb)
                       .AppendValue(r.AutoGrowthMb)
                       .AppendValue(r.MaxSizeMb)
                       .AppendValue(r.RecoveryModel)
                       .AppendValue(r.CompatibilityLevel)
                       .AppendValue(r.StateDesc)
                       .AppendValue(r.VolumeMountPoint)
                       .AppendValue(r.VolumeTotalMb)
                       .AppendValue(r.VolumeFreeMb)
                       .EndRow();
                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} database size rows for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
