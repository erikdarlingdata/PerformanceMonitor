/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>One file of the latest database-size snapshot, with the growth, recovery-model and VLF columns the
/// desktop's Database Sizes grid shows. A null size is the Hyperscale log file (the log service); a null file id
/// is the one row another database on an Azure SQL Database server gets.</summary>
public sealed record DatabaseSizeFileDto(
    DateTime CollectionTime, string DatabaseName, string FileTypeDesc, string FileName,
    decimal? TotalSizeMb, decimal? UsedSizeMb, decimal? MaxSizeMb,
    string? VolumeMountPoint, decimal? VolumeTotalMb, decimal? VolumeFreeMb,
    string? RecoveryModel, decimal? AutoGrowthMb, bool? IsPercentGrowth, int? GrowthPct, int? VlfCount, int? FileId);

/// <summary>The latest per-file database-size snapshot for one server: the desktop viewer's
/// <c>DatabaseSizeLatestSql</c> columns plus <c>max_size_mb</c>, read through the same windowed snapshot probe.</summary>
public static class DarlingFinOpsDatabaseSizesReader
{
    /// <summary>$1 server_id, $2 collection_time (resolved by <see cref="DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync"/>).
    /// Biggest files first; the trailing keys make the order total.</summary>
    public const string LatestSql = @"
SELECT
    collection_time,
    database_name,
    file_type_desc,
    file_name,
    total_size_mb,
    used_size_mb,
    max_size_mb,
    volume_mount_point,
    volume_total_mb,
    volume_free_mb,
    recovery_model_desc,
    auto_growth_mb,
    is_percent_growth,
    growth_pct,
    vlf_count,
    file_id
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = $2
ORDER BY total_size_mb DESC NULLS LAST, database_name, file_type_desc, file_name, file_id";

    public static async Task<List<DatabaseSizeFileDto>> GetLatestAsync(
        NpgsqlDataSource dataSource, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var items = new List<DatabaseSizeFileDto>();
        var snapshotTime = await DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync(
            dataSource, serverId, commandTimeoutSeconds, cancellationToken);
        if (snapshotTime is null)
        {
            return items;
        }

        await using var command = dataSource.CreateCommand(LatestSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = snapshotTime.Value });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseSizeFileDto(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                Dec(reader, 4), Dec(reader, 5), Dec(reader, 6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                Dec(reader, 8), Dec(reader, 9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                Dec(reader, 11),
                reader.IsDBNull(12) ? null : reader.GetBoolean(12),
                reader.IsDBNull(13) ? null : Convert.ToInt32(reader.GetValue(13)),
                reader.IsDBNull(14) ? null : Convert.ToInt32(reader.GetValue(14)),
                reader.IsDBNull(15) ? null : Convert.ToInt32(reader.GetValue(15))));
        }
        return items;
    }

    private static decimal? Dec(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToDecimal(reader.GetValue(ordinal));
}
