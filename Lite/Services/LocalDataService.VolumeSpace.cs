/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest free space for each distinct volume on the server, ordered worst (lowest free %)
    /// first, for low-disk alert checking. Files on the same volume share one row. Volumes with no
    /// mount point (Azure SQL DB has no volume stats) are excluded, so the result is empty there.
    /// </summary>
    public async Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    volume_mount_point,
    MAX(volume_total_mb) AS volume_total_mb,
    MIN(volume_free_mb) AS volume_free_mb
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_database_size_stats
    WHERE server_id = $1
)
AND   volume_mount_point IS NOT NULL
AND   volume_total_mb > 0
GROUP BY volume_mount_point
ORDER BY MIN(volume_free_mb) / MAX(volume_total_mb)";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<VolumeFreeSpaceInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new VolumeFreeSpaceInfo
            {
                MountPoint = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                FreeMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }

        return items;
    }
}

public class VolumeFreeSpaceInfo
{
    public string MountPoint { get; set; } = "";
    public double TotalMb { get; set; }
    public double FreeMb { get; set; }

    public double FreePercent => TotalMb > 0 ? FreeMb / TotalMb * 100 : 0;
    public double FreeGb => FreeMb / 1024.0;
}
