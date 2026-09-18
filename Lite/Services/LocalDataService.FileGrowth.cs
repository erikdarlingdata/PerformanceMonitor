/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's half of the file-growth read (#2349) — the DuckDB twin of
/// <c>DarlingAlertReadAdapter.DatabaseFileGrowthSql</c>, so the alert behaves identically in both SKUs.
///
/// <para>Same shape as the Postgres side and for the same reasons: newest row per (database, file), a baseline
/// from the oldest sample inside the window, and growth as the difference — 0 when the window holds one sample,
/// which reads as "no rise observed" rather than as a rise of the whole file.</para>
///
/// <para>DuckDB has no <c>DISTINCT ON</c>, so the same selection is expressed with <c>ROW_NUMBER()</c>
/// partitioned by the file key — the idiom the rest of Lite's store SQL already uses where Postgres would use
/// <c>DISTINCT ON</c>.</para>
///
/// <para>The newest sample's <c>collection_time</c> rides along as <c>observed_at</c>, the last column (#3636):
/// the growth is a fact about two collections that reads identically on every alert pass until the next one
/// lands — hourly, for this collector, against a 5-minute cooldown — and the engine needs the observation's
/// identity to fire the rise gate once per collection rather than up to twelve times. Appended so the fourteen
/// ordinals already bound do not move; Darling's twin carries the same column at the same position, and the
/// Lite.Tests pin holds both texts to it. #3579's <c>observed_at</c> on the forced-plan read is the same
/// column for the same reason.</para>
/// </summary>
public partial class LocalDataService
{
    /// <summary>The file-growth read's text, exposed like <see cref="ForcePlanFailuresSql"/> so the tests can
    /// pin its shape against Darling's twin without a DuckDB round trip. $1 server_id, $2 window start.</summary>
    public const string DatabaseFileGrowthSql = @"
WITH windowed AS (
    SELECT
        database_name, file_name, physical_name, file_type_desc, collection_time,
        total_size_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb,
        volume_mount_point, volume_total_mb, volume_free_mb,
        ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time DESC) AS rn_new,
        ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time ASC)  AS rn_old
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
)
SELECT
    c.database_name,
    c.file_name,
    COALESCE(c.physical_name, '') AS physical_name,
    COALESCE(c.file_type_desc, '') AS file_type_desc,
    COALESCE(c.total_size_mb, 0) AS total_size_mb,
    COALESCE(c.total_size_mb, 0) - COALESCE(b.total_size_mb, c.total_size_mb, 0) AS growth_mb,
    COALESCE(date_diff('second', b.collection_time, c.collection_time) / 60.0, 0) AS growth_window_minutes,
    COALESCE(c.volume_mount_point, '') AS volume_mount_point,
    COALESCE(c.volume_total_mb, 0) AS volume_total_mb,
    COALESCE(c.volume_free_mb, 0) AS volume_free_mb,
    c.auto_growth_mb,
    COALESCE(c.is_percent_growth, false) AS is_percent_growth,
    c.growth_pct,
    c.max_size_mb,
    c.collection_time AS observed_at
FROM windowed c
LEFT JOIN windowed b
  ON  b.database_name = c.database_name
  AND b.file_name = c.file_name
  AND b.rn_old = 1
WHERE c.rn_new = 1
AND   c.total_size_mb IS NOT NULL
ORDER BY c.database_name, c.file_name";

    public async Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(int serverId, int lookbackMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = DatabaseFileGrowthSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter
        {
            Value = DateTime.UtcNow.AddMinutes(-Math.Max(1, lookbackMinutes))
        });

        var items = new List<DatabaseFileGrowthInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseFileGrowthInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                PhysicalName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                FileTypeDesc = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TotalSizeMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                GrowthMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                GrowthWindowMinutes = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                VolumeMountPoint = reader.IsDBNull(7) ? "" : reader.GetString(7),
                VolumeTotalMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                VolumeFreeMb = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                AutoGrowthMb = reader.IsDBNull(10) ? null : ToDouble(reader.GetValue(10)),
                IsPercentGrowth = !reader.IsDBNull(11) && Convert.ToBoolean(reader.GetValue(11)),
                GrowthPct = reader.IsDBNull(12) ? null : ToDouble(reader.GetValue(12)),
                MaxSizeMb = reader.IsDBNull(13) ? null : ToDouble(reader.GetValue(13)),
                /* #3636: a naive-UTC TIMESTAMP read back Kind-Unspecified, stamped Utc because that is what it
                   is. The engine compares one file's stamps only with each other. */
                ObservedAtUtc = reader.IsDBNull(14) ? null : DateTime.SpecifyKind(reader.GetDateTime(14), DateTimeKind.Utc),
            });
        }

        return items;
    }
}
