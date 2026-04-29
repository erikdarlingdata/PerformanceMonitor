/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest database size snapshot per server per file (cross-server).
    /// </summary>
    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    database_name,
    file_type_desc,
    file_name,
    total_size_mb,
    used_size_mb,
    volume_mount_point,
    volume_total_mb,
    volume_free_mb,
    recovery_model_desc,
    auto_growth_mb,
    is_percent_growth,
    growth_pct,
    vlf_count
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_database_size_stats
    WHERE server_id = $1
)
ORDER BY database_name, file_type_desc, file_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseSizeRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileTypeDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                TotalSizeMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                UsedSizeMb = reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                VolumeMountPoint = reader.IsDBNull(5) ? null : reader.GetString(5),
                VolumeTotalMb = reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6)),
                VolumeFreeMb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                RecoveryModel = reader.IsDBNull(8) ? null : reader.GetString(8),
                AutoGrowthMb = reader.IsDBNull(9) ? null : Convert.ToDecimal(reader.GetValue(9)),
                IsPercentGrowth = reader.IsDBNull(10) ? null : (bool?)(Convert.ToInt32(reader.GetValue(10)) == 1),
                GrowthPct = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)),
                VlfCount = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets per-database total allocated and used space for the utilization size chart.
    /// Aggregates across all files per database for the selected server.
    /// </summary>
    public async Task<List<DatabaseSizeSummaryRow>> GetDatabaseSizeSummaryAsync(int serverId, int topN = 10)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    database_name,
    SUM(total_size_mb) AS total_mb,
    SUM(used_size_mb) AS used_mb
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1
)
GROUP BY database_name
ORDER BY total_mb DESC
LIMIT $2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = topN });

        var items = new List<DatabaseSizeSummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeSummaryRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                UsedMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2))
            });
        }
        return items;
    }

    /// <summary>
    /// Detects databases with zero query executions over the last N days.
    /// </summary>
    public async Task<List<IdleDatabaseRow>> GetIdleDatabasesAsync(int serverId, int daysBack = 7)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        command.CommandText = @"
WITH db_sizes AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS total_size_mb,
        COUNT(*) AS file_count
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    GROUP BY database_name
),
db_activity AS (
    SELECT
        database_name,
        SUM(delta_execution_count) AS total_executions,
        MAX(last_execution_time) AS last_execution
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_execution_count IS NOT NULL
    GROUP BY database_name
)
SELECT
    ds.database_name,
    ds.total_size_mb,
    ds.file_count,
    a.last_execution
FROM db_sizes ds
LEFT JOIN db_activity a ON a.database_name = ds.database_name
WHERE COALESCE(a.total_executions, 0) = 0
AND   ds.database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
ORDER BY ds.total_size_mb DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<IdleDatabaseRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new IdleDatabaseRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
            });
        }
        return items;
    }

    /// <summary>
    /// Gets tempdb pressure summary: latest and 24h peak values.
    /// </summary>
    public async Task<List<TempdbSummaryRow>> GetTempdbSummaryAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        command.CommandText = @"
WITH latest AS (
    SELECT
        user_object_reserved_mb,
        internal_object_reserved_mb,
        version_store_reserved_mb,
        total_reserved_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
peak AS (
    SELECT
        MAX(user_object_reserved_mb) AS max_user_mb,
        MAX(internal_object_reserved_mb) AS max_internal_mb,
        MAX(version_store_reserved_mb) AS max_version_store_mb,
        MAX(total_reserved_mb) AS max_total_mb
    FROM v_tempdb_stats
    WHERE server_id = $1
    AND   collection_time >= $2
)
SELECT 'User Objects', l.user_object_reserved_mb, p.max_user_mb,
    CASE WHEN p.max_user_mb > 1024 THEN 'High user object usage' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Internal Objects', l.internal_object_reserved_mb, p.max_internal_mb,
    CASE WHEN p.max_internal_mb > 1024 THEN 'High internal object usage (sorts/hashes)' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Version Store', l.version_store_reserved_mb, p.max_version_store_mb,
    CASE WHEN p.max_version_store_mb > 2048 THEN 'Version store pressure — check long-running transactions' ELSE '' END
FROM latest l CROSS JOIN peak p
UNION ALL
SELECT 'Total Reserved', l.total_reserved_mb, p.max_total_mb, ''
FROM latest l CROSS JOIN peak p";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<TempdbSummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TempdbSummaryRow
            {
                Metric = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Peak24hMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                Warning = reader.IsDBNull(3) ? "" : reader.GetString(3)
            });
        }
        return items;
    }
}
