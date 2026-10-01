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
    /// Per-database size now, 7 days ago and 30 days ago. $1 server_id, $2 the 7-day cutoff, $3 the 30-day cutoff.
    ///
    /// <para>A file whose row in the latest snapshot has no size is left out of all three sums, by one predicate
    /// (the <c>NOT EXISTS</c> against <c>log_service_files</c>) repeated in each. That file is the log of an Azure
    /// SQL Database Hyperscale database (<see cref="PerformanceMonitor.Common.HyperscaleLogSize"/>). Its older rows
    /// can still hold the ~1 TB that sys.database_files reported before the collector stored NULL for it, and
    /// summing them on the past side alone read as a -99% drop. The rule is applied at read time, so it covers
    /// history collected before the change without rewriting it. On the latest side it drops only the rows SUM
    /// already skips. A file that is gone from the latest snapshot has no row there, so it still counts on the
    /// past side, as shrinkage.</para>
    /// </summary>
    internal const string StorageGrowthSql = @"
WITH log_service_files AS (
    SELECT
        database_name,
        file_id
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    AND   total_size_mb IS NULL
),
latest AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS current_size_mb
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    GROUP BY s.database_name
),
past_7d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $2
    )
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    GROUP BY s.database_name
),
past_30d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $3
    )
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    GROUP BY s.database_name
)
SELECT
    l.database_name,
    l.current_size_mb,
    p7.size_mb,
    p30.size_mb,
    l.current_size_mb - COALESCE(p7.size_mb, l.current_size_mb) AS growth_7d_mb,
    l.current_size_mb - COALESCE(p30.size_mb, l.current_size_mb) AS growth_30d_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL
        THEN (l.current_size_mb - p30.size_mb) / 30.0
        WHEN p7.size_mb IS NOT NULL
        THEN (l.current_size_mb - p7.size_mb) / 7.0
        ELSE 0
    END AS daily_growth_rate_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL AND p30.size_mb > 0
        THEN (l.current_size_mb - p30.size_mb) * 100.0 / p30.size_mb
        ELSE 0
    END AS growth_pct_30d
FROM latest l
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name
ORDER BY growth_30d_mb DESC";

    /// <summary>
    /// Gets per-database storage growth trends comparing current size to 7d and 30d ago.
    /// </summary>
    public async Task<List<StorageGrowthRow>> GetStorageGrowthAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var now = DateTime.UtcNow;
        var cutoff7d = now.AddDays(-7);
        var cutoff30d = now.AddDays(-30);

        command.CommandText = StorageGrowthSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff7d });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff30d });

        var items = new List<StorageGrowthRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new StorageGrowthRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                CurrentSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                Size7dAgoMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                Size30dAgoMb = reader.IsDBNull(3) ? null : Convert.ToDecimal(reader.GetValue(3)),
                Growth7dMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                Growth30dMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                DailyGrowthRateMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                GrowthPct30d = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7))
            });
        }
        return items;
    }
}
