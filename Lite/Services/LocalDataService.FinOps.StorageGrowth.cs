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
using PerformanceMonitor.Common;

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
    ///
    /// <para>A row stored before the allocated/used fix for another database on an Azure SQL Database server holds that
    /// database's USED space as its total, where every later row holds the ALLOCATED size
    /// (<see cref="PerformanceMonitor.Common.AzureSiblingDatabaseSize"/>). The same predicate leaves those rows out of
    /// all three sums, so the one-time change reads as no history and not as growth: the database shows a blank past
    /// size and growth n/a (null) until a newer sample is old enough to compare against, as a database added inside the window
    /// does. Until the first collection after the upgrade the latest snapshot holds only old-shape rows, so the
    /// database is not listed here at all.</para>
    ///
    /// <para>The <c>latest</c> CTE also flags each database whose size leaves its log out: <c>has_sibling_row</c> is true
    /// when the database has the one row another database on an Azure SQL Database server gets
    /// (<see cref="PerformanceMonitor.Common.AzureSiblingDatabaseSize.RowPredicate"/>, so its size is data space only),
    /// and <c>has_log_service_file</c> is true when it has a row in <c>log_service_files</c> (the Hyperscale log, which
    /// the sums skip). Both come from the latest snapshot only, and the row's <c>Note</c> says which.</para>
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
        SUM(s.total_size_mb) AS current_size_mb,
        MAX(s.collection_time) AS snap_time,
        bool_or(" + AzureSiblingDatabaseSize.RowPredicate + @") AS has_sibling_row,
        EXISTS (
            SELECT 1
            FROM log_service_files AS ls
            WHERE ls.database_name = s.database_name
        ) AS has_log_service_file
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
    AND   " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
    GROUP BY s.database_name
),
past_7d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb,
        MAX(s.collection_time) AS snap_time
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $2
    )
    AND   s.collection_time < (
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
    AND   " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
    GROUP BY s.database_name
),
past_30d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb,
        MAX(s.collection_time) AS snap_time
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $3
    )
    AND   s.collection_time < (
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
    AND   " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
    GROUP BY s.database_name
)
SELECT
    l.database_name,
    l.current_size_mb,
    p7.size_mb,
    p30.size_mb,
    l.current_size_mb - p7.size_mb AS growth_7d_mb,
    l.current_size_mb - p30.size_mb AS growth_30d_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL
        THEN (l.current_size_mb - p30.size_mb) / NULLIF(date_diff('second', p30.snap_time, l.snap_time) / 86400.0, 0)
        WHEN p7.size_mb IS NOT NULL
        THEN (l.current_size_mb - p7.size_mb) / NULLIF(date_diff('second', p7.snap_time, l.snap_time) / 86400.0, 0)
        ELSE NULL
    END AS daily_growth_rate_mb,
    CASE
        WHEN p30.size_mb IS NOT NULL AND p30.size_mb > 0
        THEN (l.current_size_mb - p30.size_mb) * 100.0 / p30.size_mb
        ELSE NULL
    END AS growth_pct_30d,
    l.has_sibling_row,
    l.has_log_service_file
FROM latest l
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name
ORDER BY growth_30d_mb DESC NULLS LAST, growth_7d_mb DESC NULLS LAST, l.database_name";

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
                Growth7dMb = reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                Growth30dMb = reader.IsDBNull(5) ? null : Convert.ToDecimal(reader.GetValue(5)),
                DailyGrowthRateMb = reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6)),
                GrowthPct30d = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                HasSiblingRow = !reader.IsDBNull(8) && reader.GetBoolean(8),
                HasLogServiceFile = !reader.IsDBNull(9) && reader.GetBoolean(9)
            });
        }
        return items;
    }
}
