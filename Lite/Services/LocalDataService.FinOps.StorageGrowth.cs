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
    /// Per-database size now, 7 days ago and 30 days ago. $1 server_id, $2 the 7-day mark, $3 the 30-day mark.
    ///
    /// <para>Each baseline is the snapshot NEAREST its mark, and only when that snapshot is within one day of it
    /// (86400 seconds). The older shape took the newest snapshot at or before the mark, however far
    /// before: a store with 25 days of history then called its 25-day-old sample "30d ago" and measured Growth % and the daily
    /// rate against it, and a store with a gap labeled whatever sample the gap left as the 7-day baseline. With no snapshot near
    /// the mark the baseline is NULL, and the grid shows n/a with a tooltip saying no sample from that many days ago exists.</para>
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
    /// <para>#5498: a row named "(whole database)" for another database on an Azure SQL Database server is history from
    /// before the collector read each user database on its own connection: it holds data space only, and the newest
    /// snapshot of that database now holds its data and log files
    /// (<see cref="PerformanceMonitor.Common.AzureSiblingDatabaseSize"/>). The 7-day and 30-day sums leave EVERY such
    /// row out (<c>ExcludeAllSiblingRows</c>), so the log does not read as growth. The latest sum leaves out only the
    /// old shape that holds no used space (<c>ExcludePreFixRows</c>), so a snapshot taken before the upgrade still
    /// lists the database. A database with no comparable older row shows a blank past size and growth n/a (null), as
    /// a database added inside the window does, until a newer sample is old enough to compare against.</para>
    ///
    /// <para>The <c>latest</c> CTE also flags each database whose size leaves its log out: <c>has_sibling_row</c> is true
    /// when the database has the one row another database on an Azure SQL Database server gets
    /// (<see cref="PerformanceMonitor.Common.AzureSiblingDatabaseSize.RowPredicate"/>, so its size is data space only),
    /// and <c>has_log_service_file</c> is true when it has a row in <c>log_service_files</c> (the Hyperscale log, which
    /// the sums skip). Both come from the latest snapshot only, and the row's <c>Note</c> says which.</para>
    /// </summary>
    internal const string StorageGrowthSql = @"
WITH snaps AS (
    SELECT DISTINCT collection_time AS t
    FROM v_database_size_stats
    WHERE server_id = $1
),
base_7d AS (
    SELECT t
    FROM snaps
    WHERE t < (SELECT MAX(t) FROM snaps)
    AND   abs(epoch(t) - epoch(CAST($2 AS TIMESTAMP))) <= 86400
    ORDER BY abs(epoch(t) - epoch(CAST($2 AS TIMESTAMP))), t DESC
    LIMIT 1
),
base_30d AS (
    SELECT t
    FROM snaps
    WHERE t < (SELECT MAX(t) FROM snaps)
    AND   abs(epoch(t) - epoch(CAST($3 AS TIMESTAMP))) <= 86400
    ORDER BY abs(epoch(t) - epoch(CAST($3 AS TIMESTAMP))), t DESC
    LIMIT 1
),
log_service_files AS (
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
    AND   s.collection_time = (SELECT t FROM base_7d)
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    AND   " + AzureSiblingDatabaseSize.ExcludeAllSiblingRows + @"
    GROUP BY s.database_name
),
past_30d AS (
    SELECT
        s.database_name,
        SUM(s.total_size_mb) AS size_mb,
        MAX(s.collection_time) AS snap_time
    FROM v_database_size_stats AS s
    WHERE s.server_id = $1
    AND   s.collection_time = (SELECT t FROM base_30d)
    AND   NOT EXISTS (
        SELECT 1
        FROM log_service_files AS ls
        WHERE ls.database_name = s.database_name
        AND   ls.file_id = s.file_id
    )
    AND   " + AzureSiblingDatabaseSize.ExcludeAllSiblingRows + @"
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
    /// #5312: <paramref name="databaseNames"/> is the saved database filter (the Storage Growth grid also lists the databases the
    /// table and index drill starts from); null or empty is every database. The rows are narrowed after the read, by exact
    /// (ordinal) name, the same rule as the viewer's twin. The statement has no row cap, so the narrowing cannot lose a chosen
    /// database. A row whose name was NULL reads as "" and never matches a chosen name.
    /// </summary>
    public async Task<List<StorageGrowthRow>> GetStorageGrowthAsync(int serverId, IReadOnlyList<string>? databaseNames = null)
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
        if (databaseNames is { Count: > 0 })
        {
            var chosen = new HashSet<string>(databaseNames, StringComparer.Ordinal);
            items = items.Where(r => chosen.Contains(r.DatabaseName)).ToList();
        }

        return items;
    }
}
