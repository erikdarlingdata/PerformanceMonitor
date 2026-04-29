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
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // FinOps — Storage (Sizes, Growth, Idle, Tempdb)
        // ============================================

        /// <summary>
        /// Fetches latest database size stats from collect.database_size_stats.
        /// </summary>
        public async Task<List<FinOpsDatabaseSizeStats>> GetFinOpsDatabaseSizeStatsAsync()
        {
            var items = new List<FinOpsDatabaseSizeStats>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    collection_time,
                    database_name,
                    database_id,
                    file_id,
                    file_type_desc,
                    file_name,
                    physical_name,
                    total_size_mb,
                    used_size_mb,
                    free_space_mb,
                    used_pct,
                    auto_growth_mb,
                    max_size_mb,
                    recovery_model_desc,
                    compatibility_level,
                    state_desc,
                    volume_mount_point,
                    volume_total_mb,
                    volume_free_mb,
                    is_percent_growth,
                    growth_pct,
                    vlf_count
                FROM collect.database_size_stats
                WHERE collection_time =
                (
                    SELECT
                        MAX(collection_time)
                    FROM collect.database_size_stats
                )
                ORDER BY
                    database_name,
                    file_type_desc,
                    file_name
                OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_DatabaseSizeStats", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsDatabaseSizeStats
                    {
                        CollectionTime = reader.IsDBNull(0) ? DateTime.MinValue : reader.GetDateTime(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        DatabaseId = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        FileId = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                        FileTypeDesc = reader.IsDBNull(4) ? "" : reader.GetString(4),
                        FileName = reader.IsDBNull(5) ? "" : reader.GetString(5),
                        PhysicalName = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        TotalSizeMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        UsedSizeMb = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8)),
                        FreeSpaceMb = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                        UsedPct = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10)),
                        AutoGrowthMb = reader.IsDBNull(11) ? 0m : Convert.ToDecimal(reader.GetValue(11)),
                        MaxSizeMb = reader.IsDBNull(12) ? 0m : Convert.ToDecimal(reader.GetValue(12)),
                        RecoveryModelDesc = reader.IsDBNull(13) ? "" : reader.GetString(13),
                        CompatibilityLevel = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                        StateDesc = reader.IsDBNull(15) ? "" : reader.GetString(15),
                        VolumeMountPoint = reader.IsDBNull(16) ? "" : reader.GetString(16),
                        VolumeTotalMb = reader.IsDBNull(17) ? 0m : Convert.ToDecimal(reader.GetValue(17)),
                        VolumeFreeMb = reader.IsDBNull(18) ? 0m : Convert.ToDecimal(reader.GetValue(18)),
                        IsPercentGrowth = reader.IsDBNull(19) ? null : (bool?)(Convert.ToInt32(reader.GetValue(19)) == 1),
                        GrowthPct = reader.IsDBNull(20) ? null : Convert.ToInt32(reader.GetValue(20)),
                        VlfCount = reader.IsDBNull(21) ? null : Convert.ToInt32(reader.GetValue(21))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets per-database total allocated and used space for the utilization size chart.
        /// </summary>
        public async Task<List<FinOpsDatabaseSizeSummary>> GetFinOpsDatabaseSizeSummaryAsync(int topN = 10)
        {
            var items = new List<FinOpsDatabaseSizeSummary>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP(@topN)
    database_name,
    total_mb =
        SUM(total_size_mb),
    used_mb =
        SUM(used_size_mb)
FROM collect.database_size_stats
WHERE collection_time =
(
    SELECT MAX(collection_time)
    FROM collect.database_size_stats
)
GROUP BY
    database_name
ORDER BY
    SUM(total_size_mb) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_DatabaseSizeSummary", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsDatabaseSizeSummary
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        UsedMb = reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets per-database storage growth trends comparing current size to 7d and 30d ago.
        /// </summary>
        public async Task<List<FinOpsStorageGrowthRow>> GetFinOpsStorageGrowthAsync()
        {
            var items = new List<FinOpsStorageGrowthRow>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    boundaries AS
    (
        SELECT
            latest_time  = MAX(collection_time),
            earliest_time = MIN(collection_time),
            days_of_data = DATEDIFF(DAY, MIN(collection_time), MAX(collection_time))
        FROM collect.database_size_stats
    ),
    latest AS
    (
        SELECT
            database_name,
            current_size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT latest_time
            FROM boundaries
        )
        GROUP BY
            database_name
    ),
    past_7d AS
    (
        SELECT
            database_name,
            size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.database_size_stats
            WHERE collection_time <= DATEADD(DAY, -7, SYSDATETIME())
        )
        GROUP BY
            database_name
    ),
    past_30d AS
    (
        SELECT
            database_name,
            size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.database_size_stats
            WHERE collection_time <= DATEADD(DAY, -30, SYSDATETIME())
        )
        GROUP BY
            database_name
    ),
    oldest AS
    (
        SELECT
            database_name,
            size_mb =
                SUM(total_size_mb)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT earliest_time
            FROM boundaries
        )
        GROUP BY
            database_name
    )
SELECT
    l.database_name,
    l.current_size_mb,
    COALESCE(p7.size_mb, o.size_mb),
    COALESCE(p30.size_mb, p7.size_mb, o.size_mb),
    growth_7d_mb =
        l.current_size_mb - COALESCE(p7.size_mb, o.size_mb, l.current_size_mb),
    growth_30d_mb =
        l.current_size_mb - COALESCE(p30.size_mb, p7.size_mb, o.size_mb, l.current_size_mb),
    daily_growth_rate_mb =
        CASE
            WHEN b.days_of_data >= 1
            THEN (l.current_size_mb - COALESCE(o.size_mb, l.current_size_mb)) / CAST(b.days_of_data AS decimal(10,1))
            ELSE 0
        END,
    growth_pct_30d =
        CASE
            WHEN COALESCE(p30.size_mb, p7.size_mb, o.size_mb) IS NOT NULL
            AND  COALESCE(p30.size_mb, p7.size_mb, o.size_mb) > 0
            THEN (l.current_size_mb - COALESCE(p30.size_mb, p7.size_mb, o.size_mb)) * 100.0
                 / COALESCE(p30.size_mb, p7.size_mb, o.size_mb)
            ELSE 0
        END
FROM latest AS l
CROSS JOIN boundaries AS b
LEFT JOIN past_7d AS p7
  ON p7.database_name = l.database_name
LEFT JOIN past_30d AS p30
  ON p30.database_name = l.database_name
LEFT JOIN oldest AS o
  ON o.database_name = l.database_name
ORDER BY
    l.current_size_mb - COALESCE(p30.size_mb, p7.size_mb, o.size_mb, l.current_size_mb) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_StorageGrowth", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsStorageGrowthRow
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
            }

            return items;
        }

        /// <summary>
        /// Detects databases with zero query executions over the last N days.
        /// </summary>
        public async Task<List<FinOpsIdleDatabase>> GetFinOpsIdleDatabasesAsync(int daysBack = 7)
        {
            var items = new List<FinOpsIdleDatabase>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    db_sizes AS
    (
        SELECT
            database_name,
            total_size_mb =
                SUM(total_size_mb),
            file_count =
                COUNT(*)
        FROM collect.database_size_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.database_size_stats
        )
        GROUP BY
            database_name
    ),
    db_activity AS
    (
        SELECT
            database_name,
            total_executions =
                SUM(execution_count_delta),
            last_execution =
                MAX(last_execution_time)
        FROM collect.query_stats
        WHERE collection_time >= DATEADD(DAY, -@daysBack, SYSDATETIME())
        AND   execution_count_delta IS NOT NULL
        GROUP BY
            database_name
    )
SELECT
    ds.database_name,
    ds.total_size_mb,
    ds.file_count,
    a.last_execution
FROM db_sizes AS ds
LEFT JOIN db_activity AS a
  ON a.database_name = ds.database_name
WHERE ISNULL(a.total_executions, 0) = 0
AND   ds.database_name NOT IN (N'master', N'model', N'msdb', N'tempdb', N'PerformanceMonitor')
ORDER BY
    ds.total_size_mb DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@daysBack", daysBack);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_IdleDatabases", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsIdleDatabase
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                        LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets tempdb pressure summary: latest and 24h peak values.
        /// </summary>
        public async Task<List<FinOpsTempdbSummary>> GetFinOpsTempdbSummaryAsync()
        {
            var items = new List<FinOpsTempdbSummary>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    latest AS
    (
        SELECT TOP (1)
            user_object_reserved_mb,
            internal_object_reserved_mb,
            version_store_reserved_mb,
            total_reserved_mb
        FROM collect.tempdb_stats
        ORDER BY
            collection_time DESC
    ),
    peak AS
    (
        SELECT
            max_user_mb =
                MAX(user_object_reserved_mb),
            max_internal_mb =
                MAX(internal_object_reserved_mb),
            max_version_store_mb =
                MAX(version_store_reserved_mb),
            max_total_mb =
                MAX(total_reserved_mb)
        FROM collect.tempdb_stats
        WHERE collection_time >= DATEADD(HOUR, -24, SYSDATETIME())
    )
SELECT
    metric = N'User Objects',
    current_mb = l.user_object_reserved_mb,
    peak_24h_mb = p.max_user_mb,
    warning =
        CASE
            WHEN p.max_user_mb > 1024
            THEN N'High user object usage'
            ELSE N''
        END
FROM latest AS l
CROSS JOIN peak AS p
UNION ALL
SELECT
    N'Internal Objects',
    l.internal_object_reserved_mb,
    p.max_internal_mb,
    CASE
        WHEN p.max_internal_mb > 1024
        THEN N'High internal object usage (sorts/hashes)'
        ELSE N''
    END
FROM latest AS l
CROSS JOIN peak AS p
UNION ALL
SELECT
    N'Version Store',
    l.version_store_reserved_mb,
    p.max_version_store_mb,
    CASE
        WHEN p.max_version_store_mb > 2048
        THEN N'Version store pressure — check long-running transactions'
        ELSE N''
    END
FROM latest AS l
CROSS JOIN peak AS p
UNION ALL
SELECT
    N'Total Reserved',
    l.total_reserved_mb,
    p.max_total_mb,
    N''
FROM latest AS l
CROSS JOIN peak AS p
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_TempdbSummary", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new FinOpsTempdbSummary
                    {
                        Metric = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        CurrentMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                        Peak24hMb = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                        Warning = reader.IsDBNull(3) ? "" : reader.GetString(3)
                    });
                }
            }

            return items;
        }
    }
}
