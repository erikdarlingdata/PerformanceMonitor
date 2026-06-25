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
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // FinOps — Object/Index stats (sizes+growth, usage, locking)
        // Reads collect.index_object_stats. Sizes are absolute; usage/locking
        // counters are cumulative since sqlserver_start_time (shown as totals).
        // ============================================

        /// <summary>
        /// Per-table size and growth (indexes rolled up per table), comparing the latest
        /// snapshot to 7d/30d ago, with a daily growth rate over the available history.
        /// </summary>
        public async Task<List<ObjectSizeGrowthRow>> GetObjectSizeGrowthAsync(int topN = 100)
        {
            var items = new List<ObjectSizeGrowthRow>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    boundaries AS
    (
        SELECT
            latest_time = MAX(collection_time),
            earliest_time = MIN(collection_time),
            days_of_data = DATEDIFF(DAY, MIN(collection_time), MAX(collection_time))
        FROM collect.index_object_stats
    ),
    latest AS
    (
        SELECT
            database_name,
            schema_name,
            table_name,
            current_reserved_mb = SUM(reserved_mb),
            current_used_mb = SUM(used_mb),
            total_rows = MAX(total_rows),
            index_count = COUNT_BIG(*)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT b.latest_time
            FROM boundaries AS b
        )
        GROUP BY
            database_name,
            schema_name,
            table_name
    ),
    past_7d AS
    (
        SELECT
            database_name,
            schema_name,
            table_name,
            reserved_mb = SUM(reserved_mb)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.index_object_stats
            WHERE collection_time <= DATEADD(DAY, -7, SYSDATETIME())
        )
        GROUP BY
            database_name,
            schema_name,
            table_name
    ),
    past_30d AS
    (
        SELECT
            database_name,
            schema_name,
            table_name,
            reserved_mb = SUM(reserved_mb)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT MAX(collection_time)
            FROM collect.index_object_stats
            WHERE collection_time <= DATEADD(DAY, -30, SYSDATETIME())
        )
        GROUP BY
            database_name,
            schema_name,
            table_name
    ),
    oldest AS
    (
        SELECT
            database_name,
            schema_name,
            table_name,
            reserved_mb = SUM(reserved_mb)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT b.earliest_time
            FROM boundaries AS b
        )
        GROUP BY
            database_name,
            schema_name,
            table_name
    )
SELECT TOP (@topN)
    l.database_name,
    l.schema_name,
    l.table_name,
    l.current_reserved_mb,
    l.current_used_mb,
    l.total_rows,
    l.index_count,
    growth_7d_mb =
        l.current_reserved_mb - COALESCE(p7.reserved_mb, o.reserved_mb, l.current_reserved_mb),
    growth_30d_mb =
        l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb, l.current_reserved_mb),
    daily_growth_rate_mb =
        CASE
            WHEN b.days_of_data >= 1
            THEN (l.current_reserved_mb - COALESCE(o.reserved_mb, l.current_reserved_mb)) / CAST(b.days_of_data AS decimal(10,1))
            ELSE 0
        END,
    growth_pct_30d =
        CASE
            WHEN COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb) > 0
            THEN (l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb)) * 100.0
                 / COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb)
            ELSE 0
        END
FROM latest AS l
CROSS JOIN boundaries AS b
LEFT JOIN past_7d AS p7
  ON  p7.database_name = l.database_name
  AND p7.schema_name = l.schema_name
  AND p7.table_name = l.table_name
LEFT JOIN past_30d AS p30
  ON  p30.database_name = l.database_name
  AND p30.schema_name = l.schema_name
  AND p30.table_name = l.table_name
LEFT JOIN oldest AS o
  ON  o.database_name = l.database_name
  AND o.schema_name = l.schema_name
  AND o.table_name = l.table_name
ORDER BY
    l.current_reserved_mb DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_ObjectSizeGrowth", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new ObjectSizeGrowthRow
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        TableName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        CurrentReservedMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        CurrentUsedMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        TotalRows = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                        IndexCount = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                        Growth7dMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        Growth30dMb = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8)),
                        DailyGrowthRateMb = reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                        GrowthPct30d = reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10))
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Per-index usage from the latest snapshot, surfacing unused and write-only indexes.
        /// Counters are cumulative since the last instance restart (sqlserver_start_time).
        /// </summary>
        public async Task<List<IndexUsageRow>> GetIndexUsageAsync(int topN = 200)
        {
            var items = new List<IndexUsageRow>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (@topN)
    ios.database_name,
    ios.schema_name,
    ios.table_name,
    ios.index_name,
    ios.index_type_desc,
    ios.index_id,
    ios.reserved_mb,
    ios.total_rows,
    user_seeks = ISNULL(ios.user_seeks, 0),
    user_scans = ISNULL(ios.user_scans, 0),
    user_lookups = ISNULL(ios.user_lookups, 0),
    total_reads = ios.total_reads,
    user_updates = ISNULL(ios.user_updates, 0),
    last_user_access =
    (
        SELECT MAX(v)
        FROM
        (
            VALUES
                (ios.last_user_seek),
                (ios.last_user_scan),
                (ios.last_user_lookup),
                (ios.last_user_update)
        ) AS x (v)
    ),
    classification =
        CASE
            WHEN ios.total_reads = 0 AND ISNULL(ios.user_updates, 0) = 0
            THEN N'Unused'
            WHEN ios.total_reads = 0 AND ISNULL(ios.user_updates, 0) > 0
            THEN N'Write-only'
            ELSE N'Active'
        END
FROM collect.index_object_stats AS ios
WHERE ios.collection_time =
(
    SELECT MAX(collection_time)
    FROM collect.index_object_stats
)
ORDER BY
    CASE WHEN ios.total_reads = 0 THEN 0 ELSE 1 END,
    ios.reserved_mb DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_IndexUsage", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new IndexUsageRow
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        TableName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        IndexName = reader.IsDBNull(3) ? "(heap)" : reader.GetString(3),
                        IndexTypeDesc = reader.IsDBNull(4) ? "" : reader.GetString(4),
                        IndexId = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                        ReservedMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                        TotalRows = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                        UserSeeks = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                        UserScans = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                        UserLookups = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                        TotalReads = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                        UserUpdates = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                        LastUserAccess = reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                        Classification = reader.IsDBNull(14) ? "" : reader.GetString(14)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Per-index locking/latch contention from the latest snapshot, top objects by lock waits.
        /// Counters are cumulative since the metadata last entered cache (≈ instance restart).
        /// </summary>
        public async Task<List<IndexLockingRow>> GetIndexLockingAsync(int topN = 200)
        {
            var items = new List<IndexLockingRow>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (@topN)
    ios.database_name,
    ios.schema_name,
    ios.table_name,
    ios.index_name,
    ios.index_type_desc,
    ios.reserved_mb,
    ios.total_rows,
    row_lock_count = ISNULL(ios.row_lock_count, 0),
    row_lock_wait_count = ISNULL(ios.row_lock_wait_count, 0),
    row_lock_wait_in_ms = ISNULL(ios.row_lock_wait_in_ms, 0),
    page_lock_count = ISNULL(ios.page_lock_count, 0),
    page_lock_wait_count = ISNULL(ios.page_lock_wait_count, 0),
    page_lock_wait_in_ms = ISNULL(ios.page_lock_wait_in_ms, 0),
    index_lock_promotion_count = ISNULL(ios.index_lock_promotion_count, 0),
    page_latch_wait_in_ms = ISNULL(ios.page_latch_wait_in_ms, 0),
    page_io_latch_wait_in_ms = ISNULL(ios.page_io_latch_wait_in_ms, 0)
FROM collect.index_object_stats AS ios
WHERE ios.collection_time =
(
    SELECT MAX(collection_time)
    FROM collect.index_object_stats
)
AND
(
    ISNULL(ios.row_lock_wait_in_ms, 0) > 0
    OR ISNULL(ios.page_lock_wait_in_ms, 0) > 0
    OR ISNULL(ios.page_latch_wait_in_ms, 0) > 0
    OR ISNULL(ios.page_io_latch_wait_in_ms, 0) > 0
    OR ISNULL(ios.index_lock_promotion_count, 0) > 0
)
ORDER BY
    ISNULL(ios.row_lock_wait_in_ms, 0)
    + ISNULL(ios.page_lock_wait_in_ms, 0)
    + ISNULL(ios.page_latch_wait_in_ms, 0)
    + ISNULL(ios.page_io_latch_wait_in_ms, 0) DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@topN", topN);
            command.CommandTimeout = 120;

            using (StartQueryTiming("FinOps_IndexLocking", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new IndexLockingRow
                    {
                        DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        TableName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        IndexName = reader.IsDBNull(3) ? "(heap)" : reader.GetString(3),
                        IndexTypeDesc = reader.IsDBNull(4) ? "" : reader.GetString(4),
                        ReservedMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                        TotalRows = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                        RowLockCount = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                        RowLockWaitCount = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                        RowLockWaitInMs = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                        PageLockCount = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                        PageLockWaitCount = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                        PageLockWaitInMs = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                        IndexLockPromotionCount = reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
                        PageLatchWaitInMs = reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
                        PageIoLatchWaitInMs = reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15))
                    });
                }
            }

            return items;
        }
    }
}

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>Per-table size + growth (indexes rolled up).</summary>
    public class ObjectSizeGrowthRow
    {
        public string DatabaseName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string TableName { get; set; } = "";
        public decimal CurrentReservedMb { get; set; }
        public decimal CurrentUsedMb { get; set; }
        public long TotalRows { get; set; }
        public int IndexCount { get; set; }
        public decimal Growth7dMb { get; set; }
        public decimal Growth30dMb { get; set; }
        public decimal DailyGrowthRateMb { get; set; }
        public decimal GrowthPct30d { get; set; }
    }

    /// <summary>Per-index usage with unused/write-only classification.</summary>
    public class IndexUsageRow
    {
        public string DatabaseName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string TableName { get; set; } = "";
        public string IndexName { get; set; } = "";
        public string IndexTypeDesc { get; set; } = "";
        public int IndexId { get; set; }
        public decimal ReservedMb { get; set; }
        public long TotalRows { get; set; }
        public long UserSeeks { get; set; }
        public long UserScans { get; set; }
        public long UserLookups { get; set; }
        public long TotalReads { get; set; }
        public long UserUpdates { get; set; }
        public DateTime? LastUserAccess { get; set; }
        public string Classification { get; set; } = "";
    }

    /// <summary>Per-index locking/latch contention.</summary>
    public class IndexLockingRow
    {
        public string DatabaseName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string TableName { get; set; } = "";
        public string IndexName { get; set; } = "";
        public string IndexTypeDesc { get; set; } = "";
        public decimal ReservedMb { get; set; }
        public long TotalRows { get; set; }
        public long RowLockCount { get; set; }
        public long RowLockWaitCount { get; set; }
        public long RowLockWaitInMs { get; set; }
        public long PageLockCount { get; set; }
        public long PageLockWaitCount { get; set; }
        public long PageLockWaitInMs { get; set; }
        public long IndexLockPromotionCount { get; set; }
        public long PageLatchWaitInMs { get; set; }
        public long PageIoLatchWaitInMs { get; set; }
    }
}
