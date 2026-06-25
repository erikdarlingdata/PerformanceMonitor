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
    // ============================================
    // FinOps — Object/Index stats (sizes+growth, usage, locking).
    // Reads v_index_object_stats (hot DuckDB UNION archived parquet).
    // ============================================

    /// <summary>
    /// Per-table size and growth (indexes rolled up per table) for a server, comparing the
    /// latest snapshot to 7d/30d ago, with a daily growth rate over the available history.
    /// </summary>
    public async Task<List<ObjectSizeGrowthRow>> GetObjectSizeGrowthAsync(int serverId, int topN = 100)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var now = DateTime.UtcNow;
        var cutoff7d = now.AddDays(-7);
        var cutoff30d = now.AddDays(-30);

        command.CommandText = $@"
WITH boundaries AS (
    SELECT
        MAX(collection_time) AS latest_time,
        MIN(collection_time) AS earliest_time,
        date_diff('day', MIN(collection_time), MAX(collection_time)) AS days_of_data
    FROM v_index_object_stats
    WHERE server_id = $1
),
latest AS (
    SELECT database_name, schema_name, table_name,
        SUM(reserved_mb) AS current_reserved_mb,
        SUM(used_mb) AS current_used_mb,
        MAX(total_rows) AS total_rows,
        COUNT(*) AS index_count
    FROM v_index_object_stats
    WHERE server_id = $1 AND collection_time = (SELECT latest_time FROM boundaries)
    GROUP BY database_name, schema_name, table_name
),
past_7d AS (
    SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND collection_time = (
        SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND collection_time <= $2)
    GROUP BY database_name, schema_name, table_name
),
past_30d AS (
    SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND collection_time = (
        SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND collection_time <= $3)
    GROUP BY database_name, schema_name, table_name
),
oldest AS (
    SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND collection_time = (SELECT earliest_time FROM boundaries)
    GROUP BY database_name, schema_name, table_name
)
SELECT
    l.database_name,
    l.schema_name,
    l.table_name,
    l.current_reserved_mb,
    l.current_used_mb,
    l.total_rows,
    l.index_count,
    l.current_reserved_mb - COALESCE(p7.reserved_mb, o.reserved_mb, l.current_reserved_mb) AS growth_7d_mb,
    l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb, l.current_reserved_mb) AS growth_30d_mb,
    CASE WHEN b.days_of_data >= 1
         THEN (l.current_reserved_mb - COALESCE(o.reserved_mb, l.current_reserved_mb)) / CAST(b.days_of_data AS DOUBLE)
         ELSE 0 END AS daily_growth_rate_mb,
    CASE WHEN COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb) > 0
         THEN (l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb)) * 100.0
              / COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb)
         ELSE 0 END AS growth_pct_30d
FROM latest l
CROSS JOIN boundaries b
LEFT JOIN past_7d p7 ON p7.database_name = l.database_name AND p7.schema_name = l.schema_name AND p7.table_name = l.table_name
LEFT JOIN past_30d p30 ON p30.database_name = l.database_name AND p30.schema_name = l.schema_name AND p30.table_name = l.table_name
LEFT JOIN oldest o ON o.database_name = l.database_name AND o.schema_name = l.schema_name AND o.table_name = l.table_name
ORDER BY l.current_reserved_mb DESC
LIMIT {topN}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff7d });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff30d });

        var items = new List<ObjectSizeGrowthRow>();
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
        return items;
    }

    /// <summary>
    /// Per-index usage from the latest snapshot for a server, surfacing unused/write-only indexes.
    /// </summary>
    public async Task<List<IndexUsageRow>> GetIndexUsageAsync(int serverId, int topN = 200)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = $@"
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    index_type_desc,
    index_id,
    reserved_mb,
    total_rows,
    COALESCE(user_seeks, 0) AS user_seeks,
    COALESCE(user_scans, 0) AS user_scans,
    COALESCE(user_lookups, 0) AS user_lookups,
    COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) AS total_reads,
    COALESCE(user_updates, 0) AS user_updates,
    GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update) AS last_user_access,
    CASE
        WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0
             AND COALESCE(user_updates, 0) = 0 THEN 'Unused'
        WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0
             AND COALESCE(user_updates, 0) > 0 THEN 'Write-only'
        ELSE 'Active'
    END AS classification
FROM v_index_object_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
ORDER BY
    CASE WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0 THEN 0 ELSE 1 END,
    reserved_mb DESC
LIMIT {topN}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<IndexUsageRow>();
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
        return items;
    }

    /// <summary>
    /// Per-index locking/latch contention from the latest snapshot for a server.
    /// </summary>
    public async Task<List<IndexLockingRow>> GetIndexLockingAsync(int serverId, int topN = 200)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = $@"
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    index_type_desc,
    reserved_mb,
    total_rows,
    COALESCE(row_lock_count, 0) AS row_lock_count,
    COALESCE(row_lock_wait_count, 0) AS row_lock_wait_count,
    COALESCE(row_lock_wait_in_ms, 0) AS row_lock_wait_in_ms,
    COALESCE(page_lock_count, 0) AS page_lock_count,
    COALESCE(page_lock_wait_count, 0) AS page_lock_wait_count,
    COALESCE(page_lock_wait_in_ms, 0) AS page_lock_wait_in_ms,
    COALESCE(index_lock_promotion_count, 0) AS index_lock_promotion_count,
    COALESCE(page_latch_wait_in_ms, 0) AS page_latch_wait_in_ms,
    COALESCE(page_io_latch_wait_in_ms, 0) AS page_io_latch_wait_in_ms
FROM v_index_object_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
AND (
    COALESCE(row_lock_wait_in_ms, 0) > 0
    OR COALESCE(page_lock_wait_in_ms, 0) > 0
    OR COALESCE(page_latch_wait_in_ms, 0) > 0
    OR COALESCE(page_io_latch_wait_in_ms, 0) > 0
    OR COALESCE(index_lock_promotion_count, 0) > 0
)
ORDER BY
    COALESCE(row_lock_wait_in_ms, 0) + COALESCE(page_lock_wait_in_ms, 0)
    + COALESCE(page_latch_wait_in_ms, 0) + COALESCE(page_io_latch_wait_in_ms, 0) DESC
LIMIT {topN}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<IndexLockingRow>();
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
        return items;
    }
}

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
