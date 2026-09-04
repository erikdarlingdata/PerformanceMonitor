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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Locking &amp; Contention reads — Lite's <c>GetIndexLockingAsync</c> /
/// <c>GetIndexLockingDatabasesAsync</c> (<c>LocalDataService.FinOps.IndexObjects.cs</c>) ported to
/// Postgres. Cumulative-snapshot top-N indexes by total lock+latch wait (#1138 §3B), per-database latest so
/// "all databases" shows every DB's newest row. Where Lite splices an optional DB filter into one SQL
/// string, the viewer uses two <c>const</c> strings (all-databases / single-database) so both stay
/// test-pinnable; the DuckDB SQL is otherwise byte-identical on PG.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Top-N indexes by lock+latch wait across ALL databases (per-database latest). $1 server_id, $2 topN.</summary>
    public const string IndexLockingAllSql = @"
WITH latest AS (
    SELECT database_name, MAX(collection_time) AS latest_time
    FROM v_index_object_stats
    WHERE server_id = $1
    GROUP BY database_name
)
SELECT
    ios.database_name,
    ios.schema_name,
    ios.table_name,
    ios.index_name,
    ios.index_type_desc,
    ios.reserved_mb,
    ios.total_rows,
    COALESCE(ios.row_lock_count, 0) AS row_lock_count,
    COALESCE(ios.row_lock_wait_count, 0) AS row_lock_wait_count,
    COALESCE(ios.row_lock_wait_in_ms, 0) AS row_lock_wait_in_ms,
    COALESCE(ios.page_lock_count, 0) AS page_lock_count,
    COALESCE(ios.page_lock_wait_count, 0) AS page_lock_wait_count,
    COALESCE(ios.page_lock_wait_in_ms, 0) AS page_lock_wait_in_ms,
    COALESCE(ios.index_lock_promotion_count, 0) AS index_lock_promotion_count,
    COALESCE(ios.page_latch_wait_in_ms, 0) AS page_latch_wait_in_ms,
    COALESCE(ios.page_io_latch_wait_in_ms, 0) AS page_io_latch_wait_in_ms,
    COALESCE(ios.page_latch_wait_count, 0) AS page_latch_wait_count,
    COALESCE(ios.page_io_latch_wait_count, 0) AS page_io_latch_wait_count
FROM v_index_object_stats ios
JOIN latest l ON l.database_name = ios.database_name AND l.latest_time = ios.collection_time
WHERE ios.server_id = $1
AND (
    COALESCE(ios.row_lock_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_lock_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_latch_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_io_latch_wait_in_ms, 0) > 0
    OR COALESCE(ios.index_lock_promotion_count, 0) > 0
)
ORDER BY
    COALESCE(ios.row_lock_wait_in_ms, 0) + COALESCE(ios.page_lock_wait_in_ms, 0)
    + COALESCE(ios.page_latch_wait_in_ms, 0) + COALESCE(ios.page_io_latch_wait_in_ms, 0) DESC
LIMIT $2";

    /// <summary>Top-N indexes by lock+latch wait for ONE database. $1 server_id, $2 database, $3 topN.</summary>
    public const string IndexLockingByDbSql = @"
WITH latest AS (
    SELECT database_name, MAX(collection_time) AS latest_time
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2
    GROUP BY database_name
)
SELECT
    ios.database_name,
    ios.schema_name,
    ios.table_name,
    ios.index_name,
    ios.index_type_desc,
    ios.reserved_mb,
    ios.total_rows,
    COALESCE(ios.row_lock_count, 0) AS row_lock_count,
    COALESCE(ios.row_lock_wait_count, 0) AS row_lock_wait_count,
    COALESCE(ios.row_lock_wait_in_ms, 0) AS row_lock_wait_in_ms,
    COALESCE(ios.page_lock_count, 0) AS page_lock_count,
    COALESCE(ios.page_lock_wait_count, 0) AS page_lock_wait_count,
    COALESCE(ios.page_lock_wait_in_ms, 0) AS page_lock_wait_in_ms,
    COALESCE(ios.index_lock_promotion_count, 0) AS index_lock_promotion_count,
    COALESCE(ios.page_latch_wait_in_ms, 0) AS page_latch_wait_in_ms,
    COALESCE(ios.page_io_latch_wait_in_ms, 0) AS page_io_latch_wait_in_ms,
    COALESCE(ios.page_latch_wait_count, 0) AS page_latch_wait_count,
    COALESCE(ios.page_io_latch_wait_count, 0) AS page_io_latch_wait_count
FROM v_index_object_stats ios
JOIN latest l ON l.database_name = ios.database_name AND l.latest_time = ios.collection_time
WHERE ios.server_id = $1
AND (
    COALESCE(ios.row_lock_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_lock_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_latch_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_io_latch_wait_in_ms, 0) > 0
    OR COALESCE(ios.index_lock_promotion_count, 0) > 0
)
ORDER BY
    COALESCE(ios.row_lock_wait_in_ms, 0) + COALESCE(ios.page_lock_wait_in_ms, 0)
    + COALESCE(ios.page_latch_wait_in_ms, 0) + COALESCE(ios.page_io_latch_wait_in_ms, 0) DESC
LIMIT $3";

    public async Task<List<IndexLockingRow>> GetIndexLockingAsync(int serverId, int topN = 200, string? databaseName = null, CancellationToken cancellationToken = default)
    {
        await using var command = databaseName == null
            ? _dataSource.CreateCommand(IndexLockingAllSql)
            : _dataSource.CreateCommand(IndexLockingByDbSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;

        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        if (databaseName != null)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        }
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        var items = new List<IndexLockingRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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
                PageIoLatchWaitInMs = reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15)),
                PageLatchWaitCount = reader.IsDBNull(16) ? 0L : Convert.ToInt64(reader.GetValue(16)),
                PageIoLatchWaitCount = reader.IsDBNull(17) ? 0L : Convert.ToInt64(reader.GetValue(17))
            });
        }
        return items;
    }

    /// <summary>Distinct databases with any lock/latch contention at their latest snapshot (the DB selector source). $1 server_id.</summary>
    public const string IndexLockingDatabasesSql = @"
WITH latest AS (
    SELECT database_name, MAX(collection_time) AS latest_time
    FROM v_index_object_stats
    WHERE server_id = $1
    GROUP BY database_name
)
SELECT DISTINCT ios.database_name
FROM v_index_object_stats ios
JOIN latest l ON l.database_name = ios.database_name AND l.latest_time = ios.collection_time
WHERE ios.server_id = $1
AND (
    COALESCE(ios.row_lock_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_lock_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_latch_wait_in_ms, 0) > 0
    OR COALESCE(ios.page_io_latch_wait_in_ms, 0) > 0
    OR COALESCE(ios.index_lock_promotion_count, 0) > 0
)
ORDER BY ios.database_name";

    public async Task<List<string>> GetIndexLockingDatabasesAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(IndexLockingDatabasesSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        var items = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(0)) items.Add(reader.GetString(0));
        }
        return items;
    }
}
