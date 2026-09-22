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
/// Postgres. Cumulative-snapshot top-N indexes by total lock+latch wait (#1138 §3B), anchored on the
/// SERVER's latest capture so "all databases" shows the databases the newest pass actually collected. Where
/// Lite splices an optional DB filter into one SQL string, the viewer uses two <c>const</c> strings
/// (all-databases / single-database) so both stay test-pinnable; the DuckDB SQL is otherwise byte-identical
/// on PG.
///
/// <para><b>#3878 — the anchor these three reads used to take, and why it was wrong.</b> "Latest" was
/// resolved PER <c>database_name</c>: <c>MAX(collection_time)</c> GROUPed BY the name string, joined back to
/// the rows. That makes every name the store has ever seen its own permanent group, so a database renamed
/// away keeps a group whose newest row is the last capture before the rename — truthfully "the latest row
/// for that name", and therefore returned forever. #3876 is the field report: a month after a rename, the
/// Locking &amp; Contention grid still listed the old names beside the live ones while Database Sizes,
/// Database Resources and Storage Growth had all dropped them, because those three anchor on the server's
/// newest capture. The defect was ported into this file from Lite with the rest of the read (the header
/// above says as much), which is why both SKUs carried it; #3877 fixed Lite's half and this is Darling's.
///
/// <para>The per-name grouping was meant to keep a database visible when it missed the newest pass, and it
/// cannot buy that: one collector run stamps EVERY database it collects with a single <c>collection_time</c>
/// (one <c>DateTime.UtcNow</c> per run, handed to every write batch), so for any database present in the
/// newest pass the per-name MAX <b>is</b> the server-wide MAX — the very same rows — and for a database
/// absent from it the only thing the grouping adds is a row for a name that is gone. Pure downside. So all
/// three reads now anchor on <c>(SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)</c>,
/// the shape <c>DatabaseSizeLatestSql</c> and <c>StorageGrowthSql</c> have always used: ONE resolution path
/// for which databases exist, not two answers on one tab.</para>
///
/// <para>Grid and selector move together on purpose. <see cref="IndexLockingDatabasesSql"/> feeds the DB
/// dropdown, so under the old anchor a dead name could still be PICKED, not merely displayed — fixing only
/// the grid would have left the reporter able to select the old name and get a populated grid. The
/// single-database arm carries the same anchor for the same reason. Capture-time names stay in the store
/// untouched: they are honest history, and none of this rewrites them — they are simply no longer read as
/// the present.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Top-N indexes by lock+latch wait across ALL databases, at the SERVER's latest capture (#3878 —
    /// see the file header for why this is not per-database latest). $1 server_id, $2 topN.</summary>
    public const string IndexLockingAllSql = @"
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
WHERE ios.server_id = $1
AND   ios.collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
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

    /// <summary>Top-N indexes by lock+latch wait for ONE database, at the SERVER's latest capture — the same
    /// anchor as <see cref="IndexLockingAllSql"/>, so the filtered arm cannot resurrect a name the grid and the
    /// selector have dropped (#3878). $1 server_id, $2 database, $3 topN.</summary>
    public const string IndexLockingByDbSql = @"
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
WHERE ios.server_id = $1
AND   ios.collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
AND   ios.database_name = $2
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;

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

    /// <summary>Distinct databases with any lock/latch contention at the SERVER's latest capture — the DB
    /// selector's source, and the half of #3878 that let a renamed-away database still be PICKED rather than
    /// merely displayed. Same anchor as the grid it drives. $1 server_id.</summary>
    public const string IndexLockingDatabasesSql = @"
SELECT DISTINCT ios.database_name
FROM v_index_object_stats ios
WHERE ios.server_id = $1
AND   ios.collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
