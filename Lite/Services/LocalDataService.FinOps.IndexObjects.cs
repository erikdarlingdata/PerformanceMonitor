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
    // ============================================
    // FinOps — Object/Index stats (sizes+growth, usage, locking).
    // Reads v_index_object_stats (hot DuckDB UNION archived parquet).
    // ============================================

    /// <summary>
    /// Per-table size and growth (indexes rolled up per table) for a server: the latest snapshot's sizes
    /// beside the same table's reserved size at the newest snapshot at/older than 7 and 30 days and at the
    /// store's earliest snapshot, projected RAW (#3541 A12). The growth figures are derived on
    /// <see cref="ObjectSizeGrowthBaselineRow"/>, where each can refuse when its baseline does not exist.
    /// <para>The SQL this replaced derived them here through <c>COALESCE(p30, p7, oldest, current)</c>, which
    /// is three lies in one expression: with ten days of history "30-day growth" was growth since the
    /// SEVEN-day snapshot; with two days it was growth since the oldest snapshot, still labelled 30d; and a
    /// table absent from every baseline (created this week) fell through to <c>current - current = 0</c>,
    /// "not growing", for the one table that is nothing BUT growth. The two cutoff snapshots are resolved
    /// once in <c>boundaries</c> with <c>FILTER</c> so the baseline CTEs and the projected snapshot times
    /// cannot disagree about which capture was used. Darling's <c>DarlingObjectStatsReader.ObjectSizeGrowthSql</c>
    /// is the twin.</para>
    /// </summary>
    public async Task<List<ObjectSizeGrowthBaselineRow>> GetObjectSizeGrowthAsync(int serverId, int topN = 100)
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
        CAST(MAX(collection_time) AS DATE) - CAST(MIN(collection_time) AS DATE) AS days_of_data,
        MAX(collection_time) FILTER (WHERE collection_time <= $2) AS snapshot_7d_time,
        MAX(collection_time) FILTER (WHERE collection_time <= $3) AS snapshot_30d_time
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
    WHERE server_id = $1 AND collection_time = (SELECT snapshot_7d_time FROM boundaries)
    GROUP BY database_name, schema_name, table_name
),
past_30d AS (
    SELECT database_name, schema_name, table_name, SUM(reserved_mb) AS reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND collection_time = (SELECT snapshot_30d_time FROM boundaries)
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
    p7.reserved_mb AS reserved_mb_7d_ago,
    p30.reserved_mb AS reserved_mb_30d_ago,
    o.reserved_mb AS reserved_mb_oldest,
    b.snapshot_7d_time,
    b.snapshot_30d_time,
    b.earliest_time,
    b.latest_time,
    b.days_of_data
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

        var items = new List<ObjectSizeGrowthBaselineRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ObjectSizeGrowthBaselineRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TableName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                CurrentReservedMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                CurrentUsedMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalRows = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                IndexCount = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                /* The baselines stay NULL when the store has none — a missing baseline is not a 0 baseline. */
                ReservedMb7dAgo = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                ReservedMb30dAgo = reader.IsDBNull(8) ? null : Convert.ToDecimal(reader.GetValue(8)),
                ReservedMbOldest = reader.IsDBNull(9) ? null : Convert.ToDecimal(reader.GetValue(9)),
                Snapshot7dTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                Snapshot30dTime = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                EarliestSnapshotTime = reader.GetDateTime(12),
                LatestSnapshotTime = reader.GetDateTime(13),
                DaysOfData = Convert.ToInt32(reader.GetValue(14)),
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
    /// Per-index locking/latch contention at the server's latest snapshot, top objects by total lock+latch
    /// wait. Cumulative totals, no delta (#1138 §3B). Optionally scoped to one database (the Locking grid's
    /// DB selector).
    /// <para>#3876: this read used to resolve "latest" PER <c>database_name</c> — <c>MAX(collection_time)</c>
    /// GROUPed BY the name string — which made every name the store has ever seen its own immortal group.
    /// Rename a database and its old name keeps a group whose newest row is the last capture before the
    /// rename; that group is still "the latest for that name" forever, so the grid and its DB selector showed
    /// databases that had not existed for a month (the reporter's four-tab split: Database sizes, Database
    /// Resources and Storage growth all anchor on the server's latest capture and had already dropped the old
    /// names). The per-name grouping was meant to keep a database visible when it missed the newest pass, but
    /// it cannot buy that: one collector run stamps every database it collects with a single
    /// <c>collection_time</c> (<c>RemoteCollectorService.DefinitionRunner.cs</c> takes one
    /// <c>DateTime.UtcNow</c> per run and hands it to every batch), so for a database present in the newest
    /// pass the per-name MAX IS the server-wide MAX — identical rows — and for a database absent from it the
    /// only thing the grouping adds is a row for a name that is gone. So the anchor is now the server's
    /// latest capture, which is how <see cref="GetIndexUsageAsync"/> one screen up, Database sizes
    /// (<c>LocalDataService.FinOps.Inventory.cs</c>) and Storage growth
    /// (<c>LocalDataService.FinOps.StorageGrowth.cs</c>) have always resolved it: ONE resolution path for the
    /// current set of databases, not two truths about which databases exist. Capture-time names stay in the
    /// store untouched — history is not rewritten, it is simply no longer read as the present.</para>
    /// <para>#3880: <c>ios.collection_time</c> is projected on the ROW statement so <c>get_object_locking</c>
    /// can publish the snapshot's <c>captured_at</c> — Erik's ruling on the judgment call Darling's twin
    /// recorded in #3878/#3879, taken identically on both SKUs because Lite carries the same latest-read
    /// stamp convention (its own <c>McpLatestSnapshotStampTests</c> roster). Never a second
    /// <c>MAX(collection_time)</c> read for the stamp: that one can resolve to the NEXT capture. The grid
    /// ignores the column; it costs the snapshot's own anchor value per row and buys the MCP surface a
    /// truthful age on a DAILY-collected read.</para>
    /// </summary>
    public async Task<List<IndexLockingRow>> GetIndexLockingAsync(int serverId, int topN = 200, string? databaseName = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        // Build the optional DB filter as literal SQL so a NULL parameter never has to be typed by DuckDB.
        var dbFilter = databaseName == null ? "" : " AND ios.database_name = $2";

        command.CommandText = $@"
SELECT
    ios.collection_time,
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
AND   ios.collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1){dbFilter}
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
LIMIT {topN}";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        if (databaseName != null)
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });

        var items = new List<IndexLockingRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new IndexLockingRow
            {
                /* #3880: ordinal 0 is the read's own anchor column, the snapshot's stamp. */
                CollectionTime = reader.GetDateTime(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                SchemaName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                TableName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                IndexName = reader.IsDBNull(4) ? "(heap)" : reader.GetString(4),
                IndexTypeDesc = reader.IsDBNull(5) ? "" : reader.GetString(5),
                ReservedMb = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                TotalRows = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                RowLockCount = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                RowLockWaitCount = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                RowLockWaitInMs = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                PageLockCount = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                PageLockWaitCount = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
                PageLockWaitInMs = reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13)),
                IndexLockPromotionCount = reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14)),
                PageLatchWaitInMs = reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15)),
                PageIoLatchWaitInMs = reader.IsDBNull(16) ? 0L : Convert.ToInt64(reader.GetValue(16)),
                PageLatchWaitCount = reader.IsDBNull(17) ? 0L : Convert.ToInt64(reader.GetValue(17)),
                PageIoLatchWaitCount = reader.IsDBNull(18) ? 0L : Convert.ToInt64(reader.GetValue(18))
            });
        }
        return items;
    }

    /// <summary>
    /// Distinct databases that have any lock/latch contention at the server's latest snapshot — the source
    /// for the Locking grid's database selector (#1138 §3B). Anchored on the same server-latest capture as
    /// <see cref="GetIndexLockingAsync"/>: the selector and the grid must agree on which databases exist, and
    /// under #3876's per-name grouping both offered month-dead names (the selector is how the reporter's old
    /// names could still be PICKED, not merely displayed).
    /// </summary>
    public async Task<List<string>> GetIndexLockingDatabasesAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
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

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0)) items.Add(reader.GetString(0));
        }
        return items;
    }

    // ============================================
    // #1138 — Object-growth heatmap drill (per-DB)
    // ============================================

    /// <summary>
    /// Data for the per-database object-growth heatmap drill (#1138 §3A): the top-N objects in a single
    /// database ranked by reserved-MB growth over the window, plus their daily reserved-MB series for the
    /// heatmap. Two-step + DB-scoped for perf — the series only touches the ranked top-N. Returns the ranked
    /// summary rows (companion grid) and the long-form samples (pivoted to a matrix by FinOpsHeatmapBuilder).
    /// </summary>
    public async Task<(List<ObjectSizeGrowthRow> Objects, List<FinOpsObjectDaySample> Samples)> GetObjectGrowthHeatmapDataAsync(
        int serverId, string databaseName, int daysBack = 30, int topN = 20)
    {
        using var connection = await OpenConnectionAsync();

        var windowStart = DateTime.UtcNow.AddDays(-daysBack);

        var objects = new List<ObjectSizeGrowthRow>();
        var samples = new List<FinOpsObjectDaySample>();

        // Query 1 — ranked top-N summary (drives the companion grid).
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
WITH bounds AS (
    SELECT MAX(collection_time) AS latest_time, MIN(collection_time) AS earliest_time
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time >= $3
),
latest AS (
    SELECT schema_name, table_name,
        SUM(reserved_mb) AS cur_reserved_mb,
        SUM(used_mb) AS cur_used_mb,
        MAX(total_rows) AS cur_rows,
        COUNT(*) AS index_count
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT latest_time FROM bounds)
    GROUP BY schema_name, table_name
),
earliest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS e_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT earliest_time FROM bounds)
    GROUP BY schema_name, table_name
)
SELECT
    l.schema_name,
    l.table_name,
    l.cur_reserved_mb,
    l.cur_used_mb,
    l.cur_rows,
    l.index_count,
    l.cur_reserved_mb - COALESCE(e.e_reserved_mb, l.cur_reserved_mb) AS growth_mb
FROM latest l
LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
ORDER BY growth_mb DESC, l.schema_name, l.table_name
LIMIT {topN}";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = windowStart });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var current = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2));
                var growth = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6));
                var earlier = current - growth;
                objects.Add(new ObjectSizeGrowthRow
                {
                    DatabaseName = databaseName,
                    SchemaName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    TableName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    CurrentReservedMb = current,
                    CurrentUsedMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                    TotalRows = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                    IndexCount = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                    Growth30dMb = growth,
                    DailyGrowthRateMb = daysBack > 0 ? growth / daysBack : 0m,
                    GrowthPct30d = earlier > 0 ? growth * 100m / earlier : 0m
                });
            }
        }

        // Query 2 — daily reserved-MB series for the same ranked top-N (ranking recomputed as a CTE; DuckDB
        // .NET returns one result set per command, so this is a second command rather than NextResult()).
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
WITH bounds AS (
    SELECT MAX(collection_time) AS latest_time, MIN(collection_time) AS earliest_time
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time >= $3
),
latest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS cur_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT latest_time FROM bounds)
    GROUP BY schema_name, table_name
),
earliest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS e_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT earliest_time FROM bounds)
    GROUP BY schema_name, table_name
),
ranked AS (
    SELECT l.schema_name, l.table_name,
        l.cur_reserved_mb - COALESCE(e.e_reserved_mb, l.cur_reserved_mb) AS growth_mb
    FROM latest l
    LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
    ORDER BY growth_mb DESC, l.schema_name, l.table_name
    LIMIT {topN}
)
SELECT
    ios.schema_name,
    ios.table_name,
    date_trunc('day', ios.collection_time) AS the_day,
    SUM(ios.reserved_mb) AS reserved_mb
FROM v_index_object_stats ios
JOIN ranked r ON r.schema_name = ios.schema_name AND r.table_name = ios.table_name
WHERE ios.server_id = $1 AND ios.database_name = $2 AND ios.collection_time >= $3
GROUP BY ios.schema_name, ios.table_name, date_trunc('day', ios.collection_time)
ORDER BY ios.schema_name, ios.table_name, the_day";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = windowStart });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var schema = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var table = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var day = reader.IsDBNull(2) ? DateTime.MinValue : reader.GetDateTime(2);
                var reservedMb = reader.IsDBNull(3) ? 0d : Convert.ToDouble(reader.GetValue(3));
                samples.Add(new FinOpsObjectDaySample($"{schema}.{table}", day, reservedMb));
            }
        }

        return (objects, samples);
    }

    /// <summary>
    /// Per-index detail for a single object at its database's latest snapshot (#1138 §3C): the leaf of the
    /// Storage Growth → object → index drill, folding the old Index Usage tab's per-index usage alongside
    /// size. Reuses <see cref="IndexUsageRow"/>.
    /// </summary>
    public async Task<List<IndexUsageRow>> GetObjectIndexDetailAsync(int serverId, string databaseName, string schemaName, string tableName)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
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
AND   database_name = $2
AND   schema_name = $3
AND   table_name = $4
AND   collection_time = (
    SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1 AND database_name = $2)
ORDER BY index_id";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = schemaName });
        command.Parameters.Add(new DuckDBParameter { Value = tableName });

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

/// <summary>
/// One per-table size + growth row for the MCP read, carrying the raw baselines the growth figures derive
/// from (#3541 A12, contract rule 5): a nominal window the store cannot reach is not a smaller window, it is
/// no measurement, and each derived property below refuses (null) rather than substituting a nearer baseline
/// or a 0. A separate class from <see cref="ObjectSizeGrowthRow"/> because that one is the FinOps heatmap
/// drill's grid row (#1138), whose growth is a single window it sets directly. Darling's
/// <c>DarlingObjectStatsReader.ObjectSizeGrowthRow</c> derives the same figures by the same rules.
/// </summary>
public class ObjectSizeGrowthBaselineRow
{
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public decimal CurrentReservedMb { get; set; }
    public decimal CurrentUsedMb { get; set; }
    public long TotalRows { get; set; }
    public int IndexCount { get; set; }

    /// <summary>Reserved MB at the newest snapshot at/before the 7-day cutoff; null when no such snapshot exists or the table was not in it.</summary>
    public decimal? ReservedMb7dAgo { get; set; }
    /// <summary>Same for the 30-day cutoff.</summary>
    public decimal? ReservedMb30dAgo { get; set; }
    /// <summary>Reserved MB at the store's EARLIEST snapshot; null when the table was not in it (created since).</summary>
    public decimal? ReservedMbOldest { get; set; }
    /// <summary>The snapshot the 7-day baseline was read from; null when the store holds nothing that old.</summary>
    public DateTime? Snapshot7dTime { get; set; }
    /// <summary>Same for 30 days.</summary>
    public DateTime? Snapshot30dTime { get; set; }
    public DateTime EarliestSnapshotTime { get; set; }
    public DateTime LatestSnapshotTime { get; set; }
    /// <summary>Whole calendar days between the earliest and latest snapshots. 0 means one day of snapshots: no growth is knowable.</summary>
    public int DaysOfData { get; set; }

    /// <summary>Growth since the 7-day baseline; null when there is no such baseline for this table.</summary>
    public decimal? Growth7dMb => ReservedMb7dAgo is { } b ? CurrentReservedMb - b : null;

    /// <summary>Growth since the 30-day baseline; null when there is no such baseline for this table.</summary>
    public decimal? Growth30dMb => ReservedMb30dAgo is { } b ? CurrentReservedMb - b : null;

    /// <summary>Percent growth over the 30-day baseline; null without a baseline, and null on a 0 baseline
    /// (no denominator — a table that was empty 30 days ago has no ratio, not an infinite one).</summary>
    public decimal? GrowthPct30d => ReservedMb30dAgo is > 0 ? (CurrentReservedMb - ReservedMb30dAgo.Value) * 100m / ReservedMb30dAgo.Value : null;

    /// <summary>Growth since the store's earliest snapshot — the honest figure when the nominal windows are out
    /// of reach. Null when the store holds a single day (no span) or the table was not in the earliest snapshot.</summary>
    public decimal? GrowthOverAvailableHistoryMb => DaysOfData >= 1 && ReservedMbOldest is { } o ? CurrentReservedMb - o : null;

    /// <summary>Percent form of <see cref="GrowthOverAvailableHistoryMb"/>; null on a 0 baseline.</summary>
    public decimal? GrowthOverAvailableHistoryPct =>
        DaysOfData >= 1 && ReservedMbOldest is > 0 ? (CurrentReservedMb - ReservedMbOldest.Value) * 100m / ReservedMbOldest.Value : null;

    /// <summary>MB per day over the available span; null when there is no span to divide by.</summary>
    public decimal? DailyGrowthRateMb => GrowthOverAvailableHistoryMb is { } g ? g / DaysOfData : null;
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
    /// <summary>The capture this row came from — the anchor <see cref="LocalDataService.GetIndexLockingAsync"/>
    /// resolves the read on, projected since #3880 so <c>get_object_locking</c> can publish the snapshot's
    /// <c>captured_at</c>. The grid does not bind it.</summary>
    public DateTime CollectionTime { get; set; }

    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public string IndexTypeDesc { get; set; } = "";

    /// <summary>
    /// <c>schema.table</c> for the Locking &amp; Contention grid's Table column (#3576). The grid used to bind
    /// the bare <see cref="TableName"/>, which is ambiguous the moment two schemas hold a table of the same
    /// name — routine on Azure SQL DB, where per-tenant or per-environment schemas are the usual pattern — so
    /// two different <c>Orders</c> tables read as one. One computed string (rather than a converter or a second
    /// column) so the column sorts, filters, and CSV-exports on the qualified name as a unit. Falls back to the
    /// bare name when the schema is empty, the same shape as <c>ProcedureStatsRow.FullName</c>.
    /// </summary>
    public string FullName => string.IsNullOrEmpty(SchemaName) ? TableName : $"{SchemaName}.{TableName}";

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
    public long PageLatchWaitCount { get; set; }
    public long PageIoLatchWaitCount { get; set; }

    /// <summary>
    /// Per-column 0..1 log color-scale intensities for the four *_wait_in_ms cells (#1138 §3B). Set by the
    /// loader after fetch (each column normalized over the visible rows via
    /// <see cref="PerformanceMonitor.Common.FinOpsHeatmapBuilder.ColumnLogIntensities"/>); bound to the cell
    /// background through HeatIntensityToBrushConverter. Not from the database.
    /// </summary>
    public double RowLockHeat { get; set; }
    public double PageLockHeat { get; set; }
    public double PageLatchHeat { get; set; }
    public double PageIoLatchHeat { get; set; }
}
