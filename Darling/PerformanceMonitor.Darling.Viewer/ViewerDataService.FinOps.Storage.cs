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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Database Sizes / Storage Growth (parent + object-growth heatmap drill + index detail) /
/// Optimization (idle databases + tempdb) reads — Lite's <c>LocalDataService.FinOps.StorageGrowth.cs</c>,
/// <c>.Inventory.cs</c>, and <c>.IndexObjects.cs</c> storage halves ported to Postgres. The DuckDB SQL
/// ports near-verbatim: the correlated <c>collection_time = (SELECT MAX(...))</c> latest-snapshot pattern,
/// <c>EXCEPT</c>, <c>GREATEST</c>, <c>date_trunc('day', ...)</c>, and date subtraction all run identically
/// on PG. The object-growth heatmap's two DuckDB commands (DuckDB returns one result set per command)
/// become two pooled Npgsql commands. SQL kept in <c>public const</c> so tests pin it.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Latest database size snapshot per file for one server, biggest files first (a capacity view —
    /// you hunt the largest files, not browse alphabetically; files can interleave across databases by design).
    /// The <c>total_size_mb DESC</c> lead is display-only and safe to change: the grid is the sole order-sensitive
    /// consumer — the other two callers (<c>GetUtilizationEfficiencyAsync</c>'s free-space math and the dormant-DB
    /// recommendation's cost share) only <c>Sum</c> the rows, and the "Allocated vs Used" chart is a separate read
    /// (<c>GetDatabaseSizeSummaryAsync</c>). $1 server_id.</summary>
    public const string DatabaseSizeLatestSql = @"
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
ORDER BY total_size_mb DESC, database_name, file_type_desc, file_name";

    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DatabaseSizeLatestSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        var items = new List<DatabaseSizeRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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
                IsPercentGrowth = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                GrowthPct = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)),
                VlfCount = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12))
            });
        }
        return items;
    }

    /// <summary>Per-database allocated + used space for the Utilization size chart. $1 server_id, $2 topN.</summary>
    public const string DatabaseSizeSummarySql = @"
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

    public async Task<List<DatabaseSizeSummaryRow>> GetDatabaseSizeSummaryAsync(int serverId, int topN = 10, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DatabaseSizeSummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

        var items = new List<DatabaseSizeSummaryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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

    /// <summary>Databases with zero query executions over the last N days. $1 server_id, $2 cutoff.</summary>
    public const string IdleDatabasesSql = @"
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

    public async Task<List<IdleDatabaseRow>> GetIdleDatabasesAsync(int serverId, int daysBack = 7, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        await using var command = _dataSource.CreateCommand(IdleDatabasesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<IdleDatabaseRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new IdleDatabaseRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                /* last_execution_time (sys.dm_exec_query_stats) is SERVER-LOCAL in the store, not naive
                   UTC, so it is read verbatim like Lite — NOT through ViewerTimeHelper.ForDisplay (which
                   assumes naive UTC and, in Local/Server mode, would shift it). Contrast the
                   collection_time-derived timestamps in this port, which ARE naive UTC and correctly use ForDisplay. */
                LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
            });
        }
        return items;
    }

    /// <summary>tempdb pressure summary: latest and 24h peak values. $1 server_id, $2 cutoff.</summary>
    public const string TempdbSummarySql = @"
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

    public async Task<List<TempdbSummaryRow>> GetTempdbSummaryAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        await using var command = _dataSource.CreateCommand(TempdbSummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var items = new List<TempdbSummaryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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

    /// <summary>Per-database storage growth vs 7d/30d ago (Storage Growth parent grid). $1 server_id, $2 cutoff7d, $3 cutoff30d.</summary>
    public const string StorageGrowthSql = @"
WITH latest AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS current_size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
    )
    GROUP BY database_name
),
past_7d AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $2
    )
    GROUP BY database_name
),
past_30d AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS size_mb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   collection_time <= $3
    )
    GROUP BY database_name
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

    public async Task<List<StorageGrowthRow>> GetStorageGrowthAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;
        var cutoff7d = now.AddDays(-7);
        var cutoff30d = now.AddDays(-30);

        await using var command = _dataSource.CreateCommand(StorageGrowthSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff7d, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff30d, DateTimeKind.Unspecified) });

        var items = new List<StorageGrowthRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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

    /// <summary>Ranked top-N object growth summary for the object drill (#1138 §3A). $1 server_id, $2 database, $3 window start, $4 topN.</summary>
    public const string ObjectGrowthSummarySql = @"
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
LIMIT $4";

    /// <summary>Daily reserved-MB series for the ranked top-N objects (heatmap). $1 server_id, $2 database, $3 window start, $4 topN.</summary>
    public const string ObjectGrowthSeriesSql = @"
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
    LIMIT $4
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

    /// <summary>
    /// Object-growth heatmap data for a single database (#1138 §3A): the ranked top-N summary rows and the
    /// long-form daily reserved-MB samples (pivoted to a matrix by FinOpsHeatmapBuilder). Two pooled
    /// commands (mirrors Lite's two-command split for DuckDB's one-result-set-per-command limit).
    /// </summary>
    public async Task<(List<ObjectSizeGrowthRow> Objects, List<FinOpsObjectDaySample> Samples)> GetObjectGrowthHeatmapDataAsync(
        int serverId, string databaseName, int daysBack = 30, int topN = 20, CancellationToken cancellationToken = default)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-daysBack), DateTimeKind.Unspecified);

        var objects = new List<ObjectSizeGrowthRow>();
        var samples = new List<FinOpsObjectDaySample>();

        await using (var command = _dataSource.CreateCommand(ObjectGrowthSummarySql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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

        await using (var command = _dataSource.CreateCommand(ObjectGrowthSeriesSql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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

    /// <summary>Per-index size + usage for one object at its database's latest snapshot (Storage Growth index drill).</summary>
    public const string ObjectIndexDetailSql = @"
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

    public async Task<List<IndexUsageRow>> GetObjectIndexDetailAsync(int serverId, string databaseName, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ObjectIndexDetailSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = tableName });

        var items = new List<IndexUsageRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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
                /* last_user_seek/scan/lookup/update (sys.dm_db_index_usage_stats) are SERVER-LOCAL in the
                   store, not naive UTC, so this GREATEST is read verbatim like Lite — NOT through
                   ViewerTimeHelper.ForDisplay (which assumes naive UTC and, in Local/Server mode, would shift it). */
                LastUserAccess = reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                Classification = reader.IsDBNull(14) ? "" : reader.GetString(14)
            });
        }
        return items;
    }
}
