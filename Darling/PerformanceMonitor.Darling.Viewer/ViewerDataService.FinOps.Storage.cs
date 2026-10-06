/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Database Sizes / Storage Growth (parent + object-growth heatmap drill + index detail) /
/// Optimization (idle databases + tempdb) reads — Lite's <c>LocalDataService.FinOps.StorageGrowth.cs</c>,
/// <c>.Inventory.cs</c>, and <c>.IndexObjects.cs</c> storage halves ported to Postgres. The DuckDB SQL
/// ports near-verbatim: the correlated <c>collection_time = (SELECT MAX(...))</c> latest-snapshot pattern,
/// <c>EXCEPT</c>, <c>GREATEST</c>, <c>date_trunc('day', ...)</c>, and date subtraction all run identically
/// on PG. The object-growth heatmap's two DuckDB commands (DuckDB returns one result set per command)
/// become two pooled Npgsql commands. SQL kept in <c>public const</c> so tests pin it.
///
/// <para><b>#4245: the one place the DuckDB port stops being verbatim.</b> DuckDB has no chunks, so a
/// correlated <c>collection_time = (SELECT MAX(collection_time) ...)</c> costs DuckDB nothing extra to plan.
/// On a <c>database_size_stats</c> hypertable it costs the PostgreSQL planner one subplan per retained
/// chunk, unconditionally, because nothing in the query lets it exclude any chunk before walking it — the
/// field measurement was 148 ms of planning against 2.6 ms of execution, over 71 chunks. The single-server
/// "latest snapshot" reads below (<see cref="GetDatabaseSizeLatestAsync"/>, <see cref="GetDatabaseSizeSummaryAsync"/>,
/// and <see cref="GetStorageGrowthAsync"/>'s current-size CTE) go through <see cref="DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync"/>
/// instead: a windowed probe the planner CAN bound, read as its own round trip so the snapshot's
/// <c>collection_time</c> comes back as a literal value the main read can bind an equality to, rather than a
/// subquery the planner must re-derive per chunk. <see cref="GetStorageGrowthAsync"/>'s 7-day-ago and
/// 30-day-ago comparison points go through <see cref="DarlingFinOpsStorageGrowthReader.GetDatabaseSizeSnapshotAtOrBeforeAsync"/> instead — same
/// two-step shape, but genuinely bounded above, since "at or before a past cutoff" is what they mean. A
/// *latest* read must not carry that upper bound: nothing should hide a snapshot merely because the collector
/// host's clock ran ahead of the reader's own <c>DateTime.UtcNow</c> (#4245 follow-up).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The windowed half of the at-or-before snapshot probe; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string DatabaseSizeSnapshotWindowedProbeSql = DarlingFinOpsStorageGrowthReader.DatabaseSizeSnapshotWindowedProbeSql;

    /// <summary>The unbounded fallback half of the at-or-before snapshot probe; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string DatabaseSizeSnapshotFallbackProbeSql = DarlingFinOpsStorageGrowthReader.DatabaseSizeSnapshotFallbackProbeSql;

    /// <summary>The windowed half of the latest snapshot probe; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string DatabaseSizeLatestSnapshotWindowedProbeSql = DarlingFinOpsStorageGrowthReader.DatabaseSizeLatestSnapshotWindowedProbeSql;

    /// <summary>The unbounded fallback half of the latest snapshot probe; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string DatabaseSizeLatestSnapshotFallbackProbeSql = DarlingFinOpsStorageGrowthReader.DatabaseSizeLatestSnapshotFallbackProbeSql;

    /// <summary>Latest database size snapshot per file for one server, biggest files first (a capacity view —
    /// you hunt the largest files, not browse alphabetically; files can interleave across databases by design).
    /// The <c>total_size_mb DESC</c> lead is display-only and safe to change: the grid is the sole order-sensitive
    /// consumer — the other two callers (<c>GetUtilizationEfficiencyAsync</c>'s free-space math and the dormant-DB
    /// recommendation's cost share) only <c>Sum</c> the rows, and the "Allocated vs Used" chart is a separate read
    /// (<c>GetDatabaseSizeSummaryAsync</c>). #5312: $3 is the saved database filter (a text[], NULL = every database). $1 server_id, $2 collection_time (resolved by
    /// <see cref="DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync"/> — see #4245 on the type doc).</summary>
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
    vlf_count,
    file_id
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = $2
AND   ($3::text[] IS NULL OR database_name = ANY($3))
ORDER BY total_size_mb DESC NULLS LAST, database_name, file_type_desc, file_name";

    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync(int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<DatabaseSizeRow>();
        var snapshotTime = await DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        if (snapshotTime is null)
        {
            return items;
        }

        await using var command = _dataSource.CreateCommand(DatabaseSizeLatestSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = snapshotTime.Value });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseSizeRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileTypeDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                /* NULL is the Hyperscale log file (the log service): it stays null, never 0, so the grid shows
                   n/a (log service) instead of a size and the allocated totals leave it out. */
                TotalSizeMb = reader.IsDBNull(3) ? null : Convert.ToDecimal(reader.GetValue(3)),
                UsedSizeMb = reader.IsDBNull(4) ? null : Convert.ToDecimal(reader.GetValue(4)),
                VolumeMountPoint = reader.IsDBNull(5) ? null : reader.GetString(5),
                VolumeTotalMb = reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6)),
                VolumeFreeMb = reader.IsDBNull(7) ? null : Convert.ToDecimal(reader.GetValue(7)),
                RecoveryModel = reader.IsDBNull(8) ? null : reader.GetString(8),
                AutoGrowthMb = reader.IsDBNull(9) ? null : Convert.ToDecimal(reader.GetValue(9)),
                IsPercentGrowth = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                GrowthPct = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)),
                VlfCount = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12)),
                /* NULL is the one row another database on an Azure SQL Database server gets: it has no file id. */
                FileId = reader.IsDBNull(13) ? null : Convert.ToInt32(reader.GetValue(13))
            });
        }
        return items;
    }

    /// <summary>Per-database allocated + used space for the Utilization size chart. $1 server_id,
    /// $2 collection_time (resolved by <see cref="DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync"/> — see #4245 on
    /// the type doc), $3 topN, $4 the saved database filter (#5312, a text[], NULL = every database).</summary>
    public const string DatabaseSizeSummarySql = @"
SELECT
    database_name,
    SUM(total_size_mb) AS total_mb,
    /* Used is summed only over the files whose size counts, so used and allocated stay on one footing: the
       Hyperscale log file (NULL size, the log service) is in neither. */
    SUM(CASE WHEN total_size_mb IS NOT NULL THEN used_size_mb END) AS used_mb
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = $2
AND   ($4::text[] IS NULL OR database_name = ANY($4))
GROUP BY database_name
ORDER BY total_mb DESC
LIMIT $3";

    public async Task<List<DatabaseSizeSummaryRow>> GetDatabaseSizeSummaryAsync(int serverId, int topN = 10, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<DatabaseSizeSummaryRow>();
        var snapshotTime = await DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        if (snapshotTime is null)
        {
            return items;
        }

        await using var command = _dataSource.CreateCommand(DatabaseSizeSummarySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = snapshotTime.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));

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

    /// <summary>The idle-database read's SQL; lives in <see cref="DarlingFinOpsOptimizationReader"/>.</summary>
    public const string IdleDatabasesSql = DarlingFinOpsOptimizationReader.IdleDatabasesSql;

    public async Task<List<IdleDatabaseRow>> GetIdleDatabasesAsync(int serverId, int daysBack = 7, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        var rows = await DarlingFinOpsOptimizationReader.GetIdleDatabasesAsync(
            _dataSource, serverId, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(IdleDatabaseRow.From).ToList();
    }

    /// <summary>The tempdb summary's SQL; lives in <see cref="DarlingFinOpsOptimizationReader"/>.</summary>
    public const string TempdbSummarySql = DarlingFinOpsOptimizationReader.TempdbSummarySql;

    public async Task<List<TempdbSummaryRow>> GetTempdbSummaryAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        var rows = await DarlingFinOpsOptimizationReader.GetTempdbSummaryAsync(
            _dataSource, serverId, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(TempdbSummaryRow.From).ToList();
    }

    /// <summary>The storage-growth read's SQL; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string StorageGrowthSql = DarlingFinOpsStorageGrowthReader.StorageGrowthSql;

    /// <summary>
    /// One row per database (the Storage Growth grid, which also lists the databases the table and index heatmap's picker offers).
    /// #5312: <paramref name="databaseNames"/> is the saved database filter; null or empty is every database. The rows are narrowed after the
    /// read, by the same exact-name rule as the SQL clause <c>database_name = ANY(filter)</c>, so a NULL database never matches a filter.
    /// </summary>
    public async Task<List<StorageGrowthRow>> GetStorageGrowthAsync(int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;

        var rows = await DarlingFinOpsStorageGrowthReader.GetStorageGrowthAsync(
            _dataSource, serverId, now, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        var mapped = rows.Select(StorageGrowthRow.From);
        if (databaseNames is { Count: > 0 })
        {
            var chosen = new HashSet<string>(databaseNames, StringComparer.Ordinal);
            mapped = mapped.Where(r => chosen.Contains(r.DatabaseName));
        }

        return mapped.ToList();
    }

    /// <summary>The object-growth bounds SQL; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string ObjectGrowthBoundsSql = DarlingFinOpsStorageGrowthReader.ObjectGrowthBoundsSql;

    /// <summary>The object-growth summary SQL; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string ObjectGrowthSummarySql = DarlingFinOpsStorageGrowthReader.ObjectGrowthSummarySql;

    /// <summary>The object-growth series SQL; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string ObjectGrowthSeriesSql = DarlingFinOpsStorageGrowthReader.ObjectGrowthSeriesSql;

    /// <summary>
    /// Object-growth heatmap data for a single database (#1138 §3A): the ranked top-N summary rows and the
    /// long-form daily reserved-MB samples (pivoted to a matrix by FinOpsHeatmapBuilder).
    /// </summary>
    public async Task<(List<ObjectSizeGrowthRow> Objects, List<FinOpsObjectDaySample> Samples)> GetObjectGrowthHeatmapDataAsync(
        int serverId, string databaseName, int daysBack = 30, int topN = 20, CancellationToken cancellationToken = default)
    {
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-daysBack), DateTimeKind.Unspecified);

        var (objects, samples) = await DarlingFinOpsStorageGrowthReader.GetObjectGrowthHeatmapDataAsync(
            _dataSource, serverId, databaseName, windowStart, daysBack, topN,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return (objects.Select(o => ObjectSizeGrowthRow.From(o, databaseName)).ToList(), samples);
    }

    /// <summary>The index-detail SQL; lives in <see cref="DarlingFinOpsStorageGrowthReader"/>.</summary>
    public const string ObjectIndexDetailSql = DarlingFinOpsStorageGrowthReader.ObjectIndexDetailSql;

    public async Task<List<IndexUsageRow>> GetObjectIndexDetailAsync(int serverId, string databaseName, string schemaName, string tableName, CancellationToken cancellationToken = default)
    {
        var rows = await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(
            _dataSource, serverId, databaseName, schemaName, tableName,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(IndexUsageRow.From).ToList();
    }
}
