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
    /// <summary>#7174: the Database Sizes rows for <paramref name="databaseNames"/> (null or empty = every database), each with its
    /// <see cref="DatabaseSizeRow.MonthlyCostShare"/> as its share of the SERVER's allocated size times the monthly cost. The grid is
    /// narrowed by the saved database filter but the budget is the whole server's, so the total comes from the unfiltered snapshot:
    /// dividing by the filtered total made one picked database cost 100% of the server. A file with no allocated size takes no share.
    /// No cost (0 or less) leaves every share 0. The snapshot is resolved ONCE and both reads use it, so a size collection that
    /// lands between the two reads cannot make a share come from two different snapshots. <paramref name="betweenReads"/> is a test
    /// seam that runs after the rows are read and before the server total is.</summary>
    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestWithCostShareAsync(
        int serverId, IReadOnlyList<string>? databaseNames, decimal monthlyCostUsd, Func<Task>? betweenReads = null)
    {
        var snapshot = await GetLatestDatabaseSizeSnapshotAsync(serverId);
        if (snapshot is null) return new List<DatabaseSizeRow>();

        var rows = await GetDatabaseSizeAtAsync(serverId, snapshot.Value, databaseNames);
        if (betweenReads is not null) await betweenReads();
        if (monthlyCostUsd <= 0m || rows.Count == 0) return rows;

        var serverRows = databaseNames is { Count: > 0 }
            ? await GetDatabaseSizeAtAsync(serverId, snapshot.Value, null)
            : rows;
        var totalMb = DatabaseSizeRow.AllocatedTotalMb(serverRows);
        if (totalMb > 0m)
        {
            foreach (var d in rows)
                d.MonthlyCostShare = ((d.TotalSizeMb ?? 0m) / totalMb) * monthlyCostUsd;
        }
        return rows;
    }

    /// <summary>#7216: a server's newest database size snapshot and the relation that answered for it. The one probe that picked
    /// the relation travels with the timestamp, so the reads at that snapshot cannot probe again and land on the other relation.</summary>
    internal readonly record struct DatabaseSizeSnapshot(DateTime CollectionTime, string Relation);

    internal async Task<DatabaseSizeSnapshot?> GetLatestDatabaseSizeSnapshotAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        /* #7216: the unbounded MAX through the v_ view read every archived row of database_size_stats. The hot table holds the newest
           snapshot of any server still being collected; the view is read only for a server with no hot row (collection stopped and
           archival moved every row out), which still shows the last values it had. This is the refresh's only probe: GetDatabaseSizeAtAsync
           reads the relation chosen here. */
        var sizeRelation = await HotTableHasServerAsync(connection, "database_size_stats", serverId) ? "database_size_stats" : "v_database_size_stats";
        command.CommandText = $"SELECT MAX(collection_time) FROM {sizeRelation} WHERE server_id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        var value = await command.ExecuteScalarAsync();
        return value is DateTime stamp ? new DatabaseSizeSnapshot(stamp, sizeRelation) : null;
    }

    /// <summary>
    /// Gets the latest database size snapshot per server per file (cross-server).
    /// <para>#5312: <paramref name="databaseNames"/> is the saved per-server database filter (null or empty = every database,
    /// the statement as it always read). It narrows the OUTER rows only: the newest snapshot is still the server's newest, so a
    /// filter never changes which snapshot is read.</para>
    /// </summary>
    public async Task<List<DatabaseSizeRow>> GetDatabaseSizeLatestAsync(int serverId, IReadOnlyList<string>? databaseNames = null)
    {
        var snapshot = await GetLatestDatabaseSizeSnapshotAsync(serverId);
        return snapshot is null
            ? new List<DatabaseSizeRow>()
            : await GetDatabaseSizeAtAsync(serverId, snapshot.Value, databaseNames);
    }

    internal async Task<List<DatabaseSizeRow>> GetDatabaseSizeAtAsync(int serverId, DatabaseSizeSnapshot snapshot, IReadOnlyList<string>? databaseNames)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        /* #7216: the relation the snapshot lookup chose, not a second probe: a hot row that lands between the two reads must not
           send this read to a table that does not hold the archived snapshot's timestamp. */
        var sizeRelation = snapshot.Relation;
        var dbClause = BuildDbInClause(databaseNames, "database_name", 3, out var dbValues);
        command.CommandText = $@"
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
FROM {sizeRelation}
WHERE server_id = $1
AND   collection_time = $2{dbClause}
ORDER BY database_name, file_type_desc, file_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = snapshot.CollectionTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<DatabaseSizeRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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
                IsPercentGrowth = reader.IsDBNull(10) ? null : (bool?)(Convert.ToInt32(reader.GetValue(10)) == 1),
                GrowthPct = reader.IsDBNull(11) ? null : Convert.ToInt32(reader.GetValue(11)),
                VlfCount = reader.IsDBNull(12) ? null : Convert.ToInt32(reader.GetValue(12)),
                /* NULL is the one row another database on an Azure SQL Database server gets: it has no file id. */
                FileId = reader.IsDBNull(13) ? null : Convert.ToInt32(reader.GetValue(13))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets per-database total allocated and used space for the utilization size chart.
    /// Aggregates across all files per database for the selected server.
    /// <para>#5312: <paramref name="databaseNames"/> is the saved database filter (null or empty = every database). The top
    /// <paramref name="topN"/> is taken among the chosen databases; the newest snapshot is still the server's newest.</para>
    /// </summary>
    public async Task<List<DatabaseSizeSummaryRow>> GetDatabaseSizeSummaryAsync(int serverId, int topN = 10, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* $1 server, $2 topN, so the optional database list starts at $3. */
        /* #7216: the newest-snapshot lookup has no time bound, so through the v_ view it read every archived row of database_size_stats.
           The hot table holds the newest snapshot of any server still being collected; the view is read only for a server with no
           hot row (collection stopped and archival moved every row out), which still shows the last values it had. */
        var sizeRelation = await HotTableHasServerAsync(connection, "database_size_stats", serverId) ? "database_size_stats" : "v_database_size_stats";
        var dbClause = BuildDbInClause(databaseNames, "database_name", 3, out var dbValues);
        command.CommandText = $@"
SELECT
    database_name,
    SUM(total_size_mb) AS total_mb,
    /* Used is summed only over the files whose size counts, so used and allocated stay on one footing: the
       Hyperscale log file (NULL size, the log service) is in neither. */
    SUM(CASE WHEN total_size_mb IS NOT NULL THEN used_size_mb END) AS used_mb
FROM {sizeRelation}
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time) FROM {sizeRelation} WHERE server_id = $1
){dbClause}
GROUP BY database_name
ORDER BY total_mb DESC
LIMIT $2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = topN });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<DatabaseSizeSummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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

    /// <summary>
    /// Detects databases with zero query executions over the last N days.
    /// </summary>
    public async Task<List<IdleDatabaseRow>> GetIdleDatabasesAsync(int serverId, int daysBack = 7)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddDays(-daysBack);

        /* #7216: the newest-snapshot lookup has no time bound, so through the v_ view it read every archived row of database_size_stats.
           The hot table holds the newest snapshot of any server still being collected; the view is read only for a server with no
           hot row (collection stopped and archival moved every row out), which still shows the last values it had. */
        var sizeRelation = await HotTableHasServerAsync(connection, "database_size_stats", serverId) ? "database_size_stats" : "v_database_size_stats";

        command.CommandText = $@"
WITH db_sizes AS (
    SELECT
        database_name,
        SUM(total_size_mb) AS total_size_mb,
        COUNT(*) AS file_count
    FROM {sizeRelation}
    WHERE server_id = $1
    AND   collection_time = (
        SELECT MAX(collection_time)
        FROM {sizeRelation}
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

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<IdleDatabaseRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new IdleDatabaseRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalSizeMb = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                FileCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                LastExecutionTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
            });
        }
        return items;
    }

    /// <summary>
    /// Gets tempdb pressure summary: latest and 24h peak values.
    /// </summary>
    public async Task<List<TempdbSummaryRow>> GetTempdbSummaryAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-24);

        /* #7216: the "latest" row has no time bound, so through the v_ view it ordered every archived row of tempdb_stats. The hot table
           holds the newest row of any server still being collected; the view is read only for a server with no hot row. The 24 hour
           "peak" below is windowed and stays on the view. */
        var latestRelation = await HotTableHasServerAsync(connection, "tempdb_stats", serverId) ? "tempdb_stats" : "v_tempdb_stats";

        command.CommandText = $@"
WITH latest AS (
    SELECT
        user_object_reserved_mb,
        internal_object_reserved_mb,
        version_store_reserved_mb,
        total_reserved_mb
    FROM {latestRelation}
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

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = cutoff });

        var items = new List<TempdbSummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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
}
