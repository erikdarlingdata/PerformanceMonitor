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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// #8883: the two reads behind the TEMPDB_FILE_SETUP fact and its drill-down, so both list the same files: tempdb's files from the
/// newest <c>database_size_stats</c> collection in the lookback, and the newest visible online scheduler count. Darling's twin is
/// <c>PgTempDbFileSetupReader</c>. The caller holds <see cref="LatestValueBounds"/> (the lookback start is stamped on the context).
/// </summary>
internal static class TempDbFileSetupReader
{
    /// <summary>$1 server_id, $2 the lookback start, $3 the window end. One reference to the view, so the hot-table swap is one replace.</summary>
    internal const string FilesSql = @"
WITH ranked AS (
    SELECT file_id, file_type_desc, file_name, total_size_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb,
           is_autogrow_all_files, collection_time,
           DENSE_RANK() OVER (ORDER BY collection_time DESC) AS newest
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   database_id = 2
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT file_type_desc, file_name, total_size_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb, is_autogrow_all_files, collection_time
FROM ranked
WHERE newest = 1
ORDER BY file_id";

    /// <summary>$1 server_id, $2 the lookback start, $3 the window end. The newest collection's scheduler count, its rows combined.</summary>
    internal static readonly string SchedulerCountSql = $@"
WITH newest AS
(
    {PerformanceMonitor.Common.CpuSchedulerCollectionSql.PerCollection("v_cpu_scheduler_stats", "server_id = $1 AND collection_time >= $2 AND collection_time <= $3", newestOnly: true)}
)
SELECT scheduler_count, collection_time
FROM newest";

    internal static async Task<List<TempDbSetupFile>> ReadFilesAsync(DuckDbInitializer duckDb, AnalysisContext context)
    {
        var start = context.LatestValueStartFor(DatabaseSizeStatsCollector.Instance.Name);
        var plan = await duckDb.PlanArchiveReadAsync("database_size_stats", "collection_time", start, context.CancellationToken);
        using var readLock = duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = plan.Apply(FilesSql);
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = start });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        var files = new List<TempDbSetupFile>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            files.Add(new TempDbSetupFile(
                FileType: reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName: reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalSizeMb: reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)),
                AutoGrowthMb: reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)),
                IsPercentGrowth: !reader.IsDBNull(4) && Convert.ToBoolean(reader.GetValue(4)),
                GrowthPct: reader.IsDBNull(5) ? null : Convert.ToDouble(reader.GetValue(5)),
                MaxSizeMb: reader.IsDBNull(6) ? null : Convert.ToDouble(reader.GetValue(6)),
                IsAutogrowAllFiles: reader.IsDBNull(7) ? null : Convert.ToBoolean(reader.GetValue(7)),
                CollectedAt: reader.GetDateTime(8)));
        }

        return files;
    }

    /// <summary>The newest visible online scheduler count and when it was collected, or null when none was collected in the lookback.</summary>
    internal static async Task<TempDbSchedulerCount?> ReadSchedulerCountAsync(DuckDbInitializer duckDb, AnalysisContext context)
    {
        /* Its own collector's lookback, not the file read's: a slowed database size collector must not widen this read. */
        var start = context.LatestValueStartFor(CpuSchedulerStatsCollector.Instance.Name);
        var plan = await duckDb.PlanArchiveReadAsync("cpu_scheduler_stats", "collection_time", start, context.CancellationToken);
        using var readLock = duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = plan.Apply(SchedulerCountSql);
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = start });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken) || reader.IsDBNull(0))
            return null;

        var count = Convert.ToInt32(reader.GetValue(0));
        return count > 0 ? new TempDbSchedulerCount(count, reader.GetDateTime(1)) : null;
    }
}
