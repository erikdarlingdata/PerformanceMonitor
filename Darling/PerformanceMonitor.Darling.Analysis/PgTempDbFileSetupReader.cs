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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// #8883: the two reads behind the TEMPDB_FILE_SETUP fact and its drill-down, so both list the same files: tempdb's files from the
/// newest <c>database_size_stats</c> collection in the lookback, and the newest visible online scheduler count. Lite's twin is
/// <c>TempDbFileSetupReader</c>, with the same statements. The caller holds <see cref="PgLatestValueBounds"/>.
/// </summary>
internal static class PgTempDbFileSetupReader
{
    /// <summary>$1 server_id, $2 the lookback start, $3 the window end.</summary>
    internal const string FilesSql = @"
WITH ranked AS (
    SELECT file_id, file_type_desc, file_name, total_size_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb,
           is_autogrow_all_files, collection_time,
           DENSE_RANK() OVER (ORDER BY collection_time DESC) AS newest
    FROM database_size_stats
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
    {PerformanceMonitor.Common.CpuSchedulerCollectionSql.PerCollection("cpu_scheduler_stats", "server_id = $1 AND collection_time >= $2 AND collection_time <= $3", newestOnly: true)}
)
SELECT scheduler_count, collection_time
FROM newest";

    private static DateTime AsNaive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    internal static async Task<List<TempDbSetupFile>> ReadFilesAsync(NpgsqlDataSource postgres, AnalysisContext context, int commandTimeoutSeconds)
    {
        await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.AnalysisFacts, context.CancellationToken);
        using var cmd = new NpgsqlCommand(FilesSql, connection) { CommandTimeout = commandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.LatestValueStartFor(DatabaseSizeStatsCollector.Instance.Name)));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var files = new List<TempDbSetupFile>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            files.Add(new TempDbSetupFile(
                FileType: reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName: reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalSizeMb: reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)),
                AutoGrowthMb: reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)),
                IsPercentGrowth: !reader.IsDBNull(4) && reader.GetBoolean(4),
                GrowthPct: reader.IsDBNull(5) ? null : Convert.ToDouble(reader.GetValue(5)),
                MaxSizeMb: reader.IsDBNull(6) ? null : Convert.ToDouble(reader.GetValue(6)),
                IsAutogrowAllFiles: reader.IsDBNull(7) ? null : reader.GetBoolean(7),
                CollectedAt: reader.GetDateTime(8)));
        }

        return files;
    }

    /// <summary>The newest visible online scheduler count and when it was collected, or null when none was collected in the lookback.</summary>
    internal static async Task<TempDbSchedulerCount?> ReadSchedulerCountAsync(NpgsqlDataSource postgres, AnalysisContext context, int commandTimeoutSeconds)
    {
        /* Its own collector's lookback, not the file read's: a slowed database size collector must not widen this read. */
        await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.AnalysisFacts, context.CancellationToken);
        using var cmd = new NpgsqlCommand(SchedulerCountSql, connection) { CommandTimeout = commandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.LatestValueStartFor(CpuSchedulerStatsCollector.Instance.Name)));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken) || reader.IsDBNull(0))
            return null;

        var count = Convert.ToInt32(reader.GetValue(0));
        return count > 0 ? new TempDbSchedulerCount(count, reader.GetDateTime(1)) : null;
    }
}
