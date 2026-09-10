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

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgDrillDownCollector
{
    public const string FileLatencyBreakdownSql = @"
SELECT database_name, file_type,
       AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_ms,
       AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_ms,
       SUM(delta_reads)::BIGINT AS total_reads,
       SUM(delta_writes)::BIGINT AS total_writes
FROM v_file_io_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)
GROUP BY database_name, file_type
ORDER BY avg_read_ms DESC NULLS LAST
LIMIT 10";

    private async Task CollectFileLatencyBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(FileLatencyBreakdownSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                file_type = reader.IsDBNull(1) ? "" : reader.GetString(1),
                avg_read_latency_ms = reader.IsDBNull(2) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(2)), 2),
                avg_write_latency_ms = reader.IsDBNull(3) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(3)), 2),
                total_reads = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                total_writes = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["file_latency_breakdown"] = items;
    }

    public const string AutogrowthPercentFilesSql = @"
WITH latest AS (
    SELECT database_name, file_id, file_type_desc, file_name, total_size_mb, is_percent_growth, growth_pct,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_id ORDER BY collection_time DESC) AS rn
    FROM v_database_size_stats
    WHERE server_id = $1
)
SELECT database_name, file_type_desc, file_name, total_size_mb, growth_pct
FROM latest
WHERE rn = 1
AND   is_percent_growth = true
AND   total_size_mb >= 10240
AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')
ORDER BY total_size_mb DESC
LIMIT 50";

    /// <summary>
    /// Lists the large (>= 10 GB) data/log files on PERCENTAGE autogrowth (WS3), latest
    /// snapshot per file, excluding system databases — and attaches a copy-paste
    /// ALTER DATABASE ... MODIFY FILE fix per file (FILEGROWTH set to a size-tiered fixed MB).
    /// Same structured fields + SHARED renderer as the Lite/Dashboard collectors so the
    /// copy-paste is byte-identical across apps.
    /// </summary>
    private async Task CollectAutogrowthPercentFiles(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(AutogrowthPercentFilesSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var database = reader.IsDBNull(0) ? "" : reader.GetString(0);
            var fileType = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var logical = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var sizeMb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var growthPct = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4));
            if (string.IsNullOrEmpty(database) || string.IsNullOrEmpty(logical)) continue;

            var growthMb = FactRemediation.RecommendedGrowthMbFor(fileType);
            items.Add(new
            {
                database,
                logical_file_name = logical,
                file_type = fileType,
                total_size_mb = sizeMb,
                growth_pct = growthPct,
                issue = $"{growthPct}% autogrowth on {sizeMb / 1024.0:N1} GB {fileType} file",
                alter_statement = FactRemediation.BuildModifyFileStatement(database, logical, growthMb)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["autogrowth_percent_files"] = items;
    }

    public const string TempDbBreakdownSql = @"
SELECT collection_time, user_object_reserved_mb, internal_object_reserved_mb,
       version_store_reserved_mb, unallocated_mb
FROM v_tempdb_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
ORDER BY (user_object_reserved_mb + internal_object_reserved_mb + version_store_reserved_mb) DESC
LIMIT 5";

    private async Task CollectTempDbBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(TempDbBreakdownSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            items.Add(new
            {
                time = reader.GetDateTime(0).ToString("o"),
                user_objects_mb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                internal_objects_mb = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                version_store_mb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                unallocated_mb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["tempdb_breakdown"] = items;
    }
}
