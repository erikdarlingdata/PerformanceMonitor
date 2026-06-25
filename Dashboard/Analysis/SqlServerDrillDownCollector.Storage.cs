using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerDrillDownCollector
{
    private async Task CollectFileLatencyBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    database_name,
    file_type_desc AS file_type,
    AVG(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) AS avg_read_ms,
    AVG(io_stall_write_ms_delta * 1.0 / NULLIF(num_of_writes_delta, 0)) AS avg_write_ms,
    CAST(SUM(num_of_reads_delta) AS BIGINT) AS total_reads,
    CAST(SUM(num_of_writes_delta) AS BIGINT) AS total_writes
FROM collect.file_io_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   (num_of_reads_delta > 0 OR num_of_writes_delta > 0)
GROUP BY database_name, file_type_desc
ORDER BY AVG(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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

    /// <summary>
    /// Lists the large (&gt;= 10 GB) data/log files on PERCENTAGE autogrowth (WS3), latest
    /// snapshot per file, excluding system databases — and attaches a copy-paste
    /// <c>ALTER DATABASE ... MODIFY FILE</c> fix per file (FILEGROWTH set to a size-tiered
    /// fixed MB). The structured fields (<c>database</c>, <c>logical_file_name</c>,
    /// <c>total_size_mb</c>, <c>growth_pct</c>) are what the shared extractor
    /// (FactRemediation.ExtractFileGrowthTargets) reads; the rendered <c>alter_statement</c>
    /// uses the SHARED renderer so it is byte-identical to the reader's copy-paste rebuild.
    /// Advisory only — no Apply.
    /// </summary>
    private async Task CollectAutogrowthPercentFiles(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH latest AS (
    SELECT
        database_name,
        file_id,
        file_type_desc,
        file_name,
        total_size_mb,
        is_percent_growth,
        growth_pct,
        ROW_NUMBER() OVER (PARTITION BY database_name, file_id ORDER BY collection_time DESC) AS rn
    FROM collect.database_size_stats
    WHERE database_name NOT IN ('master', 'msdb', 'model', 'tempdb')
)
SELECT TOP (50)
    database_name,
    file_type_desc,
    file_name,
    total_size_mb,
    growth_pct
FROM latest
WHERE rn = 1
AND   is_percent_growth = 1
AND   total_size_mb >= @minSizeMb
ORDER BY total_size_mb DESC;";

        cmd.Parameters.Add(new SqlParameter("@minSizeMb", 10240.0)); /* 10 GB */

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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

    private async Task CollectTempDbBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    unallocated_mb
FROM collect.tempdb_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY (user_object_reserved_mb + internal_object_reserved_mb + version_store_reserved_mb) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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
