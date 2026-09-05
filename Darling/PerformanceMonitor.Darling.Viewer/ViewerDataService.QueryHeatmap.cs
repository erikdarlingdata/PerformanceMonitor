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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Which per-execution metric the Query Heatmap buckets rows by (Lite's <c>HeatmapMetric</c>).</summary>
public enum HeatmapMetric
{
    Duration,
    Cpu,
    LogicalReads,
    LogicalWrites,
    ExecutionCount,
}

/// <summary>One heatmap cell — the query count in a (5-minute bin × log-magnitude bucket) plus the
/// hottest query (most-executed) in that cell, for the hover tooltip. Viewer copy of Lite's
/// <c>HeatmapCell</c>.</summary>
public sealed class HeatmapCell
{
    public DateTime TimeBucket { get; set; }
    public int BucketIndex { get; set; }
    public long Count { get; set; }
    public string TopQueryHash { get; set; } = "";
    public string TopQueryText { get; set; } = "";
}

/// <summary>The assembled heatmap grid the chart renders — intensities[bucket, timeBin], the time
/// axis, the magnitude-bucket labels, and the per-cell detail for hover. Viewer copy of Lite's
/// <c>HeatmapResult</c>.</summary>
public sealed class HeatmapResult
{
    public double[,] Intensities { get; set; } = new double[0, 0];
    public DateTime[] TimeBuckets { get; set; } = Array.Empty<DateTime>();
    public string[] BucketLabels { get; set; } = Array.Empty<string>();
    public HeatmapCell[,] CellDetails { get; set; } = new HeatmapCell[0, 0];
}

public sealed partial class ViewerDataService
{
    private static readonly Dictionary<HeatmapMetric, string[]> HeatmapBucketLabels = new()
    {
        [HeatmapMetric.Duration] = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
        [HeatmapMetric.Cpu] = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
        [HeatmapMetric.LogicalReads] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
        [HeatmapMetric.LogicalWrites] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
        [HeatmapMetric.ExecutionCount] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
    };

    /// <summary>
    /// The per-execution metric expression Lite's <c>GetMetricColumn</c> uses — byte-identical, and all
    /// native Postgres (the <c>/ 1000.0</c> µs→ms scaling, <c>NULLIF(delta_execution_count, 0)</c>
    /// per-execution average, <c>CAST(… AS DOUBLE PRECISION)</c>). Internal constants only (no user
    /// input), so string-composed into the heatmap SQL is injection-safe.
    /// </summary>
    internal static string HeatmapMetricExpr(HeatmapMetric metric) => metric switch
    {
        HeatmapMetric.Duration => "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.Cpu => "(delta_worker_time / 1000.0) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.LogicalReads => "CAST(delta_logical_reads AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.LogicalWrites => "CAST(delta_logical_writes AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.ExecutionCount => "CAST(delta_execution_count AS DOUBLE PRECISION)",
        _ => "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)",
    };

    /// <summary>
    /// Builds the Query-Heatmap SQL for one metric — Lite's <c>GetQueryHeatmapAsync</c> query ported
    /// from DuckDB to Postgres. Two dialect rewrites the task called out (the rest is verbatim):
    ///   (1) DuckDB <c>time_bucket(INTERVAL '5 minutes', collection_time)</c> → Postgres
    ///       <c>date_bin(INTERVAL '5 minutes', collection_time, TIMESTAMP '1970-01-01 00:00:00')</c>.
    ///       date_bin needs an explicit origin; the Unix epoch aligns 5-minute bins to :00/:05/:10…,
    ///       matching time_bucket's default epoch alignment (both stride evenly into the hour).
    ///   (2) DuckDB <c>ARG_MAX(query_hash, delta_execution_count)</c> (the hottest query per cell) has
    ///       no Postgres equivalent, so it becomes a window: <c>ROW_NUMBER() OVER (PARTITION BY the
    ///       cell ORDER BY delta_execution_count DESC)</c> filtered to rn = 1, with the cell's
    ///       <c>COUNT(*) OVER</c> the same partition carried alongside so one pass yields both the
    ///       count and the top row. Ties resolve arbitrarily, exactly as ARG_MAX did.
    /// The magnitude CASE, the <c>delta_execution_count &gt; 0</c> / <c>metric IS NOT NULL</c> filters,
    /// the <c>LEFT(query_text, 120)</c> preview, and the $1/$2/$3 placeholders are Lite's verbatim.
    /// </summary>
    public static string BuildQueryHeatmapSql(HeatmapMetric metric)
    {
        var metricExpr = HeatmapMetricExpr(metric);
        return $"""
            WITH base AS (
                SELECT
                    date_bin(INTERVAL '5 minutes', collection_time, TIMESTAMP '1970-01-01 00:00:00') AS time_bin,
                    {metricExpr} AS metric_value,
                    query_hash,
                    LEFT(query_text, 120) AS query_preview,
                    delta_execution_count
                FROM v_query_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
                AND   delta_execution_count > 0
                AND   {metricExpr} IS NOT NULL
            ),
            binned AS (
                SELECT
                    time_bin,
                    CASE
                        WHEN metric_value < 1 THEN 0
                        WHEN metric_value < 10 THEN 1
                        WHEN metric_value < 100 THEN 2
                        WHEN metric_value < 1000 THEN 3
                        WHEN metric_value < 10000 THEN 4
                        WHEN metric_value < 100000 THEN 5
                        ELSE 6
                    END AS bucket_index,
                    query_hash,
                    query_preview,
                    delta_execution_count
                FROM base
            ),
            ranked AS (
                SELECT
                    time_bin,
                    bucket_index,
                    query_hash,
                    query_preview,
                    COUNT(*) OVER (PARTITION BY time_bin, bucket_index) AS query_count,
                    ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index ORDER BY delta_execution_count DESC) AS rn
                FROM binned
            )
            SELECT
                time_bin,
                bucket_index,
                query_count,
                query_hash AS top_query_hash,
                query_preview AS top_query_text
            FROM ranked
            WHERE rn = 1
            ORDER BY time_bin, bucket_index
            """;
    }

    private const int HeatmapBucketCount = 7;

    /// <summary>
    /// The Query Heatmap grid for one metric over [<paramref name="startUtc"/>, <paramref name="endUtc"/>]:
    /// query counts per (5-minute bin × log-magnitude bucket), with the hottest query per cell for hover.
    /// The row read is the PG rewrite (<see cref="BuildQueryHeatmapSql"/>); the grid assembly (distinct
    /// time axis, clamp into the 7 buckets, empty <see cref="HeatmapResult"/> on no rows) is Lite's
    /// post-processing verbatim.
    /// </summary>
    public async Task<HeatmapResult> GetQueryHeatmapAsync(
        int serverId, HeatmapMetric metric, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rawCells = new List<HeatmapCell>();

        await using var command = _dataSource.CreateCommand(BuildQueryHeatmapSql(metric));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rawCells.Add(new HeatmapCell
            {
                TimeBucket = reader.GetDateTime(0),
                BucketIndex = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                Count = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                TopQueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TopQueryText = reader.IsDBNull(4) ? "" : reader.GetString(4),
            });
        }

        if (rawCells.Count == 0)
        {
            return new HeatmapResult();
        }

        var times = rawCells.Select(c => c.TimeBucket).Distinct().OrderBy(t => t).ToArray();
        var timeIndex = new Dictionary<DateTime, int>();
        for (int i = 0; i < times.Length; i++)
        {
            timeIndex[times[i]] = i;
        }

        var intensities = new double[HeatmapBucketCount, times.Length];
        var cellDetails = new HeatmapCell[HeatmapBucketCount, times.Length];

        foreach (var cell in rawCells)
        {
            if (!timeIndex.TryGetValue(cell.TimeBucket, out int col))
            {
                continue;
            }

            int row = Math.Clamp(cell.BucketIndex, 0, HeatmapBucketCount - 1);
            intensities[row, col] = cell.Count;
            cellDetails[row, col] = cell;
        }

        return new HeatmapResult
        {
            Intensities = intensities,
            TimeBuckets = times,
            BucketLabels = HeatmapBucketLabels[metric],
            CellDetails = cellDetails,
        };
    }
}
