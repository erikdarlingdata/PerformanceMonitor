/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Ui;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        private static string GetHeatmapMetricExpr(Models.HeatmapMetric metric) => metric switch
        {
            Models.HeatmapMetric.Duration => "(qs.total_elapsed_time_delta / 1000.0) / NULLIF(qs.execution_count_delta, 0)",
            Models.HeatmapMetric.Cpu => "(qs.total_worker_time_delta / 1000.0) / NULLIF(qs.execution_count_delta, 0)",
            Models.HeatmapMetric.LogicalReads => "CAST(qs.total_logical_reads_delta AS float) / NULLIF(qs.execution_count_delta, 0)",
            Models.HeatmapMetric.LogicalWrites => "CAST(qs.total_logical_writes_delta AS float) / NULLIF(qs.execution_count_delta, 0)",
            Models.HeatmapMetric.ExecutionCount => "CAST(qs.execution_count_delta AS float)",
            _ => "(qs.total_elapsed_time_delta / 1000.0) / NULLIF(qs.execution_count_delta, 0)"
        };

        private static readonly string[] HeatmapDurationLabels = { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" };
        private static readonly string[] HeatmapCountLabels = { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" };

        public async Task<Models.HeatmapResult> GetQueryHeatmapAsync(Models.HeatmapMetric metric, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            var metricExpr = GetHeatmapMetricExpr(metric);

            string timeFilter;
            if (fromDate.HasValue && toDate.HasValue)
            {
                timeFilter = "AND qs.collection_time >= @from_date AND qs.collection_time <= @to_date";
            }
            else
            {
                timeFilter = $"AND qs.collection_time >= DATEADD(HOUR, -{hoursBack}, GETDATE())";
            }

            var query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH per_query AS
(
    SELECT
        time_bin = DATEADD(MINUTE, DATEDIFF(MINUTE, 0, qs.collection_time) / 5 * 5, 0),
        metric_value = {metricExpr},
        qs.query_hash,
        query_preview = LEFT(CAST(DECOMPRESS(qs.query_text) AS nvarchar(max)), 120),
        qs.execution_count_delta
    FROM collect.query_stats AS qs
    WHERE qs.execution_count_delta > 0
    AND   {metricExpr} IS NOT NULL
    {timeFilter}
)
SELECT
    pq.time_bin,
    bucket_index =
        CASE
            WHEN pq.metric_value < 1 THEN 0
            WHEN pq.metric_value < 10 THEN 1
            WHEN pq.metric_value < 100 THEN 2
            WHEN pq.metric_value < 1000 THEN 3
            WHEN pq.metric_value < 10000 THEN 4
            WHEN pq.metric_value < 100000 THEN 5
            ELSE 6
        END,
    query_count = COUNT(*),
    top_query_hash = CONVERT(varchar(20), MAX(CASE WHEN pq.execution_count_delta = m.max_exec THEN pq.query_hash END), 1),
    top_query_text = MAX(CASE WHEN pq.execution_count_delta = m.max_exec THEN pq.query_preview END)
FROM per_query AS pq
CROSS APPLY
(
    SELECT max_exec = MAX(pq2.execution_count_delta)
    FROM per_query AS pq2
    WHERE pq2.time_bin = pq.time_bin
    AND   CASE
            WHEN pq2.metric_value < 1 THEN 0
            WHEN pq2.metric_value < 10 THEN 1
            WHEN pq2.metric_value < 100 THEN 2
            WHEN pq2.metric_value < 1000 THEN 3
            WHEN pq2.metric_value < 10000 THEN 4
            WHEN pq2.metric_value < 100000 THEN 5
            ELSE 6
          END =
          CASE
            WHEN pq.metric_value < 1 THEN 0
            WHEN pq.metric_value < 10 THEN 1
            WHEN pq.metric_value < 100 THEN 2
            WHEN pq.metric_value < 1000 THEN 3
            WHEN pq.metric_value < 10000 THEN 4
            WHEN pq.metric_value < 100000 THEN 5
            ELSE 6
          END
) AS m
GROUP BY
    pq.time_bin,
    CASE
        WHEN pq.metric_value < 1 THEN 0
        WHEN pq.metric_value < 10 THEN 1
        WHEN pq.metric_value < 100 THEN 2
        WHEN pq.metric_value < 1000 THEN 3
        WHEN pq.metric_value < 10000 THEN 4
        WHEN pq.metric_value < 100000 THEN 5
        ELSE 6
    END
ORDER BY
    pq.time_bin,
    bucket_index;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            if (fromDate.HasValue && toDate.HasValue)
            {
                command.Parameters.AddWithValue("@from_date", fromDate.Value);
                command.Parameters.AddWithValue("@to_date", toDate.Value);
            }

            var rawCells = new System.Collections.Generic.List<Models.HeatmapCell>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rawCells.Add(new Models.HeatmapCell
                {
                    TimeBucket = reader.GetDateTime(0),
                    BucketIndex = reader.GetInt32(1),
                    Count = Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture),
                    TopQueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    TopQueryText = reader.IsDBNull(4) ? "" : reader.GetString(4)
                });
            }

            if (rawCells.Count == 0)
                return new Models.HeatmapResult();

            var times = new System.Collections.Generic.List<DateTime>();
            var timeIndex = new System.Collections.Generic.Dictionary<DateTime, int>();
            foreach (var cell in rawCells)
            {
                if (!timeIndex.ContainsKey(cell.TimeBucket))
                {
                    timeIndex[cell.TimeBucket] = times.Count;
                    times.Add(cell.TimeBucket);
                }
            }

            int numBuckets = 7;
            var intensities = new double[numBuckets, times.Count];
            var cellDetails = new Models.HeatmapCell[numBuckets, times.Count];

            foreach (var cell in rawCells)
            {
                if (!timeIndex.TryGetValue(cell.TimeBucket, out int col)) continue;
                int row = Math.Clamp(cell.BucketIndex, 0, numBuckets - 1);
                intensities[row, col] = cell.Count;
                cellDetails[row, col] = cell;
            }

            var labels = metric == Models.HeatmapMetric.LogicalReads || metric == Models.HeatmapMetric.LogicalWrites || metric == Models.HeatmapMetric.ExecutionCount
                ? HeatmapCountLabels
                : HeatmapDurationLabels;

            return new Models.HeatmapResult
            {
                Intensities = intensities,
                TimeBuckets = times.ToArray(),
                BucketLabels = labels,
                CellDetails = cellDetails
            };
        }
    }
}
