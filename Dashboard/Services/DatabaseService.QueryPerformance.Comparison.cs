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
        /// <summary>
        /// Gets query stats comparison between a current time range and a baseline range.
        /// Uses delta columns for accurate period-level aggregation.
        /// </summary>
        public async Task<List<QueryStatsComparisonItem>> GetQueryStatsComparisonAsync(
            DateTime currentStart, DateTime currentEnd,
            DateTime baselineStart, DateTime baselineEnd)
        {
            var items = new List<QueryStatsComparisonItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH top_hashes AS (
    SELECT DISTINCT query_hash, database_name, object_name, schema_name, object_type
    FROM (
        SELECT TOP 100 query_hash, database_name, object_name, schema_name, object_type
        FROM collect.query_stats
        WHERE collection_time >= @currentStart AND collection_time <= @currentEnd
        AND   execution_count_delta > 0
        GROUP BY query_hash, database_name, object_name, schema_name, object_type
        ORDER BY SUM(execution_count_delta) DESC
        UNION
        SELECT TOP 100 query_hash, database_name, object_name, schema_name, object_type
        FROM collect.query_stats
        WHERE collection_time >= @baselineStart AND collection_time <= @baselineEnd
        AND   execution_count_delta > 0
        GROUP BY query_hash, database_name, object_name, schema_name, object_type
        ORDER BY SUM(execution_count_delta) DESC
    ) AS combined
),
current_period AS (
    SELECT th.database_name,
           CONVERT(nvarchar(20), th.query_hash, 1) AS query_hash,
           th.object_name, th.schema_name, th.object_type,
           SUM(qs.execution_count_delta) AS exec_count,
           SUM(qs.total_elapsed_time_delta) / NULLIF(SUM(qs.execution_count_delta), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.total_worker_time_delta) / NULLIF(SUM(qs.execution_count_delta), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.total_physical_reads_delta) / NULLIF(SUM(qs.execution_count_delta), 0) AS avg_reads,
           LEFT(CAST(DECOMPRESS(MAX(qs.query_text)) AS nvarchar(max)), 500) AS query_text
    FROM top_hashes th
    INNER JOIN collect.query_stats qs
      ON  qs.query_hash = th.query_hash
      AND qs.database_name = th.database_name
      AND ISNULL(qs.object_name, N'') = ISNULL(th.object_name, N'')
    WHERE qs.collection_time >= @currentStart AND qs.collection_time <= @currentEnd
    AND   qs.execution_count_delta > 0
    GROUP BY th.database_name, th.query_hash, th.object_name, th.schema_name, th.object_type
),
baseline_period AS (
    SELECT th.database_name,
           CONVERT(nvarchar(20), th.query_hash, 1) AS query_hash,
           th.object_name, th.schema_name, th.object_type,
           SUM(qs.execution_count_delta) AS exec_count,
           SUM(qs.total_elapsed_time_delta) / NULLIF(SUM(qs.execution_count_delta), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.total_worker_time_delta) / NULLIF(SUM(qs.execution_count_delta), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.total_physical_reads_delta) / NULLIF(SUM(qs.execution_count_delta), 0) AS avg_reads,
           LEFT(CAST(DECOMPRESS(MAX(qs.query_text)) AS nvarchar(max)), 500) AS query_text
    FROM top_hashes th
    INNER JOIN collect.query_stats qs
      ON  qs.query_hash = th.query_hash
      AND qs.database_name = th.database_name
      AND ISNULL(qs.object_name, N'') = ISNULL(th.object_name, N'')
    WHERE qs.collection_time >= @baselineStart AND qs.collection_time <= @baselineEnd
    AND   qs.execution_count_delta > 0
    GROUP BY th.database_name, th.query_hash, th.object_name, th.schema_name, th.object_type
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.query_hash, b.query_hash) AS query_hash,
       COALESCE(c.object_name, b.object_name) AS object_name,
       COALESCE(c.schema_name, b.schema_name) AS schema_name,
       COALESCE(c.object_type, b.object_type) AS object_type,
       COALESCE(c.query_text, b.query_text) AS query_text,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  ISNULL(c.database_name, N'') = ISNULL(b.database_name, N'')
  AND ISNULL(c.query_hash, N'') = ISNULL(b.query_hash, N'')
  AND ISNULL(c.object_name, N'') = ISNULL(b.object_name, N'')
  AND ISNULL(c.schema_name, N'') = ISNULL(b.schema_name, N'')
  AND ISNULL(c.object_type, N'') = ISNULL(b.object_type, N'');";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@currentStart", SqlDbType.DateTime2) { Value = currentStart });
            command.Parameters.Add(new SqlParameter("@currentEnd", SqlDbType.DateTime2) { Value = currentEnd });
            command.Parameters.Add(new SqlParameter("@baselineStart", SqlDbType.DateTime2) { Value = baselineStart });
            command.Parameters.Add(new SqlParameter("@baselineEnd", SqlDbType.DateTime2) { Value = baselineEnd });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new QueryStatsComparisonItem
                {
                    DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    ObjectName = reader.IsDBNull(2) ? null : reader.GetString(2),
                    SchemaName = reader.IsDBNull(3) ? null : reader.GetString(3),
                    ObjectType = reader.IsDBNull(4) ? null : reader.GetString(4),
                    QueryText = reader.IsDBNull(5) ? "" : reader.GetString(5),
                    ExecutionCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    AvgDurationMs = reader.IsDBNull(7) ? 0 : Convert.ToDouble(reader.GetValue(7), CultureInfo.InvariantCulture),
                    AvgCpuMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8), CultureInfo.InvariantCulture),
                    AvgReads = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture),
                    BaselineExecutionCount = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                    BaselineAvgDurationMs = reader.IsDBNull(11) ? 0 : Convert.ToDouble(reader.GetValue(11), CultureInfo.InvariantCulture),
                    BaselineAvgCpuMs = reader.IsDBNull(12) ? 0 : Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture),
                    BaselineAvgReads = reader.IsDBNull(13) ? 0 : Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture),
                });
            }

            return items;
        }

        /// <summary>
        /// Gets procedure stats comparison between a current time range and a baseline range.
        /// </summary>
        public async Task<List<ProcedureStatsComparisonItem>> GetProcedureStatsComparisonAsync(
            DateTime currentStart, DateTime currentEnd,
            DateTime baselineStart, DateTime baselineEnd)
        {
            var items = new List<ProcedureStatsComparisonItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH top_procs AS (
    SELECT DISTINCT database_name, schema_name, object_name
    FROM (
        SELECT TOP 100 database_name, schema_name, object_name
        FROM collect.procedure_stats
        WHERE collection_time >= @currentStart AND collection_time <= @currentEnd
        AND   execution_count_delta > 0
        GROUP BY database_name, schema_name, object_name
        ORDER BY SUM(execution_count_delta) DESC
        UNION
        SELECT TOP 100 database_name, schema_name, object_name
        FROM collect.procedure_stats
        WHERE collection_time >= @baselineStart AND collection_time <= @baselineEnd
        AND   execution_count_delta > 0
        GROUP BY database_name, schema_name, object_name
        ORDER BY SUM(execution_count_delta) DESC
    ) AS combined
),
current_period AS (
    SELECT tp.database_name, tp.schema_name, tp.object_name,
           SUM(ps.execution_count_delta) AS exec_count,
           SUM(ps.total_elapsed_time_delta) / NULLIF(SUM(ps.execution_count_delta), 0) / 1000.0 AS avg_duration_ms,
           SUM(ps.total_worker_time_delta) / NULLIF(SUM(ps.execution_count_delta), 0) / 1000.0 AS avg_cpu_ms,
           SUM(ps.total_physical_reads_delta) / NULLIF(SUM(ps.execution_count_delta), 0) AS avg_reads
    FROM top_procs tp
    INNER JOIN collect.procedure_stats ps
      ON  ps.database_name = tp.database_name
      AND ISNULL(ps.schema_name, N'') = ISNULL(tp.schema_name, N'')
      AND ps.object_name = tp.object_name
    WHERE ps.collection_time >= @currentStart AND ps.collection_time <= @currentEnd
    AND   ps.execution_count_delta > 0
    GROUP BY tp.database_name, tp.schema_name, tp.object_name
),
baseline_period AS (
    SELECT tp.database_name, tp.schema_name, tp.object_name,
           SUM(ps.execution_count_delta) AS exec_count,
           SUM(ps.total_elapsed_time_delta) / NULLIF(SUM(ps.execution_count_delta), 0) / 1000.0 AS avg_duration_ms,
           SUM(ps.total_worker_time_delta) / NULLIF(SUM(ps.execution_count_delta), 0) / 1000.0 AS avg_cpu_ms,
           SUM(ps.total_physical_reads_delta) / NULLIF(SUM(ps.execution_count_delta), 0) AS avg_reads
    FROM top_procs tp
    INNER JOIN collect.procedure_stats ps
      ON  ps.database_name = tp.database_name
      AND ISNULL(ps.schema_name, N'') = ISNULL(tp.schema_name, N'')
      AND ps.object_name = tp.object_name
    WHERE ps.collection_time >= @baselineStart AND ps.collection_time <= @baselineEnd
    AND   ps.execution_count_delta > 0
    GROUP BY tp.database_name, tp.schema_name, tp.object_name
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.schema_name, b.schema_name) AS schema_name,
       COALESCE(c.object_name, b.object_name) AS object_name,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  ISNULL(c.database_name, N'') = ISNULL(b.database_name, N'')
  AND ISNULL(c.schema_name, N'') = ISNULL(b.schema_name, N'')
  AND ISNULL(c.object_name, N'') = ISNULL(b.object_name, N'');";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@currentStart", SqlDbType.DateTime2) { Value = currentStart });
            command.Parameters.Add(new SqlParameter("@currentEnd", SqlDbType.DateTime2) { Value = currentEnd });
            command.Parameters.Add(new SqlParameter("@baselineStart", SqlDbType.DateTime2) { Value = baselineStart });
            command.Parameters.Add(new SqlParameter("@baselineEnd", SqlDbType.DateTime2) { Value = baselineEnd });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new ProcedureStatsComparisonItem
                {
                    DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    ObjectName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    ExecutionCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4), CultureInfo.InvariantCulture),
                    AvgCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5), CultureInfo.InvariantCulture),
                    AvgReads = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6), CultureInfo.InvariantCulture),
                    BaselineExecutionCount = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8), CultureInfo.InvariantCulture),
                    BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture),
                    BaselineAvgReads = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10), CultureInfo.InvariantCulture),
                });
            }

            return items;
        }

        /// <summary>
        /// Gets query store comparison between a current time range and a baseline range.
        /// Reuses QueryStatsComparisonItem model (same identity: database + query_hash).
        /// </summary>
        public async Task<List<QueryStatsComparisonItem>> GetQueryStoreComparisonAsync(
            DateTime currentStart, DateTime currentEnd,
            DateTime baselineStart, DateTime baselineEnd)
        {
            var items = new List<QueryStatsComparisonItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH top_hashes AS (
    SELECT DISTINCT database_name, query_hash
    FROM (
        SELECT TOP 100 database_name, query_hash
        FROM collect.query_store_stats
        WHERE collection_time >= @currentStart AND collection_time <= @currentEnd
        AND   execution_count > 0
        GROUP BY database_name, query_hash
        ORDER BY SUM(execution_count) DESC
        UNION
        SELECT TOP 100 database_name, query_hash
        FROM collect.query_store_stats
        WHERE collection_time >= @baselineStart AND collection_time <= @baselineEnd
        AND   execution_count > 0
        GROUP BY database_name, query_hash
        ORDER BY SUM(execution_count) DESC
    ) AS combined
),
current_period AS (
    SELECT th.database_name,
           CONVERT(nvarchar(20), th.query_hash, 1) AS query_hash,
           SUM(qs.execution_count) AS exec_count,
           SUM(CAST(qs.execution_count AS bigint) * qs.avg_duration_us) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(CAST(qs.execution_count AS bigint) * qs.avg_cpu_time_us) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(CAST(qs.execution_count AS bigint) * qs.avg_logical_io_reads) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN collect.query_store_stats qs
      ON  qs.query_hash = th.query_hash
      AND qs.database_name = th.database_name
    WHERE qs.collection_time >= @currentStart AND qs.collection_time <= @currentEnd
    AND   qs.execution_count > 0
    GROUP BY th.database_name, th.query_hash
),
baseline_period AS (
    SELECT th.database_name,
           CONVERT(nvarchar(20), th.query_hash, 1) AS query_hash,
           SUM(qs.execution_count) AS exec_count,
           SUM(CAST(qs.execution_count AS bigint) * qs.avg_duration_us) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(CAST(qs.execution_count AS bigint) * qs.avg_cpu_time_us) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(CAST(qs.execution_count AS bigint) * qs.avg_logical_io_reads) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN collect.query_store_stats qs
      ON  qs.query_hash = th.query_hash
      AND qs.database_name = th.database_name
    WHERE qs.collection_time >= @baselineStart AND qs.collection_time <= @baselineEnd
    AND   qs.execution_count > 0
    GROUP BY th.database_name, th.query_hash
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.query_hash, b.query_hash) AS query_hash,
       COALESCE(c.query_text, b.query_text) AS query_text,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  ISNULL(c.database_name, N'') = ISNULL(b.database_name, N'')
  AND ISNULL(c.query_hash, N'') = ISNULL(b.query_hash, N'');";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@currentStart", SqlDbType.DateTime2) { Value = currentStart });
            command.Parameters.Add(new SqlParameter("@currentEnd", SqlDbType.DateTime2) { Value = currentEnd });
            command.Parameters.Add(new SqlParameter("@baselineStart", SqlDbType.DateTime2) { Value = baselineStart });
            command.Parameters.Add(new SqlParameter("@baselineEnd", SqlDbType.DateTime2) { Value = baselineEnd });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new QueryStatsComparisonItem
                {
                    DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    ExecutionCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4), CultureInfo.InvariantCulture),
                    AvgCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5), CultureInfo.InvariantCulture),
                    AvgReads = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6), CultureInfo.InvariantCulture),
                    BaselineExecutionCount = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8), CultureInfo.InvariantCulture),
                    BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture),
                    BaselineAvgReads = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10), CultureInfo.InvariantCulture),
                });
            }

            return items;
        }
    }
}
