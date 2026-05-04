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
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // Execution and duration trend aggregations, regressions, and long-running patterns.
        // ============================================

        /// <summary>
        /// Gets execution count trends from query stats deltas, aggregated by collection time.
        /// </summary>
        public async Task<List<ExecutionTrendItem>> GetExecutionTrendsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<ExecutionTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH exec_deltas AS
        (
            SELECT
                qs.collection_time,
                total_execution_count = SUM(qs.execution_count_delta),
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(qs.collection_time, 1, qs.collection_time) OVER
                        (
                            ORDER BY
                                qs.collection_time
                        ),
                        qs.collection_time
                    )
            FROM collect.query_stats AS qs
            WHERE qs.collection_time >= @from_date
            AND   qs.collection_time <= @to_date
            GROUP BY
                qs.collection_time
        )
        SELECT
            ed.collection_time,
            executions_per_second = CAST(CAST(ed.total_execution_count AS decimal(19, 4)) / ed.interval_seconds AS decimal(18, 4))
        FROM exec_deltas AS ed
        WHERE ed.interval_seconds > 0
        ORDER BY
            ed.collection_time;";
            }
            else
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH exec_deltas AS
        (
            SELECT
                qs.collection_time,
                total_execution_count = SUM(qs.execution_count_delta),
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(qs.collection_time, 1, qs.collection_time) OVER
                        (
                            ORDER BY
                                qs.collection_time
                        ),
                        qs.collection_time
                    )
            FROM collect.query_stats AS qs
            WHERE qs.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
            GROUP BY
                qs.collection_time
        )
        SELECT
            ed.collection_time,
            executions_per_second = CAST(CAST(ed.total_execution_count AS decimal(19, 4)) / ed.interval_seconds AS decimal(18, 4))
        FROM exec_deltas AS ed
        WHERE ed.interval_seconds > 0
        ORDER BY
            ed.collection_time;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new ExecutionTrendItem
                {
                    CollectionTime = reader.GetDateTime(0),
                    ExecutionsPerSecond = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets query duration trends from query_stats deltas, aggregated by collection time.
        /// Returns elapsed_ms_per_second (rate-normalized like the Lite Dashboard).
        /// </summary>
        public async Task<List<DurationTrendItem>> GetQueryDurationTrendsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<DurationTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH raw AS
        (
            SELECT
                qs.collection_time,
                total_elapsed_ms = SUM(qs.total_elapsed_time_delta) / 1000.0,
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(qs.collection_time, 1, qs.collection_time) OVER
                        (
                            ORDER BY
                                qs.collection_time
                        ),
                        qs.collection_time
                    )
            FROM collect.query_stats AS qs
            WHERE qs.collection_time >= @from_date
            AND   qs.collection_time <= @to_date
            GROUP BY
                qs.collection_time
        )
        SELECT
            r.collection_time,
            elapsed_ms_per_second = r.total_elapsed_ms / r.interval_seconds
        FROM raw AS r
        WHERE r.interval_seconds > 0
        ORDER BY
            r.collection_time;";
            }
            else
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH raw AS
        (
            SELECT
                qs.collection_time,
                total_elapsed_ms = SUM(qs.total_elapsed_time_delta) / 1000.0,
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(qs.collection_time, 1, qs.collection_time) OVER
                        (
                            ORDER BY
                                qs.collection_time
                        ),
                        qs.collection_time
                    )
            FROM collect.query_stats AS qs
            WHERE qs.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
            GROUP BY
                qs.collection_time
        )
        SELECT
            r.collection_time,
            elapsed_ms_per_second = r.total_elapsed_ms / r.interval_seconds
        FROM raw AS r
        WHERE r.interval_seconds > 0
        ORDER BY
            r.collection_time;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(1))
                {
                    items.Add(new DurationTrendItem
                    {
                        CollectionTime = reader.GetDateTime(0),
                        AvgDurationMs = Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets procedure duration trends from procedure_stats deltas, aggregated by collection time.
        /// Returns elapsed_ms_per_second (rate-normalized like the Lite Dashboard).
        /// </summary>
        public async Task<List<DurationTrendItem>> GetProcedureDurationTrendsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<DurationTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH raw AS
        (
            SELECT
                ps.collection_time,
                total_elapsed_ms = SUM(ps.total_elapsed_time_delta) / 1000.0,
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(ps.collection_time, 1, ps.collection_time) OVER
                        (
                            ORDER BY
                                ps.collection_time
                        ),
                        ps.collection_time
                    )
            FROM collect.procedure_stats AS ps
            WHERE ps.collection_time >= @from_date
            AND   ps.collection_time <= @to_date
            GROUP BY
                ps.collection_time
        )
        SELECT
            r.collection_time,
            elapsed_ms_per_second = r.total_elapsed_ms / r.interval_seconds
        FROM raw AS r
        WHERE r.interval_seconds > 0
        ORDER BY
            r.collection_time;";
            }
            else
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH raw AS
        (
            SELECT
                ps.collection_time,
                total_elapsed_ms = SUM(ps.total_elapsed_time_delta) / 1000.0,
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(ps.collection_time, 1, ps.collection_time) OVER
                        (
                            ORDER BY
                                ps.collection_time
                        ),
                        ps.collection_time
                    )
            FROM collect.procedure_stats AS ps
            WHERE ps.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
            GROUP BY
                ps.collection_time
        )
        SELECT
            r.collection_time,
            elapsed_ms_per_second = r.total_elapsed_ms / r.interval_seconds
        FROM raw AS r
        WHERE r.interval_seconds > 0
        ORDER BY
            r.collection_time;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(1))
                {
                    items.Add(new DurationTrendItem
                    {
                        CollectionTime = reader.GetDateTime(0),
                        AvgDurationMs = Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture)
                    });
                }
            }

            return items;
        }

        /// <summary>
        /// Gets Query Store duration trends, aggregated by collection time.
        /// Returns elapsed_ms_per_second (rate-normalized like the Lite Dashboard).
        /// Query Store has no delta columns, so uses avg_duration * count_executions as total work.
        /// </summary>
        public async Task<List<DurationTrendItem>> GetQueryStoreDurationTrendsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<DurationTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH raw AS
        (
            SELECT
                qsd.collection_time,
                total_elapsed_ms = SUM(qsd.avg_duration * qsd.count_executions) / 1000.0,
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(qsd.collection_time, 1, qsd.collection_time) OVER
                        (
                            ORDER BY
                                qsd.collection_time
                        ),
                        qsd.collection_time
                    )
            FROM collect.query_store_data AS qsd
            WHERE qsd.collection_time >= @from_date
            AND   qsd.collection_time <= @to_date
            GROUP BY
                qsd.collection_time
        )
        SELECT
            r.collection_time,
            elapsed_ms_per_second = r.total_elapsed_ms / r.interval_seconds
        FROM raw AS r
        WHERE r.interval_seconds > 0
        ORDER BY
            r.collection_time;";
            }
            else
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH raw AS
        (
            SELECT
                qsd.collection_time,
                total_elapsed_ms = SUM(qsd.avg_duration * qsd.count_executions) / 1000.0,
                interval_seconds =
                    DATEDIFF
                    (
                        SECOND,
                        LAG(qsd.collection_time, 1, qsd.collection_time) OVER
                        (
                            ORDER BY
                                qsd.collection_time
                        ),
                        qsd.collection_time
                    )
            FROM collect.query_store_data AS qsd
            WHERE qsd.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
            GROUP BY
                qsd.collection_time
        )
        SELECT
            r.collection_time,
            elapsed_ms_per_second = r.total_elapsed_ms / r.interval_seconds
        FROM raw AS r
        WHERE r.interval_seconds > 0
        ORDER BY
            r.collection_time;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(1))
                {
                    items.Add(new DurationTrendItem
                    {
                        CollectionTime = reader.GetDateTime(0),
                        AvgDurationMs = Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture)
                    });
                }
            }

            return items;
        }

                public async Task<List<QueryStoreRegressionItem>> GetQueryStoreRegressionsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QueryStoreRegressionItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    /*
                    report.query_store_regressions inline TVF:
                    - Bounded baseline (mirror window before @start_date, same duration as recent)
                    - Weighted averages (execution-count weighted)
                    - Multi-metric detection (duration, CPU, or reads >25%)
                    - Absolute minimums (baseline >= 1ms duration or >= 100 reads)
                    - Ranked by additional_duration_ms (total extra time = delta * exec count)
                    */
                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            qsr.database_name,
            qsr.query_id,
            qsr.baseline_duration_ms,
            qsr.recent_duration_ms,
            qsr.duration_regression_percent,
            qsr.baseline_cpu_ms,
            qsr.recent_cpu_ms,
            qsr.cpu_regression_percent,
            qsr.baseline_reads,
            qsr.recent_reads,
            qsr.io_regression_percent,
            qsr.additional_duration_ms,
            qsr.baseline_exec_count,
            qsr.recent_exec_count,
            qsr.baseline_plan_count,
            qsr.recent_plan_count,
            qsr.severity,
            qsr.query_text_sample,
            qsr.last_execution_time
        FROM report.query_store_regressions(@start_date, @end_date) AS qsr
        ORDER BY
            qsr.additional_duration_ms DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    /*Calculate the time window - baseline is mirror window before start_date, recent is start_date to end_date*/
                    DateTime startDate;
                    DateTime endDate;

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        startDate = fromDate.Value;
                        /*If toDate is at midnight (date-only selection), extend to end of that day*/
                        endDate = toDate.Value.TimeOfDay == TimeSpan.Zero
                            ? toDate.Value.AddDays(1).AddTicks(-1)
                            : toDate.Value;
                    }
                    else
                    {
                        startDate = Helpers.ServerTimeHelper.ServerNow.AddHours(-hoursBack);
                        endDate = Helpers.ServerTimeHelper.ServerNow;
                    }

                    command.Parameters.Add(new SqlParameter("@start_date", SqlDbType.DateTime2) { Value = startDate });
                    command.Parameters.Add(new SqlParameter("@end_date", SqlDbType.DateTime2) { Value = endDate });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new QueryStoreRegressionItem
                        {
                            DatabaseName = reader.GetString(0),
                            QueryId = reader.GetInt64(1),
                            BaselineDurationMs = reader.IsDBNull(2) ? 0 : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                            RecentDurationMs = reader.IsDBNull(3) ? 0 : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                            DurationRegressionPercent = reader.IsDBNull(4) ? 0 : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture),
                            BaselineCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture),
                            RecentCpuMs = reader.IsDBNull(6) ? 0 : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                            CpuRegressionPercent = reader.IsDBNull(7) ? 0 : Convert.ToDecimal(reader.GetValue(7), CultureInfo.InvariantCulture),
                            BaselineReads = reader.IsDBNull(8) ? 0 : Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture),
                            RecentReads = reader.IsDBNull(9) ? 0 : Convert.ToDecimal(reader.GetValue(9), CultureInfo.InvariantCulture),
                            IoRegressionPercent = reader.IsDBNull(10) ? 0 : Convert.ToDecimal(reader.GetValue(10), CultureInfo.InvariantCulture),
                            AdditionalDurationMs = reader.IsDBNull(11) ? 0 : Convert.ToDecimal(reader.GetValue(11), CultureInfo.InvariantCulture),
                            BaselineExecCount = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
                            RecentExecCount = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
                            BaselinePlanCount = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                            RecentPlanCount = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15)),
                            Severity = reader.IsDBNull(16) ? string.Empty : reader.GetString(16),
                            QueryTextSample = reader.IsDBNull(17) ? string.Empty : reader.GetString(17),
                            LastExecutionTime = reader.IsDBNull(18) ? null : reader.GetDateTime(18)
                        });
                    }
        
                    return items;
                }

                public async Task<List<LongRunningQueryPatternItem>> GetLongRunningQueryPatternsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<LongRunningQueryPatternItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    /* Inline the aggregation with time-bounded CTE instead of using the view.
                       The view aggregates ALL time then takes TOP 50 by avg_duration, which causes
                       the dashboard's time filter to find zero matches when recent patterns are
                       shorter-running than old load test patterns (GitHub issue #168). */
                    string timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "ta.end_time >= @from_date AND ta.end_time <= @to_date"
                        : "ta.end_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            query_patterns AS
        (
            SELECT
                ta.database_name,
                query_pattern = LEFT(ta.sql_text, 200),
                executions = COUNT_BIG(*),
                avg_duration_ms = AVG(ta.duration_ms),
                max_duration_ms = MAX(ta.duration_ms),
                avg_cpu_ms = AVG(ta.cpu_ms),
                avg_reads = AVG(ta.reads),
                avg_writes = AVG(ta.writes),
                sample_query_text = MAX(ta.sql_text),
                last_execution = MAX(ta.end_time)
            FROM collect.trace_analysis AS ta
            WHERE {timeFilter}
            GROUP BY
                ta.database_name,
                LEFT(ta.sql_text, 200)
        )
        SELECT TOP (50)
            database_name,
            query_pattern,
            executions,
            avg_duration_sec = avg_duration_ms / 1000.0,
            max_duration_sec = max_duration_ms / 1000.0,
            avg_cpu_sec = avg_cpu_ms / 1000.0,
            avg_reads,
            avg_writes,
            concern_level =
                CASE
                    WHEN avg_duration_ms > 60000 THEN N'CRITICAL - Avg > 1 minute'
                    WHEN avg_duration_ms > 30000 THEN N'HIGH - Avg > 30 seconds'
                    WHEN avg_duration_ms > 10000 THEN N'MEDIUM - Avg > 10 seconds'
                    ELSE N'INFO'
                END,
            recommendation =
                CASE
                    WHEN avg_reads > 1000000 THEN N'High read count - check for missing indexes, table scans'
                    WHEN avg_cpu_ms > avg_duration_ms * 0.8 THEN N'CPU-bound query - check for complex calculations, functions'
                    WHEN avg_writes > 100000 THEN N'High write volume - review update/delete patterns'
                    ELSE N'Review execution plan for optimization opportunities'
                END,
            sample_query_text,
            last_execution
        FROM query_patterns
        WHERE executions > 1
        ORDER BY
            avg_duration_ms DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new LongRunningQueryPatternItem
                        {
                            DatabaseName = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                            QueryPattern = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            Executions = reader.GetInt64(2),
                            AvgDurationSec = reader.IsDBNull(3) ? 0 : reader.GetDecimal(3),
                            MaxDurationSec = reader.IsDBNull(4) ? 0 : reader.GetDecimal(4),
                            AvgCpuSec = reader.IsDBNull(5) ? 0 : reader.GetDecimal(5),
                            AvgReads = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            AvgWrites = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            ConcernLevel = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                            Recommendation = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
                            SampleQueryText = reader.IsDBNull(10) ? string.Empty : reader.GetString(10),
                            LastExecution = reader.IsDBNull(11) ? null : reader.GetDateTime(11)
                        });
                    }
        
                    return items;
                }

    }
}
