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
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // Overview Tab Data Access
        // ============================================

                public async Task<List<DailySummaryItem>> GetDailySummaryAsync(DateTime? summaryDate = null, CpuAlertMode cpuAlertMode = CpuAlertMode.Total)
                {
                    var items = new List<DailySummaryItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    // CPU column for the High CPU events count + critical-health check (PM#1004).
                    // The view at report.daily_summary always uses total_cpu_utilization (no per-user prefs available there).
                    // This date-parameterized path additionally honors the user's CpuAlertMode.
                    // Total mode coalesces to the SQL-only figure because total_cpu_utilization is
                    // NULL on SQL Server on Linux, where host CPU is not derivable (Issue #1048).
                    // Expressions are fully cus.-qualified so they drop straight into the predicates below.
                    string cpuColumn = cpuAlertMode == CpuAlertMode.SqlOnly
                        ? "cus.sqlserver_cpu_utilization"
                        : "ISNULL(cus.total_cpu_utilization, cus.sqlserver_cpu_utilization)";

                    // If no date provided, use the view directly (today's summary)
                    // Otherwise, replicate the view logic with the specified date
                    string query;
                    if (summaryDate.HasValue)
                    {
                        query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        DECLARE
            @target_date date = @summary_date,
            @day_start datetime2(7),
            @day_end datetime2(7);
        
        SELECT
            @day_start = CONVERT(datetime2(7), @target_date),
            @day_end = DATEADD(DAY, 1, CONVERT(datetime2(7), @target_date));
        
        SELECT
            summary_date = @target_date,
            total_wait_time_sec =
            (
                SELECT
                    SUM(ws.wait_time_ms_delta) / 1000.0
                FROM collect.wait_stats AS ws
                WHERE ws.collection_time >= @day_start
                AND   ws.collection_time < @day_end
                AND   ws.wait_time_ms_delta > 0
            ),
            top_wait_type =
            (
                SELECT TOP (1)
                    ws.wait_type
                FROM collect.wait_stats AS ws
                WHERE ws.collection_time >= @day_start
                AND   ws.collection_time < @day_end
                AND   ws.wait_time_ms_delta > 0
                ORDER BY
                    ws.wait_time_ms_delta DESC
            ),
            expensive_queries_count =
            (
                SELECT
                    COUNT_BIG(DISTINCT qs.query_hash)
                FROM collect.query_stats AS qs
                WHERE qs.collection_time >= @day_start
                AND   qs.collection_time < @day_end
            ),
            deadlock_count =
            (
                SELECT
                    COUNT_BIG(*)
                FROM collect.deadlock_xml AS dx
                WHERE dx.collection_time >= @day_start
                AND   dx.collection_time < @day_end
            ),
            blocking_events_count =
            (
                SELECT
                    COUNT_BIG(*)
                FROM collect.blocked_process_xml AS bpx
                WHERE bpx.collection_time >= @day_start
                AND   bpx.collection_time < @day_end
            ),
            memory_pressure_events =
            (
                SELECT
                    COUNT_BIG(*)
                FROM collect.memory_pressure_events AS mpe
                WHERE mpe.collection_time >= @day_start
                AND   mpe.collection_time < @day_end
                AND   (mpe.memory_indicators_process >= 2 OR mpe.memory_indicators_system >= 2)
            ),
            high_cpu_events =
            (
                SELECT
                    COUNT_BIG(*)
                FROM collect.cpu_utilization_stats AS cus
                WHERE {cpuColumn} >= 80
                AND   cus.collection_time >= @day_start
                AND   cus.collection_time < @day_end
            ),
            collectors_failing =
            (
                SELECT
                    COUNT_BIG(*)
                FROM report.collection_health AS ch
                WHERE ch.health_status IN (N'STALE', N'FAILING', N'NEVER_RUN')
            ),
            overall_health =
                CASE
                    WHEN EXISTS
                    (
                        SELECT
                            1/0
                        FROM collect.deadlock_xml AS dx
                        WHERE dx.collection_time >= @day_start
                        AND   dx.collection_time < @day_end
                    )
                    THEN N'DEADLOCKS_OCCURRED'
                    WHEN EXISTS
                    (
                        SELECT
                            1/0
                        FROM collect.cpu_utilization_stats AS cus
                        WHERE {cpuColumn} >= 90
                        AND   cus.collection_time >= @day_start
                        AND   cus.collection_time < @day_end
                    )
                    THEN N'CPU_CRITICAL'
                    WHEN EXISTS
                    (
                        SELECT
                            1/0
                        FROM collect.memory_pressure_events AS mpe
                        WHERE mpe.memory_indicators_process >= 3
                        AND   mpe.collection_time >= @day_start
                        AND   mpe.collection_time < @day_end
                    )
                    THEN N'MEMORY_CRITICAL'
                    ELSE N'NORMAL'
                END;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ds.summary_date,
            ds.total_wait_time_sec,
            ds.top_wait_type,
            ds.expensive_queries_count,
            ds.deadlock_count,
            ds.blocking_events_count,
            ds.memory_pressure_events,
            ds.high_cpu_events,
            ds.collectors_failing,
            ds.overall_health
        FROM report.daily_summary AS ds;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    if (summaryDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@summary_date", SqlDbType.Date) { Value = summaryDate.Value.Date });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new DailySummaryItem
                        {
                            SummaryDate = reader.GetDateTime(0),
                            TotalWaitTimeSec = reader.IsDBNull(1) ? null : reader.GetDecimal(1),
                            TopWaitType = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            ExpensiveQueriesCount = reader.GetInt64(3),
                            DeadlockCount = reader.GetInt64(4),
                            BlockingEventsCount = reader.GetInt64(5),
                            MemoryPressureEvents = reader.GetInt64(6),
                            HighCpuEvents = reader.GetInt64(7),
                            CollectorsFailing = reader.GetInt64(8),
                            OverallHealth = reader.IsDBNull(9) ? string.Empty : reader.GetString(9)
                        });
                    }
        
                    return items;
                }

                public async Task<List<CriticalIssueItem>> GetCriticalIssuesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<CriticalIssueItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ci.issue_id,
            ci.log_date,
            ci.severity,
            ci.problem_area,
            ci.source_collector,
            ci.affected_database,
            ci.message,
            ci.investigate_query,
            ci.threshold_value,
            ci.threshold_limit
        FROM config.critical_issues AS ci
        WHERE ci.log_date >= @from_date AND ci.log_date <= @to_date
        ORDER BY ci.log_date DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ci.issue_id,
            ci.log_date,
            ci.severity,
            ci.problem_area,
            ci.source_collector,
            ci.affected_database,
            ci.message,
            ci.investigate_query,
            ci.threshold_value,
            ci.threshold_limit
        FROM config.critical_issues AS ci
        WHERE ci.log_date >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY ci.log_date DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CriticalIssueItem
                        {
                            IssueId = reader.GetInt64(0),
                            LogDate = reader.GetDateTime(1),
                            Severity = reader.GetString(2),
                            ProblemArea = reader.GetString(3),
                            SourceCollector = reader.GetString(4),
                            AffectedDatabase = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            Message = reader.GetString(6),
                            InvestigateQuery = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            ThresholdValue = reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                            ThresholdLimit = reader.IsDBNull(9) ? null : reader.GetDecimal(9)
                        });
                    }
        
                    return items;
                }

        public async Task<List<RunningJobItem>> GetRunningJobsAsync()
        {
            var items = new List<RunningJobItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            rj.job_name,
            rj.job_id,
            rj.job_enabled,
            rj.start_time,
            rj.current_duration_seconds,
            rj.current_duration_formatted,
            rj.avg_duration_seconds,
            rj.avg_duration_formatted,
            rj.p95_duration_seconds,
            rj.successful_run_count,
            rj.is_running_long,
            rj.percent_of_average
        FROM report.running_jobs AS rj
        ORDER BY
            rj.is_running_long DESC,
            rj.current_duration_seconds DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("Running Jobs", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new RunningJobItem
                    {
                        JobName = reader.GetString(0),
                        JobId = reader.GetGuid(1),
                        JobEnabled = reader.GetBoolean(2),
                        StartTime = reader.GetDateTime(3),
                        CurrentDurationSeconds = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), System.Globalization.CultureInfo.InvariantCulture),
                        CurrentDurationFormatted = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                        AvgDurationSeconds = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), System.Globalization.CultureInfo.InvariantCulture),
                        AvgDurationFormatted = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                        P95DurationSeconds = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8), System.Globalization.CultureInfo.InvariantCulture),
                        SuccessfulRunCount = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9), System.Globalization.CultureInfo.InvariantCulture),
                        IsRunningLong = reader.IsDBNull(10) ? false : reader.GetBoolean(10),
                        PercentOfAverage = reader.IsDBNull(11) ? null : Convert.ToDecimal(reader.GetValue(11), System.Globalization.CultureInfo.InvariantCulture)
                    });
                }
            }

            return items;
        }
    }
}
