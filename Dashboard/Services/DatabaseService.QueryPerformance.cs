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
        // ============================================
        // Query Performance Data Access
        // ============================================

                public async Task<List<ExpensiveQueryItem>> GetExpensiveQueriesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<ExpensiveQueryItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    // Use the report view with WHERE clause for date filtering based on execution times
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (20)
                                source,
                                database_name,
                                object_identifier,
                                object_name,
                                execution_count,
                                total_worker_time_sec,
                                avg_worker_time_ms,
                                total_elapsed_time_sec,
                                avg_elapsed_time_ms,
                                total_logical_reads,
                                avg_logical_reads,
                                total_logical_writes,
                                avg_logical_writes,
                                total_physical_reads,
                                avg_physical_reads,
                                max_grant_mb,
                                query_text_sample,
                                query_plan_xml,
                                first_execution_time,
                                last_execution_time
                            FROM report.expensive_queries_today
                            WHERE (first_execution_time >= @from_date AND first_execution_time <= @to_date)
                            OR    (last_execution_time >= @from_date AND last_execution_time <= @to_date)
                            OR    (first_execution_time <= @from_date AND last_execution_time >= @to_date)
                            ORDER BY
                                avg_worker_time_ms DESC
                            OPTION(RECOMPILE, HASH GROUP);";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (20)
                                source,
                                database_name,
                                object_identifier,
                                object_name,
                                execution_count,
                                total_worker_time_sec,
                                avg_worker_time_ms,
                                total_elapsed_time_sec,
                                avg_elapsed_time_ms,
                                total_logical_reads,
                                avg_logical_reads,
                                total_logical_writes,
                                avg_logical_writes,
                                total_physical_reads,
                                avg_physical_reads,
                                max_grant_mb,
                                query_text_sample,
                                query_plan_xml,
                                first_execution_time,
                                last_execution_time
                            FROM report.expensive_queries_today
                            WHERE last_execution_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                avg_worker_time_ms DESC
                            OPTION(RECOMPILE, HASH GROUP);";
                    }

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using (StartQueryTiming("Expensive Queries", query, connection))
                    {
                        using var reader = await command.ExecuteReaderAsync();
                        while (await reader.ReadAsync())
                        {
                            items.Add(new ExpensiveQueryItem
                            {
                                Source = reader.GetString(0),
                                DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                                ObjectIdentifier = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                                ObjectName = reader.IsDBNull(3) ? null : reader.GetString(3),
                                ExecutionCount = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                                TotalWorkerTimeSec = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture),
                                AvgWorkerTimeMs = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                                TotalElapsedTimeSec = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7), CultureInfo.InvariantCulture),
                                AvgElapsedTimeMs = reader.IsDBNull(8) ? 0m : Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture),
                                TotalLogicalReads = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9), CultureInfo.InvariantCulture),
                                AvgLogicalReads = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10), CultureInfo.InvariantCulture),
                                TotalLogicalWrites = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11), CultureInfo.InvariantCulture),
                                AvgLogicalWrites = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12), CultureInfo.InvariantCulture),
                                TotalPhysicalReads = reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13), CultureInfo.InvariantCulture),
                                AvgPhysicalReads = reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14), CultureInfo.InvariantCulture),
                                MaxGrantMb = reader.IsDBNull(15) ? null : Convert.ToDecimal(reader.GetValue(15), CultureInfo.InvariantCulture),
                                QueryTextSample = reader.IsDBNull(16) ? string.Empty : reader.GetString(16),
                                QueryPlanXml = reader.IsDBNull(17) ? null : reader.GetString(17),
                                FirstExecutionTime = reader.IsDBNull(18) ? null : reader.GetDateTime(18),
                                LastExecutionTime = reader.IsDBNull(19) ? null : reader.GetDateTime(19)
                            });
                        }
                    }

                    return items;
                }

                public async Task<List<CollectionLogEntry>> GetCollectionLogAsync(string collectorName)
                {
                    var items = new List<CollectionLogEntry>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            log_id,
                            collection_time,
                            collector_name,
                            collection_status,
                            rows_collected,
                            duration_ms,
                            error_message
                        FROM config.collection_log
                        WHERE collector_name = @collector_name
                        ORDER BY
                            collection_time DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@collector_name", SqlDbType.NVarChar, 100) { Value = collectorName });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CollectionLogEntry
                        {
                            LogId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            CollectorName = reader.GetString(2),
                            CollectionStatus = reader.GetString(3),
                            RowsCollected = reader.GetInt32(4),
                            DurationMs = reader.GetInt32(5),
                            ErrorMessage = reader.IsDBNull(6) ? null : reader.GetString(6)
                        });
                    }
        
                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetBlockingSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE b.collection_time >= @from_date AND b.collection_time <= @to_date"
                        : "WHERE b.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, b.collection_time), 0) AS bucket_hour,
    COUNT(*) AS event_count,
    ISNULL(SUM(b.wait_time_ms), 0) / 1000.0 AS total_wait_sec,
    COUNT(DISTINCT b.spid) AS distinct_blocked,
    COUNT(DISTINCT b.database_name) AS distinct_databases
FROM collect.blocking_BlockedProcessReport AS b
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, b.collection_time), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var eventCount = Convert.ToInt64(reader.GetValue(1));
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = eventCount,
                            TotalCpu = Convert.ToDouble(reader.GetValue(2)),
                            TotalReads = Convert.ToDouble(reader.GetValue(3)),
                            TotalLogicalReads = Convert.ToDouble(reader.GetValue(4)),
                            Value = eventCount,
                        });
                    }

                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetDeadlockSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE d.event_date >= @from_date AND d.event_date <= @to_date"
                        : "WHERE d.event_date >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, d.event_date), 0) AS bucket_hour,
    COUNT(*) AS deadlock_count
FROM collect.deadlocks AS d
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, d.event_date), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var count = Convert.ToInt64(reader.GetValue(1));
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = count,
                            Value = count,
                        });
                    }

                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetActiveQuerySlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND qs.collection_time >= @from_date AND qs.collection_time <= @to_date"
                        : "AND qs.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, qs.collection_time), 0) AS bucket_hour,
    COUNT(*) AS session_count,
    ISNULL(SUM(TRY_CAST(qs.CPU AS money)), 0) AS total_cpu,
    ISNULL(SUM(TRY_CAST(qs.CPU AS money)), 0) AS total_elapsed,
    ISNULL(SUM(TRY_CAST(qs.reads AS money)), 0) AS total_reads,
    ISNULL(SUM(TRY_CAST(qs.physical_reads AS money)), 0) AS total_physical_reads,
    ISNULL(SUM(TRY_CAST(qs.writes AS money)), 0) AS total_writes
FROM report.query_snapshots AS qs
WHERE CONVERT(nvarchar(max), qs.sql_text) NOT LIKE N'WAITFOR%'
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, qs.collection_time), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = Convert.ToInt64(reader.GetValue(1)),
                            TotalCpu = Convert.ToDouble(reader.GetValue(2)),
                            TotalElapsed = Convert.ToDouble(reader.GetValue(3)),
                            TotalReads = Convert.ToDouble(reader.GetValue(4)),
                            TotalLogicalReads = Convert.ToDouble(reader.GetValue(5)),
                            TotalWrites = Convert.ToDouble(reader.GetValue(6)),
                            Value = Convert.ToDouble(reader.GetValue(1)),
                        });
                    }
                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetQueryStatsSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND qs.collection_time >= @from_date AND qs.collection_time <= @to_date"
                        : "AND qs.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, qs.collection_time), 0) AS bucket_hour,
    COUNT(DISTINCT qs.query_hash) AS query_count,
    ISNULL(SUM(CAST(qs.total_worker_time AS float)), 0) / 1000.0 AS total_cpu_ms,
    ISNULL(SUM(CAST(qs.total_elapsed_time AS float)), 0) / 1000.0 AS total_elapsed_ms,
    ISNULL(SUM(CAST(qs.total_logical_reads AS float)), 0) AS total_reads,
    ISNULL(SUM(CAST(qs.total_physical_reads AS float)), 0) AS total_physical_reads,
    ISNULL(SUM(CAST(qs.total_logical_writes AS float)), 0) AS total_writes
FROM collect.query_stats AS qs
WHERE 1 = 1
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, qs.collection_time), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = Convert.ToInt64(reader.GetValue(1)),
                            TotalCpu = Convert.ToDouble(reader.GetValue(2)),
                            TotalElapsed = Convert.ToDouble(reader.GetValue(3)),
                            TotalReads = Convert.ToDouble(reader.GetValue(4)),
                            TotalLogicalReads = Convert.ToDouble(reader.GetValue(5)),
                            TotalWrites = Convert.ToDouble(reader.GetValue(6)),
                            Value = Convert.ToDouble(reader.GetValue(2)),
                        });
                    }
                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetProcStatsSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND ps.collection_time >= @from_date AND ps.collection_time <= @to_date"
                        : "AND ps.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, ps.collection_time), 0) AS bucket_hour,
    COUNT(DISTINCT ps.object_name) AS proc_count,
    ISNULL(SUM(CAST(ps.total_worker_time AS float)), 0) / 1000.0 AS total_cpu_ms,
    ISNULL(SUM(CAST(ps.total_elapsed_time AS float)), 0) / 1000.0 AS total_elapsed_ms,
    ISNULL(SUM(CAST(ps.total_logical_reads AS float)), 0) AS total_reads,
    ISNULL(SUM(CAST(ps.total_physical_reads AS float)), 0) AS total_physical_reads,
    ISNULL(SUM(CAST(ps.total_logical_writes AS float)), 0) AS total_writes
FROM collect.procedure_stats AS ps
WHERE 1 = 1
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, ps.collection_time), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = Convert.ToInt64(reader.GetValue(1)),
                            TotalCpu = Convert.ToDouble(reader.GetValue(2)),
                            TotalElapsed = Convert.ToDouble(reader.GetValue(3)),
                            TotalReads = Convert.ToDouble(reader.GetValue(4)),
                            TotalLogicalReads = Convert.ToDouble(reader.GetValue(5)),
                            TotalWrites = Convert.ToDouble(reader.GetValue(6)),
                            Value = Convert.ToDouble(reader.GetValue(2)),
                        });
                    }
                    return items;
                }

                public async Task<List<TimeSliceBucket>> GetQueryStoreSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TimeSliceBucket>();
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND qsd.collection_time >= @from_date AND qsd.collection_time <= @to_date"
                        : "AND qsd.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    DATEADD(HOUR, DATEDIFF(HOUR, 0, qsd.collection_time), 0) AS bucket_hour,
    COUNT(DISTINCT qsd.query_id) AS query_count,
    ISNULL(SUM(qsd.avg_cpu_time * qsd.count_executions), 0) / 1000.0 AS total_cpu_ms,
    ISNULL(SUM(qsd.avg_duration * qsd.count_executions), 0) / 1000.0 AS total_duration_ms,
    ISNULL(SUM(qsd.avg_logical_io_reads * qsd.count_executions), 0) AS total_reads,
    ISNULL(SUM(qsd.avg_physical_io_reads * qsd.count_executions), 0) AS total_physical_reads,
    ISNULL(SUM(qsd.avg_logical_io_writes * qsd.count_executions), 0) AS total_writes
FROM collect.query_store_data AS qsd
WHERE 1 = 1
{timeFilter}
GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, qsd.collection_time), 0)
ORDER BY bucket_hour;";

                    using var command = new SqlCommand(query, connection) { CommandTimeout = 120 };
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = Convert.ToInt64(reader.GetValue(1)),
                            TotalCpu = Convert.ToDouble(reader.GetValue(2)),
                            TotalElapsed = Convert.ToDouble(reader.GetValue(3)),
                            TotalReads = Convert.ToDouble(reader.GetValue(4)),
                            TotalLogicalReads = Convert.ToDouble(reader.GetValue(5)),
                            TotalWrites = Convert.ToDouble(reader.GetValue(6)),
                            Value = Convert.ToDouble(reader.GetValue(2)),
                        });
                    }
                    return items;
                }

                /// <summary>
                /// Fetches the query plan XML for a specific Query Store query on-demand.
                /// </summary>
                public async Task<string?> GetQueryStorePlanXmlAsync(string databaseName, long queryId)
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT TOP (1)
            qss.query_plan_xml
        FROM report.query_store_summary AS qss
        WHERE qss.database_name = @databaseName
        AND   qss.query_id = @queryId;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@databaseName", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@queryId", SqlDbType.BigInt) { Value = queryId });

                    var result = await command.ExecuteScalarAsync();
                    return result == DBNull.Value ? null : result as string;
                }

                /// <summary>
                /// Fetches (and DECOMPRESSes) the cached plan XML for a single query_stats row on demand.
                /// GetQueryStatsAsync deliberately does NOT hydrate plan XML for its TOP (500) grid rows
                /// (that DECOMPRESS cost ~7s of CPU); the plan is fetched here only when a plan is opened.
                /// </summary>
                public async Task<string?> GetQueryStatsPlanXmlAsync(string databaseName, string queryHash)
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT TOP (1)
            query_plan_xml = CAST(DECOMPRESS(qs.query_plan_text) AS nvarchar(max))
        FROM collect.query_stats AS qs
        WHERE qs.query_hash = CONVERT(binary(8), @queryHash, 1)
        AND   qs.database_name = @databaseName
        AND   qs.query_plan_text IS NOT NULL
        ORDER BY qs.collection_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@databaseName", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@queryHash", SqlDbType.NVarChar, 20) { Value = queryHash });

                    var result = await command.ExecuteScalarAsync();
                    return result == DBNull.Value ? null : result as string;
                }

                public async Task<List<SessionStatsItem>> GetSessionStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<SessionStatsItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    bool useCustomDates = fromDate.HasValue && toDate.HasValue;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ss.collection_id,
            ss.collection_time,
            ss.total_sessions,
            ss.running_sessions,
            ss.sleeping_sessions,
            ss.background_sessions,
            ss.dormant_sessions,
            ss.idle_sessions_over_30min,
            ss.sessions_waiting_for_memory,
            ss.databases_with_connections,
            ss.top_application_name,
            ss.top_application_connections,
            ss.top_host_name,
            ss.top_host_connections
        FROM collect.session_stats AS ss
        WHERE (
            (@useCustomDates = 0 AND ss.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME()))
            OR
            (@useCustomDates = 1 AND ss.collection_time >= @fromDate AND ss.collection_time <= @toDate)
        )
        ORDER BY
            ss.collection_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@useCustomDates", SqlDbType.Bit) { Value = useCustomDates });
                    command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = (object?)fromDate ?? DBNull.Value });
                    command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = (object?)toDate ?? DBNull.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new SessionStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            TotalSessions = reader.GetInt32(2),
                            RunningSessions = reader.GetInt32(3),
                            SleepingSessions = reader.GetInt32(4),
                            BackgroundSessions = reader.GetInt32(5),
                            DormantSessions = reader.GetInt32(6),
                            IdleSessionsOver30Min = reader.GetInt32(7),
                            SessionsWaitingForMemory = reader.GetInt32(8),
                            DatabasesWithConnections = reader.GetInt32(9),
                            TopApplicationName = reader.IsDBNull(10) ? null : reader.GetString(10),
                            TopApplicationConnections = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                            TopHostName = reader.IsDBNull(12) ? null : reader.GetString(12),
                            TopHostConnections = reader.IsDBNull(13) ? null : reader.GetInt32(13)
                        });
                    }
        
                    return items;
                }

    }
}
