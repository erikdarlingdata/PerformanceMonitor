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

                public async Task<List<Models.TimeSliceBucket>> GetBlockingSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<Models.TimeSliceBucket>();
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
                        items.Add(new Models.TimeSliceBucket
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

                public async Task<List<Models.TimeSliceBucket>> GetDeadlockSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<Models.TimeSliceBucket>();
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
                        items.Add(new Models.TimeSliceBucket
                        {
                            BucketTime = reader.GetDateTime(0),
                            SessionCount = count,
                            Value = count,
                        });
                    }

                    return items;
                }

                public async Task<List<Models.TimeSliceBucket>> GetActiveQuerySlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<Models.TimeSliceBucket>();
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
                        items.Add(new Models.TimeSliceBucket
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

                public async Task<List<Models.TimeSliceBucket>> GetQueryStatsSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<Models.TimeSliceBucket>();
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
                        items.Add(new Models.TimeSliceBucket
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

                public async Task<List<Models.TimeSliceBucket>> GetProcStatsSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<Models.TimeSliceBucket>();
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
                        items.Add(new Models.TimeSliceBucket
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

                public async Task<List<Models.TimeSliceBucket>> GetQueryStoreSlicerDataAsync(
                    int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<Models.TimeSliceBucket>();
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
                        items.Add(new Models.TimeSliceBucket
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

        /// <summary>
        /// Fetches query plan XML on demand for a single query_store_data row.
        /// </summary>
        public async Task<string?> GetQueryStorePlanXmlByCollectionIdAsync(long collectionId)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            CAST(DECOMPRESS(qsd.query_plan_text) AS nvarchar(max)) AS query_plan_text
        FROM collect.query_store_data AS qsd
        WHERE qsd.collection_id = @collection_id;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@collection_id", SqlDbType.BigInt) { Value = collectionId });

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value || result == null ? null : (string)result;
        }

        /// <summary>
        /// Fetches query plan XML on demand for a single procedure_stats row.
        /// </summary>
        public async Task<string?> GetProcedureStatsPlanXmlByCollectionIdAsync(long collectionId)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            CAST(DECOMPRESS(ps.query_plan_text) AS nvarchar(max)) AS query_plan_text
        FROM collect.procedure_stats AS ps
        WHERE ps.collection_id = @collection_id;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@collection_id", SqlDbType.BigInt) { Value = collectionId });

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value || result == null ? null : (string)result;
        }

        /// <summary>
        /// Fetches query plan XML on demand for a single query_stats row.
        /// </summary>
        public async Task<string?> GetQueryStatsPlanXmlByCollectionIdAsync(long collectionId)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            CAST(DECOMPRESS(qs.query_plan_text) AS nvarchar(max)) AS query_plan_text
        FROM collect.query_stats AS qs
        WHERE qs.collection_id = @collection_id;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@collection_id", SqlDbType.BigInt) { Value = collectionId });

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value || result == null ? null : (string)result;
        }

        /// <summary>
        /// Fetches the most recent plan XML for a query identified by query_hash.
        /// Used by MCP plan analysis tools.
        /// </summary>
        public async Task<string?> GetPlanXmlByQueryHashAsync(string queryHash)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT TOP (1)
            CAST(DECOMPRESS(qs.query_plan_text) AS nvarchar(max))
        FROM collect.query_stats AS qs
        WHERE qs.query_hash = CONVERT(binary(8), @queryHash, 1)
        ORDER BY qs.last_execution_time DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@queryHash", SqlDbType.NVarChar, 20) { Value = queryHash });

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value || result == null ? null : (string)result;
        }

        /// <summary>
        /// Fetches the most recent plan XML for a procedure identified by sql_handle.
        /// Used by MCP plan analysis tools.
        /// </summary>
        public async Task<string?> GetProcedurePlanXmlBySqlHandleAsync(string sqlHandle)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT TOP (1)
            CAST(DECOMPRESS(ps.query_plan_text) AS nvarchar(max))
        FROM collect.procedure_stats AS ps
        WHERE ps.sql_handle = CONVERT(varbinary(64), @sqlHandle, 1)
        ORDER BY ps.last_execution_time DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@sqlHandle", SqlDbType.NVarChar, 130) { Value = sqlHandle });

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value || result == null ? null : (string)result;
        }

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

        /// <summary>
        /// Gets query stats comparison between a current time range and a baseline range.
        /// Uses delta columns for accurate period-level aggregation.
        /// </summary>
        public async Task<List<Models.QueryStatsComparisonItem>> GetQueryStatsComparisonAsync(
            DateTime currentStart, DateTime currentEnd,
            DateTime baselineStart, DateTime baselineEnd)
        {
            var items = new List<Models.QueryStatsComparisonItem>();

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
                items.Add(new Models.QueryStatsComparisonItem
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
        public async Task<List<Models.ProcedureStatsComparisonItem>> GetProcedureStatsComparisonAsync(
            DateTime currentStart, DateTime currentEnd,
            DateTime baselineStart, DateTime baselineEnd)
        {
            var items = new List<Models.ProcedureStatsComparisonItem>();

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
                items.Add(new Models.ProcedureStatsComparisonItem
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
        public async Task<List<Models.QueryStatsComparisonItem>> GetQueryStoreComparisonAsync(
            DateTime currentStart, DateTime currentEnd,
            DateTime baselineStart, DateTime baselineEnd)
        {
            var items = new List<Models.QueryStatsComparisonItem>();

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
                items.Add(new Models.QueryStatsComparisonItem
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
