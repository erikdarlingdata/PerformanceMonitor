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
        // Resource Metrics Data Access
        // ============================================

                public async Task<List<WaitStatItem>> GetWaitStatsAsync()
                {
                    var items = new List<WaitStatItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            wait_type,
                            wait_time_ms,
                            wait_time_sec,
                            waiting_tasks,
                            signal_wait_ms,
                            resource_wait_ms,
                            avg_wait_ms_per_task,
                            last_seen
                        FROM report.top_waits_last_hour
                        ORDER BY
                            wait_time_ms DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    using (StartQueryTiming("Wait Stats", query, connection))
                    {
                        using var reader = await command.ExecuteReaderAsync();
                        while (await reader.ReadAsync())
                        {
                            items.Add(new WaitStatItem
                            {
                                WaitType = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                                WaitTimeMs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
                                WaitTimeSec = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                                WaitingTasks = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
                                SignalWaitMs = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                                ResourceWaitMs = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                                AvgWaitMsPerTask = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                                LastSeen = reader.IsDBNull(7) ? DateTime.MinValue : reader.GetDateTime(7)
                            });
                        }
                    }

                    return items;
                }

                public async Task<CpuPressureItem?> GetCpuPressureAsync()
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            collection_time,
                            total_schedulers,
                            total_runnable_tasks,
                            avg_runnable_tasks_per_scheduler,
                            total_workers,
                            max_workers,
                            worker_utilization_percent,
                            runnable_percent,
                            total_queued_requests,
                            total_active_requests,
                            pressure_level,
                            recommendation,
                            worker_thread_exhaustion_warning,
                            runnable_tasks_warning,
                            blocked_tasks_warning,
                            queued_requests_warning,
                            physical_memory_pressure_warning
                        FROM report.cpu_scheduler_pressure;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        return new CpuPressureItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            TotalSchedulers = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                            TotalRunnableTasks = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                            AvgRunnableTasksPerScheduler = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                            TotalWorkers = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture),
                            MaxWorkers = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                            WorkerUtilizationPercent = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                            RunnablePercent = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7), CultureInfo.InvariantCulture),
                            TotalQueuedRequests = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture),
                            TotalActiveRequests = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture),
                            PressureLevel = reader.IsDBNull(10) ? string.Empty : reader.GetString(10),
                            Recommendation = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                            WorkerThreadExhaustionWarning = reader.IsDBNull(12) ? false : reader.GetBoolean(12),
                            RunnableTasksWarning = reader.IsDBNull(13) ? false : reader.GetBoolean(13),
                            BlockedTasksWarning = reader.IsDBNull(14) ? false : reader.GetBoolean(14),
                            QueuedRequestsWarning = reader.IsDBNull(15) ? false : reader.GetBoolean(15),
                            PhysicalMemoryPressureWarning = reader.IsDBNull(16) ? false : reader.GetBoolean(16)
                        };
                    }
        
                    return null;
                }

                public async Task<MemoryPressureItem?> GetMemoryPressureAsync()
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            collection_time,
                            active_grants,
                            queries_waiting,
                            available_memory_mb,
                            granted_memory_mb,
                            used_memory_mb,
                            memory_utilization_percent,
                            timeout_errors,
                            forced_grants,
                            pressure_level,
                            recommendation
                        FROM report.memory_grant_pressure;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        return new MemoryPressureItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            ActiveGrants = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                            QueriesWaiting = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                            AvailableMemoryMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                            GrantedMemoryMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture),
                            UsedMemoryMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture),
                            MemoryUtilizationPercent = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                            TimeoutErrors = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
                            ForcedGrants = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture),
                            PressureLevel = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
                            Recommendation = reader.IsDBNull(10) ? string.Empty : reader.GetString(10)
                        };
                    }

                    return null;
                }

                public async Task<List<FileIoLatencyItem>> GetFileIoLatencyAsync()
                {
                    var items = new List<FileIoLatencyItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            database_name,
                            file_type,
                            file_name,
                            avg_read_latency_ms,
                            avg_write_latency_ms,
                            reads_last_15min,
                            writes_last_15min,
                            latency_issue,
                            recommendation,
                            last_seen
                        FROM report.file_io_latency
                        ORDER BY
                            avg_read_latency_ms DESC,
                            avg_write_latency_ms DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoLatencyItem
                        {
                            DatabaseName = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                            FileType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            AvgReadLatencyMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                            AvgWriteLatencyMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture),
                            ReadsLast15Min = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                            WritesLast15Min = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                            LatencyIssue = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            Recommendation = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                            LastSeen = reader.IsDBNull(9) ? DateTime.MinValue : reader.GetDateTime(9)
                        });
                    }
        
                    return items;
                }

                public async Task<List<CpuDataPoint>> GetCpuDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<CpuDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                sample_time,
                                sqlserver_cpu_utilization,
                                other_process_cpu_utilization,
                                total_cpu_utilization
                            FROM collect.cpu_utilization_stats
                            WHERE collection_time >= @from_date
                            AND collection_time <= @to_date
                            ORDER BY
                                sample_time ASC;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                sample_time,
                                sqlserver_cpu_utilization,
                                other_process_cpu_utilization,
                                total_cpu_utilization
                            FROM collect.cpu_utilization_stats
                            WHERE collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                sample_time ASC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CpuDataPoint
                        {
                            SampleTime = reader.GetDateTime(0),
                            SqlServerCpu = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            OtherProcessCpu = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                            TotalCpu = reader.IsDBNull(3) ? 0 : reader.GetInt32(3)
                        });
                    }
        
                    return items;
                }

                public async Task<List<MemoryDataPoint>> GetMemoryDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<MemoryDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                ms.collection_time,
                                ms.buffer_pool_mb,
                                ms.plan_cache_mb,
                                ms.physical_memory_in_use_mb,
                                ms.available_physical_memory_mb,
                                ms.memory_utilization_percentage,
                                ms.total_memory_mb,
                                granted_memory_mb = ISNULL(
                                    (
                                        SELECT
                                            SUM(mgs.granted_memory_mb)
                                        FROM collect.memory_grant_stats AS mgs
                                        WHERE mgs.collection_time = ms.collection_time
                                    ), 0)
                            FROM collect.memory_stats AS ms
                            WHERE ms.collection_time >= @from_date
                            AND   ms.collection_time <= @to_date
                            ORDER BY
                                ms.collection_time ASC;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                ms.collection_time,
                                ms.buffer_pool_mb,
                                ms.plan_cache_mb,
                                ms.physical_memory_in_use_mb,
                                ms.available_physical_memory_mb,
                                ms.memory_utilization_percentage,
                                ms.total_memory_mb,
                                granted_memory_mb = ISNULL(
                                    (
                                        SELECT
                                            SUM(mgs.granted_memory_mb)
                                        FROM collect.memory_grant_stats AS mgs
                                        WHERE mgs.collection_time = ms.collection_time
                                    ), 0)
                            FROM collect.memory_stats AS ms
                            WHERE ms.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                ms.collection_time ASC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new MemoryDataPoint
                        {
                            CollectionTime = reader.GetDateTime(0),
                            BufferPoolMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1),
                            PlanCacheMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                            PhysicalMemoryInUseMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                            AvailablePhysicalMemoryMb = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            MemoryUtilizationPercentage = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                            TotalMemoryMb = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            GrantedMemoryMb = reader.IsDBNull(7) ? 0m : reader.GetDecimal(7)
                        });
                    }
        
                    return items;
                }

                public async Task<List<WaitStatsDataPoint>> GetWaitStatsDataAsync(int hoursBack = 24, int topWaitTypes = 5, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<WaitStatsDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            WITH top_waits AS
                            (
                                SELECT TOP (@top_wait_types)
                                    wait_type
                                FROM collect.wait_stats
                                WHERE collection_time >= @from_date
                                AND   collection_time <= @to_date
                                GROUP BY
                                    wait_type
                                ORDER BY
                                    MAX(wait_time_ms) DESC
                            ),
                            wait_deltas AS
                            (
                                SELECT
                                    collection_time = ws.collection_time,
                                    wait_type = ws.wait_type,
                                    wait_time_ms_delta =
                                        ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
                                    signal_wait_time_ms_delta =
                                        ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
                                    interval_seconds =
                                        DATEDIFF
                                        (
                                            SECOND,
                                            LAG(ws.collection_time, 1, ws.collection_time) OVER
                                            (
                                                PARTITION BY
                                                    ws.wait_type
                                                ORDER BY
                                                    ws.collection_time
                                            ),
                                            ws.collection_time
                                        ),
                                    waiting_tasks_count = ws.waiting_tasks_count
                                FROM collect.wait_stats AS ws
                                WHERE ws.collection_time >= @from_date
                                AND   ws.collection_time <= @to_date
                                AND   ws.wait_type IN (SELECT wait_type FROM top_waits)
                            )
                            SELECT
                                wd.collection_time,
                                wd.wait_type,
                                wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
                                        ELSE 0
                                    END,
                                signal_wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
                                        ELSE 0
                                    END,
                                wd.waiting_tasks_count
                            FROM wait_deltas AS wd
                            WHERE wd.wait_time_ms_delta >= 0
                            ORDER BY
                                wd.collection_time ASC,
                                wd.wait_type;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            WITH top_waits AS
                            (
                                SELECT TOP (@top_wait_types)
                                    wait_type
                                FROM collect.wait_stats
                                WHERE collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                                GROUP BY
                                    wait_type
                                ORDER BY
                                    MAX(wait_time_ms) DESC
                            ),
                            wait_deltas AS
                            (
                                SELECT
                                    collection_time = ws.collection_time,
                                    wait_type = ws.wait_type,
                                    wait_time_ms_delta =
                                        ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
                                    signal_wait_time_ms_delta =
                                        ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
                                    interval_seconds =
                                        DATEDIFF
                                        (
                                            SECOND,
                                            LAG(ws.collection_time, 1, ws.collection_time) OVER
                                            (
                                                PARTITION BY
                                                    ws.wait_type
                                                ORDER BY
                                                    ws.collection_time
                                            ),
                                            ws.collection_time
                                        ),
                                    waiting_tasks_count = ws.waiting_tasks_count
                                FROM collect.wait_stats AS ws
                                WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                                AND   ws.wait_type IN (SELECT wait_type FROM top_waits)
                            )
                            SELECT
                                wd.collection_time,
                                wd.wait_type,
                                wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
                                        ELSE 0
                                    END,
                                signal_wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
                                        ELSE 0
                                    END,
                                wd.waiting_tasks_count
                            FROM wait_deltas AS wd
                            WHERE wd.wait_time_ms_delta >= 0
                            ORDER BY
                                wd.collection_time ASC,
                                wd.wait_type;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    command.Parameters.Add(new SqlParameter("@top_wait_types", SqlDbType.Int) { Value = topWaitTypes });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new WaitStatsDataPoint
                        {
                            CollectionTime = reader.GetDateTime(0),
                            WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                            SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture)
                        });
                    }
        
                    return items;
                }

                public async Task<List<FileIoDataPoint>> GetFileIoDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<FileIoDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                fio.collection_time,
                                fio.database_name,
                                fio.file_name,
                                fio.file_type_desc,
                                avg_read_latency_ms =
                                    CASE
                                        WHEN fio.num_of_reads > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_read_ms * 1.0 / fio.num_of_reads)
                                        ELSE 0
                                    END,
                                avg_write_latency_ms =
                                    CASE
                                        WHEN fio.num_of_writes > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_write_ms * 1.0 / fio.num_of_writes)
                                        ELSE 0
                                    END
                            FROM collect.file_io_stats AS fio
                            WHERE fio.collection_time >= @from_date
                            AND   fio.collection_time <= @to_date
                            ORDER BY
                                fio.collection_time ASC,
                                fio.database_name,
                                fio.file_name;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                fio.collection_time,
                                fio.database_name,
                                fio.file_name,
                                fio.file_type_desc,
                                avg_read_latency_ms =
                                    CASE
                                        WHEN fio.num_of_reads > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_read_ms * 1.0 / fio.num_of_reads)
                                        ELSE 0
                                    END,
                                avg_write_latency_ms =
                                    CASE
                                        WHEN fio.num_of_writes > 0
                                        THEN CONVERT(decimal(19,2), fio.io_stall_write_ms * 1.0 / fio.num_of_writes)
                                        ELSE 0
                                    END
                            FROM collect.file_io_stats AS fio
                            WHERE fio.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                fio.collection_time ASC,
                                fio.database_name,
                                fio.file_name;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoDataPoint
                        {
                            CollectionTime = reader.GetDateTime(0),
                            DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            FileType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            AvgReadLatencyMs = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            AvgWriteLatencyMs = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5)
                        });
                    }
        
                    return items;
                }

                public async Task<List<CpuUtilizationHistoryItem>> GetCpuUtilizationHistoryAsync(int hoursBack = 24)
                {
                    var items = new List<CpuUtilizationHistoryItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            collection_time,
                            sample_time,
                            sqlserver_cpu_utilization,
                            other_process_cpu_utilization,
                            total_cpu_utilization
                        FROM collect.cpu_utilization_stats
                        WHERE collection_time >= DATEADD(HOUR, @HoursBack, SYSDATETIME())
                        ORDER BY collection_time ASC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.Parameters.Add(new SqlParameter("@HoursBack", SqlDbType.Int) { Value = -hoursBack });
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CpuUtilizationHistoryItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            SampleTime = reader.GetDateTime(1),
                            SqlServerCpuUtilization = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                            OtherProcessCpuUtilization = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                            TotalCpuUtilization = reader.IsDBNull(4) ? 0 : reader.GetInt32(4)
                        });
                    }
        
                    return items;
                }

                public async Task<List<MemoryHistoryItem>> GetMemoryHistoryAsync(int hoursBack = 24)
                {
                    var items = new List<MemoryHistoryItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            collection_time,
                            buffer_pool_mb,
                            plan_cache_mb,
                            other_memory_mb,
                            total_memory_mb,
                            physical_memory_in_use_mb,
                            available_physical_memory_mb,
                            memory_utilization_percentage
                        FROM collect.memory_stats
                        WHERE collection_time >= DATEADD(HOUR, @HoursBack, SYSDATETIME())
                        ORDER BY collection_time ASC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.Parameters.Add(new SqlParameter("@HoursBack", SqlDbType.Int) { Value = -hoursBack });
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new MemoryHistoryItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            BufferPoolMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1),
                            PlanCacheMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                            OtherMemoryMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                            TotalMemoryMb = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            PhysicalMemoryInUseMb = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                            AvailablePhysicalMemoryMb = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            MemoryUtilizationPercentage = reader.IsDBNull(7) ? 0 : reader.GetInt32(7)
                        });
                    }
        
                    return items;
                }

                public async Task<List<LatchStatsItem>> GetLatchStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<LatchStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ls.collection_time >= @fromDate AND ls.collection_time <= @toDate"
                        : "WHERE ls.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ls.collection_id,
            ls.collection_time,
            ls.server_start_time,
            ls.latch_class,
            ls.waiting_requests_count,
            ls.wait_time_ms,
            ls.max_wait_time_ms,
            ls.waiting_requests_count_delta,
            ls.wait_time_ms_delta,
            ls.max_wait_time_ms_delta,
            ls.sample_interval_seconds,
            severity =
                CASE
                    WHEN ISNULL(ls.wait_time_ms_delta, 0) > 10000 THEN N'HIGH'
                    WHEN ISNULL(ls.wait_time_ms_delta, 0) > 5000 THEN N'MEDIUM'
                    ELSE N'LOW'
                END,
            latch_description =
                CASE ls.latch_class
                    WHEN N'BUFFER' THEN N'Synchronize short term access to database pages.'
                    WHEN N'BUFFER_POOL_GROW' THEN N'Buffer pool grow operations.'
                    WHEN N'DATABASE_CHECKPOINT' THEN N'Serialize checkpoints within a database.'
                    WHEN N'FCB' THEN N'Synchronize access to the file control block.'
                    WHEN N'FGCB_ADD_REMOVE' THEN N'Synchronize file add/drop/grow/shrink operations.'
                    WHEN N'LOG_MANAGER' THEN N'Transaction log manager synchronization.'
                    ELSE N'Internal SQL Server synchronization.'
                END,
            recommendation =
                CASE
                    WHEN ls.latch_class LIKE N'PAGEIOLATCH%' THEN N'I/O bottleneck - check disk latency, add memory'
                    WHEN ls.latch_class LIKE N'PAGELATCH%' THEN N'Page contention - check for hot pages, tempdb issues'
                    WHEN ls.latch_class = N'BUFFER' THEN N'Buffer pool contention - check for memory pressure'
                    WHEN ls.latch_class LIKE N'ACCESS_METHODS%' THEN N'Index/heap access contention'
                    WHEN ls.latch_class LIKE N'ALLOC%' THEN N'Allocation contention - consider pre-sizing files'
                    WHEN ls.latch_class IN (N'LOG_MANAGER', N'LOGCACHE_ACCESS') THEN N'Log contention - check log disk'
                    ELSE N'Review latch class documentation'
                END
        FROM collect.latch_stats AS ls
        {dateFilter}
        ORDER BY
            ls.collection_time DESC,
            ls.wait_time_ms_delta DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new LatchStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            LatchClass = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            WaitingRequestsCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                            WaitTimeMs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                            MaxWaitTimeMs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            WaitingRequestsCountDelta = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                            WaitTimeMsDelta = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            MaxWaitTimeMsDelta = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            SampleIntervalSeconds = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                            Severity = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                            LatchDescription = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
                            Recommendation = reader.IsDBNull(13) ? string.Empty : reader.GetString(13)
                        });
                    }
        
                    return items;
                }

        /// <summary>
        /// Gets latch stats filtered to only the top N latch classes by total wait time delta.
        /// </summary>
        public async Task<List<LatchStatsItem>> GetLatchStatsTopNAsync(int topN = 5, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<LatchStatsItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "ls.collection_time >= @fromDate AND ls.collection_time <= @toDate"
                : "ls.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";

            string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH top_latches AS
        (
            SELECT TOP (@topN)
                ls.latch_class
            FROM collect.latch_stats AS ls
            WHERE {dateFilter}
            AND   ls.wait_time_ms_delta IS NOT NULL
            GROUP BY
                ls.latch_class
            ORDER BY
                SUM(ls.wait_time_ms_delta) DESC
        )
        SELECT
            ls.collection_id,
            ls.collection_time,
            ls.server_start_time,
            ls.latch_class,
            ls.waiting_requests_count,
            ls.wait_time_ms,
            ls.max_wait_time_ms,
            ls.waiting_requests_count_delta,
            ls.wait_time_ms_delta,
            ls.max_wait_time_ms_delta,
            ls.sample_interval_seconds,
            severity =
                CASE
                    WHEN ISNULL(ls.wait_time_ms_delta, 0) > 10000 THEN N'HIGH'
                    WHEN ISNULL(ls.wait_time_ms_delta, 0) > 5000 THEN N'MEDIUM'
                    ELSE N'LOW'
                END,
            latch_description =
                CASE ls.latch_class
                    WHEN N'BUFFER' THEN N'Synchronize short term access to database pages.'
                    WHEN N'BUFFER_POOL_GROW' THEN N'Buffer pool grow operations.'
                    WHEN N'DATABASE_CHECKPOINT' THEN N'Serialize checkpoints within a database.'
                    WHEN N'FCB' THEN N'Synchronize access to the file control block.'
                    WHEN N'FGCB_ADD_REMOVE' THEN N'Synchronize file add/drop/grow/shrink operations.'
                    WHEN N'LOG_MANAGER' THEN N'Transaction log manager synchronization.'
                    ELSE N'Internal SQL Server synchronization.'
                END,
            recommendation =
                CASE
                    WHEN ls.latch_class LIKE N'PAGEIOLATCH%' THEN N'I/O bottleneck - check disk latency, add memory'
                    WHEN ls.latch_class LIKE N'PAGELATCH%' THEN N'Page contention - check for hot pages, tempdb issues'
                    WHEN ls.latch_class = N'BUFFER' THEN N'Buffer pool contention - check for memory pressure'
                    WHEN ls.latch_class LIKE N'ACCESS_METHODS%' THEN N'Index/heap access contention'
                    WHEN ls.latch_class LIKE N'ALLOC%' THEN N'Allocation contention - consider pre-sizing files'
                    WHEN ls.latch_class IN (N'LOG_MANAGER', N'LOGCACHE_ACCESS') THEN N'Log contention - check log disk'
                    ELSE N'Review latch class documentation'
                END
        FROM collect.latch_stats AS ls
        WHERE {dateFilter}
        AND   ls.latch_class IN (SELECT tl.latch_class FROM top_latches AS tl)
        ORDER BY
            ls.collection_time DESC,
            ls.wait_time_ms_delta DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            command.Parameters.Add(new SqlParameter("@topN", SqlDbType.Int) { Value = topN });

            if (fromDate.HasValue && toDate.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
            }
            else
            {
                command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new LatchStatsItem
                {
                    CollectionId = reader.GetInt64(0),
                    CollectionTime = reader.GetDateTime(1),
                    ServerStartTime = reader.GetDateTime(2),
                    LatchClass = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                    WaitingRequestsCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    WaitTimeMs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                    MaxWaitTimeMs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    WaitingRequestsCountDelta = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                    WaitTimeMsDelta = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    MaxWaitTimeMsDelta = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                    SampleIntervalSeconds = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                    Severity = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                    LatchDescription = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
                    Recommendation = reader.IsDBNull(13) ? string.Empty : reader.GetString(13)
                });
            }

            return items;
        }

                public async Task<List<SpinlockStatsItem>> GetSpinlockStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<SpinlockStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ss.collection_time >= @fromDate AND ss.collection_time <= @toDate"
                        : "WHERE ss.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ss.collection_id,
            ss.collection_time,
            ss.server_start_time,
            ss.spinlock_name,
            ss.collisions,
            ss.spins,
            ss.spins_per_collision,
            ss.sleep_time,
            ss.backoffs,
            ss.collisions_delta,
            ss.spins_delta,
            ss.sleep_time_delta,
            ss.backoffs_delta,
            ss.sample_interval_seconds,
            spinlock_description =
                CASE ss.spinlock_name
                    WHEN N'BACKUP_CTX' THEN N'Page I/O during backup - high spins during checkpoint/lazywriter.'
                    WHEN N'DBTABLE' THEN N'In-memory data structure access for database properties.'
                    WHEN N'DP_LIST' THEN N'Dirty page list with indirect checkpoint enabled.'
                    WHEN N'LOCK_HASH' THEN N'Lock manager hash table access.'
                    WHEN N'LOCK_RW_SECURITY_CACHE' THEN N'Security token and access check cache.'
                    WHEN N'SOS_CACHESTORE' THEN N'Various in-memory caches (plan cache, temp tables).'
                    ELSE N'Internal use only.'
                END
        FROM collect.spinlock_stats AS ss
        {dateFilter}
        ORDER BY
            ss.collection_time DESC,
            ss.collisions_delta DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new SpinlockStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            SpinlockName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            Collisions = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                            Spins = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                            SpinsPerCollision = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            SleepTime = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            Backoffs = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                            CollisionsDelta = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            SpinsDelta = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            SleepTimeDelta = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            BackoffsDelta = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            SampleIntervalSeconds = reader.IsDBNull(13) ? null : reader.GetInt32(13),
                            SpinlockDescription = reader.IsDBNull(14) ? string.Empty : reader.GetString(14)
                        });
                    }
        
                    return items;
                }

        /// <summary>
        /// Gets spinlock stats filtered to only the top N spinlocks by total collisions delta.
        /// Reduces row count from ~8.5K to ~1.4K for chart display.
        /// </summary>
        public async Task<List<SpinlockStatsItem>> GetSpinlockStatsTopNAsync(int topN = 5, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<SpinlockStatsItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "ss.collection_time >= @fromDate AND ss.collection_time <= @toDate"
                : "ss.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";

            string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH top_spinlocks AS
        (
            SELECT TOP (@topN)
                ss.spinlock_name
            FROM collect.spinlock_stats AS ss
            WHERE {dateFilter}
            AND   ss.collisions_delta IS NOT NULL
            GROUP BY
                ss.spinlock_name
            ORDER BY
                SUM(ss.collisions_delta) DESC
        )
        SELECT
            ss.collection_id,
            ss.collection_time,
            ss.server_start_time,
            ss.spinlock_name,
            ss.collisions,
            ss.spins,
            ss.spins_per_collision,
            ss.sleep_time,
            ss.backoffs,
            ss.collisions_delta,
            ss.spins_delta,
            ss.sleep_time_delta,
            ss.backoffs_delta,
            ss.sample_interval_seconds,
            spinlock_description =
                CASE ss.spinlock_name
                    WHEN N'BACKUP_CTX' THEN N'Page I/O during backup - high spins during checkpoint/lazywriter.'
                    WHEN N'DBTABLE' THEN N'In-memory data structure access for database properties.'
                    WHEN N'DP_LIST' THEN N'Dirty page list with indirect checkpoint enabled.'
                    WHEN N'LOCK_HASH' THEN N'Lock manager hash table access.'
                    WHEN N'LOCK_RW_SECURITY_CACHE' THEN N'Security token and access check cache.'
                    WHEN N'SOS_CACHESTORE' THEN N'Various in-memory caches (plan cache, temp tables).'
                    ELSE N'Internal use only.'
                END
        FROM collect.spinlock_stats AS ss
        WHERE {dateFilter}
        AND   ss.spinlock_name IN (SELECT ts.spinlock_name FROM top_spinlocks AS ts)
        ORDER BY
            ss.collection_time DESC,
            ss.collisions_delta DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            command.Parameters.Add(new SqlParameter("@topN", SqlDbType.Int) { Value = topN });

            if (fromDate.HasValue && toDate.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
            }
            else
            {
                command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new SpinlockStatsItem
                {
                    CollectionId = reader.GetInt64(0),
                    CollectionTime = reader.GetDateTime(1),
                    ServerStartTime = reader.GetDateTime(2),
                    SpinlockName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                    Collisions = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    Spins = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                    SpinsPerCollision = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                    SleepTime = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    Backoffs = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                    CollisionsDelta = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                    SpinsDelta = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                    SleepTimeDelta = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                    BackoffsDelta = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                    SampleIntervalSeconds = reader.IsDBNull(13) ? null : reader.GetInt32(13),
                    SpinlockDescription = reader.IsDBNull(14) ? string.Empty : reader.GetString(14)
                });
            }

            return items;
        }

                public async Task<List<TempdbStatsItem>> GetTempdbStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TempdbStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ts.collection_time >= @fromDate AND ts.collection_time <= @toDate"
                        : "WHERE ts.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ts.collection_id,
            ts.collection_time,
            ts.user_object_reserved_page_count,
            ts.internal_object_reserved_page_count,
            ts.version_store_reserved_page_count,
            ts.mixed_extent_page_count,
            ts.unallocated_extent_page_count,
            ts.top_task_user_objects_mb,
            ts.top_task_internal_objects_mb,
            ts.top_task_total_mb,
            ts.top_task_session_id,
            ts.top_task_request_id,
            ts.total_sessions_using_tempdb,
            ts.sessions_with_user_objects,
            ts.sessions_with_internal_objects,
            ts.version_store_high_warning,
            ts.allocation_contention_warning,
            version_store_percent =
                CASE
                    WHEN (ts.user_object_reserved_page_count + ts.internal_object_reserved_page_count + ts.version_store_reserved_page_count) > 0
                    THEN CONVERT(decimal(5,2), ts.version_store_reserved_page_count * 100.0 /
                        (ts.user_object_reserved_page_count + ts.internal_object_reserved_page_count + ts.version_store_reserved_page_count))
                    ELSE 0
                END,
            pressure_level =
                CASE
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 5000 THEN N'CRITICAL - Version store > 5GB'
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 2000 THEN N'HIGH - Version store > 2GB'
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 1000 THEN N'MEDIUM - Version store > 1GB'
                    WHEN ts.unallocated_extent_page_count * 8 / 1024 < 100 THEN N'MEDIUM - Low free space'
                    ELSE N'NORMAL'
                END,
            recommendation =
                CASE
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 1000 THEN N'Check for long-running transactions, snapshot isolation'
                    WHEN ts.unallocated_extent_page_count * 8 / 1024 < 100 THEN N'Consider increasing TempDB file sizes'
                    WHEN ts.internal_object_reserved_page_count > ts.user_object_reserved_page_count * 2 THEN N'High internal usage - check sorts/hash ops'
                    ELSE N'TempDB usage is within normal range'
                END
        FROM collect.tempdb_stats AS ts
        {dateFilter}
        ORDER BY
            ts.collection_time DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TempdbStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            UserObjectReservedPageCount = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                            InternalObjectReservedPageCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                            VersionStoreReservedPageCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                            MixedExtentPageCount = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                            UnallocatedExtentPageCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            TopTaskUserObjectsMb = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            TopTaskInternalObjectsMb = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                            TopTaskTotalMb = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            TopTaskSessionId = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                            TopTaskRequestId = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                            TotalSessionsUsingTempdb = reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                            SessionsWithUserObjects = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                            SessionsWithInternalObjects = reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                            VersionStoreHighWarning = reader.IsDBNull(15) ? false : reader.GetBoolean(15),
                            AllocationContentionWarning = reader.IsDBNull(16) ? false : reader.GetBoolean(16),
                            VersionStorePercent = reader.IsDBNull(17) ? null : reader.GetDecimal(17),
                            PressureLevel = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
                            Recommendation = reader.IsDBNull(19) ? string.Empty : reader.GetString(19)
                        });
                    }
        
                    return items;
                }

                public async Task<List<CpuSpikeItem>> GetCpuSpikesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<CpuSpikeItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE cus.sample_time >= @fromDate AND cus.sample_time <= @toDate"
                        : "WHERE cus.sample_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            event_time = cus.sample_time,
            sql_server_cpu = cus.sqlserver_cpu_utilization,
            other_process_cpu = cus.other_process_cpu_utilization,
            total_cpu = cus.total_cpu_utilization,
            severity =
                CASE
                    WHEN cus.sqlserver_cpu_utilization >= 90
                    THEN N'CRITICAL'
                    WHEN cus.sqlserver_cpu_utilization >= 80
                    THEN N'HIGH'
                    WHEN cus.sqlserver_cpu_utilization >= 60
                    THEN N'MEDIUM'
                    ELSE N'LOW'
                END
        FROM collect.cpu_utilization_stats AS cus
        {dateFilter}
        ORDER BY
            cus.sample_time DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CpuSpikeItem
                        {
                            EventTime = reader.GetDateTime(0),
                            SqlServerCpu = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            OtherProcessCpu = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                            TotalCpu = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                            Severity = reader.IsDBNull(4) ? string.Empty : reader.GetString(4)
                        });
                    }
        
                    return items;
                }

                public async Task<List<FileIoLatencyTimeSeriesItem>> GetFileIoLatencyTimeSeriesAsync(bool isTempDb, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<FileIoLatencyTimeSeriesItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND fio.collection_time >= @fromDate AND fio.collection_time <= @toDate"
                        : "AND fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string dbFilter = isTempDb
                        ? "AND fio.database_name = N'tempdb'"
                        : "AND fio.database_name <> N'tempdb'";
        
                    // Get files that have had latency issues (outliers)
                    // Only include files with at least some I/O activity and meaningful latency
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            file_avg_latency AS
        (
            SELECT
                fio.database_name,
                fio.file_name,
                fio.file_type_desc,
                avg_read_latency_ms =
                    CASE
                        WHEN SUM(fio.num_of_reads_delta) > 0
                        THEN CONVERT(decimal(19,2), SUM(fio.io_stall_read_ms_delta) * 1.0 / SUM(fio.num_of_reads_delta))
                        ELSE 0
                    END,
                avg_write_latency_ms =
                    CASE
                        WHEN SUM(fio.num_of_writes_delta) > 0
                        THEN CONVERT(decimal(19,2), SUM(fio.io_stall_write_ms_delta) * 1.0 / SUM(fio.num_of_writes_delta))
                        ELSE 0
                    END,
                total_reads = SUM(ISNULL(fio.num_of_reads_delta, 0)),
                total_writes = SUM(ISNULL(fio.num_of_writes_delta, 0))
            FROM collect.file_io_stats AS fio
            WHERE fio.database_name IS NOT NULL
            {dbFilter}
            {dateFilter}
            GROUP BY
                fio.database_name,
                fio.file_name,
                fio.file_type_desc
            HAVING
                SUM(ISNULL(fio.num_of_reads_delta, 0)) + SUM(ISNULL(fio.num_of_writes_delta, 0)) > 0
        ),
            top_files AS
        (
            SELECT TOP (10)
                fal.database_name,
                fal.file_name,
                fal.file_type_desc
            FROM file_avg_latency AS fal
            ORDER BY
                fal.total_reads + fal.total_writes DESC
        )
        SELECT
            fio.collection_time,
            fio.database_name,
            fio.file_name,
            fio.file_type_desc,
            read_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_reads_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), fio.io_stall_read_ms_delta * 1.0 / fio.num_of_reads_delta)
                    ELSE 0
                END,
            write_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_writes_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), fio.io_stall_write_ms_delta * 1.0 / fio.num_of_writes_delta)
                    ELSE 0
                END,
            read_queued_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_reads_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), ISNULL(fio.io_stall_queued_read_ms_delta, 0) * 1.0 / fio.num_of_reads_delta)
                    ELSE 0
                END,
            write_queued_latency_ms =
                CASE
                    WHEN ISNULL(fio.num_of_writes_delta, 0) > 0
                    THEN CONVERT(decimal(19,2), ISNULL(fio.io_stall_queued_write_ms_delta, 0) * 1.0 / fio.num_of_writes_delta)
                    ELSE 0
                END,
            read_count = ISNULL(fio.num_of_reads_delta, 0),
            write_count = ISNULL(fio.num_of_writes_delta, 0)
        FROM collect.file_io_stats AS fio
        JOIN top_files AS tf
          ON  tf.database_name = fio.database_name
          AND tf.file_name = fio.file_name
        WHERE fio.database_name IS NOT NULL
        {dbFilter}
        {dateFilter}
        ORDER BY
            fio.collection_time,
            fio.database_name,
            fio.file_name;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoLatencyTimeSeriesItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            FileType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            ReadLatencyMs = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            WriteLatencyMs = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                            ReadQueuedLatencyMs = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            WriteQueuedLatencyMs = reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
                            ReadCount = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                            WriteCount = reader.IsDBNull(9) ? 0 : reader.GetInt64(9)
                        });
                    }
        
                    return items;
                }

                public async Task<List<FileIoLatencyTimeSeriesItem>> GetFileIoThroughputTimeSeriesAsync(bool isTempDb, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<FileIoLatencyTimeSeriesItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND fio.collection_time >= @fromDate AND fio.collection_time <= @toDate"
                        : "AND fio.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";

                    string dbFilter = isTempDb
                        ? "AND fio.database_name = N'tempdb'"
                        : "AND fio.database_name <> N'tempdb'";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            file_avg_throughput AS
        (
            SELECT
                fio.database_name,
                fio.file_name,
                fio.file_type_desc,
                total_bytes = SUM(ISNULL(fio.num_of_bytes_read_delta, 0)) + SUM(ISNULL(fio.num_of_bytes_written_delta, 0)),
                total_io = SUM(ISNULL(fio.num_of_reads_delta, 0)) + SUM(ISNULL(fio.num_of_writes_delta, 0))
            FROM collect.file_io_stats AS fio
            WHERE fio.database_name IS NOT NULL
            {dbFilter}
            {dateFilter}
            GROUP BY
                fio.database_name,
                fio.file_name,
                fio.file_type_desc
            HAVING
                SUM(ISNULL(fio.num_of_bytes_read_delta, 0)) + SUM(ISNULL(fio.num_of_bytes_written_delta, 0)) > 0
        ),
            top_files AS
        (
            SELECT TOP (10)
                fat.database_name,
                fat.file_name,
                fat.file_type_desc
            FROM file_avg_throughput AS fat
            ORDER BY
                fat.total_bytes DESC
        )
        SELECT
            fio.collection_time,
            fio.database_name,
            fio.file_name,
            fio.file_type_desc,
            read_throughput_mb_per_sec =
                CASE
                    WHEN ISNULL(fio.sample_ms_delta, 0) > 0
                    THEN CONVERT(decimal(19,4), fio.num_of_bytes_read_delta * 1000.0 / fio.sample_ms_delta / 1048576.0)
                    ELSE 0
                END,
            write_throughput_mb_per_sec =
                CASE
                    WHEN ISNULL(fio.sample_ms_delta, 0) > 0
                    THEN CONVERT(decimal(19,4), fio.num_of_bytes_written_delta * 1000.0 / fio.sample_ms_delta / 1048576.0)
                    ELSE 0
                END,
            read_count = ISNULL(fio.num_of_reads_delta, 0),
            write_count = ISNULL(fio.num_of_writes_delta, 0)
        FROM collect.file_io_stats AS fio
        JOIN top_files AS tf
          ON  tf.database_name = fio.database_name
          AND tf.file_name = fio.file_name
        WHERE fio.database_name IS NOT NULL
        {dbFilter}
        {dateFilter}
        ORDER BY
            fio.collection_time,
            fio.database_name,
            fio.file_name;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new FileIoLatencyTimeSeriesItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            FileName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            FileType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            ReadThroughputMbPerSec = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            WriteThroughputMbPerSec = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                            ReadCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            WriteCount = reader.IsDBNull(7) ? 0 : reader.GetInt64(7)
                        });
                    }

                    return items;
                }

                public async Task<List<CpuUtilizationItem>> GetCpuUtilizationAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<CpuUtilizationItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE cu.sample_time >= @fromDate AND cu.sample_time <= @toDate"
                        : "WHERE cu.sample_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            cu.collection_id,
            cu.collection_time,
            cu.sample_time,
            cu.sqlserver_cpu_utilization,
            cu.other_process_cpu_utilization,
            cu.total_cpu_utilization
        FROM collect.cpu_utilization_stats AS cu
        {dateFilter}
        ORDER BY
            cu.sample_time DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new CpuUtilizationItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            SampleTime = reader.GetDateTime(2),
                            SqlServerCpuUtilization = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                            OtherProcessCpuUtilization = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                            TotalCpuUtilization = reader.IsDBNull(5) ? 0 : reader.GetInt32(5)
                        });
                    }
        
                    return items;
                }

                public async Task<List<PerfmonStatsItem>> GetPerfmonStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<PerfmonStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ps.collection_time >= @fromDate AND ps.collection_time <= @toDate"
                        : "WHERE ps.collection_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ps.collection_id,
            ps.collection_time,
            ps.server_start_time,
            ps.object_name,
            ps.counter_name,
            ps.instance_name,
            ps.cntr_value,
            ps.cntr_type,
            ps.cntr_value_delta,
            ps.sample_interval_seconds,
            ps.cntr_value_per_second
        FROM collect.perfmon_stats AS ps
        {dateFilter}
        ORDER BY
            ps.collection_time DESC,
            ps.object_name,
            ps.counter_name;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    }
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new PerfmonStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            ObjectName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            CounterName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            InstanceName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            CntrValue = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            CntrType = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            CntrValueDelta = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            SampleIntervalSeconds = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            CntrValuePerSecond = reader.IsDBNull(10) ? null : reader.GetInt64(10)
                        });
                    }
        
                    return items;
                }

        /// <summary>
        /// Gets perfmon stats filtered to specific counter names.
        /// Used by Server Trends (4 counters) and Perfmon Counters tab (user-selected counters).
        /// </summary>
        public async Task<List<PerfmonStatsItem>> GetPerfmonStatsFilteredAsync(string[] counterNames, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<PerfmonStatsItem>();
            if (counterNames == null || counterNames.Length == 0)
                return items;

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "WHERE ps.collection_time >= @fromDate AND ps.collection_time <= @toDate"
                : "WHERE ps.collection_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";

            var counterParams = new List<string>();
            for (int i = 0; i < counterNames.Length; i++)
                counterParams.Add($"@counter{i}");

            string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ps.collection_id,
            ps.collection_time,
            ps.server_start_time,
            ps.object_name,
            ps.counter_name,
            ps.instance_name,
            ps.cntr_value,
            ps.cntr_type,
            ps.cntr_value_delta,
            ps.sample_interval_seconds,
            ps.cntr_value_per_second
        FROM collect.perfmon_stats AS ps
        {dateFilter}
        AND   ps.counter_name IN ({string.Join(", ", counterParams)})
        ORDER BY
            ps.collection_time DESC,
            ps.object_name,
            ps.counter_name;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            if (fromDate.HasValue && toDate.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
            }
            else
            {
                command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            }

            for (int i = 0; i < counterNames.Length; i++)
                command.Parameters.Add(new SqlParameter($"@counter{i}", SqlDbType.NVarChar, 256) { Value = counterNames[i] });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new PerfmonStatsItem
                {
                    CollectionId = reader.GetInt64(0),
                    CollectionTime = reader.GetDateTime(1),
                    ServerStartTime = reader.GetDateTime(2),
                    ObjectName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                    CounterName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                    InstanceName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                    CntrValue = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    CntrType = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    CntrValueDelta = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    SampleIntervalSeconds = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                    CntrValuePerSecond = reader.IsDBNull(10) ? null : reader.GetInt64(10)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets distinct perfmon counter names for the counter picker UI.
        /// Lightweight query that returns only unique (object_name, counter_name) pairs.
        /// </summary>
        public async Task<List<(string ObjectName, string CounterName)>> GetPerfmonCounterNamesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<(string ObjectName, string CounterName)>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "WHERE ps.collection_time >= @fromDate AND ps.collection_time <= @toDate"
                : "WHERE ps.collection_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";

            string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT DISTINCT
            ps.object_name,
            ps.counter_name
        FROM collect.perfmon_stats AS ps
        {dateFilter}
        ORDER BY
            ps.object_name,
            ps.counter_name;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;

            if (fromDate.HasValue && toDate.HasValue)
            {
                command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = fromDate.Value });
                command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = toDate.Value });
            }
            else
            {
                command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add((
                    reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1)
                ));
            }

            return items;
        }

        /// <summary>
        /// Gets all wait stats data (all wait types with activity) for the Wait Stats selector.
        /// Unlike GetWaitStatsDataAsync which limits to top N wait types, this returns all wait types
        /// so users can select any wait types they want to correlate.
        /// </summary>
        public async Task<List<WaitStatsDataPoint>> GetAllWaitStatsDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<WaitStatsDataPoint>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        interval_seconds =
            DATEDIFF
            (
                SECOND,
                LAG(ws.collection_time, 1, ws.collection_time) OVER
                (
                    PARTITION BY
                        ws.wait_type
                    ORDER BY
                        ws.collection_time
                ),
                ws.collection_time
            )
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= @from_date
    AND   ws.collection_time <= @to_date
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
            ELSE 0
        END,
    signal_wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
            ELSE 0
        END,
    avg_ms_per_wait =
        CASE
            WHEN wd.waiting_tasks_delta > 0
            THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta
            ELSE 0
        END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY
    wd.collection_time,
    wd.wait_type;";
            }
            else
            {
                query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        interval_seconds =
            DATEDIFF
            (
                SECOND,
                LAG(ws.collection_time, 1, ws.collection_time) OVER
                (
                    PARTITION BY
                        ws.wait_type
                    ORDER BY
                        ws.collection_time
                ),
                ws.collection_time
            )
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
            ELSE 0
        END,
    signal_wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
            ELSE 0
        END,
    avg_ms_per_wait =
        CASE
            WHEN wd.waiting_tasks_delta > 0
            THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta
            ELSE 0
        END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY
    wd.collection_time,
    wd.wait_type;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120; // Longer timeout since this can return more data
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new WaitStatsDataPoint
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                    SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                    AvgMsPerWait = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets wait stats data filtered to specific wait types.
        /// Used by the Wait Stats Detail picker after the user selects which types to display.
        /// </summary>
        public async Task<List<WaitStatsDataPoint>> GetWaitStatsDataForTypesAsync(string[] waitTypes, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<WaitStatsDataPoint>();
            if (waitTypes == null || waitTypes.Length == 0)
                return items;

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            var typeParams = new List<string>();
            for (int i = 0; i < waitTypes.Length; i++)
                typeParams.Add($"@wt{i}");

            string typeFilter = $"AND ws.wait_type IN ({string.Join(", ", typeParams)})";

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        interval_seconds =
            DATEDIFF(SECOND, LAG(ws.collection_time, 1, ws.collection_time) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ), ws.collection_time)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= @from_date
    AND   ws.collection_time <= @to_date
    {typeFilter}
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
        ELSE 0 END,
    signal_wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
        ELSE 0 END,
    avg_ms_per_wait =
        CASE WHEN wd.waiting_tasks_delta > 0
        THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta
        ELSE 0 END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY wd.collection_time, wd.wait_type;";
            }
            else
            {
                query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        interval_seconds =
            DATEDIFF(SECOND, LAG(ws.collection_time, 1, ws.collection_time) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ), ws.collection_time)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
    {typeFilter}
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
        ELSE 0 END,
    signal_wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds
        ELSE 0 END,
    avg_ms_per_wait =
        CASE WHEN wd.waiting_tasks_delta > 0
        THEN CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta
        ELSE 0 END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY wd.collection_time, wd.wait_type;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            for (int i = 0; i < waitTypes.Length; i++)
                command.Parameters.Add(new SqlParameter($"@wt{i}", SqlDbType.NVarChar, 256) { Value = waitTypes[i] });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new WaitStatsDataPoint
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                    SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                    AvgMsPerWait = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets distinct wait type names with total wait time for ranking in the picker UI.
        /// Lightweight alternative to GetAllWaitStatsDataAsync for populating the wait type selector.
        /// </summary>
        public async Task<List<(string WaitType, decimal TotalWaitTimeMsPerSecond)>> GetWaitTypeNamesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<(string WaitType, decimal TotalWaitTimeMsPerSecond)>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "ws.collection_time >= @from_date AND ws.collection_time <= @to_date"
                : "ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())";

            string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    ws.wait_type,
    total_wait_time_ms_per_second = SUM(ws.wait_time_ms - LAG_val)
FROM
(
    SELECT
        ws.wait_type,
        LAG_val = LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
        (
            PARTITION BY
                ws.wait_type
            ORDER BY
                ws.collection_time
        ),
        ws.wait_time_ms
    FROM collect.wait_stats AS ws
    WHERE {dateFilter}
) AS ws
WHERE ws.wait_time_ms - ws.LAG_val > 0
GROUP BY
    ws.wait_type
ORDER BY
    SUM(ws.wait_time_ms - ws.LAG_val) DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add((
                    reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                    reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture)
                ));
            }

            return items;
        }
    }
}
