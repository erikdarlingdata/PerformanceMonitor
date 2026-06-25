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
                                /* total is NULL on SQL Server on Linux (host CPU not derivable, Issue #1048);
                                   degrade to the correct SQL-only figure so the chart never plots a total below SQL. */
                                total_cpu_utilization = ISNULL(total_cpu_utilization, sqlserver_cpu_utilization)
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
                                /* total is NULL on SQL Server on Linux (host CPU not derivable, Issue #1048);
                                   degrade to the correct SQL-only figure so the chart never plots a total below SQL. */
                                total_cpu_utilization = ISNULL(total_cpu_utilization, sqlserver_cpu_utilization)
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
                            /* total_cpu_utilization is NULL on SQL Server on Linux; fall back to the
                               SQL-only figure so the value isn't reported as 0 (#1048). */
                            total_cpu_utilization = ISNULL(total_cpu_utilization, sqlserver_cpu_utilization)
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
            /* total_cpu_utilization is NULL on SQL Server on Linux; fall back to SQL-only (#1048). */
            total_cpu = ISNULL(cus.total_cpu_utilization, cus.sqlserver_cpu_utilization),
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
            /* total is NULL on SQL Server on Linux (host CPU not derivable, Issue #1048);
               degrade to the correct SQL-only figure rather than reporting 0. */
            total_cpu_utilization = ISNULL(cu.total_cpu_utilization, cu.sqlserver_cpu_utilization)
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
    }
}
