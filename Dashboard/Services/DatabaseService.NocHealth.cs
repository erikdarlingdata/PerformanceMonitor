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
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        /// <summary>
        /// Fetches all NOC health metrics for the landing page.
        /// Returns a populated ServerHealthStatus object.
        /// </summary>
        public async Task<ServerHealthStatus> GetNocHealthStatusAsync(ServerConnection server, int engineEdition = 0)
        {
            var status = new ServerHealthStatus(server);

            try
            {
                await using var tc = await OpenThrottledConnectionAsync();
                var connection = tc.Connection;

                status.IsOnline = true;

                // Run all health queries in parallel for speed
                var cpuTask = GetCpuPercentAsync(connection, engineEdition);
                var memoryTask = GetMemoryStatusAsync(connection, status);
                var blockingTask = GetBlockingStatusAsync(connection, status);
                var threadsTask = GetThreadStatusAsync(connection, status);
                var deadlockTask = GetDeadlockCountAsync(connection);
                var collectorTask = GetCollectorStatusAsync(connection, status);
                var waitsTask = GetTopWaitAsync(connection, status);
                var lastBlockingTask = GetLastBlockingEventTimeAsync(connection, status);
                var lastDeadlockTask = GetLastDeadlockEventTimeAsync(connection, status);

                await Task.WhenAll(cpuTask, memoryTask, blockingTask, threadsTask, deadlockTask, collectorTask, waitsTask, lastBlockingTask, lastDeadlockTask);

                var cpuResult = await cpuTask;
                status.CpuPercent = cpuResult.SqlCpu;
                status.OtherCpuPercent = cpuResult.OtherCpu;
                status.DeadlockCount = await deadlockTask;

                status.LastUpdated = DateTime.Now;
                status.NotifyOverallSeverityChanged();
            }
            catch (Exception ex)
            {
                status.IsOnline = false;
                status.ErrorMessage = ex.Message;
                Logger.Warning($"Failed to get NOC health for {server.DisplayName}: {ex.Message}");
            }

            return status;
        }

        /// <summary>
        /// Updates an existing ServerHealthStatus with fresh data.
        /// </summary>
        public async Task RefreshNocHealthStatusAsync(ServerHealthStatus status, int engineEdition = 0)
        {
            status.IsLoading = true;
            Logger.Info($"RefreshNocHealthStatusAsync starting for {status.DisplayName}");

            try
            {
                await using var tc = await OpenThrottledConnectionAsync();
                var connection = tc.Connection;
                Logger.Info($"Connection opened for {status.DisplayName}");

                status.IsOnline = true;
                status.ErrorMessage = null;

                // Run all health queries in parallel
                var cpuTask = GetCpuPercentAsync(connection, engineEdition);
                var memoryTask = GetMemoryStatusAsync(connection, status);
                var blockingTask = GetBlockingStatusAsync(connection, status);
                var threadsTask = GetThreadStatusAsync(connection, status);
                var deadlockTask = GetDeadlockCountAsync(connection);
                var collectorTask = GetCollectorStatusAsync(connection, status);
                var waitsTask = GetTopWaitAsync(connection, status);
                var lastBlockingTask = GetLastBlockingEventTimeAsync(connection, status);
                var lastDeadlockTask = GetLastDeadlockEventTimeAsync(connection, status);

                await Task.WhenAll(cpuTask, memoryTask, blockingTask, threadsTask, deadlockTask, collectorTask, waitsTask, lastBlockingTask, lastDeadlockTask);
                Logger.Info($"All NOC queries completed for {status.DisplayName}");

                var cpuResult = await cpuTask;
                status.CpuPercent = cpuResult.SqlCpu;
                status.OtherCpuPercent = cpuResult.OtherCpu;
                status.DeadlockCount = await deadlockTask;

                Logger.Info($"NOC status for {status.DisplayName}: CPU={status.CpuPercent}%, Blocked={status.TotalBlocked}, LongestBlock={status.LongestBlockedSeconds}s");

                status.LastUpdated = DateTime.Now;
                status.NotifyOverallSeverityChanged();
            }
            catch (Exception ex)
            {
                status.IsOnline = false;
                status.ErrorMessage = ex.Message;
                Logger.Warning($"Failed to refresh NOC health for {status.DisplayName}: {ex.Message}");
            }
            finally
            {
                status.IsLoading = false;
            }
        }

        /// <summary>
        /// Lightweight alert-only health check. Runs 3 queries instead of 9.
        /// Used by MainWindow's independent alert timer.
        /// </summary>
        public async Task<AlertHealthResult> GetAlertHealthAsync(
            int engineEdition = 0,
            int longRunningQueryThresholdMinutes = 30,
            int longRunningJobMultiplier = 3,
            int longRunningQueryMaxResults = 5,
            bool excludeSpServerDiagnostics = true,
            bool excludeWaitFor = true,
            bool excludeBackups = true,
            bool excludeMiscWaits = true,
            bool excludeCdc = true,
            int failedJobLookbackMinutes = 60,
            IReadOnlyList<string>? excludedDatabases = null)
        {
            var result = new AlertHealthResult();

            try
            {
                await using var tc = await OpenThrottledConnectionAsync();
                var connection = tc.Connection;

                result.IsOnline = true;

                var cpuTask = GetCpuPercentAsync(connection, engineEdition);
                var blockingTask = GetBlockingValuesAsync(connection, excludedDatabases ?? Array.Empty<string>());
                var deadlockTask = GetDeadlockCountAsync(connection);
                var filteredDeadlockTask = excludedDatabases?.Count > 0
                    ? GetFilteredDeadlockCountAsync(connection, excludedDatabases)
                    : null;
                var poisonWaitTask = GetPoisonWaitDeltasAsync(connection);
                var longRunningTask = GetLongRunningQueriesAsync(connection, longRunningQueryThresholdMinutes, longRunningQueryMaxResults, excludeSpServerDiagnostics, excludeWaitFor, excludeBackups, excludeMiscWaits, excludeCdc);
                var tempDbTask = GetTempDbSpaceAsync(connection);
                var volumeTask = GetVolumeFreeSpaceAsync(connection);
                var anomalousJobTask = GetAnomalousJobsAsync(connection, longRunningJobMultiplier);
                /* Azure SQL DB has no SQL Agent, so msdb.dbo.sysjobhistory doesn't exist there —
                   skip the live failed-job query entirely (the other queries already gate Azure
                   the same way via engineEdition). */
                var failedJobTask = engineEdition == 5
                    ? Task.FromResult(new List<FailedJobInfo>())
                    : GetRecentlyFailedJobsAsync(connection, failedJobLookbackMinutes);
                var missingCaptureTask = GetMissingCaptureSessionsAsync(connection);

                var allTasks = filteredDeadlockTask != null
                    ? new Task[] { cpuTask, blockingTask, deadlockTask, filteredDeadlockTask, poisonWaitTask, longRunningTask, tempDbTask, volumeTask, anomalousJobTask, failedJobTask, missingCaptureTask }
                    : new Task[] { cpuTask, blockingTask, deadlockTask, poisonWaitTask, longRunningTask, tempDbTask, volumeTask, anomalousJobTask, failedJobTask, missingCaptureTask };
                await Task.WhenAll(allTasks);

                var cpuResult = await cpuTask;
                result.CpuPercent = cpuResult.SqlCpu;
                result.OtherCpuPercent = cpuResult.OtherCpu;

                var blockingResult = await blockingTask;
                result.TotalBlocked = blockingResult.TotalBlocked;
                result.LongestBlockedSeconds = blockingResult.LongestBlockedSeconds;

                result.DeadlockCount = await deadlockTask;
                if (filteredDeadlockTask != null)
                    result.FilteredDeadlockCount = await filteredDeadlockTask;
                result.PoisonWaits = await poisonWaitTask;
                result.LongRunningQueries = await longRunningTask;
                result.TempDbSpace = await tempDbTask;
                result.Volumes = await volumeTask;
                result.AnomalousJobs = await anomalousJobTask;
                result.RecentlyFailedJobs = await failedJobTask;
                result.MissingCaptureSessions = await missingCaptureTask;
            }
            catch (Exception ex)
            {
                result.IsOnline = false;
                Logger.Warning($"Failed to get alert health: {ex.Message}");
            }

            return result;
        }

        /// <summary>
        /// Returns capture types ("Blocking", "Deadlock") whose collector most recently
        /// logged SESSION_MISSING — the XE session is absent and couldn't be created,
        /// so capture is non-functional even though reads "succeed" with zero rows (#1086).
        /// </summary>
        private async Task<List<string>> GetMissingCaptureSessionsAsync(SqlConnection connection)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    x.collector_name
                FROM
                (
                    SELECT
                        cl.collector_name,
                        cl.collection_status,
                        n = ROW_NUMBER() OVER (PARTITION BY cl.collector_name ORDER BY cl.log_id DESC)
                    FROM config.collection_log AS cl
                    WHERE cl.collector_name IN (N'blocked_process_xml_collector', N'deadlock_xml_collector')
                ) AS x
                WHERE x.n = 1
                AND   x.collection_status = N'SESSION_MISSING'
                OPTION(RECOMPILE);";

            var missing = new List<string>();

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var collectorName = reader.GetString(0);
                    missing.Add(collectorName == "blocked_process_xml_collector" ? "Blocking" : "Deadlock");
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to check capture session status: {ex.Message}");
            }

            return missing;
        }

        /// <summary>
        /// Returns blocking values directly (without writing to a ServerHealthStatus).
        /// Used by GetAlertHealthAsync for lightweight alert checks.
        /// </summary>
        private async Task<(long TotalBlocked, decimal LongestBlockedSeconds)> GetBlockingValuesAsync(SqlConnection connection, IReadOnlyList<string> excludedDatabases)
        {
            var dbFilter = "";
            var dbParams = new List<string>();
            for (int i = 0; i < excludedDatabases.Count; i++)
                dbParams.Add($"@exdb{i}");
            if (dbParams.Count > 0)
                dbFilter = $"AND DB_NAME(s.dbid) NOT IN ({string.Join(", ", dbParams)})";

            var query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    total_blocked = COUNT_BIG(*),
                    longest_blocked_seconds = ISNULL(MAX(s.waittime), 0) / 1000.0
                FROM sys.sysprocesses AS s
                WHERE s.blocked <> 0
                AND   s.lastwaittype LIKE N'LCK%'
                {dbFilter}
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                for (int i = 0; i < excludedDatabases.Count; i++)
                    cmd.Parameters.AddWithValue($"@exdb{i}", excludedDatabases[i]);
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    var totalBlockedValue = reader.GetValue(0);
                    var longestSecondsValue = reader.GetValue(1);

                    var totalBlocked = Convert.ToInt64(totalBlockedValue, System.Globalization.CultureInfo.InvariantCulture);
                    var longestSeconds = Convert.ToDecimal(longestSecondsValue, System.Globalization.CultureInfo.InvariantCulture);

                    return (totalBlocked, longestSeconds);
                }
                return (0, 0);
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get blocking values: {ex.Message}");
                return (0, 0);
            }
        }

        private async Task<(int? SqlCpu, int? OtherCpu)> GetCpuPercentAsync(SqlConnection connection, int engineEdition = 0)
        {
            /* Azure SQL DB (edition 5) doesn't have dm_os_ring_buffers.
               Use sys.dm_db_resource_stats instead (reports avg_cpu_percent over 15-second intervals). */
            bool isAzureSqlDb = engineEdition == 5;

            string query = isAzureSqlDb
                ? @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (1)
                    sql_cpu_percent = CONVERT(integer, avg_cpu_percent),
                    other_cpu_percent = CONVERT(integer, 0)
                FROM sys.dm_db_resource_stats
                ORDER BY
                    end_time DESC
                OPTION(MAXDOP 1);"
                : @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                DECLARE @is_linux bit = 0;

                /* SystemIdle is always 0 in the SCHEDULER_MONITOR ring buffer on SQL Server
                   on Linux, so 100 - SystemIdle - ProcessUtilization fabricates a host figure
                   that pins total CPU at 100% forever (Issue #1048). No DMV exposes true host
                   CPU on Linux, so report other/host CPU as NULL there and let the alert engine
                   fall back to the SQL-only figure. sys.dm_os_host_info is 2017+; referenced via
                   sp_executesql so SQL 2016 (no Linux build) never binds it (@is_linux stays 0). */
                IF OBJECT_ID(N'sys.dm_os_host_info', N'V') IS NOT NULL
                BEGIN
                    EXEC sys.sp_executesql
                        N'SELECT @linux = CASE WHEN hi.host_platform = N''Linux'' THEN 1 ELSE 0 END FROM sys.dm_os_host_info AS hi;',
                        N'@linux bit OUTPUT',
                        @linux = @is_linux OUTPUT;
                END;

                SELECT TOP (1)
                    sql_cpu_percent =
                        x.rb.value
                        (
                            '(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]',
                            'integer'
                        ),
                    other_cpu_percent =
                        CASE
                            WHEN @is_linux = 1
                            THEN NULL
                            ELSE 100
                                 - x.rb.value
                                 (
                                     '(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]',
                                     'integer'
                                 )
                                 - x.rb.value
                                 (
                                     '(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]',
                                     'integer'
                                 )
                        END
                FROM
                (
                    SELECT
                        rb.timestamp,
                        rb = TRY_CAST(rb.record AS XML)
                    FROM sys.dm_os_ring_buffers AS rb
                    WHERE rb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                ) AS x
                ORDER BY
                    x.timestamp DESC
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    var sqlCpu = reader.IsDBNull(0) ? null : (int?)reader.GetInt32(0);
                    var otherCpu = reader.IsDBNull(1) ? null : (int?)reader.GetInt32(1);
                    return (sqlCpu, otherCpu);
                }
                return (null, null);
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get CPU percent: {ex.Message}");
                return (null, null);
            }
        }

        private async Task GetMemoryStatusAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    buffer_pool_gb =
                    (
                        SELECT
                            SUM(domc.pages_kb) / 1024.0 / 1024.0
                        FROM sys.dm_os_memory_clerks AS domc
                        WHERE domc.type = N'MEMORYCLERK_SQLBUFFERPOOL'
                        AND   domc.memory_node_id < 64
                    ),
                    total_granted_memory_gb =
                        SUM(deqrs.granted_memory_kb) / 1024.0 / 1024.0,
                    total_used_memory_gb =
                        SUM(deqrs.used_memory_kb) / 1024.0 / 1024.0,
                    requests_waiting_for_memory =
                        SUM(deqrs.waiter_count)
                FROM sys.dm_exec_query_resource_semaphores AS deqrs
                WHERE deqrs.max_target_memory_kb IS NOT NULL
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    status.BufferPoolGb = reader.IsDBNull(0) ? null : reader.GetDecimal(0);
                    status.GrantedMemoryGb = reader.IsDBNull(1) ? null : reader.GetDecimal(1);
                    status.UsedMemoryGb = reader.IsDBNull(2) ? null : reader.GetDecimal(2);
                    status.RequestsWaitingForMemory = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get memory status: {ex.Message}");
            }
        }

        private async Task GetBlockingStatusAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    total_blocked = COUNT_BIG(*),
                    longest_blocked_seconds = ISNULL(MAX(s.waittime), 0) / 1000.0
                FROM sys.sysprocesses AS s
                WHERE s.blocked <> 0
                AND   s.lastwaittype LIKE N'LCK%'
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    // Use GetValue + Convert for safety with varying SQL types
                    var totalBlockedValue = reader.GetValue(0);
                    var longestSecondsValue = reader.GetValue(1);
                    
                    var totalBlocked = Convert.ToInt64(totalBlockedValue, System.Globalization.CultureInfo.InvariantCulture);
                    var longestSeconds = Convert.ToDecimal(longestSecondsValue, System.Globalization.CultureInfo.InvariantCulture);
                    
                    status.TotalBlocked = totalBlocked;
                    status.LongestBlockedSeconds = longestSeconds;
                    
                    Logger.Info($"Blocking status: {totalBlocked} blocked, longest {longestSeconds}s");
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get blocking status: {ex.Message}");
            }
        }

        private async Task GetThreadStatusAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    total_threads =
                        MAX(osi.max_workers_count),
                    available_threads =
                        MAX(osi.max_workers_count) - SUM(dos.active_workers_count),
                    threads_waiting_for_cpu =
                        SUM(dos.runnable_tasks_count),
                    requests_waiting_for_threads =
                        SUM(dos.work_queue_count)
                FROM sys.dm_os_schedulers AS dos
                CROSS JOIN sys.dm_os_sys_info AS osi
                WHERE dos.status = N'VISIBLE ONLINE'
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    /*
                    Use Convert.ToInt32 to handle both int and bigint return types
                    (SQL Server SUM/MAX on int columns may return bigint on some versions)
                    */
                    status.TotalThreads = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                    status.AvailableThreads = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
                    status.ThreadsWaitingForCpu = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
                    status.RequestsWaitingForThreads = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3));
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get thread status: {ex.Message}");
            }
        }

        private async Task<long> GetDeadlockCountAsync(SqlConnection connection)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    deadlock_count = SUM(pc.cntr_value)
                FROM sys.dm_os_performance_counters AS pc
                WHERE pc.counter_name LIKE N'Number of Deadlocks/sec%'
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                var result = await cmd.ExecuteScalarAsync();
                return result is long l ? l : 0;
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get deadlock count: {ex.Message}");
                return 0;
            }
        }

        /// <summary>
        /// Counts recent deadlocks from collect.blocking_deadlock_stats, excluding the specified databases.
        /// Uses a 5-minute window matching the alert cooldown so each cooldown period
        /// reflects only deadlocks from non-excluded databases.
        /// This is the filtered equivalent of GetDeadlockCountAsync, which reads from
        /// sys.dm_os_performance_counters and cannot be filtered by database.
        /// </summary>
        private async Task<long?> GetFilteredDeadlockCountAsync(SqlConnection connection, IReadOnlyList<string> excludedDatabases)
        {
            var dbFilter = "";
            var dbParams = new List<string>();
            for (int i = 0; i < excludedDatabases.Count; i++)
                dbParams.Add($"@exdb{i}");
            if (dbParams.Count > 0)
                dbFilter = $"AND bds.database_name NOT IN ({string.Join(", ", dbParams)})";

            var query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    filtered_deadlock_count =
                        COALESCE(SUM(bds.deadlock_count_delta), 0)
                FROM collect.blocking_deadlock_stats AS bds
                /* collection_time is server-local (SYSDATETIME default); match that clock, not UTC,
                   or the window is hours off and the COALESCE(...,0) silently zeroes deadlock alerts. */
                WHERE bds.collection_time >= DATEADD(MINUTE, -5, SYSDATETIME())
                AND   bds.deadlock_count_delta IS NOT NULL
                {dbFilter}
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                for (int i = 0; i < excludedDatabases.Count; i++)
                    cmd.Parameters.AddWithValue($"@exdb{i}", excludedDatabases[i]);
                var result = await cmd.ExecuteScalarAsync();
                return result is long l ? l : (result is int i2 ? (long)i2 : 0);
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get filtered deadlock count: {ex.Message}");
                return null; // Fall back to raw delta
            }
        }

        private async Task GetCollectorStatusAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    healthy_collector_count =
                        SUM(CASE WHEN ch.health_status = N'HEALTHY' THEN 1 ELSE 0 END),
                    failed_collector_count =
                        SUM(CASE WHEN ch.health_status = N'FAILING' THEN 1 ELSE 0 END)
                FROM report.collection_health AS ch
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    status.HealthyCollectorCount = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                    status.FailedCollectorCount = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get collector status: {ex.Message}");
            }
        }

        private async Task GetTopWaitAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (1)
                    dowt.wait_type,
                    wait_duration_seconds =
                        SUM(dowt.wait_duration_ms) / 1000.0
                FROM sys.dm_os_waiting_tasks AS dowt
                WHERE dowt.session_id > 50
                AND   NOT EXISTS
                      (
                          SELECT
                              1/0
                          FROM config.ignored_wait_types AS iwt
                          WHERE iwt.wait_type = dowt.wait_type
                      )
                GROUP BY
                    dowt.wait_type
                ORDER BY
                    SUM(dowt.wait_duration_ms) DESC
                OPTION(MAXDOP 1, RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    status.TopWaitType = reader.IsDBNull(0) ? null : reader.GetString(0);
                    status.TopWaitDurationSeconds = reader.IsDBNull(1) ? 0 : reader.GetDecimal(1);
                }
                else
                {
                    status.TopWaitType = null;
                    status.TopWaitDurationSeconds = 0;
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get top wait: {ex.Message}");
            }
        }

        private async Task GetLastBlockingEventTimeAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (1)
                    minutes_ago =
                        DATEDIFF(MINUTE, bpx.event_time, SYSUTCDATETIME())
                FROM collect.blocked_process_xml AS bpx
                ORDER BY
                    bpx.id DESC
                OPTION(MAXDOP 1);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                var result = await cmd.ExecuteScalarAsync();
                status.LastBlockingMinutesAgo = result as int?;
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get last blocking event time: {ex.Message}");
            }
        }

        private async Task GetLastDeadlockEventTimeAsync(SqlConnection connection, ServerHealthStatus status)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (1)
                    minutes_ago =
                        DATEDIFF(MINUTE, dx.event_time, SYSUTCDATETIME())
                FROM collect.deadlock_xml AS dx
                ORDER BY
                    dx.id DESC
                OPTION(MAXDOP 1);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                var result = await cmd.ExecuteScalarAsync();
                status.LastDeadlockMinutesAgo = result as int?;
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get last deadlock event time: {ex.Message}");
            }
        }

        /// <summary>
        /// Gets recent poison wait deltas (THREADPOOL, RESOURCE_SEMAPHORE, RESOURCE_SEMAPHORE_QUERY_COMPILE)
        /// from collected wait stats. Returns entries where avg ms per wait exceeds zero.
        /// </summary>
        private async Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(SqlConnection connection)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (3)
                    wait_type,
                    wait_time_ms_delta,
                    waiting_tasks_count_delta,
                    avg_ms_per_wait =
                        CASE WHEN waiting_tasks_count_delta > 0
                        THEN CAST(CAST(wait_time_ms_delta AS decimal(19, 2)) / waiting_tasks_count_delta AS decimal(18, 4))
                        ELSE 0 END
                FROM collect.wait_stats
                WHERE wait_type IN (N'THREADPOOL', N'RESOURCE_SEMAPHORE', N'RESOURCE_SEMAPHORE_QUERY_COMPILE')
                AND waiting_tasks_count_delta > 0
                AND collection_time >= DATEADD(MINUTE, -10, SYSDATETIME())
                ORDER BY collection_time DESC
                OPTION(MAXDOP 1, RECOMPILE);";

            var results = new List<PoisonWaitDelta>();

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    results.Add(new PoisonWaitDelta
                    {
                        WaitType = reader.GetString(0),
                        DeltaMs = Convert.ToInt64(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture),
                        DeltaTasks = Convert.ToInt64(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture),
                        AvgMsPerWait = Convert.ToDouble(reader.GetValue(3), System.Globalization.CultureInfo.InvariantCulture)
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get poison wait deltas: {ex.Message}");
            }

            return results;
        }

        /// <summary>
        /// Gets currently running queries that exceed the duration threshold.
        /// Uses live DMV data (sys.dm_exec_requests) for immediate detection.
        /// </summary>
        private async Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
            SqlConnection connection,
            int thresholdMinutes,
            int maxResults = 5,
            bool excludeSpServerDiagnostics = true,
            bool excludeWaitFor = true,
            bool excludeBackups = true,
            bool excludeMiscWaits = true,
            bool excludeCdc = true)
        {
            maxResults = Math.Clamp(maxResults, 1, 1000);

            string spServerDiagnosticsFilter = excludeSpServerDiagnostics
                ? "AND r.wait_type NOT LIKE N'%SP_SERVER_DIAGNOSTICS%'" : "";
            string waitForFilter = excludeWaitFor
                ? "AND r.wait_type NOT IN (N'WAITFOR', N'BROKER_RECEIVE_WAITFOR')" : "";
            string backupsFilter = excludeBackups
                ? "AND r.wait_type NOT IN (N'BACKUPTHREAD', N'BACKUPIO')" : "";
            string miscWaitsFilter = excludeMiscWaits
                ? "AND r.wait_type NOT IN (N'XE_LIVE_TARGET_TVF')" : "";
            // CDC capture runs continuously as a SQL Agent job (EXEC sys.sp_MScdc_capture_job -> sys.sp_cdc_scan),
            // so it permanently exceeds the duration threshold and none of the wait_type filters above catch it.
            //
            // Primary signal: resolve the capture job_id(s) from msdb.dbo.cdc_jobs and match the running session via
            // its SQL Agent program_name ('SQLAgent - TSQL JobStep (Job 0x<job_id> : Step N)'). This is CDC-specific
            // and never hides unrelated Agent jobs. The msdb reference is deferred through sp_executesql inside
            // TRY/CATCH so a login without msdb access gets a *catchable* error (not an uncatchable cross-db 916) and
            // cleanly falls back to a text match on the whole batch/object text.
            string cdcSetup = excludeCdc ? @"
                DECLARE @cdc_capture_jobs TABLE (job_id uniqueidentifier PRIMARY KEY);
                DECLARE @cdc_readable bit = 0;
                BEGIN TRY
                    INSERT @cdc_capture_jobs (job_id)
                    EXEC sys.sp_executesql N'SELECT cj.job_id FROM msdb.dbo.cdc_jobs AS cj WHERE cj.job_type = N''capture'';';
                    SET @cdc_readable = 1;
                END TRY
                BEGIN CATCH
                    SET @cdc_readable = 0;
                END CATCH;
" : "";
            string cdcFilter = excludeCdc ? @"
                    AND NOT
                    (
                        (
                            @cdc_readable = 1
                            AND s.program_name LIKE N'SQLAgent - TSQL JobStep (Job 0x%'
                            AND TRY_CONVERT(uniqueidentifier, TRY_CONVERT(binary(16), SUBSTRING(s.program_name, 32, 32), 2))
                                IN (SELECT j.job_id FROM @cdc_capture_jobs AS j)
                        )
                        OR
                        (
                            @cdc_readable = 0
                            AND t.text IS NOT NULL
                            AND (t.text LIKE N'%sp_MScdc_capture_job%' OR t.text LIKE N'%sp_cdc_scan%')
                        )
                    )" : "";

            string query = @$"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                {cdcSetup}
                SELECT TOP(@maxResults)
                    r.session_id,
                    DB_NAME(r.database_id) AS database_name,
                    SUBSTRING(t.text, 1, 300) AS query_text,
                    s.program_name,
                    r.total_elapsed_time / 1000 AS elapsed_seconds,
                    r.cpu_time AS cpu_time_ms,
                    r.reads,
                    r.writes,
                    r.wait_type,
                    r.blocking_session_id,
                    CONVERT(varchar(18), r.query_hash, 1) AS query_hash
                FROM sys.dm_exec_requests AS r
                CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
                JOIN sys.dm_exec_sessions AS s ON s.session_id = r.session_id
                WHERE 
                    r.session_id > 50
                    AND r.total_elapsed_time >= @thresholdMs
                    {spServerDiagnosticsFilter}
                    {waitForFilter}
                    {backupsFilter}
                    {miscWaitsFilter}
                    {cdcFilter}
                ORDER BY r.total_elapsed_time DESC
                OPTION(MAXDOP 1, RECOMPILE);";

            var results = new List<LongRunningQueryInfo>();

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                cmd.Parameters.Add(new SqlParameter("@thresholdMs", SqlDbType.BigInt) { Value = (long)thresholdMinutes * 60 * 1000 });
                cmd.Parameters.Add(new SqlParameter("@maxResults", SqlDbType.Int) { Value = maxResults});
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    results.Add(new LongRunningQueryInfo
                    {
                        SessionId = Convert.ToInt32(reader.GetValue(0)),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                        ProgramName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                        ElapsedSeconds = Convert.ToInt64(reader.GetValue(4), System.Globalization.CultureInfo.InvariantCulture),
                        CpuTimeMs = Convert.ToInt64(reader.GetValue(5), System.Globalization.CultureInfo.InvariantCulture),
                        Reads = Convert.ToInt64(reader.GetValue(6), System.Globalization.CultureInfo.InvariantCulture),
                        Writes = Convert.ToInt64(reader.GetValue(7), System.Globalization.CultureInfo.InvariantCulture),
                        WaitType = reader.IsDBNull(8) ? null : reader.GetString(8),
                        BlockingSessionId = reader.IsDBNull(9) ? null : (int?)Convert.ToInt32(reader.GetValue(9), System.Globalization.CultureInfo.InvariantCulture),
                        QueryHash = reader.IsDBNull(10) ? null : reader.GetString(10)
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get long-running queries: {ex.Message}");
            }

            return results;
        }

        private async Task<List<AnomalousJobInfo>> GetAnomalousJobsAsync(SqlConnection connection, int multiplier)
        {
            var results = new List<AnomalousJobInfo>();
            var thresholdPercent = multiplier * 100;

            var query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (5)
                    job_name,
                    CAST(job_id AS VARCHAR(36)),
                    current_duration_seconds,
                    avg_duration_seconds,
                    p95_duration_seconds,
                    percent_of_average,
                    start_time
                FROM collect.running_jobs
                WHERE collection_time = (SELECT MAX(collection_time) FROM collect.running_jobs)
                AND avg_duration_seconds >= 60
                AND percent_of_average >= @thresholdPercent
                ORDER BY percent_of_average DESC
                OPTION(MAXDOP 1);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                cmd.Parameters.Add(new SqlParameter("@thresholdPercent", SqlDbType.Int) { Value = thresholdPercent });
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    results.Add(new AnomalousJobInfo
                    {
                        JobName = reader.GetString(0),
                        JobId = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        CurrentDurationSeconds = Convert.ToInt64(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture),
                        AvgDurationSeconds = Convert.ToInt64(reader.GetValue(3), System.Globalization.CultureInfo.InvariantCulture),
                        P95DurationSeconds = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4), System.Globalization.CultureInfo.InvariantCulture),
                        PercentOfAverage = reader.IsDBNull(5) ? null : Convert.ToDecimal(reader.GetValue(5), System.Globalization.CultureInfo.InvariantCulture),
                        StartTime = reader.GetDateTime(6)
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get anomalous jobs: {ex.Message}");
            }

            return results;
        }

        /// <summary>
        /// Live query against the monitored server's msdb for SQL Agent job runs (step_id = 0
        /// outcome row, run_status = 0 = Failed) that failed within the lookback window.
        /// run_date/run_time integers are converted to a server-local datetime and filtered to the
        /// last N minutes. Degrades gracefully: a login without msdb / SQLAgentReaderRole access
        /// raises a catchable SqlException (916/229/297/300) that returns an empty list rather than
        /// failing the alert cycle. Azure SQL DB (no Agent) is skipped by the caller.
        /// </summary>
        private async Task<List<FailedJobInfo>> GetRecentlyFailedJobsAsync(SqlConnection connection, int lookbackMinutes)
        {
            var results = new List<FailedJobInfo>();

            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (50)
                    job_name = j.name,
                    job_id = CONVERT(varchar(36), j.job_id),
                    run_datetime =
                        DATEADD
                        (
                            SECOND,
                            (jh.run_time / 10000) * 3600 +
                            ((jh.run_time / 100) % 100) * 60 +
                            (jh.run_time % 100),
                            CONVERT(datetime, CONVERT(varchar(8), jh.run_date))
                        ),
                    step_id = jh.step_id,
                    step_name = jh.step_name,
                    message = jh.message
                FROM msdb.dbo.sysjobhistory AS jh
                JOIN msdb.dbo.sysjobs AS j
                  ON j.job_id = jh.job_id
                WHERE jh.step_id = 0
                AND   jh.run_status = 0
                AND   jh.run_date >= CONVERT(integer, CONVERT(varchar(8), DATEADD(MINUTE, -@lookback_minutes, GETDATE()), 112))
                AND   DATEADD
                      (
                          SECOND,
                          (jh.run_time / 10000) * 3600 +
                          ((jh.run_time / 100) % 100) * 60 +
                          (jh.run_time % 100),
                          CONVERT(datetime, CONVERT(varchar(8), jh.run_date))
                      ) >= DATEADD(MINUTE, -@lookback_minutes, GETDATE())
                ORDER BY
                    run_datetime DESC
                OPTION(RECOMPILE);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                cmd.Parameters.Add(new SqlParameter("@lookback_minutes", SqlDbType.Int) { Value = lookbackMinutes });
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    results.Add(new FailedJobInfo
                    {
                        JobName = reader.GetString(0),
                        JobId = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        RunDateTime = reader.GetDateTime(2),
                        StepId = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3), System.Globalization.CultureInfo.InvariantCulture),
                        StepName = reader.IsDBNull(4) ? "" : reader.GetString(4),
                        Message = reader.IsDBNull(5) ? "" : reader.GetString(5)
                    });
                }
            }
            catch (Microsoft.Data.SqlClient.SqlException ex) when (ex.Number is 229 or 297 or 300 or 916)
            {
                /* Login lacks msdb / SQLAgentReaderRole access — expected for read-only monitoring
                   accounts; hit every alert cycle, so log at Info (not Warning) to avoid burying real warnings. */
                Logger.Info($"Skipping recently-failed-job check (msdb/SQLAgentReaderRole access needed): {ex.Message}");
            }
            catch (Exception ex)
            {
                /* Unexpected error (timeout, transient, etc.) — surface at Warning so a genuine read
                   failure can't masquerade as "no failed jobs". Still returns empty so the cycle continues. */
                Logger.Warning($"Recently-failed-job check errored: {ex.Message}");
            }

            return results;
        }

        private async Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(SqlConnection connection)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT TOP (1)
                    total_reserved_mb,
                    unallocated_mb,
                    user_object_reserved_mb,
                    internal_object_reserved_mb,
                    version_store_reserved_mb,
                    top_task_total_mb,
                    top_task_session_id
                FROM collect.tempdb_stats
                ORDER BY collection_time DESC
                OPTION(MAXDOP 1);";

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    return new TempDbSpaceInfo
                    {
                        TotalReservedMb = reader.IsDBNull(0) ? 0 : Convert.ToDouble(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture),
                        UnallocatedMb = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture),
                        UserObjectReservedMb = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture),
                        InternalObjectReservedMb = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3), System.Globalization.CultureInfo.InvariantCulture),
                        VersionStoreReservedMb = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4), System.Globalization.CultureInfo.InvariantCulture),
                        TopConsumerMb = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5), System.Globalization.CultureInfo.InvariantCulture),
                        TopConsumerSessionId = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6), System.Globalization.CultureInfo.InvariantCulture)
                    };
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get TempDB space: {ex.Message}");
            }

            return null;
        }

        /// <summary>
        /// Returns latest free space for each distinct volume (mount point), worst (lowest free %)
        /// first, for the low-disk alert. Files on the same volume collapse to one row. Volumes with
        /// no mount point (Azure SQL DB has no volume stats) are excluded, so Azure yields no rows.
        /// </summary>
        private async Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(SqlConnection connection)
        {
            const string query = @"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    volume_mount_point,
                    volume_total_mb =
                        MAX(volume_total_mb),
                    volume_free_mb =
                        MIN(volume_free_mb)
                FROM collect.database_size_stats
                WHERE collection_time =
                (
                    SELECT MAX(collection_time)
                    FROM collect.database_size_stats
                )
                AND   volume_mount_point IS NOT NULL
                AND   volume_total_mb > 0
                GROUP BY
                    volume_mount_point
                ORDER BY
                    MIN(volume_free_mb) / MAX(volume_total_mb)
                OPTION(MAXDOP 1);";

            var results = new List<VolumeFreeSpaceInfo>();

            try
            {
                using var cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 10;
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    results.Add(new VolumeFreeSpaceInfo
                    {
                        MountPoint = reader.IsDBNull(0) ? "" : reader.GetString(0),
                        TotalMb = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture),
                        FreeMb = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture)
                    });
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to get volume free space: {ex.Message}");
            }

            return results;
        }
    }
}
