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
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
                public async Task<List<HealthParserSystemHealthItem>> GetHealthParserSystemHealthAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserSystemHealthItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpsh.id,
            hpsh.collection_time,
            hpsh.event_time,
            hpsh.state,
            hpsh.spinlockBackoffs,
            hpsh.sickSpinlockType,
            hpsh.sickSpinlockTypeAfterAv,
            hpsh.latchWarnings,
            hpsh.isAccessViolationOccurred,
            hpsh.writeAccessViolationCount,
            hpsh.totalDumpRequests,
            hpsh.intervalDumpRequests,
            hpsh.nonYieldingTasksReported,
            hpsh.pageFaults,
            hpsh.systemCpuUtilization,
            hpsh.sqlCpuUtilization,
            hpsh.BadPagesDetected,
            hpsh.BadPagesFixed
        FROM collect.HealthParser_SystemHealth AS hpsh
        WHERE hpsh.collection_time >= @from_date AND hpsh.collection_time <= @to_date
        ORDER BY hpsh.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpsh.id,
            hpsh.collection_time,
            hpsh.event_time,
            hpsh.state,
            hpsh.spinlockBackoffs,
            hpsh.sickSpinlockType,
            hpsh.sickSpinlockTypeAfterAv,
            hpsh.latchWarnings,
            hpsh.isAccessViolationOccurred,
            hpsh.writeAccessViolationCount,
            hpsh.totalDumpRequests,
            hpsh.intervalDumpRequests,
            hpsh.nonYieldingTasksReported,
            hpsh.pageFaults,
            hpsh.systemCpuUtilization,
            hpsh.sqlCpuUtilization,
            hpsh.BadPagesDetected,
            hpsh.BadPagesFixed
        FROM collect.HealthParser_SystemHealth AS hpsh
        WHERE hpsh.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpsh.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserSystemHealthItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            State = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            SpinlockBackoffs = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            SickSpinlockType = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            SickSpinlockTypeAfterAv = reader.IsDBNull(6) ? string.Empty : reader.GetString(6),
                            LatchWarnings = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                            IsAccessViolationOccurred = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            WriteAccessViolationCount = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            TotalDumpRequests = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            IntervalDumpRequests = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            NonYieldingTasksReported = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            PageFaults = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            SystemCpuUtilization = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            SqlCpuUtilization = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            BadPagesDetected = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                            BadPagesFixed = reader.IsDBNull(17) ? null : reader.GetInt64(17)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserSevereErrorItem>> GetHealthParserSevereErrorsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserSevereErrorItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpse.id,
            hpse.collection_time,
            hpse.event_time,
            hpse.error_number,
            hpse.severity,
            hpse.state,
            hpse.message,
            hpse.database_name,
            hpse.database_id
        FROM collect.HealthParser_SevereErrors AS hpse
        WHERE hpse.collection_time >= @from_date AND hpse.collection_time <= @to_date
        ORDER BY hpse.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpse.id,
            hpse.collection_time,
            hpse.event_time,
            hpse.error_number,
            hpse.severity,
            hpse.state,
            hpse.message,
            hpse.database_name,
            hpse.database_id
        FROM collect.HealthParser_SevereErrors AS hpse
        WHERE hpse.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpse.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserSevereErrorItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            ErrorNumber = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            Severity = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            State = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                            Message = reader.IsDBNull(6) ? string.Empty : reader.GetString(6),
                            DatabaseName = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            DatabaseId = reader.IsDBNull(8) ? null : reader.GetInt32(8)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserIOIssueItem>> GetHealthParserIOIssuesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserIOIssueItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpio.id,
            hpio.collection_time,
            hpio.event_time,
            hpio.state,
            hpio.ioLatchTimeouts,
            hpio.intervalLongIos,
            hpio.totalLongIos,
            hpio.longestPendingRequests_duration_ms,
            hpio.longestPendingRequests_filePath
        FROM collect.HealthParser_IOIssues AS hpio
        WHERE hpio.collection_time >= @from_date AND hpio.collection_time <= @to_date
        ORDER BY hpio.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpio.id,
            hpio.collection_time,
            hpio.event_time,
            hpio.state,
            hpio.ioLatchTimeouts,
            hpio.intervalLongIos,
            hpio.totalLongIos,
            hpio.longestPendingRequests_duration_ms,
            hpio.longestPendingRequests_filePath
        FROM collect.HealthParser_IOIssues AS hpio
        WHERE hpio.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpio.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserIOIssueItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            State = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            IoLatchTimeouts = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            IntervalLongIos = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                            TotalLongIos = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                            LongestPendingRequestsDurationMs = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            LongestPendingRequestsFilePath = reader.IsDBNull(8) ? string.Empty : reader.GetString(8)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserSchedulerIssueItem>> GetHealthParserSchedulerIssuesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserSchedulerIssueItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpsi.id,
            hpsi.collection_time,
            hpsi.event_time,
            hpsi.scheduler_id,
            hpsi.cpu_id,
            hpsi.status,
            hpsi.is_online,
            hpsi.is_runnable,
            hpsi.is_running,
            hpsi.non_yielding_time_ms,
            hpsi.thread_quantum_ms
        FROM collect.HealthParser_SchedulerIssues AS hpsi
        WHERE hpsi.collection_time >= @from_date AND hpsi.collection_time <= @to_date
        ORDER BY hpsi.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpsi.id,
            hpsi.collection_time,
            hpsi.event_time,
            hpsi.scheduler_id,
            hpsi.cpu_id,
            hpsi.status,
            hpsi.is_online,
            hpsi.is_runnable,
            hpsi.is_running,
            hpsi.non_yielding_time_ms,
            hpsi.thread_quantum_ms
        FROM collect.HealthParser_SchedulerIssues AS hpsi
        WHERE hpsi.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpsi.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserSchedulerIssueItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            SchedulerId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            CpuId = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            Status = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            IsOnline = reader.IsDBNull(6) ? null : reader.GetBoolean(6),
                            IsRunnable = reader.IsDBNull(7) ? null : reader.GetBoolean(7),
                            IsRunning = reader.IsDBNull(8) ? null : reader.GetBoolean(8),
                            NonYieldingTimeMs = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
                            ThreadQuantumMs = reader.IsDBNull(10) ? string.Empty : reader.GetString(10)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserMemoryConditionItem>> GetHealthParserMemoryConditionsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserMemoryConditionItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpmc.id,
            hpmc.collection_time,
            hpmc.event_time,
            hpmc.lastNotification,
            hpmc.outOfMemoryExceptions,
            hpmc.isAnyPoolOutOfMemory,
            hpmc.processOutOfMemoryPeriod,
            hpmc.name,
            hpmc.available_physical_memory_gb,
            hpmc.available_virtual_memory_gb,
            hpmc.available_paging_file_gb,
            hpmc.working_set_gb,
            hpmc.percent_of_committed_memory_in_ws,
            hpmc.page_faults,
            hpmc.system_physical_memory_high,
            hpmc.system_physical_memory_low,
            hpmc.process_physical_memory_low,
            hpmc.process_virtual_memory_low,
            hpmc.vm_reserved_gb,
            hpmc.vm_committed_gb,
            hpmc.target_committed_gb,
            hpmc.current_committed_gb
        FROM collect.HealthParser_MemoryConditions AS hpmc
        WHERE hpmc.collection_time >= @from_date AND hpmc.collection_time <= @to_date
        ORDER BY hpmc.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpmc.id,
            hpmc.collection_time,
            hpmc.event_time,
            hpmc.lastNotification,
            hpmc.outOfMemoryExceptions,
            hpmc.isAnyPoolOutOfMemory,
            hpmc.processOutOfMemoryPeriod,
            hpmc.name,
            hpmc.available_physical_memory_gb,
            hpmc.available_virtual_memory_gb,
            hpmc.available_paging_file_gb,
            hpmc.working_set_gb,
            hpmc.percent_of_committed_memory_in_ws,
            hpmc.page_faults,
            hpmc.system_physical_memory_high,
            hpmc.system_physical_memory_low,
            hpmc.process_physical_memory_low,
            hpmc.process_virtual_memory_low,
            hpmc.vm_reserved_gb,
            hpmc.vm_committed_gb,
            hpmc.target_committed_gb,
            hpmc.current_committed_gb
        FROM collect.HealthParser_MemoryConditions AS hpmc
        WHERE hpmc.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpmc.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserMemoryConditionItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            LastNotification = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            OutOfMemoryExceptions = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            IsAnyPoolOutOfMemory = reader.IsDBNull(5) ? null : reader.GetBoolean(5),
                            ProcessOutOfMemoryPeriod = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                            Name = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            AvailablePhysicalMemoryGb = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            AvailableVirtualMemoryGb = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            AvailablePagingFileGb = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            WorkingSetGb = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            PercentOfCommittedMemoryInWs = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            PageFaults = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            SystemPhysicalMemoryHigh = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            SystemPhysicalMemoryLow = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            ProcessPhysicalMemoryLow = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                            ProcessVirtualMemoryLow = reader.IsDBNull(17) ? null : reader.GetInt64(17),
                            VmReservedGb = reader.IsDBNull(18) ? null : reader.GetInt64(18),
                            VmCommittedGb = reader.IsDBNull(19) ? null : reader.GetInt64(19),
                            TargetCommittedGb = reader.IsDBNull(20) ? null : reader.GetInt64(20),
                            CurrentCommittedGb = reader.IsDBNull(21) ? null : reader.GetInt64(21)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserCPUTasksItem>> GetHealthParserCPUTasksAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserCPUTasksItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpct.id,
            hpct.collection_time,
            hpct.event_time,
            hpct.state,
            hpct.maxWorkers,
            hpct.workersCreated,
            hpct.workersIdle,
            hpct.tasksCompletedWithinInterval,
            hpct.pendingTasks,
            hpct.oldestPendingTaskWaitingTime,
            hpct.hasUnresolvableDeadlockOccurred,
            hpct.hasDeadlockedSchedulersOccurred,
            hpct.didBlockingOccur
        FROM collect.HealthParser_CPUTasks AS hpct
        WHERE hpct.collection_time >= @from_date AND hpct.collection_time <= @to_date
        ORDER BY hpct.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpct.id,
            hpct.collection_time,
            hpct.event_time,
            hpct.state,
            hpct.maxWorkers,
            hpct.workersCreated,
            hpct.workersIdle,
            hpct.tasksCompletedWithinInterval,
            hpct.pendingTasks,
            hpct.oldestPendingTaskWaitingTime,
            hpct.hasUnresolvableDeadlockOccurred,
            hpct.hasDeadlockedSchedulersOccurred,
            hpct.didBlockingOccur
        FROM collect.HealthParser_CPUTasks AS hpct
        WHERE hpct.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpct.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserCPUTasksItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            State = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            MaxWorkers = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            WorkersCreated = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                            WorkersIdle = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                            TasksCompletedWithinInterval = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                            PendingTasks = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            OldestPendingTaskWaitingTime = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            HasUnresolvableDeadlockOccurred = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                            HasDeadlockedSchedulersOccurred = reader.IsDBNull(11) ? null : reader.GetBoolean(11),
                            DidBlockingOccur = reader.IsDBNull(12) ? null : reader.GetBoolean(12)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserMemoryBrokerItem>> GetHealthParserMemoryBrokerAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserMemoryBrokerItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpmb.id,
            hpmb.collection_time,
            hpmb.event_time,
            hpmb.broker_id,
            hpmb.pool_metadata_id,
            hpmb.delta_time,
            hpmb.memory_ratio,
            hpmb.new_target,
            hpmb.overall,
            hpmb.rate,
            hpmb.currently_predicated,
            hpmb.currently_allocated,
            hpmb.previously_allocated,
            hpmb.broker,
            hpmb.notification
        FROM collect.HealthParser_MemoryBroker AS hpmb
        WHERE hpmb.collection_time >= @from_date AND hpmb.collection_time <= @to_date
        ORDER BY hpmb.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpmb.id,
            hpmb.collection_time,
            hpmb.event_time,
            hpmb.broker_id,
            hpmb.pool_metadata_id,
            hpmb.delta_time,
            hpmb.memory_ratio,
            hpmb.new_target,
            hpmb.overall,
            hpmb.rate,
            hpmb.currently_predicated,
            hpmb.currently_allocated,
            hpmb.previously_allocated,
            hpmb.broker,
            hpmb.notification
        FROM collect.HealthParser_MemoryBroker AS hpmb
        WHERE hpmb.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpmb.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserMemoryBrokerItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            BrokerId = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                            PoolMetadataId = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            DeltaTime = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                            MemoryRatio = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                            NewTarget = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                            Overall = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            Rate = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            CurrentlyPredicated = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            CurrentlyAllocated = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            PreviouslyAllocated = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            Broker = reader.IsDBNull(13) ? string.Empty : reader.GetString(13),
                            Notification = reader.IsDBNull(14) ? string.Empty : reader.GetString(14)
                        });
                    }
        
                    return items;
                }

                public async Task<List<HealthParserMemoryNodeOOMItem>> GetHealthParserMemoryNodeOOMAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<HealthParserMemoryNodeOOMItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpmn.id,
            hpmn.collection_time,
            hpmn.event_time,
            hpmn.node_id,
            hpmn.memory_node_id,
            hpmn.memory_utilization_pct,
            hpmn.total_physical_memory_kb,
            hpmn.available_physical_memory_kb,
            hpmn.total_page_file_kb,
            hpmn.available_page_file_kb,
            hpmn.total_virtual_address_space_kb,
            hpmn.available_virtual_address_space_kb,
            hpmn.target_kb,
            hpmn.reserved_kb,
            hpmn.committed_kb,
            hpmn.shared_committed_kb,
            hpmn.awe_kb,
            hpmn.pages_kb,
            hpmn.failure_type,
            hpmn.failure_value,
            hpmn.resources,
            hpmn.factor_text,
            hpmn.factor_value,
            hpmn.last_error,
            hpmn.pool_metadata_id,
            hpmn.is_process_in_job,
            hpmn.is_system_physical_memory_high,
            hpmn.is_system_physical_memory_low,
            hpmn.is_process_physical_memory_low,
            hpmn.is_process_virtual_memory_low
        FROM collect.HealthParser_MemoryNodeOOM AS hpmn
        WHERE hpmn.collection_time >= @from_date AND hpmn.collection_time <= @to_date
        ORDER BY hpmn.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            hpmn.id,
            hpmn.collection_time,
            hpmn.event_time,
            hpmn.node_id,
            hpmn.memory_node_id,
            hpmn.memory_utilization_pct,
            hpmn.total_physical_memory_kb,
            hpmn.available_physical_memory_kb,
            hpmn.total_page_file_kb,
            hpmn.available_page_file_kb,
            hpmn.total_virtual_address_space_kb,
            hpmn.available_virtual_address_space_kb,
            hpmn.target_kb,
            hpmn.reserved_kb,
            hpmn.committed_kb,
            hpmn.shared_committed_kb,
            hpmn.awe_kb,
            hpmn.pages_kb,
            hpmn.failure_type,
            hpmn.failure_value,
            hpmn.resources,
            hpmn.factor_text,
            hpmn.factor_value,
            hpmn.last_error,
            hpmn.pool_metadata_id,
            hpmn.is_process_in_job,
            hpmn.is_system_physical_memory_high,
            hpmn.is_system_physical_memory_low,
            hpmn.is_process_physical_memory_low,
            hpmn.is_process_virtual_memory_low
        FROM collect.HealthParser_MemoryNodeOOM AS hpmn
        WHERE hpmn.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY hpmn.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new HealthParserMemoryNodeOOMItem
                        {
                            Id = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            NodeId = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                            MemoryNodeId = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            MemoryUtilizationPct = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                            TotalPhysicalMemoryKb = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                            AvailablePhysicalMemoryKb = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                            TotalPageFileKb = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            AvailablePageFileKb = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            TotalVirtualAddressSpaceKb = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            AvailableVirtualAddressSpaceKb = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            TargetKb = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            ReservedKb = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            CommittedKb = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            SharedCommittedKb = reader.IsDBNull(15) ? null : reader.GetDecimal(15),
                            AweKb = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                            PagesKb = reader.IsDBNull(17) ? null : reader.GetInt64(17),
                            FailureType = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
                            FailureValue = reader.IsDBNull(19) ? null : reader.GetInt32(19),
                            Resources = reader.IsDBNull(20) ? null : reader.GetInt32(20),
                            FactorText = reader.IsDBNull(21) ? string.Empty : reader.GetString(21),
                            FactorValue = reader.IsDBNull(22) ? null : reader.GetInt32(22),
                            LastError = reader.IsDBNull(23) ? null : reader.GetInt32(23),
                            PoolMetadataId = reader.IsDBNull(24) ? null : reader.GetInt32(24),
                            IsProcessInJob = reader.IsDBNull(25) ? string.Empty : reader.GetString(25),
                            IsSystemPhysicalMemoryHigh = reader.IsDBNull(26) ? string.Empty : reader.GetString(26),
                            IsSystemPhysicalMemoryLow = reader.IsDBNull(27) ? string.Empty : reader.GetString(27),
                            IsProcessPhysicalMemoryLow = reader.IsDBNull(28) ? string.Empty : reader.GetString(28),
                            IsProcessVirtualMemoryLow = reader.IsDBNull(29) ? string.Empty : reader.GetString(29)
                        });
                    }
        
                    return items;
                }
    }
}
