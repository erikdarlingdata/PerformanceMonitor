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
                            OPTION(HASH GROUP);";
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
                            OPTION(HASH GROUP);";
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

                public async Task<List<BlockingEventItem>> GetBlockingEventsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<BlockingEventItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (100)
                                b.blocking_id,
                                b.collection_time,
                                b.blocked_process_report,
                                b.event_time,
                                b.database_name,
                                b.currentdbname,
                                b.contentious_object,
                                b.activity,
                                b.blocking_tree,
                                b.spid,
                                b.ecid,
                                CONVERT(nvarchar(max), b.query_text) AS query_text,
                                b.wait_time_ms,
                                b.status,
                                b.isolation_level,
                                b.lock_mode,
                                b.resource_owner_type,
                                b.transaction_count,
                                b.transaction_name,
                                b.last_transaction_started,
                                b.last_transaction_completed,
                                b.client_option_1,
                                b.client_option_2,
                                b.wait_resource,
                                b.priority,
                                b.log_used,
                                b.client_app,
                                b.host_name,
                                b.login_name,
                                b.transaction_id,
                                CONVERT(nvarchar(max), b.blocked_process_report_xml) AS blocked_process_report_xml
                            FROM collect.blocking_BlockedProcessReport AS b
                            WHERE b.collection_time >= @from_date
                            AND   b.collection_time <= @to_date
                            ORDER BY
                                b.event_time DESC,
                                CASE b.activity WHEN N'blocking' THEN 0 ELSE 1 END,
                                LEN(b.blocking_tree) - LEN(REPLACE(b.blocking_tree, N'>', N''));";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (100)
                                b.blocking_id,
                                b.collection_time,
                                b.blocked_process_report,
                                b.event_time,
                                b.database_name,
                                b.currentdbname,
                                b.contentious_object,
                                b.activity,
                                b.blocking_tree,
                                b.spid,
                                b.ecid,
                                CONVERT(nvarchar(max), b.query_text) AS query_text,
                                b.wait_time_ms,
                                b.status,
                                b.isolation_level,
                                b.lock_mode,
                                b.resource_owner_type,
                                b.transaction_count,
                                b.transaction_name,
                                b.last_transaction_started,
                                b.last_transaction_completed,
                                b.client_option_1,
                                b.client_option_2,
                                b.wait_resource,
                                b.priority,
                                b.log_used,
                                b.client_app,
                                b.host_name,
                                b.login_name,
                                b.transaction_id,
                                CONVERT(nvarchar(max), b.blocked_process_report_xml) AS blocked_process_report_xml
                            FROM collect.blocking_BlockedProcessReport AS b
                            WHERE b.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                b.event_time DESC,
                                CASE b.activity WHEN N'blocking' THEN 0 ELSE 1 END,
                                LEN(b.blocking_tree) - LEN(REPLACE(b.blocking_tree, N'>', N''));";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new BlockingEventItem
                        {
                            BlockingId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            BlockedProcessReport = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            EventTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3),
                            DatabaseName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            CurrentDbName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            ContentiousObject = reader.IsDBNull(6) ? string.Empty : reader.GetString(6),
                            Activity = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            BlockingTree = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                            Spid = reader.IsDBNull(9) ? (int?)null : reader.GetInt32(9),
                            Ecid = reader.IsDBNull(10) ? (int?)null : reader.GetInt32(10),
                            QueryText = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                            WaitTimeMs = reader.IsDBNull(12) ? (long?)null : reader.GetInt64(12),
                            Status = reader.IsDBNull(13) ? string.Empty : reader.GetString(13),
                            IsolationLevel = reader.IsDBNull(14) ? string.Empty : reader.GetString(14),
                            LockMode = reader.IsDBNull(15) ? string.Empty : reader.GetString(15),
                            ResourceOwnerType = reader.IsDBNull(16) ? string.Empty : reader.GetString(16),
                            TransactionCount = reader.IsDBNull(17) ? (int?)null : reader.GetInt32(17),
                            TransactionName = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
                            LastTransactionStarted = reader.IsDBNull(19) ? (DateTime?)null : reader.GetDateTime(19),
                            LastTransactionCompleted = reader.IsDBNull(20) ? (DateTime?)null : reader.GetDateTime(20),
                            ClientOption1 = reader.IsDBNull(21) ? string.Empty : reader.GetString(21),
                            ClientOption2 = reader.IsDBNull(22) ? string.Empty : reader.GetString(22),
                            WaitResource = reader.IsDBNull(23) ? string.Empty : reader.GetString(23),
                            Priority = reader.IsDBNull(24) ? (int?)null : reader.GetInt32(24),
                            LogUsed = reader.IsDBNull(25) ? (long?)null : reader.GetInt64(25),
                            ClientApp = reader.IsDBNull(26) ? string.Empty : reader.GetString(26),
                            HostName = reader.IsDBNull(27) ? string.Empty : reader.GetString(27),
                            LoginName = reader.IsDBNull(28) ? string.Empty : reader.GetString(28),
                            TransactionId = reader.IsDBNull(29) ? (long?)null : reader.GetInt64(29),
                            BlockedProcessReportXml = reader.IsDBNull(30) ? string.Empty : reader.GetString(30)
                        });
                    }
        
                    return items;
                }

                public async Task<List<DeadlockItem>> GetDeadlocksAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<DeadlockItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (100)
                                d.deadlock_id,
                                d.collection_time,
                                d.ServerName,
                                d.deadlock_type,
                                d.event_date,
                                d.database_name,
                                d.spid,
                                d.deadlock_group,
                                CONVERT(nvarchar(max), d.query) AS query,
                                CONVERT(nvarchar(max), d.object_names) AS object_names,
                                d.isolation_level,
                                d.owner_mode,
                                d.waiter_mode,
                                d.lock_mode,
                                d.transaction_count,
                                d.client_option_1,
                                d.client_option_2,
                                d.login_name,
                                d.host_name,
                                d.client_app,
                                d.wait_time,
                                d.wait_resource,
                                d.priority,
                                d.log_used,
                                d.last_tran_started,
                                d.last_batch_started,
                                d.last_batch_completed,
                                d.transaction_name,
                                d.status,
                                d.owner_waiter_type,
                                d.owner_activity,
                                d.owner_waiter_activity,
                                d.owner_merging,
                                d.owner_spilling,
                                d.owner_waiting_to_close,
                                d.waiter_waiter_type,
                                d.waiter_owner_activity,
                                d.waiter_waiter_activity,
                                d.waiter_merging,
                                d.waiter_spilling,
                                d.waiter_waiting_to_close,
                                CONVERT(nvarchar(max), d.deadlock_graph) AS deadlock_graph
                            FROM collect.deadlocks AS d
                            WHERE d.event_date >= @from_date
                            AND   d.event_date <= @to_date
                            ORDER BY
                                d.event_date DESC;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (100)
                                d.deadlock_id,
                                d.collection_time,
                                d.ServerName,
                                d.deadlock_type,
                                d.event_date,
                                d.database_name,
                                d.spid,
                                d.deadlock_group,
                                CONVERT(nvarchar(max), d.query) AS query,
                                CONVERT(nvarchar(max), d.object_names) AS object_names,
                                d.isolation_level,
                                d.owner_mode,
                                d.waiter_mode,
                                d.lock_mode,
                                d.transaction_count,
                                d.client_option_1,
                                d.client_option_2,
                                d.login_name,
                                d.host_name,
                                d.client_app,
                                d.wait_time,
                                d.wait_resource,
                                d.priority,
                                d.log_used,
                                d.last_tran_started,
                                d.last_batch_started,
                                d.last_batch_completed,
                                d.transaction_name,
                                d.status,
                                d.owner_waiter_type,
                                d.owner_activity,
                                d.owner_waiter_activity,
                                d.owner_merging,
                                d.owner_spilling,
                                d.owner_waiting_to_close,
                                d.waiter_waiter_type,
                                d.waiter_owner_activity,
                                d.waiter_waiter_activity,
                                d.waiter_merging,
                                d.waiter_spilling,
                                d.waiter_waiting_to_close,
                                CONVERT(nvarchar(max), d.deadlock_graph) AS deadlock_graph
                            FROM collect.deadlocks AS d
                            WHERE d.event_date >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                d.event_date DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new DeadlockItem
                        {
                            DeadlockId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            DeadlockType = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            EventDate = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                            DatabaseName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            Spid = reader.IsDBNull(6) ? (short?)null : reader.GetInt16(6),
                            DeadlockGroup = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                            Query = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
                            ObjectNames = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
                            IsolationLevel = reader.IsDBNull(10) ? string.Empty : reader.GetString(10),
                            OwnerMode = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                            WaiterMode = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
                            LockMode = reader.IsDBNull(13) ? string.Empty : reader.GetString(13),
                            TransactionCount = reader.IsDBNull(14) ? (long?)null : reader.GetInt64(14),
                            ClientOption1 = reader.IsDBNull(15) ? string.Empty : reader.GetString(15),
                            ClientOption2 = reader.IsDBNull(16) ? string.Empty : reader.GetString(16),
                            LoginName = reader.IsDBNull(17) ? string.Empty : reader.GetString(17),
                            HostName = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
                            ClientApp = reader.IsDBNull(19) ? string.Empty : reader.GetString(19),
                            WaitTime = reader.IsDBNull(20) ? (long?)null : reader.GetInt64(20),
                            WaitResource = reader.IsDBNull(21) ? string.Empty : reader.GetString(21),
                            Priority = reader.IsDBNull(22) ? (short?)null : reader.GetInt16(22),
                            LogUsed = reader.IsDBNull(23) ? (long?)null : reader.GetInt64(23),
                            LastTranStarted = reader.IsDBNull(24) ? (DateTime?)null : reader.GetDateTime(24),
                            LastBatchStarted = reader.IsDBNull(25) ? (DateTime?)null : reader.GetDateTime(25),
                            LastBatchCompleted = reader.IsDBNull(26) ? (DateTime?)null : reader.GetDateTime(26),
                            TransactionName = reader.IsDBNull(27) ? string.Empty : reader.GetString(27),
                            Status = reader.IsDBNull(28) ? string.Empty : reader.GetString(28),
                            OwnerWaiterType = reader.IsDBNull(29) ? string.Empty : reader.GetString(29),
                            OwnerActivity = reader.IsDBNull(30) ? string.Empty : reader.GetString(30),
                            OwnerWaiterActivity = reader.IsDBNull(31) ? string.Empty : reader.GetString(31),
                            OwnerMerging = reader.IsDBNull(32) ? string.Empty : reader.GetString(32),
                            OwnerSpilling = reader.IsDBNull(33) ? string.Empty : reader.GetString(33),
                            OwnerWaitingToClose = reader.IsDBNull(34) ? string.Empty : reader.GetString(34),
                            WaiterWaiterType = reader.IsDBNull(35) ? string.Empty : reader.GetString(35),
                            WaiterOwnerActivity = reader.IsDBNull(36) ? string.Empty : reader.GetString(36),
                            WaiterWaiterActivity = reader.IsDBNull(37) ? string.Empty : reader.GetString(37),
                            WaiterMerging = reader.IsDBNull(38) ? string.Empty : reader.GetString(38),
                            WaiterSpilling = reader.IsDBNull(39) ? string.Empty : reader.GetString(39),
                            WaiterWaitingToClose = reader.IsDBNull(40) ? string.Empty : reader.GetString(40),
                            DeadlockGraph = reader.IsDBNull(41) ? string.Empty : reader.GetString(41)
                        });
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

                public async Task<List<QuerySnapshotItem>> GetQuerySnapshotsAsync(int hoursBack = 1, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QuerySnapshotItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    // First check if the view exists
                    string checkViewQuery = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT 1 FROM sys.views
                        WHERE name = 'query_snapshots'
                        AND schema_id = SCHEMA_ID('report')";
        
                    using var checkCommand = new SqlCommand(checkViewQuery, connection);
                    var viewExists = await checkCommand.ExecuteScalarAsync();
        
                    if (viewExists == null)
                    {
                        // View doesn't exist yet - no data collected
                        return items;
                    }
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (500)
                                qs.collection_time,
                                qs.[dd hh:mm:ss.mss],
                                qs.session_id,
                                qs.status,
                                qs.wait_info,
                                qs.blocking_session_id,
                                qs.blocked_session_count,
                                qs.database_name,
                                qs.login_name,
                                qs.host_name,
                                qs.program_name,
                                sql_text = CONVERT(nvarchar(max), qs.sql_text),
                                sql_command = CONVERT(nvarchar(max), qs.sql_command),
                                qs.CPU,
                                qs.reads,
                                qs.writes,
                                qs.physical_reads,
                                qs.context_switches,
                                qs.used_memory,
                                qs.tempdb_current,
                                qs.tempdb_allocations,
                                qs.tran_log_writes,
                                qs.open_tran_count,
                                qs.percent_complete,
                                qs.start_time,
                                qs.tran_start_time,
                                qs.request_id,
                                additional_info = CONVERT(nvarchar(max), qs.additional_info)
                                /* query_plan fetched on-demand via GetQuerySnapshotPlanAsync */
                            FROM report.query_snapshots AS qs
                            WHERE qs.collection_time >= @from_date
                            AND   qs.collection_time <= @to_date
                            AND   CONVERT(nvarchar(max), qs.sql_text) NOT LIKE N'WAITFOR%'
                            ORDER BY
                                qs.collection_time DESC,
                                qs.session_id;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT TOP (500)
                                qs.collection_time,
                                qs.[dd hh:mm:ss.mss],
                                qs.session_id,
                                qs.status,
                                qs.wait_info,
                                qs.blocking_session_id,
                                qs.blocked_session_count,
                                qs.database_name,
                                qs.login_name,
                                qs.host_name,
                                qs.program_name,
                                sql_text = CONVERT(nvarchar(max), qs.sql_text),
                                sql_command = CONVERT(nvarchar(max), qs.sql_command),
                                qs.CPU,
                                qs.reads,
                                qs.writes,
                                qs.physical_reads,
                                qs.context_switches,
                                qs.used_memory,
                                qs.tempdb_current,
                                qs.tempdb_allocations,
                                qs.tran_log_writes,
                                qs.open_tran_count,
                                qs.percent_complete,
                                qs.start_time,
                                qs.tran_start_time,
                                qs.request_id,
                                additional_info = CONVERT(nvarchar(max), qs.additional_info)
                                /* query_plan fetched on-demand via GetQuerySnapshotPlanAsync */
                            FROM report.query_snapshots AS qs
                            WHERE qs.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            AND   CONVERT(nvarchar(max), qs.sql_text) NOT LIKE N'WAITFOR%'
                            ORDER BY
                                qs.collection_time DESC,
                                qs.session_id;";
                    }

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new QuerySnapshotItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            Duration = reader.IsDBNull(1) ? string.Empty : reader.GetValue(1)?.ToString() ?? string.Empty,
                            SessionId = SafeToInt16(reader.GetValue(2), "session_id") ?? 0,
                            Status = reader.IsDBNull(3) ? null : reader.GetValue(3)?.ToString(),
                            WaitInfo = reader.IsDBNull(4) ? null : reader.GetValue(4)?.ToString(),
                            BlockingSessionId = SafeToInt16(reader.GetValue(5), "blocking_session_id"),
                            BlockedSessionCount = SafeToInt16(reader.GetValue(6), "blocked_session_count"),
                            DatabaseName = reader.IsDBNull(7) ? null : reader.GetValue(7)?.ToString(),
                            LoginName = reader.IsDBNull(8) ? null : reader.GetValue(8)?.ToString(),
                            HostName = reader.IsDBNull(9) ? null : reader.GetValue(9)?.ToString(),
                            ProgramName = reader.IsDBNull(10) ? null : reader.GetValue(10)?.ToString(),
                            SqlText = reader.IsDBNull(11) ? null : reader.GetValue(11)?.ToString(),
                            SqlCommand = reader.IsDBNull(12) ? null : reader.GetValue(12)?.ToString(),
                            Cpu = SafeToInt64(reader.GetValue(13), "CPU"),
                            Reads = SafeToInt64(reader.GetValue(14), "reads"),
                            Writes = SafeToInt64(reader.GetValue(15), "writes"),
                            PhysicalReads = SafeToInt64(reader.GetValue(16), "physical_reads"),
                            ContextSwitches = SafeToInt64(reader.GetValue(17), "context_switches"),
                            UsedMemoryMb = SafeToDecimal(reader.GetValue(18), "used_memory"),
                            TempdbCurrentMb = SafeToDecimal(reader.GetValue(19), "tempdb_current"),
                            TempdbAllocations = SafeToDecimal(reader.GetValue(20), "tempdb_allocations"),
                            TranLogWrites = reader.IsDBNull(21) ? null : reader.GetValue(21)?.ToString(),
                            OpenTranCount = SafeToInt16(reader.GetValue(22), "open_tran_count"),
                            PercentComplete = SafeToDecimal(reader.GetValue(23), "percent_complete"),
                            StartTime = reader.IsDBNull(24) ? null : reader.GetDateTime(24),
                            TranStartTime = reader.IsDBNull(25) ? null : reader.GetDateTime(25),
                            RequestId = SafeToInt16(reader.GetValue(26), "request_id"),
                            AdditionalInfo = reader.IsDBNull(27) ? null : reader.GetValue(27)?.ToString()
                            // QueryPlan fetched on-demand via GetQuerySnapshotPlanAsync
                        });

                    }
        
                    return items;
                }

                /// <summary>
                /// Fetches the query plan for a specific query snapshot on-demand.
                /// </summary>
                public async Task<string?> GetQuerySnapshotPlanAsync(DateTime collectionTime, short sessionId)
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT TOP (1)
            query_plan = CONVERT(nvarchar(max), qs.query_plan)
        FROM report.query_snapshots AS qs
        WHERE qs.collection_time = @collectionTime
        AND   qs.session_id = @sessionId;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@collectionTime", SqlDbType.DateTime2) { Value = collectionTime });
                    command.Parameters.Add(new SqlParameter("@sessionId", SqlDbType.SmallInt) { Value = sessionId });

                    var result = await command.ExecuteScalarAsync();
                    return result == DBNull.Value ? null : result as string;
                }

                public async Task<List<QueryStatsItem>> GetQueryStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QueryStatsItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    bool useCustomDates = fromDate.HasValue && toDate.HasValue;

                    // Aggregate inline from collect.query_stats with time filter applied
                    // BEFORE the GROUP BY so counts/averages reflect only the selected time range.
                    // Uses a CTE to first get MAX per plan lifetime (creation_time), then SUM across
                    // lifetimes. This handles plan eviction correctly — when a plan is evicted and
                    // re-cached, the cumulative counter resets, so MAX alone undercounts.
                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH per_lifetime AS
        (
            SELECT
                database_name = qs.database_name,
                query_hash = qs.query_hash,
                object_type = MAX(qs.object_type),
                schema_name = MAX(qs.schema_name),
                object_name = MAX(qs.object_name),
                first_execution_time = MIN(qs.creation_time),
                last_execution_time = MAX(qs.last_execution_time),
                execution_count = MAX(qs.execution_count),
                total_worker_time = MAX(qs.total_worker_time),
                min_worker_time = MIN(qs.min_worker_time),
                max_worker_time = MAX(qs.max_worker_time),
                total_elapsed_time = MAX(qs.total_elapsed_time),
                min_elapsed_time = MIN(qs.min_elapsed_time),
                max_elapsed_time = MAX(qs.max_elapsed_time),
                total_logical_reads = MAX(qs.total_logical_reads),
                total_logical_writes = MAX(qs.total_logical_writes),
                total_physical_reads = MAX(qs.total_physical_reads),
                min_physical_reads = MIN(qs.min_physical_reads),
                max_physical_reads = MAX(qs.max_physical_reads),
                total_rows = MAX(qs.total_rows),
                min_rows = MIN(qs.min_rows),
                max_rows = MAX(qs.max_rows),
                min_dop = MIN(qs.min_dop),
                max_dop = MAX(qs.max_dop),
                min_grant_kb = MIN(qs.min_grant_kb),
                max_grant_kb = MAX(qs.max_grant_kb),
                total_spills = MAX(qs.total_spills),
                min_spills = MIN(qs.min_spills),
                max_spills = MAX(qs.max_spills),
                query_text = MAX(qs.query_text),
                query_plan_text = MAX(qs.query_plan_text),
                query_plan_hash = MAX(qs.query_plan_hash),
                sql_handle = MAX(qs.sql_handle),
                plan_handle = MAX(qs.plan_handle)
            FROM collect.query_stats AS qs
            WHERE (
                (@useCustomDates = 0 AND qs.last_execution_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME()))
                OR
                (@useCustomDates = 1 AND
                    ((qs.creation_time >= @fromDate AND qs.creation_time <= @toDate)
                    OR (qs.last_execution_time >= @fromDate AND qs.last_execution_time <= @toDate)
                    OR (qs.creation_time <= @fromDate AND qs.last_execution_time >= @toDate)))
            )
            AND qs.query_text NOT LIKE N'WAITFOR%'
            GROUP BY
                qs.database_name,
                qs.query_hash,
                qs.creation_time
        )
        SELECT
            database_name = pl.database_name,
            query_hash = CONVERT(nvarchar(20), pl.query_hash, 1),
            object_type = MAX(pl.object_type),
            object_name =
                CASE MAX(pl.object_type)
                    WHEN 'STATEMENT'
                    THEN N'Adhoc'
                    ELSE QUOTENAME(MAX(pl.schema_name)) + N'.' + QUOTENAME(MAX(pl.object_name))
                END,
            first_execution_time = MIN(pl.first_execution_time),
            last_execution_time = MAX(pl.last_execution_time),
            execution_count = SUM(pl.execution_count),
            total_worker_time = SUM(pl.total_worker_time),
            avg_worker_time_ms = SUM(pl.total_worker_time) / 1000.0 / NULLIF(SUM(pl.execution_count), 0),
            min_worker_time_ms = MIN(pl.min_worker_time) / 1000.0,
            max_worker_time_ms = MAX(pl.max_worker_time) / 1000.0,
            total_elapsed_time = SUM(pl.total_elapsed_time),
            avg_elapsed_time_ms = SUM(pl.total_elapsed_time) / 1000.0 / NULLIF(SUM(pl.execution_count), 0),
            min_elapsed_time_ms = MIN(pl.min_elapsed_time) / 1000.0,
            max_elapsed_time_ms = MAX(pl.max_elapsed_time) / 1000.0,
            total_logical_reads = SUM(pl.total_logical_reads),
            avg_logical_reads = SUM(pl.total_logical_reads) / NULLIF(SUM(pl.execution_count), 0),
            total_logical_writes = SUM(pl.total_logical_writes),
            avg_logical_writes = SUM(pl.total_logical_writes) / NULLIF(SUM(pl.execution_count), 0),
            total_physical_reads = SUM(pl.total_physical_reads),
            avg_physical_reads = SUM(pl.total_physical_reads) / NULLIF(SUM(pl.execution_count), 0),
            total_rows = SUM(pl.total_rows),
            avg_rows = SUM(pl.total_rows) / NULLIF(SUM(pl.execution_count), 0),
            min_rows = MIN(pl.min_rows),
            max_rows = MAX(pl.max_rows),
            min_dop = MIN(pl.min_dop),
            max_dop = MAX(pl.max_dop),
            min_grant_kb = MIN(pl.min_grant_kb),
            max_grant_kb = MAX(pl.max_grant_kb),
            total_spills = SUM(pl.total_spills),
            min_spills = MIN(pl.min_spills),
            max_spills = MAX(pl.max_spills),
            query_text = CONVERT(nvarchar(max), MAX(pl.query_text)),
            query_plan_xml = MAX(pl.query_plan_text),
            query_plan_hash = CONVERT(nvarchar(20), MAX(pl.query_plan_hash), 1),
            sql_handle = CONVERT(nvarchar(130), MAX(pl.sql_handle), 1),
            plan_handle = CONVERT(nvarchar(130), MAX(pl.plan_handle), 1)
        FROM per_lifetime AS pl
        GROUP BY
            pl.database_name,
            pl.query_hash
        ORDER BY
            avg_worker_time_ms DESC
        OPTION
        (
            HASH GROUP,
            HASH JOIN,
            USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')
        );";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@useCustomDates", SqlDbType.Bit) { Value = useCustomDates });
                    command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = (object?)fromDate ?? DBNull.Value });
                    command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = (object?)toDate ?? DBNull.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new QueryStatsItem
                        {
                            DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                            QueryHash = reader.IsDBNull(1) ? null : reader.GetString(1),
                            ObjectType = reader.IsDBNull(2) ? "" : reader.GetString(2),
                            ObjectName = reader.IsDBNull(3) ? null : reader.GetString(3),
                            FirstExecutionTime = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                            LastExecutionTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            ExecutionCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            TotalWorkerTime = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            AvgWorkerTimeMs = reader.IsDBNull(8) ? null : Convert.ToDouble(reader.GetValue(8), CultureInfo.InvariantCulture),
                            MinWorkerTimeMs = reader.IsDBNull(9) ? null : Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture),
                            MaxWorkerTimeMs = reader.IsDBNull(10) ? null : Convert.ToDouble(reader.GetValue(10), CultureInfo.InvariantCulture),
                            TotalElapsedTime = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                            AvgElapsedTimeMs = reader.IsDBNull(12) ? null : Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture),
                            MinElapsedTimeMs = reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture),
                            MaxElapsedTimeMs = reader.IsDBNull(14) ? null : Convert.ToDouble(reader.GetValue(14), CultureInfo.InvariantCulture),
                            TotalLogicalReads = reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                            AvgLogicalReads = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                            TotalLogicalWrites = reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                            AvgLogicalWrites = reader.IsDBNull(18) ? null : reader.GetInt64(18),
                            TotalPhysicalReads = reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
                            AvgPhysicalReads = reader.IsDBNull(20) ? null : reader.GetInt64(20),
                            TotalRows = reader.IsDBNull(21) ? 0 : reader.GetInt64(21),
                            AvgRows = reader.IsDBNull(22) ? null : reader.GetInt64(22),
                            MinRows = reader.IsDBNull(23) ? null : reader.GetInt64(23),
                            MaxRows = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                            MinDop = reader.IsDBNull(25) ? null : reader.GetInt16(25),
                            MaxDop = reader.IsDBNull(26) ? null : reader.GetInt16(26),
                            MinGrantKb = reader.IsDBNull(27) ? null : reader.GetInt64(27),
                            MaxGrantKb = reader.IsDBNull(28) ? null : reader.GetInt64(28),
                            TotalSpills = reader.IsDBNull(29) ? 0 : reader.GetInt64(29),
                            MinSpills = reader.IsDBNull(30) ? null : reader.GetInt64(30),
                            MaxSpills = reader.IsDBNull(31) ? null : reader.GetInt64(31),
                            QueryText = reader.IsDBNull(32) ? null : reader.GetString(32),
                            QueryPlanXml = reader.IsDBNull(33) ? null : reader.GetString(33),
                            QueryPlanHash = reader.IsDBNull(34) ? null : reader.GetString(34),
                            SqlHandle = reader.IsDBNull(35) ? null : reader.GetString(35),
                            PlanHandle = reader.IsDBNull(36) ? null : reader.GetString(36)
                        });
                    }

                    return items;
                }

                public async Task<List<ProcedureStatsItem>> GetProcedureStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<ProcedureStatsItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    bool useCustomDates = fromDate.HasValue && toDate.HasValue;

                    // Aggregate inline from collect.procedure_stats with time filter applied
                    // BEFORE the GROUP BY so counts/averages reflect only the selected time range.
                    // Uses a CTE to first get MAX per plan lifetime (cached_time), then SUM across
                    // lifetimes. This handles plan eviction correctly — when a plan is evicted and
                    // re-cached, the cumulative counter resets, so MAX alone undercounts.
                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH per_lifetime AS
        (
            SELECT
                database_name = ps.database_name,
                schema_name = ps.schema_name,
                object_name = ps.object_name,
                object_id = MAX(ps.object_id),
                object_type = MAX(ps.object_type),
                type_desc = MAX(ps.type_desc),
                first_cached_time = MIN(ps.cached_time),
                last_execution_time = MAX(ps.last_execution_time),
                execution_count = MAX(ps.execution_count),
                total_worker_time = MAX(ps.total_worker_time),
                min_worker_time = MIN(ps.min_worker_time),
                max_worker_time = MAX(ps.max_worker_time),
                total_elapsed_time = MAX(ps.total_elapsed_time),
                min_elapsed_time = MIN(ps.min_elapsed_time),
                max_elapsed_time = MAX(ps.max_elapsed_time),
                total_logical_reads = MAX(ps.total_logical_reads),
                min_logical_reads = MIN(ps.min_logical_reads),
                max_logical_reads = MAX(ps.max_logical_reads),
                total_logical_writes = MAX(ps.total_logical_writes),
                min_logical_writes = MIN(ps.min_logical_writes),
                max_logical_writes = MAX(ps.max_logical_writes),
                total_physical_reads = MAX(ps.total_physical_reads),
                min_physical_reads = MIN(ps.min_physical_reads),
                max_physical_reads = MAX(ps.max_physical_reads),
                total_spills = MAX(ps.total_spills),
                min_spills = MIN(ps.min_spills),
                max_spills = MAX(ps.max_spills),
                query_plan_text = MAX(ps.query_plan_text),
                sql_handle = MAX(ps.sql_handle),
                plan_handle = MAX(ps.plan_handle)
            FROM collect.procedure_stats AS ps
            WHERE (
                (@useCustomDates = 0 AND ps.last_execution_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME()))
                OR
                (@useCustomDates = 1 AND
                    ((ps.cached_time >= @fromDate AND ps.cached_time <= @toDate)
                    OR (ps.last_execution_time >= @fromDate AND ps.last_execution_time <= @toDate)
                    OR (ps.cached_time <= @fromDate AND ps.last_execution_time >= @toDate)))
            )
            GROUP BY
                ps.database_name,
                ps.schema_name,
                ps.object_name,
                ps.cached_time
        )
        SELECT
            database_name = pl.database_name,
            object_id = MAX(pl.object_id),
            object_name = QUOTENAME(pl.schema_name) + N'.' + QUOTENAME(pl.object_name),
            schema_name = pl.schema_name,
            procedure_name = pl.object_name,
            object_type = MAX(pl.object_type),
            type_desc = MAX(pl.type_desc),
            first_cached_time = MIN(pl.first_cached_time),
            last_execution_time = MAX(pl.last_execution_time),
            execution_count = SUM(pl.execution_count),
            total_worker_time = SUM(pl.total_worker_time),
            avg_worker_time_ms = SUM(pl.total_worker_time) / 1000.0 / NULLIF(SUM(pl.execution_count), 0),
            min_worker_time_ms = MIN(pl.min_worker_time) / 1000.0,
            max_worker_time_ms = MAX(pl.max_worker_time) / 1000.0,
            total_elapsed_time = SUM(pl.total_elapsed_time),
            avg_elapsed_time_ms = SUM(pl.total_elapsed_time) / 1000.0 / NULLIF(SUM(pl.execution_count), 0),
            min_elapsed_time_ms = MIN(pl.min_elapsed_time) / 1000.0,
            max_elapsed_time_ms = MAX(pl.max_elapsed_time) / 1000.0,
            total_logical_reads = SUM(pl.total_logical_reads),
            avg_logical_reads = SUM(pl.total_logical_reads) / NULLIF(SUM(pl.execution_count), 0),
            min_logical_reads = MIN(pl.min_logical_reads),
            max_logical_reads = MAX(pl.max_logical_reads),
            total_logical_writes = SUM(pl.total_logical_writes),
            avg_logical_writes = SUM(pl.total_logical_writes) / NULLIF(SUM(pl.execution_count), 0),
            min_logical_writes = MIN(pl.min_logical_writes),
            max_logical_writes = MAX(pl.max_logical_writes),
            total_physical_reads = SUM(pl.total_physical_reads),
            avg_physical_reads = SUM(pl.total_physical_reads) / NULLIF(SUM(pl.execution_count), 0),
            min_physical_reads = MIN(pl.min_physical_reads),
            max_physical_reads = MAX(pl.max_physical_reads),
            total_spills = SUM(pl.total_spills),
            avg_spills = SUM(pl.total_spills) / NULLIF(SUM(pl.execution_count), 0),
            min_spills = MIN(pl.min_spills),
            max_spills = MAX(pl.max_spills),
            query_plan_xml = MAX(pl.query_plan_text),
            sql_handle = CONVERT(nvarchar(130), MAX(pl.sql_handle), 1),
            plan_handle = CONVERT(nvarchar(130), MAX(pl.plan_handle), 1)
        FROM per_lifetime AS pl
        GROUP BY
            pl.database_name,
            pl.schema_name,
            pl.object_name
        ORDER BY
            avg_worker_time_ms DESC
        OPTION
        (
            HASH GROUP,
            HASH JOIN,
            USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')
        );";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@useCustomDates", SqlDbType.Bit) { Value = useCustomDates });
                    command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = (object?)fromDate ?? DBNull.Value });
                    command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = (object?)toDate ?? DBNull.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new ProcedureStatsItem
                        {
                            DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                            ObjectId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                            ObjectName = reader.IsDBNull(2) ? null : reader.GetString(2),
                            SchemaName = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ProcedureName = reader.IsDBNull(4) ? null : reader.GetString(4),
                            ObjectType = reader.IsDBNull(5) ? "" : reader.GetString(5),
                            TypeDesc = reader.IsDBNull(6) ? null : reader.GetString(6),
                            FirstCachedTime = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                            LastExecutionTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                            ExecutionCount = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                            TotalWorkerTime = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                            AvgWorkerTimeMs = reader.IsDBNull(11) ? null : Convert.ToDouble(reader.GetValue(11), CultureInfo.InvariantCulture),
                            MinWorkerTimeMs = reader.IsDBNull(12) ? null : Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture),
                            MaxWorkerTimeMs = reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture),
                            TotalElapsedTime = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                            AvgElapsedTimeMs = reader.IsDBNull(15) ? null : Convert.ToDouble(reader.GetValue(15), CultureInfo.InvariantCulture),
                            MinElapsedTimeMs = reader.IsDBNull(16) ? null : Convert.ToDouble(reader.GetValue(16), CultureInfo.InvariantCulture),
                            MaxElapsedTimeMs = reader.IsDBNull(17) ? null : Convert.ToDouble(reader.GetValue(17), CultureInfo.InvariantCulture),
                            TotalLogicalReads = reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                            AvgLogicalReads = reader.IsDBNull(19) ? null : reader.GetInt64(19),
                            MinLogicalReads = reader.IsDBNull(20) ? null : reader.GetInt64(20),
                            MaxLogicalReads = reader.IsDBNull(21) ? null : reader.GetInt64(21),
                            TotalLogicalWrites = reader.IsDBNull(22) ? 0 : reader.GetInt64(22),
                            AvgLogicalWrites = reader.IsDBNull(23) ? null : reader.GetInt64(23),
                            MinLogicalWrites = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                            MaxLogicalWrites = reader.IsDBNull(25) ? null : reader.GetInt64(25),
                            TotalPhysicalReads = reader.IsDBNull(26) ? 0 : reader.GetInt64(26),
                            AvgPhysicalReads = reader.IsDBNull(27) ? null : reader.GetInt64(27),
                            MinPhysicalReads = reader.IsDBNull(28) ? null : reader.GetInt64(28),
                            MaxPhysicalReads = reader.IsDBNull(29) ? null : reader.GetInt64(29),
                            TotalSpills = reader.IsDBNull(30) ? null : reader.GetInt64(30),
                            AvgSpills = reader.IsDBNull(31) ? null : reader.GetInt64(31),
                            MinSpills = reader.IsDBNull(32) ? null : reader.GetInt64(32),
                            MaxSpills = reader.IsDBNull(33) ? null : reader.GetInt64(33),
                            QueryPlanXml = reader.IsDBNull(34) ? null : reader.GetString(34),
                            SqlHandle = reader.IsDBNull(35) ? null : reader.GetString(35),
                            PlanHandle = reader.IsDBNull(36) ? null : reader.GetString(36)
                        });
                    }

                    return items;
                }

                public async Task<List<QueryStoreItem>> GetQueryStoreDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QueryStoreItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    bool useCustomDates = fromDate.HasValue && toDate.HasValue;

                    // Aggregate inline from collect.query_store_data with time filter applied
                    // BEFORE the GROUP BY so counts/averages reflect only the selected time range.
                    // Note: query_plan_xml is NOT fetched here for performance - use GetQueryStorePlanXmlAsync on demand
                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            database_name = qsd.database_name,
            query_id = qsd.query_id,
            execution_type_desc = MAX(qsd.execution_type_desc),
            module_name = MAX(qsd.module_name),
            first_execution_time = MIN(qsd.server_first_execution_time),
            last_execution_time = MAX(qsd.server_last_execution_time),
            execution_count = SUM(qsd.count_executions),
            plan_count = COUNT_BIG(DISTINCT qsd.plan_id),
            avg_duration_ms = SUM(qsd.avg_duration * qsd.count_executions) / 1000.0 / NULLIF(SUM(qsd.count_executions), 0),
            min_duration_ms = MIN(qsd.min_duration) / 1000.0,
            max_duration_ms = MAX(qsd.max_duration) / 1000.0,
            avg_cpu_time_ms = SUM(qsd.avg_cpu_time * qsd.count_executions) / 1000.0 / NULLIF(SUM(qsd.count_executions), 0),
            min_cpu_time_ms = MIN(qsd.min_cpu_time) / 1000.0,
            max_cpu_time_ms = MAX(qsd.max_cpu_time) / 1000.0,
            avg_logical_reads = SUM(qsd.avg_logical_io_reads * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_logical_reads = MIN(qsd.min_logical_io_reads),
            max_logical_reads = MAX(qsd.max_logical_io_reads),
            avg_logical_writes = SUM(qsd.avg_logical_io_writes * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_logical_writes = MIN(qsd.min_logical_io_writes),
            max_logical_writes = MAX(qsd.max_logical_io_writes),
            avg_physical_reads = SUM(qsd.avg_physical_io_reads * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_physical_reads = MIN(qsd.min_physical_io_reads),
            max_physical_reads = MAX(qsd.max_physical_io_reads),
            min_dop = MIN(qsd.min_dop),
            max_dop = MAX(qsd.max_dop),
            avg_memory_pages = SUM(qsd.avg_query_max_used_memory * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_memory_pages = MIN(qsd.min_query_max_used_memory),
            max_memory_pages = MAX(qsd.max_query_max_used_memory),
            avg_rowcount = SUM(qsd.avg_rowcount * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_rowcount = MIN(qsd.min_rowcount),
            max_rowcount = MAX(qsd.max_rowcount),
            avg_tempdb_pages = SUM(ISNULL(qsd.avg_tempdb_space_used, 0) * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_tempdb_pages = MIN(qsd.min_tempdb_space_used),
            max_tempdb_pages = MAX(qsd.max_tempdb_space_used),
            plan_type = MAX(qsd.plan_type),
            is_forced_plan = MAX(CONVERT(tinyint, qsd.is_forced_plan)),
            compatibility_level = MAX(qsd.compatibility_level),
            query_sql_text = CONVERT(nvarchar(max), MAX(qsd.query_sql_text)),
            query_plan_hash = CONVERT(nvarchar(20), MAX(qsd.query_plan_hash), 1)
        FROM collect.query_store_data AS qsd
        WHERE (
            (@useCustomDates = 0 AND qsd.server_last_execution_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME()))
            OR
            (@useCustomDates = 1 AND
                ((qsd.server_first_execution_time >= @fromDate AND qsd.server_first_execution_time <= @toDate)
                OR (qsd.server_last_execution_time >= @fromDate AND qsd.server_last_execution_time <= @toDate)
                OR (qsd.server_first_execution_time <= @fromDate AND qsd.server_last_execution_time >= @toDate)))
        )
        AND qsd.query_sql_text NOT LIKE N'WAITFOR%'
        GROUP BY
            qsd.database_name,
            qsd.query_id
        ORDER BY
            avg_cpu_time_ms DESC
        OPTION
        (
            HASH GROUP,
            HASH JOIN,
            USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')
        );";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@useCustomDates", SqlDbType.Bit) { Value = useCustomDates });
                    command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
                    command.Parameters.Add(new SqlParameter("@fromDate", SqlDbType.DateTime2) { Value = (object?)fromDate ?? DBNull.Value });
                    command.Parameters.Add(new SqlParameter("@toDate", SqlDbType.DateTime2) { Value = (object?)toDate ?? DBNull.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new QueryStoreItem
                        {
                            DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                            QueryId = reader.GetInt64(1),
                            ExecutionTypeDesc = reader.IsDBNull(2) ? null : reader.GetString(2),
                            ModuleName = reader.IsDBNull(3) ? null : reader.GetString(3),
                            FirstExecutionTime = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                            LastExecutionTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            ExecutionCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            PlanCount = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            AvgDurationMs = reader.IsDBNull(8) ? null : Convert.ToDouble(reader.GetValue(8), CultureInfo.InvariantCulture),
                            MinDurationMs = reader.IsDBNull(9) ? null : Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture),
                            MaxDurationMs = reader.IsDBNull(10) ? null : Convert.ToDouble(reader.GetValue(10), CultureInfo.InvariantCulture),
                            AvgCpuTimeMs = reader.IsDBNull(11) ? null : Convert.ToDouble(reader.GetValue(11), CultureInfo.InvariantCulture),
                            MinCpuTimeMs = reader.IsDBNull(12) ? null : Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture),
                            MaxCpuTimeMs = reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture),
                            AvgLogicalReads = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            MinLogicalReads = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            MaxLogicalReads = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                            AvgLogicalWrites = reader.IsDBNull(17) ? null : reader.GetInt64(17),
                            MinLogicalWrites = reader.IsDBNull(18) ? null : reader.GetInt64(18),
                            MaxLogicalWrites = reader.IsDBNull(19) ? null : reader.GetInt64(19),
                            AvgPhysicalReads = reader.IsDBNull(20) ? null : reader.GetInt64(20),
                            MinPhysicalReads = reader.IsDBNull(21) ? null : reader.GetInt64(21),
                            MaxPhysicalReads = reader.IsDBNull(22) ? null : reader.GetInt64(22),
                            MinDop = reader.IsDBNull(23) ? null : reader.GetInt64(23),
                            MaxDop = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                            AvgMemoryPages = reader.IsDBNull(25) ? null : reader.GetInt64(25),
                            MinMemoryPages = reader.IsDBNull(26) ? null : reader.GetInt64(26),
                            MaxMemoryPages = reader.IsDBNull(27) ? null : reader.GetInt64(27),
                            AvgRowcount = reader.IsDBNull(28) ? null : reader.GetInt64(28),
                            MinRowcount = reader.IsDBNull(29) ? null : reader.GetInt64(29),
                            MaxRowcount = reader.IsDBNull(30) ? null : reader.GetInt64(30),
                            AvgTempdbPages = reader.IsDBNull(31) ? null : reader.GetInt64(31),
                            MinTempdbPages = reader.IsDBNull(32) ? null : reader.GetInt64(32),
                            MaxTempdbPages = reader.IsDBNull(33) ? null : reader.GetInt64(33),
                            PlanType = reader.IsDBNull(34) ? null : reader.GetString(34),
                            IsForcedPlan = !reader.IsDBNull(35) && reader.GetByte(35) == 1,
                            CompatibilityLevel = reader.IsDBNull(36) ? null : reader.GetInt16(36),
                            QuerySqlText = reader.IsDBNull(37) ? null : reader.GetString(37),
                            QueryPlanHash = reader.IsDBNull(38) ? null : reader.GetString(38)
                            // QueryPlanXml is fetched on-demand via GetQueryStorePlanXmlAsync
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

                public async Task<List<QueryStoreRegressionItem>> GetQueryStoreRegressionsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QueryStoreRegressionItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    /*
                    report.query_store_regressions is now an inline TVF requiring parameters:
                    - @start_date: divides baseline from recent (baseline = data BEFORE this date)
                    - @end_date: end of recent period (recent = data between start_date and end_date)
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
            qsr.severity,
            qsr.query_text_sample,
            qsr.last_execution_time
        FROM report.query_store_regressions(@start_date, @end_date) AS qsr
        ORDER BY
            qsr.duration_regression_percent DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    /*Calculate the time window - baseline is everything before start_date, recent is start_date to end_date*/
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
                        /*Use Convert.ToDecimal for robustness - TVF may return bigint or decimal depending on query store aggregations*/
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
                            Severity = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
                            QueryTextSample = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
                            LastExecutionTime = reader.IsDBNull(13) ? null : reader.GetDateTime(13)
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
            sample_query_text = CONVERT(nvarchar(500), sample_query_text),
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

                public async Task<List<BlockingDeadlockStatsItem>> GetBlockingDeadlockStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<BlockingDeadlockStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            bds.collection_id,
            bds.collection_time,
            bds.database_name,
            bds.blocking_event_count,
            bds.total_blocking_duration_ms,
            bds.max_blocking_duration_ms,
            bds.avg_blocking_duration_ms,
            bds.deadlock_count,
            bds.total_deadlock_wait_time_ms,
            bds.victim_count,
            bds.blocking_event_count_delta,
            bds.total_blocking_duration_ms_delta,
            bds.max_blocking_duration_ms_delta,
            bds.deadlock_count_delta,
            bds.total_deadlock_wait_time_ms_delta,
            bds.victim_count_delta,
            bds.sample_interval_seconds
        FROM collect.blocking_deadlock_stats AS bds
        WHERE bds.collection_time >= @from_date AND bds.collection_time <= @to_date
        ORDER BY bds.collection_time DESC;";
                    }
                    else
                    {
                        query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            bds.collection_id,
            bds.collection_time,
            bds.database_name,
            bds.blocking_event_count,
            bds.total_blocking_duration_ms,
            bds.max_blocking_duration_ms,
            bds.avg_blocking_duration_ms,
            bds.deadlock_count,
            bds.total_deadlock_wait_time_ms,
            bds.victim_count,
            bds.blocking_event_count_delta,
            bds.total_blocking_duration_ms_delta,
            bds.max_blocking_duration_ms_delta,
            bds.deadlock_count_delta,
            bds.total_deadlock_wait_time_ms_delta,
            bds.victim_count_delta,
            bds.sample_interval_seconds
        FROM collect.blocking_deadlock_stats AS bds
        WHERE bds.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        ORDER BY bds.collection_time DESC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new BlockingDeadlockStatsItem
                        {
                            CollectionId = reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            DatabaseName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            BlockingEventCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                            TotalBlockingDurationMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                            MaxBlockingDurationMs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                            AvgBlockingDurationMs = reader.IsDBNull(6) ? 0 : reader.GetDecimal(6),
                            DeadlockCount = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            TotalDeadlockWaitTimeMs = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                            VictimCount = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                            BlockingEventCountDelta = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                            TotalBlockingDurationMsDelta = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                            MaxBlockingDurationMsDelta = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                            DeadlockCountDelta = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                            TotalDeadlockWaitTimeMsDelta = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                            VictimCountDelta = reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                            SampleIntervalSeconds = reader.IsDBNull(16) ? 0 : reader.GetInt32(16)
                        });
                    }
        
                    return items;
                }

                public async Task<List<QueryExecutionHistoryItem>> GetQueryStoreHistoryAsync(string databaseName, long queryId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QueryExecutionHistoryItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND   qsd.server_last_execution_time >= @from_date AND qsd.server_last_execution_time <= @to_date"
                        : "AND   qsd.server_last_execution_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            collection_id = MAX(qsd.collection_id),
            qsd.collection_time,
            qsd.plan_id,
            count_executions = SUM(qsd.count_executions),
            avg_duration = SUM(qsd.avg_duration * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_duration = MIN(qsd.min_duration),
            max_duration = MAX(qsd.max_duration),
            avg_cpu_time = SUM(qsd.avg_cpu_time * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_cpu_time = MIN(qsd.min_cpu_time),
            max_cpu_time = MAX(qsd.max_cpu_time),
            avg_logical_io_reads = SUM(qsd.avg_logical_io_reads * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_logical_io_reads = MIN(qsd.min_logical_io_reads),
            max_logical_io_reads = MAX(qsd.max_logical_io_reads),
            avg_logical_io_writes = SUM(qsd.avg_logical_io_writes * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_logical_io_writes = MIN(qsd.min_logical_io_writes),
            max_logical_io_writes = MAX(qsd.max_logical_io_writes),
            avg_physical_io_reads = SUM(qsd.avg_physical_io_reads * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_physical_io_reads = MIN(qsd.min_physical_io_reads),
            max_physical_io_reads = MAX(qsd.max_physical_io_reads),
            min_dop = MIN(qsd.min_dop),
            max_dop = MAX(qsd.max_dop),
            avg_query_max_used_memory = SUM(qsd.avg_query_max_used_memory * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_query_max_used_memory = MIN(qsd.min_query_max_used_memory),
            max_query_max_used_memory = MAX(qsd.max_query_max_used_memory),
            avg_rowcount = SUM(qsd.avg_rowcount * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_rowcount = MIN(qsd.min_rowcount),
            max_rowcount = MAX(qsd.max_rowcount),
            avg_tempdb_space_used = SUM(qsd.avg_tempdb_space_used * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
            min_tempdb_space_used = MIN(qsd.min_tempdb_space_used),
            max_tempdb_space_used = MAX(qsd.max_tempdb_space_used),
            plan_type = MAX(qsd.plan_type),
            is_forced_plan = CAST(MAX(CAST(qsd.is_forced_plan AS tinyint)) AS bit),
            force_failure_count = MAX(qsd.force_failure_count),
            last_force_failure_reason_desc = MAX(qsd.last_force_failure_reason_desc),
            plan_forcing_type = MAX(qsd.plan_forcing_type),
            compatibility_level = MAX(qsd.compatibility_level)
        FROM collect.query_store_data AS qsd
        WHERE qsd.database_name = @database_name
        AND   qsd.query_id = @query_id
        {timeFilter}
        GROUP BY
            qsd.collection_time,
            qsd.plan_id
        ORDER BY
            qsd.collection_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@query_id", SqlDbType.BigInt) { Value = queryId });

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new QueryExecutionHistoryItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            PlanId = reader.GetInt64(2),
                            CountExecutions = reader.GetInt64(3),
                            AvgDuration = reader.GetInt64(4),
                            MinDuration = reader.GetInt64(5),
                            MaxDuration = reader.GetInt64(6),
                            AvgCpuTime = reader.GetInt64(7),
                            MinCpuTime = reader.GetInt64(8),
                            MaxCpuTime = reader.GetInt64(9),
                            AvgLogicalReads = reader.GetInt64(10),
                            MinLogicalReads = reader.GetInt64(11),
                            MaxLogicalReads = reader.GetInt64(12),
                            AvgLogicalWrites = reader.GetInt64(13),
                            MinLogicalWrites = reader.GetInt64(14),
                            MaxLogicalWrites = reader.GetInt64(15),
                            AvgPhysicalReads = reader.GetInt64(16),
                            MinPhysicalReads = reader.GetInt64(17),
                            MaxPhysicalReads = reader.GetInt64(18),
                            MinDop = reader.GetInt64(19),
                            MaxDop = reader.GetInt64(20),
                            AvgMemoryPages = reader.GetInt64(21),
                            MinMemoryPages = reader.GetInt64(22),
                            MaxMemoryPages = reader.GetInt64(23),
                            AvgRowcount = reader.GetInt64(24),
                            MinRowcount = reader.GetInt64(25),
                            MaxRowcount = reader.GetInt64(26),
                            AvgTempdbSpaceUsed = reader.IsDBNull(27) ? null : reader.GetInt64(27),
                            MinTempdbSpaceUsed = reader.IsDBNull(28) ? null : reader.GetInt64(28),
                            MaxTempdbSpaceUsed = reader.IsDBNull(29) ? null : reader.GetInt64(29),
                            PlanType = reader.IsDBNull(30) ? null : reader.GetString(30),
                            IsForcedPlan = reader.GetBoolean(31),
                            ForceFailureCount = reader.IsDBNull(32) ? null : reader.GetInt64(32),
                            LastForceFailureReasonDesc = reader.IsDBNull(33) ? null : reader.GetString(33),
                            PlanForcingType = reader.IsDBNull(34) ? null : reader.GetString(34),
                            CompatibilityLevel = reader.IsDBNull(35) ? null : reader.GetInt16(35)
                        });
                    }

                    return items;
                }

                public async Task<List<ProcedureExecutionHistoryItem>> GetProcedureStatsHistoryAsync(string databaseName, int objectId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<ProcedureExecutionHistoryItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND   ps.collection_time >= @from_date AND ps.collection_time <= @to_date"
                        : "AND   ps.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ps.collection_id,
            ps.collection_time,
            ps.server_start_time,
            ps.object_type,
            ps.type_desc,
            ps.cached_time,
            ps.last_execution_time,
            ps.execution_count,
            ps.total_worker_time,
            ps.min_worker_time,
            ps.max_worker_time,
            ps.total_elapsed_time,
            ps.min_elapsed_time,
            ps.max_elapsed_time,
            ps.total_logical_reads,
            ps.min_logical_reads,
            ps.max_logical_reads,
            ps.total_physical_reads,
            ps.min_physical_reads,
            ps.max_physical_reads,
            ps.total_logical_writes,
            ps.min_logical_writes,
            ps.max_logical_writes,
            ps.total_spills,
            ps.min_spills,
            ps.max_spills,
            ps.execution_count_delta,
            ps.total_worker_time_delta,
            ps.total_elapsed_time_delta,
            ps.total_logical_reads_delta,
            ps.total_physical_reads_delta,
            ps.total_logical_writes_delta,
            ps.sample_interval_seconds
        FROM collect.procedure_stats AS ps
        WHERE ps.database_name = @database_name
        AND   ps.object_id = @object_id
        {timeFilter}
        ORDER BY
            ps.collection_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@object_id", SqlDbType.Int) { Value = objectId });

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new ProcedureExecutionHistoryItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            ObjectType = reader.GetString(3),
                            TypeDesc = reader.IsDBNull(4) ? null : reader.GetString(4),
                            CachedTime = reader.GetDateTime(5),
                            LastExecutionTime = reader.GetDateTime(6),
                            ExecutionCount = reader.GetInt64(7),
                            TotalWorkerTime = reader.GetInt64(8),
                            MinWorkerTime = reader.GetInt64(9),
                            MaxWorkerTime = reader.GetInt64(10),
                            TotalElapsedTime = reader.GetInt64(11),
                            MinElapsedTime = reader.GetInt64(12),
                            MaxElapsedTime = reader.GetInt64(13),
                            TotalLogicalReads = reader.GetInt64(14),
                            MinLogicalReads = reader.GetInt64(15),
                            MaxLogicalReads = reader.GetInt64(16),
                            TotalPhysicalReads = reader.GetInt64(17),
                            MinPhysicalReads = reader.GetInt64(18),
                            MaxPhysicalReads = reader.GetInt64(19),
                            TotalLogicalWrites = reader.GetInt64(20),
                            MinLogicalWrites = reader.GetInt64(21),
                            MaxLogicalWrites = reader.GetInt64(22),
                            TotalSpills = reader.IsDBNull(23) ? null : reader.GetInt64(23),
                            MinSpills = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                            MaxSpills = reader.IsDBNull(25) ? null : reader.GetInt64(25),
                            ExecutionCountDelta = reader.IsDBNull(26) ? null : reader.GetInt64(26),
                            TotalWorkerTimeDelta = reader.IsDBNull(27) ? null : reader.GetInt64(27),
                            TotalElapsedTimeDelta = reader.IsDBNull(28) ? null : reader.GetInt64(28),
                            TotalLogicalReadsDelta = reader.IsDBNull(29) ? null : reader.GetInt64(29),
                            TotalPhysicalReadsDelta = reader.IsDBNull(30) ? null : reader.GetInt64(30),
                            TotalLogicalWritesDelta = reader.IsDBNull(31) ? null : reader.GetInt64(31),
                            SampleIntervalSeconds = reader.IsDBNull(32) ? null : reader.GetInt32(32)
                        });
                    }

                    return items;
                }

                public async Task<List<ProcedureExecutionHistoryItem>> GetProcedureStatsHistoryAsync(string databaseName, string schemaName, string procedureName, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<ProcedureExecutionHistoryItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND   ps.collection_time >= @from_date AND ps.collection_time <= @to_date"
                        : "AND   ps.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ps.collection_id,
            ps.collection_time,
            ps.server_start_time,
            ps.object_type,
            ps.type_desc,
            ps.cached_time,
            ps.last_execution_time,
            ps.execution_count,
            ps.total_worker_time,
            ps.min_worker_time,
            ps.max_worker_time,
            ps.total_elapsed_time,
            ps.min_elapsed_time,
            ps.max_elapsed_time,
            ps.total_logical_reads,
            ps.min_logical_reads,
            ps.max_logical_reads,
            ps.total_physical_reads,
            ps.min_physical_reads,
            ps.max_physical_reads,
            ps.total_logical_writes,
            ps.min_logical_writes,
            ps.max_logical_writes,
            ps.total_spills,
            ps.min_spills,
            ps.max_spills,
            ps.execution_count_delta,
            ps.total_worker_time_delta,
            ps.total_elapsed_time_delta,
            ps.total_logical_reads_delta,
            ps.total_physical_reads_delta,
            ps.total_logical_writes_delta,
            ps.sample_interval_seconds
        FROM collect.procedure_stats AS ps
        WHERE ps.database_name = @database_name
        AND   ps.schema_name = @schema_name
        AND   ps.object_name = @object_name
        {timeFilter}
        ORDER BY
            ps.collection_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@schema_name", SqlDbType.NVarChar, 128) { Value = schemaName });
                    command.Parameters.Add(new SqlParameter("@object_name", SqlDbType.NVarChar, 128) { Value = procedureName });

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new ProcedureExecutionHistoryItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            ObjectType = reader.GetString(3),
                            TypeDesc = reader.IsDBNull(4) ? null : reader.GetString(4),
                            CachedTime = reader.GetDateTime(5),
                            LastExecutionTime = reader.GetDateTime(6),
                            ExecutionCount = reader.GetInt64(7),
                            TotalWorkerTime = reader.GetInt64(8),
                            MinWorkerTime = reader.GetInt64(9),
                            MaxWorkerTime = reader.GetInt64(10),
                            TotalElapsedTime = reader.GetInt64(11),
                            MinElapsedTime = reader.GetInt64(12),
                            MaxElapsedTime = reader.GetInt64(13),
                            TotalLogicalReads = reader.GetInt64(14),
                            MinLogicalReads = reader.GetInt64(15),
                            MaxLogicalReads = reader.GetInt64(16),
                            TotalPhysicalReads = reader.GetInt64(17),
                            MinPhysicalReads = reader.GetInt64(18),
                            MaxPhysicalReads = reader.GetInt64(19),
                            TotalLogicalWrites = reader.GetInt64(20),
                            MinLogicalWrites = reader.GetInt64(21),
                            MaxLogicalWrites = reader.GetInt64(22),
                            TotalSpills = reader.IsDBNull(23) ? null : reader.GetInt64(23),
                            MinSpills = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                            MaxSpills = reader.IsDBNull(25) ? null : reader.GetInt64(25),
                            ExecutionCountDelta = reader.IsDBNull(26) ? null : reader.GetInt64(26),
                            TotalWorkerTimeDelta = reader.IsDBNull(27) ? null : reader.GetInt64(27),
                            TotalElapsedTimeDelta = reader.IsDBNull(28) ? null : reader.GetInt64(28),
                            TotalLogicalReadsDelta = reader.IsDBNull(29) ? null : reader.GetInt64(29),
                            TotalPhysicalReadsDelta = reader.IsDBNull(30) ? null : reader.GetInt64(30),
                            TotalLogicalWritesDelta = reader.IsDBNull(31) ? null : reader.GetInt64(31),
                            SampleIntervalSeconds = reader.IsDBNull(32) ? null : reader.GetInt32(32)
                        });
                    }

                    return items;
                }

                public async Task<List<QueryStatsHistoryItem>> GetQueryStatsHistoryAsync(string databaseName, string queryHash, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QueryStatsHistoryItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    var timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "AND   qs.collection_time >= @from_date AND qs.collection_time <= @to_date"
                        : "AND   qs.collection_time >= DATEADD(HOUR, -@hours_back, SYSDATETIME())";

                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            collection_id = MAX(qs.collection_id),
            qs.collection_time,
            server_start_time = MAX(qs.server_start_time),
            object_type = MAX(qs.object_type),
            creation_time = MIN(qs.creation_time),
            last_execution_time = MAX(qs.last_execution_time),
            execution_count = MAX(qs.execution_count),
            total_worker_time = MAX(qs.total_worker_time),
            min_worker_time = MIN(qs.min_worker_time),
            max_worker_time = MAX(qs.max_worker_time),
            total_elapsed_time = MAX(qs.total_elapsed_time),
            min_elapsed_time = MIN(qs.min_elapsed_time),
            max_elapsed_time = MAX(qs.max_elapsed_time),
            total_logical_reads = MAX(qs.total_logical_reads),
            total_physical_reads = MAX(qs.total_physical_reads),
            min_physical_reads = MIN(qs.min_physical_reads),
            max_physical_reads = MAX(qs.max_physical_reads),
            total_logical_writes = MAX(qs.total_logical_writes),
            total_clr_time = MAX(qs.total_clr_time),
            total_rows = MAX(qs.total_rows),
            min_rows = MIN(qs.min_rows),
            max_rows = MAX(qs.max_rows),
            min_dop = MIN(qs.min_dop),
            max_dop = MAX(qs.max_dop),
            min_grant_kb = MIN(qs.min_grant_kb),
            max_grant_kb = MAX(qs.max_grant_kb),
            min_used_grant_kb = MIN(qs.min_used_grant_kb),
            max_used_grant_kb = MAX(qs.max_used_grant_kb),
            min_ideal_grant_kb = MIN(qs.min_ideal_grant_kb),
            max_ideal_grant_kb = MAX(qs.max_ideal_grant_kb),
            min_reserved_threads = MIN(qs.min_reserved_threads),
            max_reserved_threads = MAX(qs.max_reserved_threads),
            min_used_threads = MIN(qs.min_used_threads),
            max_used_threads = MAX(qs.max_used_threads),
            total_spills = MAX(qs.total_spills),
            min_spills = MIN(qs.min_spills),
            max_spills = MAX(qs.max_spills),
            execution_count_delta = SUM(qs.execution_count_delta),
            total_worker_time_delta = SUM(qs.total_worker_time_delta),
            total_elapsed_time_delta = SUM(qs.total_elapsed_time_delta),
            total_logical_reads_delta = SUM(qs.total_logical_reads_delta),
            total_physical_reads_delta = SUM(qs.total_physical_reads_delta),
            total_logical_writes_delta = SUM(qs.total_logical_writes_delta),
            sample_interval_seconds = MAX(qs.sample_interval_seconds)
        FROM collect.query_stats AS qs
        WHERE qs.database_name = @database_name
        AND   qs.query_hash = CONVERT(binary(8), @query_hash, 1)
        {timeFilter}
        GROUP BY
            qs.collection_time
        ORDER BY
            qs.collection_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@query_hash", SqlDbType.NVarChar, 20) { Value = queryHash });

                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                        command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
                    }
                    else
                    {
                        command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = hoursBack });
                    }

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new QueryStatsHistoryItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            ObjectType = reader.GetString(3),
                            CreationTime = reader.GetDateTime(4),
                            LastExecutionTime = reader.GetDateTime(5),
                            ExecutionCount = reader.GetInt64(6),
                            TotalWorkerTime = reader.GetInt64(7),
                            MinWorkerTime = reader.GetInt64(8),
                            MaxWorkerTime = reader.GetInt64(9),
                            TotalElapsedTime = reader.GetInt64(10),
                            MinElapsedTime = reader.GetInt64(11),
                            MaxElapsedTime = reader.GetInt64(12),
                            TotalLogicalReads = reader.GetInt64(13),
                            TotalPhysicalReads = reader.GetInt64(14),
                            MinPhysicalReads = reader.GetInt64(15),
                            MaxPhysicalReads = reader.GetInt64(16),
                            TotalLogicalWrites = reader.GetInt64(17),
                            TotalClrTime = reader.GetInt64(18),
                            TotalRows = reader.GetInt64(19),
                            MinRows = reader.GetInt64(20),
                            MaxRows = reader.GetInt64(21),
                            MinDop = reader.GetInt16(22),
                            MaxDop = reader.GetInt16(23),
                            MinGrantKb = reader.GetInt64(24),
                            MaxGrantKb = reader.GetInt64(25),
                            MinUsedGrantKb = reader.GetInt64(26),
                            MaxUsedGrantKb = reader.GetInt64(27),
                            MinIdealGrantKb = reader.GetInt64(28),
                            MaxIdealGrantKb = reader.GetInt64(29),
                            MinReservedThreads = reader.GetInt32(30),
                            MaxReservedThreads = reader.GetInt32(31),
                            MinUsedThreads = reader.GetInt32(32),
                            MaxUsedThreads = reader.GetInt32(33),
                            TotalSpills = reader.GetInt64(34),
                            MinSpills = reader.GetInt64(35),
                            MaxSpills = reader.GetInt64(36),
                            ExecutionCountDelta = reader.IsDBNull(37) ? null : reader.GetInt64(37),
                            TotalWorkerTimeDelta = reader.IsDBNull(38) ? null : reader.GetInt64(38),
                            TotalElapsedTimeDelta = reader.IsDBNull(39) ? null : reader.GetInt64(39),
                            TotalLogicalReadsDelta = reader.IsDBNull(40) ? null : reader.GetInt64(40),
                            TotalPhysicalReadsDelta = reader.IsDBNull(41) ? null : reader.GetInt64(41),
                            TotalLogicalWritesDelta = reader.IsDBNull(42) ? null : reader.GetInt64(42),
                            SampleIntervalSeconds = reader.IsDBNull(43) ? null : reader.GetInt32(43)
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
            qsd.query_plan_text
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
            ps.query_plan
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
            qs.query_plan_text
        FROM collect.query_stats AS qs
        WHERE qs.collection_id = @collection_id;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@collection_id", SqlDbType.BigInt) { Value = collectionId });

            var result = await command.ExecuteScalarAsync();
            return result == DBNull.Value || result == null ? null : (string)result;
        }

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
            executions_per_second = CAST(ed.total_execution_count AS decimal(19, 4)) / ed.interval_seconds
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
            executions_per_second = CAST(ed.total_execution_count AS decimal(19, 4)) / ed.interval_seconds
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

        /// <summary>
        /// Gets LCK (lock) wait stats from wait_stats deltas, aggregated by collection time and wait type.
        /// </summary>
        public async Task<List<LockWaitStatsItem>> GetLockWaitStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<LockWaitStatsItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH lock_deltas AS
        (
            SELECT
                ws.collection_time,
                ws.wait_type,
                ws.wait_time_ms_delta,
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
            WHERE ws.wait_type LIKE N'LCK%'
            AND   ws.collection_time >= @from_date
            AND   ws.collection_time <= @to_date
        )
        SELECT
            ld.collection_time,
            ld.wait_type,
            wait_time_ms_per_second =
                CASE
                    WHEN ld.interval_seconds > 0
                    THEN CAST(ld.wait_time_ms_delta AS decimal(19, 4)) / ld.interval_seconds
                    ELSE 0
                END
        FROM lock_deltas AS ld
        WHERE ld.wait_time_ms_delta >= 0
        ORDER BY
            ld.collection_time,
            ld.wait_type;";
            }
            else
            {
                query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH lock_deltas AS
        (
            SELECT
                ws.collection_time,
                ws.wait_type,
                ws.wait_time_ms_delta,
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
            WHERE ws.wait_type LIKE N'LCK%'
            AND   ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
        )
        SELECT
            ld.collection_time,
            ld.wait_type,
            wait_time_ms_per_second =
                CASE
                    WHEN ld.interval_seconds > 0
                    THEN CAST(ld.wait_time_ms_delta AS decimal(19, 4)) / ld.interval_seconds
                    ELSE 0
                END
        FROM lock_deltas AS ld
        WHERE ld.wait_time_ms_delta >= 0
        ORDER BY
            ld.collection_time,
            ld.wait_type;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new LockWaitStatsItem
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        /// <summary>
        /// Queries dm_exec_requests directly for currently running queries (live snapshot).
        /// Returns results with query plans in memory for on-demand download.
        /// </summary>
        public async Task<List<LiveQueryItem>> GetCurrentActiveQueriesAsync()
        {
            var items = new List<LiveQueryItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000;

SELECT
    der.session_id,
    database_name = DB_NAME(der.database_id),
    elapsed_time_formatted =
        CASE
            WHEN der.total_elapsed_time < 0
            THEN '00 00:00:00.000'
            ELSE RIGHT(REPLICATE('0', 2) + CONVERT(varchar(10), der.total_elapsed_time / 86400000), 2) +
                 ' ' + RIGHT(CONVERT(varchar(30), DATEADD(second, der.total_elapsed_time / 1000, 0), 120), 9) +
                 '.' + RIGHT('000' + CONVERT(varchar(3), der.total_elapsed_time % 1000), 3)
        END,
    query_text = SUBSTRING(dest.text, (der.statement_start_offset / 2) + 1,
        ((CASE der.statement_end_offset WHEN -1 THEN DATALENGTH(dest.text)
          ELSE der.statement_end_offset END - der.statement_start_offset) / 2) + 1),
    query_plan = TRY_CAST(deqp.query_plan AS nvarchar(max)),
    live_query_plan = deqs.query_plan,
    der.status,
    der.blocking_session_id,
    der.wait_type,
    wait_time_ms = CONVERT(bigint, der.wait_time),
    der.wait_resource,
    cpu_time_ms = CONVERT(bigint, der.cpu_time),
    total_elapsed_time_ms = CONVERT(bigint, der.total_elapsed_time),
    der.reads,
    der.writes,
    der.logical_reads,
    granted_query_memory_gb = CONVERT(decimal(38, 2), (der.granted_query_memory / 128. / 1024.)),
    transaction_isolation_level =
        CASE der.transaction_isolation_level
            WHEN 0 THEN 'Unspecified'
            WHEN 1 THEN 'Read Uncommitted'
            WHEN 2 THEN 'Read Committed'
            WHEN 3 THEN 'Repeatable Read'
            WHEN 4 THEN 'Serializable'
            WHEN 5 THEN 'Snapshot'
            ELSE '???'
        END,
    der.dop,
    der.parallel_worker_count,
    des.login_name,
    des.host_name,
    des.program_name,
    des.open_transaction_count,
    der.percent_complete
FROM sys.dm_exec_requests AS der
JOIN sys.dm_exec_sessions AS des
    ON des.session_id = der.session_id
OUTER APPLY sys.dm_exec_sql_text(der.plan_handle) AS dest
OUTER APPLY sys.dm_exec_text_query_plan(der.plan_handle, der.statement_start_offset, der.statement_end_offset) AS deqp
OUTER APPLY sys.dm_exec_query_statistics_xml(der.session_id) AS deqs
WHERE der.session_id <> @@SPID
AND   der.session_id >= 50
AND   dest.text IS NOT NULL
AND   der.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
ORDER BY der.cpu_time DESC, der.parallel_worker_count DESC
OPTION(MAXDOP 1, RECOMPILE);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;

            var snapshotTime = DateTime.Now;

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new LiveQueryItem
                {
                    SnapshotTime = snapshotTime,
                    SessionId = Convert.ToInt32(reader.GetValue(0)),
                    DatabaseName = reader.IsDBNull(1) ? null : reader.GetString(1),
                    ElapsedTimeFormatted = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                    QueryText = reader.IsDBNull(3) ? null : reader.GetString(3),
                    QueryPlan = reader.IsDBNull(4) ? null : reader.GetString(4),
                    LiveQueryPlan = reader.IsDBNull(5) ? null : reader.GetValue(5)?.ToString(),
                    Status = reader.IsDBNull(6) ? null : reader.GetString(6),
                    BlockingSessionId = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                    WaitType = reader.IsDBNull(8) ? null : reader.GetString(8),
                    WaitTimeMs = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
                    WaitResource = reader.IsDBNull(10) ? null : reader.GetString(10),
                    CpuTimeMs = reader.IsDBNull(11) ? 0 : Convert.ToInt64(reader.GetValue(11)),
                    TotalElapsedTimeMs = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
                    Reads = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
                    Writes = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
                    LogicalReads = reader.IsDBNull(15) ? 0 : Convert.ToInt64(reader.GetValue(15)),
                    GrantedQueryMemoryGb = reader.IsDBNull(16) ? 0m : reader.GetDecimal(16),
                    TransactionIsolationLevel = reader.IsDBNull(17) ? null : reader.GetString(17),
                    Dop = reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18)),
                    ParallelWorkerCount = reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19)),
                    LoginName = reader.IsDBNull(20) ? null : reader.GetString(20),
                    HostName = reader.IsDBNull(21) ? null : reader.GetString(21),
                    ProgramName = reader.IsDBNull(22) ? null : reader.GetString(22),
                    OpenTransactionCount = reader.IsDBNull(23) ? 0 : Convert.ToInt32(reader.GetValue(23)),
                    PercentComplete = reader.IsDBNull(24) ? 0m : reader.GetDecimal(24)
                });
            }

            return items;
        }
    }
}
