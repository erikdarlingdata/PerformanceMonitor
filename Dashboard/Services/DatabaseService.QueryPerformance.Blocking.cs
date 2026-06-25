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
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // Blocking, deadlock, and lock-wait data access.
        // ============================================

                public async Task<List<BlockingEventItem>> GetBlockingEventsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, bool includeReport = true)
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
                                CONVERT(nvarchar(max), b.blocked_process_report_xml) AS blocked_process_report_xml,
                                CAST(CASE WHEN b.blocked_process_report_xml IS NOT NULL THEN 1 ELSE 0 END AS bit) AS has_blocked_process_report,
                                b.blocking_spid,
                                b.blocking_last_tran_started,
                                b.monitor_loop
                            FROM collect.blocking_BlockedProcessReport AS b
                            WHERE b.collection_time >= @from_date
                            AND   b.collection_time <= @to_date
                            ORDER BY
                                b.event_time DESC,
                                CASE b.activity WHEN N'blocking' THEN 0 ELSE 1 END,
                                LEN(b.blocking_tree) - LEN(REPLACE(b.blocking_tree, N'>', N''))
                            OPTION(RECOMPILE);";
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
                                CONVERT(nvarchar(max), b.blocked_process_report_xml) AS blocked_process_report_xml,
                                CAST(CASE WHEN b.blocked_process_report_xml IS NOT NULL THEN 1 ELSE 0 END AS bit) AS has_blocked_process_report,
                                b.blocking_spid,
                                b.blocking_last_tran_started,
                                b.monitor_loop
                            FROM collect.blocking_BlockedProcessReport AS b
                            WHERE b.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                b.event_time DESC,
                                CASE b.activity WHEN N'blocking' THEN 0 ELSE 1 END,
                                LEN(b.blocking_tree) - LEN(REPLACE(b.blocking_tree, N'>', N''))
                            OPTION(RECOMPILE);";
                    }
        
                    // The grid never displays the report XML — it only needs to know one exists
                    // (to enable the View Plan / Download buttons). Skip the expensive
                    // CONVERT(nvarchar(max), xml) of ~10 KB per row; fetch on demand instead.
                    if (!includeReport)
                        query = query.Replace(
                            "CONVERT(nvarchar(max), b.blocked_process_report_xml) AS blocked_process_report_xml",
                            "CONVERT(nvarchar(max), NULL) AS blocked_process_report_xml");

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
                            BlockedProcessReportXml = reader.IsDBNull(30) ? string.Empty : reader.GetString(30),
                            HasBlockedProcessReport = !reader.IsDBNull(31) && reader.GetBoolean(31),
                            BlockingSpid = reader.IsDBNull(32) ? (int?)null : reader.GetInt32(32),
                            BlockingLastTranStarted = reader.IsDBNull(33) ? (DateTime?)null : reader.GetDateTime(33),
                            MonitorLoop = reader.IsDBNull(34) ? (int?)null : reader.GetInt32(34)
                        });
                    }
        
                    return items;
                }

                // On-demand fetch of a single blocked process report XML (deferred from the grid query
                // above). CONVERT(nvarchar(max), xml) is used so the result carries no UTF-16 encoding
                // declaration — keeping it parseable by XElement.Parse downstream.
                public async Task<string?> GetBlockedProcessReportAsync(long blockingId, DateTime collectionTime)
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
                    const string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT TOP (1) CONVERT(nvarchar(max), b.blocked_process_report_xml)
                        FROM collect.blocking_BlockedProcessReport AS b
                        WHERE b.blocking_id = @blockingId
                        AND   b.collection_time = @collectionTime;";
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@blockingId", SqlDbType.BigInt) { Value = blockingId });
                    command.Parameters.Add(new SqlParameter("@collectionTime", SqlDbType.DateTime2) { Value = collectionTime });
                    var result = await command.ExecuteScalarAsync();
                    return result == DBNull.Value ? null : result as string;
                }

                public async Task<List<DeadlockItem>> GetDeadlocksAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, bool includeGraph = true)
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
                                d.event_date DESC
                            OPTION(RECOMPILE);";
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
                                d.event_date DESC
                            OPTION(RECOMPILE);";
                    }
        
                    // The grid never displays the deadlock graph (View/Download fetch it on demand), and
                    // CONVERT(nvarchar(max), xml) over ~100 graphs is the bulk of the Locking-tab load.
                    // Skip it for grid callers; the alert engine / MCP keep includeGraph = true.
                    if (!includeGraph)
                        query = query.Replace("CONVERT(nvarchar(max), d.deadlock_graph)", "CONVERT(nvarchar(max), NULL)");

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

                /// <summary>
                /// Fetches one deadlock's graph XML on demand (View/Download). The grid query skips the
                /// graph (includeGraph: false) because CONVERT(nvarchar(max), xml) over ~100 graphs is the
                /// bulk of the Locking-tab load. CONVERT here yields a declaration-free string the deadlock
                /// graph parser accepts.
                /// </summary>
                public async Task<string?> GetDeadlockGraphAsync(DateTime collectionTime, long deadlockId)
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string query = @"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT TOP (1)
            CONVERT(nvarchar(max), d.deadlock_graph)
        FROM collect.deadlocks AS d
        WHERE d.collection_time = @collectionTime
        AND   d.deadlock_id = @deadlockId;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@collectionTime", SqlDbType.DateTime2) { Value = collectionTime });
                    command.Parameters.Add(new SqlParameter("@deadlockId", SqlDbType.BigInt) { Value = deadlockId });

                    var result = await command.ExecuteScalarAsync();
                    return result == DBNull.Value ? null : result as string;
                }

                /// <summary>
                /// Fetches the blocked/blocker pair rows for a window, for the block-chain viewer to feed
                /// <see cref="BlockingChainReconstructor"/>. Internal (not public): <see cref="BlockingPairRow"/>
                /// is internal to the analysis assembly — a public method returning it would be CS0050. Uses
                /// the viewer variant <see cref="BlockingPairRowQuery.SqlWithBlockerIdentity"/>: the same apex
                /// filter as the drill-down + fact collectors, plus the correlated blocker identity so the apex
                /// node shows login/host/app (the collectors keep the lighter query; they don't display it).
                /// </summary>
                internal async Task<List<BlockingPairRow>> GetBlockingPairRowsAsync(DateTime start, DateTime end)
                {
                    var rows = new List<BlockingPairRow>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    using var command = new SqlCommand(BlockingPairRowQuery.SqlWithBlockerIdentity, connection);
                    command.CommandTimeout = 120;
                    BlockingPairRowQuery.AddParameters(command, start, end);

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                        rows.Add(BlockingPairRowQuery.ReadWithBlockerIdentity(reader));

                    return rows;
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

        public async Task<List<BlockedSessionTrendItem>> GetBlockedSessionTrendAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<BlockedSessionTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    SELECT
                        wt.collection_time,
                        wt.database_name,
                        blocked_count = COUNT(*)
                    FROM collect.waiting_tasks AS wt
                    WHERE wt.blocking_session_id > 0
                    AND   wt.collection_time >= @from_date
                    AND   wt.collection_time <= @to_date
                    AND   wt.database_name IS NOT NULL
                    GROUP BY
                        wt.collection_time,
                        wt.database_name
                    ORDER BY
                        wt.collection_time,
                        wt.database_name;";
            }
            else
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    SELECT
                        wt.collection_time,
                        wt.database_name,
                        blocked_count = COUNT(*)
                    FROM collect.waiting_tasks AS wt
                    WHERE wt.blocking_session_id > 0
                    AND   wt.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                    AND   wt.database_name IS NOT NULL
                    GROUP BY
                        wt.collection_time,
                        wt.database_name
                    ORDER BY
                        wt.collection_time,
                        wt.database_name;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new BlockedSessionTrendItem
                {
                    CollectionTime = reader.GetDateTime(0),
                    DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    BlockedCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        public async Task<List<BlockedSessionTrendItem>> GetDeadlockTrendAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<BlockedSessionTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    SELECT
                        CollectionTime = DATEADD(MINUTE, DATEDIFF(MINUTE, 0, d.event_date), 0),
                        DatabaseName = N'',
                        BlockedCount = COUNT(*)
                    FROM collect.deadlocks AS d
                    WHERE d.event_date >= @from_date
                    AND   d.event_date <= @to_date
                    GROUP BY
                        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, d.event_date), 0)
                    ORDER BY
                        CollectionTime;";
            }
            else
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    SELECT
                        CollectionTime = DATEADD(MINUTE, DATEDIFF(MINUTE, 0, d.event_date), 0),
                        DatabaseName = N'',
                        BlockedCount = COUNT(*)
                    FROM collect.deadlocks AS d
                    WHERE d.event_date >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                    GROUP BY
                        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, d.event_date), 0)
                    ORDER BY
                        CollectionTime;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new BlockedSessionTrendItem
                {
                    CollectionTime = reader.GetDateTime(0),
                    DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    BlockedCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

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
                    THEN CAST(CAST(ld.wait_time_ms_delta AS decimal(19, 4)) / ld.interval_seconds AS decimal(18, 4))
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
                    THEN CAST(CAST(ld.wait_time_ms_delta AS decimal(19, 4)) / ld.interval_seconds AS decimal(18, 4))
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

        public async Task<List<WaitingTaskTrendItem>> GetWaitingTaskTrendAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<WaitingTaskTrendItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    SELECT
                        wt.collection_time,
                        wt.wait_type,
                        total_wait_ms = SUM(wt.wait_duration_ms)
                    FROM collect.waiting_tasks AS wt
                    WHERE wt.collection_time >= @from_date
                    AND   wt.collection_time <= @to_date
                    AND   wt.wait_type IS NOT NULL
                    GROUP BY
                        wt.collection_time,
                        wt.wait_type
                    ORDER BY
                        wt.collection_time,
                        wt.wait_type;";
            }
            else
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    SELECT
                        wt.collection_time,
                        wt.wait_type,
                        total_wait_ms = SUM(wt.wait_duration_ms)
                    FROM collect.waiting_tasks AS wt
                    WHERE wt.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                    AND   wt.wait_type IS NOT NULL
                    GROUP BY
                        wt.collection_time,
                        wt.wait_type
                    ORDER BY
                        wt.collection_time,
                        wt.wait_type;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new WaitingTaskTrendItem
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    TotalWaitMs = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

    }
}
