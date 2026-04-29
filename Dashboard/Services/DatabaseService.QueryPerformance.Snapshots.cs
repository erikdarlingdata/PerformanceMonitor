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
        // Query snapshot data access (active query telemetry).
        // ============================================

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
                                sql_text = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_text), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
                                sql_command = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_command), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
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
                                sql_text = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_text), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
                                sql_command = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_command), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
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

                /// <summary>
                /// Gets query snapshots filtered by wait type for the wait drill-down feature.
                /// Uses LIKE on wait_info to match sp_WhoIsActive's formatted wait string.
                /// </summary>
                public async Task<List<QuerySnapshotItem>> GetQuerySnapshotsByWaitTypeAsync(
                    string waitType, int hoursBack = 1,
                    DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<QuerySnapshotItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    // Check if the view exists
                    string checkViewQuery = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT 1 FROM sys.views
                        WHERE name = 'query_snapshots'
                        AND schema_id = SCHEMA_ID('report')";

                    using var checkCommand = new SqlCommand(checkViewQuery, connection);
                    var viewExists = await checkCommand.ExecuteScalarAsync();

                    if (viewExists == null)
                        return items;

                    bool useCustomDates = fromDate.HasValue && toDate.HasValue;

                    // sp_WhoIsActive formats wait_info as "(1x: 349ms)LCK_M_X, (1x: 12ms)..."
                    // The ')' always precedes the wait type name, so we use '%)WAIT_TYPE%'
                    // to avoid false positives (e.g., LCK_M_X matching LCK_M_IX)
                    string query = useCustomDates
                        ? @"
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
                                sql_text = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_text), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
                                sql_command = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_command), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
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
                            FROM report.query_snapshots AS qs
                            WHERE qs.collection_time >= @from_date
                            AND   qs.collection_time <= @to_date
                            AND   CONVERT(nvarchar(max), qs.wait_info) LIKE N'%)' + @wait_type + N'%'
                            ORDER BY
                                qs.collection_time DESC,
                                qs.session_id;"
                        : @"
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
                                sql_text = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_text), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
                                sql_command = REPLACE(REPLACE(CONVERT(nvarchar(max), qs.sql_command), N'<?query --' + CHAR(13) + CHAR(10), N''), CHAR(13) + CHAR(10) + N'--?>', N''),
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
                            FROM report.query_snapshots AS qs
                            WHERE qs.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            AND   CONVERT(nvarchar(max), qs.wait_info) LIKE N'%)' + @wait_type + N'%'
                            ORDER BY
                                qs.collection_time DESC,
                                qs.session_id;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@wait_type", SqlDbType.NVarChar, 200) { Value = waitType });
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
                        });
                    }

                    return items;
                }

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
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der.sql_handle, der.plan_handle)) AS dest
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
                    GrantedQueryMemoryGb = reader.IsDBNull(16) ? 0m : Convert.ToDecimal(reader.GetValue(16)),
                    TransactionIsolationLevel = reader.IsDBNull(17) ? null : reader.GetString(17),
                    Dop = reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18)),
                    ParallelWorkerCount = reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19)),
                    LoginName = reader.IsDBNull(20) ? null : reader.GetString(20),
                    HostName = reader.IsDBNull(21) ? null : reader.GetString(21),
                    ProgramName = reader.IsDBNull(22) ? null : reader.GetString(22),
                    OpenTransactionCount = reader.IsDBNull(23) ? 0 : Convert.ToInt32(reader.GetValue(23)),
                    PercentComplete = reader.IsDBNull(24) ? 0m : Convert.ToDecimal(reader.GetValue(24))
                });
            }

            return items;
        }

    }
}
