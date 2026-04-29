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
        // Per-object execution history and trace pattern history data access.
        // ============================================

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
            query_hash = CONVERT(varchar(20), MAX(qsd.query_hash), 1),
            query_plan_hash = CONVERT(varchar(20), MAX(qsd.query_plan_hash), 1),
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
                            QueryHash = reader.IsDBNull(30) ? null : reader.GetString(30),
                            QueryPlanHash = reader.IsDBNull(31) ? null : reader.GetString(31),
                            PlanType = reader.IsDBNull(32) ? null : reader.GetString(32),
                            IsForcedPlan = reader.GetBoolean(33),
                            ForceFailureCount = reader.IsDBNull(34) ? null : reader.GetInt64(34),
                            LastForceFailureReasonDesc = reader.IsDBNull(35) ? null : reader.GetString(35),
                            PlanForcingType = reader.IsDBNull(36) ? null : reader.GetString(36),
                            CompatibilityLevel = reader.IsDBNull(37) ? null : reader.GetInt16(37)
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
            ps.sample_interval_seconds,
            sql_handle = CONVERT(varchar(130), ps.sql_handle, 1),
            plan_handle = CONVERT(varchar(130), ps.plan_handle, 1)
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
                            SampleIntervalSeconds = reader.IsDBNull(32) ? null : reader.GetInt32(32),
                            SqlHandle = reader.IsDBNull(33) ? null : reader.GetString(33),
                            PlanHandle = reader.IsDBNull(34) ? null : reader.GetString(34)
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
            ps.sample_interval_seconds,
            sql_handle = CONVERT(varchar(130), ps.sql_handle, 1),
            plan_handle = CONVERT(varchar(130), ps.plan_handle, 1)
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
                            SampleIntervalSeconds = reader.IsDBNull(32) ? null : reader.GetInt32(32),
                            SqlHandle = reader.IsDBNull(33) ? null : reader.GetString(33),
                            PlanHandle = reader.IsDBNull(34) ? null : reader.GetString(34)
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
            sample_interval_seconds = MAX(qs.sample_interval_seconds),
            sql_handle = CONVERT(varchar(130), MAX(qs.sql_handle), 1),
            plan_handle = CONVERT(varchar(130), MAX(qs.plan_handle), 1),
            query_hash = CONVERT(varchar(20), MAX(qs.query_hash), 1),
            query_plan_hash = CONVERT(varchar(20), MAX(qs.query_plan_hash), 1)
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
                            MinDop = Convert.ToInt16(reader.GetValue(22)),
                            MaxDop = Convert.ToInt16(reader.GetValue(23)),
                            MinGrantKb = reader.GetInt64(24),
                            MaxGrantKb = reader.GetInt64(25),
                            MinUsedGrantKb = reader.GetInt64(26),
                            MaxUsedGrantKb = reader.GetInt64(27),
                            MinIdealGrantKb = reader.GetInt64(28),
                            MaxIdealGrantKb = reader.GetInt64(29),
                            MinReservedThreads = Convert.ToInt32(reader.GetValue(30)),
                            MaxReservedThreads = Convert.ToInt32(reader.GetValue(31)),
                            MinUsedThreads = Convert.ToInt32(reader.GetValue(32)),
                            MaxUsedThreads = Convert.ToInt32(reader.GetValue(33)),
                            TotalSpills = reader.GetInt64(34),
                            MinSpills = reader.GetInt64(35),
                            MaxSpills = reader.GetInt64(36),
                            ExecutionCountDelta = reader.IsDBNull(37) ? null : reader.GetInt64(37),
                            TotalWorkerTimeDelta = reader.IsDBNull(38) ? null : reader.GetInt64(38),
                            TotalElapsedTimeDelta = reader.IsDBNull(39) ? null : reader.GetInt64(39),
                            TotalLogicalReadsDelta = reader.IsDBNull(40) ? null : reader.GetInt64(40),
                            TotalPhysicalReadsDelta = reader.IsDBNull(41) ? null : reader.GetInt64(41),
                            TotalLogicalWritesDelta = reader.IsDBNull(42) ? null : reader.GetInt64(42),
                            SampleIntervalSeconds = reader.IsDBNull(43) ? null : reader.GetInt32(43),
                            SqlHandle = reader.IsDBNull(44) ? null : reader.GetString(44),
                            PlanHandle = reader.IsDBNull(45) ? null : reader.GetString(45),
                            QueryHash = reader.IsDBNull(46) ? null : reader.GetString(46),
                            QueryPlanHash = reader.IsDBNull(47) ? null : reader.GetString(47)
                        });
                    }

                    return items;
                }

                public async Task<List<TracePatternDetailItem>> GetTracePatternHistoryAsync(string databaseName, string queryPattern, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TracePatternDetailItem>();

                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;

                    string timeFilter = fromDate.HasValue && toDate.HasValue
                        ? "ta.end_time >= @from_date AND ta.end_time <= @to_date"
                        : "ta.end_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())";

                    /* Trace events can appear in multiple collection cycles because the trace file
                       retains events until it rolls over. Deduplicate by partitioning on the event's
                       natural key (end_time + duration + cpu + reads) and keeping only the first row. */
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH
            numbered AS
        (
            SELECT
                ta.analysis_id,
                ta.collection_time,
                ta.event_name,
                ta.database_name,
                ta.login_name,
                ta.nt_user_name,
                ta.application_name,
                ta.host_name,
                ta.spid,
                ta.duration_ms,
                ta.cpu_ms,
                ta.reads,
                ta.writes,
                ta.row_counts,
                ta.start_time,
                ta.end_time,
                sql_text = LEFT(ta.sql_text, 4000),
                ta.object_id,
                rn = ROW_NUMBER() OVER
                (
                    PARTITION BY
                        ta.end_time,
                        ta.duration_ms,
                        ta.cpu_ms,
                        ta.reads,
                        ta.spid
                    ORDER BY
                        ta.collection_time
                )
            FROM collect.trace_analysis AS ta
            WHERE ta.database_name = @database_name
            AND   LEFT(ta.sql_text, 200) = @query_pattern
            AND   {timeFilter}
        )
        SELECT
            analysis_id,
            collection_time,
            event_name,
            database_name,
            login_name,
            nt_user_name,
            application_name,
            host_name,
            spid,
            duration_ms,
            cpu_ms,
            reads,
            writes,
            row_counts,
            start_time,
            end_time,
            sql_text,
            object_id
        FROM numbered
        WHERE rn = 1
        ORDER BY
            end_time DESC;";

                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
                    command.Parameters.Add(new SqlParameter("@query_pattern", SqlDbType.NVarChar, 200) { Value = queryPattern });
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new TracePatternDetailItem
                        {
                            AnalysisId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            EventName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                            DatabaseName = reader.IsDBNull(3) ? null : reader.GetString(3),
                            LoginName = reader.IsDBNull(4) ? null : reader.GetString(4),
                            NtUserName = reader.IsDBNull(5) ? null : reader.GetString(5),
                            ApplicationName = reader.IsDBNull(6) ? null : reader.GetString(6),
                            HostName = reader.IsDBNull(7) ? null : reader.GetString(7),
                            Spid = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                            DurationMs = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            CpuMs = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            Reads = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            Writes = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            RowCounts = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            StartTime = reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                            EndTime = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                            SqlText = reader.IsDBNull(16) ? null : reader.GetString(16),
                            ObjectId = reader.IsDBNull(17) ? null : reader.GetInt64(17)
                        });
                    }

                    return items;
                }

    }
}
