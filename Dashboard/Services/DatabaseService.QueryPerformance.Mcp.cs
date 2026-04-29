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
        // MCP-optimized query/procedure/Query Store data access (multi-phase temp tables).
        // ============================================

        /// <summary>
        /// MCP-optimized query stats: aggregate numerics first, rank TOP N, then hydrate text.
        /// </summary>
        public async Task<List<QueryStatsItem>> GetQueryStatsForMcpAsync(
            int hoursBack, int top, string? databaseName = null,
            bool parallelOnly = false, int minDop = 0)
        {
            var items = new List<QueryStatsItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/*Phase 1: aggregate per-lifetime — numeric only, no DECOMPRESS*/
DROP TABLE IF EXISTS #per_lifetime;

SELECT
    qs.database_name,
    qs.query_hash,
    qs.creation_time,
    object_type = MAX(qs.object_type),
    schema_name = MAX(qs.schema_name),
    object_name = MAX(qs.object_name),
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
    query_plan_hash = MAX(qs.query_plan_hash),
    sql_handle = MAX(qs.sql_handle),
    plan_handle = MAX(qs.plan_handle)
INTO #per_lifetime
FROM collect.query_stats AS qs
WHERE qs.last_execution_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
AND   (@databaseName IS NULL OR qs.database_name = @databaseName)
GROUP BY
    qs.database_name,
    qs.query_hash,
    qs.creation_time
OPTION
(
    RECOMPILE,
    HASH GROUP,
    USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')
);

/*Phase 2: sum across lifetimes, rank, take TOP N+5*/
DROP TABLE IF EXISTS #top_ranked;

SELECT TOP (@top + 5)
    database_name = pl.database_name,
    query_hash = pl.query_hash,
    object_type = MAX(pl.object_type),
    object_name =
        CASE MAX(pl.object_type)
            WHEN 'STATEMENT'
            THEN N'Adhoc'
            ELSE QUOTENAME(MAX(pl.schema_name)) + N'.' + QUOTENAME(MAX(pl.object_name))
        END,
    first_execution_time = MIN(pl.creation_time),
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
    min_physical_reads = MIN(pl.min_physical_reads),
    max_physical_reads = MAX(pl.max_physical_reads),
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
    query_plan_hash = CONVERT(nvarchar(20), MAX(pl.query_plan_hash), 1),
    sql_handle = CONVERT(nvarchar(130), MAX(pl.sql_handle), 1),
    plan_handle = CONVERT(nvarchar(130), MAX(pl.plan_handle), 1)
INTO #top_ranked
FROM #per_lifetime AS pl
WHERE (@parallelOnly = 0 OR pl.max_dop > 1)
AND   (@minDop = 0 OR pl.max_dop >= @minDop)
GROUP BY
    pl.database_name,
    pl.query_hash
ORDER BY
    avg_worker_time_ms DESC
OPTION
(
    RECOMPILE,
    HASH GROUP
);

/*Phase 3: hydrate text for winners only, apply WAITFOR filter*/
SELECT TOP (@top)
    tr.database_name,
    query_hash = CONVERT(nvarchar(20), tr.query_hash, 1),
    tr.object_type,
    tr.object_name,
    tr.first_execution_time,
    tr.last_execution_time,
    tr.execution_count,
    tr.total_worker_time,
    tr.avg_worker_time_ms,
    tr.min_worker_time_ms,
    tr.max_worker_time_ms,
    tr.total_elapsed_time,
    tr.avg_elapsed_time_ms,
    tr.min_elapsed_time_ms,
    tr.max_elapsed_time_ms,
    tr.total_logical_reads,
    tr.avg_logical_reads,
    tr.total_logical_writes,
    tr.avg_logical_writes,
    tr.total_physical_reads,
    tr.avg_physical_reads,
    tr.min_physical_reads,
    tr.max_physical_reads,
    tr.total_rows,
    tr.avg_rows,
    tr.min_rows,
    tr.max_rows,
    tr.min_dop,
    tr.max_dop,
    tr.min_grant_kb,
    tr.max_grant_kb,
    tr.total_spills,
    tr.min_spills,
    tr.max_spills,
    tr.query_plan_hash,
    tr.sql_handle,
    tr.plan_handle,
    qt.query_text
FROM #top_ranked AS tr
OUTER APPLY
(
    SELECT TOP (1)
        query_text = CAST(DECOMPRESS(qs2.query_text) AS nvarchar(max))
    FROM collect.query_stats AS qs2
    WHERE qs2.query_hash = tr.query_hash
    AND   qs2.database_name = tr.database_name
    ORDER BY qs2.collection_time DESC
) AS qt
WHERE qt.query_text IS NULL
OR    qt.query_text NOT LIKE N'WAITFOR%'
ORDER BY
    tr.avg_worker_time_ms DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            command.Parameters.Add(new SqlParameter("@top", SqlDbType.Int) { Value = top });
            command.Parameters.Add(new SqlParameter("@databaseName", SqlDbType.NVarChar, 128) { Value = (object?)databaseName ?? DBNull.Value });
            command.Parameters.Add(new SqlParameter("@parallelOnly", SqlDbType.Bit) { Value = parallelOnly });
            command.Parameters.Add(new SqlParameter("@minDop", SqlDbType.Int) { Value = minDop });

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
                    MinPhysicalReads = reader.IsDBNull(21) ? null : reader.GetInt64(21),
                    MaxPhysicalReads = reader.IsDBNull(22) ? null : reader.GetInt64(22),
                    TotalRows = reader.IsDBNull(23) ? 0 : reader.GetInt64(23),
                    AvgRows = reader.IsDBNull(24) ? null : reader.GetInt64(24),
                    MinRows = reader.IsDBNull(25) ? null : reader.GetInt64(25),
                    MaxRows = reader.IsDBNull(26) ? null : reader.GetInt64(26),
                    MinDop = reader.IsDBNull(27) ? null : Convert.ToInt16(reader.GetValue(27)),
                    MaxDop = reader.IsDBNull(28) ? null : Convert.ToInt16(reader.GetValue(28)),
                    MinGrantKb = reader.IsDBNull(29) ? null : reader.GetInt64(29),
                    MaxGrantKb = reader.IsDBNull(30) ? null : reader.GetInt64(30),
                    TotalSpills = reader.IsDBNull(31) ? 0 : reader.GetInt64(31),
                    MinSpills = reader.IsDBNull(32) ? null : reader.GetInt64(32),
                    MaxSpills = reader.IsDBNull(33) ? null : reader.GetInt64(33),
                    QueryPlanHash = reader.IsDBNull(34) ? null : reader.GetString(34),
                    SqlHandle = reader.IsDBNull(35) ? null : reader.GetString(35),
                    PlanHandle = reader.IsDBNull(36) ? null : reader.GetString(36),
                    QueryText = reader.IsDBNull(37) ? null : reader.GetString(37),
                    QueryPlanXml = null
                });
            }

            return items;
        }

        /// <summary>
        /// MCP-optimized procedure stats: aggregate numerics first, rank TOP N.
        /// No text hydration needed — procedure names are sysname columns, not compressed.
        /// </summary>
        public async Task<List<ProcedureStatsItem>> GetProcedureStatsForMcpAsync(
            int hoursBack, int top, string? databaseName = null)
        {
            var items = new List<ProcedureStatsItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/*Phase 1: aggregate per-lifetime — numeric only, no DECOMPRESS*/
DROP TABLE IF EXISTS #per_lifetime;

SELECT
    ps.database_name,
    ps.schema_name,
    ps.object_name,
    ps.cached_time,
    object_id = MAX(ps.object_id),
    object_type = MAX(ps.object_type),
    type_desc = MAX(ps.type_desc),
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
    sql_handle = MAX(ps.sql_handle),
    plan_handle = MAX(ps.plan_handle)
INTO #per_lifetime
FROM collect.procedure_stats AS ps
WHERE ps.last_execution_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
AND   (@databaseName IS NULL OR ps.database_name = @databaseName)
GROUP BY
    ps.database_name,
    ps.schema_name,
    ps.object_name,
    ps.cached_time
OPTION
(
    RECOMPILE,
    HASH GROUP,
    USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')
);

/*Phase 2: sum across lifetimes, rank, return TOP N*/
SELECT TOP (@top)
    database_name = pl.database_name,
    object_id = MAX(pl.object_id),
    object_name = QUOTENAME(pl.schema_name) + N'.' + QUOTENAME(pl.object_name),
    schema_name = pl.schema_name,
    procedure_name = pl.object_name,
    object_type = MAX(pl.object_type),
    type_desc = MAX(pl.type_desc),
    first_cached_time = MIN(pl.cached_time),
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
    sql_handle = CONVERT(nvarchar(130), MAX(pl.sql_handle), 1),
    plan_handle = CONVERT(nvarchar(130), MAX(pl.plan_handle), 1)
FROM #per_lifetime AS pl
GROUP BY
    pl.database_name,
    pl.schema_name,
    pl.object_name
ORDER BY
    avg_worker_time_ms DESC
OPTION
(
    RECOMPILE,
    HASH GROUP
);";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            command.Parameters.Add(new SqlParameter("@top", SqlDbType.Int) { Value = top });
            command.Parameters.Add(new SqlParameter("@databaseName", SqlDbType.NVarChar, 128) { Value = (object?)databaseName ?? DBNull.Value });

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
                    TotalSpills = reader.IsDBNull(30) ? 0 : reader.GetInt64(30),
                    AvgSpills = reader.IsDBNull(31) ? null : reader.GetInt64(31),
                    MinSpills = reader.IsDBNull(32) ? null : reader.GetInt64(32),
                    MaxSpills = reader.IsDBNull(33) ? null : reader.GetInt64(33),
                    SqlHandle = reader.IsDBNull(34) ? null : reader.GetString(34),
                    PlanHandle = reader.IsDBNull(35) ? null : reader.GetString(35),
                    QueryPlanXml = null
                });
            }

            return items;
        }

        /// <summary>
        /// MCP-optimized Query Store: aggregate numerics first, rank TOP N, then hydrate text.
        /// </summary>
        public async Task<List<QueryStoreItem>> GetQueryStoreDataForMcpAsync(
            int hoursBack, int top, string? databaseName = null,
            bool parallelOnly = false, int minDop = 0)
        {
            var items = new List<QueryStoreItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

/*Phase 1: aggregate by (database_name, query_id) — numeric only, no DECOMPRESS*/
DROP TABLE IF EXISTS #top_qs;

SELECT TOP (@top + 5)
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
    query_plan_hash = CONVERT(nvarchar(20), MAX(qsd.query_plan_hash), 1),
    force_failure_count = SUM(qsd.force_failure_count),
    last_force_failure_reason_desc = MAX(qsd.last_force_failure_reason_desc),
    plan_forcing_type = MAX(qsd.plan_forcing_type),
    min_clr_time_ms = MIN(qsd.min_clr_time) / 1000.0,
    max_clr_time_ms = MAX(qsd.max_clr_time) / 1000.0,
    min_num_physical_io_reads = MIN(qsd.min_num_physical_io_reads),
    max_num_physical_io_reads = MAX(qsd.max_num_physical_io_reads),
    min_log_bytes_used = MIN(qsd.min_log_bytes_used),
    max_log_bytes_used = MAX(qsd.max_log_bytes_used)
INTO #top_qs
FROM collect.query_store_data AS qsd
WHERE qsd.server_last_execution_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())
AND   (@databaseName IS NULL OR qsd.database_name = @databaseName)
AND   (@parallelOnly = 0 OR qsd.max_dop > 1)
AND   (@minDop = 0 OR qsd.max_dop >= @minDop)
GROUP BY
    qsd.database_name,
    qsd.query_id
ORDER BY
    avg_cpu_time_ms DESC
OPTION
(
    RECOMPILE,
    HASH GROUP,
    HASH JOIN,
    USE HINT('ENABLE_PARALLEL_PLAN_PREFERENCE')
);

/*Phase 2: hydrate text for winners only, apply WAITFOR filter*/
SELECT TOP (@top)
    tq.database_name,
    tq.query_id,
    tq.execution_type_desc,
    tq.module_name,
    tq.first_execution_time,
    tq.last_execution_time,
    tq.execution_count,
    tq.plan_count,
    tq.avg_duration_ms,
    tq.min_duration_ms,
    tq.max_duration_ms,
    tq.avg_cpu_time_ms,
    tq.min_cpu_time_ms,
    tq.max_cpu_time_ms,
    tq.avg_logical_reads,
    tq.min_logical_reads,
    tq.max_logical_reads,
    tq.avg_logical_writes,
    tq.min_logical_writes,
    tq.max_logical_writes,
    tq.avg_physical_reads,
    tq.min_physical_reads,
    tq.max_physical_reads,
    tq.min_dop,
    tq.max_dop,
    tq.avg_memory_pages,
    tq.min_memory_pages,
    tq.max_memory_pages,
    tq.avg_rowcount,
    tq.min_rowcount,
    tq.max_rowcount,
    tq.avg_tempdb_pages,
    tq.min_tempdb_pages,
    tq.max_tempdb_pages,
    tq.plan_type,
    tq.is_forced_plan,
    tq.compatibility_level,
    tq.query_plan_hash,
    tq.force_failure_count,
    tq.last_force_failure_reason_desc,
    tq.plan_forcing_type,
    tq.min_clr_time_ms,
    tq.max_clr_time_ms,
    tq.min_num_physical_io_reads,
    tq.max_num_physical_io_reads,
    tq.min_log_bytes_used,
    tq.max_log_bytes_used,
    qt.query_sql_text
FROM #top_qs AS tq
OUTER APPLY
(
    SELECT TOP (1)
        query_sql_text = CAST(DECOMPRESS(qsd2.query_sql_text) AS nvarchar(max))
    FROM collect.query_store_data AS qsd2
    WHERE qsd2.database_name = tq.database_name
    AND   qsd2.query_id = tq.query_id
    ORDER BY qsd2.collection_time DESC
) AS qt
WHERE qt.query_sql_text IS NULL
OR    qt.query_sql_text NOT LIKE N'WAITFOR%'
ORDER BY
    tq.avg_cpu_time_ms DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            command.Parameters.Add(new SqlParameter("@hoursBack", SqlDbType.Int) { Value = hoursBack });
            command.Parameters.Add(new SqlParameter("@top", SqlDbType.Int) { Value = top });
            command.Parameters.Add(new SqlParameter("@databaseName", SqlDbType.NVarChar, 128) { Value = (object?)databaseName ?? DBNull.Value });
            command.Parameters.Add(new SqlParameter("@parallelOnly", SqlDbType.Bit) { Value = parallelOnly });
            command.Parameters.Add(new SqlParameter("@minDop", SqlDbType.Int) { Value = minDop });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new QueryStoreItem
                {
                    DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
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
                    QueryPlanHash = reader.IsDBNull(37) ? null : reader.GetString(37),
                    ForceFailureCount = reader.IsDBNull(38) ? null : reader.GetInt64(38),
                    LastForceFailureReasonDesc = reader.IsDBNull(39) ? null : reader.GetString(39),
                    PlanForcingType = reader.IsDBNull(40) ? null : reader.GetString(40),
                    MinClrTimeMs = reader.IsDBNull(41) ? null : Convert.ToDouble(reader.GetValue(41), CultureInfo.InvariantCulture),
                    MaxClrTimeMs = reader.IsDBNull(42) ? null : Convert.ToDouble(reader.GetValue(42), CultureInfo.InvariantCulture),
                    MinNumPhysicalIoReads = reader.IsDBNull(43) ? null : reader.GetInt64(43),
                    MaxNumPhysicalIoReads = reader.IsDBNull(44) ? null : reader.GetInt64(44),
                    MinLogBytesUsed = reader.IsDBNull(45) ? null : reader.GetInt64(45),
                    MaxLogBytesUsed = reader.IsDBNull(46) ? null : reader.GetInt64(46),
                    QuerySqlText = reader.IsDBNull(47) ? null : reader.GetString(47),
                    QueryPlanXml = null
                });
            }

            return items;
        }

    }
}
