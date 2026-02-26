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

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        // ============================================
        // Memory Analysis Data Access
        // ============================================

                public async Task<List<MemoryStatsItem>> GetMemoryStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<MemoryStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ms.collection_time >= @fromDate AND ms.collection_time <= @toDate"
                        : "WHERE ms.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ms.collection_id,
            ms.collection_time,
            ms.buffer_pool_mb,
            ms.plan_cache_mb,
            ms.other_memory_mb,
            ms.total_memory_mb,
            ms.physical_memory_in_use_mb,
            ms.available_physical_memory_mb,
            ms.memory_utilization_percentage,
            ms.buffer_pool_pressure_warning,
            ms.plan_cache_pressure_warning,
            ms.total_physical_memory_mb,
            ms.committed_target_memory_mb
        FROM collect.memory_stats AS ms
        {dateFilter}
        ORDER BY
            ms.collection_time DESC;";
        
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
                        items.Add(new MemoryStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            BufferPoolMb = reader.GetDecimal(2),
                            PlanCacheMb = reader.GetDecimal(3),
                            OtherMemoryMb = reader.GetDecimal(4),
                            TotalMemoryMb = reader.GetDecimal(5),
                            PhysicalMemoryInUseMb = reader.GetDecimal(6),
                            AvailablePhysicalMemoryMb = reader.GetDecimal(7),
                            MemoryUtilizationPercentage = reader.GetInt32(8),
                            BufferPoolPressureWarning = reader.GetBoolean(9),
                            PlanCachePressureWarning = reader.GetBoolean(10),
                            TotalPhysicalMemoryMb = reader.IsDBNull(11) ? null : reader.GetDecimal(11),
                            CommittedTargetMemoryMb = reader.IsDBNull(12) ? null : reader.GetDecimal(12)
                        });
                    }
        
                    return items;
                }

                public async Task<List<MemoryPressureEventItem>> GetMemoryPressureEventsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<MemoryPressureEventItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE mpe.collection_time >= @fromDate AND mpe.collection_time <= @toDate"
                        : "WHERE mpe.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            mpe.collection_id,
            mpe.collection_time,
            mpe.sample_time,
            mpe.memory_notification,
            mpe.memory_indicators_process,
            mpe.memory_indicators_system,
            severity =
                CASE
                    WHEN mpe.memory_indicators_process >= 3 OR mpe.memory_indicators_system >= 3
                    THEN N'HIGH'
                    WHEN mpe.memory_indicators_process >= 2 OR mpe.memory_indicators_system >= 2
                    THEN N'MEDIUM'
                    ELSE N'LOW'
                END
        FROM collect.memory_pressure_events AS mpe
        {dateFilter}
        ORDER BY
            mpe.sample_time DESC;";
        
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
                        items.Add(new MemoryPressureEventItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            SampleTime = reader.GetDateTime(2),
                            MemoryNotification = reader.GetString(3),
                            MemoryIndicatorsProcess = reader.GetInt32(4),
                            MemoryIndicatorsSystem = reader.GetInt32(5),
                            Severity = reader.GetString(6)
                        });
                    }
        
                    return items;
                }

                public async Task<List<MemoryGrantStatsItem>> GetMemoryGrantStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<MemoryGrantStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE mgs.collection_time >= @fromDate AND mgs.collection_time <= @toDate"
                        : "WHERE mgs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            mgs.collection_id,
            mgs.collection_time,
            mgs.server_start_time,
            mgs.resource_semaphore_id,
            mgs.pool_id,
            mgs.target_memory_mb,
            mgs.max_target_memory_mb,
            mgs.total_memory_mb,
            mgs.available_memory_mb,
            mgs.granted_memory_mb,
            mgs.used_memory_mb,
            mgs.grantee_count,
            mgs.waiter_count,
            mgs.timeout_error_count,
            mgs.forced_grant_count,
            mgs.timeout_error_count_delta,
            mgs.forced_grant_count_delta,
            mgs.sample_interval_seconds
        FROM collect.memory_grant_stats AS mgs
        {dateFilter}
        ORDER BY
            mgs.collection_time DESC;";
        
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
                        items.Add(new MemoryGrantStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.IsDBNull(2) ? DateTime.MinValue : reader.GetDateTime(2),
                            ResourceSemaphoreId = reader.GetInt16(3),
                            PoolId = reader.GetInt32(4),
                            TargetMemoryMb = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                            MaxTargetMemoryMb = reader.IsDBNull(6) ? null : reader.GetDecimal(6),
                            TotalMemoryMb = reader.IsDBNull(7) ? null : reader.GetDecimal(7),
                            AvailableMemoryMb = reader.IsDBNull(8) ? null : reader.GetDecimal(8),
                            GrantedMemoryMb = reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                            UsedMemoryMb = reader.IsDBNull(10) ? null : reader.GetDecimal(10),
                            GranteeCount = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                            WaiterCount = reader.IsDBNull(12) ? null : reader.GetInt32(12),
                            TimeoutErrorCount = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            ForcedGrantCount = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            TimeoutErrorCountDelta = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            ForcedGrantCountDelta = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                            SampleIntervalSeconds = reader.IsDBNull(17) ? null : reader.GetInt32(17)
                        });
                    }
        
                    return items;
                }

                public async Task<List<MemoryClerksItem>> GetMemoryClerksAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<MemoryClerksItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE mcs.collection_time >= @fromDate AND mcs.collection_time <= @toDate"
                        : "WHERE mcs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            mcs.collection_id,
            mcs.collection_time,
            mcs.clerk_type,
            mcs.memory_node_id,
            mcs.pages_kb,
            mcs.virtual_memory_reserved_kb,
            mcs.virtual_memory_committed_kb,
            mcs.awe_allocated_kb,
            mcs.shared_memory_reserved_kb,
            mcs.shared_memory_committed_kb,
            mcs.pages_kb_delta,
            mcs.virtual_memory_reserved_kb_delta,
            mcs.virtual_memory_committed_kb_delta,
            mcs.awe_allocated_kb_delta,
            mcs.shared_memory_reserved_kb_delta,
            mcs.shared_memory_committed_kb_delta,
            mcs.sample_interval_seconds,
            percent_of_total = CONVERT(decimal(5,2), 0), /* Calculated at display time */
            concern_level =
                CASE
                    WHEN mcs.clerk_type = N'MEMORYCLERK_SQLBUFFERPOOL' THEN N'NORMAL'
                    WHEN mcs.clerk_type IN (N'CACHESTORE_SQLCP', N'CACHESTORE_OBJCP') AND mcs.pages_kb / 1024 > 8192 THEN N'REVIEW - Possible plan cache bloat'
                    WHEN mcs.clerk_type = N'OBJECTSTORE_LOCK_MANAGER' AND mcs.pages_kb / 1024 > 1024 THEN N'REVIEW - Heavy lock activity'
                    WHEN mcs.clerk_type = N'MEMORYCLERK_SQLQUERYEXEC' AND mcs.pages_kb / 1024 > 5120 THEN N'REVIEW - Large query execution memory'
                    WHEN mcs.clerk_type = N'USERSTORE_TOKENPERM' AND mcs.pages_kb / 1024 > 1024 THEN N'REVIEW - Large token cache'
                    WHEN mcs.clerk_type LIKE N'%COLUMNSTORE%' THEN N'NORMAL'
                    WHEN mcs.clerk_type LIKE N'%XTP%' THEN N'NORMAL'
                    WHEN mcs.pages_kb / 1024 > 2048 AND mcs.clerk_type NOT IN (N'MEMORYCLERK_SQLBUFFERPOOL', N'MEMORYCLERK_SQLGENERAL', N'CACHESTORE_SQLCP', N'CACHESTORE_OBJCP') THEN N'MONITOR - Unusually large'
                    ELSE N'NORMAL'
                END,
            clerk_description =
                CASE mcs.clerk_type
                    WHEN N'MEMORYCLERK_SQLBUFFERPOOL' THEN N'Data and index pages cache (Buffer Pool).'
                    WHEN N'MEMORYCLERK_SQLGENERAL' THEN N'Multiple consumers: replication, diagnostics, parser.'
                    WHEN N'MEMORYCLERK_SQLQUERYEXEC' THEN N'Batch mode, parallel query, sort and hash operations.'
                    WHEN N'MEMORYCLERK_SQLQERESERVATIONS' THEN N'Memory Grant allocations for sort and hash.'
                    WHEN N'CACHESTORE_SQLCP' THEN N'Ad hoc queries and prepared statements in plan cache.'
                    WHEN N'CACHESTORE_OBJCP' THEN N'Stored procedures, functions, triggers in plan cache.'
                    WHEN N'OBJECTSTORE_LOCK_MANAGER' THEN N'Lock Manager allocations.'
                    WHEN N'USERSTORE_TOKENPERM' THEN N'Security tokens and permission cache.'
                    WHEN N'MEMORYCLERK_QUERYDISKSTORE' THEN N'Query Store memory allocations.'
                    ELSE N'See documentation for clerk type details.'
                END
        FROM collect.memory_clerks_stats AS mcs
        {dateFilter}
        ORDER BY
            mcs.collection_time DESC,
            mcs.pages_kb DESC;";
        
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
                        items.Add(new MemoryClerksItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ClerkType = reader.GetString(2),
                            MemoryNodeId = reader.GetInt16(3),
                            PagesKb = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                            VirtualMemoryReservedKb = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                            VirtualMemoryCommittedKb = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                            AweAllocatedKb = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                            SharedMemoryReservedKb = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            SharedMemoryCommittedKb = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                            PagesKbDelta = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                            VirtualMemoryReservedKbDelta = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                            VirtualMemoryCommittedKbDelta = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                            AweAllocatedKbDelta = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                            SharedMemoryReservedKbDelta = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                            SharedMemoryCommittedKbDelta = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                            SampleIntervalSeconds = reader.IsDBNull(16) ? null : reader.GetInt32(16),
                            PercentOfTotal = reader.IsDBNull(17) ? null : reader.GetDecimal(17),
                            ConcernLevel = reader.GetString(18),
                            ClerkDescription = reader.GetString(19)
                        });
                    }
        
                    return items;
                }

        /// <summary>
        /// Gets memory clerk stats filtered to only the top N clerk types by total pages.
        /// Reduces row count from ~8.5K to ~1.4K for chart display.
        /// </summary>
        public async Task<List<MemoryClerksItem>> GetMemoryClerksTopNAsync(int topN = 5, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<MemoryClerksItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "mcs.collection_time >= @fromDate AND mcs.collection_time <= @toDate"
                : "mcs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";

            string query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        WITH top_clerks AS
        (
            SELECT TOP (@topN)
                mcs.clerk_type
            FROM collect.memory_clerks_stats AS mcs
            WHERE {dateFilter}
            GROUP BY
                mcs.clerk_type
            ORDER BY
                SUM(mcs.pages_kb) DESC
        )
        SELECT
            mcs.collection_id,
            mcs.collection_time,
            mcs.clerk_type,
            mcs.memory_node_id,
            mcs.pages_kb,
            mcs.virtual_memory_reserved_kb,
            mcs.virtual_memory_committed_kb,
            mcs.awe_allocated_kb,
            mcs.shared_memory_reserved_kb,
            mcs.shared_memory_committed_kb,
            mcs.pages_kb_delta,
            mcs.virtual_memory_reserved_kb_delta,
            mcs.virtual_memory_committed_kb_delta,
            mcs.awe_allocated_kb_delta,
            mcs.shared_memory_reserved_kb_delta,
            mcs.shared_memory_committed_kb_delta,
            mcs.sample_interval_seconds,
            percent_of_total = CONVERT(decimal(5,2), 0),
            concern_level =
                CASE
                    WHEN mcs.clerk_type = N'MEMORYCLERK_SQLBUFFERPOOL' THEN N'NORMAL'
                    WHEN mcs.clerk_type IN (N'CACHESTORE_SQLCP', N'CACHESTORE_OBJCP') AND mcs.pages_kb / 1024 > 8192 THEN N'REVIEW - Possible plan cache bloat'
                    WHEN mcs.clerk_type = N'OBJECTSTORE_LOCK_MANAGER' AND mcs.pages_kb / 1024 > 1024 THEN N'REVIEW - Heavy lock activity'
                    WHEN mcs.clerk_type = N'MEMORYCLERK_SQLQUERYEXEC' AND mcs.pages_kb / 1024 > 5120 THEN N'REVIEW - Large query execution memory'
                    WHEN mcs.clerk_type = N'USERSTORE_TOKENPERM' AND mcs.pages_kb / 1024 > 1024 THEN N'REVIEW - Large token cache'
                    WHEN mcs.clerk_type LIKE N'%COLUMNSTORE%' THEN N'NORMAL'
                    WHEN mcs.clerk_type LIKE N'%XTP%' THEN N'NORMAL'
                    WHEN mcs.pages_kb / 1024 > 2048 AND mcs.clerk_type NOT IN (N'MEMORYCLERK_SQLBUFFERPOOL', N'MEMORYCLERK_SQLGENERAL', N'CACHESTORE_SQLCP', N'CACHESTORE_OBJCP') THEN N'MONITOR - Unusually large'
                    ELSE N'NORMAL'
                END,
            clerk_description =
                CASE mcs.clerk_type
                    WHEN N'MEMORYCLERK_SQLBUFFERPOOL' THEN N'Data and index pages cache (Buffer Pool).'
                    WHEN N'MEMORYCLERK_SQLGENERAL' THEN N'Multiple consumers: replication, diagnostics, parser.'
                    WHEN N'MEMORYCLERK_SQLQUERYEXEC' THEN N'Batch mode, parallel query, sort and hash operations.'
                    WHEN N'MEMORYCLERK_SQLQERESERVATIONS' THEN N'Memory Grant allocations for sort and hash.'
                    WHEN N'CACHESTORE_SQLCP' THEN N'Ad hoc queries and prepared statements in plan cache.'
                    WHEN N'CACHESTORE_OBJCP' THEN N'Stored procedures, functions, triggers in plan cache.'
                    WHEN N'OBJECTSTORE_LOCK_MANAGER' THEN N'Lock Manager allocations.'
                    WHEN N'USERSTORE_TOKENPERM' THEN N'Security tokens and permission cache.'
                    WHEN N'MEMORYCLERK_QUERYDISKSTORE' THEN N'Query Store memory allocations.'
                    ELSE N'See documentation for clerk type details.'
                END
        FROM collect.memory_clerks_stats AS mcs
        WHERE {dateFilter}
        AND   mcs.clerk_type IN (SELECT tc.clerk_type FROM top_clerks AS tc)
        ORDER BY
            mcs.collection_time DESC,
            mcs.pages_kb DESC;";

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
                items.Add(new MemoryClerksItem
                {
                    CollectionId = reader.GetInt64(0),
                    CollectionTime = reader.GetDateTime(1),
                    ClerkType = reader.GetString(2),
                    MemoryNodeId = reader.GetInt16(3),
                    PagesKb = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    VirtualMemoryReservedKb = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    VirtualMemoryCommittedKb = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    AweAllocatedKb = reader.IsDBNull(7) ? null : reader.GetInt64(7),
                    SharedMemoryReservedKb = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    SharedMemoryCommittedKb = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                    PagesKbDelta = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                    VirtualMemoryReservedKbDelta = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                    VirtualMemoryCommittedKbDelta = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                    AweAllocatedKbDelta = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                    SharedMemoryReservedKbDelta = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                    SharedMemoryCommittedKbDelta = reader.IsDBNull(15) ? null : reader.GetInt64(15),
                    SampleIntervalSeconds = reader.IsDBNull(16) ? null : reader.GetInt32(16),
                    PercentOfTotal = reader.IsDBNull(17) ? null : reader.GetDecimal(17),
                    ConcernLevel = reader.GetString(18),
                    ClerkDescription = reader.GetString(19)
                });
            }

            return items;
        }

                public async Task<List<PlanCacheStatsItem>> GetPlanCacheStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<PlanCacheStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE pcs.collection_time >= @fromDate AND pcs.collection_time <= @toDate"
                        : "WHERE pcs.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            pcs.collection_id,
            pcs.collection_time,
            pcs.cacheobjtype,
            pcs.objtype,
            pcs.total_plans,
            pcs.total_size_mb,
            pcs.single_use_plans,
            pcs.single_use_size_mb,
            pcs.multi_use_plans,
            pcs.multi_use_size_mb,
            pcs.avg_use_count,
            pcs.avg_size_kb,
            pcs.oldest_plan_create_time,
            bloat_level =
                CASE
                    WHEN pcs.total_plans > 0 AND pcs.single_use_plans * 100.0 / pcs.total_plans > 50 THEN N'CRITICAL'
                    WHEN pcs.total_plans > 0 AND pcs.single_use_plans * 100.0 / pcs.total_plans > 30 THEN N'HIGH'
                    WHEN pcs.total_plans > 0 AND pcs.single_use_plans * 100.0 / pcs.total_plans > 20 THEN N'MEDIUM'
                    ELSE N'NORMAL'
                END,
            recommendation =
                CASE
                    WHEN pcs.total_plans > 0 AND pcs.single_use_plans * 100.0 / pcs.total_plans > 20
                    THEN N'Check for unparameterized queries, consider Forced Parameterization'
                    ELSE N'Plan cache composition is healthy'
                END
        FROM collect.plan_cache_stats AS pcs
        {dateFilter}
        ORDER BY
            pcs.collection_time DESC,
            pcs.total_size_mb DESC;";
        
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
                        items.Add(new PlanCacheStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            CacheObjType = reader.GetString(2),
                            ObjType = reader.GetString(3),
                            TotalPlans = reader.GetInt32(4),
                            TotalSizeMb = reader.GetInt32(5),
                            SingleUsePlans = reader.GetInt32(6),
                            SingleUseSizeMb = reader.GetInt32(7),
                            MultiUsePlans = reader.GetInt32(8),
                            MultiUseSizeMb = reader.GetInt32(9),
                            AvgUseCount = reader.GetDecimal(10),
                            AvgSizeKb = reader.GetInt32(11),
                            OldestPlanCreateTime = reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                            BloatLevel = reader.GetString(13),
                            Recommendation = reader.GetString(14)
                        });
                    }
        
                    return items;
                }
    }
}
