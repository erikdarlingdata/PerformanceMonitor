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
    }
}
