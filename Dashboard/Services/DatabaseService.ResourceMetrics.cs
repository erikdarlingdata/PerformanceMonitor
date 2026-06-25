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
        // Resource Metrics Data Access
        // ============================================

                public async Task<List<TempdbStatsItem>> GetTempdbStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<TempdbStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ts.collection_time >= @fromDate AND ts.collection_time <= @toDate"
                        : "WHERE ts.collection_time >= DATEADD(HOUR, -@hoursBack, SYSDATETIME())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ts.collection_id,
            ts.collection_time,
            ts.user_object_reserved_page_count,
            ts.internal_object_reserved_page_count,
            ts.version_store_reserved_page_count,
            ts.mixed_extent_page_count,
            ts.unallocated_extent_page_count,
            ts.top_task_user_objects_mb,
            ts.top_task_internal_objects_mb,
            ts.top_task_total_mb,
            ts.top_task_session_id,
            ts.top_task_request_id,
            ts.total_sessions_using_tempdb,
            ts.sessions_with_user_objects,
            ts.sessions_with_internal_objects,
            ts.version_store_high_warning,
            ts.allocation_contention_warning,
            version_store_percent =
                CASE
                    WHEN (ts.user_object_reserved_page_count + ts.internal_object_reserved_page_count + ts.version_store_reserved_page_count) > 0
                    THEN CONVERT(decimal(5,2), ts.version_store_reserved_page_count * 100.0 /
                        (ts.user_object_reserved_page_count + ts.internal_object_reserved_page_count + ts.version_store_reserved_page_count))
                    ELSE 0
                END,
            pressure_level =
                CASE
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 5000 THEN N'CRITICAL - Version store > 5GB'
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 2000 THEN N'HIGH - Version store > 2GB'
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 1000 THEN N'MEDIUM - Version store > 1GB'
                    WHEN ts.unallocated_extent_page_count * 8 / 1024 < 100 THEN N'MEDIUM - Low free space'
                    ELSE N'NORMAL'
                END,
            recommendation =
                CASE
                    WHEN ts.version_store_reserved_page_count * 8 / 1024 > 1000 THEN N'Check for long-running transactions, snapshot isolation'
                    WHEN ts.unallocated_extent_page_count * 8 / 1024 < 100 THEN N'Consider increasing TempDB file sizes'
                    WHEN ts.internal_object_reserved_page_count > ts.user_object_reserved_page_count * 2 THEN N'High internal usage - check sorts/hash ops'
                    ELSE N'TempDB usage is within normal range'
                END
        FROM collect.tempdb_stats AS ts
        {dateFilter}
        ORDER BY
            ts.collection_time DESC;";
        
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
                        items.Add(new TempdbStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            UserObjectReservedPageCount = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                            InternalObjectReservedPageCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                            VersionStoreReservedPageCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                            MixedExtentPageCount = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                            UnallocatedExtentPageCount = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            TopTaskUserObjectsMb = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            TopTaskInternalObjectsMb = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                            TopTaskTotalMb = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            TopTaskSessionId = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                            TopTaskRequestId = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                            TotalSessionsUsingTempdb = reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                            SessionsWithUserObjects = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                            SessionsWithInternalObjects = reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                            VersionStoreHighWarning = reader.IsDBNull(15) ? false : reader.GetBoolean(15),
                            AllocationContentionWarning = reader.IsDBNull(16) ? false : reader.GetBoolean(16),
                            VersionStorePercent = reader.IsDBNull(17) ? null : reader.GetDecimal(17),
                            PressureLevel = reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
                            Recommendation = reader.IsDBNull(19) ? string.Empty : reader.GetString(19)
                        });
                    }
        
                    return items;
                }
    }
}
