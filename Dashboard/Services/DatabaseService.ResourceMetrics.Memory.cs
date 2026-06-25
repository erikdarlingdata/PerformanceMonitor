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

                public async Task<MemoryPressureItem?> GetMemoryPressureAsync()
                {
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            collection_time,
                            active_grants,
                            queries_waiting,
                            available_memory_mb,
                            granted_memory_mb,
                            used_memory_mb,
                            memory_utilization_percent,
                            timeout_errors,
                            forced_grants,
                            pressure_level,
                            recommendation
                        FROM report.memory_grant_pressure;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        return new MemoryPressureItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            ActiveGrants = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                            QueriesWaiting = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                            AvailableMemoryMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                            GrantedMemoryMb = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture),
                            UsedMemoryMb = reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture),
                            MemoryUtilizationPercent = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                            TimeoutErrors = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
                            ForcedGrants = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture),
                            PressureLevel = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
                            Recommendation = reader.IsDBNull(10) ? string.Empty : reader.GetString(10)
                        };
                    }

                    return null;
                }

                public async Task<List<MemoryDataPoint>> GetMemoryDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<MemoryDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                ms.collection_time,
                                ms.buffer_pool_mb,
                                ms.plan_cache_mb,
                                ms.physical_memory_in_use_mb,
                                ms.available_physical_memory_mb,
                                ms.memory_utilization_percentage,
                                ms.total_memory_mb,
                                granted_memory_mb = ISNULL(
                                    (
                                        SELECT TOP (1)
                                            SUM(mgs.granted_memory_mb)
                                        FROM collect.memory_grant_stats AS mgs
                                        WHERE mgs.collection_time >= DATEADD(MINUTE, -5, ms.collection_time)
                                        AND   mgs.collection_time <= DATEADD(MINUTE, 5, ms.collection_time)
                                        GROUP BY
                                            mgs.collection_time
                                        ORDER BY
                                            ABS(DATEDIFF(SECOND, mgs.collection_time, ms.collection_time)) ASC
                                    ), 0)
                            FROM collect.memory_stats AS ms
                            WHERE ms.collection_time >= @from_date
                            AND   ms.collection_time <= @to_date
                            ORDER BY
                                ms.collection_time ASC;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            SELECT
                                ms.collection_time,
                                ms.buffer_pool_mb,
                                ms.plan_cache_mb,
                                ms.physical_memory_in_use_mb,
                                ms.available_physical_memory_mb,
                                ms.memory_utilization_percentage,
                                ms.total_memory_mb,
                                granted_memory_mb = ISNULL(
                                    (
                                        SELECT TOP (1)
                                            SUM(mgs.granted_memory_mb)
                                        FROM collect.memory_grant_stats AS mgs
                                        WHERE mgs.collection_time >= DATEADD(MINUTE, -5, ms.collection_time)
                                        AND   mgs.collection_time <= DATEADD(MINUTE, 5, ms.collection_time)
                                        GROUP BY
                                            mgs.collection_time
                                        ORDER BY
                                            ABS(DATEDIFF(SECOND, mgs.collection_time, ms.collection_time)) ASC
                                    ), 0)
                            FROM collect.memory_stats AS ms
                            WHERE ms.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                            ORDER BY
                                ms.collection_time ASC;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new MemoryDataPoint
                        {
                            CollectionTime = reader.GetDateTime(0),
                            BufferPoolMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1),
                            PlanCacheMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                            PhysicalMemoryInUseMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                            AvailablePhysicalMemoryMb = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            MemoryUtilizationPercentage = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                            TotalMemoryMb = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            GrantedMemoryMb = reader.IsDBNull(7) ? 0m : reader.GetDecimal(7)
                        });
                    }
        
                    return items;
                }

                public async Task<List<MemoryHistoryItem>> GetMemoryHistoryAsync(int hoursBack = 24)
                {
                    var items = new List<MemoryHistoryItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            collection_time,
                            buffer_pool_mb,
                            plan_cache_mb,
                            other_memory_mb,
                            total_memory_mb,
                            physical_memory_in_use_mb,
                            available_physical_memory_mb,
                            memory_utilization_percentage
                        FROM collect.memory_stats
                        WHERE collection_time >= DATEADD(HOUR, @HoursBack, SYSDATETIME())
                        ORDER BY collection_time ASC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.Parameters.Add(new SqlParameter("@HoursBack", SqlDbType.Int) { Value = -hoursBack });
                    command.CommandTimeout = 120;
        
                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new MemoryHistoryItem
                        {
                            CollectionTime = reader.GetDateTime(0),
                            BufferPoolMb = reader.IsDBNull(1) ? 0m : reader.GetDecimal(1),
                            PlanCacheMb = reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                            OtherMemoryMb = reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                            TotalMemoryMb = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                            PhysicalMemoryInUseMb = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5),
                            AvailablePhysicalMemoryMb = reader.IsDBNull(6) ? 0m : reader.GetDecimal(6),
                            MemoryUtilizationPercentage = reader.IsDBNull(7) ? 0 : reader.GetInt32(7)
                        });
                    }
        
                    return items;
                }
    }
}
