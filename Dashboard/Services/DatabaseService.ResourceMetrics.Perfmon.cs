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

                public async Task<List<PerfmonStatsItem>> GetPerfmonStatsAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<PerfmonStatsItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string dateFilter = fromDate.HasValue && toDate.HasValue
                        ? "WHERE ps.collection_time >= @fromDate AND ps.collection_time <= @toDate"
                        : "WHERE ps.collection_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";
        
                    string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ps.collection_id,
            ps.collection_time,
            ps.server_start_time,
            ps.object_name,
            ps.counter_name,
            ps.instance_name,
            ps.cntr_value,
            ps.cntr_type,
            ps.cntr_value_delta,
            ps.sample_interval_seconds,
            ps.cntr_value_per_second
        FROM collect.perfmon_stats AS ps
        {dateFilter}
        ORDER BY
            ps.collection_time DESC,
            ps.object_name,
            ps.counter_name;";
        
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
                        items.Add(new PerfmonStatsItem
                        {
                            CollectionId = reader.GetInt64(0),
                            CollectionTime = reader.GetDateTime(1),
                            ServerStartTime = reader.GetDateTime(2),
                            ObjectName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            CounterName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            InstanceName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                            CntrValue = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                            CntrType = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                            CntrValueDelta = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                            SampleIntervalSeconds = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            CntrValuePerSecond = reader.IsDBNull(10) ? null : reader.GetInt64(10)
                        });
                    }
        
                    return items;
                }

        /// <summary>
        /// Gets perfmon stats filtered to specific counter names.
        /// Used by Server Trends (4 counters) and Perfmon Counters tab (user-selected counters).
        /// </summary>
        public async Task<List<PerfmonStatsItem>> GetPerfmonStatsFilteredAsync(string[] counterNames, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<PerfmonStatsItem>();
            if (counterNames == null || counterNames.Length == 0)
                return items;

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "WHERE ps.collection_time >= @fromDate AND ps.collection_time <= @toDate"
                : "WHERE ps.collection_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";

            var counterParams = new List<string>();
            for (int i = 0; i < counterNames.Length; i++)
                counterParams.Add($"@counter{i}");

            string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT
            ps.collection_id,
            ps.collection_time,
            ps.server_start_time,
            ps.object_name,
            ps.counter_name,
            ps.instance_name,
            ps.cntr_value,
            ps.cntr_type,
            ps.cntr_value_delta,
            ps.sample_interval_seconds,
            ps.cntr_value_per_second
        FROM collect.perfmon_stats AS ps
        {dateFilter}
        AND   ps.counter_name IN ({string.Join(", ", counterParams)})
        ORDER BY
            ps.collection_time DESC,
            ps.object_name,
            ps.counter_name;";

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

            for (int i = 0; i < counterNames.Length; i++)
                command.Parameters.Add(new SqlParameter($"@counter{i}", SqlDbType.NVarChar, 256) { Value = counterNames[i] });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new PerfmonStatsItem
                {
                    CollectionId = reader.GetInt64(0),
                    CollectionTime = reader.GetDateTime(1),
                    ServerStartTime = reader.GetDateTime(2),
                    ObjectName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                    CounterName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                    InstanceName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                    CntrValue = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    CntrType = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    CntrValueDelta = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                    SampleIntervalSeconds = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                    CntrValuePerSecond = reader.IsDBNull(10) ? null : reader.GetInt64(10)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets distinct perfmon counter names for the counter picker UI.
        /// Lightweight query that returns only unique (object_name, counter_name) pairs.
        /// </summary>
        public async Task<List<(string ObjectName, string CounterName)>> GetPerfmonCounterNamesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<(string ObjectName, string CounterName)>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "WHERE ps.collection_time >= @fromDate AND ps.collection_time <= @toDate"
                : "WHERE ps.collection_time >= DATEADD(HOUR, -@hoursBack, GETDATE())";

            string query = $@"
        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

        SELECT DISTINCT
            ps.object_name,
            ps.counter_name
        FROM collect.perfmon_stats AS ps
        {dateFilter}
        ORDER BY
            ps.object_name,
            ps.counter_name;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;

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
                items.Add((
                    reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1)
                ));
            }

            return items;
        }
    }
}
