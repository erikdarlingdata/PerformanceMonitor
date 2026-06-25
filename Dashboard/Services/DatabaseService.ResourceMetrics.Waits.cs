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

                public async Task<List<WaitStatItem>> GetWaitStatsAsync()
                {
                    var items = new List<WaitStatItem>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query = @"
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                        SELECT
                            wait_type,
                            wait_time_ms,
                            wait_time_sec,
                            waiting_tasks,
                            signal_wait_ms,
                            resource_wait_ms,
                            avg_wait_ms_per_task,
                            last_seen
                        FROM report.top_waits_last_hour
                        ORDER BY
                            wait_time_ms DESC;";
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;

                    using (StartQueryTiming("Wait Stats", query, connection))
                    {
                        using var reader = await command.ExecuteReaderAsync();
                        while (await reader.ReadAsync())
                        {
                            items.Add(new WaitStatItem
                            {
                                WaitType = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                                WaitTimeMs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
                                WaitTimeSec = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                                WaitingTasks = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
                                SignalWaitMs = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                                ResourceWaitMs = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                                AvgWaitMsPerTask = reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture),
                                LastSeen = reader.IsDBNull(7) ? DateTime.MinValue : reader.GetDateTime(7)
                            });
                        }
                    }

                    return items;
                }

                public async Task<List<WaitStatsDataPoint>> GetWaitStatsDataAsync(int hoursBack = 24, int topWaitTypes = 5, DateTime? fromDate = null, DateTime? toDate = null)
                {
                    var items = new List<WaitStatsDataPoint>();
        
                    await using var tc = await OpenThrottledConnectionAsync();
                    var connection = tc.Connection;
        
                    string query;
                    if (fromDate.HasValue && toDate.HasValue)
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            WITH top_waits AS
                            (
                                SELECT TOP (@top_wait_types)
                                    wait_type
                                FROM collect.wait_stats
                                WHERE collection_time >= @from_date
                                AND   collection_time <= @to_date
                                GROUP BY
                                    wait_type
                                ORDER BY
                                    MAX(wait_time_ms) DESC
                            ),
                            wait_deltas AS
                            (
                                SELECT
                                    collection_time = ws.collection_time,
                                    wait_type = ws.wait_type,
                                    wait_time_ms_delta =
                                        ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
                                    signal_wait_time_ms_delta =
                                        ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
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
                                        ),
                                    waiting_tasks_count = ws.waiting_tasks_count
                                FROM collect.wait_stats AS ws
                                WHERE ws.collection_time >= @from_date
                                AND   ws.collection_time <= @to_date
                                AND   ws.wait_type IN (SELECT wait_type FROM top_waits)
                            )
                            SELECT
                                wd.collection_time,
                                wd.wait_type,
                                wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                        ELSE 0
                                    END,
                                signal_wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                        ELSE 0
                                    END,
                                wd.waiting_tasks_count
                            FROM wait_deltas AS wd
                            WHERE wd.wait_time_ms_delta >= 0
                            ORDER BY
                                wd.collection_time ASC,
                                wd.wait_type;";
                    }
                    else
                    {
                        query = @"
                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                            WITH top_waits AS
                            (
                                SELECT TOP (@top_wait_types)
                                    wait_type
                                FROM collect.wait_stats
                                WHERE collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                                GROUP BY
                                    wait_type
                                ORDER BY
                                    MAX(wait_time_ms) DESC
                            ),
                            wait_deltas AS
                            (
                                SELECT
                                    collection_time = ws.collection_time,
                                    wait_type = ws.wait_type,
                                    wait_time_ms_delta =
                                        ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
                                    signal_wait_time_ms_delta =
                                        ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
                                        (
                                            PARTITION BY
                                                ws.wait_type
                                            ORDER BY
                                                ws.collection_time
                                        ),
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
                                        ),
                                    waiting_tasks_count = ws.waiting_tasks_count
                                FROM collect.wait_stats AS ws
                                WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                                AND   ws.wait_type IN (SELECT wait_type FROM top_waits)
                            )
                            SELECT
                                wd.collection_time,
                                wd.wait_type,
                                wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                        ELSE 0
                                    END,
                                signal_wait_time_ms_per_second =
                                    CASE
                                        WHEN wd.interval_seconds > 0
                                        THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                        ELSE 0
                                    END,
                                wd.waiting_tasks_count
                            FROM wait_deltas AS wd
                            WHERE wd.wait_time_ms_delta >= 0
                            ORDER BY
                                wd.collection_time ASC,
                                wd.wait_type;";
                    }
        
                    using var command = new SqlCommand(query, connection);
                    command.CommandTimeout = 120;
                    command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
                    command.Parameters.Add(new SqlParameter("@top_wait_types", SqlDbType.Int) { Value = topWaitTypes });
                    if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
                    if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

                    using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        items.Add(new WaitStatsDataPoint
                        {
                            CollectionTime = reader.GetDateTime(0),
                            WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                            WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                            SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture)
                        });
                    }
        
                    return items;
                }

        /// <summary>
        /// Gets all wait stats data (all wait types with activity) for the Wait Stats selector.
        /// Unlike GetWaitStatsDataAsync which limits to top N wait types, this returns all wait types
        /// so users can select any wait types they want to correlate.
        /// </summary>
        public async Task<List<WaitStatsDataPoint>> GetAllWaitStatsDataAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<WaitStatsDataPoint>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
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
    WHERE ws.collection_time >= @from_date
    AND   ws.collection_time <= @to_date
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
            ELSE 0
        END,
    signal_wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
            ELSE 0
        END,
    avg_ms_per_wait =
        CASE
            WHEN wd.waiting_tasks_delta > 0
            THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta AS decimal(18, 4))
            ELSE 0
        END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY
    wd.collection_time,
    wd.wait_type;";
            }
            else
            {
                query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY
                    ws.wait_type
                ORDER BY
                    ws.collection_time
            ),
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
    WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
            ELSE 0
        END,
    signal_wait_time_ms_per_second =
        CASE
            WHEN wd.interval_seconds > 0
            THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
            ELSE 0
        END,
    avg_ms_per_wait =
        CASE
            WHEN wd.waiting_tasks_delta > 0
            THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta AS decimal(18, 4))
            ELSE 0
        END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY
    wd.collection_time,
    wd.wait_type;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120; // Longer timeout since this can return more data
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new WaitStatsDataPoint
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                    SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                    AvgMsPerWait = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets wait stats data filtered to specific wait types.
        /// Used by the Wait Stats Detail picker after the user selects which types to display.
        /// </summary>
        public async Task<List<WaitStatsDataPoint>> GetWaitStatsDataForTypesAsync(string[] waitTypes, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<WaitStatsDataPoint>();
            if (waitTypes == null || waitTypes.Length == 0)
                return items;

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            var typeParams = new List<string>();
            for (int i = 0; i < waitTypes.Length; i++)
                typeParams.Add($"@wt{i}");

            string typeFilter = $"AND ws.wait_type IN ({string.Join(", ", typeParams)})";

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        interval_seconds =
            DATEDIFF(SECOND, LAG(ws.collection_time, 1, ws.collection_time) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ), ws.collection_time)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= @from_date
    AND   ws.collection_time <= @to_date
    {typeFilter}
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
        ELSE 0 END,
    signal_wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
        ELSE 0 END,
    avg_ms_per_wait =
        CASE WHEN wd.waiting_tasks_delta > 0
        THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta AS decimal(18, 4))
        ELSE 0 END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY wd.collection_time, wd.wait_type;";
            }
            else
            {
                query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    wait_deltas AS
(
    SELECT
        collection_time = ws.collection_time,
        wait_type = ws.wait_type,
        wait_time_ms_delta =
            ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        signal_wait_time_ms_delta =
            ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        waiting_tasks_delta =
            ws.waiting_tasks_count - LAG(ws.waiting_tasks_count, 1, ws.waiting_tasks_count) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ),
        interval_seconds =
            DATEDIFF(SECOND, LAG(ws.collection_time, 1, ws.collection_time) OVER
            (
                PARTITION BY ws.wait_type
                ORDER BY ws.collection_time
            ), ws.collection_time)
    FROM collect.wait_stats AS ws
    WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
    {typeFilter}
)
SELECT
    wd.collection_time,
    wd.wait_type,
    wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
        ELSE 0 END,
    signal_wait_time_ms_per_second =
        CASE WHEN wd.interval_seconds > 0
        THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
        ELSE 0 END,
    avg_ms_per_wait =
        CASE WHEN wd.waiting_tasks_delta > 0
        THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.waiting_tasks_delta AS decimal(18, 4))
        ELSE 0 END
FROM wait_deltas AS wd
WHERE wd.wait_time_ms_delta > 0
ORDER BY wd.collection_time, wd.wait_type;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            for (int i = 0; i < waitTypes.Length; i++)
                command.Parameters.Add(new SqlParameter($"@wt{i}", SqlDbType.NVarChar, 256) { Value = waitTypes[i] });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new WaitStatsDataPoint
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                    SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture),
                    AvgMsPerWait = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }

        /// <summary>
        /// Gets distinct wait type names with total wait time for ranking in the picker UI.
        /// Lightweight alternative to GetAllWaitStatsDataAsync for populating the wait type selector.
        /// </summary>
        public async Task<List<(string WaitType, decimal TotalWaitTimeMsPerSecond)>> GetWaitTypeNamesAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<(string WaitType, decimal TotalWaitTimeMsPerSecond)>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string dateFilter = fromDate.HasValue && toDate.HasValue
                ? "ws.collection_time >= @from_date AND ws.collection_time <= @to_date"
                : "ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())";

            string query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    ws.wait_type,
    total_wait_time_ms_per_second = SUM(ws.wait_time_ms - LAG_val)
FROM
(
    SELECT
        ws.wait_type,
        LAG_val = LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
        (
            PARTITION BY
                ws.wait_type
            ORDER BY
                ws.collection_time
        ),
        ws.wait_time_ms
    FROM collect.wait_stats AS ws
    WHERE {dateFilter}
) AS ws
WHERE ws.wait_time_ms - ws.LAG_val > 0
GROUP BY
    ws.wait_type
ORDER BY
    SUM(ws.wait_time_ms - ws.LAG_val) DESC;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 30;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add((
                    reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                    reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture)
                ));
            }

            return items;
        }

        public async Task<List<WaitStatsDataPoint>> GetTotalWaitStatsTrendAsync(int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
        {
            var items = new List<WaitStatsDataPoint>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query;
            if (fromDate.HasValue && toDate.HasValue)
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    WITH wait_deltas AS
                    (
                        SELECT
                            collection_time = ws.collection_time,
                            wait_type = ws.wait_type,
                            wait_time_ms_delta =
                                ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
                                (
                                    PARTITION BY
                                        ws.wait_type
                                    ORDER BY
                                        ws.collection_time
                                ),
                            signal_wait_time_ms_delta =
                                ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
                                (
                                    PARTITION BY
                                        ws.wait_type
                                    ORDER BY
                                        ws.collection_time
                                ),
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
                        WHERE ws.collection_time >= @from_date
                        AND   ws.collection_time <= @to_date
                    )
                    SELECT
                        wd.collection_time,
                        wait_type = N'Total',
                        wait_time_ms_per_second =
                            SUM
                            (
                                CASE
                                    WHEN wd.interval_seconds > 0
                                    THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                    ELSE 0
                                END
                            ),
                        signal_wait_time_ms_per_second =
                            SUM
                            (
                                CASE
                                    WHEN wd.interval_seconds > 0
                                    THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                    ELSE 0
                                END
                            )
                    FROM wait_deltas AS wd
                    WHERE wd.wait_time_ms_delta >= 0
                    GROUP BY
                        wd.collection_time
                    ORDER BY
                        wd.collection_time ASC;";
            }
            else
            {
                query = @"
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                    WITH wait_deltas AS
                    (
                        SELECT
                            collection_time = ws.collection_time,
                            wait_type = ws.wait_type,
                            wait_time_ms_delta =
                                ws.wait_time_ms - LAG(ws.wait_time_ms, 1, ws.wait_time_ms) OVER
                                (
                                    PARTITION BY
                                        ws.wait_type
                                    ORDER BY
                                        ws.collection_time
                                ),
                            signal_wait_time_ms_delta =
                                ws.signal_wait_time_ms - LAG(ws.signal_wait_time_ms, 1, ws.signal_wait_time_ms) OVER
                                (
                                    PARTITION BY
                                        ws.wait_type
                                    ORDER BY
                                        ws.collection_time
                                ),
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
                        WHERE ws.collection_time >= DATEADD(HOUR, @hours_back, SYSDATETIME())
                    )
                    SELECT
                        wd.collection_time,
                        wait_type = N'Total',
                        wait_time_ms_per_second =
                            SUM
                            (
                                CASE
                                    WHEN wd.interval_seconds > 0
                                    THEN CAST(CAST(wd.wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                    ELSE 0
                                END
                            ),
                        signal_wait_time_ms_per_second =
                            SUM
                            (
                                CASE
                                    WHEN wd.interval_seconds > 0
                                    THEN CAST(CAST(wd.signal_wait_time_ms_delta AS decimal(19, 4)) / wd.interval_seconds AS decimal(18, 4))
                                    ELSE 0
                                END
                            )
                    FROM wait_deltas AS wd
                    WHERE wd.wait_time_ms_delta >= 0
                    GROUP BY
                        wd.collection_time
                    ORDER BY
                        wd.collection_time ASC;";
            }

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@hours_back", SqlDbType.Int) { Value = -hoursBack });
            if (fromDate.HasValue) command.Parameters.Add(new SqlParameter("@from_date", SqlDbType.DateTime2) { Value = fromDate.Value });
            if (toDate.HasValue) command.Parameters.Add(new SqlParameter("@to_date", SqlDbType.DateTime2) { Value = toDate.Value });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new WaitStatsDataPoint
                {
                    CollectionTime = reader.GetDateTime(0),
                    WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture),
                    SignalWaitTimeMsPerSecond = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture)
                });
            }

            return items;
        }
    }
}
