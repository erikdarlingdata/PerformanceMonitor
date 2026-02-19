/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest perfmon counters for a server.
    /// </summary>
    public async Task<List<PerfmonRow>> GetLatestPerfmonStatsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    counter_name,
    instance_name,
    cntr_value,
    delta_cntr_value
FROM v_perfmon_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_perfmon_stats WHERE server_id = $1)
ORDER BY counter_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<PerfmonRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PerfmonRow
            {
                CounterName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                InstanceName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                Value = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeltaValue = reader.IsDBNull(3) ? 0 : reader.GetInt64(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the distinct perfmon counter names for a server.
    /// </summary>
    public async Task<List<string>> GetDistinctPerfmonCountersAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT DISTINCT counter_name
FROM v_perfmon_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY counter_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(reader.GetString(0));
        }
        return items;
    }

    /// <summary>
    /// Gets perfmon counter trend data for charting.
    /// </summary>
    public async Task<List<PerfmonTrendPoint>> GetPerfmonTrendAsync(int serverId, string counterName, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    cntr_value,
    delta_cntr_value
FROM v_perfmon_stats
WHERE server_id = $1
AND   counter_name = $2
AND   collection_time >= $3
AND   collection_time <= $4
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = counterName });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<PerfmonTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new PerfmonTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                DeltaValue = reader.IsDBNull(2) ? 0 : reader.GetInt64(2)
            });
        }

        return items;
    }

}

public class PerfmonRow
{
    public string CounterName { get; set; } = "";
    public string InstanceName { get; set; } = "";
    public long Value { get; set; }
    public long DeltaValue { get; set; }
}

public class PerfmonTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public long Value { get; set; }
    public long DeltaValue { get; set; }
}
