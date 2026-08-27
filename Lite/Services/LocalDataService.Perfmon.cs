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
    public async Task<List<string>> GetDistinctPerfmonCountersAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

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
    public async Task<List<PerfmonTrendPoint>> GetPerfmonTrendAsync(int serverId, string counterName, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    collection_time,
    SUM(cntr_value) AS cntr_value,
    SUM(delta_cntr_value) AS delta_cntr_value,
    MAX(sample_interval_seconds) AS sample_interval_seconds
FROM v_perfmon_stats
WHERE server_id = $1
AND   counter_name = $2
AND   collection_time >= $3
AND   collection_time <= $4
GROUP BY collection_time
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
                DeltaValue = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                SampleIntervalSeconds = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3))
            });
        }

        return items;
    }

    /// <summary>
    /// Batched sibling of <see cref="GetPerfmonTrendAsync"/>: fetches the trend for ALL selected
    /// counters in ONE query (replacing an N+1 query-per-counter loop), grouped by counter name.
    /// </summary>
    public async Task<Dictionary<string, List<PerfmonTrendPoint>>> GetPerfmonTrendsByCountersAsync(int serverId, List<string> counterNames, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetPerfmonTrendsByCountersAsync", "v_perfmon_stats trends batched by counter");
        var result = new Dictionary<string, List<PerfmonTrendPoint>>();
        if (counterNames.Count == 0) return result;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var nameParams = string.Join(", ", counterNames.Select((_, i) => "$" + (i + 4)));

        command.CommandText = $@"
SELECT
    counter_name,
    collection_time,
    SUM(cntr_value) AS cntr_value,
    SUM(delta_cntr_value) AS delta_cntr_value,
    MAX(sample_interval_seconds) AS sample_interval_seconds
FROM v_perfmon_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   counter_name IN ({nameParams})
GROUP BY counter_name, collection_time
ORDER BY counter_name, collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var cn in counterNames)
            command.Parameters.Add(new DuckDBParameter { Value = cn });

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var cn = reader.GetString(0);
            if (!result.TryGetValue(cn, out var list))
            {
                list = new List<PerfmonTrendPoint>();
                result[cn] = list;
            }
            list.Add(new PerfmonTrendPoint
            {
                CollectionTime = reader.GetDateTime(1),
                Value = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeltaValue = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                SampleIntervalSeconds = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4))
            });
        }

        return result;
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

    /// <summary>The wall-clock seconds <see cref="DeltaValue"/> covers, and the only thing that makes a
    /// zero delta readable: the collector reports 0 in exactly the cases where no delta was knowable
    /// (first sighting, counter reset, gap past the policy), so (0, 0) is "unknown" while (0, n) is
    /// "genuinely idle" (#2234). MAX, never SUM, across a counter's instance rows — it is one measured
    /// sweep gap repeated per instance, and Transactions/sec carries a median of 12 of them.</summary>
    public long SampleIntervalSeconds { get; set; }
}
