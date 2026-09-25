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
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>#4234: TTL memoization for <see cref="GetDistinctPerfmonCountersAsync"/> — see <see cref="LiteNameListCache"/>.</summary>
    private readonly LiteNameListCache _distinctPerfmonCountersCache = new();

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
    delta_cntr_value,
    collection_time,
    cntr_type
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
                /* NULL stays NULL: a gauge row stores no delta (v62), and a 0 here would be #3642's fabricated zero. */
                DeltaValue = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                CollectionTime = reader.GetDateTime(4),
                CntrType = reader.IsDBNull(5) ? null : reader.GetInt32(5)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the distinct perfmon counter names for a server.
    /// <para>#4234: memoized through <see cref="_distinctPerfmonCountersCache"/> — keyed on (server, window
    /// length) for <see cref="LiteNameListCache.Ttl"/>, so the full-window <c>DISTINCT</c> behind this runs at
    /// most once per 15 minutes rather than on every 1-minute auto-refresh. <paramref name="nowUtc"/> is the
    /// cache's clock seam — null uses the wall clock; a test passes an explicit time to fast-forward past the
    /// TTL without sleeping.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctPerfmonCountersAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null, DateTime? nowUtc = null)
    {
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var windowLength = endTime - startTime;
        if (_distinctPerfmonCountersCache.TryGet(serverId, windowLength, endTime, effectiveNow, out var cached))
        {
            return cached;
        }

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

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

        _distinctPerfmonCountersCache.Set(serverId, windowLength, endTime, items, effectiveNow);
        return items;
    }

    /// <summary>
    /// Gets perfmon counter trend data for charting.
    /// </summary>
    public async Task<List<PerfmonTrendPoint>> GetPerfmonTrendAsync(int serverId, string counterName, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    collection_time,
    SUM(cntr_value) AS cntr_value,
    SUM(delta_cntr_value) AS delta_cntr_value,
    MAX(sample_interval_seconds) AS sample_interval_seconds,
    CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type
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
                /* NULL stays NULL: a gauge's instance rows store no delta (v62), so the SUM over them is NULL, not 0. */
                DeltaValue = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                /* NULL stays NULL (#3540's third state); 0 is the marker and must not be manufactured from it. */
                SampleIntervalSeconds = reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3)),
                CntrType = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4))
            });
        }

        return items;
    }

    /// <summary>
    /// Batched sibling of <see cref="GetPerfmonTrendAsync"/>: fetches the trend for ALL selected
    /// counters in ONE query (replacing an N+1 query-per-counter loop), grouped by counter name.
    /// <para>#4234: wraps the unchanged per-collection SUM as a subquery and re-aggregates into buckets sized
    /// to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (<c>seriesCount</c> always 1 into
    /// <see cref="TrendBuckets.AutoMinutes"/>, the ruling's own wording). <see cref="PerfmonTrendPoint"/>'s
    /// fields keep their per-collection MEANING: <c>Value</c> is the bucket's rounded GAUGE average (the
    /// ruling's rule for a gauge); <c>DeltaValue</c>/<c>SampleIntervalSeconds</c> are summed only over rated
    /// collections (a stored interval &gt; 0), so <c>DeltaValue / SampleIntervalSeconds</c> downstream is
    /// still the ruling's rate (summed deltas over summed intervals), and both are NULL together for a bucket
    /// with no rated collection — the same "no delta" state an unrated per-collection row always carried;
    /// <c>CntrType</c> keeps the per-collection MIN=MAX-agreement rule, now double-aggregated. When EVERY
    /// bucket the whole call returned holds exactly one physical collection, each point is stamped at its
    /// bucket's raw <c>first_collection_time</c> instead of the <c>time_bucket</c> grid line; a single merged
    /// bucket anywhere (any counter) keeps <c>bucket_start</c> throughout. Darling's twin is
    /// <c>ViewerDataService.PerfmonTrendsSql</c> / <c>GetPerfmonTrendsByCountersAsync</c> (#4234, PR #4304).</para>
    /// </summary>
    public async Task<Dictionary<string, List<PerfmonTrendPoint>>> GetPerfmonTrendsByCountersAsync(int serverId, List<string> counterNames, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetPerfmonTrendsByCountersAsync", "v_perfmon_stats trends batched by counter, bucketed");
        var result = new Dictionary<string, List<PerfmonTrendPoint>>();
        if (counterNames.Count == 0) return result;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);
        var nameParams = string.Join(", ", counterNames.Select((_, i) => "$" + (i + 4)));
        var widthParam = "$" + (counterNames.Count + 4);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = $@"
SELECT
    counter_name,
    GREATEST(time_bucket(to_minutes(CAST({widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    CAST(ROUND(AVG(cntr_value)) AS BIGINT) AS cntr_value,
    SUM(delta_cntr_value) FILTER (WHERE sample_interval_seconds > 0) AS delta_cntr_value,
    SUM(sample_interval_seconds) FILTER (WHERE sample_interval_seconds > 0) AS sample_interval_seconds,
    CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM (
    SELECT
        counter_name,
        collection_time,
        SUM(cntr_value) AS cntr_value,
        SUM(delta_cntr_value) AS delta_cntr_value,
        MAX(sample_interval_seconds) AS sample_interval_seconds,
        CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type
    FROM v_perfmon_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   counter_name IN ({nameParams})
    GROUP BY counter_name, collection_time
) AS collections
GROUP BY counter_name, 2
ORDER BY counter_name, 2";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var cn in counterNames)
            command.Parameters.Add(new DuckDBParameter { Value = cn });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string CounterName, DateTime BucketStart, DateTime FirstCollectionTime, long Value, long? DeltaValue, long? SampleIntervalSeconds, int? CntrType)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (reader.GetInt64(7) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(6),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                /* NULL stays NULL: a gauge's instance rows store no delta (v62), so the SUM is NULL, not 0. */
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : Convert.ToInt64(reader.GetValue(4)),
                reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5))));
        }

        foreach (var row in rows)
        {
            if (!result.TryGetValue(row.CounterName, out var list))
            {
                list = new List<PerfmonTrendPoint>();
                result[row.CounterName] = list;
            }

            list.Add(new PerfmonTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                Value = row.Value,
                DeltaValue = row.DeltaValue,
                SampleIntervalSeconds = row.SampleIntervalSeconds,
                CntrType = row.CntrType
            });
        }

        return result;
    }

}

public class PerfmonRow
{
    /// <summary>The snapshot this counter row belongs to (#3541 A10); every row of one snapshot shares it.</summary>
    public DateTime CollectionTime { get; set; }
    public string CounterName { get; set; } = "";
    public string InstanceName { get; set; } = "";
    public long Value { get; set; }

    /// <summary>The stored per-interval delta; <c>null</c> on a gauge row, which stores none (v62) — the
    /// reader used to coerce that to 0, which is #3642's fabricated zero on a level that has no delta.</summary>
    public long? DeltaValue { get; set; }

    /// <summary>The DMV's <c>cntr_type</c> as stored (v62, #3653 A7): the id every reader classifies by through
    /// <c>PerfmonCounterTypes</c>; <c>null</c> on a row written before the rung.</summary>
    public int? CntrType { get; set; }
}

public class PerfmonTrendPoint
{
    public DateTime CollectionTime { get; set; }

    /// <summary>The raw <c>cntr_value</c> summed across the counter's instances — for a gauge, the reading the
    /// chart plots (v62); for a rate, the cumulative count the delta was taken from.</summary>
    public long Value { get; set; }

    /// <summary>The stored per-interval delta summed across instances; <c>null</c> when no instance row stored one
    /// — a gauge since v62 — rather than the 0 the reader used to manufacture.</summary>
    public long? DeltaValue { get; set; }

    /// <summary>The counter's stored <c>cntr_type</c> when every instance row summed into this point agrees on
    /// one (v62, #3653 A7), else <c>null</c>: a row written before the rung, or a counter whose instances carry
    /// different types (the wait-statistics object's <c>Lock waits</c> family mixes a rate, a gauge and an
    /// average across its instances — their SUM was never one quantity, and no single type can describe it),
    /// which the chart classifies by the #3702 name proxy as it did before the rung.</summary>
    public int? CntrType { get; set; }

    /// <summary>The wall-clock seconds <see cref="DeltaValue"/> covers, and the only thing that makes a
    /// zero delta readable: the collector reports 0 in exactly the cases where no delta was knowable
    /// (first sighting, counter reset, gap past the policy), so (0, 0) is "unknown" while (0, n) is
    /// "genuinely idle" (#2234). MAX, never SUM, across a counter's instance rows — it is one measured
    /// sweep gap repeated per instance, and Transactions/sec carries a median of 12 of them. Nullable
    /// since #3653 A7 so the third state survives the read (#3540): <c>null</c> is a row that never stored
    /// an interval, which the reader used to coerce to the 0 marker. The perfmon chart now plots THROUGH
    /// this field via the shared <c>DeltaSeriesShaping</c>; <c>get_perfmon_trend</c> hands it to the caller
    /// as it always did (a NULL row, which perfmon_stats has never written, would now publish null).</summary>
    public long? SampleIntervalSeconds { get; set; }
}
