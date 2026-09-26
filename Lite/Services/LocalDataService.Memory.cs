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
    /// <summary>#4234: TTL memoization for <see cref="GetDistinctMemoryClerkTypesForPickerAsync"/> — see
    /// <see cref="LiteNameListCache"/>.</summary>
    private readonly LiteNameListCache _distinctMemoryClerkTypesCache = new();

    /// <summary>
    /// Gets the most recent memory stats snapshot for a server.
    /// </summary>
    public async Task<MemoryStatsRow?> GetLatestMemoryStatsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    total_physical_memory_mb,
    available_physical_memory_mb,
    total_page_file_mb,
    available_page_file_mb,
    system_memory_state,
    sql_memory_model,
    target_server_memory_mb,
    total_server_memory_mb,
    buffer_pool_mb,
    plan_cache_mb
FROM v_memory_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return new MemoryStatsRow
        {
            CollectionTime = reader.GetDateTime(0),
            TotalPhysicalMemoryMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
            AvailablePhysicalMemoryMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
            TotalPageFileMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
            AvailablePageFileMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
            SystemMemoryState = reader.IsDBNull(5) ? "" : reader.GetString(5),
            SqlMemoryModel = reader.IsDBNull(6) ? "" : reader.GetString(6),
            TargetServerMemoryMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
            TotalServerMemoryMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
            BufferPoolMb = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
            PlanCacheMb = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10))
        };
    }

    /// <summary>
    /// Gets memory stats trend for charting.
    /// </summary>
    public async Task<List<MemoryTrendPoint>> GetMemoryTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        /* #4234: bucketed to TrendBudget.Chart's point budget — this read feeds the Overview lane (buffer pool
           only) and the Memory tab's own chart (all four gauges), so every caller wants the same bucketing.
           seriesCount is always 1: the four gauges ride the same row/bucket, not separate series. */
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = MemoryTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(DateTime BucketStart, double Total, double Target, double Buffer, double Plan, DateTime FirstCollectionTime, long CollectionCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var collectionCount = reader.GetInt64(6);
            if (collectionCount != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                ToDouble(reader.GetValue(1)),
                ToDouble(reader.GetValue(2)),
                ToDouble(reader.GetValue(3)),
                ToDouble(reader.GetValue(4)),
                reader.GetDateTime(5),
                collectionCount));
        }

        /* Every bucket held exactly one collection: stamp at that collection's own clock, byte-identical to
           the pre-#4234 per-collection read (see GetCpuUtilizationAsync for the same rule). */
        var items = new List<MemoryTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new MemoryTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                TotalServerMemoryMb = row.Total,
                TargetServerMemoryMb = row.Target,
                BufferPoolMb = row.Buffer,
                PlanCacheMb = row.Plan
            });
        }

        return items;
    }

    /// <summary>
    /// The bucketed memory trend statement text (#4234), pulled out of <see cref="GetMemoryTrendAsync"/> so its
    /// shape is checkable without a live DuckDB. $1 server_id, $2/$3 the UTC window (also the GREATEST clamp so
    /// the first bucket never renders earlier than the window), $4 the bucket width in minutes. A NULL gauge
    /// counts as 0 in the average, exactly as the per-collection read always counted it in C#.
    /// </summary>
    internal static string MemoryTrendSql => $@"
WITH raw AS
(
    SELECT
        collection_time,
        total_server_memory_mb,
        target_server_memory_mb,
        buffer_pool_mb,
        plan_cache_mb
    FROM v_memory_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    AVG(COALESCE(total_server_memory_mb, 0)) AS total_server_memory_mb,
    AVG(COALESCE(target_server_memory_mb, 0)) AS target_server_memory_mb,
    AVG(COALESCE(buffer_pool_mb, 0)) AS buffer_pool_mb,
    AVG(COALESCE(plan_cache_mb, 0)) AS plan_cache_mb,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM raw
GROUP BY 1
ORDER BY 1";

    /// <summary>
    /// Whether this server has EVER recorded a memory sample, ignoring any window.
    /// <para>Lets an empty memory trend say WHICH kind of nothing it found. "No memory trend data" is true
    /// both of a quiet window and of a server the collector has never touched, and those want opposite
    /// responses — widen the window, versus go find out why collection is not running. Reads
    /// <c>v_memory_stats</c>, the same source <see cref="GetMemoryTrendAsync"/> reads, so it can never
    /// report "collected" for rows the trend cannot see. Darling's twin is
    /// <c>DarlingTrendReader.HasAnyMemoryStatAsync</c>; the two must stay in step so a user moving between
    /// the SKUs is not told a different story about the same state.</para>
    /// </summary>
    public async Task<bool> HasAnyMemoryStatAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_memory_stats
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets the distinct memory clerk types collected for a server, ordered by total memory descending.
    /// <para>Uncached: MCP shares no read with the clerk picker today, but this stays uncached on the same
    /// footing as <see cref="GetDistinctWaitTypesAsync"/> so an MCP caller added later never sees a stale
    /// answer. The picker's memoized entry point is <see cref="GetDistinctMemoryClerkTypesForPickerAsync"/>.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctMemoryClerkTypesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    clerk_type
FROM v_memory_clerks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY clerk_type
ORDER BY SUM(memory_mb) DESC";

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
    /// Picker-only entry point for <see cref="GetDistinctMemoryClerkTypesAsync"/>: checks
    /// <see cref="_distinctMemoryClerkTypesCache"/> first, falls back to the shared uncached read, then
    /// memoizes it.
    /// <para>#4234: keyed on (server, window length) for <see cref="LiteNameListCache.Ttl"/>, so the
    /// full-window read behind this runs at most once per 15 minutes rather than on every 1-minute
    /// auto-refresh. Only <c>ServerTab</c>'s clerk picker goes through this cache.
    /// <paramref name="nowUtc"/> is the cache's clock seam — null uses the wall clock; a test passes an
    /// explicit time to fast-forward past the TTL without sleeping.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctMemoryClerkTypesForPickerAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? nowUtc = null)
    {
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var windowLength = endTime - startTime;
        if (_distinctMemoryClerkTypesCache.TryGet(serverId, windowLength, endTime, effectiveNow, out var cached))
        {
            return cached;
        }

        var items = await GetDistinctMemoryClerkTypesAsync(serverId, hoursBack, fromDate, toDate);

        _distinctMemoryClerkTypesCache.Set(serverId, windowLength, endTime, items, effectiveNow);
        return items;
    }

    /// <summary>
    /// The bucketed batched-trend statement text (#4234), pulled out of <see cref="GetMemoryClerkTrendsByTypesAsync"/>
    /// so its shape (the bucket width in its own trailing parameter, after the dynamic <c>clerk_type IN (...)</c>
    /// list so that list's numbering does not shift) is checkable without a live DuckDB.
    /// </summary>
    internal static string MemoryClerkTrendsSql(int clerkTypeCount)
    {
        var typeParams = string.Join(", ", Enumerable.Range(0, clerkTypeCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (clerkTypeCount + 4);
        return $@"
SELECT
    clerk_type,
    GREATEST(time_bucket(to_minutes(CAST({widthParam} AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    AVG(memory_mb) AS memory_mb,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM v_memory_clerks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   clerk_type IN ({typeParams})
GROUP BY clerk_type, 2
ORDER BY clerk_type, 2";
    }

    /// <summary>
    /// Batched sibling of the removed per-clerk loop: fetches the trend for ALL selected clerk types in ONE
    /// query, grouped by clerk type.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (<c>seriesCount</c>
    /// always 1 into <see cref="TrendBuckets.AutoMinutes"/>, the ruling's own wording). A clerk's memory is a
    /// GAUGE, not a counter, so a bucket's value is the plain average of the collections inside it — there is
    /// no unrated-row concept here (#3540 only marks delta rows), so every row in the window counts and no
    /// <c>HAVING</c> is needed to drop a bucket. When EVERY bucket the whole call returned holds exactly one
    /// physical collection, each point is stamped at its bucket's raw <c>first_collection_time</c> instead of
    /// the <c>time_bucket</c> grid line; a single merged bucket anywhere (any clerk type) keeps
    /// <c>bucket_start</c> throughout — the same rule <see cref="WaitTrendsSql"/> and <c>PerfmonTrendsSql</c>
    /// use.</para>
    /// </summary>
    public async Task<Dictionary<string, List<MemoryClerkTrendPoint>>> GetMemoryClerkTrendsByTypesAsync(int serverId, List<string> clerkTypes, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetMemoryClerkTrendsByTypesAsync", "v_memory_clerks trends batched by type, bucketed");
        var result = new Dictionary<string, List<MemoryClerkTrendPoint>>();
        if (clerkTypes.Count == 0) return result;

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = MemoryClerkTrendsSql(clerkTypes.Count);

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var ct in clerkTypes)
            command.Parameters.Add(new DuckDBParameter { Value = ct });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string ClerkType, DateTime BucketStart, DateTime FirstCollectionTime, double MemoryMb)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (reader.GetInt64(4) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                /* ToDouble, not GetDouble: DuckDB's AVG over a DECIMAL column can hand back a boxed value
                   GetDouble does not accept — see GetWaitStatsTrendsByTypesAsync's own sum columns for the
                   same guard. */
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))));
        }

        foreach (var row in rows)
        {
            if (!result.TryGetValue(row.ClerkType, out var list))
            {
                list = new List<MemoryClerkTrendPoint>();
                result[row.ClerkType] = list;
            }

            list.Add(new MemoryClerkTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                MemoryMb = row.MemoryMb
            });
        }

        return result;
    }

    /// <summary>
    /// Gets memory pressure events (from RING_BUFFER_RESOURCE_MONITOR) for charting.
    /// </summary>
    public async Task<List<MemoryPressureEventRow>> GetMemoryPressureEventsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    sample_time,
    memory_notification,
    memory_indicators_process,
    memory_indicators_system
FROM v_memory_pressure_events
WHERE server_id = $1
AND   sample_time >= $2
AND   sample_time <= $3
ORDER BY sample_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<MemoryPressureEventRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryPressureEventRow
            {
                SampleTime = reader.GetDateTime(0),
                MemoryNotification = reader.IsDBNull(1) ? "" : reader.GetString(1),
                MemoryIndicatorsProcess = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                MemoryIndicatorsSystem = reader.IsDBNull(3) ? 0 : reader.GetInt32(3)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest memory clerk breakdown.
    /// </summary>
    public async Task<List<MemoryClerkRow>> GetLatestMemoryClerksAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        /* #3541 A10: collection_time rides on the row (same statement as the values), so get_memory_clerks
           can publish captured_at — every clerk of one snapshot shares it by construction. */
        command.CommandText = @"
SELECT clerk_type, memory_mb, collection_time
FROM v_memory_clerks
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_memory_clerks WHERE server_id = $1)
ORDER BY memory_mb DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<MemoryClerkRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new MemoryClerkRow
            {
                ClerkType = reader.GetString(0),
                MemoryMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                CollectionTime = reader.GetDateTime(2)
            });
        }

        return items;
    }
}

public class MemoryStatsRow
{
    public DateTime CollectionTime { get; set; }
    public double TotalPhysicalMemoryMb { get; set; }
    public double AvailablePhysicalMemoryMb { get; set; }
    public double TotalPageFileMb { get; set; }
    public double AvailablePageFileMb { get; set; }
    public string SystemMemoryState { get; set; } = "";
    public string SqlMemoryModel { get; set; } = "";
    public double TargetServerMemoryMb { get; set; }
    public double TotalServerMemoryMb { get; set; }
    public double BufferPoolMb { get; set; }
    public double PlanCacheMb { get; set; }
    public double UsedPhysicalMemoryMb => TotalPhysicalMemoryMb - AvailablePhysicalMemoryMb;
    public double MemoryUtilizationPercent => TotalPhysicalMemoryMb > 0 ? UsedPhysicalMemoryMb / TotalPhysicalMemoryMb * 100 : 0;
}

public class MemoryTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double TotalServerMemoryMb { get; set; }
    public double TargetServerMemoryMb { get; set; }
    public double BufferPoolMb { get; set; }
    public double PlanCacheMb { get; set; }
    public double TotalGrantedMb { get; set; }
}

public class MemoryClerkRow
{
    /// <summary>The snapshot this clerk row belongs to (#3541 A10); every row of one snapshot shares it.</summary>
    public DateTime CollectionTime { get; set; }
    public string ClerkType { get; set; } = "";
    public double MemoryMb { get; set; }
    public string MemoryFormatted => MemoryMb >= 1024 ? $"{MemoryMb / 1024:F1} GB" : $"{MemoryMb:F1} MB";
}

public class MemoryClerkTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string ClerkType { get; set; } = "";
    public double MemoryMb { get; set; }
}

public class MemoryPressureEventRow
{
    public DateTime SampleTime { get; set; }
    public string MemoryNotification { get; set; } = "";
    public int MemoryIndicatorsProcess { get; set; }
    public int MemoryIndicatorsSystem { get; set; }
}
