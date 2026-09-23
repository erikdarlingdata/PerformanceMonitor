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

/*
 * The bucketed trend reads behind the MCP tools (#3897) — get_file_io_trend, get_lock_wait_trend,
 * get_query_duration_trend and get_procedure_duration_trend. Each is the per-collection read the desktop chart
 * runs (GetFileIoLatencyTrendAsync, GetLockWaitTrendAsync, GetQueryDurationTrendAsync,
 * GetProcedureDurationTrendAsync — left exactly as they are, because a chart wants every collection) with the
 * collections then gathered into buckets of $N minutes, so an MCP answer stays near TrendBuckets.McpPointBudget
 * points instead of one point per collection per series: a day of file I/O was 12,500 rows and 1.4 MB.
 *
 * Darling's twins are DarlingTrendReader (file I/O, the duration pair via DurationTrendRouting's bucketed
 * builders) and DarlingBlockingTrendReader (lock waits); the SQL differs only in dialect — DuckDB's time_bucket
 * where PostgreSQL has date_bin, both on TrendBuckets.OriginSql so a width that does not divide a day still
 * bins the same rows the same way on both SKUs. The rules are the same: counts summed, rates and ratios
 * recomputed from the bucket's sums (never averaged averages), the worst single collection kept as the peak,
 * every point stamped at its bucket's start and the first at the window's start.
 */
public partial class LocalDataService
{
    /// <summary>
    /// The ranking both file I/O statements share, so the series the first counts are the series the second
    /// charts — Darling's <c>DarlingTrendReader.FileIoRankedCte</c> in DuckDB's dialect. A series is a
    /// (database, file type) pair, or each file of the database <c>$4</c> names; ranked by the window's summed
    /// I/O stall, operations then names breaking ties. Only series that read or wrote rank, and a restart row
    /// (a stored interval of 0, #3540) ranks nothing, because the trend drops it.
    /// $1 server_id, $2/$3 window (UTC), $4 database (NULL = all).
    /// </summary>
    private const string FileIoRankedCte = @"
ranked AS (
    SELECT
        database_name,
        file_type,
        CASE WHEN $4 IS NULL THEN NULL ELSE file_name END AS file_name,
        COUNT(DISTINCT file_name) AS files,
        CAST(SUM(delta_stall_read_ms + delta_stall_write_ms) AS BIGINT) AS stall_ms,
        CAST(SUM(delta_reads + delta_writes) AS BIGINT) AS ops,
        ROW_NUMBER() OVER (
            ORDER BY SUM(delta_stall_read_ms + delta_stall_write_ms) DESC,
                     SUM(delta_reads + delta_writes) DESC,
                     database_name,
                     file_type,
                     CASE WHEN $4 IS NULL THEN NULL ELSE file_name END
        ) AS series_rank
    FROM v_file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   ($4 IS NULL OR database_name = $4)
    AND   (delta_reads > 0 OR delta_writes > 0)
    AND   sample_interval_seconds IS DISTINCT FROM 0
    GROUP BY database_name, file_type, CASE WHEN $4 IS NULL THEN NULL ELSE file_name END
)";

    /// <summary>
    /// The window's active file I/O series in rank order (#3897) — the first of get_file_io_trend's two reads,
    /// whose length decides the bucket width. Darling's twin is <c>DarlingTrendReader.FileIoSeriesSql</c>.
    /// </summary>
    internal async Task<List<FileIoSeries>> GetFileIoSeriesAsync(int serverId, int hoursBack, DateTime asOfUtc, string? databaseName)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = $@"WITH{FileIoRankedCte}
SELECT database_name, file_type, file_name, files, stall_ms, ops, series_rank
FROM ranked
ORDER BY series_rank";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)databaseName ?? DBNull.Value });

        var items = new List<FileIoSeries>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new FileIoSeries(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : (long)ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : (long)ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : (long)ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : (long)ToDouble(reader.GetValue(6))));
        }

        return items;
    }

    /// <summary>
    /// The second read (#3897): the top <paramref name="chartedSeries"/> series each on its own line, every
    /// ranked series past them folded into one "(other)" line per COLLECTION before bucketing, and latency
    /// recomputed as the bucket's summed stall over its summed operations (NULL over none — the desktop read's
    /// <c>ELSE 0</c> stays in the desktop read). Restart rows (a stored interval of 0) are dropped as they are
    /// there (#3540). Darling's twin is <c>DarlingTrendReader.FileIoTrendSql</c>.
    /// </summary>
    internal async Task<List<FileIoPoint>> GetFileIoTrendAsync(
        int serverId, int hoursBack, DateTime asOfUtc, string? databaseName, int chartedSeries, int bucketMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = $@"WITH{FileIoRankedCte},
labelled AS (
    SELECT
        f.collection_time,
        CASE WHEN r.series_rank <= $5 THEN r.database_name ELSE '(other)' END AS database_name,
        CASE WHEN r.series_rank <= $5 THEN r.file_type ELSE '(other)' END AS file_type,
        CASE
            WHEN r.series_rank <= $5 THEN r.file_name
            WHEN $4 IS NULL THEN NULL
            ELSE '(other)'
        END AS file_name,
        f.delta_reads,
        f.delta_writes,
        f.delta_stall_read_ms,
        f.delta_stall_write_ms
    FROM v_file_io_stats AS f
    JOIN ranked AS r
      ON  r.database_name = f.database_name
      AND r.file_type = f.file_type
      AND (r.file_name IS NULL OR r.file_name = f.file_name)
    WHERE f.server_id = $1
    AND   f.collection_time >= $2
    AND   f.collection_time <= $3
    AND   ($4 IS NULL OR f.database_name = $4)
    AND   f.sample_interval_seconds IS DISTINCT FROM 0
),
per_collection AS (
    SELECT
        collection_time,
        database_name,
        file_type,
        file_name,
        SUM(delta_reads) AS reads,
        SUM(delta_writes) AS writes,
        SUM(CAST(delta_stall_read_ms AS DOUBLE PRECISION)) AS stall_read_ms,
        SUM(CAST(delta_stall_write_ms AS DOUBLE PRECISION)) AS stall_write_ms
    FROM labelled
    GROUP BY collection_time, database_name, file_type, file_name
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($6 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    database_name,
    file_type,
    file_name,
    CAST(SUM(reads) AS BIGINT) AS reads,
    CAST(SUM(writes) AS BIGINT) AS writes,
    SUM(stall_read_ms) AS stall_read_ms,
    SUM(stall_write_ms) AS stall_write_ms,
    SUM(stall_read_ms) / NULLIF(CAST(SUM(reads) AS DOUBLE PRECISION), 0) AS avg_read_latency_ms,
    SUM(stall_write_ms) / NULLIF(CAST(SUM(writes) AS DOUBLE PRECISION), 0) AS avg_write_latency_ms,
    MAX(stall_read_ms / NULLIF(CAST(reads AS DOUBLE PRECISION), 0)) AS peak_read_latency_ms,
    MAX(stall_write_ms / NULLIF(CAST(writes AS DOUBLE PRECISION), 0)) AS peak_write_latency_ms
FROM per_collection
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)databaseName ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = chartedSeries });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var items = new List<FileIoPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new FileIoPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : (long)ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : (long)ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                reader.IsDBNull(8) ? null : ToDouble(reader.GetValue(8)),
                reader.IsDBNull(9) ? null : ToDouble(reader.GetValue(9)),
                reader.IsDBNull(10) ? null : ToDouble(reader.GetValue(10)),
                reader.IsDBNull(11) ? null : ToDouble(reader.GetValue(11))));
        }

        return items;
    }

    /// <summary>
    /// The LCK% rows both lock-wait statements read (#3897): the desktop read's <c>raw</c> CTE (the stored
    /// interval, 0 → NULL; the per-type LAG only for a pre-v60 row), reduced to the rows whose rate is knowable and
    /// whose delta is not a restart's negative. Darling's twin is <c>DarlingBlockingTrendReader.LockWaitRatedCtes</c>.
    /// $1 server_id, $2/$3 window (UTC).
    /// </summary>
    private const string LockWaitRatedCtes = @"
raw AS
(
    SELECT
        collection_time,
        wait_type,
        delta_wait_time_ms,
        CASE WHEN sample_interval_seconds IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time))))
             ELSE NULLIF(sample_interval_seconds, 0)
        END AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   wait_type LIKE 'LCK%'
    AND   collection_time >= $2
    AND   collection_time <= $3
),
rated AS
(
    SELECT
        collection_time,
        wait_type,
        CAST(delta_wait_time_ms AS DOUBLE PRECISION) AS wait_ms,
        interval_seconds
    FROM raw
    WHERE interval_seconds > 0
    AND   delta_wait_time_ms >= 0
)";

    /// <summary>
    /// Every LCK% type the window rated, with its window totals (#3897) — the legend get_lock_wait_trend lists
    /// beside the family series. Darling's twin is <c>DarlingBlockingTrendReader.LockWaitTypesSql</c>.
    /// </summary>
    internal async Task<List<LockWaitType>> GetLockWaitTypesAsync(int serverId, int hoursBack, DateTime asOfUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = $@"WITH{LockWaitRatedCtes}
SELECT
    wait_type,
    SUM(wait_ms) AS total_wait_ms,
    SUM(interval_seconds) AS rated_seconds,
    MAX(CASE WHEN interval_seconds > 0 THEN wait_ms / interval_seconds END) AS peak_wait_time_ms_per_second
FROM rated
GROUP BY wait_type
ORDER BY wait_type";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<LockWaitType>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LockWaitType(
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : ToDouble(reader.GetValue(3))));
        }

        return items;
    }

    /// <summary>
    /// The lock-wait FAMILY bucketed (#3897): every LCK% type's wait summed per collection and rated over that
    /// collection's ONE interval (so summing types cannot multiply the denominator), then time-weighted across
    /// the bucket; the peak is the worst single collection's family rate. Darling's twin is
    /// <c>DarlingBlockingTrendReader.LockWaitTrendSql</c>.
    /// </summary>
    internal async Task<List<LockWaitPoint>> GetLockWaitFamilyTrendAsync(int serverId, int hoursBack, DateTime asOfUtc, int bucketMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = $@"WITH{LockWaitRatedCtes},
per_collection AS
(
    SELECT
        collection_time,
        SUM(wait_ms) AS wait_ms,
        MAX(interval_seconds) AS interval_seconds
    FROM rated
    GROUP BY collection_time
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(wait_ms) / SUM(interval_seconds) AS wait_time_ms_per_second,
    MAX(CASE WHEN interval_seconds > 0 THEN wait_ms / interval_seconds END) AS peak_wait_time_ms_per_second
FROM per_collection
GROUP BY 1
ORDER BY 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var items = new List<LockWaitPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LockWaitPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : ToDouble(reader.GetValue(2))));
        }

        return items;
    }

    /// <summary>The query-stats duration trend bucketed (#3897) — <see cref="GetQueryDurationTrendAsync"/>'s
    /// per-collection read over <c>v_query_stats</c>, gathered into buckets.</summary>
    public Task<List<QueryTrendPoint>> GetBucketedQueryDurationTrendAsync(int serverId, int hoursBack, DateTime asOfUtc, int bucketMinutes) =>
        ReadBucketedDurationTrendAsync("v_query_stats", serverId, hoursBack, asOfUtc, bucketMinutes);

    /// <summary>The procedure-stats duration trend bucketed (#3897) — <see cref="GetProcedureDurationTrendAsync"/>'s
    /// per-collection read over <c>v_procedure_stats</c>, gathered into buckets.</summary>
    public Task<List<QueryTrendPoint>> GetBucketedProcedureDurationTrendAsync(int serverId, int hoursBack, DateTime asOfUtc, int bucketMinutes) =>
        ReadBucketedDurationTrendAsync("v_procedure_stats", serverId, hoursBack, asOfUtc, bucketMinutes);

    /// <summary>
    /// The shared body of the two bucketed duration reads (#3897), Darling's
    /// <c>DurationTrendRouting.BuildBucketedRawTrendSql</c> in DuckDB's dialect: the desktop read's per-collection
    /// CTE (the stored interval three-state, #3653 A11), then each bucket's rate is its RATED collections' summed
    /// work over their summed seconds — a collection with no knowable interval is left out of both, not counted as
    /// zero — its peak is the worst single collection's rate, and <c>unrated_collections</c> counts what was left
    /// out. <paramref name="relation"/> is one of two constants, never caller text.
    /// </summary>
    private async Task<List<QueryTrendPoint>> ReadBucketedDurationTrendAsync(
        string relation, int serverId, int hoursBack, DateTime asOfUtc, int bucketMinutes)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc, utcOffsetMinutes: 0);

        command.CommandText = $@"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
        SUM(delta_execution_count) AS total_executions,
        CASE WHEN MAX(sample_interval_seconds) IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
             ELSE NULLIF(MAX(sample_interval_seconds), 0)
        END AS interval_seconds
    FROM {relation}
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
),
rated AS
(
    SELECT
        collection_time,
        CASE WHEN interval_seconds > 0 THEN total_elapsed_ms END AS rated_elapsed_ms,
        CASE WHEN interval_seconds > 0 THEN total_executions END AS rated_executions,
        CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds,
        CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds END AS elapsed_ms_per_second,
        CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
    FROM raw
)
SELECT
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(rated_elapsed_ms) / SUM(rated_seconds) AS elapsed_ms_per_second,
    CAST(SUM(rated_executions) AS DOUBLE PRECISION) / SUM(rated_seconds) AS executions_per_second,
    MAX(elapsed_ms_per_second) AS peak_elapsed_ms_per_second,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) - COUNT(rated_seconds) AS unrated_collections
FROM rated
GROUP BY 1
ORDER BY 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var items = new List<QueryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* NULL stays NULL (#3541 A12): a bucket whose every collection was unrated is an unrated point. */
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? null : ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? null : (long)ToDouble(reader.GetValue(2)),
                ExecutionsPerSecond = reader.IsDBNull(2) ? null : ToDouble(reader.GetValue(2)),
                PeakElapsedMsPerSecond = reader.IsDBNull(3) ? null : ToDouble(reader.GetValue(3)),
                FirstCollectionTime = reader.GetDateTime(4),
                UnratedInBucket = reader.IsDBNull(5) ? 0 : (long)ToDouble(reader.GetValue(5)),
            });
        }

        return items;
    }
}
