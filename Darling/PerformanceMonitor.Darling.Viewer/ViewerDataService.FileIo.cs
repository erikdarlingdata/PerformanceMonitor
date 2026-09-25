/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One file's I/O-latency point for the File I/O tab's Latency sub-tab — a mirror of Lite's
/// <c>FileIoTrendPoint</c> (<c>LocalDataService.FileIo.cs</c>). The read averages stall-ms / op per
/// (bucket, database_name, file_name); the chart groups on <c>database_name.file_name</c>,
/// sums read+write latency to rank the top files, and overlays the queued-I/O latencies as dashed
/// lines when any are present.
/// <para>#4234: <c>CollectionTime</c> is a <c>date_bin</c> bucket start, EXCEPT when every bucket the call
/// returned holds exactly one physical collection, in which case every point is stamped at its own raw
/// collection time instead — see <see cref="ViewerDataService.GetFileIoLatencyTrendAsync"/>.</para>
/// </summary>
public sealed record FileIoLatencyPoint(
    DateTime CollectionTime,
    string DatabaseName,
    string FileName,
    double AvgReadLatencyMs,
    double AvgWriteLatencyMs,
    double AvgQueuedReadLatencyMs,
    double AvgQueuedWriteLatencyMs);

/// <summary>
/// One file's throughput point (MB/s) for the File I/O tab's Throughput sub-tab — a mirror of Lite's
/// <c>FileIoThroughputPoint</c>. <c>file_label</c> is the pre-concatenated <c>database_name.file_name</c>
/// the read builds, and the per-second rates come from the delta bytes divided by the row's stored
/// <c>sample_interval_seconds</c> (LAG-derived only for pre-V127 rows that never recorded one, #3540).
/// <para>#4234: <c>CollectionTime</c> is a <c>date_bin</c> bucket start, EXCEPT when every bucket the call
/// returned holds exactly one physical collection, in which case every point is stamped at its own raw
/// collection time instead — see <see cref="ViewerDataService.GetFileIoThroughputTrendAsync"/>.</para>
/// </summary>
public sealed record FileIoThroughputPoint(
    DateTime CollectionTime,
    string FileLabel,
    double ReadMbPerSec,
    double WriteMbPerSec);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The File I/O latency read — Lite's <c>GetFileIoLatencyTrendAsync</c> ported to Postgres, then bucketed
    /// (#4234). Reads <c>v_file_io_stats</c> (a <c>SELECT *</c> passthrough over <c>file_io_stats</c>): a
    /// <c>top_files</c> CTE picks the 10 busiest (database, file) pairs by total delta ops over the window —
    /// unchanged by bucketing — then average read/write (and queued read/write) latency per bucket is computed
    /// as summed stall-ms / summed ops, with the delta-stall sums CAST to double precision before division. The
    /// queued-stall columns are COALESCE'd to 0 so a server whose build predates them still reads 0.
    /// <para>#3540, carried into the bucket: a row whose stored <c>sample_interval_seconds</c> is 0 (no delta
    /// knowable — first sighting, counter reset, a gap past the policy) has its deltas nulled out of the sums
    /// via the <c>rated</c> CTE rather than contributing a confident 0, exactly as the pre-bucket read dropped
    /// the row outright. <c>IS DISTINCT FROM 0</c> still keeps pre-V127 rows (NULL: interval never recorded).
    /// Unlike the pre-bucket read, the row is NOT removed from the FROM clause — it still counts toward
    /// <c>collection_count</c> below, so a bucket is never mistaken for a true singleton just because its one
    /// physical collection happened to be unrated. <c>HAVING COUNT(rated_reads) &gt; 0</c> drops a bucket whose
    /// every row was unrated, matching the pre-bucket read's "absent, not 0.00 ms" answer for that case; a
    /// bucket with at least one rated row keeps its per-metric <c>CASE ... ELSE 0</c> zero-safety exactly as
    /// before (a rated row with 0 ops is a real 0 ms, not an absent point).</para>
    /// <para>#4234 review (item 3): <c>first_collection_time</c> (<c>MIN(collection_time)</c>, every row in
    /// <c>rated</c> — rated or not) and <c>collection_count</c> (<c>COUNT(*)</c> over that same population) ride
    /// along so the caller can tell a true singleton bucket from one the bucketing actually merged, exactly like
    /// <c>WaitTrendsSql</c>. $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket width minutes.
    /// </para>
    /// </summary>
    public const string FileIoLatencyTrendSql = $$"""
        WITH top_files AS (
            SELECT database_name, file_name
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   (delta_reads > 0 OR delta_writes > 0)
            GROUP BY database_name, file_name
            ORDER BY SUM(delta_reads + delta_writes) DESC
            LIMIT 10
        ),
        rated AS (
            SELECT
                f.collection_time,
                f.database_name,
                f.file_name,
                /* #3540: a stored interval of 0 means this row's own deltas are not knowable, so they are
                   nulled out of the sums below — the row still counts as a physical collection (collection_count)
                   but contributes nothing to a bucket's rate. */
                CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN f.delta_reads END AS rated_reads,
                CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN f.delta_writes END AS rated_writes,
                CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN f.delta_stall_read_ms END AS rated_stall_read_ms,
                CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN f.delta_stall_write_ms END AS rated_stall_write_ms,
                CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN COALESCE(f.delta_stall_queued_read_ms, 0) END AS rated_stall_queued_read_ms,
                CASE WHEN f.sample_interval_seconds IS DISTINCT FROM 0 THEN COALESCE(f.delta_stall_queued_write_ms, 0) END AS rated_stall_queued_write_ms
            FROM v_file_io_stats f
            JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
            WHERE f.server_id = $1
            AND   f.collection_time >= $2
            AND   f.collection_time <= $3
        )
        SELECT
            database_name,
            file_name,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            CASE WHEN SUM(rated_reads) > 0
                 THEN SUM(CAST(rated_stall_read_ms AS double precision)) / SUM(rated_reads)
                 ELSE 0 END AS avg_read_latency_ms,
            CASE WHEN SUM(rated_writes) > 0
                 THEN SUM(CAST(rated_stall_write_ms AS double precision)) / SUM(rated_writes)
                 ELSE 0 END AS avg_write_latency_ms,
            CASE WHEN SUM(rated_reads) > 0
                 THEN SUM(CAST(rated_stall_queued_read_ms AS double precision)) / SUM(rated_reads)
                 ELSE 0 END AS avg_queued_read_latency_ms,
            CASE WHEN SUM(rated_writes) > 0
                 THEN SUM(CAST(rated_stall_queued_write_ms AS double precision)) / SUM(rated_writes)
                 ELSE 0 END AS avg_queued_write_latency_ms,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY database_name, file_name, 3
        HAVING COUNT(rated_reads) > 0
        ORDER BY database_name, file_name, 3
        """;

    /// <summary>
    /// The File I/O throughput read — Lite's <c>GetFileIoThroughputTrendAsync</c> ported to Postgres, then
    /// bucketed (#4234). A <c>top_files</c> CTE picks the 10 busiest (database, file) pairs by total delta
    /// bytes — unchanged by bucketing — a <c>with_interval</c> CTE takes each row's stored
    /// <c>sample_interval_seconds</c> (falling back to the LAG of the prior collection_time, partitioned by
    /// server/database/file, for pre-V127 rows that never recorded one — #3540), and a <c>rated</c> CTE nulls
    /// out a row's bytes/interval when the interval is unknowable. A bucket's per-second MB rate is time-weighted
    /// — summed rated bytes over summed rated interval-seconds / 1 MiB — never an average of per-collection
    /// rates, mirroring <c>WaitTrendsSql</c>. <c>HAVING COUNT(rated_seconds) &gt; 0</c> drops a bucket with no
    /// rated collection, same as the pre-bucket read always dropped that collection (#3653: NULL, never a
    /// measured 0.00 MB/s).
    /// <para>#4234 review (item 3): <c>first_collection_time</c> / <c>collection_count</c> ride along exactly
    /// like <c>WaitTrendsSql</c>'s, counted over every physical row (rated or not) so a bucket is never mistaken
    /// for a true singleton just because its one physical collection happened to be unrated.</para>
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket width minutes.
    /// </summary>
    public const string FileIoThroughputTrendSql = $$"""
        WITH top_files AS (
            SELECT database_name, file_name
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   (delta_read_bytes > 0 OR delta_write_bytes > 0)
            GROUP BY database_name, file_name
            ORDER BY SUM(delta_read_bytes + delta_write_bytes) DESC
            LIMIT 10
        ),
        with_interval AS (
            SELECT
                f.collection_time,
                f.database_name || '.' || f.file_name AS file_label,
                f.delta_read_bytes,
                f.delta_write_bytes,
                /* #3540: the STORED interval where the row has one — 0 (no delta knowable) becomes NULL
                   through NULLIF and is nulled out below, exactly as it always dropped the first row per
                   file. NULL (a pre-V127 row that never recorded one) falls back to the LAG this read
                   always used. */
                CASE WHEN f.sample_interval_seconds IS NULL
                     THEN EXTRACT(EPOCH FROM (f.collection_time - LAG(f.collection_time) OVER (
                              PARTITION BY f.server_id, f.database_name, f.file_name
                              ORDER BY f.collection_time
                          )))
                     ELSE NULLIF(f.sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_file_io_stats f
            JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
            WHERE f.server_id = $1
            AND   f.collection_time >= $2
            AND   f.collection_time <= $3
        ),
        rated AS (
            SELECT
                collection_time,
                file_label,
                CASE WHEN interval_seconds > 0 THEN delta_read_bytes END AS rated_read_bytes,
                CASE WHEN interval_seconds > 0 THEN delta_write_bytes END AS rated_write_bytes,
                CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds
            FROM with_interval
        )
        SELECT
            file_label,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            CAST(SUM(rated_read_bytes) AS double precision) / SUM(rated_seconds) / 1048576.0 AS read_mb_per_sec,
            CAST(SUM(rated_write_bytes) AS double precision) / SUM(rated_seconds) / 1048576.0 AS write_mb_per_sec,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY file_label, 2
        HAVING COUNT(rated_seconds) > 0
        ORDER BY file_label, 2
        """;

    /// <summary>
    /// Per-file I/O-latency points (top 10 files by delta ops) for one server over the window, for the
    /// File I/O tab's Latency sub-tab.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (file), like
    /// <c>GetWaitStatsTrendsByTypesAsync</c>; <paramref name="serverId"/>'s window alone decides the width
    /// (<c>seriesCount</c> is always 1 into <see cref="TrendBuckets.AutoMinutes"/>, the ruling's own "per
    /// series" wording). When every bucket the call returns holds exactly one physical collection (any file,
    /// any bucket), every point is stamped at its own raw <c>first_collection_time</c> instead of the
    /// <c>bucket_start</c> grid line — see <see cref="FileIoLatencyPoint"/>.</para>
    /// </summary>
    public async Task<List<FileIoLatencyPoint>> GetFileIoLatencyTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoLatencyPoint>();

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(FileIoLatencyTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string DatabaseName, string FileName, DateTime BucketStart, DateTime FirstCollectionTime,
            double AvgRead, double AvgWrite, double AvgQueuedRead, double AvgQueuedWrite)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(8) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.GetDateTime(2),
                reader.GetDateTime(7),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6)));
        }

        foreach (var row in rows)
        {
            items.Add(new FileIoLatencyPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.DatabaseName,
                row.FileName,
                row.AvgRead,
                row.AvgWrite,
                row.AvgQueuedRead,
                row.AvgQueuedWrite));
        }

        return items;
    }

    /// <summary>
    /// Per-file throughput points (MB/s, top 10 files by delta bytes) for one server over the window,
    /// for the File I/O tab's Throughput sub-tab.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (file), exactly like
    /// <see cref="GetFileIoLatencyTrendAsync"/> — see its remarks for the width and singleton-stamping rules,
    /// which this read shares verbatim.</para>
    /// </summary>
    public async Task<List<FileIoThroughputPoint>> GetFileIoThroughputTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoThroughputPoint>();

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(FileIoThroughputTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string FileLabel, DateTime BucketStart, DateTime FirstCollectionTime, double Read, double Write)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(5) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(4),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3)));
        }

        foreach (var row in rows)
        {
            items.Add(new FileIoThroughputPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.FileLabel,
                row.Read,
                row.Write));
        }

        return items;
    }
}
