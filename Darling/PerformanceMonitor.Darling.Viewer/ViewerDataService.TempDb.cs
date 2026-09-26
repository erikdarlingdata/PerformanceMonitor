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
/// One tempdb space-usage sample for the tempdb tab's usage and allocated-size charts — a mirror of
/// Lite's <c>TempDbRow</c> (<c>LocalDataService.TempDb.cs</c>). The usage chart plots the three
/// reserved-space series; the allocated chart plots <c>TotalReservedMb + UnallocatedMb</c>. The
/// session columns (<see cref="TotalSessionsUsingTempDb"/>/<see cref="TopSessionId"/>/
/// <see cref="TopSessionTempDbMb"/>) are carried to keep the port of Lite's query column-for-column,
/// even though the three charts don't render them yet.
/// </summary>
public sealed record TempDbSample(
    DateTime CollectionTime,
    double UserObjectReservedMb,
    double InternalObjectReservedMb,
    double VersionStoreReservedMb,
    double TotalReservedMb,
    double UnallocatedMb,
    long TotalSessionsUsingTempDb,
    int TopSessionId,
    double TopSessionTempDbMb);

/// <summary>
/// One tempdb file's I/O-latency point — a bucket (or, for a singleton bucket, a raw collection) time,
/// the file name, and the read/write average latencies (ms). Mirror of Lite's <c>FileIoTrendPoint</c> as
/// the tempdb file-I/O chart consumes it; the chart sums read+write latency per file and cycles a color
/// per file.
/// <para>#4234: <c>CollectionTime</c> is a <c>date_bin</c> bucket start, EXCEPT when every bucket the call
/// returned holds exactly one physical collection, in which case every point is stamped at its own raw
/// collection time instead — see <see cref="ViewerDataService.GetTempDbFileIoTrendAsync"/>.</para>
/// </summary>
public sealed record TempDbFileIoSample(
    DateTime CollectionTime,
    string FileName,
    double AvgReadLatencyMs,
    double AvgWriteLatencyMs);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The tempdb usage/size read — Lite's <c>GetTempDbTrendAsync</c> ported to Postgres. Reads
    /// <c>v_tempdb_stats</c> (a <c>SELECT *</c> passthrough over <c>tempdb_stats</c> in the Darling
    /// store) to mirror Lite's view-based query verbatim. The MB columns are <c>numeric(18,2)</c>, so
    /// each is CAST to double precision for the typed GetDouble reader (the established viewer
    /// convention); <c>total_sessions_using_tempdb</c> is <c>bigint</c> (GetInt64 — GetInt32 would
    /// throw against a bigint), <c>top_session_id</c> is <c>integer</c>.
    /// $1 server_id, $2 window start (naive UTC).
    /// </summary>
    public const string TempDbTrendSql = """
        SELECT
            collection_time,
            CAST(user_object_reserved_mb AS double precision) AS user_object_reserved_mb,
            CAST(internal_object_reserved_mb AS double precision) AS internal_object_reserved_mb,
            CAST(version_store_reserved_mb AS double precision) AS version_store_reserved_mb,
            CAST(total_reserved_mb AS double precision) AS total_reserved_mb,
            CAST(unallocated_mb AS double precision) AS unallocated_mb,
            total_sessions_using_tempdb,
            top_session_id,
            CAST(top_session_tempdb_mb AS double precision) AS top_session_tempdb_mb
        FROM v_tempdb_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        ORDER BY collection_time
        """;

    /// <summary>
    /// The tempdb per-file I/O-latency read — Lite's <c>GetTempDbFileIoTrendAsync</c> ported to Postgres,
    /// then bucketed (#4234), matching Lite's own bucketed twin (<c>LocalDataService.FileIo.cs</c>,
    /// #4340) column-for-column. Reads <c>v_file_io_stats</c> (a <c>SELECT *</c> passthrough over
    /// <c>file_io_stats</c>) to mirror Lite's view-based query, filtered to tempdb, and computes average
    /// read/write latency per bucket (stall ms / operation count) with the delta-stall sums CAST to
    /// double precision before division. Grouped by file_name so each tempdb data file is its own series
    /// — no top-10 ranking pass, unlike <see cref="FileIoLatencyTrendSql"/>, since tempdb's own file count
    /// is already small.
    /// <para>#3540, carried into the bucket exactly like <see cref="FileIoLatencyTrendSql"/>: a row whose
    /// stored <c>sample_interval_seconds</c> is 0 (no delta knowable — first sighting, counter reset, a
    /// gap past the policy) has its deltas nulled out of the sums via the <c>rated</c> CTE rather than
    /// contributing a confident 0, exactly as the pre-bucket read dropped the row outright. <c>IS DISTINCT
    /// FROM 0</c> still keeps pre-V127 rows (NULL: interval never recorded). The row is NOT removed from
    /// the FROM clause — it still counts toward <c>collection_count</c> below, so a bucket is never
    /// mistaken for a true singleton just because its one physical collection happened to be unrated.
    /// <c>HAVING COUNT(rated_reads) &gt; 0</c> drops a bucket whose every row was unrated, matching the
    /// pre-bucket read's "absent, not 0.00 ms" answer for that case.</para>
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket width minutes.
    /// </summary>
    public const string TempDbFileIoTrendSql = $$"""
        WITH rated AS (
            SELECT
                file_name,
                collection_time,
                /* #3540: see FileIoLatencyTrendSql's rated CTE for why this nulls rather than filters. */
                CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN delta_reads END AS rated_reads,
                CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN delta_writes END AS rated_writes,
                CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(delta_stall_read_ms AS double precision) END AS rated_stall_read_ms,
                CASE WHEN sample_interval_seconds IS DISTINCT FROM 0 THEN CAST(delta_stall_write_ms AS double precision) END AS rated_stall_write_ms
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   database_name = 'tempdb'
        )
        SELECT
            file_name,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            CASE WHEN SUM(rated_reads) > 0 THEN SUM(rated_stall_read_ms) / SUM(rated_reads) ELSE 0 END AS avg_read_latency_ms,
            CASE WHEN SUM(rated_writes) > 0 THEN SUM(rated_stall_write_ms) / SUM(rated_writes) ELSE 0 END AS avg_write_latency_ms,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY file_name, 2
        HAVING COUNT(rated_reads) > 0
        ORDER BY file_name, 2
        """;

    /// <summary>
    /// tempdb space-usage samples for one server since <paramref name="sinceUtc"/>, time-ordered, for
    /// the tempdb tab's usage and allocated-size charts.
    /// </summary>
    public async Task<List<TempDbSample>> GetTempDbTrendAsync(int serverId, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<TempDbSample>();

        await using var command = _dataSource.CreateCommand(TempDbTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new TempDbSample(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                /* bigint — GetInt64; GetInt32 would throw against the bigint column. */
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8)));
        }

        return samples;
    }

    /// <summary>
    /// tempdb per-file I/O-latency points for one server over the window, file- then bucket-ordered, for
    /// the tempdb tab's file-I/O chart.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget, exactly like
    /// <see cref="GetFileIoLatencyTrendAsync"/> — see its remarks for the width and singleton-stamping
    /// rules, which this read shares verbatim (<paramref name="endUtc"/> replaces the old start-only
    /// window so the SQL can bucket on a known width).</para>
    /// </summary>
    public async Task<List<TempDbFileIoSample>> GetTempDbFileIoTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<TempDbFileIoSample>();

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(TempDbFileIoTrendSql);
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

        var rows = new List<(string FileName, DateTime BucketStart, DateTime FirstCollectionTime, double AvgRead, double AvgWrite)>();
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
            samples.Add(new TempDbFileIoSample(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.FileName,
                row.AvgRead,
                row.AvgWrite));
        }

        return samples;
    }
}
