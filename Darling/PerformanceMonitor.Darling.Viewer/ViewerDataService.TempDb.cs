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
/// One tempdb file's I/O-latency point — a collection time, the file name, and the read/write average
/// latencies (ms) for that collection. Mirror of Lite's <c>FileIoTrendPoint</c> as the tempdb file-I/O
/// chart consumes it; the chart sums read+write latency per file and cycles a color per file.
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
    /// The tempdb per-file I/O-latency read — Lite's <c>GetTempDbFileIoTrendAsync</c> ported to
    /// Postgres. Reads <c>v_file_io_stats</c> (a <c>SELECT *</c> passthrough over <c>file_io_stats</c>)
    /// to mirror Lite's view-based query, filtered to tempdb, and computes per-collection average
    /// read/write latency (stall ms / operation count) with the delta-stall sums CAST to double
    /// precision before division. Grouped by (collection_time, file_name) so each tempdb data file is
    /// its own series.
    /// $1 server_id, $2 window start (naive UTC).
    /// </summary>
    public const string TempDbFileIoTrendSql = """
        SELECT
            collection_time,
            file_name,
            CASE WHEN SUM(delta_reads) > 0 THEN SUM(CAST(delta_stall_read_ms AS double precision)) / SUM(delta_reads) ELSE 0 END AS avg_read_latency_ms,
            CASE WHEN SUM(delta_writes) > 0 THEN SUM(CAST(delta_stall_write_ms AS double precision)) / SUM(delta_writes) ELSE 0 END AS avg_write_latency_ms
        FROM v_file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   database_name = 'tempdb'
        GROUP BY collection_time, file_name
        ORDER BY collection_time, file_name
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
    /// tempdb per-file I/O-latency points for one server since <paramref name="sinceUtc"/>,
    /// time- then file-ordered, for the tempdb tab's file-I/O chart.
    /// </summary>
    public async Task<List<TempDbFileIoSample>> GetTempDbFileIoTrendAsync(int serverId, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<TempDbFileIoSample>();

        await using var command = _dataSource.CreateCommand(TempDbFileIoTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new TempDbFileIoSample(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3)));
        }

        return samples;
    }
}
