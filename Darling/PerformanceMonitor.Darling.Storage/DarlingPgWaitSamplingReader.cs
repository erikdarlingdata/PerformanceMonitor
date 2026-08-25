/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads <c>collect.pg_wait_sampling</c> — wait events attributed to the query that waited (#2603).
///
/// <para><b>This one deltas, and it has to.</b> The collector stores the profile CUMULATIVE, exactly as
/// <c>pg_wait_sampling</c> reports it. Showing that raw would rank waits by how long the server has been
/// up, so the newest reading is differenced against the oldest one in the window.</para>
///
/// <para><b>A counter that goes BACKWARDS is a reset, not a negative wait.</b>
/// <c>pg_wait_sampling_reset_profile()</c> and a server restart both zero the profile, and the second of
/// those happens without anyone deciding to. <c>GREATEST(newest - oldest, 0)</c> would quietly report zero
/// waits across a restart; instead the newest value is used whole when it is smaller than the oldest, which
/// is the only honest reading — everything since the reset — and <c>counter_reset</c> says so rather than
/// leaving the reader to wonder.</para>
///
/// <para><b><c>sample_count</c> is samples, and <c>estimated_wait_ms</c> is the inference made explicit.</b>
/// The multiplication by <c>profile_period_ms</c> happens here, once, beside the number it derives from,
/// rather than in each caller that might use a different period.</para>
/// </summary>
public static class DarlingPgWaitSamplingReader
{
    /// <param name="EventType">Wait class. <c>CPU</c> means the backend was NOT waiting.</param>
    /// <param name="QueryId">Joins <c>pg_statement_stats.queryid</c>. Zero is a wait belonging to no
    /// statement — a background process — kept so attributed and unattributed waits stay distinguishable.</param>
    /// <param name="SampleCount">Samples observed IN THE WINDOW, already differenced.</param>
    /// <param name="EstimatedWaitMs"><c>SampleCount × profile_period_ms</c>. An estimate from a sampling
    /// profiler, not a measured duration.</param>
    /// <param name="CounterReset">True when the profile was reset inside the window, so the figure covers
    /// only the time since the reset rather than the whole window.</param>
    public sealed record PgWaitSamplingRow(
        string? EventType,
        string? Event,
        long QueryId,
        long SampleCount,
        long EstimatedWaitMs,
        int BackendCount,
        bool CounterReset,
        DateTime CaptureTime);

    /* Newest and oldest per key in one pass each, then differenced. Two DISTINCT ON scans rather than a
       window function because the key is compound and the hypertable is ordered by time - the same idiom
       every other reader here uses.

       The key does NOT include database_name: the profile is cluster-wide and the table carries no such
       column, deliberately (#2599 is about not inventing that attribution). */
    public const string PgWaitSamplingSql = """
        WITH newest AS (
            SELECT DISTINCT ON (event_type, event, query_id)
                   event_type, event, query_id, sample_count, profile_period_ms, backend_count, collection_time
            FROM pg_wait_sampling
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY event_type, event, query_id, collection_time DESC
        ),
        oldest AS (
            SELECT DISTINCT ON (event_type, event, query_id)
                   event_type, event, query_id, sample_count
            FROM pg_wait_sampling
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY event_type, event, query_id, collection_time ASC
        )
        SELECT
            n.event_type,
            n.event,
            n.query_id,
            CASE WHEN n.sample_count < o.sample_count
                 THEN n.sample_count
                 ELSE n.sample_count - coalesce(o.sample_count, 0)
            END AS sample_count,
            n.profile_period_ms,
            n.backend_count,
            (n.sample_count < o.sample_count) AS counter_reset,
            n.collection_time
        FROM newest AS n
        LEFT JOIN oldest AS o
          ON  o.event_type IS NOT DISTINCT FROM n.event_type
          AND o.event      IS NOT DISTINCT FROM n.event
          AND o.query_id   = n.query_id
        ORDER BY 4 DESC, n.event_type, n.event
        LIMIT $4
        """;

    public static async Task<List<PgWaitSamplingRow>> GetPgWaitSamplingAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgWaitSamplingRow>();
        await using var command = postgres.CreateCommand(PgWaitSamplingSql);
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same as every other PostgreSQL read here: Npgsql infers
           timestamptz from Kind=Utc and PostgreSQL then resolves the comparison against these NAIVE columns
           at the store session's TimeZone, so east of UTC the window slides off the data entirely. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var samples = reader.IsDBNull(3) ? 0 : reader.GetInt64(3);
            var periodMs = reader.IsDBNull(4) ? 10 : reader.GetInt32(4);

            rows.Add(new PgWaitSamplingRow(
                EventType: reader.IsDBNull(0) ? null : reader.GetString(0),
                Event: reader.IsDBNull(1) ? null : reader.GetString(1),
                QueryId: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                SampleCount: samples,
                EstimatedWaitMs: samples * periodMs,
                BackendCount: reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                CounterReset: !reader.IsDBNull(6) && reader.GetBoolean(6),
                CaptureTime: reader.IsDBNull(7)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)));
        }

        return rows;
    }
}
