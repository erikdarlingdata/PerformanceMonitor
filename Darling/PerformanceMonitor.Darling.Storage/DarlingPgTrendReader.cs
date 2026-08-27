// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Time-series reads for PostgreSQL (#2663). The service shipped fourteen trend reads and none of them
/// worked on a PostgreSQL target, so "is this getting worse" — the question a monitoring tool exists to
/// answer — had no PostgreSQL answer, while every <c>get_pg_*</c> read described a single window.
///
/// <para><b>Both series difference CONSECUTIVE snapshots, not the window's ends.</b> That is the whole
/// difference between a trend and the window reads next door: those two want one number for the window and
/// take newest-minus-oldest, while a shape over time needs each interval on its own.</para>
///
/// <para><b>A counter going backwards is a RESET, not negative work</b>, and this follows
/// <see cref="DarlingPgWaitSamplingReader"/> rather than inventing a second rule.
/// <c>pg_wait_sampling_reset_profile()</c> and a server restart both zero the profile, and the second
/// happens without anyone deciding to. <c>GREATEST(delta, 0)</c> would report a quiet interval across a
/// restart — the one reading that is definitely wrong, because the server was not idle. The interval takes
/// the new value whole, which is everything since the reset, and says so per point.</para>
/// </summary>
public static class DarlingPgTrendReader
{
    /// <param name="EstimatedWaitMsPerSecond">Samples in the interval × <c>profile_period_ms</c>, over the
    /// interval's length. Per SECOND because collection intervals are not uniform — a restart or a slow
    /// cycle stretches one — and a raw per-interval total would show that as a spike in the data.</param>
    /// <param name="CounterReset">The profile was reset inside this interval, so the point covers only the
    /// time since the reset rather than the whole interval.</param>
    public readonly record struct PgWaitTrendPoint(
        DateTime CollectionTimeUtc,
        long SampleCount,
        double EstimatedWaitMsPerSecond,
        int BackendCount,
        bool CounterReset);

    /// <param name="MeanExecMs">The interval's total execution time over its calls — what one execution
    /// cost during that interval, which is the number a regression moves.</param>
    public readonly record struct PgQueryDurationTrendPoint(
        DateTime CollectionTimeUtc,
        long Calls,
        double TotalExecMs,
        double MeanExecMs,
        double CallsPerSecond);

    /// <summary>
    /// One wait event over time, summed across the queries that waited on it.
    ///
    /// <para>Summed across <c>query_id</c> because the question this answers is about the SERVER — is this
    /// wait growing — and the per-query attribution is what <c>get_pg_wait_sampling</c> already serves for a
    /// single window. Keeping the split here would make every point a set rather than a value.</para>
    /// </summary>
    public const string WaitTrendSql = """
        WITH per_snapshot AS (
            SELECT
                collection_time,
                SUM(sample_count)               AS samples,
                MAX(profile_period_ms)          AS profile_period_ms,
                MAX(backend_count)              AS backend_count
            FROM pg_wait_sampling
            WHERE server_id = $1
            AND   event = $2
            AND   collection_time >= $3
            AND   collection_time <= $4
            GROUP BY collection_time
        ),
        differenced AS (
            SELECT
                collection_time,
                samples,
                profile_period_ms,
                backend_count,
                LAG(samples) OVER (ORDER BY collection_time) AS prev_samples,
                extract(epoch FROM (collection_time - LAG(collection_time) OVER (ORDER BY collection_time))) AS interval_seconds
            FROM per_snapshot
        )
        SELECT
            collection_time,
            /* Across a reset the new value is taken WHOLE - it is everything since the reset, which is the
               only honest reading. GREATEST(delta, 0) would report zero waits across a restart. */
            CASE WHEN samples < prev_samples THEN samples ELSE samples - prev_samples END AS delta_samples,
            CASE
                WHEN interval_seconds > 0
                THEN (CASE WHEN samples < prev_samples THEN samples ELSE samples - prev_samples END)
                     * coalesce(profile_period_ms, 0)::double precision / interval_seconds
                ELSE 0
            END AS estimated_wait_ms_per_second,
            coalesce(backend_count, 0)          AS backend_count,
            (samples < prev_samples)            AS counter_reset
        FROM differenced
        /* The first snapshot has nothing to difference against. Dropping it is not a lost point: it is the
           interval's left edge, and reporting it as a value would report the whole uptime as one interval. */
        WHERE prev_samples IS NOT NULL
        ORDER BY collection_time
        """;

    /// <summary>
    /// One statement over time, by <c>queryid</c>.
    ///
    /// <para>Reads the <c>delta_*</c> columns the collector already computes rather than differencing the
    /// cumulative ones again. They are written on collection, where the previous reading is in hand, and
    /// re-deriving them here would give a DIFFERENT answer whenever a snapshot is missing — the collector's
    /// delta spans the gap it actually observed, and a LAG here would span the gap in the stored data.</para>
    /// </summary>
    public const string QueryDurationTrendSql = """
        WITH per_snapshot AS (
            SELECT
                collection_time,
                SUM(delta_calls)                AS calls,
                SUM(delta_total_exec_time_ms)   AS total_exec_ms,
                extract(epoch FROM (collection_time - LAG(collection_time) OVER (ORDER BY collection_time))) AS interval_seconds
            FROM pg_statement_stats
            WHERE server_id = $1
            AND   queryid = $2
            AND   collection_time >= $3
            AND   collection_time <= $4
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            coalesce(calls, 0)::bigint                      AS calls,
            coalesce(total_exec_ms, 0)::double precision    AS total_exec_ms,
            /* Null rather than zero when the statement did not run in this interval: a mean over no calls
               is not 0 ms, it is absent, and a zero would draw a line through the floor of the chart at
               exactly the moments the query was idle. */
            CASE WHEN coalesce(calls, 0) > 0
                 THEN coalesce(total_exec_ms, 0)::double precision / calls
                 ELSE NULL
            END                                             AS mean_exec_ms,
            CASE WHEN interval_seconds > 0
                 THEN coalesce(calls, 0)::double precision / interval_seconds
                 ELSE 0
            END                                             AS calls_per_second
        FROM per_snapshot
        ORDER BY collection_time
        """;


    /// <summary>
    /// The wait event with the most samples in the window, so a caller that has not chosen one gets the
    /// event that actually dominates this server rather than a name somebody guessed. Null when nothing was
    /// sampled.
    ///
    /// <para>Ranked on the DIFFERENCE across the window, not on the newest cumulative value — the profile is
    /// cumulative, so ranking it raw would return whichever event has been accumulating longest since the
    /// server started, which is a fact about uptime rather than about now.</para>
    ///
    /// <para><b><c>event_type = 'CPU'</c> is excluded from the CHOICE, and only from the choice.</b> That
    /// class means the backend was NOT waiting, and on any healthy server it dominates the profile by a wide
    /// margin — measured on the rig, <c>Running</c> grew by 1,534 samples against 194 for the next event.
    /// Picking it as the default for a WAIT trend answers the opposite of the question asked. It stays
    /// askable by name, because how much time backends spend on CPU is a real signal; it is just never the
    /// automatic answer to "what is this server waiting on".</para>
    /// </summary>
    public const string DominantWaitEventSql = """
        WITH bounds AS (
            SELECT
                event,
                MIN(sample_count) AS lo,
                MAX(sample_count) AS hi
            FROM (
                SELECT event, collection_time, SUM(sample_count) AS sample_count
                FROM pg_wait_sampling
                WHERE server_id = $1
                AND   event IS NOT NULL
                AND   coalesce(event_type, '') <> 'CPU'
                AND   collection_time >= $2
                AND   collection_time <= $3
                GROUP BY event, collection_time
            ) AS per_snapshot
            GROUP BY event
        )
        SELECT event
        FROM bounds
        ORDER BY (hi - lo) DESC, event
        LIMIT 1
        """;

    /// <summary>
    /// The statement that spent the most execution time in the window. Same purpose as
    /// <see cref="DominantWaitEventSql"/>: a caller with no queryid gets the one worth looking at.
    /// </summary>
    public const string TopQueryIdSql = """
        SELECT queryid
        FROM pg_statement_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY queryid
        HAVING SUM(delta_total_exec_time_ms) > 0
        ORDER BY SUM(delta_total_exec_time_ms) DESC
        LIMIT 1
        """;

    public static async Task<string?> GetDominantWaitEventAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(DominantWaitEventSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value as string;
    }

    public static async Task<long?> GetTopQueryIdAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(TopQueryIdSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is long id ? id : null;
    }

    public static async Task<List<PgWaitTrendPoint>> GetWaitTrendAsync(
        NpgsqlDataSource postgres, int serverId, string waitEvent, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var points = new List<PgWaitTrendPoint>();
        await using var command = postgres.CreateCommand(WaitTrendSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(waitEvent);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then converts these naive columns at the store session's
           TimeZone, which silently empties the window east of UTC. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new PgWaitTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : (int)reader.GetInt64(3),
                !reader.IsDBNull(4) && reader.GetBoolean(4)));
        }

        return points;
    }

    public static async Task<List<PgQueryDurationTrendPoint>> GetQueryDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, long queryId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var points = new List<PgQueryDurationTrendPoint>();
        await using var command = postgres.CreateCommand(QueryDurationTrendSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new PgQueryDurationTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
        }

        return points;
    }
}
