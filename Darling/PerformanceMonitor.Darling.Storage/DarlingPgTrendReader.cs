// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Time-series reads for PostgreSQL (#2663). The service shipped fourteen trend reads and none of them
/// worked on a PostgreSQL target, so "is this getting worse" — the question a monitoring tool exists to
/// answer — had no PostgreSQL answer, while every <c>get_pg_*</c> read described a single window.
///
/// <para><b>Every series here differences CONSECUTIVE snapshots, not the window's ends.</b> That is the
/// whole difference between a trend and the window reads next door: those want one number for the window
/// and take newest-minus-oldest, while a shape over time needs each interval on its own.</para>
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

    /// <param name="IntervalSeconds">The interval this point covers. Carried out rather than left implicit
    /// because the rates beside it are only interpretable with it: collection cadence is not uniform, and a
    /// point covering 75 seconds instead of 60 is the ordinary case on a busy sweep, not an anomaly.</param>
    /// <param name="AvgReadMs">Null when nothing was read — and nulled again by the tool when
    /// <c>track_io_timing</c> is off, where the underlying time is 0 and the quotient would claim
    /// sub-microsecond I/O rather than an unmeasured one.</param>
    /// <param name="WriteCountersTracked">False on Amazon Aurora, where backends do not write data files and
    /// the whole write side is NULL. The zeros beside it then mean unmeasured, not idle.</param>
    /// <param name="BytesMeasured">PostgreSQL 18's measured byte totals are present.
    /// <paramref name="BytesEstimable"/> is the pre-18 alternative — <c>op_bytes</c>, the block size, times
    /// the operation count. Exactly one of the two is true on any given server, and they are different
    /// quantities: 18 moves several blocks per operation, so the older estimate undercounts there.</param>
    /// <remarks>A <c>sealed record</c>, not the <c>readonly record struct</c> the two trend points above
    /// are, on the size rule <see cref="DarlingPgIoReader.PgIoRow"/> already follows: those carry five
    /// fields and this carries twenty-four, and a struct that wide is copied whole by every LINQ pass over
    /// a thousand-point series.</remarks>
    public sealed record PgIoTrendPoint(
        DateTime CollectionTimeUtc,
        double IntervalSeconds,
        long Reads,
        double ReadTimeMs,
        long Writes,
        double WriteTimeMs,
        long Extends,
        long Hits,
        long Evictions,
        decimal ReadBytes,
        decimal WriteBytes,
        double ReadsPerSecond,
        double WritesPerSecond,
        double ExtendsPerSecond,
        double HitsPerSecond,
        double ReadBytesPerSecond,
        double WriteBytesPerSecond,
        double? AvgReadMs,
        double? AvgWriteMs,
        double? CacheHitPct,
        bool WriteCountersTracked,
        bool BytesMeasured,
        bool BytesEstimable,
        bool CounterReset);

    /// <param name="RollbackPct">Null rather than zero for an interval with no completed transactions: a
    /// ratio over nothing is absent, and a zero would draw a healthy-looking floor at exactly the intervals
    /// the database was idle.</param>
    /// <param name="CacheHitPct">The hit ratio for THIS interval, which is the whole reason this read
    /// exists. <c>pg_stat_database</c>'s counters are cumulative since the last reset, so the ratio computed
    /// from them raw is a lifetime average that barely moves — a database that fell off a cliff an hour ago
    /// still reports 99% because of the weeks behind it. Differenced per interval, the cliff is visible.</param>
    /// <param name="Deadlocks">A COUNT for the interval, deliberately not a rate. Deadlocks are discrete
    /// server-recorded events a few per hour at worst, and per-second would render every real one as a
    /// number with four leading zeros.</param>
    /// <remarks>A <c>sealed record</c> for the same reason as <see cref="PgIoTrendPoint"/>, and so the two
    /// reads this PR adds do not differ in a way that means nothing.</remarks>
    public sealed record PgDatabaseTrendPoint(
        DateTime CollectionTimeUtc,
        long XactCommit,
        long XactRollback,
        double? RollbackPct,
        double TransactionsPerSecond,
        long BlksRead,
        long BlksHit,
        double? CacheHitPct,
        long TempFiles,
        long TempBytes,
        double TempBytesPerSecond,
        long Deadlocks,
        bool CounterReset);

    /// <summary>
    /// One (backend_type, context) pair from <c>pg_io_stats</c> over time, summed across object types.
    ///
    /// <para><b>The subject is a PAIR because a hit ratio summed across contexts means nothing.</b> The
    /// single-window read already says why: <c>bulkread</c> is a sequential scan deliberately using a small
    /// ring buffer so it cannot evict the pool, so averaging its misses together with the normal context's
    /// understates both, and the two have opposite remedies — more <c>shared_buffers</c> helps one and
    /// cannot help the other. Object type IS summed over: WAL rows carry no <c>hits</c> at all, so folding
    /// them in adds write volume without distorting the ratio.</para>
    ///
    /// <para><b>These are differenced with <c>LAG</c> because the collector writes no <c>delta_*</c>
    /// columns for this table.</b> Where it does — <c>pg_statement_stats</c>, above — those are used
    /// instead, since the collector's delta spans the interval it OBSERVED while a read-time <c>LAG</c>
    /// spans the gap in the STORED data, and the two differ whenever a snapshot is missing. Checked against
    /// the stored schema rather than assumed: <c>pg_io_stats</c> and <c>pg_database_stats</c> both store the
    /// cumulative counters only.</para>
    ///
    /// <para><b>A counter going backwards is a RESET, and the interval takes the new value WHOLE.</b>
    /// <c>GREATEST(delta, 0)</c> — which the single-window read uses, correctly, because it wants one total
    /// for the window — would report a QUIET interval across a <c>pg_stat_reset_shared('io')</c> or a
    /// restart. That is the one reading that is definitely wrong: the server was not idle. Both reset
    /// signals from the database read are carried, because neither sees every reset: the explicit
    /// <c>stats_reset</c> move catches a reset that climbed back past its old value inside one interval
    /// (where every difference is positive and nothing looks unusual), and the implicit rewind catches a
    /// crash restart, where <c>stats_reset</c> can stay NULL through a genuine loss.</para>
    ///
    /// <para>A combination first SEEN mid-window contributes zero to the interval it appears in rather than
    /// its cumulative value, which is everything since server start and would be a spike made of history.
    /// Verified against controlled rows: a series arriving with 5,000,000 reads added 100, not 5,000,100.</para>
    ///
    /// <para>$1 server_id, $2 backend_type, $3 context, $4/$5 window (naive UTC).</para>
    /// </summary>
    public const string IoTrendSql = """
        WITH bounded AS (
            SELECT
                collection_time,
                object_type,
                stats_reset,
                op_bytes,
                reads,
                read_time_ms,
                writes,
                write_time_ms,
                extends,
                hits,
                evictions,
                read_bytes,
                write_bytes
            FROM pg_io_stats
            WHERE server_id = $1
            AND   backend_type = $2
            AND   context = $3
            AND   collection_time >= $4
            AND   collection_time <= $5
        ),
        /* The interval comes from the DISTINCT collection times of the series being followed, not from a
           LAG inside each object_type's own window. Object types do not all appear in every snapshot, so a
           per-partition interval would differ between rows of one snapshot and there would be no single
           honest denominator for the rates below. */
        spans AS (
            SELECT
                collection_time,
                CAST(extract(epoch FROM (collection_time - LAG(collection_time) OVER (ORDER BY collection_time)))
                     AS double precision) AS interval_seconds
            FROM (SELECT DISTINCT collection_time FROM bounded) AS snapshots
        ),
        sampled AS (
            SELECT
                collection_time,
                op_bytes,
                reads,
                read_time_ms,
                writes,
                write_time_ms,
                extends,
                hits,
                evictions,
                read_bytes,
                write_bytes,
                reads         - LAG(reads)         OVER series AS raw_reads,
                read_time_ms  - LAG(read_time_ms)  OVER series AS raw_read_time_ms,
                writes        - LAG(writes)        OVER series AS raw_writes,
                write_time_ms - LAG(write_time_ms) OVER series AS raw_write_time_ms,
                extends       - LAG(extends)       OVER series AS raw_extends,
                hits          - LAG(hits)          OVER series AS raw_hits,
                evictions     - LAG(evictions)     OVER series AS raw_evictions,
                read_bytes    - LAG(read_bytes)    OVER series AS raw_read_bytes,
                write_bytes   - LAG(write_bytes)   OVER series AS raw_write_bytes,
                /* Whether this combination tracks writes AT ALL, as opposed to having written nothing.
                   Aurora reports NULL here across the board; a self-managed target reports numbers. */
                (writes IS NOT NULL)     AS writes_tracked,
                /* read_bytes is the probe for all three byte columns: a WAL row legitimately reports no
                   extend_bytes, so extend_bytes IS NOT NULL would answer "not measured" for a row that
                   simply does not extend. */
                (read_bytes IS NOT NULL) AS bytes_measured,
                (op_bytes IS NOT NULL)   AS bytes_estimable,
                /* ROW_NUMBER > 1, NOT `LAG(stats_reset) IS NOT NULL`. LAG is NULL in two situations a guard
                   on it cannot tell apart - there is no previous row, and there IS one whose stats_reset was
                   itself NULL - and the second is the common state, because stats_reset stays NULL until the
                   first reset ever. A guard on LAG would therefore miss a server's FIRST reset entirely. */
                (ROW_NUMBER() OVER series > 1
                 AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here
            FROM bounded
            WINDOW series AS (
                PARTITION BY object_type
                ORDER BY collection_time
            )
        ),
        differenced AS (
            SELECT
                collection_time,
                /* The reset rule, per counter: a value below its predecessor is everything SINCE the reset,
                   so it is taken whole. Clamping to zero here would report an idle interval across a
                   restart. A NULL LAG - the first sample of a series - yields NULL and is coalesced to 0
                   below, which is right: its cumulative value is history, not this interval's work. */
                CASE WHEN raw_reads         < 0 THEN reads         ELSE raw_reads         END AS d_reads,
                CASE WHEN raw_read_time_ms  < 0 THEN read_time_ms  ELSE raw_read_time_ms  END AS d_read_time_ms,
                CASE WHEN raw_writes        < 0 THEN writes        ELSE raw_writes        END AS d_writes,
                CASE WHEN raw_write_time_ms < 0 THEN write_time_ms ELSE raw_write_time_ms END AS d_write_time_ms,
                CASE WHEN raw_extends       < 0 THEN extends       ELSE raw_extends       END AS d_extends,
                CASE WHEN raw_hits          < 0 THEN hits          ELSE raw_hits          END AS d_hits,
                CASE WHEN raw_evictions     < 0 THEN evictions     ELSE raw_evictions     END AS d_evictions,
                /* One byte answer per row, measured where the server measures it and derived from the block
                   size where it does not. Never both and never silently swapped - bytes_measured says which,
                   and on 18 the derivation would UNDERCOUNT because a vectored read covers several blocks. */
                CASE
                    WHEN read_bytes IS NOT NULL
                    THEN CASE WHEN raw_read_bytes < 0 THEN read_bytes ELSE raw_read_bytes END
                    WHEN op_bytes IS NOT NULL
                    THEN (CASE WHEN raw_reads < 0 THEN reads ELSE raw_reads END)::numeric * op_bytes
                    ELSE NULL
                END AS d_read_bytes,
                CASE
                    WHEN write_bytes IS NOT NULL
                    THEN CASE WHEN raw_write_bytes < 0 THEN write_bytes ELSE raw_write_bytes END
                    WHEN op_bytes IS NOT NULL
                    THEN (CASE WHEN raw_writes < 0 THEN writes ELSE raw_writes END)::numeric * op_bytes
                    ELSE NULL
                END AS d_write_bytes,
                writes_tracked,
                bytes_measured,
                bytes_estimable,
                /* LEAST ignores NULLs, so an Aurora target's absent write side does not mask a rewind on the
                   read side, and the first sample of a series - every raw NULL - yields NULL rather than
                   false, which the coalesce below turns into "no reset seen". */
                (reset_here
                 OR LEAST(raw_reads, raw_read_time_ms, raw_writes, raw_write_time_ms,
                          raw_extends, raw_hits, raw_evictions, raw_read_bytes, raw_write_bytes) < 0) AS counter_reset
            FROM sampled
        ),
        per_snapshot AS (
            SELECT
                d.collection_time,
                s.interval_seconds,
                CAST(coalesce(SUM(d.d_reads), 0) AS bigint)     AS reads,
                coalesce(SUM(d.d_read_time_ms), 0)              AS read_time_ms,
                CAST(coalesce(SUM(d.d_writes), 0) AS bigint)    AS writes,
                coalesce(SUM(d.d_write_time_ms), 0)             AS write_time_ms,
                CAST(coalesce(SUM(d.d_extends), 0) AS bigint)   AS extends,
                CAST(coalesce(SUM(d.d_hits), 0) AS bigint)      AS hits,
                CAST(coalesce(SUM(d.d_evictions), 0) AS bigint) AS evictions,
                coalesce(SUM(d.d_read_bytes), 0)                AS read_bytes,
                coalesce(SUM(d.d_write_bytes), 0)               AS write_bytes,
                bool_or(d.writes_tracked)                       AS write_counters_tracked,
                bool_or(d.bytes_measured)                       AS bytes_measured,
                bool_or(d.bytes_estimable)                      AS bytes_estimable,
                coalesce(bool_or(d.counter_reset), false)       AS counter_reset
            FROM differenced AS d
            JOIN spans AS s
              ON s.collection_time = d.collection_time
            /* The first snapshot in the window has nothing to difference against. Dropping it is not a lost
               point: it is the interval's left edge, and reporting it would report the whole uptime as one
               interval. No HAVING beside it - an interval with no I/O is a real reading in a trend, unlike
               in the single-window read, where an all-zero combination is only noise in a ranked grid. */
            WHERE s.interval_seconds IS NOT NULL
            GROUP BY d.collection_time, s.interval_seconds
        )
        SELECT
            collection_time,
            interval_seconds,
            reads,
            read_time_ms,
            writes,
            write_time_ms,
            extends,
            hits,
            evictions,
            read_bytes,
            write_bytes,
            /* Per SECOND, because collection intervals are not uniform - measured on the rig at 60 s and
               75 s within one hour - and a per-interval total renders a slow sweep as a spike in the data
               rather than in the server. */
            CASE WHEN interval_seconds > 0 THEN reads::double precision / interval_seconds ELSE 0 END       AS reads_per_second,
            CASE WHEN interval_seconds > 0 THEN writes::double precision / interval_seconds ELSE 0 END      AS writes_per_second,
            CASE WHEN interval_seconds > 0 THEN extends::double precision / interval_seconds ELSE 0 END     AS extends_per_second,
            CASE WHEN interval_seconds > 0 THEN hits::double precision / interval_seconds ELSE 0 END        AS hits_per_second,
            CASE WHEN interval_seconds > 0 THEN read_bytes::double precision / interval_seconds ELSE 0 END  AS read_bytes_per_second,
            CASE WHEN interval_seconds > 0 THEN write_bytes::double precision / interval_seconds ELSE 0 END AS write_bytes_per_second,
            CASE WHEN reads  > 0 THEN read_time_ms / reads   ELSE NULL END AS avg_read_ms,
            CASE WHEN writes > 0 THEN write_time_ms / writes ELSE NULL END AS avg_write_ms,
            /* Scoped to this (backend_type, context) pair, which is the only scope where it means anything.
               Null, not zero, when nothing was accessed at all. */
            CASE WHEN hits + reads > 0
                 THEN hits::double precision / (hits + reads) * 100
                 ELSE NULL
            END AS cache_hit_pct,
            write_counters_tracked,
            bytes_measured,
            bytes_estimable,
            counter_reset
        FROM per_snapshot
        ORDER BY collection_time
        """;

    /// <summary>
    /// One database's <c>pg_stat_database</c> counters over time — temp-file spills, cache hit ratio,
    /// deadlocks and the rollback share, interval by interval.
    ///
    /// <para>Same reset rule as <see cref="IoTrendSql"/>, and the same two independent signals, for the
    /// same reasons. Only one series here, so the window needs no partition: the WHERE pins one database.</para>
    ///
    /// <para><b><c>database_name IS NOT DISTINCT FROM $2</c>, not <c>=</c>.</b> PostgreSQL emits one row
    /// with a NULL <c>datname</c> for shared-relation activity — the cluster-wide catalog, which belongs to
    /// no database — and that NULL is a real value, not missing data. An equality test would make it the one
    /// series this read could never follow.</para>
    ///
    /// <para>$1 server_id, $2 database_name (NULL = shared relations), $3/$4 window (naive UTC).</para>
    /// </summary>
    public const string DatabaseTrendSql = """
        WITH sampled AS (
            SELECT
                collection_time,
                CAST(extract(epoch FROM (collection_time - LAG(collection_time) OVER series))
                     AS double precision) AS interval_seconds,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                temp_files,
                temp_bytes,
                deadlocks,
                xact_commit   - LAG(xact_commit)   OVER series AS raw_xact_commit,
                xact_rollback - LAG(xact_rollback) OVER series AS raw_xact_rollback,
                blks_read     - LAG(blks_read)     OVER series AS raw_blks_read,
                blks_hit      - LAG(blks_hit)      OVER series AS raw_blks_hit,
                temp_files    - LAG(temp_files)    OVER series AS raw_temp_files,
                temp_bytes    - LAG(temp_bytes)    OVER series AS raw_temp_bytes,
                deadlocks     - LAG(deadlocks)     OVER series AS raw_deadlocks,
                (ROW_NUMBER() OVER series > 1
                 AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here
            FROM pg_database_stats
            WHERE server_id = $1
            AND   database_name IS NOT DISTINCT FROM $2
            AND   collection_time >= $3
            AND   collection_time <= $4
            WINDOW series AS (
                ORDER BY collection_time
            )
        ),
        differenced AS (
            SELECT
                collection_time,
                interval_seconds,
                CASE WHEN raw_xact_commit   < 0 THEN xact_commit   ELSE raw_xact_commit   END AS d_xact_commit,
                CASE WHEN raw_xact_rollback < 0 THEN xact_rollback ELSE raw_xact_rollback END AS d_xact_rollback,
                CASE WHEN raw_blks_read     < 0 THEN blks_read     ELSE raw_blks_read     END AS d_blks_read,
                CASE WHEN raw_blks_hit      < 0 THEN blks_hit      ELSE raw_blks_hit      END AS d_blks_hit,
                CASE WHEN raw_temp_files    < 0 THEN temp_files    ELSE raw_temp_files    END AS d_temp_files,
                CASE WHEN raw_temp_bytes    < 0 THEN temp_bytes    ELSE raw_temp_bytes    END AS d_temp_bytes,
                CASE WHEN raw_deadlocks     < 0 THEN deadlocks     ELSE raw_deadlocks     END AS d_deadlocks,
                (reset_here
                 OR LEAST(raw_xact_commit, raw_xact_rollback, raw_blks_read, raw_blks_hit,
                          raw_temp_files, raw_temp_bytes, raw_deadlocks) < 0) AS counter_reset
            FROM sampled
            WHERE interval_seconds IS NOT NULL
        )
        SELECT
            collection_time,
            coalesce(d_xact_commit, 0)   AS xact_commit,
            coalesce(d_xact_rollback, 0) AS xact_rollback,
            /* Null, not zero, for an interval with no completed transactions - a ratio over nothing is
               absent, and a zero would read as a perfectly healthy minute. */
            CASE WHEN coalesce(d_xact_commit, 0) + coalesce(d_xact_rollback, 0) > 0
                 THEN coalesce(d_xact_rollback, 0)::double precision
                      / (coalesce(d_xact_commit, 0) + coalesce(d_xact_rollback, 0)) * 100
                 ELSE NULL
            END                          AS rollback_pct,
            CASE WHEN interval_seconds > 0
                 THEN (coalesce(d_xact_commit, 0) + coalesce(d_xact_rollback, 0))::double precision / interval_seconds
                 ELSE 0
            END                          AS transactions_per_second,
            coalesce(d_blks_read, 0)     AS blks_read,
            coalesce(d_blks_hit, 0)      AS blks_hit,
            /* The interval's OWN hit ratio, which is the point of differencing at all: computed from the
               cumulative counters this is a lifetime average that barely moves, so a database that fell off
               a cliff an hour ago still reports 99%. */
            CASE WHEN coalesce(d_blks_hit, 0) + coalesce(d_blks_read, 0) > 0
                 THEN coalesce(d_blks_hit, 0)::double precision
                      / (coalesce(d_blks_hit, 0) + coalesce(d_blks_read, 0)) * 100
                 ELSE NULL
            END                          AS cache_hit_pct,
            coalesce(d_temp_files, 0)    AS temp_files,
            coalesce(d_temp_bytes, 0)    AS temp_bytes,
            CASE WHEN interval_seconds > 0
                 THEN coalesce(d_temp_bytes, 0)::double precision / interval_seconds
                 ELSE 0
            END                          AS temp_bytes_per_second,
            /* A count, not a rate. Deadlocks are discrete server-recorded events at a few per hour on a bad
               day, and per-second would render every real one as four leading zeros. */
            coalesce(d_deadlocks, 0)     AS deadlocks,
            coalesce(counter_reset, false) AS counter_reset
        FROM differenced
        ORDER BY collection_time
        """;

    /// <summary>
    /// The (backend_type, context) pair that moved the most I/O in the window, so a caller that has not
    /// chosen one gets the combination that actually dominates this server.
    ///
    /// <para><b>Ranked on OPERATIONS, not on read time — and that is a change the rig forced.</b> The
    /// single-window read orders by <c>read_time_ms DESC</c>, which is the better ranking when it is
    /// populated. It is usually not: <c>track_io_timing</c> is <b>off by default</b> in PostgreSQL and was
    /// off on both rig targets, so every <c>read_time_ms</c> in the store is 0.0 and a ranking on it
    /// resolves to the tiebreak — which for a subject CHOICE means picking a name alphabetically and calling
    /// it the busiest thing on the server. Reads plus writes plus extends is always populated.</para>
    ///
    /// <para><b>Nothing is excluded from the choice</b>, deliberately unlike
    /// <see cref="DominantWaitEventSql"/>. That one has to skip the CPU class because
    /// <c>pg_wait_sampling</c>'s <c>Running</c> means the backend was NOT waiting, so defaulting to it
    /// answers the opposite of the question. <c>pg_stat_io</c> has no such row: every combination in it is
    /// real device work. A checkpointer flushing hard IS the finding on a server whose
    /// <c>max_wal_size</c> is too small, and hiding it behind a policy would be inventing a blind spot. On
    /// the rig the pick differs by major — <c>client backend</c>/<c>bulkread</c> on 17,
    /// <c>checkpointer</c>/<c>normal</c> on 18 — and each is genuinely the busiest thing there.</para>
    ///
    /// <para><b>Buffer hits QUALIFY a pair but do not rank it</b>, which running it on a quiet window
    /// forced. Operations decide the order, because physical I/O is what the trend is about — but a fully
    /// cached workload is the healthy state, and on one every combination has hits and no operations at
    /// all, so a filter over operations alone answered "nothing to follow" for a server that is perfectly
    /// observable. Hits also break the tie ahead of the name, or a window where nothing touched a disk
    /// falls through to alphabetical order — the same defect as ranking on an unmeasured read time, from a
    /// different direction.</para>
    ///
    /// <para><c>GREATEST(delta, 0)</c> here rather than the take-it-whole rule, because this is a RANKING
    /// over the window and not a reading of an interval: clamping bounds a reset's contribution instead of
    /// letting one restart hand the default to whichever combination happened to be reset.</para>
    ///
    /// <para>NULL backend_type and context rows are filtered out because the trend matches them with
    /// <c>=</c>: a pair the trend could never follow must not be the pair it recommends.</para>
    ///
    /// <para><b>$4 and $5 CONSTRAIN the choice rather than replacing it.</b> Either half of the subject can
    /// be named on its own, and the other is then chosen within it — "the busiest context for the
    /// autovacuum worker" is a question somebody asks, and the alternative was to accept half a subject and
    /// silently answer about a different backend type. The nulls are cast explicitly so a prepared statement
    /// can infer their type; spelling the filter inline rather than composing the SQL keeps the shipped text
    /// valid on its own, which is what the parse-analysis pin requires.</para>
    /// </summary>
    public const string DominantIoSubjectSql = """
        WITH sampled AS (
            SELECT
                backend_type,
                context,
                GREATEST(reads   - LAG(reads)   OVER series, 0) AS d_reads,
                GREATEST(writes  - LAG(writes)  OVER series, 0) AS d_writes,
                GREATEST(extends - LAG(extends) OVER series, 0) AS d_extends,
                GREATEST(hits    - LAG(hits)    OVER series, 0) AS d_hits
            FROM pg_io_stats
            WHERE server_id = $1
            AND   backend_type IS NOT NULL
            AND   context IS NOT NULL
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text IS NULL OR backend_type = $4)
            AND   ($5::text IS NULL OR context = $5)
            WINDOW series AS (
                PARTITION BY backend_type, object_type, context
                ORDER BY collection_time
            )
        )
        SELECT
            backend_type,
            context
        FROM sampled
        GROUP BY backend_type, context
        /* Buffer HITS qualify a pair as active, even though they rank below operations. A fully cached
           workload is the HEALTHY state, and on one every combination has hits and no physical I/O at all -
           so a HAVING over operations alone answered "nothing to follow" for a server that is perfectly
           observable, and refused to draw the most reassuring chart there is. Measured: a six-hour window
           on the rig's quieter target had 0 operations and thousands of hits. */
        HAVING coalesce(SUM(d_reads), 0) + coalesce(SUM(d_writes), 0)
             + coalesce(SUM(d_extends), 0) + coalesce(SUM(d_hits), 0) > 0
        ORDER BY
            coalesce(SUM(d_reads), 0) + coalesce(SUM(d_writes), 0) + coalesce(SUM(d_extends), 0) DESC,
            /* Hits break the tie BEFORE the name does. Without this line a window where nothing touched a
               disk falls straight through to alphabetical order, which is the same defect ranking on
               read_time_ms would have caused - a name picked by sorting and presented as the busiest thing
               on the server. */
            coalesce(SUM(d_hits), 0) DESC,
            backend_type,
            context
        LIMIT 1
        """;

    /// <summary>
    /// The database worth following: the biggest temp-file spiller, or the busiest by block access when
    /// nothing spilled at all. Same ordering as the single-window read, for the same reason — spilling is
    /// the question this data answers that nothing else here can.
    ///
    /// <para>PostgreSQL's NULL-<c>datname</c> shared-relations row is excluded from the CHOICE only. It is
    /// the cluster-wide catalog, it never spills and it is never the answer to "which database is in
    /// trouble", but its block counters are real and it stays followable by name.</para>
    /// </summary>
    public const string TopDatabaseSql = """
        WITH sampled AS (
            SELECT
                database_name,
                GREATEST(temp_bytes - LAG(temp_bytes) OVER series, 0) AS d_temp_bytes,
                GREATEST(blks_read  - LAG(blks_read)  OVER series, 0) AS d_blks_read,
                GREATEST(blks_hit   - LAG(blks_hit)   OVER series, 0) AS d_blks_hit
            FROM pg_database_stats
            WHERE server_id = $1
            AND   database_name IS NOT NULL
            AND   collection_time >= $2
            AND   collection_time <= $3
            WINDOW series AS (
                PARTITION BY database_name
                ORDER BY collection_time
            )
        )
        SELECT database_name
        FROM sampled
        GROUP BY database_name
        HAVING coalesce(SUM(d_temp_bytes), 0) + coalesce(SUM(d_blks_read), 0) + coalesce(SUM(d_blks_hit), 0) > 0
        ORDER BY
            coalesce(SUM(d_temp_bytes), 0) DESC,
            coalesce(SUM(d_blks_read), 0) DESC,
            coalesce(SUM(d_blks_hit), 0) DESC,
            database_name
        LIMIT 1
        """;

    /// <summary>
    /// Whether the target measures I/O TIME at all, from the configuration the server itself reported.
    ///
    /// <para>This exists because <c>track_io_timing</c> is <b>off by default</b>, so a zero
    /// <c>read_time_ms</c> is the ordinary case rather than a fast disk — and <c>read_time_ms / reads</c>
    /// over it is 0.000 ms, which reads as sub-microsecond I/O. That is a confident wrong answer about the
    /// one number an operator would act on. Both rig targets had it off, which is how it came up.</para>
    ///
    /// <para>Bounded by the window's end rather than taking the newest value outright, so an <c>as_of</c>
    /// read of last week is told what the setting was THEN. Null when the configuration has not been
    /// collected, which the caller reports as unknown rather than as off.</para>
    ///
    /// <para>$1 server_id, $2 window end (naive UTC).</para>
    /// </summary>
    public const string IoTimingSettingSql = """
        SELECT setting
        FROM pg_server_config
        WHERE server_id = $1
        AND   name = 'track_io_timing'
        AND   collection_time <= $2
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// A text parameter that may legitimately be NULL — PostgreSQL's shared-relations database name, and
    /// the "leave this half of the subject free" filters below.
    ///
    /// <para>Typed explicitly rather than through <c>AddWithValue</c>: <c>DBNull</c> carries no type for
    /// Npgsql to infer, so an untyped null fails at bind time rather than anywhere that says what went
    /// wrong.</para>
    /// </summary>
    private static NpgsqlParameter TextOrNull(string? value) => new()
    {
        NpgsqlDbType = NpgsqlDbType.Text,
        Value = (object?)value ?? DBNull.Value,
    };

    /// <param name="backendTypeFilter">Constrains the choice to one backend type; null leaves it free.</param>
    /// <param name="contextFilter">Constrains the choice to one context; null leaves it free.</param>
    public static async Task<(string BackendType, string Context)?> GetDominantIoSubjectAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        string? backendTypeFilter = null, string? contextFilter = null,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(DominantIoSubjectSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.Add(TextOrNull(backendTypeFilter));
        command.Parameters.Add(TextOrNull(contextFilter));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0) || reader.IsDBNull(1))
        {
            return null;
        }

        return (reader.GetString(0), reader.GetString(1));
    }

    /// <summary>
    /// The busiest database, and whether the read found one at all. A plain <c>string?</c> could not carry
    /// that: NULL is a legitimate database name here (shared relations), so "nothing to follow" and "follow
    /// the shared-relations row" would be the same return value.
    /// </summary>
    public static async Task<string?> GetTopDatabaseAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(TopDatabaseSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value as string;
    }

    /// <summary>
    /// True / false from the target's own <c>track_io_timing</c>, or null when the configuration has not
    /// been collected for this server. Null is NOT folded into false: "we do not know" and "the server does
    /// not measure it" license different readings of a zero latency.
    /// </summary>
    public static async Task<bool?> GetIoTimingTrackedAsync(
        NpgsqlDataSource postgres, int serverId, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(IoTimingSettingSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        var value = await command.ExecuteScalarAsync(cancellationToken);

        return value is string setting
            ? string.Equals(setting, "on", StringComparison.OrdinalIgnoreCase)
            : null;
    }

    public static async Task<List<PgIoTrendPoint>> GetIoTrendAsync(
        NpgsqlDataSource postgres, int serverId, string backendType, string context,
        DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var points = new List<PgIoTrendPoint>();
        await using var command = postgres.CreateCommand(IoTrendSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(backendType);
        command.Parameters.AddWithValue(context);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then converts these naive columns at the store session's
           TimeZone, which silently empties the window east of UTC. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new PgIoTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0m : reader.GetDecimal(9),
                reader.IsDBNull(10) ? 0m : reader.GetDecimal(10),
                reader.IsDBNull(11) ? 0 : reader.GetDouble(11),
                reader.IsDBNull(12) ? 0 : reader.GetDouble(12),
                reader.IsDBNull(13) ? 0 : reader.GetDouble(13),
                reader.IsDBNull(14) ? 0 : reader.GetDouble(14),
                reader.IsDBNull(15) ? 0 : reader.GetDouble(15),
                reader.IsDBNull(16) ? 0 : reader.GetDouble(16),
                /* Null-preserving, unlike the counts above: a null here means the quotient was not defined,
                   which is a different statement from a latency of zero. */
                reader.IsDBNull(17) ? (double?)null : reader.GetDouble(17),
                reader.IsDBNull(18) ? (double?)null : reader.GetDouble(18),
                reader.IsDBNull(19) ? (double?)null : reader.GetDouble(19),
                !reader.IsDBNull(20) && reader.GetBoolean(20),
                !reader.IsDBNull(21) && reader.GetBoolean(21),
                !reader.IsDBNull(22) && reader.GetBoolean(22),
                !reader.IsDBNull(23) && reader.GetBoolean(23)));
        }

        return points;
    }

    public static async Task<List<PgDatabaseTrendPoint>> GetDatabaseTrendAsync(
        NpgsqlDataSource postgres, int serverId, string? databaseName,
        DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var points = new List<PgDatabaseTrendPoint>();
        await using var command = postgres.CreateCommand(DatabaseTrendSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.Add(TextOrNull(databaseName));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            points.Add(new PgDatabaseTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? (double?)null : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? (double?)null : reader.GetDouble(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetDouble(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                !reader.IsDBNull(12) && reader.GetBoolean(12)));
        }

        return points;
    }
}
