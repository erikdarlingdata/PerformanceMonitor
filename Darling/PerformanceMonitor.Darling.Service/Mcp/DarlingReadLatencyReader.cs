/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read for <c>get_read_latency</c> (#4442 scope 2) — "what's really slow", answered from the hourly
/// histograms <see cref="ReadLatencyAccumulator"/> flushes into <c>collect.read_latency</c>, the same
/// <c>collector_cost</c> shape <see cref="DarlingCollectorCostReader"/> reads, applied to <c>/api/read/*</c>
/// and composed-panel reads instead of collector runs.
///
/// <para>Two separate aggregations, deliberately not one pass over an exploded row set: <c>totals</c> sums
/// <c>run_count</c>/<c>total_ms</c>/<c>max_ms</c>/the timeout count straight off <c>collect.read_latency</c>,
/// and <c>buckets</c> separately explodes <c>bucket_counts</c> with <c>unnest(...) WITH ORDINALITY</c> to sum
/// it ELEMENT-WISE (grouped by ordinal position, then re-collected with <c>array_agg(... ORDER BY ord)</c>).
/// Summing <c>run_count</c> off the exploded rows would multiply every row by its own bucket-array length —
/// the reason this is two joined CTEs and not one <c>GROUP BY</c> over <c>unnest</c>'s output.
/// <see cref="ReadLatencyPercentiles.FromBuckets"/> then answers p50/p95/p99 from the SUMMED histogram — never
/// per-hour, because a percentile computed once per hour and averaged is not the window's percentile.
/// <c>bucket_counts</c> is a fixed-length array for every row (<see cref="ReadLatencyAccumulator.BucketUpperBoundsMs"/>'s
/// contract), so the element-wise sum is always summing arrays of the same length regardless of which release
/// wrote which row.</para>
///
/// <para>Timeouts are counted separately (<c>run_count</c> summed WHERE <c>outcome = 'timeout'</c>, the exact
/// lowercase spelling <see cref="ReadLatencyAccumulator.FlushAsync"/> writes) rather than folded into the
/// outcome-blind row: a route's overall latency and its timeout rate are two different questions. The bucket
/// histogram itself is summed across every outcome of a read that ran (ok, timeout, error, limit alike) because a timed-out
/// or errored read still took real wall-clock time and belongs in the same "how long did this route take"
/// picture — <c>get_read_latency</c>'s note says so.</para>
///
/// <para>#5506 part 3: the rows that are not a read running are kept OUT of <c>run_count</c>, the histogram,
/// <c>total_ms</c>, <c>max_ms</c>, <c>max_in_flight</c> and <c>wait_total_ms</c> (<see cref="NonRunOutcomes"/>):
/// <c>queue_wait</c> (one per read that waited for a long-read slot, its elapsed being that wait), <c>busy</c> (a
/// refused read), <c>cache_hit</c> (an answer from memory) and <c>cancelled</c> (a caller that left). They are counted
/// on their own instead: <c>queued</c> and <c>queue_wait_ms</c> are the queue_wait rows' run count and summed
/// elapsed, <c>queue_wait_p95_ms</c> comes from their own summed histogram, <c>rejected</c> is the busy rows' run count
/// and <c>cache_hits</c> the cache_hit rows'. A cache miss is a read that ran, so it is never stored as its own row.
/// A (surface, route, band) that only has such rows still returns, with a run count of zero.</para>
/// </summary>
internal static class DarlingReadLatencyReader
{
    public sealed record ReadLatencyRow(
        string Surface,
        string Route,
        long RunCount,
        long TotalMs,
        long MaxMs,
        long[] BucketCounts,
        long Timeouts,
        long Fallbacks,
        long GateFailures,
        string WindowBand = ReadWindowBand.None,
        int MaxInFlight = 0,
        long WaitTotalMs = 0,
        int DistinctCallers = 0,
        long Queued = 0,
        long QueueWaitMs = 0,
        long QueueWaitMaxMs = 0,
        long[]? QueueWaitBucketCounts = null,
        long Rejected = 0,
        long CacheHits = 0,
        long Cancelled = 0,
        long? ReplyBytesTotal = null,
        long ReplyBytesCount = 0,
        ReadLatencySplit? Split = null)
    {
        /// <summary>Zero when nothing ran, matching how <c>get_collector_cost</c> reports its average.</summary>
        public long MeanMs => RunCount > 0 ? TotalMs / RunCount : 0;

        /// <summary>#9433: the average reply size in bytes over the reads whose size was measured (the summed total over the summed
        /// measured count), null when none was. A read that took no size, on a key that also holds measured reads, is left out.
        /// Truncates below one byte, as <see cref="MeanMs"/> does.</summary>
        public long? MeanReplyBytes => ReplyBytesTotal is { } total && ReplyBytesCount > 0 ? total / ReplyBytesCount : null;
    }

    /// <summary>One side of a split (#9434): the reads of the buckets on that side, counted the way the row's own count, max and
    /// histogram are (a read that ran, never the queue, busy, cache or cancelled rows).</summary>
    public sealed record ReadLatencySide(long Count, long MaxMs, long[] BucketCounts);

    /// <summary>A route's figures before and after the split instant (#9434). <paramref name="AmbiguousCount"/> is the reads of
    /// buckets that carry no process start (stored before V315) and whose hour holds the split instant: they are counted by
    /// their hour, as before, and may belong to either side.</summary>
    public sealed record ReadLatencySplit(ReadLatencySide Before, ReadLatencySide After, long AmbiguousCount);

    /// <summary>#5506 part 3: the stored outcomes that are not a read running, kept out of the run count, the
    /// percentiles and the wait total. <c>cancelled</c> covers both a caller that left while waiting in the queue and one
    /// that left mid-read: neither finished, so neither measures what the route costs.</summary>
    internal static readonly string[] NonRunOutcomes = ["queue_wait", "busy", "cache_hit", "cancelled"];

    /// <summary>How many process starts <see cref="GetProcessStartsAsync"/> returns at most.</summary>
    public const int MaxProcessStarts = 20;

    /// <summary>The distinct service process start instants that wrote a bucket in the window, newest first, at most
    /// <see cref="MaxProcessStarts"/> (#9434). A caller cannot learn the instant to pass as <c>split_at</c> from a log line, because a
    /// process start is taken a few seconds before or after any instant the caller can see, so the answer lists the exact stored values.
    /// Buckets stored before V315 have no start and are left out. The window is the whole store window, not the surface or route
    /// filter: a start is a fact about the service, and a build that never read one route is still a build to split on.</summary>
    public static async Task<List<DateTime>> GetProcessStartsAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        var starts = new List<DateTime>();
        await using var command = postgres.CreateCommand(
            $"SELECT DISTINCT process_started_at FROM collect.read_latency WHERE metric_time >= $1 AND process_started_at IS NOT NULL ORDER BY process_started_at DESC LIMIT {MaxProcessStarts}");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(sinceUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            starts.Add(DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc));
        }

        return starts;
    }

    public static Task<List<ReadLatencyRow>> GetSummaryAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, string? surface, string? route,
        CancellationToken cancellationToken = default)
        => GetSummaryAsync(postgres, sinceUtc, surface, route, null, cancellationToken);

    /// <summary>The same read with a split instant (#9434): each row also carries its before and after figures. Null splits nothing.</summary>
    public static async Task<List<ReadLatencyRow>> GetSummaryAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, string? surface, string? route, DateTime? splitAtUtc,
        CancellationToken cancellationToken = default)
    {
        var normalizedSurface = string.IsNullOrWhiteSpace(surface) ? null : surface.Trim().ToLowerInvariant();
        var normalizedRoute = string.IsNullOrWhiteSpace(route) ? null : route.Trim();

        var surfaceFilter = normalizedSurface is not null ? " AND surface = $2" : string.Empty;
        var routeFilter = normalizedRoute is not null
            ? (normalizedSurface is not null ? " AND route = $3" : " AND route = $2")
            : string.Empty;

        /* #9434: the split instant is the last parameter. A bucket is on the AFTER side when the process that wrote it started at or
           after the instant. A bucket written before V315 has no start: it falls back to its hour, after when the hour starts at or
           after the instant, so the hour that holds the instant counts as before, and is_ambiguous marks those rows so the answer
           can say how many reads that is. An hour that STARTS at the instant is wholly after it, so it is not ambiguous (strict less-than). Every figure below is NULL-safe on is_after: no split leaves it NULL and the side figures 0. */
        var splitParam = 1 + (normalizedSurface is not null ? 1 : 0) + (normalizedRoute is not null ? 1 : 0) + 1;
        var splitUtc = $"(${splitParam}::timestamptz AT TIME ZONE 'UTC')";
        var nonRunList = string.Join(", ", NonRunOutcomes.Select(o => $"'{o}'"));
        var sql = $@"
WITH windowed AS
(
    SELECT surface, route, window_band, outcome, run_count, total_ms, max_ms, bucket_counts, max_in_flight, wait_total_ms, distinct_callers, reply_bytes_total, reply_bytes_count,
           outcome IN ({nonRunList}) AS is_admission,
           CASE WHEN ${splitParam}::timestamptz IS NULL THEN NULL
                WHEN process_started_at IS NOT NULL THEN process_started_at >= ${splitParam}::timestamptz
                ELSE metric_time >= {splitUtc} END AS is_after,
           (process_started_at IS NULL AND ${splitParam}::timestamptz IS NOT NULL
            AND metric_time < {splitUtc} AND {splitUtc} < metric_time + interval '1 hour') AS is_ambiguous
    FROM collect.read_latency
    WHERE metric_time >= $1{surfaceFilter}{routeFilter}
),
totals AS
(
    SELECT
        surface,
        route,
        window_band,
        coalesce(sum(run_count) FILTER (WHERE NOT is_admission), 0) AS run_count,
        coalesce(sum(total_ms) FILTER (WHERE NOT is_admission), 0) AS total_ms,
        coalesce(max(max_ms) FILTER (WHERE NOT is_admission), 0) AS max_ms,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'timeout'), 0) AS timeouts,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'fallback_raw'), 0) AS fallbacks,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'gate_failed'), 0) AS gate_failures,
        coalesce(max(max_in_flight) FILTER (WHERE NOT is_admission), 0) AS max_in_flight,
        coalesce(sum(wait_total_ms) FILTER (WHERE NOT is_admission), 0) AS wait_total_ms,
        max(distinct_callers)                                 AS distinct_callers,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'queue_wait'), 0) AS queued,
        coalesce(sum(total_ms) FILTER (WHERE outcome = 'queue_wait'), 0) AS queue_wait_ms,
        coalesce(max(max_ms) FILTER (WHERE outcome = 'queue_wait'), 0) AS queue_wait_max_ms,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'busy'), 0) AS rejected,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'cache_hit'), 0) AS cache_hits,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'cancelled'), 0) AS cancelled,
        sum(reply_bytes_total) FILTER (WHERE NOT is_admission)::bigint AS reply_bytes_total,
        coalesce(sum(reply_bytes_count) FILTER (WHERE NOT is_admission), 0)::bigint AS reply_bytes_count,
        coalesce(sum(run_count) FILTER (WHERE NOT is_admission AND is_after IS FALSE), 0) AS before_count,
        coalesce(max(max_ms) FILTER (WHERE NOT is_admission AND is_after IS FALSE), 0) AS before_max_ms,
        coalesce(sum(run_count) FILTER (WHERE NOT is_admission AND is_after IS TRUE), 0) AS after_count,
        coalesce(max(max_ms) FILTER (WHERE NOT is_admission AND is_after IS TRUE), 0) AS after_max_ms,
        coalesce(sum(run_count) FILTER (WHERE NOT is_admission AND is_ambiguous), 0) AS ambiguous_count
    FROM windowed
    GROUP BY surface, route, window_band
),
buckets AS
(
    SELECT surface, route, window_band, b.ord,
           coalesce(sum(b.bucket_count) FILTER (WHERE NOT is_admission), 0)::bigint AS bucket_count,
           coalesce(sum(b.bucket_count) FILTER (WHERE outcome = 'queue_wait'), 0)::bigint AS queue_bucket_count,
           coalesce(sum(b.bucket_count) FILTER (WHERE NOT is_admission AND is_after IS FALSE), 0)::bigint AS before_bucket_count,
           coalesce(sum(b.bucket_count) FILTER (WHERE NOT is_admission AND is_after IS TRUE), 0)::bigint AS after_bucket_count
    FROM windowed
    CROSS JOIN LATERAL unnest(windowed.bucket_counts) WITH ORDINALITY AS b(bucket_count, ord)
    GROUP BY surface, route, window_band, b.ord
),
bucket_arrays AS
(
    SELECT surface, route, window_band,
           array_agg(bucket_count ORDER BY ord) AS bucket_counts,
           array_agg(queue_bucket_count ORDER BY ord) AS queue_bucket_counts,
           array_agg(before_bucket_count ORDER BY ord) AS before_bucket_counts,
           array_agg(after_bucket_count ORDER BY ord) AS after_bucket_counts
    FROM buckets
    GROUP BY surface, route, window_band
)
SELECT t.surface, t.route, t.run_count, t.total_ms, t.max_ms, ba.bucket_counts, t.timeouts, t.fallbacks, t.gate_failures,
       t.window_band, t.max_in_flight, t.wait_total_ms::bigint, t.distinct_callers,
       t.queued, t.queue_wait_ms, t.queue_wait_max_ms, ba.queue_bucket_counts, t.rejected, t.cache_hits, t.cancelled,
       t.reply_bytes_total, t.reply_bytes_count,
       t.before_count, t.before_max_ms, ba.before_bucket_counts, t.after_count, t.after_max_ms, ba.after_bucket_counts, t.ambiguous_count
FROM totals AS t
JOIN bucket_arrays AS ba ON ba.surface = t.surface AND ba.route = t.route AND ba.window_band = t.window_band
ORDER BY t.surface, t.route, t.window_band";

        var rows = new List<ReadLatencyRow>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(sinceUtc);
        if (normalizedSurface is not null)
        {
            command.Parameters.AddWithValue(normalizedSurface);
        }
        if (normalizedRoute is not null)
        {
            command.Parameters.AddWithValue(normalizedRoute);
        }
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz, Value = splitAtUtc.HasValue ? DateTime.SpecifyKind(splitAtUtc.Value, DateTimeKind.Utc) : DBNull.Value });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ReadLatencyRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                (long[])reader.GetValue(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.GetString(9),
                reader.GetInt32(10),
                reader.GetInt64(11),
                reader.GetInt32(12),
                reader.GetInt64(13),
                reader.GetInt64(14),
                reader.GetInt64(15),
                (long[])reader.GetValue(16),
                reader.GetInt64(17),
                reader.GetInt64(18),
                reader.GetInt64(19),
                reader.IsDBNull(20) ? null : reader.GetInt64(20),
                reader.GetInt64(21),
                splitAtUtc.HasValue
                    ? new ReadLatencySplit(
                        new ReadLatencySide(reader.GetInt64(22), reader.GetInt64(23), (long[])reader.GetValue(24)),
                        new ReadLatencySide(reader.GetInt64(25), reader.GetInt64(26), (long[])reader.GetValue(27)),
                        reader.GetInt64(28))
                    : null));
        }

        return rows;
    }
}
