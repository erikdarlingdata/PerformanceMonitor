/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Which side of the process answered a read (#4442 scope 2). <c>Web</c> is the <c>/api/read/*</c>
/// dispatch loop, <c>Compose</c> is the one runner behind both the web composer and the MCP custom-view tool
/// (recorded ONCE there, so an MCP custom-view call is not double-counted as both <c>Compose</c> and
/// <c>Mcp</c>), and <c>Mcp</c> is reserved for the later per-tool wrapper — nothing records it yet.</summary>
public enum ReadSurface
{
    Web,
    Compose,
    Mcp,
}

/// <summary>How a read finished (#4442 scope 2), read by <see cref="ReadOutcomeClassifier"/>. <c>Ok</c> is
/// success; <c>Timeout</c> is a caught 57014 whose message names the store's own statement_timeout;
/// <c>Cancelled</c> is the caller's own token going away, or a 57014 this process cannot attribute to the
/// store's timeout (a user cancel); <c>Error</c> is everything else.</summary>
public enum ReadOutcome
{
    Ok,
    Timeout,
    Cancelled,
    Error,
}

/// <summary>
/// The tool's own read-latency histogram (#4442 scope 2) — the <see cref="CollectorCostAccumulator"/> shape
/// applied to <c>/api/read/*</c> and composed-panel reads instead of collector runs, so "which read is really
/// bad, and how often does it time out" answers from a stored series rather than the 5-second slow-query log
/// tail (which can only ever show the newest offender, never a percentile).
///
/// <para>Keyed by <c>(Surface, Route, Outcome)</c> rather than by request: an hourly flush writes one row per
/// distinct combination actually hit, not one row per read, which is what keeps the write cheap (at most
/// surfaces × routes × outcomes rows, almost always far fewer in practice) while still separating "the plan
/// tool times out a lot" from "the wait-stats tool never does."</para>
///
/// <para><b>Buckets are FIXED and shared, on purpose.</b> <see cref="BucketUpperBoundsMs"/> is one
/// <c>static readonly</c> array for the whole process's lifetime — every route's histogram uses the SAME
/// bounds, so a percentile computed from one release's stored rows means the same thing as one from another
/// release's, and two routes' histograms can be summed bucket-for-bucket (the read tool this accumulator feeds
/// does exactly that, across a time window). Changing the bounds would silently break that comparability for
/// every row already stored, so the array is a contract, not a tuning knob to revisit per deployment.</para>
///
/// <para>Thread-safe the same way <see cref="CollectorCostAccumulator"/> is: a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// keyed bucket, with a per-bucket lock around the handful of fields a single <see cref="Record"/> call touches
/// — contended only by concurrent reads of the exact same (surface, route, outcome), which for a busy route is
/// real but bounded contention, not a global lock across every route.</para>
/// </summary>
public sealed class ReadLatencyAccumulator
{
    /// <summary>Log-scale bucket upper bounds in milliseconds, 10 ms to 120 s, plus an implicit final overflow
    /// bucket (anything above the last bound). ~24 finite bounds: fine enough near the floor (where "fast" and
    /// "fast enough" are a meaningful distinction) and coarse enough near the ceiling (where the only question
    /// left is "how far over the statement_timeout did it get").</summary>
    public static readonly int[] BucketUpperBoundsMs =
    {
        10, 20, 30, 50, 75, 100, 150, 200, 300, 500, 750,
        1_000, 1_500, 2_000, 3_000, 5_000, 7_500,
        10_000, 15_000, 20_000, 30_000, 45_000, 60_000, 90_000, 120_000,
    };

    /// <summary>One past the last finite bucket — the overflow bucket for anything above
    /// <see cref="BucketUpperBoundsMs"/>'s last entry.</summary>
    private static readonly int s_bucketCount = BucketUpperBoundsMs.Length + 1;

    private sealed class Bucket
    {
        public long Count;
        public long TotalMs;
        public long MaxMs;
        public readonly long[] BucketCounts = new long[s_bucketCount];
    }

    private readonly ConcurrentDictionary<(ReadSurface Surface, string Route, ReadOutcome Outcome), Bucket> _buckets = new();

    /// <summary>Record one finished read. Thread-safe: web and compose reads run concurrently. <paramref
    /// name="route"/> is never the caller's free text — every call site passes a bounded-cardinality label
    /// (the <c>/api/read/&lt;name&gt;</c> tool name, or a fixed panel/view-kind label for compose).</summary>
    public void Record(ReadSurface surface, string route, ReadOutcome outcome, long elapsedMs)
    {
        if (string.IsNullOrEmpty(route))
        {
            return;
        }

        var bucket = _buckets.GetOrAdd((surface, route, outcome), static _ => new Bucket());
        var clampedMs = Math.Max(elapsedMs, 0);
        var bucketIndex = BucketIndexFor(clampedMs);

        lock (bucket)
        {
            bucket.Count++;
            bucket.TotalMs += clampedMs;
            if (clampedMs > bucket.MaxMs)
            {
                bucket.MaxMs = clampedMs;
            }
            bucket.BucketCounts[bucketIndex]++;
        }
    }

    /// <summary>The bucket index for an elapsed duration: the first bound the duration is LESS THAN OR EQUAL
    /// TO (so a duration exactly on a bound lands in that bound's bucket, not the next one up), or the
    /// overflow index when it exceeds every finite bound.</summary>
    internal static int BucketIndexFor(long elapsedMs)
    {
        for (var i = 0; i < BucketUpperBoundsMs.Length; i++)
        {
            if (elapsedMs <= BucketUpperBoundsMs[i])
            {
                return i;
            }
        }

        return BucketUpperBoundsMs.Length;
    }

    internal sealed record DrainedLatency(
        ReadSurface Surface, string Route, ReadOutcome Outcome, long Count, long TotalMs, long MaxMs, long[] BucketCounts);

    /// <summary>Drain every accumulated bucket and RESET the window (<c>TryRemove</c>), so the next hour
    /// starts clean and an hour with no reads writes nothing — exactly <see cref="CollectorCostAccumulator.Drain"/>'s
    /// shape. Extracted so the aggregation is unit-testable without a store or a flush.</summary>
    internal IReadOnlyList<DrainedLatency> Drain()
    {
        var keys = _buckets.Keys.ToArray();
        var rows = new List<DrainedLatency>(keys.Length);

        foreach (var key in keys)
        {
            if (!_buckets.TryRemove(key, out var bucket))
            {
                continue;
            }

            lock (bucket)
            {
                rows.Add(new DrainedLatency(
                    key.Surface, key.Route, key.Outcome, bucket.Count, bucket.TotalMs, bucket.MaxMs,
                    (long[])bucket.BucketCounts.Clone()));
            }
        }

        return rows;
    }

    /// <summary>Rows older than this are pruned by the flush's own bounded DELETE — the same 90 days
    /// <see cref="CollectorCostAccumulator.RetentionDays"/> uses.</summary>
    public const int RetentionDays = 90;

    public const int FlushTimeoutSeconds = 120;

    /* One flush: drain the buckets and write an hourly aggregate row per (surface, route, outcome), then
       prune. ON CONFLICT adds counts, totals and per-bucket counts element-wise and takes the greater of
       the two max_ms values — the shape a SECOND flush landing in the same hour needs (this accumulator
       flushes on the same worker tick as collector_cost, so ordinarily there is one flush per hour, but a
       restart mid-hour can produce two partial ones for the same key and neither may be dropped). Draining
       (TryRemove) resets the window, so an hour with no reads writes no row rather than a zero.

       One statement PER DRAINED KEY, not collector_cost's single unnest bulk insert: Npgsql's array binding
       refuses a jagged bigint[][] parameter ("use a multidimensional array instead"), and every drained
       key's own bucket_counts is a single flat bigint[], which binds cleanly as its own parameter. The row
       count here is bounded by (surface × route × outcome) actually hit in the hour — at most a few hundred,
       never one per read — so a handful of round trips is the right shape for this table's real size. */
    private const string InsertSql = @"
INSERT INTO collect.read_latency
(
    metric_time, surface, route, outcome,
    run_count, total_ms, max_ms, bucket_counts
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (metric_time, surface, route, outcome) DO UPDATE SET
    run_count = collect.read_latency.run_count + EXCLUDED.run_count,
    total_ms = collect.read_latency.total_ms + EXCLUDED.total_ms,
    max_ms = GREATEST(collect.read_latency.max_ms, EXCLUDED.max_ms),
    bucket_counts = (
        SELECT array_agg(a + b ORDER BY ord)
        FROM unnest(collect.read_latency.bucket_counts) WITH ORDINALITY AS x(a, ord)
        JOIN unnest(EXCLUDED.bucket_counts) WITH ORDINALITY AS y(b, ord2) ON ord = ord2
    )";

    private const string RetentionDeleteSql = @"
DELETE FROM collect.read_latency
WHERE metric_time < $1";

    public async Task FlushAsync(NpgsqlConnection connection, DateTime metricTimeUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        var drained = Drain();

        foreach (var row in drained)
        {
            await using var insert = new NpgsqlCommand(InsertSql, connection) { CommandTimeout = FlushTimeoutSeconds };
            insert.Parameters.AddWithValue(metricTimeUtc);
            insert.Parameters.AddWithValue(row.Surface.ToString().ToLowerInvariant());
            insert.Parameters.AddWithValue(row.Route);
            insert.Parameters.AddWithValue(row.Outcome.ToString().ToLowerInvariant());
            insert.Parameters.AddWithValue((int)Math.Min(row.Count, int.MaxValue));
            insert.Parameters.AddWithValue(row.TotalMs);
            insert.Parameters.AddWithValue(row.MaxMs);
            insert.Parameters.AddWithValue(row.BucketCounts);
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var retention = new NpgsqlCommand(RetentionDeleteSql, connection) { CommandTimeout = FlushTimeoutSeconds };
        retention.Parameters.AddWithValue(metricTimeUtc.AddDays(-RetentionDays));
        await retention.ExecuteNonQueryAsync(cancellationToken);

        logger?.LogDebug("read_latency flush wrote {Rows} surface+route+outcome rows", drained.Count);
    }
}
