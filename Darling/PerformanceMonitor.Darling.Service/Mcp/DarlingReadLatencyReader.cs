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
/// histogram itself is summed across EVERY outcome (ok, timeout, cancelled, error alike) because a timed-out
/// or errored read still took real wall-clock time and belongs in the same "how long did this route take"
/// picture — <c>get_read_latency</c>'s note says so.</para>
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
        long Timeouts)
    {
        /// <summary>Zero when nothing ran, matching how <c>get_collector_cost</c> reports its average.</summary>
        public long MeanMs => RunCount > 0 ? TotalMs / RunCount : 0;
    }

    public static async Task<List<ReadLatencyRow>> GetSummaryAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, string? surface, string? route,
        CancellationToken cancellationToken = default)
    {
        var normalizedSurface = string.IsNullOrWhiteSpace(surface) ? null : surface.Trim().ToLowerInvariant();
        var normalizedRoute = string.IsNullOrWhiteSpace(route) ? null : route.Trim();

        var surfaceFilter = normalizedSurface is not null ? " AND surface = $2" : string.Empty;
        var routeFilter = normalizedRoute is not null
            ? (normalizedSurface is not null ? " AND route = $3" : " AND route = $2")
            : string.Empty;

        var sql = $@"
WITH windowed AS
(
    SELECT surface, route, outcome, run_count, total_ms, max_ms, bucket_counts
    FROM collect.read_latency
    WHERE metric_time >= $1{surfaceFilter}{routeFilter}
),
totals AS
(
    SELECT
        surface,
        route,
        sum(run_count)                                        AS run_count,
        sum(total_ms)                                         AS total_ms,
        max(max_ms)                                           AS max_ms,
        coalesce(sum(run_count) FILTER (WHERE outcome = 'timeout'), 0) AS timeouts
    FROM windowed
    GROUP BY surface, route
),
buckets AS
(
    SELECT surface, route, b.ord, sum(b.bucket_count)::bigint AS bucket_count
    FROM windowed
    CROSS JOIN LATERAL unnest(windowed.bucket_counts) WITH ORDINALITY AS b(bucket_count, ord)
    GROUP BY surface, route, b.ord
),
bucket_arrays AS
(
    SELECT surface, route, array_agg(bucket_count ORDER BY ord) AS bucket_counts
    FROM buckets
    GROUP BY surface, route
)
SELECT t.surface, t.route, t.run_count, t.total_ms, t.max_ms, ba.bucket_counts, t.timeouts
FROM totals AS t
JOIN bucket_arrays AS ba ON ba.surface = t.surface AND ba.route = t.route
ORDER BY t.surface, t.route";

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
                reader.GetInt64(6)));
        }

        return rows;
    }
}
