/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The count guard of a long-window read that takes its middle hours from the hourly interval rollup of
/// <c>collect.query_stats</c> and its edges from raw (#4605). The read may use the rollup for a span only when the
/// rollup holds every raw row the span has, hour by hour, so this guard compares the two counts per (server, hour)
/// and returns the number of pairs that disagree. A result of 0 means the guard passed. Any other result, or a fault,
/// means the read stays on raw.
/// <para>The raw side excludes restart rows (<c>sample_interval_seconds = 0</c>) on purpose: the rollup's own
/// <c>count(*)</c> sits under the same <c>WHERE sample_interval_seconds IS DISTINCT FROM 0</c>
/// (<see cref="TimescaleSupport.CreateQueryStatsIntervalHourlySql"/>), so the two sides count the same population,
/// and a row with a NULL interval counts on both. Restart rows need no guard: the route reads them live from raw
/// (<see cref="IntervalRollupRestartRows"/>). An hour with no non-restart rows has no row on either side and counts as
/// a match.</para>
/// <para>A count guard is exact only while raw <c>query_stats</c> rows are append-only. Late rows are fine: equal
/// counts on a growing table are closed by reading the guard and the panel in one snapshot. But an in-place UPDATE,
/// or a row moved to another hour, keeps the counts equal and passes unseen. The route relies on the collector's
/// COPY-only writes and whole-row retention, and a live pin in the runner change records that dependency (#4605).</para>
/// <para>Every instant is bound: <c>$1</c> is the start, inclusive, and <c>$2</c> the end, exclusive, both naive UTC
/// <c>timestamp</c>; <c>$3</c> is a <c>text[]</c> of server names, or NULL for every server. Both sides filter on
/// <c>server_name</c> per row.</para>
/// </summary>
public static class IntervalRollupCountGuard
{
    /// <summary>The number of (server, hour) pairs in <c>[$1, $2)</c> whose raw non-restart row count differs from the hourly rollup's <c>sum(sample_count)</c>; 0 means the guard passed.</summary>
    public const string QueryStatsSql = @"
WITH r AS (SELECT server_id, time_bucket('1 hour', collection_time) AS bucket, count(*) AS n FROM collect.query_stats
           WHERE collection_time >= $1 AND collection_time < $2 AND sample_interval_seconds IS DISTINCT FROM 0
             AND ($3::text[] IS NULL OR server_name = ANY($3)) GROUP BY 1, 2),
     c AS (SELECT server_id, bucket, sum(sample_count) AS n FROM collect.query_stats_interval_hourly
           WHERE bucket >= $1 AND bucket < $2 AND ($3::text[] IS NULL OR server_name = ANY($3)) GROUP BY 1, 2)
SELECT count(*) FROM r FULL JOIN c USING (server_id, bucket) WHERE r.n IS DISTINCT FROM c.n;";
}
