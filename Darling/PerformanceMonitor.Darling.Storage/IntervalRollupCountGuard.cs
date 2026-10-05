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
/// rollup holds every raw row the span has, hour by hour. The guard proves that without scanning raw: it compares, per
/// (server, hour), the hour ledger (<see cref="QueryStatsHourLedger"/>, a row count the collector adds in the same
/// transaction as its COPY) with the rollup's <c>sum(sample_count)</c>, and returns the number of pairs that disagree.
/// The guard it replaces counted every raw row of the window; on a large store that ran past its 15 s cap and the route
/// was never taken. The ledger makes its cost depend on the number of (server, hour) pairs, not on the number of raw rows.
/// <para>A result of 0 means the guard passed. <see cref="UncoveredResult"/> (-1) means the ledger does not cover the
/// window: the state row's <c>counted_since</c> is after the window start, or the state row is missing, so the hours
/// before it were never counted and no comparison says anything. Any other result, or a fault, means the read stays on
/// raw. A store before V164 has no ledger table, so the guard faults and the read stays on raw too.</para>
/// <para><b>Why a pass is exact.</b> The ledger counts the rollup's population: a restart row
/// (<c>sample_interval_seconds = 0</c>) is in neither, and a row with a NULL interval is in both, because the rollup's
/// own <c>count(*)</c> sits under <c>WHERE sample_interval_seconds IS DISTINCT FROM 0</c>
/// (<see cref="TimescaleSupport.CreateQueryStatsIntervalHourlySql"/>). Restart rows need no guard: the route reads them
/// live from raw (<see cref="IntervalRollupRestartRows"/>). The collector writes the ledger in its COPY's own transaction, so
/// the guard and the panel read one REPEATABLE READ snapshot in which the ledger, raw and the rollup are all the state
/// after the same set of commits. Every way the counts can drift then makes them differ, and a difference fails safe: a
/// rollup that has not refreshed over a late row is below the ledger, a (server, hour) the ledger lacks is above the
/// rollup (the FULL JOIN keeps a pair that only one side has), and a row-by-row DELETE of raw leaves the ledger above the
/// rollup once it refreshes. The one unsafe case is a writer of <c>collect.query_stats</c> that the ledger does not
/// count, and the writer census pin (<c>QueryStatsWriterCensusPins</c>) guards it. An in-place UPDATE of a raw row
/// changes no count and passes unseen, so the route also relies on the collector's append-only COPY writes and its
/// whole-row retention; a live pin in the runner change records that dependency (#4605).</para>
/// <para>Every instant is bound: <c>$1</c> is the start, inclusive, and <c>$2</c> the end, exclusive, both naive UTC
/// <c>timestamp</c>; <c>$3</c> is a <c>text[]</c> of server names, or NULL for every server. Both sides filter on
/// <c>server_name</c> per row. The coverage test compares the store-wide <c>counted_since</c> with <c>$1</c>, so a window that
/// starts exactly at <c>counted_since</c> is covered; a missing state row yields NULL, which is not <c>&lt;= $1</c>, so it reads as
/// uncovered. The comparison sits in the branch taken only for a covered window, so an uncovered window never runs it.</para>
/// </summary>
public static class IntervalRollupCountGuard
{
    /// <summary>The result that says the ledger does not cover the window (<c>counted_since</c> is after its start, or the state row is missing): the guard cannot pass it and the read stays on raw. It is not a mismatch count, so the caller notes it apart from a failed comparison.</summary>
    public const long UncoveredResult = -1;

    /// <summary>The number of (server, hour) pairs in <c>[$1, $2)</c> whose ledger count differs from the hourly rollup's <c>sum(sample_count)</c>, or <see cref="UncoveredResult"/> when the ledger does not cover the window; 0 means the guard passed.</summary>
    public const string QueryStatsSql = @"
WITH l AS (SELECT server_id, server_name, bucket, n FROM collect.query_stats_hour_ledger
           WHERE bucket >= $1 AND bucket < $2 AND ($3::text[] IS NULL OR server_name = ANY($3))),
     c AS (SELECT server_id, server_name, bucket, sum(sample_count) AS n FROM collect.query_stats_interval_hourly
           WHERE bucket >= $1 AND bucket < $2 AND ($3::text[] IS NULL OR server_name = ANY($3)) GROUP BY 1, 2, 3),
     m AS (SELECT count(*) AS n FROM l FULL JOIN c USING (server_id, server_name, bucket) WHERE l.n IS DISTINCT FROM c.n)
SELECT CASE WHEN (SELECT counted_since FROM collect.query_stats_hour_ledger_state WHERE id = 1) <= $1
            THEN (SELECT n FROM m) ELSE -1 END;";
}
