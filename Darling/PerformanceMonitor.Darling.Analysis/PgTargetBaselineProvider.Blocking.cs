/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// <c>pg_blocked_sessions</c>: the number of blocked sessions per capture, from <c>pg_blocking_edges</c>
    /// (<c>COUNT(DISTINCT blocked_pid)</c> per capture) — a point series (no differencing), the <c>PgSessionCount</c>
    /// arm's shape. A capture with no edge rows is a ZERO sample, not a missing one, and the arm says so: the
    /// captures are the <c>pg_blocking</c> collector's SUCCESS runs in <c>collection_log</c>, the denominator
    /// <c>get_pg_blocking</c> reports and the only place a "looked and found nothing" is written down (V71's own
    /// warning: the table is EMPTY on a healthy instance, so over the table alone a quiet month has no baseline at all
    /// and a server that blocks once a week has a baseline of blocking).
    ///
    /// <para>/* filled by lane 17 of #3691 — the marker stays, as v1's did. The arm is a CTE chain ending in
    /// <c>clean(collection_time, v)</c> followed by the ONE <see cref="PgBaselineProvider.RobustTierScaffold"/>, window
    /// bounds <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c> on the value (the io-arm rule), <c>server_id = $1</c>,
    /// no bare <c>now()</c>; the baseline census in <c>PgTargetAnomalyTests</c> gained the metric's (name, table) row in
    /// the same PR, and <c>pg_blocking</c> joined <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> the day
    /// this arm first read <c>pg_blocking_edges</c> (BaselineSupplyTests derives that set from the arms' text and fails
    /// without the floor). */</para>
    ///
    /// <para><b>Joining the log to the edges: by MINUTE, not by instant.</b> The runner stamps a capture's rows with its
    /// start time and writes the <c>collection_log</c> row when the run ends (<c>DarlingObservability</c>), so the two
    /// timestamps differ by the run's duration and never match exactly. Both are binned to the minute
    /// (<c>date_bin('1 minute', …, TIMESTAMP '2000-01-01')</c>, the I/O arm's fixed-origin idiom) and joined FULL
    /// OUTER: a minute with an edge capture carries its count whether or not its log row landed in the same minute
    /// (a run that starts at :59.8 and logs at :00.1 is the one straddle, and it costs one extra zero in the next
    /// minute rather than a lost sample); a logged minute with no edges is a zero. The bin is the row's
    /// <c>collection_time</c> for the scaffold — naive UTC, as every arm's — so the local-clock keying (#3653 item 12)
    /// applies to it unchanged: every real-world offset is a whole number of minutes.</para>
    ///
    /// <para><b>Cost, stated.</b> Thirty days of <c>collection_log</c> for one server is every collector's runs — on
    /// the order of a million rows on a full schedule — read once through <c>idx_collection_log_time</c> and filtered
    /// to <c>pg_blocking</c>; a second or two on the largest store, once an hour per server (the provider's
    /// <c>CacheTtl</c>), on the analysis path and never the alerting one. The alternative — buckets over the edge rows
    /// alone — is cheaper and wrong (see the first paragraph).</para>
    /// </summary>
    private static partial string? BlockedSessionsBaselineQuery() => @"
WITH looked AS (
    SELECT DISTINCT date_bin('1 minute', collection_time, TIMESTAMP '2000-01-01') AS minute
    FROM collection_log
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   collector_name = 'pg_blocking'
    AND   status = 'SUCCESS'
),
blocked AS (
    SELECT date_bin('1 minute', collection_time, TIMESTAMP '2000-01-01') AS minute,
           COUNT(DISTINCT blocked_pid)::DOUBLE PRECISION AS blocked_sessions
    FROM pg_blocking_edges
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   blocked_pid IS NOT NULL
    GROUP BY date_bin('1 minute', collection_time, TIMESTAMP '2000-01-01')
),
clean AS (
    SELECT coalesce(b.minute, l.minute)     AS collection_time,
           coalesce(b.blocked_sessions, 0)  AS v
    FROM looked AS l
    FULL OUTER JOIN blocked AS b ON b.minute = l.minute
)," + RobustTierScaffold;
}
