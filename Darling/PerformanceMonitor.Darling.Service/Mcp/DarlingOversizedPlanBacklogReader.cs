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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over <c>collect.oversized_plan_backlog</c> (#3398) — the V121 worklist of cached plans
/// <see cref="QueryPlanXmlCaptureLimits"/> declined to capture, and what the out-of-band sweep has since
/// done about each one.
///
/// <para><b>A worklist, not a series, so nothing here takes a window.</b> Every other self-monitoring read
/// on this surface bounds itself by time because its table is append-only telemetry. This table holds one
/// row per plan identity for as long as that plan keeps recurring, and the sweep MUTATES those rows in
/// place, so "the last 24 hours of it" is not a thing that exists. The verdict timestamps carry the time
/// axis instead: <c>last_captured_at</c> and <c>last_expired_at</c> answer "has the fetch half moved since
/// T" by comparison, which is the one question the recording half shipped without any client able to
/// ask.</para>
///
/// <para><b>The four verdicts are a strict partition of the total, and the read is written so they cannot
/// be anything else.</b> <c>captured</c> is <c>captured_at IS NOT NULL</c>; <c>expired</c> excludes a row
/// that also carries content, because a capture retires a standing expiry
/// (<see cref="OversizedPlanBacklog.RecordCaptureSql"/>) and so does a re-sighting
/// (<see cref="OversizedPlanBacklog.RecordSightingsSql"/>); <c>pending</c> is the sweep's own claim
/// predicate. A row with no content and a standing <c>expired_at</c> is <c>expired</c> when a fetch stamped
/// it, which also stamps <c>last_attempt_at</c>, and <c>dropped</c> when the sweep's trim did, which only
/// ever stamps a row never attempted (#8567). Every row therefore lands in exactly one bucket and
/// <c>pending + captured + expired + dropped</c> equals <c>total</c> — asserted against a live store rather
/// than argued, because a rollup whose buckets silently overlap reads as a bigger backlog than exists, and
/// one whose buckets leave a gap reads as a smaller one.</para>
///
/// <para><b><c>pending</c> is the claim's predicate, spelled the same way on purpose.</b> A read that
/// defined "still to do" independently of <see cref="OversizedPlanBacklog.ClaimSql"/> could report an empty
/// backlog while the sweep still had work, or the reverse — which is the whole class of divergence this
/// read exists to remove, reintroduced one layer up. <c>OversizedPlanBacklogReadPins</c> holds the two
/// predicates against each other and then executes both.</para>
///
/// <para><b>No size is ever derived from the stored content, and that is a cost decision as well as a
/// correctness one.</b> <c>observed_bytes</c> is the monitored server's own <c>DATALENGTH</c> over
/// <c>nvarchar(max)</c> — UTF-16 BYTES, the same unit as
/// <see cref="QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes"/>, so it is the only size figure that can
/// be compared to the cap. PostgreSQL's <c>length(plan_xml)</c> is CHARACTERS and runs exactly half of it
/// on this content, so reporting one under a byte-shaped name would put every reader a factor of two below
/// the cap they were checking against. It is also the expensive read on this table: the captured column is
/// TOASTed and compresses about 16x, measured on one production store at 525 MB logical against 32 MB of
/// <c>pg_total_relation_size</c>, so a length over the whole table detoasts half a gigabyte to produce a
/// number nobody can use. <c>plan_xml IS NOT NULL</c> reads the varlena header and detoasts nothing, which
/// is why presence is reported and size is not.</para>
///
/// <para><b><c>rows_with_content</c> is beside <c>captured</c> rather than folded into it.</b> The capture
/// statement writes the stamp and the content together, so the two counts agree on a healthy store and the
/// second column looks redundant. It is the integrity check that is otherwise unavailable: the fallback
/// reads key on <c>plan_xml IS NOT NULL</c>
/// (<see cref="OversizedPlanBacklog.QueryStatsFallbackSql"/>), so a row stamped captured with no content is
/// invisible to <c>get_plan_xml</c> while counting as done here — the same shape of silent hole the whole
/// issue is about.</para>
///
/// <para><b>LEFT JOIN to the registry, and no <c>is_enabled</c> filter.</b> Retention prunes these rows on
/// <c>last_seen_at</c> and nothing prunes them when a server leaves the registry, so an inner join or an
/// enabled-only predicate would drop rows that are really there and hand back a total that is quietly
/// short. A row whose <c>server_id</c> no longer resolves reports a null <c>server_name</c> and still
/// counts.</para>
/// </summary>
internal static class DarlingOversizedPlanBacklogReader
{
    /// <summary>The verdict a row with content carries.</summary>
    public const string VerdictCaptured = "captured";

    /// <summary>The verdict a row carries once a fetch established that its handle no longer renders a
    /// plan.</summary>
    public const string VerdictExpired = "expired";

    /// <summary>The verdict a row the sweep will still go back for carries.</summary>
    public const string VerdictPending = "pending";

    /// <summary>#8567: the verdict a row carries once the sweep's trim dropped it unfetched: a newer sighting of
    /// the same query is waiting in its place, or the server was past its pending cap.</summary>
    public const string VerdictDropped = "dropped";

    /// <summary>
    /// The sweep's claim predicate, qualified to this read's alias — the definition of "still to do" that
    /// <see cref="OversizedPlanBacklog.ClaimSql"/> selects on, restated once here and reused by every
    /// pending-shaped aggregate below so the three of them cannot drift from each other either.
    /// </summary>
    private const string PendingPredicate = "b.captured_at IS NULL AND b.expired_at IS NULL";

    /// <summary>
    /// #8567: a row the sweep's trim dropped unfetched, qualified to this read's alias. The trim stamps
    /// <c>expired_at</c> on rows never attempted and a fetch expiry stamps <c>last_attempt_at</c> with it
    /// (<see cref="OversizedPlanBacklog.RecordExpirySql"/>), so the attempt stamp is what separates the two.
    /// </summary>
    private const string DroppedPredicate =
        "b.captured_at IS NULL AND b.expired_at IS NOT NULL AND b.last_attempt_at IS NULL";

    /// <summary>
    /// One row per server that has any backlog at all ($1 = an optional server_id, or NULL for every
    /// server), most pending work first.
    ///
    /// <para>Ordered by pending descending because the first question of any read of this table is which
    /// server the sweep has not caught up on. Ties break on total and then on name, so the ordering is
    /// total rather than partial — a page that reshuffles between two calls on equal counts reads as data
    /// changing when nothing did.</para>
    ///
    /// <para><c>percentile_disc</c> rather than <c>percentile_cont</c>, deliberately. The discrete
    /// percentile returns a value that is actually IN the column and keeps its type, so the median is a
    /// size some plan really measured and comes back as a <c>bigint</c> in the same unit as its neighbours.
    /// The continuous one interpolates between two rows and returns <c>double precision</c>, which would
    /// need a cast to survive a two-argument <c>round</c> — PostgreSQL has no
    /// <c>round(double precision, integer)</c> — and would report a byte count no plan ever had.</para>
    ///
    /// <para>#8570: the median is the discrete percentile read through the index, not computed by the aggregate. <c>percentile_disc(0.5)</c>
    /// is the value at position <c>ceil(n / 2)</c> of the sorted column, which is <c>OFFSET (n + 1) / 2 - 1</c> of
    /// <c>ORDER BY observed_bytes</c> (<c>observed_bytes</c> is NOT NULL, so every row counts, as it does in <c>count(*)</c>). The aggregate
    /// form sorted every row of every server, wide rows and all (3,000,000 rows: a 228 MB spill, 5.9 s), and no index can feed an
    /// ordered-set aggregate. The lateral read walks half of a server's rows through <c>idx_oversized_plan_backlog_server_bytes</c> (V260):
    /// two narrow columns, no heap fetch. The other aggregates run on their own hash aggregate. Without the index the same statement
    /// still answers, by sorting.</para>
    /// </summary>
    public const string PerServerRollupSql = @"
WITH held AS (
SELECT
    b.server_id                                                              AS server_id,
    count(*)                                                                 AS held_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @")                        AS pending_rows,
    count(*) FILTER (WHERE b.captured_at IS NOT NULL)                        AS captured_rows,
    count(*) FILTER (WHERE b.captured_at IS NULL
                     AND   b.expired_at IS NOT NULL
                     AND   b.last_attempt_at IS NOT NULL)                    AS expired_rows,
    count(*) FILTER (WHERE b.plan_xml IS NOT NULL)                           AS rows_with_content,
    count(*) FILTER (WHERE b.collector_name = '" + OversizedPlanBacklog.QueryStatsCollectorName + @"')
                                                                             AS query_stats_rows,
    count(*) FILTER (WHERE b.collector_name = '" + OversizedPlanBacklog.ProcedureStatsCollectorName + @"')
                                                                             AS procedure_stats_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @"
                     AND   b.attempt_count > 0)                              AS pending_rows_attempted,
    coalesce(sum(b.attempt_count) FILTER (WHERE " + PendingPredicate + @"), 0)
                                                                             AS pending_attempts,
    coalesce(max(b.attempt_count) FILTER (WHERE " + PendingPredicate + @"), 0)
                                                                             AS max_attempts_on_a_pending_row,
    min(b.first_seen_at)                                                     AS oldest_first_seen_at,
    max(b.last_seen_at)                                                      AS last_seen_at,
    max(b.last_attempt_at)                                                   AS last_attempt_at,
    max(b.captured_at)                                                       AS last_captured_at,
    max(b.expired_at) FILTER (WHERE b.last_attempt_at IS NOT NULL)           AS last_expired_at,
    min(b.observed_bytes)                                                    AS min_observed_bytes,
    max(b.observed_bytes)                                                    AS max_observed_bytes,
    count(*) FILTER (WHERE b.first_seen_at >= $2)                            AS rows_added_last_day,
    count(*) FILTER (WHERE b.captured_at >= $2)                              AS rows_captured_last_day,
    count(*) FILTER (WHERE b.captured_at IS NULL
                     AND   b.expired_at >= $2
                     AND   b.last_attempt_at IS NOT NULL)                    AS rows_expired_last_day,
    count(*) FILTER (WHERE " + DroppedPredicate + @")                        AS held_dropped_rows,
    count(*) FILTER (WHERE " + DroppedPredicate + @"
                     AND   b.expired_at >= $2)                               AS held_dropped_last_day
FROM " + OversizedPlanBacklog.TableName + @" AS b
WHERE ($1::integer IS NULL OR b.server_id = $1)
GROUP BY
    b.server_id),
dropped AS (
SELECT
    d.server_id                                                              AS server_id,
    sum(d.rows_dropped)                                                      AS counted_dropped_rows,
    coalesce(sum(d.rows_dropped) FILTER (WHERE d.hour_start >= $3), 0)
                                                                             AS counted_dropped_last_day,
    coalesce(sum(d.rows_first_seen) FILTER (WHERE d.hour_start >= $3), 0)
                                                                             AS counted_added_last_day,
    coalesce(sum(d.rows_dropped) FILTER (WHERE d.collector_name = '" + OversizedPlanBacklog.QueryStatsCollectorName + @"'), 0)
                                                                             AS counted_query_stats_rows,
    coalesce(sum(d.rows_dropped) FILTER (WHERE d.collector_name = '" + OversizedPlanBacklog.ProcedureStatsCollectorName + @"'), 0)
                                                                             AS counted_procedure_stats_rows
FROM " + OversizedPlanBacklog.DropCountsTableName + @" AS d
WHERE ($1::integer IS NULL OR d.server_id = $1)
GROUP BY
    d.server_id),
agg AS (
SELECT
    coalesce(h.server_id, d.server_id)                                       AS server_id,
    coalesce(h.held_rows, 0)                                                 AS held_rows,
    coalesce(h.held_rows, 0) + coalesce(d.counted_dropped_rows, 0)           AS total_rows,
    coalesce(h.pending_rows, 0)                                              AS pending_rows,
    coalesce(h.captured_rows, 0)                                             AS captured_rows,
    coalesce(h.expired_rows, 0)                                              AS expired_rows,
    coalesce(h.rows_with_content, 0)                                         AS rows_with_content,
    coalesce(h.query_stats_rows, 0) + coalesce(d.counted_query_stats_rows, 0)
                                                                             AS query_stats_rows,
    coalesce(h.procedure_stats_rows, 0) + coalesce(d.counted_procedure_stats_rows, 0)
                                                                             AS procedure_stats_rows,
    coalesce(h.pending_rows_attempted, 0)                                    AS pending_rows_attempted,
    coalesce(h.pending_attempts, 0)                                          AS pending_attempts,
    coalesce(h.max_attempts_on_a_pending_row, 0)                             AS max_attempts_on_a_pending_row,
    h.oldest_first_seen_at                                                   AS oldest_first_seen_at,
    h.last_seen_at                                                           AS last_seen_at,
    h.last_attempt_at                                                        AS last_attempt_at,
    h.last_captured_at                                                       AS last_captured_at,
    h.last_expired_at                                                        AS last_expired_at,
    h.min_observed_bytes                                                     AS min_observed_bytes,
    h.max_observed_bytes                                                     AS max_observed_bytes,
    coalesce(h.rows_added_last_day, 0) + coalesce(d.counted_added_last_day, 0)
                                                                             AS rows_added_last_day,
    coalesce(h.rows_captured_last_day, 0)                                    AS rows_captured_last_day,
    coalesce(h.rows_expired_last_day, 0)                                     AS rows_expired_last_day,
    coalesce(h.held_dropped_rows, 0) + coalesce(d.counted_dropped_rows, 0)   AS dropped_rows,
    coalesce(h.held_dropped_last_day, 0) + coalesce(d.counted_dropped_last_day, 0)
                                                                             AS rows_dropped_last_day
FROM held AS h
FULL JOIN dropped AS d
  ON d.server_id = h.server_id)
SELECT
    agg.server_id,
    s.server_name,
    agg.total_rows,
    agg.pending_rows,
    agg.captured_rows,
    agg.expired_rows,
    agg.rows_with_content,
    agg.query_stats_rows,
    agg.procedure_stats_rows,
    agg.pending_rows_attempted,
    agg.pending_attempts,
    agg.max_attempts_on_a_pending_row,
    agg.oldest_first_seen_at,
    agg.last_seen_at,
    agg.last_attempt_at,
    agg.last_captured_at,
    agg.last_expired_at,
    agg.min_observed_bytes,
    med.observed_bytes                                                       AS median_observed_bytes,
    agg.max_observed_bytes,
    agg.rows_added_last_day,
    agg.rows_captured_last_day,
    agg.rows_expired_last_day,
    agg.dropped_rows,
    agg.rows_dropped_last_day
FROM agg
LEFT JOIN servers AS s
  ON s.server_id = agg.server_id
LEFT JOIN LATERAL
(
    SELECT m.observed_bytes
    FROM " + OversizedPlanBacklog.TableName + @" AS m
    WHERE m.server_id = agg.server_id
    ORDER BY m.observed_bytes
    OFFSET greatest((agg.held_rows + 1) / 2 - 1, 0)
    LIMIT 1
) AS med ON true
ORDER BY
    agg.pending_rows DESC,
    agg.total_rows DESC,
    agg.server_id";

    /// <summary>
    /// The same scope ($1 = an optional server_id, or NULL for every server) split by COLLECTOR rather
    /// than by server — the census, and the first thing to read.
    ///
    /// <para>#8570: this is the FLEET form (no parameter; <see cref="CollectorCensusForServerSql"/> is the one-server form). It used to
    /// sort every row of the table twice, for <c>count(DISTINCT server_id)</c> (7 s of 8 on a 3,000,000-row store) and for the median. The
    /// server count now comes from a hash aggregate over the distinct (collector, server) pairs, and the median from
    /// <c>idx_oversized_plan_backlog_collector_bytes</c> (V260), the way <see cref="PerServerRollupSql"/> reads its own.</para>
    ///
    /// <para>#8570: the heap is read once. The counts come from one pass grouped by (collector, server), and the per-collector totals and the
    /// server count are taken from those few rows, where the server count used to be a second full scan of the table for its distinct pairs.
    /// On a field store (1.23 M rows, 394 MB) the first call after a restart read the whole table twice from disk in this statement and took
    /// 16.1 s. The counts of one collector are sums of its servers' counts, its extremes are the extremes of theirs, so the rows are the same.
    /// What a read of this table still cannot avoid without an index that carries the stamps is the one pass over the heap.</para>
    ///
    /// <para>It exists because the two collectors' shares of this table are not predictable from each
    /// other and not stable across stores: measured the day after the V121 fleet install,
    /// <c>procedure_stats</c> held the larger half on one production store (1,111 rows against 1,095) and
    /// 3.6% of the other (199 against 5,306). A rollup that only totalled per server would let a reader
    /// carry an assumption about which collector's plans they were looking at, and that assumption is
    /// wrong on one of the two stores whichever way they guess.</para>
    /// </summary>
    public const string CollectorCensusSql = @"
WITH pairs AS (
SELECT
    b.collector_name                                                         AS collector_name,
    b.server_id                                                              AS server_id,
    count(*)                                                                 AS held_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @")                        AS pending_rows,
    count(*) FILTER (WHERE b.captured_at IS NOT NULL)                        AS captured_rows,
    count(*) FILTER (WHERE b.captured_at IS NULL
                     AND   b.expired_at IS NOT NULL
                     AND   b.last_attempt_at IS NOT NULL)                    AS expired_rows,
    max(b.captured_at)                                                       AS last_captured_at,
    max(b.expired_at) FILTER (WHERE b.last_attempt_at IS NOT NULL)           AS last_expired_at,
    min(b.observed_bytes)                                                    AS min_observed_bytes,
    max(b.observed_bytes)                                                    AS max_observed_bytes,
    count(*) FILTER (WHERE " + DroppedPredicate + @")                        AS held_dropped_rows
FROM " + OversizedPlanBacklog.TableName + @" AS b
GROUP BY
    b.collector_name,
    b.server_id),
held AS (
SELECT
    p.collector_name                                                         AS collector_name,
    sum(p.held_rows)::bigint                                                 AS held_rows,
    sum(p.pending_rows)::bigint                                              AS pending_rows,
    sum(p.captured_rows)::bigint                                             AS captured_rows,
    sum(p.expired_rows)::bigint                                              AS expired_rows,
    max(p.last_captured_at)                                                  AS last_captured_at,
    max(p.last_expired_at)                                                   AS last_expired_at,
    min(p.min_observed_bytes)                                                AS min_observed_bytes,
    max(p.max_observed_bytes)                                                AS max_observed_bytes,
    sum(p.held_dropped_rows)::bigint                                         AS held_dropped_rows
FROM pairs AS p
GROUP BY
    p.collector_name),
dropped AS (
SELECT
    d.collector_name                                                         AS collector_name,
    sum(d.rows_dropped)                                                      AS counted_dropped_rows
FROM " + OversizedPlanBacklog.DropCountsTableName + @" AS d
GROUP BY
    d.collector_name),
sv AS (
SELECT
    x.collector_name,
    count(*)                                                                 AS servers
FROM
(
    SELECT p.collector_name, p.server_id
    FROM pairs AS p
    UNION
    SELECT d.collector_name, d.server_id
    FROM " + OversizedPlanBacklog.DropCountsTableName + @" AS d
    WHERE d.rows_dropped > 0
) AS x
GROUP BY
    x.collector_name),
agg AS (
SELECT
    coalesce(h.collector_name, d.collector_name)                             AS collector_name,
    coalesce(h.held_rows, 0)                                                 AS held_rows,
    coalesce(h.held_rows, 0) + coalesce(d.counted_dropped_rows, 0)           AS total_rows,
    coalesce(h.pending_rows, 0)                                              AS pending_rows,
    coalesce(h.captured_rows, 0)                                             AS captured_rows,
    coalesce(h.expired_rows, 0)                                              AS expired_rows,
    h.last_captured_at                                                       AS last_captured_at,
    h.last_expired_at                                                        AS last_expired_at,
    h.min_observed_bytes                                                     AS min_observed_bytes,
    h.max_observed_bytes                                                     AS max_observed_bytes,
    coalesce(h.held_dropped_rows, 0) + coalesce(d.counted_dropped_rows, 0)   AS dropped_rows
FROM held AS h
FULL JOIN dropped AS d
  ON d.collector_name = h.collector_name)
SELECT
    agg.collector_name,
    coalesce(sv.servers, 0)                                                  AS servers,
    agg.total_rows,
    agg.pending_rows,
    agg.captured_rows,
    agg.expired_rows,
    agg.last_captured_at,
    agg.last_expired_at,
    agg.min_observed_bytes,
    med.observed_bytes                                                       AS median_observed_bytes,
    agg.max_observed_bytes,
    agg.dropped_rows
FROM agg
LEFT JOIN sv
  ON sv.collector_name = agg.collector_name
LEFT JOIN LATERAL
(
    SELECT m.observed_bytes
    FROM " + OversizedPlanBacklog.TableName + @" AS m
    WHERE m.collector_name = agg.collector_name
    ORDER BY m.observed_bytes
    OFFSET greatest((agg.held_rows + 1) / 2 - 1, 0)
    LIMIT 1
) AS med ON true
ORDER BY
    agg.total_rows DESC,
    agg.collector_name";

    /// <summary>
    /// The census for one server ($1 = server_id): the statement this read had before #8570, unchanged. One server's rows are a small,
    /// bounded set (the sweep caps what it keeps per server), so the aggregate's own sort of them is cheap, and a per-collector median for
    /// one server cannot ride <c>idx_oversized_plan_backlog_collector_bytes</c>, which holds no server.
    /// </summary>
    public const string CollectorCensusForServerSql = @"
WITH held AS (
SELECT
    b.collector_name                                                         AS collector_name,
    count(*)                                                                 AS held_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @")                        AS pending_rows,
    count(*) FILTER (WHERE b.captured_at IS NOT NULL)                        AS captured_rows,
    count(*) FILTER (WHERE b.captured_at IS NULL
                     AND   b.expired_at IS NOT NULL
                     AND   b.last_attempt_at IS NOT NULL)                    AS expired_rows,
    max(b.captured_at)                                                       AS last_captured_at,
    max(b.expired_at) FILTER (WHERE b.last_attempt_at IS NOT NULL)           AS last_expired_at,
    min(b.observed_bytes)                                                    AS min_observed_bytes,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY b.observed_bytes)            AS median_observed_bytes,
    max(b.observed_bytes)                                                    AS max_observed_bytes,
    count(*) FILTER (WHERE " + DroppedPredicate + @")                        AS held_dropped_rows
FROM " + OversizedPlanBacklog.TableName + @" AS b
WHERE b.server_id = $1
GROUP BY
    b.collector_name),
dropped AS (
SELECT
    d.collector_name                                                         AS collector_name,
    sum(d.rows_dropped)                                                      AS counted_dropped_rows
FROM " + OversizedPlanBacklog.DropCountsTableName + @" AS d
WHERE d.server_id = $1
GROUP BY
    d.collector_name)
SELECT
    coalesce(h.collector_name, d.collector_name)                             AS collector_name,
    1                                                                        AS servers,
    coalesce(h.held_rows, 0) + coalesce(d.counted_dropped_rows, 0)           AS total_rows,
    coalesce(h.pending_rows, 0)                                              AS pending_rows,
    coalesce(h.captured_rows, 0)                                             AS captured_rows,
    coalesce(h.expired_rows, 0)                                              AS expired_rows,
    h.last_captured_at                                                       AS last_captured_at,
    h.last_expired_at                                                        AS last_expired_at,
    h.min_observed_bytes                                                     AS min_observed_bytes,
    h.median_observed_bytes                                                  AS median_observed_bytes,
    h.max_observed_bytes                                                     AS max_observed_bytes,
    coalesce(h.held_dropped_rows, 0) + coalesce(d.counted_dropped_rows, 0)   AS dropped_rows
FROM held AS h
FULL JOIN dropped AS d
  ON d.collector_name = h.collector_name
ORDER BY
    total_rows DESC,
    collector_name";

    /// <summary>
    /// One server's rows ($1 = server_id, $2 = row cap), largest plan first — the claim key, the plan's
    /// identity, its measured size and all four timestamps, which is everything needed to take a
    /// <c>query_hash</c> from here into <c>get_plan_xml</c> and exercise
    /// <see cref="OversizedPlanBacklog.QueryStatsFallbackSql"/> end to end.
    ///
    /// <para>Server-scoped rather than fleet-wide because the claim key is only meaningful within one
    /// server and the two handles are long hex strings — a fleet listing would be mostly identifiers for
    /// servers the caller did not ask about.</para>
    ///
    /// <para><b>Largest first, which is deliberately NOT the claim's order.</b> The claim takes tried rows
    /// oldest attempt first, at most half its slots, and the never-tried rows by size; ordering this listing the same way
    /// would make it look like a prediction of the sweep's next three picks while omitting the rows it
    /// will skip. Size descending answers the question a reader actually has — which plans the cap cost
    /// the most visibility on — and the key columns in the tiebreak keep the page stable.</para>
    /// </summary>
    public const string RowsSql = @"
SELECT
    b.collector_name                                                         AS collector_name,
    b.plan_handle                                                            AS plan_handle,
    b.sql_handle                                                             AS sql_handle,
    b.statement_start_offset                                                 AS statement_start_offset,
    b.statement_end_offset                                                   AS statement_end_offset,
    b.database_name                                                          AS database_name,
    b.query_hash                                                             AS query_hash,
    b.observed_bytes                                                         AS observed_bytes,
    b.first_seen_at                                                          AS first_seen_at,
    b.last_seen_at                                                           AS last_seen_at,
    b.captured_at                                                            AS captured_at,
    b.expired_at                                                             AS expired_at,
    b.last_attempt_at                                                        AS last_attempt_at,
    b.attempt_count                                                          AS attempt_count,
    (b.plan_xml IS NOT NULL)                                                 AS has_plan_xml
FROM " + OversizedPlanBacklog.TableName + @" AS b
WHERE b.server_id = $1
ORDER BY
    b.observed_bytes DESC,
    b.first_seen_at,
    b.plan_handle,
    b.statement_start_offset
LIMIT $2";

    /// <summary>
    /// Which bucket a row falls in, from its two verdict stamps — the row-level twin of the rollup's three
    /// FILTER predicates, so a listed row and the count it contributed to can never disagree about what it
    /// is.
    ///
    /// <para>Content wins over an expiry, matching the store: a capture clears any standing
    /// <c>expired_at</c>, so a row carrying both stamps is a row that was retired and later fetched
    /// anyway, and the useful thing to say about it is that the plan is available.</para>
    /// </summary>
    public static string VerdictOf(DateTime? capturedAt, DateTime? expiredAt, DateTime? lastAttemptAt) =>
        capturedAt.HasValue ? VerdictCaptured
        : !expiredAt.HasValue ? VerdictPending
        : lastAttemptAt.HasValue ? VerdictExpired
        : VerdictDropped;

    /// <summary>One server's backlog, rolled up.</summary>
    public sealed record BacklogServerRollup(
        int ServerId,
        string? ServerName,
        long TotalRows,
        long PendingRows,
        long CapturedRows,
        long ExpiredRows,
        long RowsWithContent,
        long QueryStatsRows,
        long ProcedureStatsRows,
        long PendingRowsAttempted,
        long PendingAttempts,
        int MaxAttemptsOnAPendingRow,
        DateTime? OldestFirstSeenAt,
        DateTime? LastSeenAt,
        DateTime? LastAttemptAt,
        DateTime? LastCapturedAt,
        DateTime? LastExpiredAt,
        long? MinObservedBytes,
        long? MedianObservedBytes,
        long? MaxObservedBytes,
        long RowsAddedLastDay,
        long RowsCapturedLastDay,
        long RowsExpiredLastDay,
        long DroppedRows,
        long RowsDroppedLastDay);

    /// <summary>One collector's share of the scope.</summary>
    public sealed record BacklogCollectorCensus(
        string CollectorName,
        int Servers,
        long TotalRows,
        long PendingRows,
        long CapturedRows,
        long ExpiredRows,
        DateTime? LastCapturedAt,
        DateTime? LastExpiredAt,
        long? MinObservedBytes,
        long? MedianObservedBytes,
        long? MaxObservedBytes,
        long DroppedRows);

    /// <summary>One backlog row: the claim key, the plan's identity, and every stamp.</summary>
    public sealed record BacklogRow(
        string CollectorName,
        string PlanHandle,
        string SqlHandle,
        int StatementStartOffset,
        int StatementEndOffset,
        string? DatabaseName,
        string? QueryHash,
        long ObservedBytes,
        DateTime FirstSeenAt,
        DateTime LastSeenAt,
        DateTime? CapturedAt,
        DateTime? ExpiredAt,
        DateTime? LastAttemptAt,
        int AttemptCount,
        bool HasPlanXml)
    {
        /// <summary>This row's bucket — see <see cref="VerdictOf"/>.</summary>
        public string Verdict => VerdictOf(CapturedAt, ExpiredAt, LastAttemptAt);
    }

    /// <summary>#8691: the top of the hour <paramref name="lastDaySince"/> falls in, where the per-hour drop counts of the last day start.</summary>
    public static DateTime DayStartHour(DateTime lastDaySince) =>
        new(lastDaySince.Year, lastDaySince.Month, lastDaySince.Day, lastDaySince.Hour, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// #8404: how many sweep passes FINISHED in the day before the read, from the run records the sweep writes under
    /// the fleet sentinel when a pass ends, one per pass whatever the pass found
    /// (<c>DarlingObservability.LogOversizedPlanSweepRunAsync</c>). <c>$1</c> is the sentinel id, <c>$2</c> the sweep's
    /// collector name and <c>$3</c> the instant the day starts at; the statement reads no clock of its own. The worker
    /// launches a pass only after the previous one has finished, so on a store whose passes outrun their interval this
    /// count sits below the interval's, and it is the count the day's drain is held against. A pass in flight at the
    /// read has stamped its captures and has no record yet, and the count is store-wide, so it includes passes that
    /// reached no server; the reader's text says both.
    /// </summary>
    public const string SweepPassesLastDaySql = @"
SELECT count(*)
FROM collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   collection_time >= $3";

    public static async Task<int> GetSweepPassesLastDayAsync(
        NpgsqlDataSource postgres, DateTime lastDaySince, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(SweepPassesLastDaySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(DarlingObservability.FleetServerId);
        command.Parameters.AddWithValue(DarlingObservability.OversizedPlanSweepCollectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(lastDaySince, DateTimeKind.Unspecified));
        var count = await command.ExecuteScalarAsync(cancellationToken);
        return count is long passes ? (int)passes : 0;
    }

    /// <param name="lastDaySince">
    /// #8404: the naive-UTC instant the three last-day counts start at, one day before the read as the caller's clock
    /// has it. A parameter rather than <c>now()</c> in the statement, so the read has one clock, the caller's, and the
    /// statement takes no time of its own.
    /// </param>
    public static async Task<List<BacklogServerRollup>> GetPerServerRollupAsync(
        NpgsqlDataSource postgres, int? serverId, DateTime lastDaySince, CancellationToken cancellationToken = default)
    {
        var rows = new List<BacklogServerRollup>();

        await using var command = postgres.CreateCommand(PerServerRollupSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId.HasValue ? serverId.Value : (object)DBNull.Value);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(lastDaySince, DateTimeKind.Unspecified));
        // #8691: the drop counts are kept per hour, so their day starts at the top of the hour the day starts in.
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DayStartHour(lastDaySince), DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BacklogServerRollup(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.GetInt64(9),
                reader.GetInt64(10),
                reader.GetInt32(11),
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                reader.IsDBNull(17) ? null : reader.GetInt64(17),
                reader.IsDBNull(18) ? null : reader.GetInt64(18),
                reader.IsDBNull(19) ? null : reader.GetInt64(19),
                reader.GetInt64(20),
                reader.GetInt64(21),
                reader.GetInt64(22),
                reader.GetInt64(23),
                reader.GetInt64(24)));
        }

        return rows;
    }

    public static async Task<List<BacklogCollectorCensus>> GetCollectorCensusAsync(
        NpgsqlDataSource postgres, int? serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<BacklogCollectorCensus>();

        // #8570: the fleet form has no parameter and reads its median through an index; one server keeps the statement it had.
        await using var command = postgres.CreateCommand(serverId.HasValue ? CollectorCensusForServerSql : CollectorCensusSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        if (serverId.HasValue)
        {
            command.Parameters.AddWithValue(serverId.Value);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BacklogCollectorCensus(
                reader.GetString(0),
                reader.GetInt32(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.GetInt64(11)));
        }

        return rows;
    }

    public static async Task<List<BacklogRow>> GetRowsAsync(
        NpgsqlDataSource postgres, int serverId, int limit, CancellationToken cancellationToken = default)
    {
        var rows = new List<BacklogRow>();

        await using var command = postgres.CreateCommand(RowsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BacklogRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt32(3),
                reader.GetInt32(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.GetInt64(7),
                reader.GetDateTime(8),
                reader.GetDateTime(9),
                reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                reader.GetInt32(13),
                reader.GetBoolean(14)));
        }

        return rows;
    }
}
