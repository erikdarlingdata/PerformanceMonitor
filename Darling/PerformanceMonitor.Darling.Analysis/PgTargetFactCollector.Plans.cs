/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// How many flipped statements the plan reads return, at most — a PAGE SIZE, not a threshold: it bounds the
    /// rows one pass sifts for the one <c>PG_PLAN_REGRESSION</c> / <c>PG_PARAMETER_SENSITIVITY</c> fact each key
    /// carries (the keys are static, so a pass emits one of each — the worst — and counts the rest in metadata).
    /// Fifty flipped statements in one window is already a server re-planning wholesale, which the count says.
    /// </summary>
    internal const int PlanFlipCandidateCount = 50;

    /// <summary>
    /// How far before the window's START the state reads look for their latest row: <c>pg_plan_capture_readiness</c>
    /// (hourly), <c>pg_extension_availability</c> and <c>pg_column_stats</c> (daily) and <c>pg_predicate_stats</c>
    /// (hourly) describe a STATE — is the library loaded, is the extension installed, how skewed is this column, which
    /// columns does this statement filter on — not a rate over the window, so a four-hour window may hold no row of
    /// theirs at all and the latest row before it is the honest answer. Two days is two runs of the daily collectors:
    /// one missed run survives, a collector silent for longer reads as "unknown" and the family says nothing rather
    /// than reading last month's state as today's. A STALENESS horizon for a state read, not a bar.
    /// </summary>
    internal const int PlanStateLookbackDays = 2;

    /// <summary>
    /// How many captured plans the Seq-Scan read walks, at most, newest first — a PAGE SIZE, not a threshold. Each row
    /// carries a whole redacted <c>plan_json</c> (kilobytes), so the read is bounded in bytes as well as rows; two
    /// thousand logged-slow plans in one window is a server past <c>auto_explain.log_min_duration</c> two thousand
    /// times, which the truncated count still says. Truncation lowers a figure the advice already calls a lower bound.
    /// The text prefilter (<c>plan_json LIKE '%Seq Scan%'</c>) keeps plans without the node off the wire entirely;
    /// the walker is what decides.
    /// </summary>
    internal const int SeqScanCaptureRowCap = 2_000;

    /// <summary>
    /// The captured plans that MAY hold a Seq Scan node, newest first — the read behind <c>PG_SEQ_SCAN_ADVISORY</c>'s
    /// walk (<see cref="WalkSeqScans"/>). <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the row
    /// cap (<see cref="SeqScanCaptureRowCap"/>). Orphans (<c>query_id = 0</c>, no <c>%Q</c>) are excluded as the flip
    /// read excludes them: a scan with no statement joins no predicate and no bad actor. The <c>LIKE</c> is a
    /// prefilter on the redacted JSON text, never the decision — a plan whose <c>Filter</c> prose happened to hold the
    /// words is walked and found to have no such node.
    /// </summary>
    public const string PgTargetSeqScanCapturesSql = @"
SELECT query_id,
       plan_json
FROM pg_plan_capture
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_id IS NOT NULL
AND   query_id <> 0
AND   plan_json IS NOT NULL
AND   plan_json LIKE '%Seq Scan%'
ORDER BY collection_time DESC
LIMIT $4";

    /// <summary>
    /// The two witnesses for every (statement, relation) pair the walk found, in ONE round trip: the predicate
    /// (<c>pg_qualstats</c> via <c>pg_predicate_stats</c>) and the size (<c>pg_table_bloat_stats</c>). <c>$1</c>
    /// server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the state lookback days
    /// (<see cref="PlanStateLookbackDays"/> — both tables describe a STATE, hourly, so the newest row before the
    /// window's end inside the lookback is the honest answer), <c>$5</c> the statement ids and <c>$6</c> the relation
    /// names as parallel arrays (<c>unnest</c> in the select list walks them in lockstep, so the pairs need no
    /// temporary table and the FROM/JOIN census sees only collector tables and CTEs).
    ///
    /// <para><b>The predicate witness is the newest row per column, the most selective column per pair.</b>
    /// <c>pg_qualstats</c>' counters are cumulative and the reader's discipline (<c>DarlingPgPredicateStatsReader</c>)
    /// is newest-not-differenced: the question is the accumulated shape, and a delta would fight the sampler. Per
    /// (statement, database, schema, table, column) the newest row inside the lookback is taken; per (statement,
    /// database, schema, table) the columns are listed most-selective-first (<c>predicate_columns</c> — the ONLY
    /// place a column name enters this family, and the only source the advice may name an index column from) with
    /// the most selective column's figures on the row; per (statement, table) the most selective (database, schema)
    /// wins, since the plan's <c>Relation Name</c> carries neither. <c>rows_evaluated = 0</c> rows are dropped (a
    /// predicate sampled at the moment it evaluated nothing has no selectivity).</para>
    ///
    /// <para><b>The size witness prefers the same relation the predicate named.</b> <c>heap_bytes</c> is per
    /// (database, schema, table); when the predicate witness knows the database and schema, that relation's newest
    /// sample is used; otherwise the LARGEST same-named relation on the server, with <c>relations_named</c> saying
    /// how many share the name so the advice can say the size is the largest candidate's, not "the" relation's.</para>
    /// </summary>
    public const string PgTargetSeqScanWitnessSql = @"
WITH candidates AS (
    SELECT DISTINCT query_id, table_name
    FROM (SELECT unnest($5::bigint[]) AS query_id, unnest($6::text[]) AS table_name) AS pairs
),
newest AS (
    SELECT DISTINCT ON (p.query_id, p.database_name, p.schema_name, p.table_name, p.column_name)
           p.query_id,
           p.database_name,
           p.schema_name,
           p.table_name,
           p.column_name,
           p.rows_evaluated,
           p.rows_filtered,
           p.rows_filtered::double precision / p.rows_evaluated AS selectivity,
           p.sample_rate,
           p.worst_estimate_error_ratio
    FROM pg_predicate_stats AS p
    JOIN candidates AS c
      ON c.query_id = p.query_id
     AND c.table_name = p.table_name
    WHERE p.server_id = $1
    AND   p.collection_time <= $3
    AND   p.collection_time >= $2 - make_interval(days => $4)
    AND   p.column_name IS NOT NULL
    AND   p.rows_evaluated > 0
    ORDER BY p.query_id, p.database_name, p.schema_name, p.table_name, p.column_name, p.collection_time DESC
),
per_relation AS (
    SELECT query_id,
           database_name,
           schema_name,
           table_name,
           MAX(selectivity)                                                        AS selectivity,
           (array_agg(rows_evaluated ORDER BY selectivity DESC, column_name))[1]   AS rows_evaluated,
           (array_agg(rows_filtered  ORDER BY selectivity DESC, column_name))[1]   AS rows_filtered,
           MIN(sample_rate)                                                        AS sample_rate,
           MAX(worst_estimate_error_ratio)                                         AS worst_estimate_error_ratio,
           string_agg(column_name, ', ' ORDER BY selectivity DESC, column_name)    AS predicate_columns,
           COUNT(*)                                                                AS predicate_column_count
    FROM newest
    GROUP BY query_id, database_name, schema_name, table_name
),
witness AS (
    SELECT DISTINCT ON (query_id, table_name) *
    FROM per_relation
    ORDER BY query_id, table_name, selectivity DESC, database_name, schema_name
),
heaps AS (
    SELECT DISTINCT ON (b.database_name, b.schema_name, b.table_name)
           b.database_name,
           b.schema_name,
           b.table_name,
           b.heap_bytes
    FROM pg_table_bloat_stats AS b
    JOIN candidates AS c
      ON c.table_name = b.table_name
    WHERE b.server_id = $1
    AND   b.collection_time <= $3
    AND   b.collection_time >= $2 - make_interval(days => $4)
    AND   b.heap_bytes IS NOT NULL
    ORDER BY b.database_name, b.schema_name, b.table_name, b.collection_time DESC
),
sizes AS (
    SELECT table_name,
           MAX(heap_bytes) AS heap_bytes,
           COUNT(*)        AS relations_named
    FROM heaps
    GROUP BY table_name
)
SELECT c.query_id,
       c.table_name,
       w.database_name,
       w.schema_name,
       w.selectivity,
       w.rows_evaluated,
       w.rows_filtered,
       w.sample_rate,
       w.worst_estimate_error_ratio,
       w.predicate_columns,
       COALESCE(h.heap_bytes, z.heap_bytes) AS heap_bytes,
       CASE WHEN h.heap_bytes IS NOT NULL THEN 1 ELSE z.relations_named END AS relations_named
FROM candidates AS c
LEFT JOIN witness AS w
  ON w.query_id = c.query_id
 AND w.table_name = c.table_name
LEFT JOIN heaps AS h
  ON h.table_name = c.table_name
 AND h.database_name = w.database_name
 AND h.schema_name = w.schema_name
LEFT JOIN sizes AS z
  ON z.table_name = c.table_name
ORDER BY c.query_id, c.table_name";

    /// <summary>
    /// The two trackedness facets this family needs, as ONE row: whether <c>auto_explain</c> is loaded (the
    /// <c>library_loaded</c> facet of <c>pg_plan_capture_readiness</c> — the one precondition without which no plan is
    /// ever written to the log; the other facets refine a capture that can happen at all) and the state of
    /// <c>pg_qualstats</c> in <c>pg_extension_availability</c> (installed in ANY database counts — the extension is
    /// per-database and the predicate rows carry their database). Each column is NULL when the collector has no row
    /// inside <see cref="PlanStateLookbackDays"/> — "unknown", which the caller treats as silence, never as "off".
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the lookback days.
    /// </summary>
    public const string PgTargetPlanTrackednessSql = @"
WITH readiness AS (
    SELECT is_satisfied
    FROM pg_plan_capture_readiness
    WHERE server_id = $1
    AND   collection_time <= $3
    AND   collection_time >= $2 - make_interval(days => $4)
    AND   facet = 'library_loaded'
    ORDER BY collection_time DESC
    LIMIT 1
),
qualstats AS (
    SELECT state
    FROM pg_extension_availability
    WHERE server_id = $1
    AND   collection_time <= $3
    AND   collection_time >= $2 - make_interval(days => $4)
    AND   extension_name = 'pg_qualstats'
    ORDER BY (state IN ('installed', 'outdated')) DESC, collection_time DESC
    LIMIT 1
)
SELECT (SELECT is_satisfied FROM readiness) AS auto_explain_loaded,
       (SELECT state FROM qualstats)        AS pg_qualstats_state";

    /// <summary>
    /// Every statement captured under at least two distinct <c>plan_hash</c> values in the window, with its stored
    /// <c>pg_statement_stats</c> deltas split at the flip — the read behind <c>PG_PLAN_REGRESSION</c>. <c>$1</c>
    /// server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the row cap (<see cref="PlanFlipCandidateCount"/>).
    ///
    /// <para><b>The plan the window opened with against the plan it closed with.</b> Per (<c>query_id</c>,
    /// <c>plan_hash</c>) the captures collapse to a first-seen time; ordered by first-seen, the FIRST hash is
    /// "before" and the LAST is "after". <c>pg_statement_stats</c> is then read for the same statements over the same
    /// window and its STORED deltas (<c>delta_calls</c>, <c>delta_total_exec_time_ms</c> — the collector's, never a
    /// re-differencing of the cumulative columns; <c>PgTargetTopStatementsSql</c>'s discipline, same reasons) are
    /// summed on each side: rows stamped before the SECOND hash first appeared are "before" (the opening plan's
    /// period, pure), rows stamped at or after the LAST hash first appeared are "after" (the closing plan's period,
    /// pure). With two hashes the two moments coincide and the split is the flip; with three or more, the middle
    /// plans' minutes belong to neither side — counted (<c>hash_count</c>), never averaged into a plan they were not
    /// run under. A statement bouncing between plans is <c>PG_PARAMETER_SENSITIVITY</c>'s shape, read by
    /// <see cref="PgTargetPlanSensitivitySql"/> from the same captures. The one snapshot whose interval straddles a
    /// moment lands on the later side whole — stated, not corrected: the interval is a minute and the sides are
    /// hours, and pulling the straddle apart would need the interval the row may not carry (pre-V128).</para>
    ///
    /// <para><b>The hashes travel as their leading 48 bits.</b> <c>plan_hash</c> is 32 hex characters of a SHA-256
    /// (<c>PgPlanLogParser.Hash</c>) and <c>Fact.Metadata</c> is doubles-only; the first 12 characters are 48 bits,
    /// exact in a double, and a prefix a reader can match against <c>get_pg_plans</c>' full hashes. A hash that is
    /// not 12 hex characters (a planted or foreign row) yields NULL rather than a cast error, and the advice omits
    /// the clause.</para>
    ///
    /// <para><b>Orphans are excluded.</b> <c>query_id = 0</c> is a plan whose <c>log_line_prefix</c> carried no
    /// <c>%Q</c> — the collector's documented orphan — and joins no statement; it is dropped here rather than
    /// becoming a statement with a hash count.</para>
    /// </summary>
    public const string PgTargetPlanFlipSql = @"
WITH captures AS (
    SELECT query_id,
           plan_hash,
           MIN(collection_time) AS first_seen,
           COUNT(*)             AS captures,
           MAX(node_count)      AS node_count,
           MIN(top_node_type)   AS top_node_type,
           CASE WHEN plan_hash ~ '^[0-9A-Fa-f]{12}'
                THEN ('x' || left(plan_hash, 12))::bit(48)::bigint
           END                  AS hash_prefix
    FROM pg_plan_capture
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
    AND   query_id <> 0
    AND   plan_hash IS NOT NULL
    GROUP BY query_id, plan_hash
),
ordered AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY first_seen, plan_hash) AS rn,
           COUNT(*)     OVER (PARTITION BY query_id)                                AS hash_count
    FROM captures
),
flipped AS (
    SELECT f.query_id,
           f.hash_count,
           f.hash_prefix   AS hash_before,
           l.hash_prefix   AS hash_after,
           f.top_node_type AS top_node_before,
           l.top_node_type AS top_node_after,
           f.node_count    AS nodes_before,
           l.node_count    AS nodes_after,
           n.first_seen    AS first_flip_time,
           l.first_seen    AS flip_time
    FROM ordered AS f
    JOIN ordered AS n
      ON n.query_id = f.query_id
     AND n.rn = 2
    JOIN ordered AS l
      ON l.query_id = f.query_id
     AND l.rn = f.hash_count
    WHERE f.rn = 1
    AND   f.hash_count >= 2
),
sides AS (
    SELECT fl.query_id,
           SUM(s.delta_calls)              FILTER (WHERE s.collection_time <  fl.first_flip_time) AS calls_before,
           SUM(s.delta_total_exec_time_ms) FILTER (WHERE s.collection_time <  fl.first_flip_time) AS ms_before,
           SUM(s.delta_calls)              FILTER (WHERE s.collection_time >= fl.flip_time)       AS calls_after,
           SUM(s.delta_total_exec_time_ms) FILTER (WHERE s.collection_time >= fl.flip_time)       AS ms_after
    FROM flipped AS fl
    JOIN pg_statement_stats AS s
      ON s.queryid = fl.query_id
    WHERE s.server_id = $1
    AND   s.collection_time >= $2
    AND   s.collection_time <= $3
    GROUP BY fl.query_id
)
SELECT fl.query_id,
       fl.hash_count,
       fl.hash_before,
       fl.hash_after,
       fl.flip_time,
       fl.first_flip_time,
       (fl.top_node_before IS DISTINCT FROM fl.top_node_after) AS top_node_changed,
       fl.nodes_before,
       fl.nodes_after,
       CAST(COALESCE(sd.calls_before, 0) AS bigint) AS calls_before,
       CAST(COALESCE(sd.ms_before, 0) AS bigint)    AS ms_before,
       CAST(COALESCE(sd.calls_after, 0) AS bigint)  AS calls_after,
       CAST(COALESCE(sd.ms_after, 0) AS bigint)     AS ms_after,
       COUNT(*) OVER ()                             AS flipped_statements
FROM flipped AS fl
LEFT JOIN sides AS sd
  ON sd.query_id = fl.query_id
ORDER BY fl.query_id
LIMIT $4";

    /// <summary>
    /// Every statement captured under at least <c>$6</c> distinct <c>plan_hash</c> values in the window, with the most
    /// skewed of the predicate columns <c>pg_qualstats</c> recorded for it — the read behind
    /// <c>PG_PARAMETER_SENSITIVITY</c>. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the
    /// state lookback days (<see cref="PlanStateLookbackDays"/>), <c>$5</c> the row cap
    /// (<see cref="PlanFlipCandidateCount"/>), <c>$6</c> the hash floor
    /// (<see cref="PgTargetScorer.ParameterSensitivityMinHashes"/>), <c>$7</c> the skew bar
    /// (<see cref="PgTargetScorer.ParameterSensitivitySkewFrequency"/>) — both bars are the scorer's constants passed
    /// through, so the read and the grade cannot disagree about where the line is.
    ///
    /// <para><b>Predicates are joined to columns by NAME, per database.</b> <c>pg_predicate_stats</c> is per database
    /// (its OIDs mean nothing outside it — V98) and so is <c>pg_column_stats</c> (<c>pg_stats</c> describes the
    /// connected database — V91), so the join is on (<c>database_name</c>, <c>schema_name</c>, <c>table_name</c>,
    /// <c>column_name</c>), the latest column-stats row per column inside the lookback. A predicate whose column has
    /// no stats row (a table the monitoring role cannot read — V91's <c>has_column_privilege</c> filter) is a
    /// predicate with an unknown frequency, counted but never skewed.</para>
    ///
    /// <para><b>One row per statement, the skewed column's figures on it.</b> <c>predicate_columns</c> and
    /// <c>skewed_columns</c> are the statement's counts (window aggregates over the joined rows); the frequency,
    /// <c>n_distinct</c>, database and worst estimate error are the most-skewed column's. A statement with hashes
    /// and no predicate row at all returns with <c>predicate_columns = 0</c> and NULL figures, which the caller
    /// reads against the trackedness row: <c>pg_qualstats</c> absent, or installed and never sampled this statement
    /// (its default <c>sample_rate</c> is <c>1 / max_connections</c> — V98's own warning).</para>
    /// </summary>
    public const string PgTargetPlanSensitivitySql = @"
WITH hashes AS (
    SELECT query_id,
           COUNT(DISTINCT plan_hash) AS hash_count
    FROM pg_plan_capture
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
    AND   query_id <> 0
    AND   plan_hash IS NOT NULL
    GROUP BY query_id
    HAVING COUNT(DISTINCT plan_hash) >= $6
),
predicates AS (
    SELECT p.query_id,
           p.database_name,
           p.schema_name,
           p.table_name,
           p.column_name,
           MAX(p.worst_estimate_error_ratio) AS worst_estimate_error_ratio
    FROM pg_predicate_stats AS p
    JOIN hashes AS h
      ON h.query_id = p.query_id
    WHERE p.server_id = $1
    AND   p.collection_time <= $3
    AND   p.collection_time >= $2 - make_interval(days => $4)
    AND   p.column_name IS NOT NULL
    GROUP BY p.query_id, p.database_name, p.schema_name, p.table_name, p.column_name
),
columns AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name, column_name)
           database_name,
           schema_name,
           table_name,
           column_name,
           top_value_frequency,
           n_distinct
    FROM pg_column_stats
    WHERE server_id = $1
    AND   collection_time <= $3
    AND   collection_time >= $2 - make_interval(days => $4)
    AND   column_name IS NOT NULL
    ORDER BY database_name, schema_name, table_name, column_name, collection_time DESC
),
joined AS (
    SELECT pr.query_id,
           pr.database_name,
           pr.worst_estimate_error_ratio,
           c.top_value_frequency,
           c.n_distinct
    FROM predicates AS pr
    LEFT JOIN columns AS c
      ON c.database_name = pr.database_name
     AND c.schema_name   = pr.schema_name
     AND c.table_name    = pr.table_name
     AND c.column_name   = pr.column_name
),
picked AS (
    SELECT DISTINCT ON (h.query_id)
           h.query_id,
           h.hash_count,
           COUNT(j.query_id) OVER (PARTITION BY h.query_id)                                            AS predicate_columns,
           COUNT(j.query_id) FILTER (WHERE j.top_value_frequency >= $7) OVER (PARTITION BY h.query_id) AS skewed_columns,
           j.database_name,
           j.top_value_frequency,
           j.n_distinct,
           j.worst_estimate_error_ratio
    FROM hashes AS h
    LEFT JOIN joined AS j
      ON j.query_id = h.query_id
    ORDER BY h.query_id, j.top_value_frequency DESC NULLS LAST
)
SELECT query_id,
       hash_count,
       predicate_columns,
       skewed_columns,
       database_name,
       top_value_frequency,
       n_distinct,
       worst_estimate_error_ratio
FROM picked
ORDER BY (top_value_frequency IS NULL), top_value_frequency DESC, hash_count DESC, query_id
LIMIT $5";

    /// <summary>
    /// <c>PG_PLAN_REGRESSION</c> from the window's <c>pg_statement_stats</c> (the STORED per-statement deltas — copy of
    /// <c>PgTargetFactCollector.Queries.cs</c>'s discipline, never a re-differencing) split at the moment
    /// <c>pg_plan_capture</c> first saw the statement's last <c>plan_hash</c>; <c>PG_PARAMETER_SENSITIVITY</c> from
    /// <c>plan_hash</c> variance per <c>query_id</c> beside <c>pg_column_stats</c>' <c>top_value_frequency</c> on the
    /// predicate columns <c>pg_predicate_stats</c> names; and (lane 30) <c>PG_SEQ_SCAN_ADVISORY</c> from <c>plan_json</c>
    /// Seq Scan nodes beside <c>pg_predicate_stats</c> and <c>pg_table_bloat_stats</c>. The statement travels through
    /// the <c>ObjectName</c> seam as its <c>query_id</c> on the first two (<c>Fact.Metadata</c> is doubles-only and a
    /// 64-bit id is exact in one only to 2^53 — lane 7's reason); the Seq-Scan fact's <c>ObjectName</c> is the
    /// RELATION (with the predicate columns, when known) and its statement ids ride as two 32-bit halves per pair.
    /// The Seq-Scan fact carries evidence first — rate, shares, selectivity, size — and the advice names an index
    /// only where <c>pg_qualstats</c> named the columns and every gate holds (the maintainer's 2026-09-20 ruling).
    ///
    /// <para><b>The Seq-Scan read walks plans in C#, not in SQL.</b> <c>plan_json</c> is <c>text</c>, and a
    /// <c>::jsonb</c> cast in the query would fail the whole read on one malformed row; <see cref="WalkSeqScans"/>
    /// parses each plan defensively with <c>System.Text.Json</c> — a plan that does not parse, or a node without the
    /// fields, is skipped, never guessed — and the nodes are aggregated per (statement, relation) in memory before ONE
    /// witness read fetches the predicate and the size for every pair. The pairs are ranked by
    /// <c>captures_per_hour × rows_removed_share</c> (the qualstats selectivity standing in for the share when the
    /// plans carried no <c>Actual Rows</c>) and the top <see cref="PgTargetScorer.SeqScanCarriedPairs"/> ride the
    /// fact; the rate is over OBSERVED hours (#3538 A7).</para>
    ///
    /// <para><b>Silence, and the two unavailable shapes.</b> No Seq Scan node in the window is "not slow enough to
    /// log", never "no sequential scans" — nothing is emitted, unless the readiness collector says the library is
    /// NOT loaded, when the fact is <c>unavailable</c> with <c>reason_auto_explain_off</c> beside the regression's
    /// same-reason fact (two keys, two questions, two occurrence histories — <c>get_analysis_facts</c> filtered by
    /// either must answer). Nodes found but <c>pg_qualstats</c> KNOWN absent is <c>unavailable</c> with
    /// <c>reason_pg_qualstats_absent</c> AND the plan-side evidence (what was seen is said; what it means is not
    /// graded). Nodes found, extension installed or unknown, no predicate row for the pair: the fact is emitted with
    /// the selectivity ABSENT, the gate fails on the unknown, the scorer grades 0 and the advice says why.</para>
    ///
    /// <para><b>One fact per key, the worst of the window.</b> Both keys are static (the reconciler, the occurrence
    /// history and the graph key on them), so a pass emits the ONE regression that cleared the scorer's bars by the
    /// largest ratio — the bars are <see cref="PgTargetScorer"/>'s constants, referenced here for the pick and applied
    /// again there for the grade, so the collector and the scorer cannot disagree — and, when none cleared them, the
    /// largest-ratio flip that had enough calls on both sides (the scorer grades it 0 and the fact still shows the
    /// flip). The count of flipped statements rides along. Same for sensitivity: the most-skewed statement with a
    /// skewed column; when statements flipped three ways or more but no predicate could be read, ONE
    /// <c>unavailable</c> fact with the reason (<c>pg_qualstats</c> absent, or installed and never sampled).</para>
    ///
    /// <para><b>Absence of a capture is "not slow enough to log", never "no plan".</b> <c>auto_explain</c> writes only
    /// executions over <c>auto_explain.log_min_duration</c>, so a window with no captured plan says nothing about plans
    /// and this method emits nothing — except when the readiness collector says the library is NOT loaded, when the
    /// honest fact is <c>PG_PLAN_REGRESSION</c> in its <c>unavailable</c> shape with <c>reason_auto_explain_off</c>
    /// (the write family's shape for a counter the engine does not report). Unknown readiness (no row in the
    /// lookback) is silence, not "off".</para>
    ///
    /// <para><b>No fact without an observed window.</b> <c>ObservedDurationMs</c> of 0 means the coverage witness
    /// stamped nothing (#3538 A2); the pass reports <c>unavailable</c> anyway and this method contributes nothing.</para>
    ///
    /// <para>/* filled by lane 27 (lane 30 adds the Seq-Scan read in the same partial — the marked region at the end
    /// of this method). Every command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, <c>$N</c> positional, no bare <c>now()</c> / <c>CURRENT_TIMESTAMP</c>
    /// (StoreSqlClockDisciplineTests), the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and no rate is taken
    /// here at all — the family's quantities are per-call means and counts, which need no window divisor. The six
    /// tables are CollectorCatalog targets already, so the FROM/JOIN census admits them (pinned by name in
    /// PgTargetFactCollectorTests). */</para>
    /// </summary>
    private async partial Task CollectPlanFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0)
            return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var (autoExplainLoaded, qualstatsState) = await ReadPlanTrackednessAsync(context, connection);
            var flips = await ReadPlanFlipsAsync(context, connection);

            /* The regression: the worst flip that cleared the bars, else the worst flip with enough calls to be
               compared (graded 0, shown), else — when the library is KNOWN off — the unavailable shape. */
            var regression = PickRegression(flips);
            if (regression is { } pick)
            {
                facts.Add(RegressionFact(context, pick, flips.Count == 0 ? 0 : flips[0].FlippedStatements));
            }
            else if (flips.Count == 0 && autoExplainLoaded == false)
            {
                facts.Add(new Fact
                {
                    Source = PgTargetSources.PlansSource,
                    Key = PgTargetFactKeys.PlanRegression,
                    Value = 0,
                    ServerId = context.ServerId,
                    Metadata =
                    {
                        [PgTargetScorer.PlanUnavailableKey] = 1,
                        [PgTargetScorer.PlanReasonAutoExplainOffKey] = 1,
                        /* 1, not 0: no bar was chosen for this fact — nothing is graded off the unavailable shape — so
                           there is no unmeasured number for the flag to disclose (the write family's rule). */
                        ["threshold_lineage"] = 1,
                    },
                });
            }

            /* The sensitivity: read only when something flipped — a statement with one plan has no variance to explain. */
            if (flips.Count > 0)
            {
                var sensitivity = await ReadPlanSensitivityAsync(context, connection);
                var qualstatsAbsent = qualstatsState is "absent" or "available";
                if (PickSensitivity(sensitivity, qualstatsAbsent, context.ServerId) is { } fact)
                    facts.Add(fact);
            }

            /* lane 30: Seq-Scan read — PG_SEQ_SCAN_ADVISORY from plan_json Seq Scan nodes beside pg_predicate_stats and
               pg_table_bloat_stats, on this connection, after the two facts above. Evidence first; the advice names an
               index only where the predicate columns are pg_qualstats' own and every gate holds. */
            var captures = await ReadSeqScanCapturesAsync(context, connection);
            var nodes = AggregateSeqScans(captures);
            if (nodes.Count == 0)
            {
                if (autoExplainLoaded == false)
                    facts.Add(SeqScanUnavailableFact(context));
            }
            else
            {
                var witnesses = await ReadSeqScanWitnessesAsync(context, connection, nodes);
                var qualstatsAbsent = qualstatsState is "absent" or "available";
                facts.Add(SeqScanFact(context, PickSeqScans(nodes, witnesses, context.ObservedDurationMs / 3_600_000.0), qualstatsAbsent));
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_plan_capture arrived in V99, pg_predicate_stats in V98, pg_column_stats in V91, the readiness table in
               V87; a store without any of them raises 42P01 here, which the reporter classifies quiet. Degrades to "no
               facts" so one unavailable input cannot cost this server its other facts, and WHY is reported, not
               assumed (#2826). An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>One captured plan the Seq-Scan prefilter admitted, as <see cref="PgTargetSeqScanCapturesSql"/> returns it.</summary>
    internal sealed record SeqScanCaptureRow(long QueryId, string PlanJson);

    /// <summary>One Seq Scan node with a Filter, as <see cref="WalkSeqScans"/> reads it off a plan: the relation, the
    /// planner's estimate and — when <c>auto_explain.log_analyze</c> was on — the rows kept and the rows the filter
    /// removed. Null where the plan carried no such field; never 0.</summary>
    internal sealed record SeqScanNode(string RelationName, double? PlanRows, double? ActualRows, double? RowsRemovedByFilter);

    /// <summary>One (statement, relation) pair aggregated over the window's captures: how many plans held the node,
    /// and the summed kept / removed rows over the captures that carried them (the share is
    /// <c>removed / (kept + removed)</c>, null when none did).</summary>
    internal sealed record SeqScanAggregate(long QueryId, string RelationName, int Captures, double? PlanRows, double KeptRows, double RemovedRows, int AnalyzedCaptures)
    {
        public double? RowsRemovedShare => AnalyzedCaptures > 0 && KeptRows + RemovedRows > 0 ? RemovedRows / (KeptRows + RemovedRows) : null;
    }

    /// <summary>The two witnesses for one pair, as <see cref="PgTargetSeqScanWitnessSql"/> returns them. Every column
    /// but the pair's identity is null when its source had no row.</summary>
    internal sealed record SeqScanWitnessRow(
        long QueryId, string TableName, string? DatabaseName, string? SchemaName, double? Selectivity, long? RowsEvaluated, long? RowsFiltered,
        double? SampleRate, double? WorstEstimateErrorRatio, string? PredicateColumns, long? HeapBytes, long? RelationsNamed);

    /// <summary>One ranked pair, ready for the fact: the aggregate, its witnesses, and the rate over observed hours.</summary>
    internal sealed record SeqScanPair(SeqScanAggregate Scan, SeqScanWitnessRow? Witness, double CapturesPerHour)
    {
        /// <summary>The rank key: scans per hour × the share of rows the filter removed — the plan's own share where
        /// the captures carried Actual Rows, else the qualstats selectivity (the same quantity, measured by the other
        /// instrument), else 0 (a pair with neither ranks last, and is still shown).</summary>
        public double RankScore => CapturesPerHour * (Scan.RowsRemovedShare ?? Witness?.Selectivity ?? 0);

        /// <summary>The relation as the fact names it: <c>schema.table</c> when the predicate witness knows the schema.</summary>
        public string QualifiedRelation => string.IsNullOrEmpty(Witness?.SchemaName) ? Scan.RelationName : Witness!.SchemaName + "." + Scan.RelationName;
    }

    /// <summary>
    /// Every Seq Scan node with a <c>Relation Name</c> and a <c>Filter</c> in one redacted <c>auto_explain</c> plan.
    /// The root is <c>{"Plan": {…}}</c> (the parser drops <c>Query Text</c> and keeps the tree), children nest under
    /// <c>"Plans"</c> arrays at any depth (InitPlans, SubPlans and CTE scans included — they are just nodes); an
    /// <c>EXPLAIN (FORMAT JSON)</c> array root is walked too. A plan that does not parse, a node that is not an object,
    /// a Seq Scan without a relation name or without a Filter (reading the whole relation IS the plan's intent — no
    /// predicate, nothing to index) is skipped, never guessed. Numbers are read only when the field is a JSON number.
    /// Pure, so the walk is testable on arranged JSON.
    /// </summary>
    internal static List<SeqScanNode> WalkSeqScans(string planJson)
    {
        var nodes = new List<SeqScanNode>();
        try
        {
            using var document = JsonDocument.Parse(planJson);
            var root = document.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in root.EnumerateArray())
                    WalkRoot(element, nodes);
            }
            else
            {
                WalkRoot(root, nodes);
            }
        }
        catch (JsonException)
        {
            /* Not a plan this walk can read: the redactor stored what auto_explain wrote, and a truncated log line or a
               foreign row is a plan with no nodes, not an error worth the family's silence. */
        }

        return nodes;

        static void WalkRoot(JsonElement element, List<SeqScanNode> nodes)
        {
            if (element.ValueKind != JsonValueKind.Object)
                return;
            if (element.TryGetProperty("Plan", out var plan))
                WalkNode(plan, nodes);
            else if (element.TryGetProperty("Node Type", out _))
                WalkNode(element, nodes);
        }

        static void WalkNode(JsonElement node, List<SeqScanNode> nodes)
        {
            if (node.ValueKind != JsonValueKind.Object)
                return;

            if (node.TryGetProperty("Node Type", out var type) && type.ValueKind == JsonValueKind.String
                && string.Equals(type.GetString(), "Seq Scan", StringComparison.Ordinal)
                && node.TryGetProperty("Relation Name", out var relation) && relation.ValueKind == JsonValueKind.String
                && !string.IsNullOrEmpty(relation.GetString())
                && node.TryGetProperty("Filter", out var filter) && filter.ValueKind == JsonValueKind.String)
            {
                nodes.Add(new SeqScanNode(relation.GetString()!, Number(node, "Plan Rows"), Number(node, "Actual Rows"), Number(node, "Rows Removed by Filter")));
            }

            if (node.TryGetProperty("Plans", out var children) && children.ValueKind == JsonValueKind.Array)
            {
                foreach (var child in children.EnumerateArray())
                    WalkNode(child, nodes);
            }
        }

        static double? Number(JsonElement node, string name) =>
            node.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var d) ? d : null;
    }

    /// <summary>The window's captures collapsed to (statement, relation) pairs: one capture counts once per relation
    /// it scans sequentially (a plan scanning the same relation at two nodes is one capture of that pair, its rows
    /// summed), kept / removed rows summed over the captures that carried Actual Rows. Pure.</summary>
    internal static List<SeqScanAggregate> AggregateSeqScans(IReadOnlyList<SeqScanCaptureRow> captures)
    {
        var byPair = new Dictionary<(long, string), (int Captures, double? PlanRows, double Kept, double Removed, int Analyzed)>();
        foreach (var capture in captures)
        {
            /* One capture, one relation, one entry: the nodes scanning the same relation in one plan are summed here,
               then the capture counts once for the pair. */
            var perRelation = new Dictionary<string, (double? PlanRows, double Kept, double Removed, bool Analyzed)>(StringComparer.Ordinal);
            foreach (var node in WalkSeqScans(capture.PlanJson))
            {
                perRelation.TryGetValue(node.RelationName, out var soFar);
                var analyzed = node.ActualRows is { } kept && node.RowsRemovedByFilter is { } removed;
                perRelation[node.RelationName] = (
                    node.PlanRows is { } planRows ? Math.Max(soFar.PlanRows ?? 0, planRows) : soFar.PlanRows,
                    soFar.Kept + (analyzed ? node.ActualRows!.Value : 0),
                    soFar.Removed + (analyzed ? node.RowsRemovedByFilter!.Value : 0),
                    soFar.Analyzed || analyzed);
            }

            foreach (var (relation, rows) in perRelation)
            {
                byPair.TryGetValue((capture.QueryId, relation), out var agg);
                byPair[(capture.QueryId, relation)] = (
                    agg.Captures + 1,
                    rows.PlanRows is { } planRows ? Math.Max(agg.PlanRows ?? 0, planRows) : agg.PlanRows,
                    agg.Kept + rows.Kept,
                    agg.Removed + rows.Removed,
                    agg.Analyzed + (rows.Analyzed ? 1 : 0));
            }
        }

        return byPair
            .Select(kv => new SeqScanAggregate(kv.Key.Item1, kv.Key.Item2, kv.Value.Captures, kv.Value.PlanRows, kv.Value.Kept, kv.Value.Removed, kv.Value.Analyzed))
            .OrderBy(a => a.QueryId).ThenBy(a => a.RelationName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>The pairs ranked by <see cref="SeqScanPair.RankScore"/> (ties: more captures, then the smaller id, then
    /// the name — a stable order for the fact), each with its witness row when one exists, the rate over
    /// <paramref name="observedHours"/>. Pure; the caller takes the top <see cref="PgTargetScorer.SeqScanCarriedPairs"/>.</summary>
    internal static List<SeqScanPair> PickSeqScans(IReadOnlyList<SeqScanAggregate> scans, IReadOnlyList<SeqScanWitnessRow> witnesses, double observedHours)
    {
        var byPair = witnesses.ToDictionary(w => (w.QueryId, w.TableName), w => w);
        return scans
            .Select(s => new SeqScanPair(s, byPair.GetValueOrDefault((s.QueryId, s.RelationName)), observedHours > 0 ? s.Captures / observedHours : 0))
            .OrderByDescending(p => p.RankScore)
            .ThenByDescending(p => p.Scan.Captures)
            .ThenBy(p => p.Scan.QueryId)
            .ThenBy(p => p.Scan.RelationName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>The <c>PG_SEQ_SCAN_ADVISORY</c> fact for the ranked pairs: <see cref="Fact.Value"/> is the top pair's
    /// rank score, <see cref="Fact.ObjectName"/> the top pair's relation with its predicate columns when known
    /// (<c>schema.table (col1, col2)</c> — the ONE string seam; the advice splits it), <see cref="Fact.DatabaseName"/>
    /// the predicate witness's database; per carried pair the id halves, captures, rate, share, size, selectivity and
    /// their companions under a <c>_N</c> suffix, absent where the source had no row. With <c>pg_qualstats</c> KNOWN
    /// absent the fact is <c>unavailable</c> with the reason and still carries the plan-side evidence.</summary>
    internal static Fact SeqScanFact(AnalysisContext context, IReadOnlyList<SeqScanPair> ranked, bool qualstatsAbsent)
    {
        var top = ranked[0];
        var carried = Math.Min(ranked.Count, PgTargetScorer.SeqScanCarriedPairs);
        var fact = new Fact
        {
            Source = PgTargetSources.PlansSource,
            Key = PgTargetFactKeys.SeqScanAdvisory,
            Value = top.RankScore,
            ServerId = context.ServerId,
            ObjectName = string.IsNullOrEmpty(top.Witness?.PredicateColumns) ? top.QualifiedRelation : top.QualifiedRelation + " (" + top.Witness!.PredicateColumns + ")",
            DatabaseName = top.Witness?.DatabaseName,
            Metadata =
            {
                [PgTargetScorer.SeqScanPairsKey] = carried,
                [PgTargetScorer.SeqScanCandidatePairsKey] = ranked.Count,
                ["observed_hours"] = context.ObservedDurationMs / 3_600_000.0,
            },
        };

        for (var rank = 1; rank <= carried; rank++)
        {
            var pair = ranked[rank - 1];
            var (hi, lo) = PgTargetScorer.SeqScanSplitQueryId(pair.Scan.QueryId);
            Set(PgTargetScorer.SeqScanQueryIdHiKey, hi);
            Set(PgTargetScorer.SeqScanQueryIdLoKey, lo);
            Set(PgTargetScorer.SeqScanCapturesKey, pair.Scan.Captures);
            Set(PgTargetScorer.SeqScanCapturesPerHourKey, pair.CapturesPerHour);
            Set("analyzed_captures", pair.Scan.AnalyzedCaptures);
            /* Absent rather than zero, throughout: a plan without Actual Rows did not remove 0 rows, a relation the bloat
               collector never sampled is not 0 bytes, a predicate pg_qualstats never saw is not 0 % selective. */
            if (pair.Scan.PlanRows is { } planRows) Set("plan_rows", planRows);
            if (pair.Scan.RowsRemovedShare is { } share)
            {
                Set(PgTargetScorer.SeqScanRowsRemovedShareKey, share);
                Set("actual_rows", pair.Scan.KeptRows);
                Set("rows_removed_by_filter", pair.Scan.RemovedRows);
            }
            if (pair.Witness is { } w)
            {
                if (w.HeapBytes is { } heap) Set(PgTargetScorer.SeqScanHeapBytesKey, heap);
                if (w.RelationsNamed is { } named) Set("relations_named", named);
                if (!qualstatsAbsent && w.Selectivity is { } selectivity)
                {
                    Set(PgTargetScorer.SeqScanSelectivityKey, selectivity);
                    if (w.RowsEvaluated is { } evaluated) Set("rows_evaluated", evaluated);
                    if (w.RowsFiltered is { } filtered) Set("rows_filtered", filtered);
                    if (w.SampleRate is { } rate) Set("sample_rate", rate);
                    if (w.WorstEstimateErrorRatio is { } error) Set(PgTargetScorer.SeqScanEstimateErrorKey, error);
                }
            }

            void Set(string key, double value) => fact.Metadata[PgTargetScorer.SeqScanPairKey(key, rank)] = value;
        }

        if (qualstatsAbsent)
        {
            fact.Value = 0;
            fact.Metadata[PgTargetScorer.PlanUnavailableKey] = 1;
            fact.Metadata[PgTargetScorer.PlanReasonQualstatsAbsentKey] = 1;
            /* 1, not 0: nothing is graded off the unavailable shape (the write family's rule). */
            fact.Metadata["threshold_lineage"] = 1;
        }

        return fact;
    }

    /// <summary>The Seq-Scan fact's <c>unavailable</c> shape when the library is KNOWN off and no node was found — the
    /// regression's twin, on this key.</summary>
    private static Fact SeqScanUnavailableFact(AnalysisContext context) => new()
    {
        Source = PgTargetSources.PlansSource,
        Key = PgTargetFactKeys.SeqScanAdvisory,
        Value = 0,
        ServerId = context.ServerId,
        Metadata =
        {
            [PgTargetScorer.PlanUnavailableKey] = 1,
            [PgTargetScorer.PlanReasonAutoExplainOffKey] = 1,
            /* 1, not 0: nothing is graded off the unavailable shape (the write family's rule). */
            ["threshold_lineage"] = 1,
        },
    };

    /// <summary>One flipped statement, as <see cref="PgTargetPlanFlipSql"/> returns it.</summary>
    internal sealed record PlanFlipRow(
        long QueryId, long HashCount, long? HashBefore, long? HashAfter, DateTime FlipTime, DateTime FirstFlipTime, bool TopNodeChanged,
        long? NodesBefore, long? NodesAfter, long CallsBefore, long MsBefore, long CallsAfter, long MsAfter, long FlippedStatements)
    {
        /// <summary>The per-call means, absent (null) over no calls — a mean over no calls is not 0 ms.</summary>
        public double? MeanBefore => CallsBefore > 0 ? (double)MsBefore / CallsBefore : null;
        public double? MeanAfter => CallsAfter > 0 ? (double)MsAfter / CallsAfter : null;
        public double? Ratio => MeanBefore is { } b && b > 0 && MeanAfter is { } a ? a / b : null;
    }

    /// <summary>One multi-plan statement, as <see cref="PgTargetPlanSensitivitySql"/> returns it.</summary>
    internal sealed record PlanSensitivityRow(
        long QueryId, long HashCount, long PredicateColumns, long SkewedColumns, string? DatabaseName,
        double? TopValueFrequency, double? NDistinct, double? WorstEstimateErrorRatio);

    /// <summary>
    /// The regression to emit, or null: the largest-ratio flip that clears every scorer bar (both call floors, the
    /// absolute delta, the concerning ratio) — else the largest-ratio flip with enough calls on both sides, which the
    /// scorer grades 0 and the fact still shows — else nothing. The bars are <see cref="PgTargetScorer"/>'s, referenced.
    /// Pure, so the pick is testable without a store.
    /// </summary>
    internal static PlanFlipRow? PickRegression(IReadOnlyList<PlanFlipRow> flips)
    {
        PlanFlipRow? cleared = null;
        PlanFlipRow? comparable = null;
        foreach (var flip in flips)
        {
            if (flip.CallsBefore < PgTargetScorer.PlanRegressionMinCallsPerSide || flip.CallsAfter < PgTargetScorer.PlanRegressionMinCallsPerSide)
                continue;
            if (flip.Ratio is not { } ratio || flip.MeanBefore is not { } before || flip.MeanAfter is not { } after)
                continue;

            if (comparable is null || ratio > comparable.Ratio)
                comparable = flip;

            if (after - before >= PgTargetScorer.PlanRegressionMinDeltaMs && ratio >= PgTargetScorer.PlanRegressionRatioConcerning
                && (cleared is null || ratio > cleared.Ratio))
                cleared = flip;
        }

        return cleared ?? comparable;
    }

    /// <summary>
    /// The sensitivity fact to emit, or null: the most-skewed statement whose skewed column clears the bar — the rows
    /// arrive most-skewed first, so the first row with a skewed column is it — else, when statements flipped but NO
    /// predicate row exists for any of them, one <c>unavailable</c> fact on the most-flipped statement with the reason
    /// (<paramref name="qualstatsAbsent"/> → the extension is not installed; otherwise installed and never sampled) —
    /// else nothing (predicates were read and none is skewed: variance without the mechanism's evidence is not a card).
    /// Pure.
    /// </summary>
    internal static Fact? PickSensitivity(IReadOnlyList<PlanSensitivityRow> rows, bool qualstatsAbsent, int serverId)
    {
        if (rows.Count == 0)
            return null;

        PlanSensitivityRow? mostFlipped = null;
        var anyPredicate = false;
        foreach (var row in rows)
        {
            anyPredicate |= row.PredicateColumns > 0;
            if (mostFlipped is null || row.HashCount > mostFlipped.HashCount)
                mostFlipped = row;

            if (row.SkewedColumns > 0 && row.TopValueFrequency is { } frequency && frequency >= PgTargetScorer.ParameterSensitivitySkewFrequency)
            {
                var fact = new Fact
                {
                    Source = PgTargetSources.PlansSource,
                    Key = PgTargetFactKeys.ParameterSensitivity,
                    Value = frequency,
                    ServerId = serverId,
                    ObjectName = row.QueryId.ToString(CultureInfo.InvariantCulture),
                    DatabaseName = row.DatabaseName,
                    Metadata =
                    {
                        [PgTargetScorer.PlanHashCountKey] = row.HashCount,
                        [PgTargetScorer.PlanTopValueFrequencyKey] = frequency,
                        ["predicate_columns"] = row.PredicateColumns,
                        ["skewed_columns"] = row.SkewedColumns,
                        ["sensitive_statements"] = rows.Count,
                    },
                };
                if (row.NDistinct is { } n) fact.Metadata["n_distinct"] = n;
                if (row.WorstEstimateErrorRatio is { } e) fact.Metadata["worst_estimate_error_ratio"] = e;
                return fact;
            }
        }

        if (anyPredicate)
            return null;

        return new Fact
        {
            Source = PgTargetSources.PlansSource,
            Key = PgTargetFactKeys.ParameterSensitivity,
            Value = 0,
            ServerId = serverId,
            ObjectName = mostFlipped!.QueryId.ToString(CultureInfo.InvariantCulture),
            Metadata =
            {
                [PgTargetScorer.PlanHashCountKey] = mostFlipped.HashCount,
                [PgTargetScorer.PlanUnavailableKey] = 1,
                [qualstatsAbsent ? PgTargetScorer.PlanReasonQualstatsAbsentKey : PgTargetScorer.PlanReasonNoPredicateSampledKey] = 1,
                ["sensitive_statements"] = rows.Count,
                /* 1, not 0: nothing is graded off the unavailable shape (the write family's rule). */
                ["threshold_lineage"] = 1,
            },
        };
    }

    /// <summary>The <c>PG_PLAN_REGRESSION</c> fact for a picked flip. <see cref="Fact.Value"/> is the after ÷ before
    /// ratio (the decision variable); the metadata carries both means, both call counts, the hash count and the two
    /// 48-bit hash prefixes, the node-count move and whether the top node changed, the ages of the first and the
    /// last plan change against the window's end, and how many statements flipped. <c>query_id</c> rides on <see cref="Fact.ObjectName"/>;
    /// <see cref="Fact.DatabaseName"/> stays null (the statement rolls up across databases, lane 7's grain).</summary>
    private static Fact RegressionFact(AnalysisContext context, PlanFlipRow flip, long flippedStatements)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.PlansSource,
            Key = PgTargetFactKeys.PlanRegression,
            Value = flip.Ratio ?? 0,
            ServerId = context.ServerId,
            ObjectName = flip.QueryId.ToString(CultureInfo.InvariantCulture),
            Metadata =
            {
                [PgTargetScorer.PlanMeanMsBeforeKey] = flip.MeanBefore ?? 0,
                [PgTargetScorer.PlanMeanMsAfterKey] = flip.MeanAfter ?? 0,
                ["mean_ms_delta"] = (flip.MeanAfter ?? 0) - (flip.MeanBefore ?? 0),
                [PgTargetScorer.PlanCallsBeforeKey] = flip.CallsBefore,
                [PgTargetScorer.PlanCallsAfterKey] = flip.CallsAfter,
                [PgTargetScorer.PlanHashCountKey] = flip.HashCount,
                ["top_node_changed"] = flip.TopNodeChanged ? 1 : 0,
                ["flip_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(flip.FlipTime)).TotalSeconds),
                ["first_flip_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(flip.FirstFlipTime)).TotalSeconds),
                ["flipped_statements"] = flippedStatements,
            },
        };

        /* Absent rather than zero: a hash the cast could not read is not hash 0; a node count the capture did not
           carry is not 0 nodes. */
        if (flip.HashBefore is { } hb) fact.Metadata["plan_hash_before_prefix"] = hb;
        if (flip.HashAfter is { } ha) fact.Metadata["plan_hash_after_prefix"] = ha;
        if (flip.NodesBefore is { } nb && flip.NodesAfter is { } na)
        {
            fact.Metadata["nodes_before"] = nb;
            fact.Metadata["nodes_after"] = na;
            fact.Metadata["node_count_delta"] = na - nb;
        }

        return fact;
    }

    private async Task<(bool? AutoExplainLoaded, string? QualstatsState)> ReadPlanTrackednessAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetPlanTrackednessSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanStateLookbackDays);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return (null, null);
        return (
            reader.IsDBNull(0) ? null : reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private async Task<List<PlanFlipRow>> ReadPlanFlipsAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetPlanFlipSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanFlipCandidateCount);

        var rows = new List<PlanFlipRow>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new PlanFlipRow(
                QueryId: ToInt64(reader.GetValue(0)),
                HashCount: ToInt64(reader.GetValue(1)),
                HashBefore: reader.IsDBNull(2) ? null : ToInt64(reader.GetValue(2)),
                HashAfter: reader.IsDBNull(3) ? null : ToInt64(reader.GetValue(3)),
                FlipTime: reader.GetDateTime(4),
                FirstFlipTime: reader.GetDateTime(5),
                TopNodeChanged: !reader.IsDBNull(6) && reader.GetBoolean(6),
                NodesBefore: reader.IsDBNull(7) ? null : ToInt64(reader.GetValue(7)),
                NodesAfter: reader.IsDBNull(8) ? null : ToInt64(reader.GetValue(8)),
                CallsBefore: ToInt64(reader.GetValue(9)),
                MsBefore: ToInt64(reader.GetValue(10)),
                CallsAfter: ToInt64(reader.GetValue(11)),
                MsAfter: ToInt64(reader.GetValue(12)),
                FlippedStatements: ToInt64(reader.GetValue(13))));
        }

        return rows;
    }

    private async Task<List<PlanSensitivityRow>> ReadPlanSensitivityAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetPlanSensitivitySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanStateLookbackDays);
        cmd.Parameters.AddWithValue(PlanFlipCandidateCount);
        cmd.Parameters.AddWithValue((long)PgTargetScorer.ParameterSensitivityMinHashes);
        cmd.Parameters.AddWithValue(PgTargetScorer.ParameterSensitivitySkewFrequency);

        var rows = new List<PlanSensitivityRow>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new PlanSensitivityRow(
                QueryId: ToInt64(reader.GetValue(0)),
                HashCount: ToInt64(reader.GetValue(1)),
                PredicateColumns: reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                SkewedColumns: reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                DatabaseName: reader.IsDBNull(4) ? null : reader.GetString(4),
                TopValueFrequency: reader.IsDBNull(5) ? null : Convert.ToDouble(reader.GetValue(5)),
                NDistinct: reader.IsDBNull(6) ? null : Convert.ToDouble(reader.GetValue(6)),
                WorstEstimateErrorRatio: reader.IsDBNull(7) ? null : Convert.ToDouble(reader.GetValue(7))));
        }

        return rows;
    }

    private async Task<List<SeqScanCaptureRow>> ReadSeqScanCapturesAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetSeqScanCapturesSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(SeqScanCaptureRowCap);

        var rows = new List<SeqScanCaptureRow>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
            rows.Add(new SeqScanCaptureRow(ToInt64(reader.GetValue(0)), reader.GetString(1)));

        return rows;
    }

    private async Task<List<SeqScanWitnessRow>> ReadSeqScanWitnessesAsync(AnalysisContext context, NpgsqlConnection connection, IReadOnlyList<SeqScanAggregate> scans)
    {
        using var cmd = new NpgsqlCommand(PgTargetSeqScanWitnessSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanStateLookbackDays);
        cmd.Parameters.AddWithValue(scans.Select(s => s.QueryId).ToArray());
        cmd.Parameters.AddWithValue(scans.Select(s => s.RelationName).ToArray());

        var rows = new List<SeqScanWitnessRow>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new SeqScanWitnessRow(
                QueryId: ToInt64(reader.GetValue(0)),
                TableName: reader.GetString(1),
                DatabaseName: reader.IsDBNull(2) ? null : reader.GetString(2),
                SchemaName: reader.IsDBNull(3) ? null : reader.GetString(3),
                Selectivity: reader.IsDBNull(4) ? null : Convert.ToDouble(reader.GetValue(4)),
                RowsEvaluated: reader.IsDBNull(5) ? null : ToInt64(reader.GetValue(5)),
                RowsFiltered: reader.IsDBNull(6) ? null : ToInt64(reader.GetValue(6)),
                SampleRate: reader.IsDBNull(7) ? null : Convert.ToDouble(reader.GetValue(7)),
                WorstEstimateErrorRatio: reader.IsDBNull(8) ? null : Convert.ToDouble(reader.GetValue(8)),
                PredicateColumns: reader.IsDBNull(9) ? null : reader.GetString(9),
                HeapBytes: reader.IsDBNull(10) ? null : ToInt64(reader.GetValue(10)),
                RelationsNamed: reader.IsDBNull(11) ? null : ToInt64(reader.GetValue(11))));
        }

        return rows;
    }
}
