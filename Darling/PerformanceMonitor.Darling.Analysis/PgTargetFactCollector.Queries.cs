/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// How many statements the read returns, heaviest first — a PAGE SIZE, not a threshold: it bounds how
    /// many <c>PG_BAD_ACTOR_*</c> facts one pass can emit, and the share bar in <c>PgTargetScorer.Queries.cs</c>
    /// decides which of them fire. Five is the SQL Server family's cut (<see cref="PgFactCollector.BadActorSql"/>,
    /// <c>LIMIT 5</c>) kept as a shape, because the reason for it is engine-independent: a story needs ONE
    /// query-shaped leaf, and a handful of candidates is what the greedy traversal can choose among without
    /// the finding list turning into a top-queries report — which is <c>get_pg_top_queries</c>' job.
    /// </summary>
    internal const int TopStatementCount = 5;

    /// <summary>
    /// The top statements of the window from <c>pg_statement_stats</c> — the read behind
    /// <c>PG_BAD_ACTOR_&lt;queryid&gt;</c>. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC),
    /// <c>$4</c> the row cap (<see cref="TopStatementCount"/>), <c>$5</c> <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> (bound: a const string cannot interpolate an int).
    ///
    /// <para><b>The deltas are READ, not re-derived.</b> <c>delta_calls</c> and <c>delta_total_exec_time_ms</c>
    /// are written at collection, where the previous reading was in hand and the reset / eviction /
    /// first-sighting cases were classified by <c>CollectorDeltaCalculator</c>; differencing the cumulative
    /// columns again here would give a different answer whenever a stored snapshot is missing (the collector's
    /// delta spans the gap it observed; a LAG here would span the gap in the stored rows). This is the
    /// <c>DarlingPgTrendReader.QueryDurationTrendSql</c> / <c>DarlingPgStatementReader.PgTopQueriesSql</c>
    /// discipline, copied, not reinvented. The one column that keeps no stored delta and matters to this
    /// family — <c>temp_blks_written</c>, the per-statement half of lane 6's spill evidence — is differenced
    /// the way <c>PgTopQueriesSql</c> differences the block columns: <c>GREATEST(x − LAG(x), 0)</c> over the
    /// FULL series identity <c>(queryid, database_id, user_id, toplevel)</c>, so a reset interval drops and
    /// two databases' series never interleave.</para>
    ///
    /// <para><b><c>database_id</c> is the collector's key and stays so (#3540 A11c).</b> It is the
    /// <c>datid</c> OID <c>pg_stat_statements</c> reports, and an OID is reused after <c>DROP DATABASE</c> /
    /// <c>CREATE DATABASE</c> — a series can therefore splice two databases' histories under one id. That is
    /// a MEASUREMENT fact about the source, recorded there and in the delta seeder, and this read reproduces
    /// the key as written rather than "fixing" it: a different partition here would difference series the
    /// collector never differenced and disagree with every stored delta. The fact is rolled up to
    /// <c>queryid</c> alone — the grain the key, the text store and <c>pg_plan_capture</c> share — and
    /// carries <c>database_count</c> so a statement shape running against several databases says so.</para>
    ///
    /// <para><b>The rate's denominator is the seconds the collector's RUNS covered (#7566), the rule
    /// <c>QueryDurationTrendBucketedSql</c> uses.</b> Per snapshot the stored <c>sample_interval_seconds</c> (V128,
    /// #3540) is the span a delta accrued over, taken as <c>MAX</c> over the queryid's rows: 0 means every row was
    /// unknowable (a first sighting or a <c>pg_stat_statements_reset()</c>) and NULL is a pre-V128 row. But this
    /// collector skips idle rows at the write, so dividing a statement's calls by the seconds of only the snapshots it
    /// had a row in reads a statement that runs a batch every 5 minutes at 0.48 per second where its rate is 0.097.
    /// The denominator is therefore every SUCCESS run's seconds in the window (a run the statement had no row in is
    /// zero calls over its real seconds), less the runs rated out for this statement: an outage gap, and a run whose
    /// rows for the statement carry interval 0, which leave their calls out with their seconds. A snapshot no run
    /// claims (a store without the log) is rated on its own, stored interval else the <c>LAG</c> over the queryid's
    /// unclaimed snapshots, which is the old rule. The rate is NULL when no snapshot of the statement is rated, which
    /// the C# renders as no <c>calls_per_sec</c> at all. There is no <c>ELSE 0</c>.</para>
    ///
    /// <para><b>The denominator of the share is the WINDOW's, not the page's (#3541 A7 / #3613).</b>
    /// <c>SUM(SUM(total_exec_ms)) OVER ()</c> is a window aggregate over the grouped result, evaluated after
    /// <c>GROUP BY</c> / <c>HAVING</c> and before <c>ORDER BY</c> / <c>LIMIT</c>, so it is the execution time
    /// of every statement shape that ran, identical on every row; a share taken over the five rows returned
    /// would sum to 100% by construction. The share is the family's decision variable, so getting its
    /// denominator wrong would be getting the family wrong.</para>
    ///
    /// <para><b>Units.</b> <c>total_exec_time_ms</c> is milliseconds on every row: the collector's SELECT
    /// names <c>total_exec_time</c>, the PostgreSQL ≥ 13 column (<c>total_time</c> before 13 — see
    /// <c>PgStatementStatsCollector.BuildQueryText</c>), so a pre-13 target never populates this table and
    /// there is no unit branch to write here.</para>
    ///
    /// <para><b>No text here, by census.</b> <c>pg_statement_text</c> is dimension-shaped (V73: "not a
    /// hypertable and not a collector table"), and <c>PgTargetFactCollectorTests</c> pins every <c>FROM</c> /
    /// <c>JOIN</c> target of this collector to a collector table, the registry or a CTE — so the text and its
    /// <c>hashtext()</c> (the breadcrumb that survives the <c>queryid</c> re-key a major upgrade performs) are
    /// the drill-down's (<c>PgTargetDrillDownCollector.Queries.cs</c>), which has no such pin and is where
    /// the text is read for display anyway. The fact carries <c>queryid</c> in its key and its metadata.</para>
    /// </summary>
    public const string PgTargetTopStatementsSql = @"
WITH stmt_series AS (
    SELECT
        queryid,
        database_id,
        collection_time,
        delta_calls,
        delta_total_exec_time_ms,
        max_exec_time_ms,
        max_exec_peakmem_bytes,
        sample_interval_seconds,
        GREATEST(temp_blks_written - LAG(temp_blks_written) OVER identity, 0) AS d_temp_blks_written
    FROM pg_statement_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW identity AS (
        PARTITION BY queryid, database_id, user_id, toplevel
        ORDER BY collection_time
    )
),
per_snapshot AS (
    SELECT
        queryid,
        collection_time,
        SUM(delta_calls)              AS calls,
        SUM(delta_total_exec_time_ms) AS total_exec_ms,
        MAX(max_exec_time_ms)         AS max_exec_ms,
        MAX(max_exec_peakmem_bytes)   AS max_exec_peakmem_bytes,
        SUM(d_temp_blks_written)      AS temp_blks_written,
        MAX(sample_interval_seconds)  AS stored_interval
    FROM stmt_series
    GROUP BY queryid, collection_time
),
databases AS (
    SELECT queryid, COUNT(DISTINCT database_id) AS database_count
    FROM stmt_series
    GROUP BY queryid
),
runs AS (
    /* The calls-per-second denominator (#7566): the seconds the collector's RUNS covered, the rule
       DarlingPgTrendReader.QueryDurationTrendBucketedSql settled on, so the advice and the chart agree. This
       collector skips idle rows at the write, so a statement that runs a batch every 5 minutes has a row in one
       run of five, and the seconds of only those runs overstate its rate fivefold. A run is an axis point, not a
       statement's: each SUCCESS run of the collector is a point, a snapshot belongs to the nearest run logged at or
       after it (within ten minutes), and a run's seconds are the gap to the previous run point. A gap past
       DefaultMaxGapSeconds ($5) is an outage and is unrated, and a run whose rows for the statement carry interval 0 (a
       restart) is unrated for that statement alone, its calls out with its seconds. A snapshot no run claims (a store
       without the log) keeps the stored interval, else the LAG, on its own. */
    SELECT collection_time AS logged_at, COALESCE(duration_ms, 0)::bigint AS duration_ms
    FROM collection_log
    WHERE server_id = $1
    AND   collector_name = 'pg_statement_stats'
    AND   status = 'SUCCESS'
    AND   collection_time >= $2 - $5 * INTERVAL '1 second'
    AND   collection_time <= $3 + $5 * INTERVAL '1 second'
),
snapshot_events AS (
    SELECT DISTINCT collection_time AS event_time, 0 AS is_run, CAST(NULL AS timestamp) AS logged_at
    FROM stmt_series
    UNION ALL
    SELECT logged_at, 1, logged_at
    FROM runs
),
claimed AS (
    SELECT event_time, is_run,
           MIN(logged_at) OVER (ORDER BY event_time DESC, is_run DESC) AS next_run
    FROM snapshot_events
),
owned AS (
    SELECT event_time AS collection_time, next_run AS run_logged_at
    FROM claimed
    WHERE is_run = 0
    AND   next_run IS NOT NULL
    AND   next_run - event_time <= INTERVAL '600 seconds'
),
run_points AS (
    SELECT r.logged_at,
           COALESCE(MIN(o.collection_time), r.logged_at - r.duration_ms * INTERVAL '1 millisecond') AS point_time
    FROM runs AS r
    LEFT JOIN owned AS o ON o.run_logged_at = r.logged_at
    GROUP BY r.logged_at, r.duration_ms
),
run_gaps AS (
    SELECT logged_at, point_time,
           extract(epoch FROM (date_trunc('second', point_time) - date_trunc('second', LAG(point_time) OVER (ORDER BY point_time)))) AS gap_seconds
    FROM run_points
),
run_rates AS (
    SELECT logged_at, gap_seconds,
           CASE WHEN gap_seconds <= $5 THEN gap_seconds END AS run_seconds
    FROM run_gaps
    WHERE point_time >= $2
    AND   point_time <= $3
),
run_total AS (
    SELECT COALESCE(SUM(run_seconds) FILTER (WHERE run_seconds > 0), 0) AS seconds
    FROM run_rates
),
statement_runs AS (
    SELECT s.queryid, o.run_logged_at, SUM(s.calls) AS calls, MAX(s.stored_interval) AS stored_interval
    FROM per_snapshot AS s
    JOIN owned AS o ON o.collection_time = s.collection_time
    GROUP BY s.queryid, o.run_logged_at
),
statement_run_rates AS (
    SELECT q.queryid, q.calls, r.run_seconds AS global_seconds,
           CASE WHEN q.stored_interval = 0 THEN NULL
                WHEN r.gap_seconds IS NOT NULL THEN r.run_seconds
                ELSE NULLIF(q.stored_interval, 0)
           END AS seconds
    FROM statement_runs AS q
    JOIN run_rates AS r ON r.logged_at = q.run_logged_at
),
loose AS (
    SELECT p.queryid, p.calls,
           CASE WHEN p.stored_interval IS NULL
                THEN extract(epoch FROM (p.collection_time - LAG(p.collection_time) OVER (PARTITION BY p.queryid ORDER BY p.collection_time)))
                ELSE NULLIF(p.stored_interval, 0)
           END AS seconds
    FROM per_snapshot AS p
    WHERE NOT EXISTS (SELECT 1 FROM owned AS o WHERE o.collection_time = p.collection_time)
),
rate_points AS (
    SELECT queryid,
           CASE WHEN seconds > 0 THEN calls ELSE 0 END AS calls,
           CASE WHEN seconds > 0 THEN 1 ELSE 0 END AS rated,
           (CASE WHEN seconds > 0 THEN seconds ELSE 0 END) - (CASE WHEN global_seconds > 0 THEN global_seconds ELSE 0 END) AS extra_seconds
    FROM statement_run_rates
    UNION ALL
    SELECT queryid,
           CASE WHEN seconds > 0 THEN calls ELSE 0 END,
           CASE WHEN seconds > 0 THEN 1 ELSE 0 END,
           CASE WHEN seconds > 0 THEN seconds ELSE 0 END
    FROM loose
),
statement_rates AS (
    SELECT queryid, SUM(calls) AS rated_calls, SUM(rated) AS rated_snapshots, SUM(extra_seconds) AS extra_seconds
    FROM rate_points
    GROUP BY queryid
)
SELECT
    p.queryid,
    CAST(COALESCE(SUM(p.calls), 0) AS bigint)             AS calls,
    CAST(COALESCE(SUM(p.total_exec_ms), 0) AS bigint)     AS total_exec_ms,
    MAX(p.max_exec_ms)                                    AS max_exec_ms,
    MAX(p.max_exec_peakmem_bytes)                         AS max_exec_peakmem_bytes,
    CAST(COALESCE(SUM(p.temp_blks_written), 0) AS bigint) AS temp_blks_written,
    CASE WHEN MAX(r.rated_snapshots) > 0 AND MAX(t.seconds) + MAX(r.extra_seconds) > 0
         THEN CAST(MAX(r.rated_calls) AS double precision) / (MAX(t.seconds) + MAX(r.extra_seconds))
    END                                                   AS calls_per_sec,
    MAX(d.database_count)                                 AS database_count,
    CAST(SUM(SUM(p.total_exec_ms)) OVER () AS bigint)     AS window_total_exec_ms
    -- max_exec_peakmem_bytes: Aurora's aurora_stat_statements() only (#3691) - NULL, never 0, off Aurora.
FROM per_snapshot AS p
JOIN databases AS d
  ON d.queryid = p.queryid
LEFT JOIN statement_rates AS r
  ON r.queryid = p.queryid
CROSS JOIN run_total AS t
GROUP BY p.queryid
HAVING SUM(p.total_exec_ms) > 0
ORDER BY SUM(p.total_exec_ms) DESC
LIMIT $4";

    /// <summary>
    /// Emits one <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> fact per top statement of the window (at most
    /// <see cref="TopStatementCount"/>), keyed through <see cref="PgTargetFactKeys.BadActorKey"/> — the
    /// query-shaped leaf every PostgreSQL story needs from day one (#3542 v1 step 7; filled by lane 7). The
    /// read is <see cref="PgTargetTopStatementsSql"/>; the SQL Server sibling is
    /// <see cref="PgFactCollector.BadActorSql"/>, whose SHAPE (top statements, one fact each, per-statement
    /// metadata the scorer and advice read) transfers and whose numbers do not.
    ///
    /// <para><b>What the fact carries.</b> <see cref="Fact.Value"/> is the statement's SHARE of the window's
    /// total execution time — the decision variable, a fraction of the same rows' total, so it scales with
    /// <c>hours_back</c> by construction. Metadata: <c>share_of_window_time</c> (the value again, named);
    /// <c>window_total_exec_ms</c>; <c>window_busy_fraction</c> = that total over
    /// <see cref="AnalysisContext.ObservedDurationMs"/>, the "was this server doing anything" gate the scorer
    /// reads so that 60% of nothing on an idle box never roots a card; <c>calls</c>, <c>total_exec_ms</c>,
    /// <c>mean_exec_ms</c> (absent when calls is 0 — a mean over no calls is not 0 ms), <c>max_exec_ms</c>;
    /// <c>calls_per_sec</c> over the seconds the collector's runs covered in the window (#7566; absent when
    /// the statement has no rated snapshot — see the SQL);
    /// <c>max_exec_peakmem_bytes</c> (#3691) — Aurora's per-query peak executor memory, present only where
    /// <c>aurora_stat_statements()</c> populated it, absent (never zero) off Aurora; a CONTEXT fact with no
    /// bar and no threshold, carried the same way <c>max_exec_ms</c> is, and read by nothing that grades a
    /// finding — a distribution read is a separate ruling; <c>temp_blks_written</c> for lane 6's co-fire; <c>database_count</c>. The <c>queryid</c> is in the KEY
    /// and nowhere else: the metadata is doubles-only, a 64-bit id is exact in a double only up to 2^53, and
    /// most real ids exceed that — a lossy copy a reader could paste into <c>get_pg_top_queries</c> and match
    /// nothing is worse than none. The drill-down carries <c>text_hash</c> beside the text.</para>
    ///
    /// <para><b>No fact without an observed window.</b> <c>ObservedDurationMs</c> of 0 means the coverage
    /// witness stamped nothing (#3538 A2): a busy fraction over it is undefined, and emitting the facts
    /// without their gate would let an unobserved window root a card on the share alone. The pass reports
    /// <c>unavailable</c> in that case anyway; this method simply contributes nothing to it.</para>
    ///
    /// <para><see cref="Fact.DatabaseName"/> stays null: <c>pg_statement_stats</c> carries the OID, not the
    /// name, and the rollup is to <c>queryid</c> across databases. The drill-down's per-database breakdown is
    /// where the OIDs are listed.</para>
    /// </summary>
    private async partial Task CollectQueryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        var observedMs = context.ObservedDurationMs;
        if (observedMs <= 0)
            return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetTopStatementsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(TopStatementCount);
            cmd.Parameters.AddWithValue(CollectorDeltaCalculator.DefaultMaxGapSeconds);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                if (reader.IsDBNull(0)) continue;
                var queryId = ToInt64(reader.GetValue(0));
                var calls = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
                var totalExecMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
                var maxExecMs = reader.IsDBNull(3) ? (double?)null : Convert.ToDouble(reader.GetValue(3));
                var maxExecPeakmemBytes = reader.IsDBNull(4) ? (long?)null : ToInt64(reader.GetValue(4));
                var tempBlksWritten = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
                var callsPerSec = reader.IsDBNull(6) ? (double?)null : Convert.ToDouble(reader.GetValue(6));
                var databaseCount = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
                var windowTotalExecMs = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));

                /* HAVING admits only shapes with time > 0, and the window total is at least this row's, so
                   the share is defined; the guard is against a store that answers the projection differently. */
                if (windowTotalExecMs <= 0 || totalExecMs <= 0) continue;
                var share = (double)totalExecMs / windowTotalExecMs;

                var fact = new Fact
                {
                    Source = PgTargetSources.QueriesSource,
                    Key = PgTargetFactKeys.BadActorKey(queryId),
                    Value = share,
                    ServerId = context.ServerId,
                    Metadata =
                    {
                        ["share_of_window_time"] = share,
                        ["window_total_exec_ms"] = windowTotalExecMs,
                        ["window_busy_fraction"] = windowTotalExecMs / observedMs,
                        ["calls"] = calls,
                        ["total_exec_ms"] = totalExecMs,
                        ["temp_blks_written"] = tempBlksWritten,
                        ["database_count"] = databaseCount,
                    },
                };

                /* Absent rather than zero, each for its own reason: a mean over no calls is not 0 ms; a
                   high-water mark the source never reported is not 0 ms; a rate over an unknowable span is
                   not 0/s. */
                if (calls > 0) fact.Metadata["mean_exec_ms"] = (double)totalExecMs / calls;
                if (maxExecMs is { } max) fact.Metadata["max_exec_ms"] = max;
                if (callsPerSec is { } rate) fact.Metadata["calls_per_sec"] = rate;
                /* #3691: NULL off Aurora (stock PostgreSQL never populates the column), never 0 — a context
                   fact only, no bar, no threshold; nothing here reads it to grade a finding. */
                if (maxExecPeakmemBytes is { } peakMem) fact.Metadata["max_exec_peakmem_bytes"] = peakMem;

                facts.Add(fact);
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_statement_stats needs pg_stat_statements on the target and pg_statement_text arrived in V73;
               a store without either raises 42P01 here, which the reporter classifies quiet. Degrades to "no
               facts" so one unavailable input cannot cost this server its other facts, and WHY is reported,
               not assumed (#2826). An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
