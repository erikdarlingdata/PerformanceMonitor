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
    /// <c>$4</c> the row cap (<see cref="TopStatementCount"/>).
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
    /// <para><b>The interval is the three-state idiom from <c>QueryDurationTrendSql</c> (V128, #3540).</b>
    /// Per snapshot the accrual span is the stored <c>sample_interval_seconds</c> — the span the delta
    /// actually accrued over, which is NOT the gap between stored rows because the collector skips idle rows
    /// at the write — taken as <c>MAX</c> over the queryid's rows and made NULL through <c>NULLIF(…, 0)</c>
    /// when every row was unknowable (a first sighting or a <c>pg_stat_statements_reset()</c>); a NULL stored
    /// interval (a pre-V128 row) falls back to the <c>LAG</c> over the queryid's snapshots; and there is no
    /// <c>ELSE 0</c>, so the first snapshot of a pre-V128 series is absent rather than a fabricated zero. The
    /// call RATE is then calls over the snapshots whose span is known, divided by that known span — a
    /// first-sighting snapshot contributes 0 calls and 0 span, consistently — and is NULL when no snapshot's
    /// span is known, which the C# renders as no <c>calls_per_sec</c> at all.</para>
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
        CASE WHEN MAX(sample_interval_seconds) IS NULL
             THEN extract(epoch FROM (collection_time - LAG(collection_time) OVER (PARTITION BY queryid ORDER BY collection_time)))
             ELSE NULLIF(MAX(sample_interval_seconds), 0)
        END                           AS interval_seconds
    FROM stmt_series
    GROUP BY queryid, collection_time
),
databases AS (
    SELECT queryid, COUNT(DISTINCT database_id) AS database_count
    FROM stmt_series
    GROUP BY queryid
)
SELECT
    p.queryid,
    CAST(COALESCE(SUM(p.calls), 0) AS bigint)             AS calls,
    CAST(COALESCE(SUM(p.total_exec_ms), 0) AS bigint)     AS total_exec_ms,
    MAX(p.max_exec_ms)                                    AS max_exec_ms,
    MAX(p.max_exec_peakmem_bytes)                         AS max_exec_peakmem_bytes,
    CAST(COALESCE(SUM(p.temp_blks_written), 0) AS bigint) AS temp_blks_written,
    CASE WHEN SUM(p.interval_seconds) > 0
         THEN CAST(SUM(p.calls) FILTER (WHERE p.interval_seconds IS NOT NULL) AS double precision) / SUM(p.interval_seconds)
    END                                                   AS calls_per_sec,
    MAX(d.database_count)                                 AS database_count,
    CAST(SUM(SUM(p.total_exec_ms)) OVER () AS bigint)     AS window_total_exec_ms
    -- max_exec_peakmem_bytes: Aurora's aurora_stat_statements() only (#3691) - NULL, never 0, off Aurora.
FROM per_snapshot AS p
JOIN databases AS d
  ON d.queryid = p.queryid
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
    /// <c>calls_per_sec</c> over the statement's known accrual span (absent when unknowable — see the SQL);
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
