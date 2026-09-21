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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The per-table backlog read: every table whose LATEST <see cref="PgTargetScorer.BacklogPersistenceSamples"/>
    /// (<c>$4</c>) consecutive hourly samples sit past the engine's own trigger line, ranked worst-first by the
    /// ratio to that line, <c>$5</c> rows. Pattern: <c>DarlingPgAutovacuumReader.PgAutovacuumSql</c> — the same
    /// two-arm line (dead tuples against <c>vacuum_threshold</c>, inserts against <c>insert_vacuum_threshold</c>
    /// where the major has it; the <c>-1</c> sentinel stays out of the arithmetic), the same ratio-not-count
    /// ranking with disabled tables first, the same <c>IS NOT DISTINCT FROM</c> joins on the nullable name
    /// triple — plus what only an analysis read adds:
    /// <list type="bullet">
    /// <item><description><b>Persistence</b>: <c>trailing_samples_past_line</c> counts samples past the line
    /// with no clear sample after them (everything newer than the table's last clear reading), so one clear
    /// hour resets the run. "Consecutive" is by sample, which at the hourly cadence is by hour.</description></item>
    /// <item><description><b>Slope inputs</b>: the run's first sample (<c>run_started_at</c>, its dead and
    /// insert gauges) beside the latest, so the caller divides a LEVEL's change by the run's own span — never
    /// the nominal window, never an assumed cadence.</description></item>
    /// <item><description><b>Run count inputs</b>: <c>autovacuum_count</c> at both ends of the run. It is a
    /// CUMULATIVE counter (the store keeps PostgreSQL counters raw), so it is differenced by the caller and
    /// never summed; a negative difference is a statistics reset and reads as "unknown".</description></item>
    /// <item><description><c>tables_in_backlog</c>: how many tables met the gate, computed before the LIMIT
    /// (window functions run before it), so the one fact can say how many others there are.</description></item>
    /// </list>
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> persistence samples, <c>$5</c> row limit.
    ///
    /// <para><b>Composition (#3691 step 22).</b> The CTE chain and the projection are the private
    /// <see cref="BacklogRunsCte"/> / <see cref="BacklogProjection"/> halves, and this const and
    /// <see cref="PgTargetAutovacuumDisabledSql"/> are each <c>halves + own WHERE / ORDER / LIMIT</c> — a
    /// compile-time constant either way (<c>const</c> concatenation), so <c>AllSql</c>'s reflection closure and
    /// the dialect / FROM-target censuses read the finished text. One run definition, two reads: the disabled
    /// read is "the same runs, only the tables whose reloption is off", and a second copy of ninety lines of
    /// window arithmetic would be the first place the two drifted.</para>
    /// </summary>
    public const string PgTargetAutovacuumBacklogSql = BacklogRunsCte + BacklogProjection + @"
WHERE l.trailing_samples_past_line >= $4
ORDER BY
    l.autovacuum_disabled DESC,
    backlog_ratio DESC NULLS LAST,
    GREATEST(l.dead_tuples, l.inserts_since_vacuum) DESC
LIMIT $5";

    /// <summary>
    /// The per-table DISABLED read (#3691 step 22, design §3.1): the same runs as
    /// <see cref="PgTargetAutovacuumBacklogSql"/>, restricted to tables whose <c>autovacuum_enabled</c> reloption
    /// is off (<c>autovacuum_disabled</c>, stamped per row by the collector from <c>reloptions</c>) that ALSO
    /// met the persistence gate — a disabled table with no backlog is not returned, because someone may be
    /// vacuuming it by hand and the fact would be an opinion about their schedule. Ranked worst-first by the
    /// ratio to the table's own line; <c>tables_in_backlog</c> here counts the DISABLED tables that met the gate
    /// (the window function runs after this WHERE). <c>$5</c> is the top-N the fact carries the shape of.
    /// </summary>
    public const string PgTargetAutovacuumDisabledSql = BacklogRunsCte + BacklogProjection + @"
WHERE l.autovacuum_disabled
AND   l.trailing_samples_past_line >= $4
ORDER BY
    backlog_ratio DESC NULLS LAST,
    GREATEST(l.dead_tuples, l.inserts_since_vacuum) DESC
LIMIT $5";

    /// <summary>The CTE chain both autovacuum reads share: samples → marked → runs → latest → run_start
    /// (documented on <see cref="PgTargetAutovacuumBacklogSql"/>). Private and not <c>*Sql</c>-suffixed on
    /// purpose: it is half a statement, never executed on its own, and must stay out of <c>AllSql</c>.</summary>
    private const string BacklogRunsCte = @"
WITH samples AS (
    SELECT
        database_name, schema_name, table_name, collection_time,
        live_tuples, dead_tuples, vacuum_threshold,
        inserts_since_vacuum, insert_vacuum_threshold,
        autovacuum_disabled, total_bytes, last_autovacuum, autovacuum_count,
        (vacuum_threshold > 0 AND dead_tuples > vacuum_threshold)
            OR (insert_vacuum_threshold > 0 AND inserts_since_vacuum > insert_vacuum_threshold) AS past_line
    FROM pg_autovacuum_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
marked AS (
    SELECT
        s.*,
        COUNT(*) OVER per_table AS samples_in_window,
        MAX(CASE WHEN NOT past_line THEN collection_time END) OVER per_table AS last_clear_time
    FROM samples AS s
    WINDOW per_table AS (PARTITION BY database_name, schema_name, table_name)
),
runs AS (
    SELECT
        m.*,
        COUNT(*) FILTER (WHERE past_line AND (last_clear_time IS NULL OR collection_time > last_clear_time))
            OVER per_table AS trailing_samples_past_line,
        MIN(collection_time) FILTER (WHERE past_line AND (last_clear_time IS NULL OR collection_time > last_clear_time))
            OVER per_table AS run_started_at
    FROM marked AS m
    WINDOW per_table AS (PARTITION BY database_name, schema_name, table_name)
),
latest AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name) *
    FROM runs
    ORDER BY database_name, schema_name, table_name, collection_time DESC
),
run_start AS (
    SELECT
        r.database_name, r.schema_name, r.table_name,
        r.dead_tuples          AS run_first_dead_tuples,
        r.inserts_since_vacuum AS run_first_inserts_since_vacuum,
        r.autovacuum_count     AS run_first_autovacuum_count
    FROM runs AS r
    JOIN latest AS l
      ON  l.database_name IS NOT DISTINCT FROM r.database_name
      AND l.schema_name   IS NOT DISTINCT FROM r.schema_name
      AND l.table_name    IS NOT DISTINCT FROM r.table_name
      AND l.run_started_at = r.collection_time
)";

    /// <summary>The projection both autovacuum reads share — the latest sample beside the run's first, the
    /// two-arm ratio, and the post-WHERE table count. Ends at <c>JOIN run_start</c>; each read appends its own
    /// <c>WHERE</c>, <c>ORDER BY</c> and <c>LIMIT</c>.</summary>
    private const string BacklogProjection = @"
SELECT
    l.database_name,
    l.schema_name,
    l.table_name,
    l.collection_time,
    l.live_tuples,
    l.dead_tuples,
    l.vacuum_threshold,
    l.inserts_since_vacuum,
    l.insert_vacuum_threshold,
    l.autovacuum_disabled,
    l.total_bytes,
    l.last_autovacuum,
    l.autovacuum_count,
    l.samples_in_window,
    l.trailing_samples_past_line,
    l.run_started_at,
    rs.run_first_dead_tuples,
    rs.run_first_inserts_since_vacuum,
    rs.run_first_autovacuum_count,
    GREATEST(
        l.dead_tuples::numeric / NULLIF(l.vacuum_threshold, 0),
        CASE
            WHEN l.insert_vacuum_threshold > 0
                THEN l.inserts_since_vacuum::numeric / l.insert_vacuum_threshold
            ELSE 0
        END
    ) AS backlog_ratio,
    COUNT(*) OVER () AS tables_in_backlog
FROM latest AS l
JOIN run_start AS rs
  ON  rs.database_name IS NOT DISTINCT FROM l.database_name
  AND rs.schema_name   IS NOT DISTINCT FROM l.schema_name
  AND rs.table_name    IS NOT DISTINCT FROM l.table_name";

    /// <summary>
    /// The wraparound read: the LATEST reading per database with the window's peak and FIRST reading for each
    /// counter. Pattern: <c>DarlingPgWraparoundReader.PgWraparoundSql</c> — a level, not accumulated work, so
    /// the current value is read (never averaged, never summed) and the window peak answers "did autovacuum
    /// claw it back" (#2689's <c>FreezingIsKeepingUp</c>: latest below peak). The first reading is what this
    /// read adds, for the slope → time-to-wall arithmetic; the two counters ride separately because they
    /// are graded against different settings (<c>PostgresAlertInfo</c>'s per-counter argument) and are never
    /// collapsed. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    /// </summary>
    public const string PgTargetWraparoundSql = @"
SELECT DISTINCT ON (database_name)
    database_name,
    collection_time,
    frozen_xid_age,
    min_multixid_age,
    autovacuum_freeze_max_age,
    autovacuum_multixact_freeze_max_age,
    xids_remaining,
    multixids_remaining,
    MAX(frozen_xid_age)   OVER per_db AS window_peak_frozen_xid_age,
    MAX(min_multixid_age) OVER per_db AS window_peak_min_multixid_age,
    FIRST_VALUE(frozen_xid_age)   OVER ordered AS first_frozen_xid_age,
    FIRST_VALUE(min_multixid_age) OVER ordered AS first_min_multixid_age,
    FIRST_VALUE(collection_time)  OVER ordered AS first_seen_at,
    COUNT(*) OVER per_db AS samples_in_window
FROM pg_wraparound_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
WINDOW per_db AS (PARTITION BY database_name),
       ordered AS (PARTITION BY database_name ORDER BY collection_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY database_name, collection_time DESC";

    /// <summary>
    /// The xmin read: how persistently the HORIZON was held, and who held it, as two separate questions.
    ///
    /// <para><b>Why the shape changed (#3691 step 40).</b> v1 (#3674) measured persistence on the holder's
    /// IDENTITY — "the same (source, holder) won N consecutive captures" — which is the alert evaluator's
    /// identity arm, and which reads 0 on the shape a real stock storm produced: a 4.3 M-xid horizon pinned
    /// for twenty-five minutes while FIVE equally-old transactions alternated as each capture's
    /// <c>is_winner</c>, so no identity was ever seen twice. The horizon is the thing being held; who holds
    /// it is attribution. So persistence is now counted on the winning <c>xmin_age</c> (any source): the
    /// longest run of CONSECUTIVE captures whose winner sat at or above the shared bar (<c>$4</c> =
    /// <see cref="PostgresOutagePredictorThresholds.XminAgeWarningThreshold"/>, bound so the condition
    /// counted here and the one graded cannot drift), with that run's floor and peak age; attribution —
    /// per-source shares, the distinct holder count, the modal holder and the modal source — is computed
    /// over the captures of that same run and carried beside it.</para>
    ///
    /// <para>Gaps-and-islands names the run: inside a stretch of identically-flagged captures the difference
    /// between a global dense ordering and a per-flag dense ordering is constant, so it IS the run's key. The
    /// longest above-bar run wins ties by recency, because the operator is being told about the horizon that
    /// is held now.</para>
    ///
    /// <para>The denominator stays DISTINCT collection times that recorded any holder — several sources are
    /// stored per collection and the collector writes nothing at all when the horizon is unheld, so a capture
    /// with rows is a capture where something held it. The alert's own horizon arm divides by the collector's
    /// SUCCESS captures in <c>collection_log</c> (#3537, #3642 — including the healthy zero-row runs); that is
    /// a table this read may not name, so <c>held_fraction</c> is a fraction of holder-bearing captures and
    /// the prose says so rather than implying coverage it cannot measure.</para>
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> age bar.
    /// </summary>
    public const string PgTargetXminHoldSql = @"
WITH captures AS (
    SELECT DISTINCT ON (collection_time)
        collection_time,
        source,
        holder,
        xmin_age,
        (xmin_age >= $4) AS above_bar
    FROM pg_xmin_horizon
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   is_winner
    ORDER BY collection_time, xmin_age DESC, source
),
flagged AS (
    SELECT
        c.collection_time,
        c.source,
        c.holder,
        c.xmin_age,
        c.above_bar,
        ROW_NUMBER() OVER (ORDER BY c.collection_time)
            - ROW_NUMBER() OVER (PARTITION BY c.above_bar ORDER BY c.collection_time) AS run_key
    FROM captures AS c
),
runs AS (
    SELECT
        run_key,
        COUNT(*) AS captures_in_run,
        MIN(xmin_age) AS run_floor_age,
        MAX(xmin_age) AS run_peak_age,
        MAX(collection_time) AS run_end
    FROM flagged
    WHERE above_bar
    GROUP BY run_key
),
longest AS (
    SELECT run_key, captures_in_run, run_floor_age, run_peak_age
    FROM runs
    ORDER BY captures_in_run DESC, run_end DESC
    LIMIT 1
),
held AS (
    SELECT f.source, f.holder, f.collection_time
    FROM flagged AS f
    JOIN longest AS g ON g.run_key = f.run_key
    WHERE f.above_bar
),
shares AS (
    SELECT
        COUNT(*) FILTER (WHERE source = 'session')::double precision / NULLIF(COUNT(*), 0) AS backend_share,
        COUNT(*) FILTER (WHERE source IN ('replication_slot', 'replication_slot_catalog'))::double precision / NULLIF(COUNT(*), 0) AS slot_share,
        COUNT(*) FILTER (WHERE source = 'standby_feedback')::double precision / NULLIF(COUNT(*), 0) AS standby_share,
        COUNT(*) FILTER (WHERE source = 'prepared_transaction')::double precision / NULLIF(COUNT(*), 0) AS prepared_share,
        (SELECT COUNT(*) FROM (SELECT DISTINCT source, holder FROM held) AS d) AS distinct_holders
    FROM held
),
modal_holder AS (
    SELECT source, holder, COUNT(*) AS wins
    FROM held
    GROUP BY source, holder
    ORDER BY COUNT(*) DESC, MAX(collection_time) DESC
    LIMIT 1
),
modal_source AS (
    SELECT source, COUNT(*) AS wins
    FROM held
    GROUP BY source
    ORDER BY COUNT(*) DESC, MAX(collection_time) DESC
    LIMIT 1
),
window_stats AS (
    SELECT
        COUNT(DISTINCT collection_time) AS observations_total,
        MAX(xmin_age) FILTER (WHERE is_winner) AS peak_winning_age
    FROM pg_xmin_horizon
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
latest AS (
    SELECT source, holder, xmin_age, collection_time
    FROM captures
    ORDER BY collection_time DESC
    LIMIT 1
)
SELECT
    l.source,
    l.holder,
    l.xmin_age,
    l.collection_time,
    w.observations_total,
    w.peak_winning_age,
    coalesce(g.captures_in_run, 0) AS held_captures,
    g.run_floor_age,
    g.run_peak_age,
    coalesce(s.distinct_holders, 0) AS distinct_holders,
    s.backend_share,
    s.slot_share,
    s.standby_share,
    s.prepared_share,
    mh.source AS modal_holder_source,
    mh.holder AS modal_holder,
    coalesce(mh.wins, 0) AS modal_holder_captures,
    ms.source AS modal_source,
    coalesce(ms.wins, 0) AS modal_source_captures
FROM latest AS l
CROSS JOIN window_stats AS w
LEFT JOIN longest AS g ON true
LEFT JOIN shares AS s ON true
LEFT JOIN modal_holder AS mh ON true
LEFT JOIN modal_source AS ms ON true";

    /// <summary>
    /// How many backlogged tables the read returns. The fact carries ONE — the worst by ratio, the way
    /// <c>ANOMALY_OBJECT_GROWTH</c> carries its one table in <see cref="Fact.ObjectName"/> — because the
    /// engine keys facts by <see cref="Fact.Key"/> and a second row under the same key would be dropped by
    /// <c>ToFactLookup</c>; the count of the others rides in metadata and <c>get_pg_autovacuum</c> lists them.
    /// One row is therefore all the fact needs; the limit is the read's own bound.
    /// </summary>
    private const int BacklogRowLimit = 1;

    /// <summary>
    /// How many disabled-and-backlogged tables the disabled read returns: the worst is the fact (its name in
    /// <see cref="Fact.ObjectName"/>, its figures under the backlog metadata keys), the next two ride as ratio
    /// and hours under <see cref="PgTargetScorer.AutovacuumDisabledRankRatioKey"/> /
    /// <see cref="PgTargetScorer.AutovacuumDisabledRankHoursKey"/> so the advice can state the shape of the
    /// rest ("two more, 3.1× for 5 h and 1.4× for 2 h") without their names, which doubles cannot carry.
    /// Three because the card is a summary, not the list — <c>get_pg_autovacuum_health</c> is the list.
    /// </summary>
    private const int AutovacuumDisabledRowLimit = 3;

    /// <summary>
    /// <c>PG_AUTOVACUUM_BACKLOG</c> (the worst persistently-backlogged table, ratio to its OWN line and slope
    /// over the run), <c>PG_WRAPAROUND_TREND</c> (the relatively-worst database and counter, XID and MultiXact
    /// graded separately) and <c>PG_XMIN_HOLD</c> (how persistently the HORIZON was held, with the holder
    /// attributed separately — #3691 step 40) —
    /// filled by lane 4 of #3542. Three reads, each behind its own degrade so a missing table (a pre-V68 store, a
    /// flavour on which a collector does not run) costs only its own fact.
    ///
    /// <para>Emission is evidence-gated: no rows in the window → no fact, never a zero. The wraparound and
    /// xmin facts ARE emitted at base 0 when rows exist but nothing graded — a healthy sawtooth and a
    /// transient holder are context the story engine ignores and <c>get_analysis_facts</c> shows; the backlog
    /// fact exists only when a table met the persistence gate, because "no table is past its line" has no
    /// number to carry.</para>
    ///
    /// <para>The wraparound read runs first so its <c>autovacuum_freeze_max_age</c> can be handed to the xmin
    /// fact — the engine-defined top of that fact's severity ramp (a horizon held past the freeze age stops the
    /// forced anti-wraparound vacuum advancing <c>relfrozenxid</c>, §3.3). Absent, the xmin base stays flat at
    /// the alert's Warning.</para>
    ///
    /// <para><c>CONFIG_PG_AUTOVACUUM_DISABLED</c> (#3691 step 22) is the fourth read, after the backlog: a table
    /// whose reloption is off AND which met the same persistence gate. Its own degrade, same table, same
    /// 42P01 classification; a disabled table with no backlog produces nothing (§3.1).</para>
    /// </summary>
    private async partial Task CollectVacuumFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        var freezeMaxAge = await ReadWraparoundTrendAsync(context, facts);
        await ReadAutovacuumBacklogAsync(context, facts);
        await ReadAutovacuumDisabledAsync(context, facts);
        await ReadXminHoldAsync(context, facts, freezeMaxAge);
    }

    /// <summary>
    /// Emits <c>CONFIG_PG_AUTOVACUUM_DISABLED</c> for the worst table (by ratio to its own line) whose
    /// <c>autovacuum_enabled</c> reloption is off and which has sat past that line for
    /// <see cref="PgTargetScorer.BacklogPersistenceSamples"/> consecutive hourly samples — the same gate as the
    /// backlog, bound the same way, so the two facts can never disagree about whether a table is backlogged.
    /// The worst table's figures ride under the backlog metadata keys (one vocabulary, the advice reads both
    /// facts with the same names); ranks 2 and 3 ride as ratio + hours; the count of disabled tables that met
    /// the gate is <see cref="PgTargetScorer.AutovacuumDisabledTablesKey"/>. "Hours past line" is the run's
    /// own span (latest sample minus the run's first), never an assumed cadence.
    ///
    /// <para>Whether autovacuum is ALSO off server-wide is read off the <c>CONFIG_PG_AUTOVACUUM_OFF</c> fact the
    /// config read emitted earlier in this pass (emission order: Config before Vacuum), never from a second
    /// <c>pg_server_config</c> read — one pass, one truth about the setting. Absent config fact (no snapshot in
    /// the window) reads as "not known to be off", 0.</para>
    /// </summary>
    private async Task ReadAutovacuumDisabledAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetAutovacuumDisabledSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PgTargetScorer.BacklogPersistenceSamples);
            cmd.Parameters.AddWithValue(AutovacuumDisabledRowLimit);

            Fact? fact = null;
            var rank = 0;
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                rank++;
                var latestAt = reader.GetDateTime(3);
                var runStartedAt = reader.GetDateTime(15);
                var ratio = reader.IsDBNull(19) ? 0.0 : Convert.ToDouble(reader.GetValue(19));
                var runHours = (latestAt - runStartedAt).TotalHours;

                if (fact is not null)
                {
                    /* Ranks 2 and 3: the shape only. Same column positions as the first row; the names stay with
                       the tool. */
                    fact.Metadata[PgTargetScorer.AutovacuumDisabledRankRatioKey(rank)] = ratio;
                    fact.Metadata[PgTargetScorer.AutovacuumDisabledRankHoursKey(rank)] = runHours;
                    continue;
                }

                var databaseName = reader.IsDBNull(0) ? null : reader.GetString(0);
                var schema = reader.IsDBNull(1) ? null : reader.GetString(1);
                var table = reader.IsDBNull(2) ? null : reader.GetString(2);
                var live = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
                var dead = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
                var vacuumThreshold = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
                var inserts = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
                var insertThreshold = reader.IsDBNull(8) ? -1L : ToInt64(reader.GetValue(8));
                var totalBytes = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
                DateTime? lastAutovacuum = reader.IsDBNull(11) ? null : reader.GetDateTime(11);
                var samplesInWindow = ToInt64(reader.GetValue(13));
                var trailing = ToInt64(reader.GetValue(14));
                var disabledTables = ToInt64(reader.GetValue(20));

                /* The arm the ratio came from — the backlog read's re-derivation, so the advice names the right line. */
                var deadRatio = vacuumThreshold > 0 ? (double)dead / vacuumThreshold : 0.0;
                var insertRatio = insertThreshold > 0 ? (double)inserts / insertThreshold : 0.0;
                var insertArm = insertRatio > deadRatio;

                var serverOff = facts.Find(f => f.Key == PgTargetFactKeys.ConfigAutovacuumOff) is { Value: > 0 };

                fact = new Fact
                {
                    Source = PgTargetSources.VacuumSource,
                    Key = PgTargetFactKeys.ConfigAutovacuumDisabled,
                    Value = ratio,
                    ServerId = context.ServerId,
                    DatabaseName = databaseName,
                    ObjectName = string.IsNullOrEmpty(table) ? null : string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}",
                    Metadata =
                    {
                        [PgTargetScorer.BacklogRatioKey] = ratio,
                        [PgTargetScorer.BacklogArmIsInsertKey] = insertArm ? 1 : 0,
                        [PgTargetScorer.BacklogTrailingSamplesKey] = trailing,
                        [PgTargetScorer.BacklogSamplesInWindowKey] = samplesInWindow,
                        [PgTargetScorer.BacklogDeadTuplesKey] = dead,
                        [PgTargetScorer.BacklogVacuumThresholdKey] = vacuumThreshold,
                        [PgTargetScorer.BacklogLiveTuplesKey] = live,
                        [PgTargetScorer.BacklogInsertsSinceVacuumKey] = inserts,
                        [PgTargetScorer.BacklogInsertThresholdKey] = insertThreshold,
                        [PgTargetScorer.BacklogTotalBytesKey] = totalBytes,
                        [PgTargetScorer.BacklogHoursKey] = runHours,
                        [PgTargetScorer.BacklogTableAutovacuumDisabledKey] = 1,
                        [PgTargetScorer.AutovacuumDisabledTablesKey] = disabledTables,
                        [PgTargetScorer.AutovacuumDisabledServerOffKey] = serverOff ? 1 : 0,
                    },
                };

                if (lastAutovacuum.HasValue)
                    fact.Metadata[PgTargetScorer.BacklogHoursSinceLastAutovacuumKey] = (AsNaive(context.TimeRangeEnd) - lastAutovacuum.Value).TotalHours;
            }

            if (fact is not null)
                facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Same table as the backlog read, same classification: pg_autovacuum_stats arrived in V68, 42P01 on
               a pre-migration store is quiet; an abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    private async Task ReadAutovacuumBacklogAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetAutovacuumBacklogSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PgTargetScorer.BacklogPersistenceSamples);
            cmd.Parameters.AddWithValue(BacklogRowLimit);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var databaseName = reader.IsDBNull(0) ? null : reader.GetString(0);
            var schema = reader.IsDBNull(1) ? null : reader.GetString(1);
            var table = reader.IsDBNull(2) ? null : reader.GetString(2);
            var latestAt = reader.GetDateTime(3);
            var live = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var dead = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var vacuumThreshold = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
            var inserts = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
            var insertThreshold = reader.IsDBNull(8) ? -1L : ToInt64(reader.GetValue(8));
            var disabled = !reader.IsDBNull(9) && reader.GetBoolean(9);
            var totalBytes = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
            DateTime? lastAutovacuum = reader.IsDBNull(11) ? null : reader.GetDateTime(11);
            var autovacuumCount = reader.IsDBNull(12) ? 0L : ToInt64(reader.GetValue(12));
            var samplesInWindow = ToInt64(reader.GetValue(13));
            var trailing = ToInt64(reader.GetValue(14));
            var runStartedAt = reader.GetDateTime(15);
            var runFirstDead = reader.IsDBNull(16) ? 0L : ToInt64(reader.GetValue(16));
            var runFirstInserts = reader.IsDBNull(17) ? 0L : ToInt64(reader.GetValue(17));
            var runFirstAutovacuumCount = reader.IsDBNull(18) ? 0L : ToInt64(reader.GetValue(18));
            var ratio = reader.IsDBNull(19) ? 0.0 : Convert.ToDouble(reader.GetValue(19));
            var tablesInBacklog = ToInt64(reader.GetValue(20));

            /* Which arm the ratio came from — the same GREATEST the SQL ranked on, re-derived here so the
               metadata names the arm the number belongs to. */
            var deadRatio = vacuumThreshold > 0 ? (double)dead / vacuumThreshold : 0.0;
            var insertRatio = insertThreshold > 0 ? (double)inserts / insertThreshold : 0.0;
            var insertArm = insertRatio > deadRatio;

            var fact = new Fact
            {
                Source = PgTargetSources.VacuumSource,
                Key = PgTargetFactKeys.AutovacuumBacklog,
                Value = ratio,
                ServerId = context.ServerId,
                DatabaseName = databaseName,
                ObjectName = string.IsNullOrEmpty(table) ? null : string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}",
                Metadata =
                {
                    [PgTargetScorer.BacklogRatioKey] = ratio,
                    [PgTargetScorer.BacklogArmIsInsertKey] = insertArm ? 1 : 0,
                    [PgTargetScorer.BacklogTrailingSamplesKey] = trailing,
                    [PgTargetScorer.BacklogSamplesInWindowKey] = samplesInWindow,
                    [PgTargetScorer.BacklogDeadTuplesKey] = dead,
                    [PgTargetScorer.BacklogVacuumThresholdKey] = vacuumThreshold,
                    [PgTargetScorer.BacklogLiveTuplesKey] = live,
                    [PgTargetScorer.BacklogInsertsSinceVacuumKey] = inserts,
                    [PgTargetScorer.BacklogInsertThresholdKey] = insertThreshold,
                    [PgTargetScorer.BacklogTableAutovacuumDisabledKey] = disabled ? 1 : 0,
                    [PgTargetScorer.BacklogTablesKey] = tablesInBacklog,
                    [PgTargetScorer.BacklogTotalBytesKey] = totalBytes,
                },
            };

            /* Slope: a LEVEL (n_dead_tup / n_ins_since_vacuum are gauges) differenced across the run and divided
               by the run's own span — the series' timestamps, not the nominal window and not an assumed cadence.
               A run whose samples share a timestamp has no span to divide by and says so. */
            var runHours = (latestAt - runStartedAt).TotalHours;
            fact.Metadata[PgTargetScorer.BacklogHoursKey] = runHours;
            if (runHours > 0)
            {
                var delta = insertArm ? inserts - runFirstInserts : dead - runFirstDead;
                fact.Metadata[PgTargetScorer.BacklogSlopePerHourKey] = delta / runHours;
                fact.Metadata[PgTargetScorer.BacklogSlopeComputableKey] = 1;
            }
            else
            {
                fact.Metadata[PgTargetScorer.BacklogSlopeComputableKey] = 0;
            }

            /* autovacuum_count is CUMULATIVE: difference the run's ends, never sum. Backwards = a statistics
               reset (pg_stat_reset, a crash), which is "unknown", not zero runs. */
            var runs = autovacuumCount - runFirstAutovacuumCount;
            if (runs >= 0)
            {
                fact.Metadata[PgTargetScorer.BacklogAutovacuumRunsKey] = runs;
                fact.Metadata[PgTargetScorer.BacklogRunsComputableKey] = 1;
            }
            else
            {
                fact.Metadata[PgTargetScorer.BacklogRunsComputableKey] = 0;
            }

            if (lastAutovacuum.HasValue)
                fact.Metadata[PgTargetScorer.BacklogHoursSinceLastAutovacuumKey] = (AsNaive(context.TimeRangeEnd) - lastAutovacuum.Value).TotalHours;

            /* The D5 stamp onto the knob, off the config fact emitted a moment ago (emission order: Config before
               Vacuum) — the same seam PgTargetFactCollector.Database.cs uses to stamp the spill rate onto work_mem.
               CONFIG_PG_MAINT_WORK_MEM's source is pg_config, so its base comes from ScoreConfigFact, which receives
               one fact and no lookup; the backlog ratio has to be ON the fact for the knob to score anything
               (PgTargetScorer.Config.cs, ScoreConfigMaintWorkMem). Absent (no backlog fact) the knob stays context. */
            var maintWorkMem = facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaintWorkMem);
            if (maintWorkMem is not null)
                maintWorkMem.Metadata[PgTargetScorer.MaintWorkMemBacklogRatioKey] = ratio;

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_autovacuum_stats arrived in V68 and does not run in recovery (AppliesTo => !IsInRecovery) — a
               pre-migration store raises 42P01, classified quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Emits <c>PG_WRAPAROUND_TREND</c> for the relatively-worst (database, counter) and returns that
    /// database's <c>autovacuum_freeze_max_age</c> (0 when nothing was read) for the xmin fact's ramp.
    /// "Worst" is decided by <see cref="PgTargetScorer.GradeWraparoundCounter"/> — the same walk the alert
    /// evaluator takes — severity first, then the counter's fraction of its OWN setting, so a Critical
    /// MultiXact is never hidden behind a merely-warning XID with a bigger raw number.
    /// </summary>
    private async Task<long> ReadWraparoundTrendAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetWraparoundSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            Fact? worst = null;
            var worstSeverity = -1.0;
            var worstFraction = -1.0;
            long worstFreezeMaxAge = 0;
            var databases = 0;
            var graded = 0;

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                databases++;
                var databaseName = reader.IsDBNull(0) ? null : reader.GetString(0);
                var latestAt = reader.GetDateTime(1);
                var xidAge = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
                var multiAge = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
                var freezeMaxAge = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
                var multiFreezeMaxAge = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
                var xidsRemaining = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
                var multiRemaining = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
                var xidPeak = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
                var multiPeak = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));
                var firstXidAge = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
                var firstMultiAge = reader.IsDBNull(11) ? 0L : ToInt64(reader.GetValue(11));
                var firstSeenAt = reader.GetDateTime(12);
                var samples = ToInt64(reader.GetValue(13));

                /* #2689: "keeping up" = the latest reading is below the window's peak — the counter has come
                   down at least once inside the window. Latest == peak (including a one-sample window) reads
                   as NOT keeping up, the conservative default PostgresAlertInfo takes. */
                var xidKeepingUp = xidAge < xidPeak;
                var multiKeepingUp = multiAge < multiPeak;

                var xid = PgTargetScorer.GradeWraparoundCounter(xidAge, freezeMaxAge, xidKeepingUp);
                var multi = PgTargetScorer.GradeWraparoundCounter(multiAge, multiFreezeMaxAge, multiKeepingUp);
                var xidFraction = freezeMaxAge > 0 ? (double)xidAge / freezeMaxAge : 0.0;
                var multiFraction = multiFreezeMaxAge > 0 ? (double)multiAge / multiFreezeMaxAge : 0.0;

                var multiWins = multi.Severity > xid.Severity
                    || (multi.Severity == xid.Severity && multiFraction > xidFraction);
                var severity = multiWins ? multi.Severity : xid.Severity;
                var fraction = multiWins ? multiFraction : xidFraction;
                if (severity > 0) graded++;

                if (severity < worstSeverity || (severity == worstSeverity && fraction <= worstFraction))
                    continue;

                worstSeverity = severity;
                worstFraction = fraction;
                worstFreezeMaxAge = freezeMaxAge;

                var age = multiWins ? multiAge : xidAge;
                var firstAge = multiWins ? firstMultiAge : firstXidAge;
                var remaining = multiWins ? multiRemaining : xidsRemaining;
                var fact = new Fact
                {
                    Source = PgTargetSources.VacuumSource,
                    Key = PgTargetFactKeys.WraparoundTrend,
                    Value = age,
                    ServerId = context.ServerId,
                    DatabaseName = databaseName,
                    Metadata =
                    {
                        [PgTargetScorer.WraparoundXidAgeKey] = xidAge,
                        [PgTargetScorer.WraparoundMultiXactAgeKey] = multiAge,
                        [PgTargetScorer.WraparoundFreezeMaxAgeKey] = freezeMaxAge,
                        [PgTargetScorer.WraparoundMultiXactFreezeMaxAgeKey] = multiFreezeMaxAge,
                        [PgTargetScorer.WraparoundXidPeakKey] = xidPeak,
                        [PgTargetScorer.WraparoundMultiXactPeakKey] = multiPeak,
                        [PgTargetScorer.WraparoundXidKeepingUpKey] = xidKeepingUp ? 1 : 0,
                        [PgTargetScorer.WraparoundMultiXactKeepingUpKey] = multiKeepingUp ? 1 : 0,
                        [PgTargetScorer.WraparoundCounterIsMultiXactKey] = multiWins ? 1 : 0,
                        [PgTargetScorer.WraparoundXidsRemainingKey] = xidsRemaining,
                        [PgTargetScorer.WraparoundMultiXidsRemainingKey] = multiRemaining,
                        [PgTargetScorer.WraparoundArmKey] = multiWins ? multi.Arm : xid.Arm,
                        [PgTargetScorer.WraparoundSamplesKey] = samples,
                    },
                };

                /* Slope → time-to-wall: the worst counter's change across the window over the window's own
                   span. Positive slope → hours to the 2^31 wall from xids_remaining; zero or negative (autovacuum
                   clawed it back) → NOT computable, and the fact says so rather than carrying an infinity. */
                var spanHours = (latestAt - firstSeenAt).TotalHours;
                var slope = spanHours > 0 ? (age - firstAge) / spanHours : 0.0;
                fact.Metadata[PgTargetScorer.WraparoundSlopePerHourKey] = slope;
                if (slope > 0 && remaining > 0)
                {
                    fact.Metadata[PgTargetScorer.WraparoundHoursToWallKey] = remaining / slope;
                    fact.Metadata[PgTargetScorer.WraparoundTimeToWallComputableKey] = 1;
                }
                else
                {
                    fact.Metadata[PgTargetScorer.WraparoundTimeToWallComputableKey] = 0;
                }

                worst = fact;
            }

            if (worst is null) return 0;

            worst.Metadata[PgTargetScorer.WraparoundDatabasesKey] = databases;
            worst.Metadata[PgTargetScorer.WraparoundDatabasesGradedKey] = graded;
            facts.Add(worst);
            return worstFreezeMaxAge;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_wraparound_stats arrived in V63 — a pre-migration store raises 42P01, classified quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
            return 0;
        }
    }

    private async Task ReadXminHoldAsync(AnalysisContext context, List<Fact> facts, long freezeMaxAge)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetXminHoldSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PostgresOutagePredictorThresholds.XminAgeWarningThreshold);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            /* Zero rows is the HEALTHY state (an unheld horizon stores nothing) — no fact, never a zero. */
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var latestSource = reader.IsDBNull(0) ? null : reader.GetString(0);
            var latestHolder = reader.IsDBNull(1) ? null : reader.GetString(1);
            var xminAge = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var lastSeenAt = reader.GetDateTime(3);
            var observationsTotal = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var peakWinningAge = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var heldCaptures = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
            var runFloorAge = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7));
            var runPeakAge = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
            var distinctHolders = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));
            var backendShare = reader.IsDBNull(10) ? 0.0 : Convert.ToDouble(reader.GetValue(10), CultureInfo.InvariantCulture);
            var slotShare = reader.IsDBNull(11) ? 0.0 : Convert.ToDouble(reader.GetValue(11), CultureInfo.InvariantCulture);
            var standbyShare = reader.IsDBNull(12) ? 0.0 : Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture);
            var preparedShare = reader.IsDBNull(13) ? 0.0 : Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture);
            var modalHolderSource = reader.IsDBNull(14) ? null : reader.GetString(14);
            var modalHolder = reader.IsDBNull(15) ? null : reader.GetString(15);
            var modalHolderCaptures = reader.IsDBNull(16) ? 0L : ToInt64(reader.GetValue(16));
            var modalSource = reader.IsDBNull(17) ? null : reader.GetString(17);
            var modalSourceCaptures = reader.IsDBNull(18) ? 0L : ToInt64(reader.GetValue(18));

            /* Attribution is a SHARE of the held run, and a share under the dominance line names nobody. Five
               transactions taking turns is not "session:4242 held the horizon" however recently 4242 won: the
               fact says N holders alternated, the ObjectName stays empty, and the holder_source code stays 0 so
               every consumer that keys on a kind (the idle-in-transaction leaf, the slot-xmin amplifier, this
               family's own remedy arms) closes rather than picking one of five at random. */
            var modalHolderShare = heldCaptures > 0 ? (double)modalHolderCaptures / heldCaptures : 0.0;
            var modalSourceShare = heldCaptures > 0 ? (double)modalSourceCaptures / heldCaptures : 0.0;
            var holderAttributed = heldCaptures > 0 && modalHolderShare >= PgTargetScorer.XminHolderDominanceShare;
            var sourceAttributed = heldCaptures > 0 && modalSourceShare >= PgTargetScorer.XminHolderDominanceShare;

            /* No held run to attribute over (a transient hold, or a horizon that never reached the bar): the
               latest capture IS the whole story, so it supplies the subject, exactly as v1 did. */
            var noRun = heldCaptures == 0;
            var subjectSource = noRun ? latestSource : (sourceAttributed ? modalSource : null);
            var subjectHolder = noRun ? latestHolder : (holderAttributed ? modalHolder : null);
            var subjectHolderSource = noRun ? latestSource : modalHolderSource;
            var objectName = subjectHolder is { Length: > 0 }
                ? $"{subjectHolderSource}:{subjectHolder}"
                : subjectSource;

            var fact = new Fact
            {
                Source = PgTargetSources.VacuumSource,
                Key = PgTargetFactKeys.XminHold,
                Value = xminAge,
                ServerId = context.ServerId,
                /* source:holder, the evaluator's subject shape, so the story names the same thing the alert did —
                   but only when ONE holder dominated the held run. Under alternation there is no subject and the
                   advice says "N holders alternated" instead of naming the last one to win. */
                ObjectName = objectName,
                Metadata =
                {
                    [PgTargetScorer.XminAgeKey] = xminAge,
                    [PgTargetScorer.XminHolderSourceKey] = PgTargetAdvice.HolderSourceCode(subjectSource),
                    [PgTargetScorer.XminLatestHolderSourceKey] = PgTargetAdvice.HolderSourceCode(latestSource),
                    [PgTargetScorer.XminObservationsTotalKey] = observationsTotal,
                    [PgTargetScorer.XminHeldCapturesKey] = heldCaptures,
                    [PgTargetScorer.XminHeldFractionKey] = observationsTotal > 0 ? (double)heldCaptures / observationsTotal : 0.0,
                    [PgTargetScorer.XminHorizonFloorAgeKey] = runFloorAge,
                    [PgTargetScorer.XminHorizonRunPeakAgeKey] = runPeakAge,
                    [PgTargetScorer.XminPeakWinningAgeKey] = peakWinningAge,
                    [PgTargetScorer.XminDistinctHoldersKey] = distinctHolders,
                    [PgTargetScorer.XminWinnerBackendShareKey] = backendShare,
                    [PgTargetScorer.XminWinnerSlotShareKey] = slotShare,
                    [PgTargetScorer.XminWinnerStandbyShareKey] = standbyShare,
                    [PgTargetScorer.XminWinnerPreparedShareKey] = preparedShare,
                    [PgTargetScorer.XminModalHolderCapturesKey] = modalHolderCaptures,
                    [PgTargetScorer.XminModalHolderShareKey] = modalHolderShare,
                    [PgTargetScorer.XminDominantSourceShareKey] = modalSourceShare,
                    [PgTargetScorer.XminHolderAttributedKey] = holderAttributed ? 1 : 0,
                    [PgTargetScorer.XminMinutesSinceLastHolderKey] = (AsNaive(context.TimeRangeEnd) - lastSeenAt).TotalMinutes,
                },
            };
            if (freezeMaxAge > 0)
                fact.Metadata[PgTargetScorer.XminFreezeMaxAgeKey] = freezeMaxAge;

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_xmin_horizon arrived in V66 — a pre-migration store raises 42P01, classified quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
