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
    /// The table bloat TREND read over <c>pg_table_bloat_stats</c>: for every table with a usable estimate in
    /// the lookback, the latest sample beside the earliest, ranked by growth of <c>bloat_bytes_estimate</c>,
    /// the top <c>$7</c> rows — each row also carrying the server-wide population counts, and ONE row of
    /// population counts with NULL table columns when no table qualified (the <c>LEFT JOIN</c> against the
    /// summary), so the caller can tell "nothing grew" from "nothing could be estimated" from "no rows".
    ///
    /// <para><b>Usable</b> means <c>estimate_unavailable</c> is FALSE — and a NULL flag reads as unavailable,
    /// never as available: <c>PgMigrations.V85</c> records the estimator reporting 88.59 % against a true
    /// 0.50 % when the monitoring role could not see <c>pg_stats</c>, with the flag as the only witness. A
    /// table whose flag flipped inside the lookback contributes only its usable samples to the trend.</para>
    ///
    /// <para><b>Trend, never spot.</b> The graded quantity is <c>growth_bytes</c> = latest − earliest estimate
    /// across the lookback, and only for tables whose LATEST <c>heap_bytes</c> is at or over the size floor
    /// (<c>$4</c> = <see cref="PgTargetScorer.BloatSizeFloorBytes"/>); the spot percentage is carried for the
    /// reader but never ranked or graded. <c>tables_over_line</c> applies the SAME two-arm line the scorer
    /// grades (<c>$5</c> bytes, <c>$6</c> fraction of the earlier estimate — an earlier zero passes the
    /// fraction arm, so the bytes arm decides) so the count and the grade cannot drift. Window functions
    /// run before the <c>LIMIT</c>, so the counts are over every qualifying table.</para>
    ///
    /// <para><b>Span.</b> <c>$2</c>/<c>$3</c> are the LOOKBACK bounds — <see cref="PgTargetScorer.BloatLookbackDays"/>
    /// before the window end, to the window end — not the pass window; the collector is hourly and bloat moves
    /// in days, and the fact stamps the span it read.</para>
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> lookback (naive UTC), <c>$4</c> heap floor bytes, <c>$5</c> growth
    /// line bytes, <c>$6</c> growth line fraction, <c>$7</c> row limit.
    /// </summary>
    public const string PgTargetTableBloatTrendSql = @"
WITH rows_in AS (
    SELECT
        database_name, schema_name, table_name, collection_time,
        heap_bytes, bloat_bytes_estimate, bloat_pct_estimate, dead_tuples, live_tuples,
        COALESCE(estimate_unavailable, TRUE) AS unavailable
    FROM pg_table_bloat_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
summary AS (
    SELECT
        COUNT(DISTINCT (database_name, schema_name, table_name)) AS tables_seen,
        COUNT(DISTINCT (database_name, schema_name, table_name)) FILTER (WHERE NOT unavailable) AS tables_estimable,
        COUNT(DISTINCT collection_time) AS samples_in_lookback
    FROM rows_in
),
usable AS (
    SELECT
        r.*,
        COUNT(*) OVER per_table AS samples
    FROM rows_in AS r
    WHERE NOT r.unavailable
    AND   r.bloat_bytes_estimate IS NOT NULL
    WINDOW per_table AS (PARTITION BY database_name, schema_name, table_name)
),
latest AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name) *
    FROM usable
    ORDER BY database_name, schema_name, table_name, collection_time DESC
),
earliest AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name)
        database_name, schema_name, table_name,
        collection_time      AS first_at,
        bloat_bytes_estimate AS first_bloat_bytes
    FROM usable
    ORDER BY database_name, schema_name, table_name, collection_time ASC
),
graded AS (
    SELECT
        l.database_name,
        l.schema_name,
        l.table_name,
        l.collection_time,
        e.first_at,
        l.heap_bytes,
        e.first_bloat_bytes,
        l.bloat_bytes_estimate AS latest_bloat_bytes,
        l.bloat_pct_estimate,
        l.dead_tuples,
        l.live_tuples,
        l.samples,
        l.bloat_bytes_estimate - e.first_bloat_bytes AS growth_bytes,
        COUNT(*) OVER () AS tables_considered,
        COUNT(*) FILTER (
            WHERE l.bloat_bytes_estimate - e.first_bloat_bytes >= $5
            AND   (l.bloat_bytes_estimate - e.first_bloat_bytes)::double precision >= e.first_bloat_bytes * $6
        ) OVER () AS tables_over_line
    FROM latest AS l
    JOIN earliest AS e
      ON  e.database_name IS NOT DISTINCT FROM l.database_name
      AND e.schema_name   IS NOT DISTINCT FROM l.schema_name
      AND e.table_name    IS NOT DISTINCT FROM l.table_name
    WHERE l.heap_bytes >= $4
    AND   l.samples >= 2
),
ranked AS (
    SELECT *
    FROM graded
    ORDER BY growth_bytes DESC NULLS LAST, heap_bytes DESC
    LIMIT $7
)
SELECT
    s.tables_seen,
    s.tables_estimable,
    s.samples_in_lookback,
    t.tables_considered,
    t.tables_over_line,
    t.database_name,
    t.schema_name,
    t.table_name,
    t.collection_time,
    t.first_at,
    t.heap_bytes,
    t.first_bloat_bytes,
    t.latest_bloat_bytes,
    t.bloat_pct_estimate,
    t.dead_tuples,
    t.live_tuples,
    t.samples,
    t.growth_bytes
FROM summary AS s
LEFT JOIN ranked AS t ON TRUE
ORDER BY t.growth_bytes DESC NULLS LAST, t.heap_bytes DESC";

    /// <summary>
    /// The index bloat TREND read over <c>pg_index_bloat</c>, the daily sibling of
    /// <see cref="PgTargetTableBloatTrendSql"/>: usable means <c>skipped_reason IS NULL</c> (the collector
    /// writes <c>est_bloat_pct</c> / <c>est_reclaimable_bytes</c> NULL under exactly that condition, so a
    /// suppressed estimate can never read as zero bloat — <c>PgMigrations.V114</c>), the graded quantity is
    /// growth of <c>est_reclaimable_bytes</c> (the figure the reads rank on, #2561 — bytes, never a percentage),
    /// the size floor is on the latest <c>index_bytes</c>, and an index needs <c>$8</c> samples in the lookback
    /// (the daily cadence gives at most fourteen) before it is ranked at all.
    ///
    /// <para>The population columns add what the index table alone can say: how many indexes' LATEST row was
    /// skipped (<c>indexes_skipped</c>, for the skipped share), how many DISTINCT reasons those carried
    /// (counts, never the prose — <c>get_pg_index_bloat</c> shows the reasons), how many estimable indexes
    /// over the floor exist at all (<c>indexes_over_floor</c>, so "insufficient samples" can be told from
    /// "nothing over the floor"), and whether any row saw the pgstattuple extension installed — the exact
    /// route the skip reasons point at.</para>
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> lookback (naive UTC), <c>$4</c> index floor bytes, <c>$5</c> growth
    /// line bytes, <c>$6</c> growth line fraction, <c>$7</c> row limit, <c>$8</c> minimum samples.
    /// </summary>
    public const string PgTargetIndexBloatTrendSql = @"
WITH rows_in AS (
    SELECT
        database_name, schema_name, table_name, index_name, collection_time,
        index_bytes, est_bloat_pct, est_reclaimable_bytes, skipped_reason,
        COALESCE(pgstattuple_available, FALSE) AS pgstattuple_available
    FROM pg_index_bloat
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
latest_any AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name, index_name) *
    FROM rows_in
    ORDER BY database_name, schema_name, table_name, index_name, collection_time DESC
),
summary AS (
    SELECT
        (SELECT COUNT(*) FROM latest_any) AS indexes_seen,
        (SELECT COUNT(*) FROM latest_any WHERE skipped_reason IS NOT NULL) AS indexes_skipped,
        (SELECT COUNT(DISTINCT skipped_reason) FROM latest_any WHERE skipped_reason IS NOT NULL) AS skip_reasons,
        (SELECT COUNT(DISTINCT (database_name, schema_name, table_name, index_name)) FROM rows_in WHERE skipped_reason IS NULL) AS indexes_estimable,
        (SELECT COUNT(DISTINCT collection_time) FROM rows_in) AS samples_in_lookback,
        (SELECT BOOL_OR(pgstattuple_available) FROM rows_in) AS pgstattuple_available
),
usable AS (
    SELECT
        r.*,
        COUNT(*) OVER per_index AS samples
    FROM rows_in AS r
    WHERE r.skipped_reason IS NULL
    AND   r.est_reclaimable_bytes IS NOT NULL
    WINDOW per_index AS (PARTITION BY database_name, schema_name, table_name, index_name)
),
latest AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name, index_name) *
    FROM usable
    ORDER BY database_name, schema_name, table_name, index_name, collection_time DESC
),
earliest AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name, index_name)
        database_name, schema_name, table_name, index_name,
        collection_time       AS first_at,
        est_reclaimable_bytes AS first_reclaimable_bytes
    FROM usable
    ORDER BY database_name, schema_name, table_name, index_name, collection_time ASC
),
over_floor AS (
    SELECT
        l.database_name,
        l.schema_name,
        l.table_name,
        l.index_name,
        l.collection_time,
        e.first_at,
        l.index_bytes,
        e.first_reclaimable_bytes,
        l.est_reclaimable_bytes AS latest_reclaimable_bytes,
        l.est_bloat_pct,
        l.samples,
        l.est_reclaimable_bytes - e.first_reclaimable_bytes AS growth_bytes,
        COUNT(*) OVER () AS indexes_over_floor
    FROM latest AS l
    JOIN earliest AS e
      ON  e.database_name IS NOT DISTINCT FROM l.database_name
      AND e.schema_name   IS NOT DISTINCT FROM l.schema_name
      AND e.table_name    IS NOT DISTINCT FROM l.table_name
      AND e.index_name    IS NOT DISTINCT FROM l.index_name
    WHERE l.index_bytes >= $4
),
graded AS (
    SELECT
        o.*,
        COUNT(*) OVER () AS indexes_considered,
        COUNT(*) FILTER (
            WHERE o.growth_bytes >= $5
            AND   o.growth_bytes::double precision >= o.first_reclaimable_bytes * $6
        ) OVER () AS indexes_over_line
    FROM over_floor AS o
    WHERE o.samples >= $8
),
ranked AS (
    SELECT *
    FROM graded
    ORDER BY growth_bytes DESC NULLS LAST, index_bytes DESC
    LIMIT $7
)
SELECT
    s.indexes_seen,
    s.indexes_skipped,
    s.skip_reasons,
    s.indexes_estimable,
    s.samples_in_lookback,
    s.pgstattuple_available,
    (SELECT MAX(indexes_over_floor) FROM over_floor) AS indexes_over_floor,
    t.indexes_considered,
    t.indexes_over_line,
    t.database_name,
    t.schema_name,
    t.table_name,
    t.index_name,
    t.collection_time,
    t.first_at,
    t.index_bytes,
    t.first_reclaimable_bytes,
    t.latest_reclaimable_bytes,
    t.est_bloat_pct,
    t.samples,
    t.growth_bytes
FROM summary AS s
LEFT JOIN ranked AS t ON TRUE
ORDER BY t.growth_bytes DESC NULLS LAST, t.index_bytes DESC";

    /// <summary>
    /// <c>PG_BLOAT_TREND</c> (the table whose bloat-bytes estimate grew most across the lookback, with the top
    /// three by name) and <c>PG_INDEX_BLOAT_TREND</c> (the same over the daily index estimate) — filled by lane
    /// 13 of #3691. Two reads, each behind its own degrade so a missing table (a pre-V85 / pre-V94 store)
    /// costs only its own fact. No anomaly detector: the trend is computed in-window against the object's own
    /// earlier value, and hourly / daily cadences are far too sparse for hour-of-week buckets (a bucket would
    /// hold two samples a fortnight).
    ///
    /// <para>Emission is evidence-gated: no rows in the lookback → no fact, never a zero. Rows exist → ONE fact
    /// per key, always, so <c>get_analysis_facts</c> shows the population (how many objects were seen, how
    /// many could be estimated, how many passed the floor, how many crossed the line) even when nothing
    /// graded — and when nothing COULD grade, the fact says which of the honesty gates withheld it
    /// (<see cref="PgTargetScorer.BloatUnavailableReasonKey"/>) rather than reading as "no bloat".</para>
    ///
    /// <para>Every command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, and the lookback is <see cref="PgTargetScorer.BloatLookbackDays"/>
    /// before the window END — a longer span than the pass window, deliberately, and stamped on the fact.
    /// Nothing here divides by <see cref="AnalysisContext.ObservedDurationMs"/>: growth is a LEVEL differenced
    /// across the object's own samples, and the fact carries its own sample count instead of borrowing the
    /// one-minute coverage fraction (which would be a lie about an hourly series).</para>
    /// </summary>
    private async partial Task CollectBloatFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        /* filled by lane 13 of #3691 — the marker the collect-surface census pins by exact text. */
        await ReadTableBloatTrendAsync(context, facts);
        await ReadIndexBloatTrendAsync(context, facts);
    }

    private static DateTime LookbackStart(AnalysisContext context) =>
        AsNaive(context.TimeRangeEnd).AddDays(-PgTargetScorer.BloatLookbackDays);

    private async Task ReadTableBloatTrendAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetTableBloatTrendSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(LookbackStart(context));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatSizeFloorBytes);
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatGrowthConcerningBytes);
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatGrowthConcerningFraction);
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatTopObjects);

            Fact? fact = null;
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var tablesSeen = ToInt64(reader.GetValue(0));
                /* The summary CTE aggregates over an empty set into one row of zeros: no table in the lookback is
                   "no data", and no data is no fact — never a zero. */
                if (tablesSeen == 0) return;

                var tablesEstimable = ToInt64(reader.GetValue(1));
                var samplesInLookback = ToInt64(reader.GetValue(2));
                var hasTable = !reader.IsDBNull(8);

                if (fact is null)
                {
                    fact = new Fact
                    {
                        Source = PgTargetSources.BloatSource,
                        Key = PgTargetFactKeys.BloatTrend,
                        ServerId = context.ServerId,
                        Metadata =
                        {
                            [PgTargetScorer.BloatLookbackDaysKey] = PgTargetScorer.BloatLookbackDays,
                            [PgTargetScorer.BloatSamplesInLookbackKey] = samplesInLookback,
                            [PgTargetScorer.BloatObjectsSeenKey] = tablesSeen,
                            [PgTargetScorer.BloatObjectsEstimableKey] = tablesEstimable,
                            [PgTargetScorer.BloatObjectsConsideredKey] = hasTable ? ToInt64(reader.GetValue(3)) : 0,
                            [PgTargetScorer.BloatObjectsOverLineKey] = hasTable ? ToInt64(reader.GetValue(4)) : 0,
                        },
                    };

                    if (!hasTable)
                    {
                        /* Nothing qualified. Which honesty gate withheld it is the whole content of the fact: every
                           row unavailable (the #2542 grant, or never analysed) versus estimable tables all under
                           the size floor / with a single sample. */
                        fact.Metadata[PgTargetScorer.BloatEstimateAvailableKey] = 0;
                        fact.Metadata[PgTargetScorer.BloatUnavailableReasonKey] = tablesEstimable == 0
                            ? PgTargetScorer.BloatReasonEstimateUnavailable
                            : PgTargetScorer.BloatReasonBelowSizeFloor;
                        break;
                    }

                    /* The first row is the worst table (the read's ORDER BY): it is the fact's subject. */
                    var databaseName = reader.IsDBNull(5) ? null : reader.GetString(5);
                    var schema = reader.IsDBNull(6) ? null : reader.GetString(6);
                    var table = reader.IsDBNull(7) ? null : reader.GetString(7);
                    var latestAt = reader.GetDateTime(8);
                    var firstAt = reader.GetDateTime(9);
                    var heapBytes = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
                    var firstBloat = reader.IsDBNull(11) ? 0L : ToInt64(reader.GetValue(11));
                    var latestBloat = reader.IsDBNull(12) ? 0L : ToInt64(reader.GetValue(12));
                    var samples = ToInt64(reader.GetValue(16));
                    var growth = reader.IsDBNull(17) ? 0L : ToInt64(reader.GetValue(17));

                    fact.Value = growth;
                    fact.DatabaseName = databaseName;
                    fact.ObjectName = TableObjectName(schema, table);
                    fact.Metadata[PgTargetScorer.BloatEstimateAvailableKey] = 1;
                    fact.Metadata[PgTargetScorer.BloatGrowthBytesKey] = growth;
                    fact.Metadata[PgTargetScorer.BloatEarlierBytesKey] = firstBloat;
                    fact.Metadata[PgTargetScorer.BloatLatestBytesKey] = latestBloat;
                    fact.Metadata[PgTargetScorer.BloatObjectBytesKey] = heapBytes;
                    fact.Metadata[PgTargetScorer.BloatSamplesKey] = samples;
                    fact.Metadata[PgTargetScorer.BloatSpanHoursKey] = (latestAt - firstAt).TotalHours;
                    if (!reader.IsDBNull(13))
                        fact.Metadata[PgTargetScorer.BloatSpotPctKey] = Convert.ToDouble(reader.GetValue(13));
                    if (!reader.IsDBNull(14))
                        fact.Metadata[PgTargetScorer.BloatDeadTuplesKey] = ToInt64(reader.GetValue(14));
                    if (!reader.IsDBNull(15))
                        fact.Metadata[PgTargetScorer.BloatLiveTuplesKey] = ToInt64(reader.GetValue(15));
                    StampGrowthPct(fact, growth, firstBloat);
                }

                /* Every returned row — the worst included, as Ranked[0] — rides by name in the typed list, so the
                   advice can list the top three and the graph can intersect on schema.table. Until #3691 lane 43
                   these rode as growth_bytes_<schema.table> / growth_pct_<…> / dead_tuples_<…> metadata keys: the
                   NAME inside the key, and BloatNamedObjects parsing it back out with a string prefix test. The
                   figures keep the un-prefixed names they had, so a reader who knows the fact's metadata knows
                   these. The earlier-zero case carries no percentage — a quarter of nothing is not a fraction —
                   and a row with no dead-tuple reading carries no dead-tuple figure, which is the same honesty
                   the absent key used to express. */
                var rowDatabase = reader.IsDBNull(5) ? null : reader.GetString(5);
                var rowName = TableObjectName(reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7));
                var rowFirst = reader.IsDBNull(11) ? 0L : ToInt64(reader.GetValue(11));
                var rowGrowth = reader.IsDBNull(17) ? 0L : ToInt64(reader.GetValue(17));
                var figures = new Dictionary<string, double>(StringComparer.Ordinal)
                {
                    [PgTargetScorer.BloatGrowthBytesKey] = rowGrowth,
                };
                if (rowFirst > 0)
                    figures[PgTargetScorer.BloatGrowthPctKey] = 100.0 * rowGrowth / rowFirst;
                if (!reader.IsDBNull(14))
                    figures[PgTargetScorer.BloatDeadTuplesKey] = ToInt64(reader.GetValue(14));
                fact.Ranked.Add(new RankedObject(rowName, rowDatabase, rowGrowth, figures));
            }

            if (fact is not null)
                facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_table_bloat_stats arrived in V85 — a pre-migration store raises 42P01, classified quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    private async Task ReadIndexBloatTrendAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetIndexBloatTrendSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(LookbackStart(context));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatSizeFloorBytes);
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatGrowthConcerningBytes);
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatGrowthConcerningFraction);
            cmd.Parameters.AddWithValue(PgTargetScorer.BloatTopObjects);
            cmd.Parameters.AddWithValue(PgTargetScorer.IndexBloatMinimumSamples);

            Fact? fact = null;
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var indexesSeen = ToInt64(reader.GetValue(0));
                if (indexesSeen == 0) return;

                var indexesSkipped = ToInt64(reader.GetValue(1));
                var skipReasons = ToInt64(reader.GetValue(2));
                var indexesEstimable = ToInt64(reader.GetValue(3));
                var samplesInLookback = ToInt64(reader.GetValue(4));
                var pgstattuple = !reader.IsDBNull(5) && reader.GetBoolean(5);
                var indexesOverFloor = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
                var hasIndex = !reader.IsDBNull(13);

                if (fact is null)
                {
                    fact = new Fact
                    {
                        Source = PgTargetSources.BloatSource,
                        Key = PgTargetFactKeys.IndexBloatTrend,
                        ServerId = context.ServerId,
                        Metadata =
                        {
                            [PgTargetScorer.BloatLookbackDaysKey] = PgTargetScorer.BloatLookbackDays,
                            [PgTargetScorer.BloatSamplesInLookbackKey] = samplesInLookback,
                            [PgTargetScorer.BloatObjectsSeenKey] = indexesSeen,
                            [PgTargetScorer.BloatObjectsEstimableKey] = indexesEstimable,
                            [PgTargetScorer.BloatObjectsConsideredKey] = hasIndex ? ToInt64(reader.GetValue(7)) : 0,
                            [PgTargetScorer.BloatObjectsOverLineKey] = hasIndex ? ToInt64(reader.GetValue(8)) : 0,
                            [PgTargetScorer.IndexBloatSkippedShareKey] = (double)indexesSkipped / indexesSeen,
                            [PgTargetScorer.IndexBloatSkipReasonsKey] = skipReasons,
                            [PgTargetScorer.IndexBloatPgstattupleAvailableKey] = pgstattuple ? 1 : 0,
                        },
                    };

                    if (!hasIndex)
                    {
                        /* Which gate withheld it, most fundamental first: nothing estimable (and whether the exact
                           route is open), then nothing over the floor, then over the floor but not yet three samples. */
                        fact.Metadata[PgTargetScorer.BloatEstimateAvailableKey] = 0;
                        fact.Metadata[PgTargetScorer.BloatUnavailableReasonKey] =
                            indexesEstimable == 0 ? (pgstattuple ? PgTargetScorer.BloatReasonAllSkipped : PgTargetScorer.BloatReasonPgstattupleUnavailable)
                            : indexesOverFloor == 0 ? PgTargetScorer.BloatReasonBelowSizeFloor
                            : PgTargetScorer.BloatReasonInsufficientSamples;
                        break;
                    }

                    var databaseName = reader.IsDBNull(9) ? null : reader.GetString(9);
                    var schema = reader.IsDBNull(10) ? null : reader.GetString(10);
                    var table = reader.IsDBNull(11) ? null : reader.GetString(11);
                    var index = reader.IsDBNull(12) ? null : reader.GetString(12);
                    var latestAt = reader.GetDateTime(13);
                    var firstAt = reader.GetDateTime(14);
                    var indexBytes = reader.IsDBNull(15) ? 0L : ToInt64(reader.GetValue(15));
                    var firstReclaimable = reader.IsDBNull(16) ? 0L : ToInt64(reader.GetValue(16));
                    var latestReclaimable = reader.IsDBNull(17) ? 0L : ToInt64(reader.GetValue(17));
                    var samples = ToInt64(reader.GetValue(19));
                    var growth = reader.IsDBNull(20) ? 0L : ToInt64(reader.GetValue(20));

                    fact.Value = growth;
                    fact.DatabaseName = databaseName;
                    fact.ObjectName = IndexObjectName(schema, table, index);
                    fact.Metadata[PgTargetScorer.BloatEstimateAvailableKey] = 1;
                    fact.Metadata[PgTargetScorer.BloatGrowthBytesKey] = growth;
                    fact.Metadata[PgTargetScorer.BloatEarlierBytesKey] = firstReclaimable;
                    fact.Metadata[PgTargetScorer.BloatLatestBytesKey] = latestReclaimable;
                    fact.Metadata[PgTargetScorer.BloatObjectBytesKey] = indexBytes;
                    fact.Metadata[PgTargetScorer.BloatSamplesKey] = samples;
                    fact.Metadata[PgTargetScorer.BloatSpanHoursKey] = (latestAt - firstAt).TotalHours;
                    if (!reader.IsDBNull(18))
                        fact.Metadata[PgTargetScorer.BloatSpotPctKey] = Convert.ToDouble(reader.GetValue(18));
                    StampGrowthPct(fact, growth, firstReclaimable);
                }

                /* The index twin of the table read's ranked rows — same shape, same figure names, three-part
                   names (schema.table.index) so the graph can still recover the parent table. No dead-tuple
                   figure: dead tuples are the heap's, and this fact is about an index's reclaimable bytes. */
                var rowDatabase = reader.IsDBNull(9) ? null : reader.GetString(9);
                var rowName = IndexObjectName(
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11),
                    reader.IsDBNull(12) ? null : reader.GetString(12));
                var rowFirst = reader.IsDBNull(16) ? 0L : ToInt64(reader.GetValue(16));
                var rowGrowth = reader.IsDBNull(20) ? 0L : ToInt64(reader.GetValue(20));
                var figures = new Dictionary<string, double>(StringComparer.Ordinal)
                {
                    [PgTargetScorer.BloatGrowthBytesKey] = rowGrowth,
                };
                if (rowFirst > 0)
                    figures[PgTargetScorer.BloatGrowthPctKey] = 100.0 * rowGrowth / rowFirst;
                fact.Ranked.Add(new RankedObject(rowName, rowDatabase, rowGrowth, figures));
            }

            if (fact is not null)
                facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_index_bloat arrived in V94 (its estimate columns in V114) — a pre-migration store raises 42P01 /
               42703, both classified quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// <c>schema.table.index</c> — three parts so the graph can recover the parent table from the index
    /// fact's name (<see cref="PgTargetScorer.BloatNamedObjects"/> with <c>parentTables</c>). A missing schema
    /// or table collapses to what is known rather than leaving a dangling dot.
    /// </summary>
    private static string IndexObjectName(string? schema, string? table, string? index)
    {
        var parent = TableObjectName(schema, table);
        var indexName = string.IsNullOrEmpty(index) ? "?" : index;
        return string.IsNullOrEmpty(parent) ? indexName : $"{parent}.{indexName}";
    }

    /// <summary><c>schema.table</c>, or the table alone when the schema is not stored, or <c>?</c> when neither
    /// is (the columns are nullable in the store; the collector always writes both).</summary>
    private static string TableObjectName(string? schema, string? table)
    {
        if (string.IsNullOrEmpty(table)) return "?";
        return string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";
    }

    /// <summary>The percentage arm, with its "not computable" branch: an earlier estimate of zero has no
    /// percentage (the bytes line decides alone, and the scorer's fraction arm passes trivially).</summary>
    private static void StampGrowthPct(Fact fact, long growth, long earlier)
    {
        if (earlier > 0)
        {
            fact.Metadata[PgTargetScorer.BloatGrowthPctKey] = 100.0 * growth / earlier;
            fact.Metadata[PgTargetScorer.BloatGrowthPctComputableKey] = 1;
        }
        else
        {
            fact.Metadata[PgTargetScorer.BloatGrowthPctComputableKey] = 0;
        }
    }
}
