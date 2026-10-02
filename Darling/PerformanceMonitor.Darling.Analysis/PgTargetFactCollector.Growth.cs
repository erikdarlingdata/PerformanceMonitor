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
    /// The database growth TREND read over <c>pg_database_size_stats</c> (V136): for every database with a sized
    /// sample in the lookback, the latest sample beside the earliest, ranked by growth of <c>size_bytes</c>, the top
    /// <c>$7</c> rows — each row also carrying the server-wide population counts and the instance total's own trend,
    /// and ONE row of counts with NULL database columns when no database qualified (the <c>LEFT JOIN</c> against the
    /// summary), so the caller can tell "nothing grew" from "nothing could be sized" from "no rows".
    ///
    /// <para><b>Sized</b> means <c>size_bytes IS NOT NULL</c>: the collector writes NULL where the monitoring role has
    /// neither CONNECT on the database nor <c>pg_read_all_stats</c> (the two tests <c>dbsize.c</c> makes), so a NULL
    /// is "could not measure", never zero. A database with no sized sample at all in the lookback is counted in
    /// <c>databases_unsized</c> and contributes nothing; a database whose sizing flipped inside the lookback
    /// contributes only its sized samples.</para>
    ///
    /// <para><b>The instance total is trended from <c>total_bytes</c>, never summed.</b> The collector denormalises
    /// the instance total onto every row and writes it NULL on EVERY row when any database is unsized — a sum over the
    /// databases this role can see is not the instance total (the R5 rule). The read takes one <c>total_bytes</c> per
    /// <c>collection_time</c> where it is not NULL, and the total's trend exists only when at least <c>$4</c> such
    /// samples exist; otherwise <c>total_samples</c> says how few and the fact says the total is unavailable.</para>
    ///
    /// <para><b>Trend, never spot.</b> The graded quantity is <c>growth_bytes</c> = latest − earliest size across the
    /// lookback, for databases with at least <c>$4</c> samples; <c>databases_over_line</c> applies the SAME two-arm
    /// line the scorer grades (<c>$5</c> bytes, <c>$6</c> fraction of the earliest size — an earliest zero passes the
    /// fraction arm, so the bytes arm decides) so the count and the grade cannot drift. Window functions run before
    /// the <c>LIMIT</c>, so the counts are over every qualifying database. Templates are ranked with the rest: a
    /// template that grows is exactly the kind of surprise worth a name.</para>
    ///
    /// <para><b>Span.</b> <c>$2</c>/<c>$3</c> are the LOOKBACK bounds — <see cref="PgTargetScorer.GrowthLookbackDays"/>
    /// before the window end, to the window end — not the pass window: the collector is hourly and a database grows in
    /// days, and the fact stamps the span it read.</para>
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> lookback (naive UTC), <c>$4</c> minimum samples, <c>$5</c> growth line
    /// bytes, <c>$6</c> growth line fraction, <c>$7</c> row limit.
    /// </summary>
    public const string PgTargetDatabaseGrowthSql = @"
WITH rows_in AS (
    SELECT database_name, collection_time, size_bytes, total_bytes
    FROM pg_database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
summary AS (
    SELECT
        COUNT(DISTINCT collection_time) AS samples_in_lookback,
        COUNT(DISTINCT database_name)   AS databases_seen,
        COUNT(DISTINCT database_name) - COUNT(DISTINCT database_name) FILTER (WHERE size_bytes IS NOT NULL) AS databases_unsized
    FROM rows_in
),
totals AS (
    SELECT DISTINCT ON (collection_time) collection_time, total_bytes
    FROM rows_in
    WHERE total_bytes IS NOT NULL
    ORDER BY collection_time
),
total_trend AS (
    SELECT
        COUNT(*)                                                     AS total_samples,
        MIN(collection_time)                                         AS total_first_at,
        (SELECT total_bytes FROM totals ORDER BY collection_time ASC  LIMIT 1) AS total_first_bytes,
        MAX(collection_time)                                         AS total_latest_at,
        (SELECT total_bytes FROM totals ORDER BY collection_time DESC LIMIT 1) AS total_latest_bytes
    FROM totals
),
sized AS (
    SELECT r.*, COUNT(*) OVER per_database AS samples
    FROM rows_in AS r
    WHERE r.size_bytes IS NOT NULL
    WINDOW per_database AS (PARTITION BY database_name)
),
latest AS (
    SELECT DISTINCT ON (database_name) database_name, collection_time, size_bytes, samples
    FROM sized
    ORDER BY database_name, collection_time DESC
),
earliest AS (
    SELECT DISTINCT ON (database_name) database_name, collection_time AS first_at, size_bytes AS first_bytes
    FROM sized
    ORDER BY database_name, collection_time ASC
),
graded AS (
    SELECT
        l.database_name,
        e.first_at,
        e.first_bytes,
        l.collection_time AS latest_at,
        l.size_bytes      AS latest_bytes,
        l.samples,
        l.size_bytes - e.first_bytes AS growth_bytes,
        COUNT(*) OVER () AS databases_considered,
        COUNT(*) FILTER (
            WHERE l.size_bytes - e.first_bytes >= $5
            AND   (l.size_bytes - e.first_bytes)::double precision >= e.first_bytes * $6
        ) OVER () AS databases_over_line
    FROM latest AS l
    JOIN earliest AS e ON e.database_name = l.database_name
    WHERE l.samples >= $4
),
ranked AS (
    SELECT *
    FROM graded
    ORDER BY growth_bytes DESC NULLS LAST, latest_bytes DESC, database_name
    LIMIT $7
)
SELECT
    s.samples_in_lookback,
    s.databases_seen,
    s.databases_unsized,
    t.total_samples,
    t.total_first_at,
    t.total_first_bytes,
    t.total_latest_at,
    t.total_latest_bytes,
    g.databases_considered,
    g.databases_over_line,
    g.database_name,
    g.first_at,
    g.first_bytes,
    g.latest_at,
    g.latest_bytes,
    g.samples,
    g.growth_bytes
FROM summary AS s
CROSS JOIN total_trend AS t
LEFT JOIN ranked AS g ON TRUE
ORDER BY g.growth_bytes DESC NULLS LAST, g.latest_bytes DESC, g.database_name";

    /// <summary>
    /// <c>PG_DATABASE_GROWTH</c> — the database whose size grew most across the lookback, with the top three by name
    /// and the instance total's own trend beside it — filled by lane 38 of #3691. One read behind its own degrade so a
    /// pre-V136 store costs only this fact. The growth-rate anomaly lives in <c>PgTargetAnomalyDetector.Growth.cs</c>
    /// over the same table.
    ///
    /// <para>Emission is evidence-gated: no rows in the lookback → no fact, never a zero. Rows exist → ONE fact, always,
    /// so <c>get_analysis_facts</c> shows the population (databases seen, unsized, with enough samples, over the line)
    /// even when nothing graded — and when nothing COULD grade, the fact says which honesty gate withheld it
    /// (<see cref="PgTargetScorer.GrowthUnavailableReasonKey"/>) rather than reading as "no growth".</para>
    ///
    /// <para>Every command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, and the lookback is <see cref="PgTargetScorer.GrowthLookbackDays"/> before the
    /// window END — a longer span than the pass window, deliberately, and stamped on the fact. Nothing here divides by
    /// <see cref="AnalysisContext.ObservedDurationMs"/>: growth is a LEVEL differenced across the database's own
    /// samples, and the fact carries its own sample count instead of borrowing the one-minute coverage fraction (which
    /// would be a lie about an hourly series).</para>
    /// </summary>
    private async partial Task CollectGrowthFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        /* filled by lane 38 of #3691 — the marker the collect-surface census pins by exact text. */
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetDatabaseGrowthSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd).AddDays(-PgTargetScorer.GrowthLookbackDays));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(PgTargetScorer.GrowthMinimumSamples);
            cmd.Parameters.AddWithValue(PgTargetScorer.GrowthConcerningBytes);
            cmd.Parameters.AddWithValue(PgTargetScorer.GrowthConcerningFraction);
            cmd.Parameters.AddWithValue(PgTargetScorer.GrowthTopDatabases);

            Fact? fact = null;
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var samplesInLookback = ToInt64(reader.GetValue(0));
                /* The summary CTE aggregates over an empty set into one row of zeros: no row in the lookback is "no
                   data", and no data is no fact — never a zero. */
                if (samplesInLookback == 0) return;

                var databasesSeen = ToInt64(reader.GetValue(1));
                var databasesUnsized = ToInt64(reader.GetValue(2));
                var totalSamples = ToInt64(reader.GetValue(3));
                var hasDatabase = !reader.IsDBNull(10);

                if (fact is null)
                {
                    fact = new Fact
                    {
                        Source = PgTargetSources.GrowthSource,
                        Key = PgTargetFactKeys.DatabaseGrowth,
                        ServerId = context.ServerId,
                        Metadata =
                        {
                            [PgTargetScorer.GrowthLookbackDaysKey] = PgTargetScorer.GrowthLookbackDays,
                            [PgTargetScorer.GrowthSamplesInLookbackKey] = samplesInLookback,
                            [PgTargetScorer.GrowthDatabasesSeenKey] = databasesSeen,
                            [PgTargetScorer.GrowthDatabasesUnsizedKey] = databasesUnsized,
                            [PgTargetScorer.GrowthDatabasesConsideredKey] = hasDatabase ? ToInt64(reader.GetValue(8)) : 0,
                            [PgTargetScorer.GrowthDatabasesOverLineKey] = hasDatabase ? ToInt64(reader.GetValue(9)) : 0,
                            [PgTargetScorer.GrowthTotalSamplesKey] = totalSamples,
                        },
                    };

                    /* The instance total's trend, where the collector could write one: at least the sample minimum of
                       non-NULL totals. Fewer → unavailable, and the advice says why (an unsized database NULLs it). */
                    if (totalSamples >= PgTargetScorer.GrowthMinimumSamples && !reader.IsDBNull(5) && !reader.IsDBNull(7))
                    {
                        var totalFirstAt = reader.GetDateTime(4);
                        var totalFirst = ToInt64(reader.GetValue(5));
                        var totalLatestAt = reader.GetDateTime(6);
                        var totalLatest = ToInt64(reader.GetValue(7));
                        var totalGrowth = totalLatest - totalFirst;
                        var totalSpanHours = (totalLatestAt - totalFirstAt).TotalHours;
                        fact.Metadata[PgTargetScorer.GrowthTotalAvailableKey] = 1;
                        fact.Metadata[PgTargetScorer.GrowthTotalBytesKey] = totalGrowth;
                        fact.Metadata[PgTargetScorer.GrowthTotalFirstBytesKey] = totalFirst;
                        fact.Metadata[PgTargetScorer.GrowthTotalLatestBytesKey] = totalLatest;
                        fact.Metadata[PgTargetScorer.GrowthTotalSpanHoursKey] = totalSpanHours;
                        if (totalFirst > 0)
                            fact.Metadata[PgTargetScorer.GrowthTotalPctKey] = 100.0 * totalGrowth / totalFirst;
                        StampSlope(fact, PgTargetScorer.GrowthTotalBytesPerDayKey, null, PgTargetScorer.GrowthTotalDaysToDoubleKey, totalGrowth, totalLatest, totalSpanHours, totalFirst);
                    }
                    else
                    {
                        fact.Metadata[PgTargetScorer.GrowthTotalAvailableKey] = 0;
                    }

                    if (!hasDatabase)
                    {
                        /* Nothing qualified. Which honesty gate withheld it is the whole content of the fact: every
                           database unsized (the role can size nothing), or sized databases without three samples yet.
                           The total alone cannot carry the fact — it has the same sample gate. */
                        fact.Metadata[PgTargetScorer.GrowthAvailableKey] = 0;
                        fact.Metadata[PgTargetScorer.GrowthUnavailableReasonKey] = databasesUnsized >= databasesSeen
                            ? PgTargetScorer.GrowthReasonAllUnsized
                            : PgTargetScorer.GrowthReasonInsufficientSamples;
                        break;
                    }

                    /* The first row is the worst database (the read's ORDER BY): it is the fact's subject. */
                    var databaseName = reader.GetString(10);
                    var firstAt = reader.GetDateTime(11);
                    var first = ToInt64(reader.GetValue(12));
                    var latestAt = reader.GetDateTime(13);
                    var latest = ToInt64(reader.GetValue(14));
                    var samples = ToInt64(reader.GetValue(15));
                    var growth = ToInt64(reader.GetValue(16));
                    var spanHours = (latestAt - firstAt).TotalHours;

                    fact.Value = growth;
                    fact.DatabaseName = databaseName;
                    fact.ObjectName = databaseName;
                    fact.Metadata[PgTargetScorer.GrowthAvailableKey] = 1;
                    fact.Metadata[PgTargetScorer.GrowthBytesKey] = growth;
                    fact.Metadata[PgTargetScorer.GrowthFirstBytesKey] = first;
                    fact.Metadata[PgTargetScorer.GrowthLatestBytesKey] = latest;
                    fact.Metadata[PgTargetScorer.GrowthSamplesKey] = samples;
                    fact.Metadata[PgTargetScorer.GrowthSpanHoursKey] = spanHours;
                    if (first > 0)
                    {
                        fact.Metadata[PgTargetScorer.GrowthPctKey] = 100.0 * growth / first;
                        fact.Metadata[PgTargetScorer.GrowthPctComputableKey] = 1;
                    }
                    else
                    {
                        fact.Metadata[PgTargetScorer.GrowthPctComputableKey] = 0;
                    }
                    StampSlope(fact, PgTargetScorer.GrowthBytesPerDayKey, PgTargetScorer.GrowthPctPerDayKey, PgTargetScorer.GrowthDaysToDoubleKey, growth, latest, spanHours, first);
                }

                /* Every returned row — the worst included — rides by name, so the advice can list the top three and
                   the graph can intersect on the database. The earliest-zero case carries no percentage. */
                var rowName = reader.GetString(10);
                var rowFirst = ToInt64(reader.GetValue(12));
                var rowGrowth = ToInt64(reader.GetValue(16));
                fact.Metadata[PgTargetScorer.GrowthNamedBytesPrefix + rowName] = rowGrowth;
                if (rowFirst > 0)
                    fact.Metadata[PgTargetScorer.GrowthNamedPctPrefix + rowName] = 100.0 * rowGrowth / rowFirst;
            }

            if (fact is not null)
                facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_database_size_stats arrived in V136 — a pre-migration store raises 42P01, classified quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// The slope and its straight-line extrapolation: bytes per day over the subject's own span (hours → days), the
    /// percentage per day where the earliest size lets one be computed, and days-to-double = latest size over bytes per
    /// day — a STRAIGHT LINE from the lookback's two ends, stated as such by the advice. Nothing is stamped for a span
    /// of zero (one distinct timestamp) or a non-positive slope (a database that shrank or held still has no doubling
    /// time, and the absence is the honest value).
    /// </summary>
    private static void StampSlope(Fact fact, string perDayKey, string? pctPerDayKey, string doublingKey, long growth, long latest, double spanHours, long first)
    {
        if (spanHours <= 0) return;
        var spanDays = spanHours / 24.0;
        var perDay = growth / spanDays;
        fact.Metadata[perDayKey] = perDay;
        if (pctPerDayKey is not null && first > 0)
            fact.Metadata[pctPerDayKey] = 100.0 * growth / first / spanDays;
        if (perDay > 0 && latest > 0)
            fact.Metadata[doublingKey] = latest / perDay;
    }
}
