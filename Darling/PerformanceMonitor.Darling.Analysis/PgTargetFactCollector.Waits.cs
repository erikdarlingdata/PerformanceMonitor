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
    /// The Aurora wait profile: <c>pg_wait_stats</c> (one-minute cadence, <c>aurora_stat_system_waits()</c>) —
    /// the engine's own cumulative wait counters, differenced AT COLLECT into <c>delta_wait_time_us</c> /
    /// <c>delta_waits</c> (the SQL Server <c>wait_stats</c> model), summed per (type, event) over the
    /// collections the window can honestly count. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window. One row
    /// per (type, event) with the window header (observed seconds, sample count, restart count) repeated on
    /// each; the type rollups and the named standouts are grouped in C#, because the grouping key is the
    /// case-INSENSITIVE name and the display name is a constant, not a column.
    ///
    /// <para><b>The interval is the three-state idiom, and a restart collection is not a sample.</b> V128
    /// gave this table <c>sample_interval_seconds</c>; before it the collector stored deltas naked, so a
    /// first-sighting / reset / gap-re-baseline row's <c>delta = 0</c> was indistinguishable from a measured
    /// idle minute. Per collection the interval is: the STORED interval (<c>MAX</c> over the collection's
    /// rows — a wait type first seen in an otherwise steady pass carries 0 beside its siblings' real
    /// interval; <c>MAX</c> is 0 only when EVERY row was unknowable, i.e. a restart) mapped through
    /// <c>NULLIF(…, 0)</c> so a restart collection contributes NO time and NO deltas; a pre-V128 collection
    /// (stored NULL) falls back to <c>LAG(collection_time)</c>, the derivation every reader of this table
    /// used before the column existed. Never <c>ELSE 0</c>. This is <c>PgAnomalyDetector.WaitRateWindowSql</c>'s
    /// shape, not re-derived. The first in-window collection under the LAG fallback has no predecessor and
    /// contributes nothing, exactly as the coverage witness treats its first row.</para>
    ///
    /// <para><b>The denominator is the wait source's OWN observed time.</b> The pass's coverage witness is
    /// <c>pg_database_stats</c> (D3), and its observed time is what <see cref="AnalysisContext.ObservedDurationMs"/>
    /// carries; but a fraction of observed time is only honest against the time the numerator was summed
    /// over, and on this flavour that is the sum of the intervals the written rows accrued across
    /// (<c>observed_sec</c> here). The two agree on a steady Aurora target (both one-minute) and diverge on a
    /// gap in either series; the fact carries both and the advice states which the fraction is over. A
    /// collection with nothing to write (every delta zero — an idle cluster, after the collector's own skip)
    /// adds no interval, so the denominator is the time in which something waited; on a quiet cluster the
    /// fractions this overstates are the ones nearest zero.</para>
    ///
    /// <para><b>Sums are of what was observed.</b> <c>GREATEST(delta, 0)</c> so a rewound counter adds nothing;
    /// rows whose collection has no usable interval are excluded by the <c>JOIN</c>, not just their time —
    /// a restart row's delta is 0 by construction, but the join makes the rule structural. <c>CPU</c> is a
    /// wait TYPE on Aurora (the engine reports on-CPU time under the same function) and rides through here
    /// so the C# share denominator can exclude it by name; <c>Activity</c> / <c>Client</c> / <c>Timeout</c>
    /// never reach the table (<c>PgWaitStatsCollector.IgnoredWaitTypes</c>). An undecodable type (NULL name,
    /// deliberately kept by the collector) cannot be keyed and is left out of the profile here.</para>
    ///
    /// <para><b>Always at least one row.</b> The header aggregate has no <c>GROUP BY</c>, so it is one row even
    /// over an empty window, and the events are <c>LEFT JOIN</c>ed onto it: a window with no rows at all
    /// (<c>collection_count = 0</c> — this flavour does not write the table, the fallback case) is
    /// distinguishable from a window with rows but no countable interval (<c>collection_count &gt; 0</c>,
    /// <c>observed_sec = 0</c> — every collection a restart, which emits nothing and does NOT fall back).
    /// A <c>CROSS JOIN</c> would have returned zero rows for both and blinded the routing.</para>
    ///
    /// <para><b>The header also counts the OTHER source's collections (lane 24, #3691).</b> <c>sampled_collections</c>
    /// is <c>pg_wait_sampling</c>'s collection count over the same window, so the one read that decides the
    /// routing also sees whether both sources wrote: a server with Aurora deltas AND sampler rows in one window
    /// (a re-platformed target, or a sampler left running beside the engine's counters) is the adversarial pass's
    /// "both non-empty" case lane 5 could not detect. The EXACT source wins — the sampled facts are not emitted and
    /// the sampled anomaly sits out — and every exact fact says so (<c>sampled_suppressed_by_exact = 1</c>), so
    /// the suppression is a stated choice on the fact and not a silent one in the code. A scalar subquery over the
    /// hypertable's (server_id, collection_time) index, so an Aurora pass pays one index probe for the knowledge.</para>
    /// </summary>
    public const string PgWaitStatsSql = @"
WITH per_collection AS (
    SELECT
        collection_time,
        MAX(sample_interval_seconds) AS stored_interval,
        CASE WHEN MAX(sample_interval_seconds) IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
             ELSE NULLIF(MAX(sample_interval_seconds), 0)
        END AS interval_sec
    FROM pg_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
),
observed AS (
    SELECT
        coalesce(SUM(interval_sec) FILTER (WHERE interval_sec > 0), 0) AS observed_sec,
        CAST(count(*) FILTER (WHERE interval_sec > 0) AS integer)      AS sample_count,
        CAST(count(*) AS integer)                                       AS collection_count,
        CAST(count(*) FILTER (WHERE stored_interval = 0) AS integer)   AS restart_collections,
        (SELECT CAST(count(DISTINCT s.collection_time) AS integer)
         FROM pg_wait_sampling AS s
         WHERE s.server_id = $1
         AND   s.collection_time >= $2
         AND   s.collection_time <= $3)                                AS sampled_collections
    FROM per_collection
),
by_event AS (
    SELECT
        lower(w.wait_type)  AS type_key,
        lower(w.wait_event) AS event_key,
        CAST(coalesce(SUM(GREATEST(w.delta_wait_time_us, 0)), 0) AS bigint) AS wait_time_us,
        CAST(coalesce(SUM(GREATEST(w.delta_waits, 0)), 0) AS bigint)        AS waits
    FROM pg_wait_stats AS w
    JOIN per_collection AS c
      ON  c.collection_time = w.collection_time
      AND c.interval_sec > 0
    WHERE w.server_id = $1
    AND   w.collection_time >= $2
    AND   w.collection_time <= $3
    AND   w.wait_type IS NOT NULL
    GROUP BY lower(w.wait_type), lower(w.wait_event)
)
SELECT
    b.type_key,
    b.event_key,
    b.wait_time_us,
    b.waits,
    o.observed_sec,
    o.sample_count,
    o.collection_count,
    o.restart_collections,
    o.sampled_collections
FROM observed AS o
LEFT JOIN by_event AS b
  ON TRUE
ORDER BY b.wait_time_us DESC NULLS LAST";

    /// <summary>
    /// The stock wait profile: <c>pg_wait_sampling</c> — SAMPLED sightings, not measured time. The collector
    /// stores each (event_type, event, query_id) series' CUMULATIVE <c>sample_count</c> with the
    /// <c>profile_period_ms</c> it was gathered at (the extension's <c>pg_wait_sampling.profile_period</c>,
    /// 10 ms by default, read with <c>missing_ok</c>; the #3604 service sampler's 1,000 ms), five-minute
    /// cadence. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window. Same output shape as
    /// <see cref="PgWaitStatsSql"/> plus the estimate's provenance columns.
    ///
    /// <para><b>Deltas per series, per consecutive collection, with the reader's reset rule.</b>
    /// <c>DarlingPgWaitSamplingReader.PgWaitSamplingSql</c> differences the window's edges and, when the newest
    /// count is below the oldest, takes the newest whole — everything since the reset — rather than
    /// <c>GREATEST(…, 0)</c>'s silent zero. Per-collection <c>LAG</c> applies the same rule at every step so a
    /// reset mid-window costs one interval's counts, not the window's, and <c>reset_count</c> says how often.
    /// A series' FIRST in-window sighting contributes nothing: its count may have accrued before the window
    /// (the first collection), or the key may have fallen out of the collector's top-500 and returned with a
    /// history the window did not see — the reader precedent counts only what it saw twice.</para>
    ///
    /// <para><b>The estimate is made here, once, beside the count.</b> <c>estimated_wait_ms = Σ Δsamples ×
    /// profile_period_ms</c> per row, so a period that changed mid-window is honoured per interval. It is an
    /// INFERENCE: a wait shorter than the period is invisible, a continuous wait quantises to ±1 period per
    /// backend-sample, and the count is per BACKEND-sample — ten backends waiting one second is 1,000 samples
    /// and "10 s of waiting", time summed over tasks, never a share of the server's clock. Every fact from
    /// this read carries <c>is_sampled = 1</c> and <c>estimate_resolution_ms = profile_period_ms</c>, and the
    /// advice says "estimated from sampling".</para>
    ///
    /// <para><b>The denominator is the time the sampler was WATCHING, not the time between collections (lane 24,
    /// #3691; V133).</b> Two instruments write this table with the same <c>profile_period_ms</c> shape and a
    /// different duty cycle: the <c>pg_wait_sampling</c> extension samples the whole interval in-engine, and the
    /// #3604 service-side sampler watches <c>SamplerSnapshotsPerCycle</c> one-second snapshots (30 s) of each 300 s
    /// cycle. <c>Δsamples × period</c> over the interval is honest for the first and a ~10× understatement for the
    /// second, and nothing in the row said which until V133 stored <c>sampled_ms</c> — the milliseconds THAT
    /// collection actually observed (30,000 from the sampler on a full window; NULL from the extension arm, which
    /// has no duty cycle to disclose). So per countable collection the observed time is <c>sampled_ms</c> when the
    /// row carries it and the <c>LAG</c> interval otherwise (<c>coalesce</c>, per collection — a window that
    /// straddles the V133 install mixes the two honestly, both being milliseconds observed), and the fraction is
    /// ms of sampled waiting per second the sampler was watching. A pre-V133 row is NULL and came from EITHER arm
    /// — indistinguishable from the extension — so NULL is read as "the whole interval" (exactly lane 5's arithmetic)
    /// and never guessed at 30 s; <c>unknown_sampled_collections</c> counts them so the fact can say
    /// <c>sampled_ms_known = 0</c> when any countable collection had to be read that way. <c>interval_sec_total</c>
    /// keeps the wall time between the same collections beside it, so the advice can state the duty cycle the
    /// sampler ran at rather than let a reader infer it. The <c>LAG</c> is over distinct collection times (this
    /// table stores no interval; it is not a delta family); the pass's witness is one-minute <c>pg_database_stats</c>
    /// and this series is five-minute, so a gap in one is not a gap in the other, and both are stated on the fact.
    /// Same always-one-row header shape as <see cref="PgWaitStatsSql"/>.</para>
    /// </summary>
    public const string PgWaitSamplingSql = @"
WITH series AS (
    SELECT
        collection_time,
        event_type,
        event,
        query_id,
        sample_count,
        profile_period_ms,
        backend_count,
        sampled_ms,
        LAG(sample_count) OVER (PARTITION BY event_type, event, query_id ORDER BY collection_time) AS prev_count
    FROM pg_wait_sampling
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
collections AS (
    SELECT collection_time,
           MAX(sampled_ms) AS sampled_ms
    FROM series
    GROUP BY collection_time
),
per_collection AS (
    SELECT
        collection_time,
        sampled_ms,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM collections
),
observed AS (
    SELECT
        coalesce(SUM(coalesce(sampled_ms / 1000.0, interval_sec)) FILTER (WHERE interval_sec > 0), 0) AS observed_sec,
        coalesce(SUM(interval_sec) FILTER (WHERE interval_sec > 0), 0)                             AS interval_sec_total,
        CAST(count(*) FILTER (WHERE interval_sec > 0) AS integer)                                  AS sample_count,
        CAST(count(*) AS integer)                                                                   AS collection_count,
        CAST(count(*) FILTER (WHERE interval_sec > 0 AND sampled_ms IS NULL) AS integer)           AS unknown_sampled_collections
    FROM per_collection
),
deltas AS (
    SELECT
        event_type,
        event,
        profile_period_ms,
        backend_count,
        CASE WHEN prev_count IS NULL THEN NULL
             WHEN sample_count < prev_count THEN sample_count
             ELSE sample_count - prev_count
        END AS delta_samples,
        (prev_count IS NOT NULL AND sample_count < prev_count) AS reset_here
    FROM series
),
by_event AS (
    SELECT
        lower(event_type) AS type_key,
        lower(event)      AS event_key,
        CAST(coalesce(SUM(delta_samples), 0) AS bigint)                     AS delta_samples,
        CAST(coalesce(SUM(delta_samples * profile_period_ms), 0) AS bigint) AS estimated_wait_ms,
        MAX(profile_period_ms)                                              AS profile_period_ms,
        MAX(backend_count)                                                  AS peak_backends,
        CAST(count(*) FILTER (WHERE reset_here) AS integer)                 AS reset_count
    FROM deltas
    WHERE event_type IS NOT NULL
    GROUP BY lower(event_type), lower(event)
)
SELECT
    b.type_key,
    b.event_key,
    b.delta_samples,
    b.estimated_wait_ms,
    b.profile_period_ms,
    b.peak_backends,
    b.reset_count,
    o.observed_sec,
    o.sample_count,
    o.collection_count,
    o.interval_sec_total,
    o.unknown_sampled_collections
FROM observed AS o
LEFT JOIN by_event AS b
  ON TRUE
ORDER BY b.estimated_wait_ms DESC NULLS LAST";

    /// <summary>
    /// The v1 wait vocabulary (design §2a-1): the four type rollups and the four named standouts, as
    /// (type, event, display) — the display name is a CONSTANT because Aurora renames events between majors
    /// (<c>AutoVacuumMain</c> / <c>AutovacuumMain</c>) and a fact's <see cref="Fact.ObjectName"/> must read the
    /// same across an upgrade; the match is on the lower-cased column, the key through
    /// <see cref="PgTargetFactKeys.WaitKey"/>. Everything else in the profile (BufferPin, Extension, other
    /// Lock/LWLock/IO/IPC events) is summed INTO its rollup and into the share denominator but is not a fact
    /// of its own in v1.
    /// </summary>
    private static readonly (string Type, string? Event, string Display)[] s_waitVocabulary =
    [
        ("Lock", null, "Lock"),
        ("LWLock", null, "LWLock"),
        ("IO", null, "IO"),
        ("IPC", null, "IPC"),
        ("Lock", "relation", "Lock:relation"),
        ("LWLock", "WALWrite", "LWLock:WALWrite"),
        ("IO", "DataFileRead", "IO:DataFileRead"),
        ("IO", "WALSync", "IO:WALSync"),
    ];

    /// <summary>One (type, event) row of either read, already summed over the window. <c>PeakBackends</c> is the
    /// sampled read's most-backends-in-one-collection for the series; 0 on the Aurora read, which has no such column.</summary>
    internal readonly record struct WaitProfileRow(string TypeKey, string? EventKey, double WaitMs, long Count, int PeakBackends = 0);

    /// <summary>
    /// The wait-profile facts (filled by lane 5 — #3542 step 5, design §2a / §5): type rollups <c>Lock</c>,
    /// <c>LWLock</c>, <c>IO</c>, <c>IPC</c> and the named standouts <c>Lock:relation</c>, <c>LWLock:WALWrite</c>,
    /// <c>IO:DataFileRead</c>, <c>IO:WALSync</c>, each keyed through <see cref="PgTargetFactKeys.WaitKey"/> with
    /// <see cref="Fact.Value"/> = wait time as a FRACTION of the wait source's observed time (time summed over
    /// tasks / wall time — it exceeds 1.0 with concurrent waiters, exactly as the SQL Server fraction does).
    ///
    /// <para><b>Routing is READ-then-fallback, never by registry kind.</b> <c>pg_wait_stats</c> is read first;
    /// only when it has NO rows in the window is <c>pg_wait_sampling</c> read. The two sources are
    /// platform-exclusive by construction (the Aurora collector applies to Aurora, the sampling collector to
    /// everything else), so the data itself says which flavour this is — and <c>servers.engine_kind</c> is NULL
    /// until a connect stamps it, so routing on the registry would blind an Aurora target's waits on a stale
    /// row (adversarial item B). <see cref="AnalysisContext"/> is shared with Lite and does not grow an
    /// engine-kind field for this. An Aurora target with rows but no countable interval (every in-window
    /// collection a restart) emits nothing and does NOT fall back: the fallback is for "this flavour does not
    /// write that table", not "that table had a bad hour".</para>
    ///
    /// <para><b>Two evidence grades behind one key, told apart by metadata, never blended.</b> An Aurora fact
    /// is the engine's measured wait time; a stock fact is <c>Δsamples × profile_period_ms</c> over the time the
    /// sampler was WATCHING (<c>sampled_ms</c>, V133 — see <see cref="PgWaitSamplingSql"/>) and carries
    /// <c>is_sampled = 1</c> with <c>estimate_resolution_ms</c>, <c>sampled_ms_known</c> and the wall interval
    /// beside the watched time, and carries them on EVERY fact the sampled read emits — the scorer grades both
    /// on the same fraction bars with the estimate stated, the advice says "estimated from sampling", and a
    /// reader of <c>get_analysis_facts</c> can see which grade a row is without knowing the flavour.</para>
    ///
    /// <para><b>Both sources in one window: the exact one wins, and says so (lane 24).</b> The Aurora read's
    /// header counts <c>pg_wait_sampling</c>'s collections over the same window; when both wrote, the sampled
    /// facts are not emitted (they would be a second, coarser profile of the same seconds) and every exact fact
    /// carries <c>sampled_suppressed_by_exact = 1</c>. The sampled anomaly detector applies the same rule from
    /// its own read (<c>PgTargetAnomalyDetector.WaitsSampled.cs</c>). Lane 5's read-then-fallback read only one
    /// source and could not see the case; the choice is now explicit and pinned.</para>
    ///
    /// <para><b>What is NOT emitted.</b> Nothing when the pass observed no time, when the chosen source has no
    /// countable interval, or for a key with zero wait in the window (a fact about nothing). No fact for the
    /// <c>CPU</c> type — on Aurora it is on-CPU time reported under the wait function, on stock the
    /// <c>CPU/Running</c> pseudo-row; it is excluded from the share denominator too, so <c>share_of_wait_time</c>
    /// is a share of WAITING. No query attribution (the sampling table's <c>query_id</c> dimension is a later
    /// slice). No anomaly — <c>ANOMALY_PG_WAIT_PROFILE</c> is lane 9's and <c>ANOMALY_PG_SAMPLED_WAIT_PROFILE</c>
    /// lane 24's, both in <c>PgTargetAnomalyDetector</c>.</para>
    /// </summary>
    private async partial Task CollectWaitFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var rows = new List<WaitProfileRow>();
            double observedSec = 0;
            int sampleCount = 0, collectionCount = 0, restartCollections = 0, sampledCollections = 0;

            using (var cmd = new NpgsqlCommand(PgWaitStatsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    /* The header rides on every row (the LEFT JOIN's one row when the window is empty). */
                    observedSec = Convert.ToDouble(reader.GetValue(4));
                    sampleCount = Convert.ToInt32(reader.GetValue(5));
                    collectionCount = Convert.ToInt32(reader.GetValue(6));
                    restartCollections = Convert.ToInt32(reader.GetValue(7));
                    sampledCollections = Convert.ToInt32(reader.GetValue(8));
                    if (reader.IsDBNull(0)) continue;
                    rows.Add(new WaitProfileRow(
                        reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetString(1),
                        ToInt64(reader.GetValue(2)) / 1000.0,
                        ToInt64(reader.GetValue(3))));
                }
            }

            if (collectionCount > 0)
            {
                /* This flavour writes pg_wait_stats. Rows with no countable interval emit nothing, and do not
                   fall back — see the method remarks. When pg_wait_sampling ALSO wrote this window the exact source
                   wins and the fact says the sampled profile was suppressed (method remarks). */
                EmitWaitFacts(context, facts, rows, observedSec, sampleCount, collectionCount, sampled: null,
                    extra:
                    [
                        (PgTargetScorer.WaitRestartCollectionsKey, restartCollections),
                        (PgTargetScorer.WaitSampledSuppressedByExactKey, sampledCollections > 0 ? 1 : 0),
                    ]);
                return;
            }

            /* The fallback: no pg_wait_stats rows in the window means this flavour does not write that table. */
            int profilePeriodMs = 0, resetCount = 0, unknownSampledCollections = 0;
            double intervalSecTotal = 0;
            using (var cmd = new NpgsqlCommand(PgWaitSamplingSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    observedSec = Convert.ToDouble(reader.GetValue(7));
                    sampleCount = Convert.ToInt32(reader.GetValue(8));
                    collectionCount = Convert.ToInt32(reader.GetValue(9));
                    intervalSecTotal = Convert.ToDouble(reader.GetValue(10));
                    unknownSampledCollections = Convert.ToInt32(reader.GetValue(11));
                    if (reader.IsDBNull(0)) continue;
                    rows.Add(new WaitProfileRow(
                        reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetString(1),
                        Convert.ToDouble(reader.GetValue(3)),
                        ToInt64(reader.GetValue(2)),
                        reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5))));
                    /* The window's period is the largest any row saw — the coarsest resolution the estimate
                       was made at; a finer period mid-window only makes the stated resolution conservative. */
                    profilePeriodMs = Math.Max(profilePeriodMs, reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)));
                    resetCount += Convert.ToInt32(reader.GetValue(6));
                }
            }

            if (rows.Count == 0) return;

            /* sampled_ms is KNOWN for the window only when every countable collection carried it; one pre-V133
               (NULL) collection read as its whole interval makes the window's denominator partly the old
               arithmetic, and the fact says so rather than let 30 s be guessed (PgWaitSamplingSql remarks). */
            EmitWaitFacts(context, facts, rows, observedSec, sampleCount, collectionCount,
                sampled: (profilePeriodMs, resetCount, intervalSecTotal, SampledMsKnown: unknownSampledCollections == 0), extra: null);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_wait_stats arrived in V63 and pg_wait_sampling in V96; sample_interval_seconds in V128. A
               pre-migration store raises 42P01 / 42703 here, which the reporter classifies quiet. An
               abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Groups the per-(type, event) rows into the vocabulary and stamps one fact per key with wait in the
    /// window. Shared by both reads so the two grades cannot drift in shape: the only differences are the
    /// <c>is_sampled</c> / <c>estimate_resolution_ms</c> / <c>delta_samples</c> / <c>peak_backends</c> /
    /// <c>counter_resets</c> / <c>sampled_ms_known</c> / <c>wait_source_interval_ms</c> keys on a sampled fact and
    /// <c>restart_collections</c> / <c>sampled_suppressed_by_exact</c> on an Aurora one. <paramref name="observedSec"/>
    /// is the fraction's denominator in seconds: the wait source's own observed time on an Aurora call, the time the
    /// sampler was WATCHING on a sampled one (Σ <c>sampled_ms</c>, or the interval where the row carried none);
    /// <c>sampled.IntervalSec</c> is the wall time between the same collections, stated beside it. Internal so
    /// <c>PgTargetWaitTests</c> can execute the grouping and the estimate arithmetic on arranged rows without a
    /// store — the same seam <c>PgFactCollector.BuildCoverage</c> offers the coverage pins.
    /// </summary>
    internal static void EmitWaitFacts(
        AnalysisContext context,
        List<Fact> facts,
        List<WaitProfileRow> rows,
        double observedSec,
        int sampleCount,
        int collectionCount,
        (int PeriodMs, int Resets, double IntervalSec, bool SampledMsKnown)? sampled,
        IReadOnlyList<(string Key, double Value)>? extra)
    {
        if (observedSec <= 0) return;
        var waitObservedMs = observedSec * 1000.0;

        /* The share denominator: every wait the profile holds except on-CPU time. */
        double totalWaitMs = 0;
        var byType = new Dictionary<string, (double WaitMs, long Count, int Events, int PeakBackends)>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            if (row.TypeKey.Equals("cpu", StringComparison.Ordinal)) continue;
            totalWaitMs += row.WaitMs;
            byType.TryGetValue(row.TypeKey, out var acc);
            byType[row.TypeKey] = (acc.WaitMs + row.WaitMs, acc.Count + row.Count, acc.Events + 1, Math.Max(acc.PeakBackends, row.PeakBackends));
        }

        foreach (var (type, waitEvent, display) in s_waitVocabulary)
        {
            var typeKey = type.ToLowerInvariant();
            double waitMs;
            long count;
            int events, peakBackends;
            if (waitEvent is null)
            {
                if (!byType.TryGetValue(typeKey, out var rollup)) continue;
                (waitMs, count, events, peakBackends) = rollup;
            }
            else
            {
                var eventKey = waitEvent.ToLowerInvariant();
                var match = rows.Find(r => r.TypeKey.Equals(typeKey, StringComparison.Ordinal)
                                        && string.Equals(r.EventKey, eventKey, StringComparison.Ordinal));
                if (match.TypeKey is null) continue;
                (waitMs, count, events, peakBackends) = (match.WaitMs, match.Count, 1, match.PeakBackends);
            }

            if (waitMs <= 0) continue;

            var fact = new Fact
            {
                Source = PgTargetSources.WaitsSource,
                Key = PgTargetFactKeys.WaitKey(type, waitEvent),
                Value = waitMs / waitObservedMs,
                ServerId = context.ServerId,
                ObjectName = display,
                Metadata =
                {
                    [PgTargetScorer.WaitFractionKey] = waitMs / waitObservedMs,
                    [PgTargetScorer.WaitMsKey] = waitMs,
                    [PgTargetScorer.WaitCountKey] = count,
                    [PgTargetScorer.WaitSourceObservedMsKey] = waitObservedMs,
                    [PgTargetScorer.WaitSampleCountKey] = sampleCount,
                    [PgTargetScorer.WaitCollectionCountKey] = collectionCount,
                    [PgTargetScorer.WaitObservedMsKey] = context.ObservedDurationMs,
                    [PgTargetScorer.WaitWitnessFractionKey] = waitMs / context.ObservedDurationMs,
                    ["coverage_fraction"] = context.Coverage?.Fraction ?? 0,
                    [PgTargetScorer.WaitShareOfWaitTimeKey] = totalWaitMs > 0 ? waitMs / totalWaitMs : 0,
                    [PgTargetScorer.WaitIsStandoutKey] = waitEvent is null ? 0 : 1,
                },
            };

            if (waitEvent is null)
            {
                fact.Metadata[PgTargetScorer.WaitEventsInTypeKey] = events;
                /* The largest fraction any NAMED standout of this type reached — the scorer grades the rollup
                   only when no standout of its type is itself a finding, so one wait is never graded twice
                   (see PgTargetScorer.Waits.cs). 0 when the type has no named standout in the vocabulary. */
                double maxStandout = 0;
                foreach (var (standoutType, standoutEvent, _) in s_waitVocabulary)
                {
                    if (standoutEvent is null || !standoutType.Equals(type, StringComparison.Ordinal)) continue;
                    var standoutKey = standoutEvent.ToLowerInvariant();
                    var standout = rows.Find(r => r.TypeKey.Equals(typeKey, StringComparison.Ordinal)
                                                && string.Equals(r.EventKey, standoutKey, StringComparison.Ordinal));
                    if (standout.TypeKey is not null)
                        maxStandout = Math.Max(maxStandout, standout.WaitMs / waitObservedMs);
                }
                fact.Metadata[PgTargetScorer.WaitMaxStandoutFractionKey] = maxStandout;
            }
            else if (byType.TryGetValue(typeKey, out var parent) && parent.WaitMs > 0)
            {
                fact.Metadata[PgTargetScorer.WaitShareOfTypeKey] = waitMs / parent.WaitMs;
            }

            if (sampled is { } s)
            {
                fact.Metadata[PgTargetScorer.WaitIsSampledKey] = 1;
                fact.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey] = s.PeriodMs;
                fact.Metadata[PgTargetScorer.WaitDeltaSamplesKey] = count;
                fact.Metadata[PgTargetScorer.WaitPeakBackendsKey] = peakBackends;
                fact.Metadata[PgTargetScorer.WaitCounterResetsKey] = s.Resets;
                /* The duty cycle, stated: the wall time between the countable collections beside the time the
                   sampler watched (the denominator), and whether every collection disclosed the latter (V133) or
                   some were read as their whole interval (pre-V133 NULL — lane 5's arithmetic, never a guessed 30 s). */
                fact.Metadata[PgTargetScorer.WaitSourceIntervalMsKey] = s.IntervalSec > 0 ? s.IntervalSec * 1000.0 : waitObservedMs;
                fact.Metadata[PgTargetScorer.WaitSampledMsKnownKey] = s.SampledMsKnown ? 1 : 0;
            }

            if (extra is not null)
            {
                foreach (var (extraKey, extraValue) in extra)
                    fact.Metadata[extraKey] = extraValue;
            }

            facts.Add(fact);
        }
    }
}
