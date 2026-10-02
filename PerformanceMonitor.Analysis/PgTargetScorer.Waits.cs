/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_waits</c> — the wait profile (filled by lane 5 of #3542, design §2a / §5): type rollups (<c>Lock</c>,
/// <c>LWLock</c>, <c>IO</c>, <c>IPC</c>) and named standouts (<c>Lock:relation</c>, <c>LWLock:WALWrite</c>,
/// <c>IO:DataFileRead</c>, <c>IO:WALSync</c>), keyed through <see cref="PgTargetFactKeys.WaitKey"/> — the
/// PostgreSQL twin of <c>FactScorer.GetWaitThresholds</c>. Every bar is a FRACTION of the wait source's OWN
/// observed time (<see cref="WaitSourceObservedMsKey"/>): time summed over tasks per second of wall clock, the
/// axis the SQL Server wait facts grade on, so a 1.0 here means what a 1.0 there means (D5).
///
/// <para><b>Lineage: the four TYPE-ROLLUP bars and the two STANDOUT bars are measured on Aurora; the sampling
/// floor is not.</b> PostgreSQL draws no line of its own for "how much waiting is too much" — a wait event is a
/// label, not a limit — so no bar here can be engine-defined. The #3691 fleet calibration (2026-09-19) read
/// the per-type fraction distribution over <c>pg_wait_stats</c> (5-minute buckets, 7 days × 50 Aurora
/// PostgreSQL clusters of the dogfood fleet — the engine's own measured wait deltas, the source the Aurora
/// wait collector stores) and the rollup bars stand where they were chosen; its second read (2026-09-20, the
/// same clusters and window) went per EVENT and placed the standout bars too: each constant below names its
/// percentile. The population is Aurora-specific — a stock server's <c>pg_wait_sampling</c> ESTIMATE grades
/// on the same bars by design but is another instrument on another population, so a sampled fact keeps
/// <c>threshold_lineage = 0</c>; every Aurora fact — a rollup graded on its fraction, a standout graded on its
/// event bars, and a rollup that yields to a standout (the yield is decided on the standout bar, now measured)
/// — carries <c>threshold_lineage = 1</c>. No bar is a SQL Server wait constant reused by
/// value — the SQL Server table's pairs are
/// (0.01, 0.05, 0.10, 0.25, 0.30, 0.50, 0.75) and none appears here; that is checked by eye, not by accident:
/// the numbers below were chosen on the PostgreSQL side's own argument (a backend-equivalent of continuous
/// waiting) and then confirmed distinct.</para>
///
/// <para><b>Two evidence grades, one set of bars.</b> A sampled fact (<c>is_sampled = 1</c>, the stock
/// <c>pg_wait_sampling</c> estimate) grades on the SAME fractions as an Aurora fact — the estimate is stated
/// on the fact (<see cref="WaitEstimateResolutionMsKey"/>) and in the advice, not hidden in a second table
/// — with one guard: below <see cref="WaitSampledMinimumSamples"/> samples the estimate has no resolution and
/// scores 0 (adversarial item A.3). Since lane 24 (#3691, V133) the sampled fraction is over the time the
/// sampler was WATCHING (<c>sampled_ms</c>), not the wall interval, so on the service-sampler arm it reads
/// ~10× what lane 5's arithmetic read for the same rows; the four rollup bars were measured on Aurora's exact
/// deltas, and the sampled rate on stock is the same UNIT (backend-seconds waiting per second observed) from a
/// different INSTRUMENT on a population the calibration did not read — unmeasured for stock, which is why
/// every sampled fact keeps <c>threshold_lineage = 0</c> and its advice says "estimated from sampling".</para>
///
/// <para><b>"Fired" means at or past the concerning bar.</b> Below it the fact scores 0, not the shared
/// formula's sub-0.5 ramp — the convention lane 2 set for every PostgreSQL family, so that a graph edge or an
/// amplifier reading <c>BaseSeverity &gt; 0</c> on a wait fact means "this wait is a finding", never "a trace
/// of this wait exists". The SQL Server wait axis ramps from zero; the PostgreSQL one does not, and the
/// facts are still emitted below the bar so <c>get_analysis_facts</c> shows the whole profile.</para>
///
/// <para><b>One wait is graded once: a rollup is the UNNAMED story.</b> A standout's wait time is also inside
/// its type's rollup, and a standout's bar is the tighter one, so a rollup graded on its whole fraction would
/// root a second story about the same seconds right after the standout rooted the first (the inference engine
/// walks roots in severity order and a rollup cannot lead to a standout that is already consumed). So a
/// rollup scores 0 whenever a named standout of its type is itself a finding (<see cref="WaitMaxStandoutFractionKey"/>
/// at or past <see cref="WaitStandoutConcerning"/>) — the standout roots, and its advice states the share of
/// its type — and grades on its whole fraction only when the type's waiting is in events the vocabulary does
/// not name (<c>Lock:transactionid</c>, <c>LWLock:BufferMapping</c>, …), which is exactly when "the Lock
/// class" is the most specific true thing to say. The rollup's <see cref="Fact.Value"/> stays the whole
/// fraction either way; the profile is not edited, only the grade.</para>
///
/// <para><b>Metadata contract with the collector</b> (<c>PgTargetFactCollector.Waits.cs</c>) and the advice
/// partial: the keys below are the whole interface. A missing key reads as 0 through <c>GetValueOrDefault</c>,
/// which for every gate here means "does not fire".</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── metadata keys (both grades) ── */

    /// <summary>Metadata key: the fact's <see cref="Fact.Value"/> restated — wait ms / <see cref="WaitSourceObservedMsKey"/>.</summary>
    public const string WaitFractionKey = "wait_fraction";
    /// <summary>Metadata key: wait time in ms — MEASURED on an Aurora fact, ESTIMATED (<c>Δsamples × period</c>) on a sampled one.</summary>
    public const string WaitMsKey = "wait_ms";
    /// <summary>Metadata key: Aurora — Σ <c>delta_waits</c> (completed waits); sampled — Δ sample count (see <see cref="WaitDeltaSamplesKey"/>).</summary>
    public const string WaitCountKey = "wait_count";
    /// <summary>Metadata key: the wait source's own observed time in ms — the fraction's denominator.</summary>
    public const string WaitSourceObservedMsKey = "wait_source_observed_ms";
    /// <summary>Metadata key: collections of the wait source with a countable interval.</summary>
    public const string WaitSampleCountKey = "sample_count";
    /// <summary>Metadata key: collections of the wait source in the window, countable or not.</summary>
    public const string WaitCollectionCountKey = "collection_count";
    /// <summary>Metadata key: the pass's observed time from the coverage witness (<c>pg_database_stats</c>), in ms.</summary>
    public const string WaitObservedMsKey = "observed_ms";
    /// <summary>Metadata key: the same wait over the WITNESS's observed time — stated beside the fraction so a
    /// reader can see how far the two denominators sit apart.</summary>
    public const string WaitWitnessFractionKey = "witness_fraction";
    /// <summary>Metadata key: this key's wait as a share of every wait in the window (CPU excluded).</summary>
    public const string WaitShareOfWaitTimeKey = "share_of_wait_time";
    /// <summary>Metadata key: 1 on a named standout, 0 on a type rollup.</summary>
    public const string WaitIsStandoutKey = "is_standout";
    /// <summary>Metadata key (standouts): this event's share of its type's wait.</summary>
    public const string WaitShareOfTypeKey = "share_of_type";
    /// <summary>Metadata key (rollups): distinct events summed into the rollup.</summary>
    public const string WaitEventsInTypeKey = "events_in_type";
    /// <summary>Metadata key (rollups): the largest fraction any NAMED standout of this type reached in the
    /// window (0 when the type has no named standout in the vocabulary, or none had wait). The rollup is graded
    /// only when this sits below the standout bar — see <see cref="ScoreWaitFact"/>.</summary>
    public const string WaitMaxStandoutFractionKey = "max_standout_fraction";

    /* ── Aurora only ── */

    /// <summary>Metadata key: collections whose every row carried the stored-interval 0 (a restart / first
    /// sighting) — excluded from the sample count and the sums.</summary>
    public const string WaitRestartCollectionsKey = "restart_collections";
    /// <summary>Metadata key (lane 24): 1 when <c>pg_wait_sampling</c> ALSO wrote this window and the sampled
    /// profile was therefore not emitted — the exact source wins, and says so; 0 when the sampler was silent.</summary>
    public const string WaitSampledSuppressedByExactKey = "sampled_suppressed_by_exact";

    /* ── sampled only (stock pg_wait_sampling) ── */

    /// <summary>Metadata key: 1 on every fact the sampling read emits; ABSENT on an Aurora fact.</summary>
    public const string WaitIsSampledKey = "is_sampled";
    /// <summary>Metadata key: the sampling period the estimate was made at (<c>profile_period_ms</c>) — the
    /// shortest wait the estimate can see, and the quantum a continuous wait rounds to.</summary>
    public const string WaitEstimateResolutionMsKey = "estimate_resolution_ms";
    /// <summary>Metadata key: Δ backend-samples in the window behind the estimate.</summary>
    public const string WaitDeltaSamplesKey = "delta_samples";
    /// <summary>Metadata key: the most backends any one series saw waiting this way in one collection.</summary>
    public const string WaitPeakBackendsKey = "peak_backends";
    /// <summary>Metadata key: profile resets seen inside the window (a counter that went backwards).</summary>
    public const string WaitCounterResetsKey = "counter_resets";
    /// <summary>Metadata key (lane 24, V133): the WALL time between the countable collections, in ms — stated beside
    /// <see cref="WaitSourceObservedMsKey"/>, which on a sampled fact is the time the sampler was WATCHING (Σ
    /// <c>sampled_ms</c>; the #3604 service sampler watches 30 s of each 300 s cycle). The ratio of the two is the
    /// duty cycle the estimate was made at; equal when every collection was read as its whole interval.</summary>
    public const string WaitSourceIntervalMsKey = "wait_source_interval_ms";
    /// <summary>Metadata key (lane 24, V133): 1 when every countable collection carried <c>sampled_ms</c>, so the
    /// denominator is the sampler's disclosed watching time; 0 when at least one collection (a pre-V133 row, or
    /// the extension arm) carried NULL and was read as its whole interval — lane 5's arithmetic, which on the
    /// service-sampler arm understates the rate by the duty cycle, roughly 10×. Never guessed.</summary>
    public const string WaitSampledMsKnownKey = "sampled_ms_known";

    /* ── the bars ── */

    /// <summary>
    /// A sampled fact below this many Δ backend-samples scores 0: with fewer than three sightings the
    /// estimate cannot tell a one-period wait from a three-period one, and any fraction it produces is the
    /// period times a coin flip. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate against
    /// <c>pg_wait_sampling</c> before the next release. Population 0 on the dogfood fleet as of 2026-09-20 (the
    /// second calibration read found zero <c>pg_wait_sampling</c> rows fleet-wide — every cluster is Aurora with
    /// <c>pg_wait_stats</c>, none runs the extension or the service sampler), so this floor stays unmeasured until a
    /// stock target joins the fleet. Aurora facts are measured time and have no such floor.
    /// </summary>
    public const int WaitSampledMinimumSamples = 3;

    /* measured: 0.20 ≈ p97 and 1.0 ≈ p99.7 of 5-minute buckets of total non-CPU waiting (ms waited per second
       observed) over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 (pg_wait_stats,
       the engine's measured wait deltas). Per-server p50 median 0.012, p99 median 0.062, fleet p90 of p99 0.82,
       maximum 10.7; buckets at or above 0.20: median cluster 0, fleet p75 2.8 %, worst 41 % (a chronically
       waiting cluster); at or above 1.0: fleet p90 0.5 %, worst 15.7 %. Per type, ipc p99 0.35 (max 7.8), lock
       p99 0.034 (max 2.0), lwlock negligible. Aurora population, measured on Aurora's exact deltas; the sampled rate
       on stock (pg_wait_sampling over sampled_ms, lane 24) is the same unit from a different instrument — unmeasured
       for stock, and a sampled fact says threshold_lineage = 0. The argument for the shape, stated once for all eight: the fraction is backend-seconds waiting per wall
       second, so 1.0 is ONE backend waiting on this
       thing continuously for the whole window — the natural critical line for a rollup and for the standouts
       that are its dominant members. Concerning is a fifth of a backend (0.20) for the rollups: PostgreSQL
       grants heavyweight locks FIFO with no timeout by default, so a fifth of a backend permanently queued on
       Lock is a queue that is forming; LWLocks are held for microseconds by design, so a fifth of a backend
       continuously inside one is contention on a shared-memory structure (WAL insert, buffer mapping), not
       normal operation; IPC is backends waiting on each other (parallel workers, sync rep, notify) and a
       fifth of a backend there is coordination cost the workload is paying for nothing. */
    public const double WaitRollupConcerning = 0.20;
    public const double WaitRollupCritical = 1.0;

    /* measured: 0.40 ≈ p99 and 2.0 ≈ p99.9 of 5-minute buckets of IO-type waiting (ms waited per second observed)
       over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 — io p50 0.012, p90 0.077,
       p99 0.46, maximum 3.9; buckets at or above 0.40: fleet p90 2.4 %, worst 24 %. Aurora population (Aurora
       storage; a stock local-disk server is a different population and its sampled rate over sampled_ms is the
       same unit from a different instrument — unmeasured for stock). IO is
       the one type a healthy server legitimately spends time in (reading is what a database does), so its
       concerning bar sits at twice the others (0.40) and its critical at two full backends (2.0): a server
       that keeps two backends' worth of time in I/O waits continuously is I/O-bound by any definition. */
    public const double WaitIoConcerning = 0.40;
    public const double WaitIoCritical = 2.0;

    /* measured: 0.15 ≈ p99.6 and 1.0 ≈ p99.99 of per-EVENT 5-minute buckets (ms waited per ms observed — the share of
       one backend) for the non-IO events over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet,
       2026-09-20 (pg_wait_stats, the second calibration read, per event this time). Only three events ever clear
       0.15 in more than 0.02 % of their buckets: IO:DataFileRead (3.7 % of buckets at or above 0.15, 0.25 % at or
       above 1.0 — so for THAT standout 0.15 sits at ≈ p96.3 and 1.0 at ≈ p99.75, the same band the IO rollup's own
       0.40 ≈ p99 occupies: reading is what a database does, and the IO standout is the one that will fire in
       routine on a read-heavy cluster, which the advice states as the share of its type), IPC:BufferIO (0.42 % /
       0.008 %) and IPC:BtreePage (0.36 % / 0.012 %) — neither an entry in the vocabulary, graded inside the IPC
       rollup. Every other event, the three named standouts among them: at most 0.02 % at or above 0.15, about 0 at
       or above 1.0 (Lock:relation maximum 2.01 — one bucket; LWLock:WALWrite and IO:WALSync never near the bar;
       Lock:transactionid maximum 0.09). Aurora population, measured on Aurora's exact deltas; a sampled fact on
       stock says threshold_lineage = 0 as its rollup does. The argument that chose the bars, still the reason for
       their shape: the four standouts are each ONE event, and a single event holding 0.15 of a backend continuously
       is already a named cause (Lock:relation — a table or index another session holds; LWLock:WALWrite — every
       commit queued behind the WAL writer; IO:WALSync — fsync on the WAL segment, the durability cost paid per
       commit; IO:DataFileRead — heap and index pages coming off the device). Critical is one full backend, as for
       the rollups they belong to. */
    public const double WaitStandoutConcerning = 0.15;
    public const double WaitStandoutCritical = 1.0;

    /// <summary>
    /// The PostgreSQL wait-threshold table, keyed by the NORMALISED fact key (<see cref="PgTargetFactKeys.WaitKey"/>
    /// up-cases and collapses punctuation, so <c>Lock:relation</c>, <c>LOCK:RELATION</c> and an Aurora
    /// major's re-cased spelling all land on one entry — the case-insensitivity the design asks for is a
    /// property of the key, not of a lookup here). Built from the constants above so that every bar in the
    /// table has exactly one lineage comment, on the constant.
    /// </summary>
    private static readonly Dictionary<string, (double Concerning, double Critical)> s_waitBars = new(StringComparer.Ordinal)
    {
        /* measured (Aurora, 2026-09-19): WaitRollupConcerning / WaitRollupCritical (see the constants). */
        [PgTargetFactKeys.WaitKey("Lock", null)] = (WaitRollupConcerning, WaitRollupCritical),
        [PgTargetFactKeys.WaitKey("LWLock", null)] = (WaitRollupConcerning, WaitRollupCritical),
        [PgTargetFactKeys.WaitKey("IPC", null)] = (WaitRollupConcerning, WaitRollupCritical),
        /* measured (Aurora, 2026-09-19): WaitIoConcerning / WaitIoCritical (see the constants). */
        [PgTargetFactKeys.WaitKey("IO", null)] = (WaitIoConcerning, WaitIoCritical),
        /* measured (Aurora, per event, 2026-09-20): WaitStandoutConcerning / WaitStandoutCritical (see the constants). */
        [PgTargetFactKeys.WaitKey("Lock", "relation")] = (WaitStandoutConcerning, WaitStandoutCritical),
        [PgTargetFactKeys.WaitKey("LWLock", "WALWrite")] = (WaitStandoutConcerning, WaitStandoutCritical),
        [PgTargetFactKeys.WaitKey("IO", "DataFileRead")] = (WaitStandoutConcerning, WaitStandoutCritical),
        [PgTargetFactKeys.WaitKey("IO", "WALSync")] = (WaitStandoutConcerning, WaitStandoutCritical),
    };

    /// <summary>The (concerning, critical) fraction bars for a PostgreSQL wait key, or null when the key is
    /// not in the v1 vocabulary — the twin of <c>FactScorer.GetWaitThresholds</c>'s null-for-unknown.</summary>
    public static (double Concerning, double Critical)? GetPgWaitThresholds(string key) =>
        s_waitBars.TryGetValue(key, out var bars) ? bars : null;

    /// <summary>
    /// The fraction graded through the shared formula: 0 for a key outside the vocabulary, 0 when the wait
    /// source observed no time, 0 for a sampled fact under <see cref="WaitSampledMinimumSamples"/>, 0 for a
    /// rollup whose named standout fired (the class remarks), 0 below the concerning bar (not fired), 0.5 at
    /// it, 1.0 at critical. Every graded fact is stamped: <c>threshold_lineage = 1</c> for an Aurora fact — a
    /// rollup graded on its measured type bars, a standout graded on its measured event bars (2026-09-20), or a
    /// rollup that yielded on the measured standout bar; 0 for any sampled fact (the stock estimate is another
    /// population, class remarks).
    /// </summary>
    private static partial double ScoreWaitFact(Fact fact)
    {
        var bars = GetPgWaitThresholds(fact.Key);
        if (bars is null) return 0.0;

        if (fact.Metadata.GetValueOrDefault(WaitSourceObservedMsKey) <= 0) return 0.0;

        /* unmeasured: the sampled-resolution floor, WaitSampledMinimumSamples (see the constant). */
        if (fact.Metadata.GetValueOrDefault(WaitIsSampledKey) > 0
            && fact.Metadata.GetValueOrDefault(WaitDeltaSamplesKey) < WaitSampledMinimumSamples)
            return 0.0;

        /* The verdict on the bars, decided once for both exits below: every bar in s_waitBars is measured on the
           Aurora population (the type bars 2026-09-19, the event bars 2026-09-20), so an Aurora fact says 1
           whichever row grades it; a sampled fact is the stock estimate the calibration did not read — another
           instrument on another population — and says 0 on the same bars. The fact says which for get_analysis_facts. */
        var isMeasuredPopulation = fact.Metadata.GetValueOrDefault(WaitIsSampledKey) <= 0;

        /* measured: the standout bar, WaitStandoutConcerning (see the constant) — a rollup whose named standout
           is itself a finding yields the story to it (class remarks: one wait is graded once). The number that
           decided here is the standout bar, so the yielded rollup carries the same verdict a standout would. */
        if (fact.Metadata.GetValueOrDefault(WaitIsStandoutKey) <= 0
            && fact.Metadata.GetValueOrDefault(WaitMaxStandoutFractionKey) >= WaitStandoutConcerning)
        {
            fact.Metadata["threshold_lineage"] = isMeasuredPopulation ? 1 : 0;
            return 0.0;
        }

        /* The fraction ramp (see the constants behind s_waitBars). Below concerning the fact scores 0, not the shared
           formula's fraction — see the class remarks on what "fired" means for a PostgreSQL wait. */
        fact.Metadata["threshold_lineage"] = isMeasuredPopulation ? 1 : 0;
        if (fact.Value < bars.Value.Concerning) return 0.0;
        return FactScorer.ApplyThresholdFormula(fact.Value, bars.Value.Concerning, bars.Value.Critical);
    }

    /* unmeasured: chosen, not measured — calibrate against pg_wait_stats before the next release. One boost
       for every wait co-fire: the wait is the SYMPTOM and the co-firing fact is the mechanism the design names
       for it (design §5 chains), so the corroboration lifts a concerning wait (0.5) to 0.65 — above the story
       line, well short of the 1.5 notify line; a wait becomes a page on its own fraction, never on a co-fire. */
    public const double WaitCoFireBoost = 0.3;

    /// <summary>
    /// The wait family's Layer-2 amplifiers — the design's three co-fires, plus the v2 hook. Every predicate
    /// reads the other fact's <c>BaseSeverity &gt; 0</c> (fired, per each family's own bar), never a size of
    /// its own. <c>Lock</c> and <c>Lock:relation</c> co-fire with <c>PG_CONNECTION_SATURATION</c> (parked and
    /// queued sessions hold what the waiters want) and with <c>PG_IDLE_IN_TRANSACTION</c> (a v2 fact, declared
    /// so the edge is written against a constant; inert until it exists). <c>IO:WALSync</c> and
    /// <c>LWLock:WALWrite</c> co-fire with <c>PG_CHECKPOINT_PRESSURE</c> (WAL volume is forcing checkpoints and
    /// every commit is queued behind the same WAL). <c>IO:DataFileRead</c> co-fires with
    /// <c>PG_BUFFER_CACHE_PRESSURE</c> (the reads are misses). The rollups other than <c>Lock</c> and the
    /// remaining keys have no amplifier in v1.
    /// </summary>
    private static partial List<AmplifierDefinition> WaitAmplifiers(string key)
    {
        if (key == PgTargetFactKeys.WaitKey("Lock", null) || key == PgTargetFactKeys.WaitKey("Lock", "relation"))
        {
            return
            [
                new AmplifierDefinition
                {
                    Description = "PG_CONNECTION_SATURATION co-fired — sessions are at the connection ceiling, and parked or queued sessions hold the locks these backends are waiting for",
                    /* unmeasured: WaitCoFireBoost (see the constant). */
                    Boost = WaitCoFireBoost,
                    Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ConnectionSaturation, out var saturation) && saturation.BaseSeverity > 0,
                },
                new AmplifierDefinition
                {
                    Description = "PG_IDLE_IN_TRANSACTION co-fired — a parked transaction is holding locks it is not using",
                    /* unmeasured: WaitCoFireBoost (see the constant). */
                    Boost = WaitCoFireBoost,
                    Predicate = facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0,
                },
            ];
        }

        if (key == PgTargetFactKeys.WaitKey("IO", "WALSync") || key == PgTargetFactKeys.WaitKey("LWLock", "WALWrite"))
        {
            return
            [
                new AmplifierDefinition
                {
                    Description = "PG_CHECKPOINT_PRESSURE co-fired — WAL volume is forcing checkpoints ahead of checkpoint_timeout, and commits are queued behind the same WAL",
                    /* unmeasured: WaitCoFireBoost (see the constant). */
                    Boost = WaitCoFireBoost,
                    Predicate = facts => facts.TryGetValue(PgTargetFactKeys.CheckpointPressure, out var pressure) && pressure.BaseSeverity > 0,
                },
            ];
        }

        if (key == PgTargetFactKeys.WaitKey("IO", "DataFileRead"))
        {
            return
            [
                new AmplifierDefinition
                {
                    Description = "PG_BUFFER_CACHE_PRESSURE co-fired — the data file reads these backends wait on are buffer-cache misses",
                    /* unmeasured: WaitCoFireBoost (see the constant). */
                    Boost = WaitCoFireBoost,
                    Predicate = facts => facts.TryGetValue(PgTargetFactKeys.BufferCachePressure, out var pressure) && pressure.BaseSeverity > 0,
                },
            ];
        }

        return [];
    }
}
