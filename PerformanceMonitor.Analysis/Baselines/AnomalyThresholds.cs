/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// The anomaly-detector tuning constants shared by the two active detectors (Lite
/// <c>AnomalyDetector</c>, Darling <c>PgAnomalyDetector</c>) so they cannot drift. The detector
/// METHOD BODIES stay per-store (DuckDB / Npgsql SQL + binding); every numeric threshold, floor,
/// fallback bar, and sentinel is single-sourced here. The deprecated Dashboard keeps its own copy.
/// </summary>
public static class AnomalyThresholds
{
    /// <summary>
    /// Default number of standard deviations above baseline mean to flag as anomalous.
    /// </summary>
    public const double DefaultDeviationThreshold = 2.0;

    /// <summary>
    /// #1743: modified z-score cutoff for the robust (median/MAD) path — the standard 3.5
    /// convention, and CALIBRATED, not copied: measured against 52 production replicas' 24h of
    /// samples, modified-z at 3.5 traded +284 genuine catches for 7 misses against classical z at
    /// the SAME cutoff, and caught a busy tenant's real sustained evening surge (17 samples) that
    /// classical z missed at every threshold because the server's own history had inflated its
    /// stddev. Do NOT pair the robust statistic with the classical 2.0 — that quadruples firing
    /// volume; the statistic and its cutoff move together.
    /// </summary>
    public const double ModifiedZThreshold = 3.5;

    /// <summary>
    /// #1743: modified z-score cutoff for the HEAVY-TAILED families (waits, query duration) whose
    /// medians are small by nature so ordinary bursts sit many robust-sigmas out. Measured on the
    /// production fleet at 24h/42K samples with the honest ms-per-sec normalization and the
    /// 250 ms/sec floor AND-gated: 3.5 → 1,465 fires, 5.0 → 1,088, 7.0 → 744, against the ratio
    /// detector's 662 — and the ratio detector caught NOTHING modified-z missed at any of those
    /// cutoffs (strict containment). 5.0 keeps the strict-superset property with a sane volume;
    /// the existing floors stay AND-ed exactly as the ratio path had them.
    /// </summary>
    public const double HeavyTailModifiedZThreshold = 5.0;

    /// <summary>
    /// #1743: the modified-z cutoff for a metric — 5.0 for the heavy-tailed families whose medians
    /// are small by nature (waits in both grains, query duration: fleet-measured skew up to 67.9x
    /// stddev-vs-robust-sigma on waits, 3.13x mean-over-median on query duration), the standard 3.5
    /// for everything else. Shared here so Lite and Darling cannot calibrate apart.
    /// </summary>
    public static double ModifiedZThresholdFor(string metricName) => metricName switch
    {
        MetricNames.WaitStats => HeavyTailModifiedZThreshold,
        MetricNames.WaitMsPerSec => HeavyTailModifiedZThreshold,
        MetricNames.QueryDuration => HeavyTailModifiedZThreshold,
        _ => ModifiedZThreshold,
    };

    /// <summary>
    /// #1743: the modified-z cutoff SCALED by the operator's per-metric classical threshold, so the
    /// SetDeviationThreshold knob keeps its meaning on the robust path — at the shipped 2.0 default
    /// the factor is 1 and the calibrated cutoffs apply unchanged; an operator who cranks a
    /// metric's threshold N-fold scales its robust cutoff N-fold too (and a lowered one lowers it,
    /// the same proportional semantics the classical gate always had). Without this the knob went
    /// silently dead the moment a metric gained robust statistics.
    /// </summary>
    public static double ModifiedZThresholdFor(string metricName, double configuredDeviationThreshold) =>
        ModifiedZThresholdFor(metricName) * configuredDeviationThreshold / DefaultDeviationThreshold;

    /// <summary>
    /// Default ratio threshold for the wait-profile detector (peak window all-types ms/sec ÷ baseline
    /// mean). On the HONEST per-second scale now, so far below the old 5.0 that assumed a ~240x-inflated
    /// input; matches the FactScorer WaitProfileRatioFloor.
    ///
    /// <para>MEASURED, 2026-09-22 (#3871 rider; the read #3538 A5 said would calibrate it): the ratio's
    /// own distribution over 3,655 non-zero windows across two store classes over 14 days reads
    /// p99 = 3.14 and p99.9 = 28.3, with 0.63% of windows above this bar — so 4.0 sits just past the
    /// p99 and the bar's population is the storm tail it was guessed for. VALUE UNCHANGED by the
    /// measurement; facts that gate on it stamp <c>threshold_lineage = 1</c> so a reader can tell a
    /// measured bar from an inherited one.</para>
    /// </summary>
    public const double DefaultRatioThreshold = 4.0;

    /// <summary>
    /// Default ratio threshold for event-based anomaly detection (blocking/deadlocks).
    /// </summary>
    public const double DefaultEventRatioThreshold = 3.0;

    // #1486 absolute-magnitude floors (the z-path sanity ceiling) so a z-score against a thin
    // baseline can't surface a trivial value; sigma display cap so a variance-collapsed baseline
    // can't render millions-of-sigma.
    public const double CpuFloorPct = 50.0;                // %
    public const double ReadLatencyFloorMs = 10.0;         // ms
    public const double BatchRequestFloor = 500.0;         // requests/sec
    public const double SessionCountFloor = 50.0;          // connections
    public const double QueryDurationFloorUs = 1_000_000;  // total elapsed us = 1 second
    public const double MemoryPressureFloorPct = 90.0;     // total/target %
    public const double WriteLatencyFloorMs = 20.0;        // ms, was 5
    public const double SigmaDisplayCap = 25.0;

    // Low-quality-baseline ABSOLUTE-FALLBACK bars: when the baseline is too thin to trust a z-score
    // (BaselineBucket.IsTrustworthy false), the detector fires on these instead of going silent.
    // Each is deliberately HIGHER than the matching #1486 magnitude floor above (the interaction
    // trap: a young store fires only on the higher bar, never on both-AND-ed into blindness).
    public const double CpuFallbackPct = 90.0;                 // %
    /* #1996: 101, not 95, because memory pressure's HEALTHY steady state is ~100 — a warmed-up
       SQL Server holds total ≈ target by design, and the production fleet's median is exactly
       100.0 on every server. A bar at or below 100 therefore fires on NORMAL behavior whenever a
       baseline bucket is untrustworthy: measured on the 52-replica monitor, 403 of 406 firing
       (server, hour, dow) buckets sat at exactly 2 distinct days (under the Full-tier day floor of
       3, with ~82 samples each), and every one of 1,279 findings across two eras read "spiked to
       100% — 0σ above its 100% baseline". Above 100 means total EXCEEDS target — genuine
       over-target pressure, the only absolute condition worth waking someone for on this metric.
       Still strictly above the 90 magnitude floor, per the interaction-trap rule below. */
    public const double MemoryPressureFallbackPct = 101.0;     // total/target %
    public const double BatchRequestFallback = 5000.0;        // requests/sec
    public const double SessionCountFallback = 500.0;         // connections
    public const double QueryDurationFallbackUs = 5_000_000;  // total elapsed us = 5 seconds
    public const double IoLatencyFallbackMs = 50.0;           // ms (read and write)

    // Wait-profile detector (DetectWaitAnomalies → one ANOMALY_WAIT_PROFILE): the current window's
    // all-types wait ms/sec (PEAK across collections, matching the z-detectors) is compared to the
    // WaitMsPerSec baseline. DefaultRatioThreshold and the FactScorer wait slope are on the HONEST
    // per-second scale now (the old 5×/20× was calibrated to a ~240×-inflated per-hour-vs-per-interval
    // input) — a sensible starting point, still uncalibrated: see DefaultRatioThreshold for what the
    // 2026-09 fleet pass measured instead and which read would calibrate these.
    public const double WaitProfileFallbackMsPerSec = 250.0;  // untrustworthy-baseline absolute bar
    public const double NoBaselineRatio = 100.0;             // scoring sentinel for a first-occurrence (is_new)

    // Day-over-day object/index detection (delta-based, not stddev-baseline) since the
    // index_object_stats collector runs daily and its counters are cumulative. Emits
    // ANOMALY_OBJECT_GROWTH for the biggest table grower over threshold and ANOMALY_OBJECT_CONTENTION
    // for the index with the largest new lock-wait time.
    public const decimal ObjectGrowthMbThreshold = 100m;   // ignore tables that grew less than 100 MB
    public const double ObjectGrowthPctThreshold = 20.0;   // ...and less than 20% day-over-day
    public const long ObjectLockWaitMsDeltaThreshold = 60000; // 1 minute of new lock waits

    /* ── #3542 lane 9: the PostgreSQL-TARGET detectors' floors and bars (PgTargetAnomalyDetector). ──

       These inherit the SQL Server detectors' METHOD — the #1486 magnitude floor as the z-path's sanity ceiling,
       the strictly-higher absolute-fallback bar for an untrustworthy baseline, the two never AND-ed into
       blindness — and NONE of their numbers (#3538 A5 via #3605: "no PostgreSQL bar may be a SQL Server constant
       reused by value"). 500 requests/sec and 50 connections are SQL Server magnitudes: a PostgreSQL default
       install caps connections at max_connections = 100, so SessionCountFallback (500) would sit above most
       ceilings and never fire, and the connection-saturation family would have said everything first.

       Lineage, after the #3691 fleet calibration of 2026-09-19 (14 days of 1-minute counters pre-bucketed to 5-minute
       rates, 7 days of session captures and wait stats, over 50 Aurora PostgreSQL clusters of the dogfood fleet —
       the whole PostgreSQL side): the TPS floor and fallback, the CPU floor and fallback, the deadlock-rate floor
       and the wait-profile bar are MEASURED and each constant names the percentile it sits at and the population
       (Aurora; where the quantity is engine-neutral the stock-PostgreSQL population is not yet measured). The
       session-count floors are UNMEASURED in count terms (the read was sessions over each server's ceiling). The
       second calibration read (2026-09-20, the same 50 clusters, 28 days: hour-of-week centres from the first 21,
       ratios over the last 7) placed the ratio families' firing multiple, and the THIRD (2026-09-21, the same
       clusters, statement-stats windows fenced at 2026-09-19 16:40Z) placed it on the two per-statement series the
       v3 lanes built and measured the server-wide per-call mean to be blind to a per-statement step — see
       PgRatioAnomalyThreshold for the per-family verdicts, which differ. The detector stamps threshold_lineage = 1 on the TPS and CPU anomalies,
       whose every bar is measured (they are z-detectors and never grade on the multiple), and 0 on the session,
       deadlock-rate and wait-profile anomalies, each of which is still gated on at least one chosen number: the
       session COUNT floors; for the ratio families the heavy-tail cutoff (the SQL Server fleet's, by reference) and
       the scorer's own ramp spans (PgTargetScorer.RatioAnomalySaturation, FirstOccurrenceExceedanceSpan), which
       no read has placed (see PgTargetAnomalyDetector and PgTargetScorer.Anomaly.cs). The deadlock-rate fallback is
       an alias of a MEASURED bar and says so. */

    /// <summary>Magnitude floor for the TPS z-detector: below fifty transactions a second a deviation is a
    /// quiet server's noise however many sigmas it reads. measured: ≈ the fleet p75 of per-server p50 TPS over
    /// 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 (pg_database_stats; per-server p50
    /// 6.5 – 79, median 31) — a median cluster's deviations are gated out at its routine load and admitted at its
    /// spikes, which is what a floor under a baselined detector is for. Engine-neutral quantity, Aurora population;
    /// the stock-PostgreSQL population is not yet measured.</summary>
    public const double PgTpsFloor = 50.0;                    // transactions/sec

    /// <summary>Absolute-fallback bar for TPS on an untrustworthy baseline: ten times the floor, so a young store
    /// fires only on a rate that is large on any PostgreSQL. measured: above every cluster's p99 (per-server p99
    /// 6.8 – 205, median 59) and below the routine 5-minute bursts (per-server 14-day maximum median 1,710 — about
    /// fifty times its own p50, which is why the detector is hour-of-week baselined and not absolute), over 14 days
    /// × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19. Engine-neutral quantity, Aurora population;
    /// the stock-PostgreSQL population is not yet measured.</summary>
    public const double PgTpsFallback = 500.0;                // transactions/sec

    /// <summary>Magnitude floor for the session-count z-detector, a COUNT because a fraction of max_connections is
    /// meaningless as a constant (the ceiling is per server, and the detector cannot see the config fact — the
    /// ceiling-aware arm is PG_CONNECTION_SATURATION, into which this anomaly folds). Twenty is a fifth of the
    /// shipped default ceiling. unmeasured: chosen, not measured — unmeasured in COUNT terms after the 2026-09-19
    /// calibration, which read sessions as a fraction of each server's usable ceiling (ceilings 1,240 – 5,000;
    /// per-server p99 of the fraction median 0.021, fleet maximum 0.103) and not as a count distribution;
    /// calibrate against the per-capture total_sessions (numbackends from V133) distribution in pg_session_states
    /// before the next release.</summary>
    public const double PgSessionCountFloor = 20.0;           // sessions

    /// <summary>Absolute-fallback bar for session count on an untrustworthy baseline: twice the shipped default
    /// max_connections, so it fires only on a pool that has been raised well past the default — on a default
    /// ceiling the saturation fact is the never-blind arm and this one stays quiet by design (it would be the
    /// same finding twice). unmeasured: chosen, not measured — unmeasured in count terms for the same reason as the
    /// floor above; calibrate against pg_session_states before the next release.</summary>
    public const double PgSessionCountFallback = 200.0;       // sessions

    /// <summary>Magnitude floor for the Aurora CPU z-detector, on <c>acu_utilization_percent</c> — percent of
    /// the CONFIGURED capacity ceiling (#3281), never the percent-of-allocated raw reading. Under forty percent
    /// of the ceiling the instance has more headroom than it is using, whatever its own history says.
    /// measured: between the median cluster's routine load and its tail — per-server p50 median 6.3 % and p99
    /// median 49 % of the configured ceiling over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet,
    /// 2026-09-19 (pg_cpu_utilization, 5-minute rates) — so a deviation under the floor is inside what the median
    /// cluster does in its busiest percent of samples. Aurora Serverless v2 population, which is the whole
    /// population of this metric.</summary>
    public const double PgCpuFloorPct = 40.0;                 // % of configured capacity

    /// <summary>Absolute-fallback bar for Aurora CPU on an untrustworthy baseline: the fleet card's own Critical
    /// line on the same quantity (ServerHealthThresholds.CpuCriticalPercent — repeated because this assembly
    /// references nothing; PgTargetAnomalyTests pins the pair equal), so a young store's CPU anomaly and its card
    /// cannot disagree about the colour of one minute. measured: ≈ p99.9 of 5-minute acu_utilization_percent
    /// samples over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 (samples at or above
    /// 95: median cluster 0.1 %, worst 1.2 %; see PgTargetScorer.CpuCapacityCriticalPercent for the same read).
    /// Aurora Serverless v2 population.</summary>
    public const double PgCpuFallbackPct = 95.0;              // % of configured capacity

    /// <summary>The Aurora wait-profile detector's magnitude floor AND absolute-fallback bar (one number on both
    /// paths, the SQL Server wait profile's shape): all-types wait milliseconds per second, CPU excluded — 500 is
    /// half of one backend continuously waiting. measured: about eight times the per-server p99 of total non-CPU
    /// waiting (median p99 0.062 ms per ms observed = 62 ms/s; fleet p90 of p99 0.82 = 820 ms/s; maximum 10.7) over
    /// 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 (pg_wait_stats, 5-minute buckets) —
    /// the bar sits between the median cluster's tail and the fleet-p90 cluster's, so a young store fires only on
    /// a rate the majority of the fleet never reaches in its worst percent of buckets. Aurora population (the engine's measured
    /// wait deltas). Stock PostgreSQL's SAMPLED
    /// estimate is another instrument with its own arm and its own bar since lane 24 —
    /// <see cref="PgSampledWaitProfileFallbackMsPerSec"/> below, the same figure stated on its own lineage.</summary>
    public const double PgWaitProfileFallbackMsPerSec = 500.0;   // ms of waiting per second of observed time

    // lane 24 (#3691): the stock SAMPLED wait-profile detector's magnitude floor and fallback bar (PgTargetAnomalyDetector.WaitsSampled.cs).

    /// <summary>The stock sampled wait-profile detector's magnitude floor AND absolute-fallback bar, one number on
    /// both paths like its Aurora sibling: sampled wait milliseconds per second the sampler was WATCHING
    /// (<c>Δsamples × profile_period_ms</c> over <c>sampled_ms</c>, V133; NULL = the whole interval), CPU/Running
    /// excluded — 500 is half of one backend continuously seen waiting. The same figure as
    /// <see cref="PgWaitProfileFallbackMsPerSec"/> and deliberately NOT an alias of it: the two instruments must
    /// stay free to calibrate apart. unmeasured: chosen, not measured — the 2026-09-19 calibration read only the
    /// Aurora population's exact deltas (<c>pg_wait_stats</c>); the stock fleet's <c>pg_wait_sampling</c> rate at
    /// the honest <c>sampled_ms</c> denominator has no distribution yet (every pre-V133 row is NULL there), so
    /// calibrate against the per-server per-collection sampled rate over <c>pg_wait_sampling</c> before the next
    /// release. Population 0 on the dogfood fleet as of 2026-09-20 (the second calibration read: every cluster is
    /// Aurora with <c>pg_wait_stats</c>, none runs <c>pg_wait_sampling</c> or the service sampler, and the table had
    /// zero rows fleet-wide) — unmeasured until a stock target joins the fleet; the note is here so nobody re-reads an
    /// empty table for it. Lane 9 left this constant undeclared while nothing read it; the detector that reads it
    /// is lane 24's, and its facts carry <c>threshold_lineage = 0</c>.</summary>
    public const double PgSampledWaitProfileFallbackMsPerSec = 500.0;   // sampled ms of waiting per second the sampler watched

    /// <summary>The deadlock-rate ratio detector's magnitude floor, a RATE per observed hour (#3538 A7: never a
    /// count over the window, which would scale with hours_back) — under one deadlock an hour the ratio is the
    /// arithmetic of a near-empty numerator. measured: over 14 days × 50 Aurora PostgreSQL clusters of the dogfood
    /// fleet, 2026-09-19 (pg_database_stats deadlock deltas), 40 clusters had no deadlock at all and the other 10
    /// had 1 – 4 in 14 days with at most 2 in any hour — so this floor is the bar that actually fires on this
    /// population (0.3 – 0.9 % of hours on those 10, as a first occurrence), while the 5/h and 20/h tiers sit in
    /// the empty interval. Engine-neutral quantity, Aurora population; the stock-PostgreSQL population is not yet
    /// measured.</summary>
    public const double PgDeadlockRateFloorPerHour = 1.0;     // deadlocks per observed hour

    /// <summary>The deadlock-rate detector's absolute-fallback bar on an untrustworthy baseline: the Warning tier
    /// the PG_DEADLOCK_RATE fact itself grades at, BY REFERENCE — measured (p99.94 of deadlocks per server-hour,
    /// 14 days × 43 servers, #3368; see PgTargetScorer.DeadlockWarnPerHour). A young store's deadlock anomaly then
    /// fires exactly when the regular fact does and folds into it, which is the honest reading of "no baseline
    /// yet": the alert band, not a made-up multiple.</summary>
    public const double PgDeadlockRateFallbackPerHour = PgTargetScorer.DeadlockWarnPerHour;

    /// <summary>The PostgreSQL ratio families' firing multiple (deadlock rate; the wait profile's classical
    /// trigger): the window's per-hour or per-second rate over the same hour-of-week's baseline mean. Three is
    /// the multiple at which a tripled rate against its own history is a workload change rather than the
    /// bucket's dispersion. The SQL Server event-ratio detector happens to use the same multiple, and this is
    /// NOT an alias of it: the two instruments must stay free to calibrate apart. measured: the hour-of-week
    /// RATIO distributions (each 5-minute sample over its bucket's mean — the detector's centre; centres from the
    /// first 21 days, ratios over the last 7; 100,800 samples) over 28 days × 50 Aurora PostgreSQL clusters of the
    /// dogfood fleet, 2026-09-20, and the verdict differs by family, so it is stated per family:
    /// <list type="bullet">
    /// <item><description><b>TPS</b> (pg_database_stats): ratio p99 1.32, p99.9 1.65, p99.99 24.4, maximum 296;
    /// share at or above 3.0 = 0.024 % — about one 5-minute sample per server per week — so 3.0 sits at ≈ p99.97
    /// of TPS ratios: well placed. Stated for the record: the TPS anomaly is a z-detector on PgTpsFloor /
    /// PgTpsFallback and never grades on this multiple, so nothing flips on it.</description></item>
    /// <item><description><b>Wait profile</b> (pg_wait_stats, total non-CPU rate): ratio p99 12.9, p99.9 31,
    /// maximum 139; share at or above 3.0 = <b>7.1 %</b>. A quiet bucket's mean is tiny, so the wait rate's
    /// hour-of-week ratio is heavy-tailed by nature and the multiple ALONE is routine at 3.0 (≈ p93 of wait-rate
    /// ratios) — measured-inadequate alone. What keeps the wait-profile anomaly quiet is the magnitude floor
    /// (PgWaitProfileFallbackMsPerSec, 500 = 0.5 ms/ms, measured at 8× the fleet p99 total non-CPU rate of
    /// 0.062 ms/ms) and, since #3780, the peak-AND-mean gate: the multiple is asked of both statistics, the bar
    /// of the peak. The wait-profile fact stays at threshold_lineage = 0 regardless — its robust arm is decided
    /// on HeavyTailModifiedZThreshold, the SQL Server fleet's cutoff by reference, and the scorer's ramp spans
    /// are chosen (PgTargetScorer.Anomaly.cs).</description></item>
    /// <item><description><b>Deadlocks/h</b>: a measured EMPTY interval on a near-empty population — only 22 of
    /// 8,400 (server, bucket) pairs (0.26 %) have a non-zero centre at all, and no ratio reached 3 in 7 days;
    /// the deadlock anomaly is the first-occurrence (is_new) arm in practice. Twenty-two pairs place nothing, so
    /// the deadlock-rate fact keeps threshold_lineage = 0 (the ratio arm's multiple is unplaced there, and the
    /// scorer's saturation span is chosen).</description></item>
    /// </list>
    /// The THIRD read (2026-09-21, the same 50 clusters, 28 days of <c>pg_statement_stats</c> FENCED at
    /// <b>2026-09-19 16:40</b>Z — the table died on 23 of 50 clusters at 16:44Z on a schema regression, so every
    /// cluster contributes a full window that ends before it) placed the multiple on the two PER-STATEMENT series
    /// the v3 lanes built, and the verdict again differs by series:
    /// <list type="bullet">
    /// <item><description><b>Per-statement SHARE</b> of the collection's execution time against the statement's own
    /// hour-of-week share (lane 34's <c>ANOMALY_PG_BAD_ACTOR_SHARE</c>): over 1,487 statement-hours with a centre,
    /// top-5 statements per server, ratio p50 1.00, p99 2.53, p99.9 49, maximum 72; share at or above 3.0 = 0.94 %,
    /// and 0.40 % with the peak magnitude floor (PgTargetScorer.BadActorShareConcerning) also required — so 3.0 sits
    /// at ≈ p99.1 of statement-share ratios, ≈ p99.6 with the floor: well placed. Stated for the record, as TPS is:
    /// that anomaly is a Z-detector on the shared sigma cutoffs and reads this multiple NOWHERE, so this placement
    /// flips no flag — see PgTargetAnomalyDetector.Queries.cs for the bars it does grade
    /// on.</description></item>
    /// <item><description><b>Per-statement per-call MEAN ms</b> against its own hour-of-week mean, keyed
    /// (<c>ANOMALY_PG_PLAN_REGRESSION</c> since #3691 lane 39): 1,481 statement-hours over 49 servers, ratio p50
    /// 0.99, p99 2.0, p99.9 57, maximum 90; share at or above 3.0 = 0.74 % → 3.0 ≈ p99.3 of keyed ratios, well
    /// placed. The level: p50 8 ms, p99 1,092 ms.</description></item>
    /// <item><description><b>The SERVER-WIDE per-call mean</b> (the same anomaly's arm before lane 39, now its cold
    /// fallback): 352 server-hours, ratio p99 2.05 and <b>maximum 4.4 — never at or above 3.0</b> in the measured
    /// week (share 0.28 %), level p50 1.0 ms, p99 12 ms. Measured-INADEQUATE, and not inadequate at the margin: a
    /// per-statement 90× step dilutes to nothing in a server's mean over every statement. This is the read that
    /// moved the anomaly onto the keyed series and demoted this one to the no-history fallback.</description></item>
    /// </list>
    /// Engine-neutral quantities (ratios of a server to itself), Aurora population; the stock-PostgreSQL
    /// population is not yet measured.</summary>
    public const double PgRatioAnomalyThreshold = 3.0;

    /* ── lane 11 (#3691): the I/O read-latency z-detector (PgTargetAnomalyDetector.Io.cs). ──
       Unlike the lane-9 block above these two are MEASURED — the #3691 calibration pass (measurements-3691.md §B1)
       read hourly ms per data-file read over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet,
       2026-09-19 — and the detector stamps threshold_lineage = 1 on the facts it fires, with the population named
       in PgTargetScorer.Io.cs: these are Aurora-storage shapes, and stock local-disk PostgreSQL is a different
       population. */

    /// <summary>Magnitude floor for the read-latency z-detector, milliseconds per read: under two milliseconds a
    /// deviation is fast storage's jitter however many sigmas it reads. measured: the fleet's per-server p50 of
    /// hourly ms per read is 1.0–10 ms with a median of 1.29 ms over 14 days × 50 Aurora PostgreSQL clusters of the
    /// dogfood fleet, 2026-09-19 — two milliseconds sits above the median server's routine and below every
    /// server's p99 (§B1).</summary>
    public const double PgIoLatencyFloorMs = 2.0;              // ms per data-file read

    /// <summary>Absolute-fallback bar for read latency on an untrustworthy baseline: twenty milliseconds, the
    /// midpoint between the regular fact's measured WARNING (10 ms ≈ fleet p90 of per-server p99) and CRITICAL
    /// (30 ms ≈ fleet max) bars in <c>PgTargetScorer.Io.cs</c> — so a young store's latency anomaly fires only on
    /// a window the regular fact already grades as a finding, and folds into it, without being that finding
    /// twice at the warning line. measured: derived from the two §B1 bars named, 14 days × 50 Aurora PostgreSQL
    /// clusters of the dogfood fleet, 2026-09-19; <c>PgTargetIoTests</c> pins it strictly between them.</summary>
    public const double PgIoLatencyFallbackMs = 20.0;          // ms per data-file read

    // lane 15 (#3691 step 15): the WAL-volume z-detector's floor and fallback (PgTargetAnomalyDetector.Wal.cs).

    /// <summary>Magnitude floor for the WAL-volume z-detector, in WAL bytes per second per collection: below one
    /// mebibyte a second a deviation is a quiet server's write ripple however many sigmas it reads — a 30 KB/s
    /// server tripling to 90 KB/s is not a workload event. unmeasured: chosen, not measured — and NOT calibratable
    /// on the current fleet: every PostgreSQL target the product monitors today is Aurora, and Aurora does not
    /// populate <c>pg_stat_wal</c> (fifty clusters, fourteen days, <c>wal_bytes</c> = 0 throughout — #3691
    /// calibration §A7), so the detector never reaches this bar there. Calibrate against the per-collection
    /// <c>pg_write_stats</c> WAL rate of a stock population before the next release that monitors one; the
    /// detector stamps <c>threshold_lineage = 0</c> until then.</summary>
    public const double PgWalBytesFloorPerSec = 1024.0 * 1024.0;          // 1 MiB of WAL per second

    /// <summary>Absolute-fallback bar for WAL volume on an untrustworthy baseline: sixteen times the floor, so a
    /// young store fires only on a rate that is large on any PostgreSQL — 16 MiB/s fills the shipped 1 GB
    /// <c>max_wal_size</c> in about a minute, a fifth of the default <c>checkpoint_timeout</c>, so it is the rate
    /// at which a default-configured server is already checkpointing on volume. unmeasured: chosen, not
    /// measured — the same un-calibratable population as the floor (§A7); calibrate against a stock
    /// population's upper tail before the next release that monitors one.</summary>
    public const double PgWalBytesFallbackPerSec = 16.0 * 1024.0 * 1024.0;  // 16 MiB of WAL per second

    // lane 17 (#3691, wave 3): the blocked-sessions z-detector's floor and fallback (PgTargetAnomalyDetector.Blocking.cs).

    /// <summary>Magnitude floor for the blocked-sessions z-detector, a COUNT of distinct blocked sessions in one
    /// <c>pg_blocking</c> capture. The baseline is zero-heavy by construction (every capture the collection log says
    /// ran and found no edge is a zero sample), so its dispersion collapses toward the bucket's absolute floor and
    /// ONE blocked session on a server that never blocks would read as many sigmas; under three sessions queued at
    /// once a deviation is one lock handoff caught mid-flight, whatever the arithmetic says. unmeasured: chosen, not
    /// measured — the 2026-09-19 calibration took no distribution over pg_blocking_edges (its approximate row count
    /// read 0, the unanalysed-table trap); calibrate against COUNT(DISTINCT blocked_pid) per capture in
    /// pg_blocking_edges before the next release. The detector stamps <c>threshold_lineage = 0</c>.</summary>
    public const double PgBlockedSessionsFloor = 3.0;                     // distinct blocked sessions in one capture

    /// <summary>Absolute-fallback bar for blocked sessions on an untrustworthy baseline — a server whose 30 days
    /// hold no blocking at all has a zero-activity bucket (mean 0, stddev 0) and takes THIS path, so the bar decides
    /// what a young or never-blocking server's first blocking storm must look like to fire: ten sessions queued at
    /// once is a storm on any PostgreSQL, and a smaller chain is PG_BLOCKING_CHAIN's to grade on its duration.
    /// unmeasured: chosen, not measured — same unread table as the floor; calibrate against pg_blocking_edges before
    /// the next release.</summary>
    public const double PgBlockedSessionsFallback = 10.0;                 // distinct blocked sessions in one capture

    // lane 27 (#3691, v3): the per-call statement-mean z-detector's floor and fallback (PgTargetAnomalyDetector.Plans.cs).
    // Lane 39 pointed that detector at the KEYED per-queryid series and kept the server-wide one as its cold fallback;
    // these two bars are shared by both arms (the floor on each, the fallback only on the server-wide arm, whose
    // trusted-path sibling passes double.PositiveInfinity because the keyed arm refuses to grade without a bucket).

    /// <summary>Magnitude floor for the <c>pg_statement_mean_ms</c> z-detector — mean execution ms per statement call
    /// per collection (Σ <c>delta_total_exec_time_ms</c> ÷ Σ <c>delta_calls</c>), asked of ONE statement on the keyed
    /// arm and of every statement on the server-wide fallback, in ms. Under ten milliseconds per call a statement is
    /// fast whatever the sigma says: an OLTP mix's calls sit at single-digit ms, and a doubling of 2 ms is a warmer
    /// cache, not a regression anyone can feel. unmeasured: chosen, not measured — the 2026-09-19 calibration read
    /// statement SHARES and busy fractions from pg_statement_stats, never a mean-ms series, and the 2026-09-21 read
    /// (28 days FENCED at 2026-09-19 16:40Z, 50 Aurora PostgreSQL clusters — §D5) read the LEVELS but placed no
    /// floor on them: keyed per-statement per-call mean p50 8 ms / p99 1,092 ms, server-wide p50 1.0 ms / p99 12 ms.
    /// Ten milliseconds therefore sits just above the median statement's per-call cost and around the server-wide
    /// p99, which is a different admission rate on each arm; that is the thing to calibrate — pick the percentile of
    /// the KEYED level distribution this floor should sit at before the next release, now that the distribution
    /// exists. The detector stamps <c>threshold_lineage = 0</c> on both arms. NOT the SQL Server
    /// <see cref="QueryDurationFloorUs"/> reused by value: that floor is a per-query Query Store total in microseconds
    /// on another engine.</summary>
    public const double PgStatementMeanMsFloor = 10.0;                    // mean ms per statement call

    /// <summary>Absolute-fallback bar for the SERVER-WIDE per-call mean on an untrustworthy baseline (a young store, or
    /// a server whose 30 days hold too few busy collections to trust): a quarter of a second per call, averaged over
    /// EVERY statement the server ran in a collection, is slow on any PostgreSQL — a mix that averages 250 ms is a mix
    /// whose ordinary calls are waiting on something. Reached on the fallback arm only: the keyed arm checks trust
    /// BEFORE the gate and passes an unreachable bar, because handing a statement with no history of its own an
    /// absolute grade is the silent substitution the 2026-09-20 ruling forbade. unmeasured: chosen, not measured —
    /// the 2026-09-21 read gives the server-wide level's tail (p99 12 ms, §D5), which says 250 ms is far beyond
    /// anything the measured population reaches and that this bar effectively never fires on that fleet; calibrate
    /// against the upper tail of the per-collection mean before the next release rather than leaving a bar placed
    /// above the whole distribution.</summary>
    public const double PgStatementMeanMsFallback = 250.0;                // server-wide mean ms per statement call

    // lane 28 (#3691, v3): the CPU-burn z-detector's floor and fallback (PgTargetAnomalyDetector.Kernel.cs).

    /// <summary>Magnitude floor for the CPU-burn z-detector, in CORES BUSY per collection (Σ user + system + plan CPU
    /// ms across every statement pg_stat_kcache tracks, over the collection's own gap): below half a core a deviation
    /// is an idle server's ripple however many sigmas it reads — a routine of 0.05 cores busy at 3 am tripling to 0.15
    /// is not a workload event. unmeasured: chosen, not measured — and NOT calibratable on the current fleet: every
    /// PostgreSQL target the product monitors today is Aurora, and Aurora does not ship <c>pg_stat_kcache</c> (fifty
    /// clusters in the 2026-09-19 calibration, no population for this family), so the detector never reaches this bar
    /// there. Calibrate against the per-collection cores-busy distribution over <c>pg_kernel_stats</c> of a stock
    /// population before the next release that monitors one; the detector stamps <c>threshold_lineage = 0</c> until
    /// then. No core count is collected, so the unit is cores, never a percent.</summary>
    public const double PgCpuBurnCoresFloor = 0.5;                        // cores busy in one collection

    /// <summary>Absolute-fallback bar for CPU burn on an untrustworthy baseline: four cores busy, eight times the
    /// floor — the smallest figure that is a whole small host's worth of CPU on any PostgreSQL, so a young store fires
    /// only on a window that would saturate a four-core instance; a larger host's first storm is judged once its own
    /// routine exists. unmeasured: chosen, not measured — the same un-calibratable population as the floor; calibrate
    /// against a stock population's upper tail before the next release that monitors one.</summary>
    public const double PgCpuBurnCoresFallback = 4.0;                     // cores busy in one collection

    // lane 38 (#3691): the database-growth z-detector's floor and fallback (PgTargetAnomalyDetector.Growth.cs).

    /// <summary>Magnitude floor for the <c>pg_database_growth_bytes_per_day</c> z-detector — the instance total's
    /// growth between consecutive hourly <c>pg_database_size_stats</c> samples, rated per DAY. Under a quarter of a
    /// gibibyte a day a deviation is a quiet instance's ripple however many sigmas it reads: an instance whose routine
    /// is 10 MB/day tripling to 30 MB/day is a batch job, not a growth event, and the hourly cadence puts two or three
    /// samples into a one-to-four-hour analysis window, so the floor carries most of the grade until the bucket has
    /// history. unmeasured: chosen, not measured — the table (V136) is one day old at this bar's birth and the
    /// 2026-09-19 calibration ran before it existed; calibrate against the per-collection <c>total_bytes</c> difference
    /// of <c>pg_database_size_stats</c> once it holds 14 d before the next release. The detector stamps
    /// <c>threshold_lineage = 0</c>. NOT a SQL Server constant reused by value: the SQL Server engine has no growth
    /// baseline at all.</summary>
    public const double PgDatabaseGrowthFloorBytesPerDay = 256.0 * 1024.0 * 1024.0;     // 256 MiB of growth per day

    /// <summary>Absolute-fallback bar for database growth on an untrustworthy baseline — a young store, or an instance
    /// whose 30 days of hourly totals never moved (a zero-activity bucket by <c>EffectiveStdDev</c>'s contract, which is
    /// the routine shape of a read-mostly instance and the reason this path matters here): eight times the floor, two
    /// gibibytes a day, is a rate at which the trend fact's own 1 GiB / 10 % line is crossed inside a day on a small
    /// instance and inside a week on a 100 GB one — growth an operator would want named whatever the baseline says.
    /// unmeasured: chosen, not measured — the same day-old table as the floor; calibrate against the upper tail of the
    /// per-collection growth rate before the next release.</summary>
    public const double PgDatabaseGrowthFallbackBytesPerDay = 2.0 * 1024.0 * 1024.0 * 1024.0;  // 2 GiB of growth per day

    // #3653 A8 slice 1: the N-aware (Šidák) peak cutoff — see AnomalyGate class remarks for the pair gate this feeds.

    /// <summary>The reference window length the per-window peak cutoffs are anchored to: the 4-hour scheduled pass,
    /// which stays byte-identical (see <see cref="NAwarePeakCutoff"/>). Every longer <c>as_of</c> window raises its
    /// peak cutoff relative to this one; no window shorter than it lowers its cutoff (the rule clamps at k — see
    /// the method remarks).</summary>
    public static readonly TimeSpan NAwareReferenceWindow = TimeSpan.FromHours(4);

    /// <summary>
    /// #3653 A8 slice 1: the Šidák-corrected peak cutoff for a window of length <paramref name="window"/>, given the
    /// cutoff <paramref name="k"/> the family already uses at the 4-hour reference window (classical 2.0, robust
    /// 3.5, heavy-tail 5.0). The peak of N independent per-sample draws each with per-sample exceedance
    /// rate 1 − Φ(k) has a WINDOW exceedance rate of 1 − Φ(k)^N under the null; holding that window-level rate fixed
    /// across window lengths (rather than holding k fixed) is exactly the Šidák correction, and because the
    /// per-sample tail rate cancels out of the ratio, N_ref⁄N reduces to the ratio of window LENGTHS — no sample
    /// count is needed at any call site:
    /// </summary>
    /// <remarks>
    /// k_W = Φ⁻¹(1 − (1 − Φ(k)) · r), r = min(1, W_ref ⁄ W).
    ///
    /// <para>At <paramref name="window"/> ≤ <see cref="NAwareReferenceWindow"/> (r = 1), this returns <paramref name="k"/>
    /// ITSELF — the same <c>double</c>, with no Φ⁻¹(Φ(k)) round trip through the rational approximations below — so the
    /// 4-hour scheduled pass, and anything shorter, is byte-identical to the pre-#3653 verdict. Longer windows raise
    /// the cutoff (r &lt; 1 shrinks the tail probability the inverse-normal is asked for, which raises the quantile).
    /// </para>
    /// <para>Applies to the PEAK clause only (<c>AnomalyGate.Decide</c>) — the mean clause and the #1486 magnitude
    /// floor are unchanged; see the design note for #3653 A8 for why the mean is already sample-count-neutral and
    /// the peak was not.</para>
    /// </remarks>
    /// <param name="k">The family's existing cutoff at the 4-hour reference window — classical, robust, or
    /// heavy-tail; whichever tail mapping the caller's frame already uses.</param>
    /// <param name="window">The analysis window's length (<c>context.TimeRangeEnd - context.TimeRangeStart</c> at
    /// every call site).</param>
    internal static double NAwarePeakCutoff(double k, TimeSpan window)
    {
        if (window <= NAwareReferenceWindow)
            return k;

        var r = NAwareReferenceWindow.Ticks / (double)window.Ticks;
        var tailAtReference = NormalUpperTail(k);
        return InverseNormalCdf(1.0 - tailAtReference * r);
    }

    /// <summary>1 − Φ(x) — the standard normal's upper-tail probability, via <c>0.5 · erfc(x / √2)</c> so the
    /// small-tail case (large x, the only case this cutoff ever asks for) stays accurate instead of subtracting two
    /// numbers close to 1.</summary>
    private static double NormalUpperTail(double x) => 0.5 * Erfc(x / Math.Sqrt(2.0));

    /// <summary>The complementary error function, Cody's rational-Chebyshev approximation (W. J. Cody, "Rational
    /// Chebyshev Approximations for the Error Function", Math. Comp. 23 (1969), the double-precision constants from
    /// the reference CALERF/DERFC transcription at netlib.org/specfun/erf) — accurate to about 18 significant
    /// decimal digits over the whole real line, restricted here to the JINT=1 (erfc) branch, the only one this file
    /// needs.</summary>
    private static double Erfc(double x)
    {
        var y = Math.Abs(x);

        if (y <= 0.46875)
        {
            // Fortran A(1..5)/B(1..4): XNUM = A(5)*YSQ; DO I=1,3 XNUM=(XNUM+A(I))*YSQ; RESULT = X*(XNUM+A(4))/(XDEN+B(4));
            // ERF(X) for |X| <= 0.46875, then ERFC = 1 - ERF.
            var ysq = y > 1.11e-16 ? y * y : 0.0;
            var xnum = ErfA[4] * ysq;
            var xden = ysq;
            for (var i = 0; i < 3; i++)
            {
                xnum = (xnum + ErfA[i]) * ysq;
                xden = (xden + ErfB[i]) * ysq;
            }
            var result = x * (xnum + ErfA[3]) / (xden + ErfB[3]);
            return 1.0 - result;
        }

        if (y <= 4.0)
        {
            // Fortran C(1..9)/D(1..8): XNUM = C(9)*Y; DO I=1,7 ...; RESULT = (XNUM+C(8))/(XDEN+D(8)).
            var xnum = ErfcC[8] * y;
            var xden = y;
            for (var i = 0; i < 7; i++)
            {
                xnum = (xnum + ErfcC[i]) * y;
                xden = (xden + ErfcD[i]) * y;
            }
            var result = (xnum + ErfcC[7]) / (xden + ErfcD[7]);
            var ysqTrunc = Math.Truncate(y * 16.0) / 16.0;
            var del = (y - ysqTrunc) * (y + ysqTrunc);
            result = Math.Exp(-ysqTrunc * ysqTrunc) * Math.Exp(-del) * result;
            return x < 0 ? 2.0 - result : result;
        }

        {
            // XBIG: erfc underflows to 0 (double precision) for y at or beyond this.
            if (y >= 26.543)
                return x < 0 ? 2.0 : 0.0;

            // Fortran P(1..6)/Q(1..5): XNUM = P(6)*YSQ; DO I=1,4 ...; RESULT = YSQ*(XNUM+P(5))/(XDEN+Q(5)); RESULT=(SQRPI-RESULT)/Y.
            var ysqInv = 1.0 / (y * y);
            var xnum = ErfcP[5] * ysqInv;
            var xden = ysqInv;
            for (var i = 0; i < 4; i++)
            {
                xnum = (xnum + ErfcP[i]) * ysqInv;
                xden = (xden + ErfcQ[i]) * ysqInv;
            }
            var partial = ysqInv * (xnum + ErfcP[4]) / (xden + ErfcQ[4]);
            var result = (SqrtPiInv - partial) / y;
            var ysqTrunc = Math.Truncate(y * 16.0) / 16.0;
            var del = (y - ysqTrunc) * (y + ysqTrunc);
            result = Math.Exp(-ysqTrunc * ysqTrunc) * Math.Exp(-del) * result;
            return x < 0 ? 2.0 - result : result;
        }
    }

    private const double SqrtPiInv = 5.6418958354775628695e-1;

    // Fortran DATA A/1..5/, B/1..4/ (erf, |x| <= 0.46875).
    private static readonly double[] ErfA = { 3.16112374387056560e0, 1.13864154151050156e2, 3.77485237685302021e2, 3.20937758913846947e3, 1.85777706184603153e-1 };
    private static readonly double[] ErfB = { 2.36012909523441209e1, 2.44024637934444173e2, 1.28261652607737228e3, 2.84423683343917062e3 };

    // Fortran DATA C/1..9/, D/1..8/ (erfc, 0.46875 < |x| <= 4.0).
    private static readonly double[] ErfcC = { 5.64188496988670089e-1, 8.88314979438837594e0, 6.61191906371416295e1, 2.98635138197400131e2, 8.81952221241769090e2, 1.71204761263407058e3, 2.05107837782607147e3, 1.23033935479799725e3, 2.15311535474403846e-8 };
    private static readonly double[] ErfcD = { 1.57449261107098347e1, 1.17693950891312499e2, 5.37181101862009858e2, 1.62138957456669019e3, 3.29079923573345963e3, 4.36261909014324716e3, 3.43936767414372164e3, 1.23033935480374942e3 };

    // Fortran DATA P/1..6/, Q/1..5/ (erfc, |x| > 4.0).
    private static readonly double[] ErfcP = { 3.05326634961232344e-1, 3.60344899949804439e-1, 1.25781726111229246e-1, 1.60837851487422766e-2, 6.58749161529837803e-4, 1.63153871373020978e-2 };
    private static readonly double[] ErfcQ = { 2.56852019228982242e0, 1.87295284992346047e0, 5.27905102951428412e-1, 6.05183413124413191e-2, 2.33520497626869185e-3 };

    /// <summary>
    /// #3653 A8 slice 1: the standard-normal inverse CDF (probit), Wichura's Algorithm AS 241, PPND16 (M. J.
    /// Wichura, "Algorithm AS 241: The Percentage Points of the Normal Distribution", Appl. Statist. 37(3), 1988,
    /// 477–484) — accurate to about 1 part in 10^16 over the whole open interval (0, 1). Internal, not private: the
    /// unit tests pin its own accuracy directly (Φ⁻¹(0.975), Φ⁻¹(1e-10)) before trusting <see cref="NAwarePeakCutoff"/>
    /// on top of it.
    /// </summary>
    internal static double InverseNormalCdf(double p)
    {
        var q = p - 0.5;
        if (Math.Abs(q) <= 0.425)
        {
            var r = 0.180625 - q * q;
            var num = (((((((PpndA[7] * r + PpndA[6]) * r + PpndA[5]) * r + PpndA[4]) * r + PpndA[3]) * r + PpndA[2]) * r + PpndA[1]) * r + PpndA[0]);
            var den = (((((((PpndB[7] * r + PpndB[6]) * r + PpndB[5]) * r + PpndB[4]) * r + PpndB[3]) * r + PpndB[2]) * r + PpndB[1]) * r + 1.0);
            return q * num / den;
        }

        var rr = q < 0 ? p : 1.0 - p;
        if (rr <= 0.0)
            return q < 0 ? double.NegativeInfinity : double.PositiveInfinity;

        rr = Math.Sqrt(-Math.Log(rr));
        double ret;
        if (rr <= 5.0)
        {
            rr -= 1.6;
            var num = (((((((PpndC[7] * rr + PpndC[6]) * rr + PpndC[5]) * rr + PpndC[4]) * rr + PpndC[3]) * rr + PpndC[2]) * rr + PpndC[1]) * rr + PpndC[0]);
            var den = (((((((PpndD[7] * rr + PpndD[6]) * rr + PpndD[5]) * rr + PpndD[4]) * rr + PpndD[3]) * rr + PpndD[2]) * rr + PpndD[1]) * rr + 1.0);
            ret = num / den;
        }
        else
        {
            rr -= 5.0;
            var num = (((((((PpndE[7] * rr + PpndE[6]) * rr + PpndE[5]) * rr + PpndE[4]) * rr + PpndE[3]) * rr + PpndE[2]) * rr + PpndE[1]) * rr + PpndE[0]);
            var den = (((((((PpndF[7] * rr + PpndF[6]) * rr + PpndF[5]) * rr + PpndF[4]) * rr + PpndF[3]) * rr + PpndF[2]) * rr + PpndF[1]) * rr + 1.0);
            ret = num / den;
        }

        return q < 0 ? -ret : ret;
    }

    private static readonly double[] PpndA = { 3.3871328727963666080e0, 1.3314166789178437745e2, 1.9715909503065514427e3, 1.3731693765509461125e4, 4.5921953931549871457e4, 6.7265770927008700853e4, 3.3430575583588128105e4, 2.5090809287301226727e3 };
    private static readonly double[] PpndB = { 0.0, 4.2313330701600911252e1, 6.8718700749205790830e2, 5.3941960214247511077e3, 2.1213794301586595867e4, 3.9307895800092710610e4, 2.8729085735721942674e4, 5.2264952788528545610e3 };
    private static readonly double[] PpndC = { 1.42343711074968357734e0, 4.63033784615654529590e0, 5.76949722146069140550e0, 3.64784832476320460504e0, 1.27045825245236838258e0, 2.41780725177450611770e-1, 2.27238449892691845833e-2, 7.74545014278341407640e-4 };
    private static readonly double[] PpndD = { 0.0, 2.05319162663775882187e0, 1.67638483018380384940e0, 6.89767334985100004550e-1, 1.48103976427480074590e-1, 1.51986665636164571966e-2, 5.47593808499534494600e-4, 1.05075007164441684324e-9 };
    private static readonly double[] PpndE = { 6.65790464350110377720e0, 5.46378491116411436990e0, 1.78482653991729133580e0, 2.96560571828504891230e-1, 2.65321895265761230930e-2, 1.24266094738807843860e-3, 2.71155556874348757815e-5, 2.01033439929228813265e-7 };
    private static readonly double[] PpndF = { 0.0, 5.99832206555887937690e-1, 1.36929880922735805310e-1, 1.48753612908506148525e-2, 7.86869131145613259100e-4, 1.84631831751005468180e-5, 1.42151175831644588870e-7, 2.04426310338993978564e-15 };
}
