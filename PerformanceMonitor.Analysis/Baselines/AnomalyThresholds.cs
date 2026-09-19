/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
    /// input; matches the FactScorer WaitProfileRatioFloor. Still uncalibrated as of the 2026-09 dogfood
    /// measurement (#3538 A5), which read each wait TYPE's fraction of a 4-hour window and not the
    /// all-types ms/sec peak-over-baseline ratio this cutoff gates; the read that would calibrate it is
    /// that ratio's own distribution over the fleet, one more column on the same pass.
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

       Every one of these is UNMEASURED — chosen, not measured — and the detector stamps threshold_lineage = 0 on
       every fact it fires so get_analysis_facts shows it. The calibrating read for each is named on the constant;
       all of them are one pass over the dogfood PostgreSQL fleet's 30-day raw series (the V120 method that
       produced the deadlock tiers). The one exception is the deadlock-rate fallback, which is an alias of a
       MEASURED bar and says so. */

    /// <summary>Magnitude floor for the TPS z-detector: below fifty transactions a second a deviation is a
    /// quiet server's noise however many sigmas it reads. unmeasured: chosen, not measured — calibrate against
    /// the per-server distribution of the pg_database_stats transaction rate before the next release.</summary>
    public const double PgTpsFloor = 50.0;                    // transactions/sec

    /// <summary>Absolute-fallback bar for TPS on an untrustworthy baseline: ten times the floor, so a young store
    /// fires only on a rate that is large on any PostgreSQL. unmeasured: chosen, not measured — calibrate against
    /// pg_database_stats (the same distribution's upper tail) before the next release.</summary>
    public const double PgTpsFallback = 500.0;                // transactions/sec

    /// <summary>Magnitude floor for the session-count z-detector, a COUNT because a fraction of max_connections is
    /// meaningless as a constant (the ceiling is per server, and the detector cannot see the config fact — the
    /// ceiling-aware arm is PG_CONNECTION_SATURATION, into which this anomaly folds). Twenty is a fifth of the
    /// shipped default ceiling. unmeasured: chosen, not measured — calibrate against the per-capture
    /// total_sessions distribution in pg_session_states before the next release.</summary>
    public const double PgSessionCountFloor = 20.0;           // sessions

    /// <summary>Absolute-fallback bar for session count on an untrustworthy baseline: twice the shipped default
    /// max_connections, so it fires only on a pool that has been raised well past the default — on a default
    /// ceiling the saturation fact is the never-blind arm and this one stays quiet by design (it would be the
    /// same finding twice). unmeasured: chosen, not measured — calibrate against pg_session_states before the
    /// next release.</summary>
    public const double PgSessionCountFallback = 200.0;       // sessions

    /// <summary>Magnitude floor for the Aurora CPU z-detector, on <c>acu_utilization_percent</c> — percent of
    /// the CONFIGURED capacity ceiling (#3281), never the percent-of-allocated raw reading. Under forty percent
    /// of the ceiling the instance has more headroom than it is using, whatever its own history says.
    /// unmeasured: chosen, not measured — calibrate against pg_cpu_utilization before the next release.</summary>
    public const double PgCpuFloorPct = 40.0;                 // % of configured capacity

    /// <summary>Absolute-fallback bar for Aurora CPU on an untrustworthy baseline: the fleet card's own Critical
    /// line on the same quantity (ServerHealthThresholds.CpuCriticalPercent — repeated because this assembly
    /// references nothing; PgTargetAnomalyTests pins the pair equal), so a young store's CPU anomaly and its card
    /// cannot disagree about the colour of one minute. unmeasured: that ladder is stated against a quantity, not
    /// read off a distribution — calibrate against pg_cpu_utilization before the next release.</summary>
    public const double PgCpuFallbackPct = 95.0;              // % of configured capacity

    /// <summary>The Aurora wait-profile detector's magnitude floor AND absolute-fallback bar (one number on both
    /// paths, the SQL Server wait profile's shape): all-types wait milliseconds per second, CPU excluded — 500 is
    /// half of one backend continuously waiting. unmeasured: chosen, not measured — calibrate against the
    /// per-collection all-types rate in pg_wait_stats before the next release. Stock PostgreSQL's SAMPLED
    /// estimate has no arm in v1 (its resolution floor and per-backend-sample unit give it a different noise
    /// distribution), so there is deliberately no sampled twin of this bar yet.</summary>
    public const double PgWaitProfileFallbackMsPerSec = 500.0;   // ms of waiting per second of observed time

    /// <summary>The deadlock-rate ratio detector's magnitude floor, a RATE per observed hour (#3538 A7: never a
    /// count over the window, which would scale with hours_back) — under one deadlock an hour the ratio is the
    /// arithmetic of a near-empty numerator. unmeasured: chosen, not measured — calibrate against the
    /// pg_database_stats deadlock delta distribution before the next release.</summary>
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
    /// bucket's dispersion. unmeasured: chosen, not measured — the SQL Server event-ratio detector happens to
    /// use the same multiple, and this is NOT an alias of it: the two instruments must stay free to calibrate
    /// apart, so calibrate this one against the per-server ratio distribution over pg_database_stats and
    /// pg_wait_stats before the next release.</summary>
    public const double PgRatioAnomalyThreshold = 3.0;
}
