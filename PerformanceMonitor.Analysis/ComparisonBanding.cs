/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The arithmetic behind <c>compare_analysis</c>'s verdicts (#3538 A3), shared by Lite and Darling so the
/// two SKUs cannot band the same pair of windows differently. Pure over two scored fact lists and the
/// baseline buckets the caller looked up; the tools only serialize what comes back.
///
/// <para><b>The lie this replaces.</b> The tool compared ONE window against ONE window and banded every key
/// by its severity delta on a flat ±0.1 dead-band: <c>worse</c> above +0.1, <c>better</c> below −0.1,
/// <c>stable</c> between. Severity is a threshold-formula artifact, not a measurement — a saturating
/// ladder (<c>critical == null</c> in <c>FactScorer.ApplyThresholdFormula</c>) reads a doubling from 30% to
/// 60% of observed time as +0.0 ("stable") because both sides sit at 1.0, while a trace of a wait whose
/// concerning bar is 1% of the period reads a few seconds an hour as +0.125 ("worse"). None of it used the
/// per-server dispersion the baselines already store, one physical cause (an I/O stall) surfaced as four
/// to six "worse" keys, and <c>BAD_ACTOR_&lt;hash&gt;</c> keys churned into <c>new_issues</c> /
/// <c>resolved_issues</c> whenever the plan cache evicted a plan. The OtterTune field study measured DB
/// time varying 4× on an UNCHANGED configuration; a same-hour-yesterday comparison at N=1 vs N=1 has to
/// be read against that kind of noise, and this class is where the reading is decided.</para>
///
/// <para><b>Three bands, one rule per band, every rule stated in the payload.</b></para>
/// <list type="bullet">
///   <item><b>Baseline-banded</b> (<see cref="BandSourceBaseline"/>): a key whose reading is measured in the
///   same unit as one of the stored per-(server, metric, hour × day-of-week) baselines
///   (<see cref="BaselinedMetricFor"/>; the reading is the value itself for every key but two PostgreSQL ones —
///   the saturation fraction, whose peak count is read instead, and the WAL-volume mean, whose peak rate is —
///   <see cref="BaselinedValueFor"/>) expresses its
///   delta in that bucket's robust sigma
///   (<see cref="BaselineBucket.EffectiveRobustSigma"/>, MAD-based with the model's own floors) and is
///   <c>stable</c> inside ±<see cref="StableWithinRobustSigmas"/>. Used only when the bucket is
///   <see cref="BaselineBucket.IsTrustworthy"/> — the never-blind rule the anomaly gate follows: an
///   untrustworthy baseline routes to the absolute rule rather than to silence or to a giant sigma.</item>
///   <item><b>Absolute-banded</b> (<see cref="BandSourceAbsolute"/>): every other key present on both sides
///   changes status only when BOTH a relative move of at least <see cref="MinimumRelativeMove"/> of the
///   larger side AND a position of at least <see cref="MinimumLadderPosition"/> on the key's own Layer-1
///   severity ladder (the larger side's <see cref="Fact.BaseSeverity"/>) occurred. The ladder position is
///   the scorer's — read off the base severity it already computed — so every concerning bar in
///   <c>FactScorer</c> is reused without a single number copied here, including the ones that are not
///   in a table (blocking per hour, I/O latency, CPU %, the step ladders).</item>
///   <item><b>Present on one side only</b> (<see cref="BandSourcePresence"/>): a new or resolved issue only
///   when the present side clears <see cref="MinimumLadderPosition"/>; a noise-level appearance is
///   <c>stable</c>. <c>BAD_ACTOR_*</c> keys are the exception and never take this path — see
///   <see cref="IsPlanCacheIdentityKey"/>.</item>
/// </list>
///
/// <para><b>Families.</b> Every row carries a physical-cause family (<see cref="FamilyFor"/>) and the result
/// carries one family row per cause with its worst member, so the reader counts causes, not symptoms.
/// The family map mirrors the collector's wait grouping (<see cref="FactCollectorHelpers.WaitFamilyKey"/>,
/// applied first) and the symptom families <c>AnomalyIncidentReconciler</c> folds anomalies into, extended
/// along the <c>RelationshipGraph</c> edges that tie the regular keys of one cause together
/// (PAGEIOLATCH_* ↔ IO_READ_LATENCY_MS, WRITELOG ↔ IO_WRITE_LATENCY_MS ↔ HADR_SYNC_COMMIT, the memory-grant
/// chain, the lock/blocking chain). The whole graph is NOT used as the family relation: union-find over
/// its THREADPOOL bridge merges everything into one component, which is why the reconciler rejected it
/// too.</para>
///
/// <para><b>What it does not do.</b> It does not make the comparison a statistical test — one window against
/// one window cannot show that a change caused anything, and the tool's description says so. It does not
/// band wait fractions by sigma: the wait baselines are ALL-TYPES totals (ms per collection, ms per
/// second) and no per-type dispersion is stored, so per-type waits take the absolute rule. It does not
/// change what either window's facts ARE — the collectors' observed-time divisor (#3538 A2) is upstream of
/// it, and it composes with that lane's coverage caveat by flagging every verdict row when either side
/// was partly observed rather than by restating the caveat.</para>
/// </summary>
public static class ComparisonBanding
{
    /// <summary>
    /// The baseline band: a value delta inside ±1 robust sigma of the comparison hour's bucket is
    /// <c>stable</c>. One sigma is the UNIT the bucket's dispersion is stored in, not a tuned cutoff — the
    /// question this tool answers is "did it move more than this server routinely moves at this hour",
    /// and the server's routine movement IS one sigma of its own same-hour history. The anomaly
    /// detectors' 3.5σ / 5.0σ cutoffs (<see cref="AnomalyThresholds.ModifiedZThresholdFor(string)"/>)
    /// answer a different question — "is this value abnormal against the median" — and are reported
    /// beside the band as <c>beyond_anomaly_cutoff</c> rather than used as it. The per-sample sigma is an
    /// UPPER bound on the dispersion of a window average (averaging removes within-hour jitter and keeps
    /// day-to-day level differences), so the band is conservative for the averaged keys (CPU %, read
    /// latency) and exact for the single-sample one (session count).
    /// </summary>
    public const double StableWithinRobustSigmas = 1.0;

    /// <summary>
    /// The absolute band's relative arm: the value must have moved by at least a quarter of the larger
    /// side. A judgment about materiality set by the #3538 engine review rather than a fleet measurement:
    /// it refuses the 1%-of-value wobbles that the severity dead-band admitted whenever a ladder's slope
    /// was steep, and it admits a doubling whatever the ladder did with it. The read that would revise it
    /// is the fleet distribution of same-hour-yesterday relative deltas per fact key; until that exists
    /// "a quarter" is stated, not hidden.
    /// </summary>
    public const double MinimumRelativeMove = 0.25;

    /// <summary>
    /// The absolute band's ladder arm: the larger side's Layer-1 base severity must be at least a quarter
    /// — a quarter of the way to the concerning bar on a saturating ladder (<c>value / concerning</c>),
    /// half of the way on a ramped one (<c>0.5 · value / concerning</c> below concerning), and the second
    /// tier or above on the step ladders. A move between two values the scorer itself grades as
    /// negligible is not a verdict. The same "quarter" as <see cref="MinimumRelativeMove"/> on purpose:
    /// one number to explain. Together the two arms bound the absolute move from below at a sixteenth of
    /// a saturating ladder's concerning bar and an eighth of a ramped one's, which is what keeps a
    /// trace-to-trace doubling of a wait whose bar is 1% of the period from reading as change.
    /// </summary>
    public const double MinimumLadderPosition = 0.25;

    public const string StatusWorse = "worse";
    public const string StatusBetter = "better";
    public const string StatusStable = "stable";

    public const string BandSourceBaseline = "baseline";
    public const string BandSourceAbsolute = "absolute";
    public const string BandSourcePresence = "presence";

    public const string PresenceBoth = "both";
    public const string PresenceComparisonOnly = "comparison_only";
    public const string PresenceBaselineOnly = "baseline_only";

    /// <summary>The <c>BAD_ACTOR_&lt;query_hash&gt;</c> family — per-query identity, reported as churn.
    ///
    /// <para><b>Deliberately NOT <c>PG_BAD_ACTOR_</c> (#3542, decided between waves).</b> The PostgreSQL twin is keyed
    /// on <c>pg_stat_statements.queryid</c>, which is stable for the life of a PostgreSQL major (it changes only
    /// across a major upgrade or a <c>compute_query_id</c> change — the advice says so), whereas SQL Server's
    /// <c>query_hash</c> is a plan-cache identity that a recompile can re-key inside one window. So for a
    /// PostgreSQL bad actor the key-set arithmetic is HONEST: a statement present in one window and absent from
    /// the other did start or stop being a top consumer (or fell out of the top-5 cut — the one churn source that
    /// remains, and the presence text is read with that in mind). The churn band would hide a real new offender
    /// behind "the cache held a different plan", which is not a thing PostgreSQL's identity does. A
    /// <c>PG_BAD_ACTOR_*</c> key therefore falls through <see cref="IsPlanCacheIdentityKey"/> (no shared prefix)
    /// and takes the presence path.</para></summary>
    public const string PlanCacheIdentityPrefix = "BAD_ACTOR_";

    /// <summary>
    /// The one sentence per band that the payload carries so a reader never has to infer the rule from
    /// the numbers. Written once, here, so both SKUs say it in the same words.
    /// </summary>
    public static object BandRulesPayload => new
    {
        baseline = $"delta_sigma is the reading's delta in robust-sigma units (MAD-based) of this server's own hour-of-day x day-of-week baseline for the comparison window's hour; stable within ±{StableWithinRobustSigmas:0.#}σ, worse/better beyond. Used only when that baseline is trustworthy (baseline_confidence > 0); beyond_anomaly_cutoff says whether the move also clears the anomaly detector's own cutoff for the metric.",
        absolute = $"no per-key dispersion is stored, so a status changes only when BOTH the value moved at least {MinimumRelativeMove:P0} of the larger side (relative_move) AND the larger side sits at least {MinimumLadderPosition:0.##} up the key's own base-severity ladder (ladder_position) — a move between two values the scorer grades as negligible is not a verdict.",
        presence = $"a key present in one window only is a new or resolved issue only when its base severity reaches {MinimumLadderPosition:0.##}; a noise-level appearance or disappearance is stable. BAD_ACTOR_* keys never take this path: their appearance is plan-cache identity churn, reported under plan_cache_churn and excluded from new_issues / resolved_issues."
    };

    /// <summary>
    /// The stored baseline metric a fact key's <see cref="Fact.Value"/> is measured in, or null when the
    /// key has no baseline in the same unit. Only same-unit pairs are mapped: <c>CPU_SQL_PERCENT</c> is the
    /// window's average of the same <c>sqlserver_cpu_utilization</c> samples the CPU baseline is built from
    /// and <c>CPU_SPIKE</c> is their maximum (a single sample, so per-sample sigma is exactly its scale);
    /// <c>IO_READ_LATENCY_MS</c> is stall ÷ reads in ms, the I/O baseline's own ratio; <c>SESSION_STATS</c>
    /// is the latest total-connections reading, the session baseline's own sample. Wait fractions are
    /// deliberately unmapped (the wait baselines are all-types totals, not per type), as are blocking and
    /// deadlock rates (their baselines are events per day with no robust statistics) and
    /// <c>IO_WRITE_LATENCY_MS</c> (no write-latency baseline exists).
    ///
    /// <para><b>The PostgreSQL-target keys (#3691 W1).</b> v1 shipped with only the SQL Server keys mapped, so
    /// <c>compare_analysis</c> on a PostgreSQL target banded a 10 → 60 tps move "stable" (<c>band_source:
    /// absolute</c>) in the same window whose anomaly detector called it 25σ — <c>PG_TPS</c>'s base severity is
    /// 0 by design (throughput is context, not a grade), so the absolute rule's ladder arm can never move it,
    /// and the only honest reading of a throughput change IS this server's own same-hour dispersion. The
    /// four buckets <c>PgTargetBaselineProvider</c> stores are mapped where the fact's reading is in the
    /// bucket's unit: <c>PG_TPS</c> is the window's transactions ÷ observed seconds and <c>pg_tps</c> is
    /// per-collection transactions ÷ the collection's own gap (the averaged-key case: per-sample sigma is an
    /// upper bound on the average's dispersion, so the band is conservative); <c>PG_DEADLOCK_RATE</c> is
    /// deadlocks ÷ observed hours and <c>pg_deadlock_rate</c> is deadlocks × 3600 ÷ gap — the same unit, unlike
    /// SQL Server's events-per-day deadlock baseline, and on a healthy server the bucket's median and MAD sit
    /// at 0 so <see cref="BaselineBucket.EffectiveRobustSigma"/> is 0 and the key takes the absolute rule as
    /// <see cref="Compare"/> already requires; <c>PG_CPU_PERCENT</c> is the peak <c>acu_utilization_percent</c>
    /// when the window carried a capacity sample and <c>pg_cpu</c> is that column's buckets — a fact holding
    /// the RAW <c>cpu_percent</c> instead (<c>capacity_measured = 0</c>) is in a different unit and
    /// <see cref="BaselinedValueFor"/> withholds it. <c>PG_CONNECTION_SATURATION</c> is the one whose
    /// <see cref="Fact.Value"/> is NOT the bucket's unit: the value is peak ÷ usable connections (a 0–1
    /// fraction) and <c>pg_session_count</c> is <c>MAX(total_sessions)</c> per capture (a count), so the row is
    /// banded on the fact's <c>peak_total_sessions</c> metadata — the very reading the bucket is built from and
    /// the one <c>PgTargetAnomalyDetector</c> judges against it — never on the fraction in count-sigma.</para>
    ///
    /// <para><b>The v2 baselined keys (#3691 exit check, the same gap repeated).</b> The v2 lanes stored three more
    /// <c>pg_</c> buckets and shipped detectors that judge against them, and the exit check found the compare tool
    /// banding all three <c>absolute</c> / <c>baseline_metric: null</c> in the same window their detectors called
    /// 25σ. Mapped, each in the bucket's own unit: <c>PG_IO_READ_LATENCY_MS</c> is the window's read time ÷ reads in
    /// ms (<c>pg_io_stats</c>, reset-aware) and <c>pg_io_read_latency</c> is the same quotient per quarter-hour sample —
    /// the averaged-key case again, so the band is conservative; a fact that makes no latency claim
    /// (<c>latency_measured = 0</c>: no <c>pg_stat_io</c> on the major, or <c>track_io_timing</c> off) or whose quotient
    /// the scorer states but does not grade (<c>insufficient_ops = 1</c>) has no reading and
    /// <see cref="BaselinedValueFor"/> withholds it. <c>PG_REPLICATION_LAG</c> is the window's peak
    /// <c>replay_bytes_behind</c> and <c>pg_replay_lag_bytes</c> is <c>MAX(replay_bytes_behind)</c> per collection — the
    /// same bytes, the value itself. <c>PG_WAL_VOLUME_SHIFT</c> is the second key whose <see cref="Fact.Value"/> is NOT
    /// the reading banded: the value is the window's MEAN bytes/s across rated collections, but the
    /// <c>pg_wal_bytes_per_sec</c> bucket is per-collection bytes/s and <c>PgTargetAnomalyDetector.Wal.cs</c> judges
    /// the window's PEAK against it, so the row is banded on the fact's <c>peak_wal_bytes_per_sec</c> metadata —
    /// the reading whose verdict <c>beyond_anomaly_cutoff</c> claims to agree with — while <c>value_delta</c> stays
    /// the mean; the <c>unavailable</c> shape (Aurora, pre-14, an all-zero series — see
    /// <c>PgTargetFactCollector.Write.cs</c>) carries no peak and is withheld, so two unavailable sides read
    /// <c>stable</c> by the absolute rule with no sigma. The remaining PostgreSQL keys stay unmapped for the
    /// reasons the SQL Server ones do: the wait profile is per-type shares against an all-types baseline, and no
    /// bucket exists for the buffer, checkpoint, vacuum, temp or config readings.</para>
    /// </summary>
    public static string? BaselinedMetricFor(string key) => key switch
    {
        "CPU_SQL_PERCENT" => MetricNames.Cpu,
        "CPU_SPIKE" => MetricNames.Cpu,
        "IO_READ_LATENCY_MS" => MetricNames.IoLatency,
        "SESSION_STATS" => MetricNames.SessionCount,
        PgTargetFactKeys.Tps => MetricNames.PgTps,
        PgTargetFactKeys.ConnectionSaturation => MetricNames.PgSessionCount,
        PgTargetFactKeys.DeadlockRate => MetricNames.PgDeadlockRate,
        PgTargetFactKeys.CpuPercent => MetricNames.PgCpu,
        PgTargetFactKeys.IoReadLatencyMs => MetricNames.PgIoReadLatency,
        PgTargetFactKeys.ReplicationLag => MetricNames.PgReplayLagBytes,
        PgTargetFactKeys.WalVolumeShift => MetricNames.PgWalBytesPerSec,
        _ => null
    };

    /// <summary>
    /// The metadata key the PostgreSQL sessions collector (<c>PgTargetFactCollector.Sessions.cs</c>) and its
    /// advice read the window's peak session COUNT from — the same literal, because the shared vocabulary file
    /// declares fact keys and sources, not metadata names. The live compare pin proves the two agree end to end.
    /// </summary>
    private const string PgSessionPeakCountKey = "peak_total_sessions";

    /// <summary>
    /// The metadata key the PostgreSQL write collector (<c>PgTargetFactCollector.Write.cs</c>), its anomaly detector
    /// and its advice all read the window's PEAK WAL rate from — the same literal, for the same reason as
    /// <see cref="PgSessionPeakCountKey"/>. Absent on the fact's <c>unavailable</c> shape, which is how that shape
    /// is withheld from the sigma path without a second flag being consulted here.
    /// </summary>
    private const string PgWalPeakBytesPerSecKey = "peak_wal_bytes_per_sec";

    /// <summary>
    /// The reading of <paramref name="fact"/> in its baseline metric's unit, or null when this fact cannot be
    /// sigma-banded even though its key is mapped — the unit-reconciliation seam <see cref="BaselinedMetricFor"/>
    /// describes. Every SQL Server key and three of the four PostgreSQL ones return <see cref="Fact.Value"/>
    /// unchanged, so the SQL Server arithmetic is byte-for-byte what it was. <c>PG_CONNECTION_SATURATION</c>
    /// returns its <c>peak_total_sessions</c> (null when the metadata is absent, as on a hand-built fact);
    /// <c>PG_CPU_PERCENT</c> returns its value only when <c>capacity_measured</c> is stamped 1, because a fact
    /// carrying the raw <c>cpu_percent</c> is percent of the capacity CURRENTLY allocated (#3281) and the
    /// <c>pg_cpu</c> bucket is percent of the configured ceiling. Of the v2 keys, <c>PG_IO_READ_LATENCY_MS</c> returns
    /// its value only when the collector stamped it a measured, graded quotient (<c>latency_measured = 1</c> and
    /// <c>insufficient_ops = 0</c> — the unavailable shapes carry <c>latency_measured = 0</c>), and
    /// <c>PG_WAL_VOLUME_SHIFT</c> returns its <c>peak_wal_bytes_per_sec</c> (null on the <c>unavailable</c> shape,
    /// which does not carry it). A null routes the key to the absolute rule with <c>baseline_metric</c> still naming
    /// the bucket that WOULD apply — the never-blind fallback, not silence.
    /// </summary>
    public static double? BaselinedValueFor(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.ConnectionSaturation =>
            fact.Metadata.TryGetValue(PgSessionPeakCountKey, out var peak) ? peak : null,
        PgTargetFactKeys.CpuPercent =>
            fact.Metadata.GetValueOrDefault(PgTargetScorer.CpuCapacityMeasuredKey) >= 1 ? fact.Value : null,
        PgTargetFactKeys.IoReadLatencyMs =>
            fact.Metadata.GetValueOrDefault(PgTargetScorer.IoLatencyMeasuredKey) >= 1
                && fact.Metadata.GetValueOrDefault(PgTargetScorer.IoInsufficientOpsKey) < 1
                ? fact.Value : null,
        PgTargetFactKeys.WalVolumeShift =>
            fact.Metadata.TryGetValue(PgWalPeakBytesPerSecKey, out var walPeak) ? walPeak : null,
        _ => fact.Value
    };

    /// <summary>
    /// The distinct baseline metrics the caller must look up for this pair of fact lists — only the
    /// metrics some present key is measured in, so a comparison with no CPU fact costs no CPU baseline
    /// read.
    /// </summary>
    public static IReadOnlyList<string> DispersionMetricsFor(IEnumerable<Fact> baselineFacts, IEnumerable<Fact> comparisonFacts) =>
        baselineFacts.Concat(comparisonFacts)
            .Select(f => BaselinedMetricFor(f.Key))
            .Where(m => m is not null)
            .Select(m => m!)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(m => m, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// <c>BAD_ACTOR_&lt;query_hash&gt;</c>: the key IS a plan-cache identity. Its appearance in one window and
    /// absence in the other says the cache held a different plan for the top-5 cut, not that a problem
    /// began or ended — the same statement under a recompiled hash is a "new issue" and a "resolved
    /// issue" at once under key-set arithmetic. Reported as <c>plan_cache_churn</c> instead.
    /// </summary>
    public static bool IsPlanCacheIdentityKey(string key) =>
        key.StartsWith(PlanCacheIdentityPrefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The one key whose value runs the other way: <c>DISK_SPACE</c> is the FREE fraction, so a lower value
    /// is worse (<c>FactScorer.ScoreDiskFact</c> inverts it). Direction is taken from the base-severity
    /// delta first, which already carries the inversion; this set only matters when both sides sit on the
    /// same saturated rung and the value has to decide.
    /// </summary>
    private static readonly HashSet<string> HigherIsBetterKeys = new(StringComparer.Ordinal) { "DISK_SPACE" };

    /// <summary>
    /// The physical-cause family a fact key belongs to. Names are the <c>RelationshipGraph</c> edge
    /// categories where one exists, so the vocabulary is the engine's own. The collector's wait grouping
    /// is applied first (every CX* is already CXPACKET, every general lock mode already LCK by the time a
    /// fact reaches this tool; calling <see cref="FactCollectorHelpers.WaitFamilyKey"/> keeps that true for a
    /// hand-built fact too). A key with no family of its own is its own family, so every row has one and a
    /// singleton family is simply a cause with one symptom.
    /// </summary>
    public static string FamilyFor(string key)
    {
        if (IsPlanCacheIdentityKey(key))
            return "bad_actor";

        var grouped = FactCollectorHelpers.WaitFamilyKey(key);
        return grouped switch
        {
            // AnomalyIncidentReconciler: ANOMALY_CPU_SPIKE → CPU_SQL_PERCENT | CPU_SPIKE. Graph cpu_pressure:
            // CPU_SQL_PERCENT ↔ SOS_SCHEDULER_YIELD; RUNNABLE_TASKS is the scheduler-queue reading of the same.
            "CPU_SQL_PERCENT" or "CPU_SPIKE" or "SOS_SCHEDULER_YIELD" or "RUNNABLE_TASKS" => "cpu_pressure",
            // Graph parallelism / query_performance: QUERY_HIGH_DOP → CXPACKET.
            "CXPACKET" or "QUERY_HIGH_DOP" => "parallelism",
            // Reconciler: ANOMALY_READ_LATENCY → IO_READ_LATENCY_MS. Graph io_pressure / memory_pressure:
            // IO_READ_LATENCY_MS ↔ PAGEIOLATCH_SH, PAGEIOLATCH_EX → IO_READ_LATENCY_MS. One stall, one row.
            "IO_READ_LATENCY_MS" or "PAGEIOLATCH_SH" or "PAGEIOLATCH_EX" => "io_pressure",
            // Reconciler: ANOMALY_WRITE_LATENCY → IO_WRITE_LATENCY_MS. Graph log_io: WRITELOG ↔
            // IO_WRITE_LATENCY_MS, HADR_SYNC_COMMIT ↔ WRITELOG.
            "IO_WRITE_LATENCY_MS" or "WRITELOG" or "HADR_SYNC_COMMIT" => "log_io",
            // Reconciler: ANOMALY_MEMORY_PRESSURE → RESOURCE_SEMAPHORE. Graph memory_grants:
            // RESOURCE_SEMAPHORE ↔ MEMORY_GRANT_PENDING → QUERY_SPILLS; RS_QUERY_COMPILE is the compile gateway.
            "RESOURCE_SEMAPHORE" or "RESOURCE_SEMAPHORE_QUERY_COMPILE" or "MEMORY_GRANT_PENDING" or "QUERY_SPILLS" => "memory_grants",
            // Reconciler: ANOMALY_BLOCKING_SPIKE → BLOCKING_EVENTS. Graph lock_contention / blocking:
            // LCK ↔ BLOCKING_EVENTS ↔ BLOCKING_CHAIN; the ungrouped lock modes (S/IS, range, schema) are the
            // same physical queue with a different reason. DEADLOCKS stays its own family, as the
            // reconciler keeps ANOMALY_DEADLOCK_SPIKE apart from the blocking family.
            "LCK" or "LCK_M_S" or "LCK_M_IS" or "SCH_M" or "BLOCKING_EVENTS" or "BLOCKING_CHAIN" => "lock_contention",
            _ when grouped.StartsWith("LCK_M_RS_", StringComparison.Ordinal)
                || grouped.StartsWith("LCK_M_RIn_", StringComparison.Ordinal)
                || grouped.StartsWith("LCK_M_RX_", StringComparison.Ordinal) => "lock_contention",
            "DEADLOCKS" => "deadlocking",
            // Graph latch_contention: LATCH_EX → TEMPDB_USAGE / CXPACKET; PAGELATCH_UP is the tempdb
            // allocation latch. TEMPDB_USAGE itself is a space reading and stays its own family.
            "LATCH_EX" or "LATCH_SH" or "PAGELATCH_UP" => "latch_contention",
            _ => grouped
        };
    }

    /// <summary>
    /// Bands every key of the two scored fact lists. <paramref name="dispersionByMetric"/> is keyed by
    /// baseline metric name (<see cref="DispersionMetricsFor"/>); a metric absent from it, or present with an
    /// untrustworthy bucket, sends its keys to the absolute rule. <paramref name="coverageCaveat"/> is true
    /// when either window was partly observed or unobserved (#3538 A2) and flags every verdict row.
    /// </summary>
    public static ComparisonResult Compare(
        IReadOnlyList<Fact> baselineFacts,
        IReadOnlyList<Fact> comparisonFacts,
        IReadOnlyDictionary<string, BaselineBucket> dispersionByMetric,
        bool coverageCaveat)
    {
        var baselineByKey = baselineFacts.ToFactLookup();
        var comparisonByKey = comparisonFacts.ToFactLookup();
        var allKeys = baselineByKey.Keys.Union(comparisonByKey.Keys, StringComparer.Ordinal)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        var rows = new List<ComparisonRow>(allKeys.Count);
        var appeared = new List<ChurnEntry>();
        var disappeared = new List<ChurnEntry>();

        foreach (var key in allKeys)
        {
            var baseline = baselineByKey.GetValueOrDefault(key);
            var comparison = comparisonByKey.GetValueOrDefault(key);

            if (baseline is null || comparison is null)
            {
                var present = (baseline ?? comparison)!;
                if (IsPlanCacheIdentityKey(key))
                {
                    var entry = new ChurnEntry(key, present.DatabaseName, Math.Round(present.Value, 6), Math.Round(present.Severity, 4));
                    (comparison is not null ? appeared : disappeared).Add(entry);
                    continue;
                }

                rows.Add(BandOneSided(key, baseline, comparison, coverageCaveat));
                continue;
            }

            var metric = BaselinedMetricFor(key);
            var bucket = metric is not null ? dispersionByMetric.GetValueOrDefault(metric) : null;
            /* Both sides must offer a reading in the bucket's unit (BaselinedValueFor) — a PostgreSQL
               saturation fact without its peak count, a CPU fact holding the raw percent, an unavailable
               I/O or WAL fact, is not one. */
            rows.Add(bucket is { IsTrustworthy: true } && bucket.EffectiveRobustSigma > 0
                     && BaselinedValueFor(baseline) is { } baselineReading
                     && BaselinedValueFor(comparison) is { } comparisonReading
                ? BandBySigma(key, baseline, comparison, baselineReading, comparisonReading, metric!, bucket, coverageCaveat)
                : BandByLadder(key, baseline, comparison, coverageCaveat));
        }

        /* Worse rows first, then better, then stable; the larger relative move first within each
           group, then the key — a scale-free order in which a regression always outranks an
           improvement. Direction before magnitude matters for the family rollup below: a family whose
           BLOCKING_EVENTS fell 60% while its LCK_M_S rose 30% has a real regression in it, and ordering by
           magnitude alone would have made the improvement its worst member and dropped the family from
           families_worse. The old payload ordered by |severity_delta|, which put a saturated ladder's
           doubling last and a trace's formula slope first. */
        rows = rows
            .OrderBy(r => StatusRank(r.Status))
            .ThenByDescending(r => r.RelativeMove ?? 0)
            .ThenBy(r => r.Key, StringComparer.Ordinal)
            .ToList();

        var families = rows
            .GroupBy(r => r.Family, StringComparer.Ordinal)
            .Select(g =>
            {
                var members = g.ToList(); // in verdict order (worse > better > stable, then move), so First() is the worst member
                var worst = members[0];
                return new ComparisonFamily(
                    g.Key,
                    worst.Status,
                    worst.Key,
                    members.Select(m => m.Key).ToList(),
                    members.Count(m => m.Status == StatusWorse),
                    members.Count(m => m.Status == StatusBetter),
                    members.Count(m => m.Status == StatusStable),
                    coverageCaveat);
            })
            .OrderBy(f => StatusRank(f.Status))
            .ThenByDescending(f => rows.First(r => r.Key == f.WorstKey).RelativeMove ?? 0)
            .ThenBy(f => f.Family, StringComparer.Ordinal)
            .ToList();

        var presentInBoth = allKeys.Count(k => IsPlanCacheIdentityKey(k) && baselineByKey.ContainsKey(k) && comparisonByKey.ContainsKey(k));

        return new ComparisonResult(
            rows,
            families,
            new PlanCacheChurn(appeared, disappeared, presentInBoth, IsPostgresTargetFactSet(baselineFacts, comparisonFacts)),
            coverageCaveat);
    }

    /// <summary>
    /// Whether the compared facts came from a PostgreSQL target: any fact carrying a <see cref="PgTargetSources.Prefix"/>
    /// source. The engine is resolved per call upstream (<c>servers.engine_kind</c>, #2530) and the collector
    /// that ran is the engine's own, so a <c>pg_</c> source IS that resolution carried on the fact — the same
    /// prefix the shared scorer routes on — and this class never has to be told the engine through a
    /// parameter both SKUs' tool bodies would have to plumb. The coverage witness keeps its engine-neutral
    /// source and is filtered out before the comparison, so a PostgreSQL window with any fact at all has a
    /// <c>pg_</c> one; a SQL Server window never does, and its churn note is unchanged.
    /// </summary>
    private static bool IsPostgresTargetFactSet(IEnumerable<Fact> baselineFacts, IEnumerable<Fact> comparisonFacts) =>
        baselineFacts.Concat(comparisonFacts).Any(f => f.Source.StartsWith(PgTargetSources.Prefix, StringComparison.Ordinal));

    private static ComparisonRow BandBySigma(string key, Fact baseline, Fact comparison, double baselineReading, double comparisonReading, string metric, BaselineBucket bucket, bool coverageCaveat)
    {
        /* value_delta stays in the fact's own unit (for PG_CONNECTION_SATURATION, the fraction; for
           PG_WAL_VOLUME_SHIFT, the mean rate); the band is decided on the readings in the bucket's unit, which
           are the values themselves for every other key. */
        var valueDelta = comparison.Value - baseline.Value;
        var readingDelta = comparisonReading - baselineReading;
        var sigma = bucket.EffectiveRobustSigma;
        var rawDeltaSigma = readingDelta / sigma;
        /* Display-capped like the detectors' deviation_sigma (#1486): the band is decided on the raw
           value, the payload never renders a collapsed-variance thousand-sigma. */
        var deltaSigma = Math.Clamp(rawDeltaSigma, -AnomalyThresholds.SigmaDisplayCap, AnomalyThresholds.SigmaDisplayCap);
        var moved = Math.Abs(rawDeltaSigma) > StableWithinRobustSigmas;
        /* Every baselined key is higher-is-worse (CPU %, read latency, connections, deadlocks per hour). PG_TPS
           is read the way its anomaly detector reads it — the high side is the flagged one: more transactions
           than this server carries at this hour is more load, and the row says so as `worse` so a 6× surge
           counts as a regression in the family rollup rather than vanishing into `stable`. A throughput FALL
           therefore reads `better`, which is the same one-sided limit the detector has (it does not fire on a
           collapse either); a reader is given the values and the sigma to see it, and the two-sided reading is
           a v3 item on #3691, not something to fake here by inverting a key whose fall can be either an outage
           or a quiet hour. */
        var status = !moved ? StatusStable : readingDelta > 0 ? StatusWorse : StatusBetter;

        return new ComparisonRow
        {
            Key = key,
            Source = baseline.Source,
            Family = FamilyFor(key),
            Presence = PresenceBoth,
            BaselineValue = Math.Round(baseline.Value, 6),
            ComparisonValue = Math.Round(comparison.Value, 6),
            BaselineSeverity = Math.Round(baseline.Severity, 4),
            ComparisonSeverity = Math.Round(comparison.Severity, 4),
            SeverityDelta = Math.Round(comparison.Severity - baseline.Severity, 4),
            ValueDelta = Math.Round(valueDelta, 6),
            RelativeMove = Math.Round(RelativeMove(baseline.Value, comparison.Value), 4),
            LadderPosition = Math.Round(Math.Max(baseline.BaseSeverity, comparison.BaseSeverity), 4),
            Status = status,
            BandSource = BandSourceBaseline,
            DeltaSigma = Math.Round(deltaSigma, 2),
            BaselineSigma = Math.Round(sigma, 4),
            BaselineMedian = Math.Round(bucket.Median, 4),
            BaselineConfidence = Math.Round(bucket.Confidence, 2),
            BaselineTier = bucket.Tier.ToString(),
            BaselineMetric = metric,
            BeyondAnomalyCutoff = Math.Abs(rawDeltaSigma) >= AnomalyThresholds.ModifiedZThresholdFor(metric),
            CoverageCaveat = coverageCaveat
        };
    }

    private static ComparisonRow BandByLadder(string key, Fact baseline, Fact comparison, bool coverageCaveat)
    {
        var valueDelta = comparison.Value - baseline.Value;
        var relativeMove = RelativeMove(baseline.Value, comparison.Value);
        var ladderPosition = Math.Max(baseline.BaseSeverity, comparison.BaseSeverity);
        var moved = relativeMove >= MinimumRelativeMove && ladderPosition >= MinimumLadderPosition;

        return new ComparisonRow
        {
            Key = key,
            Source = baseline.Source,
            Family = FamilyFor(key),
            Presence = PresenceBoth,
            BaselineValue = Math.Round(baseline.Value, 6),
            ComparisonValue = Math.Round(comparison.Value, 6),
            BaselineSeverity = Math.Round(baseline.Severity, 4),
            ComparisonSeverity = Math.Round(comparison.Severity, 4),
            SeverityDelta = Math.Round(comparison.Severity - baseline.Severity, 4),
            ValueDelta = Math.Round(valueDelta, 6),
            RelativeMove = Math.Round(relativeMove, 4),
            LadderPosition = Math.Round(ladderPosition, 4),
            Status = !moved ? StatusStable : Direction(key, baseline, comparison),
            BandSource = BandSourceAbsolute,
            BaselineMetric = BaselinedMetricFor(key),
            CoverageCaveat = coverageCaveat
        };
    }

    private static ComparisonRow BandOneSided(string key, Fact? baseline, Fact? comparison, bool coverageCaveat)
    {
        var present = (baseline ?? comparison)!;
        var isNew = comparison is not null;
        var registers = present.BaseSeverity >= MinimumLadderPosition;

        return new ComparisonRow
        {
            Key = key,
            Source = present.Source,
            Family = FamilyFor(key),
            Presence = isNew ? PresenceComparisonOnly : PresenceBaselineOnly,
            BaselineValue = baseline is null ? null : Math.Round(baseline.Value, 6),
            ComparisonValue = comparison is null ? null : Math.Round(comparison.Value, 6),
            BaselineSeverity = baseline is null ? null : Math.Round(baseline.Severity, 4),
            ComparisonSeverity = comparison is null ? null : Math.Round(comparison.Severity, 4),
            SeverityDelta = Math.Round((comparison?.Severity ?? 0) - (baseline?.Severity ?? 0), 4),
            ValueDelta = null,
            /* A side with nothing to compare against is a whole move for ordering purposes. */
            RelativeMove = 1.0,
            LadderPosition = Math.Round(present.BaseSeverity, 4),
            Status = !registers ? StatusStable : isNew ? StatusWorse : StatusBetter,
            BandSource = BandSourcePresence,
            BaselineMetric = BaselinedMetricFor(key),
            CoverageCaveat = coverageCaveat
        };
    }

    /// <summary>The verdict order: a regression outranks an improvement outranks no change.</summary>
    private static int StatusRank(string status) => status switch
    {
        StatusWorse => 0,
        StatusBetter => 1,
        _ => 2
    };

    /// <summary>|b − a| over the larger magnitude; 0 when both are 0.</summary>
    private static double RelativeMove(double a, double b)
    {
        var larger = Math.Max(Math.Abs(a), Math.Abs(b));
        return larger > 0 ? Math.Abs(b - a) / larger : 0.0;
    }

    /// <summary>
    /// Which way a two-sided absolute-banded move went. The base-severity delta decides when it is
    /// non-zero — the scorer already knows each ladder's direction, including the inverted free-space
    /// one and the step ladders whose rung is set by metadata rather than <see cref="Fact.Value"/>. When
    /// both sides sit on the same rung (a saturated ladder, the doubling-from-30%-to-60% case) the value
    /// decides, inverted for the free-space key.
    /// </summary>
    private static string Direction(string key, Fact baseline, Fact comparison)
    {
        var baseDelta = comparison.BaseSeverity - baseline.BaseSeverity;
        if (baseDelta != 0)
            return baseDelta > 0 ? StatusWorse : StatusBetter;

        var valueDelta = comparison.Value - baseline.Value;
        var higherIsWorse = !HigherIsBetterKeys.Contains(key);
        return (valueDelta > 0) == higherIsWorse ? StatusWorse : StatusBetter;
    }
}

/// <summary>One compared key. Serialized by <see cref="ToPayload"/> so both SKUs emit the same shape.</summary>
public sealed class ComparisonRow
{
    public required string Key { get; init; }
    public required string Source { get; init; }
    public required string Family { get; init; }
    public required string Presence { get; init; }
    public double? BaselineValue { get; init; }
    public double? ComparisonValue { get; init; }
    public double? BaselineSeverity { get; init; }
    public double? ComparisonSeverity { get; init; }
    /// <summary>Kept from the pre-#3538 payload for continuity; it no longer decides the band.</summary>
    public double SeverityDelta { get; init; }
    public double? ValueDelta { get; init; }
    public double? RelativeMove { get; init; }
    public double LadderPosition { get; init; }
    public required string Status { get; init; }
    public required string BandSource { get; init; }
    public double? DeltaSigma { get; init; }
    public double? BaselineSigma { get; init; }
    public double? BaselineMedian { get; init; }
    public double? BaselineConfidence { get; init; }
    public string? BaselineTier { get; init; }
    public string? BaselineMetric { get; init; }
    public bool? BeyondAnomalyCutoff { get; init; }
    public bool CoverageCaveat { get; init; }

    public object ToPayload() => new
    {
        key = Key,
        source = Source,
        family = Family,
        presence = Presence,
        baseline_value = BaselineValue,
        comparison_value = ComparisonValue,
        baseline_severity = BaselineSeverity,
        comparison_severity = ComparisonSeverity,
        severity_delta = SeverityDelta,
        value_delta = ValueDelta,
        relative_move = RelativeMove,
        ladder_position = LadderPosition,
        status = Status,
        band_source = BandSource,
        delta_sigma = DeltaSigma,
        baseline_sigma = BaselineSigma,
        baseline_median = BaselineMedian,
        baseline_confidence = BaselineConfidence,
        baseline_tier = BaselineTier,
        baseline_metric = BaselineMetric,
        beyond_anomaly_cutoff = BeyondAnomalyCutoff,
        coverage_caveat = CoverageCaveat
    };
}

/// <summary>One physical cause: its worst member and every member key, so one I/O stall is one row.</summary>
public sealed record ComparisonFamily(
    string Family,
    string Status,
    string WorstKey,
    IReadOnlyList<string> Members,
    int Worse,
    int Better,
    int Stable,
    bool CoverageCaveat)
{
    public object ToPayload() => new
    {
        family = Family,
        status = Status,
        worst_key = WorstKey,
        members = Members,
        worse = Worse,
        better = Better,
        stable = Stable,
        coverage_caveat = CoverageCaveat
    };
}

/// <summary>A <c>BAD_ACTOR_*</c> key seen on one side only, with the side's reading.</summary>
public sealed record ChurnEntry(string Key, string? DatabaseName, double Value, double Severity)
{
    public object ToPayload() => new { key = Key, database = DatabaseName, value = Value, severity = Severity };
}

/// <summary>Plan-cache identity churn: what the top-5 cut held on one side and not the other.
///
/// <para><paramref name="PostgresTarget"/> (#3691 W2) picks the note. The block itself is emitted for both engines
/// — the payload shape is one shape — but on a PostgreSQL target the three counters are 0 by construction
/// (<c>PG_BAD_ACTOR_*</c> has no shared prefix with <see cref="ComparisonBanding.PlanCacheIdentityPrefix"/> and
/// takes the presence path; <c>PgTargetBetweenWavesTests</c> pins that), and the SQL Server sentence beside them
/// told a PostgreSQL operator that keys their payload does not contain are plan-cache identities. The PostgreSQL
/// sentence says what the keys they DO see are and where a new or resolved statement is counted.</para></summary>
public sealed record PlanCacheChurn(IReadOnlyList<ChurnEntry> Appeared, IReadOnlyList<ChurnEntry> Disappeared, int PresentInBoth, bool PostgresTarget = false)
{
    public const string SqlServerNote = "BAD_ACTOR_<hash> keys are plan-cache identities: a hash present in one window only means the cache held a different plan for the top-5 cut, not that a problem began or ended. Not counted in new_issues / resolved_issues.";

    public const string PostgresNote = "PG_BAD_ACTOR_<queryid> keys are statement identities from pg_stat_statements (queryid is stable for the life of a PostgreSQL major), not plan-cache identities: no churn arithmetic runs for a PostgreSQL target and these lists are empty by construction. A statement present in one window only is compared like every other key and, when it clears the presence bar, IS counted in new_issues / resolved_issues.";

    public object ToPayload() => new
    {
        appeared = Appeared.Select(e => e.ToPayload()).ToList(),
        disappeared = Disappeared.Select(e => e.ToPayload()).ToList(),
        present_in_both = PresentInBoth,
        note = PostgresTarget ? PostgresNote : SqlServerNote
    };
}

/// <summary>The whole verdict set for one pair of windows.</summary>
public sealed record ComparisonResult(
    IReadOnlyList<ComparisonRow> Rows,
    IReadOnlyList<ComparisonFamily> Families,
    PlanCacheChurn Churn,
    bool CoverageCaveat)
{
    public int Worse => Rows.Count(r => r.Status == ComparisonBanding.StatusWorse);
    public int Better => Rows.Count(r => r.Status == ComparisonBanding.StatusBetter);
    public int Stable => Rows.Count(r => r.Status == ComparisonBanding.StatusStable);
    public int NewIssues => Rows.Count(r => r.Presence == ComparisonBanding.PresenceComparisonOnly && r.Status == ComparisonBanding.StatusWorse);
    public int ResolvedIssues => Rows.Count(r => r.Presence == ComparisonBanding.PresenceBaselineOnly && r.Status == ComparisonBanding.StatusBetter);
    public int FamiliesWorse => Families.Count(f => f.Status == ComparisonBanding.StatusWorse);
    public int FamiliesBetter => Families.Count(f => f.Status == ComparisonBanding.StatusBetter);
    public int FamiliesStable => Families.Count(f => f.Status == ComparisonBanding.StatusStable);

    /// <summary>True when the union of both windows' keys (churn included) was empty — nothing to compare.</summary>
    public bool IsEmpty => Rows.Count == 0 && Churn.Appeared.Count == 0 && Churn.Disappeared.Count == 0;

    public object SummaryPayload() => new
    {
        worse = Worse,
        better = Better,
        stable = Stable,
        new_issues = NewIssues,
        resolved_issues = ResolvedIssues,
        families_worse = FamiliesWorse,
        families_better = FamiliesBetter,
        families_stable = FamiliesStable,
        plan_cache_churn_appeared = Churn.Appeared.Count,
        plan_cache_churn_disappeared = Churn.Disappeared.Count,
        /* True on every verdict row too; surfaced here so a reader of the summary alone sees it. */
        coverage_caveat = CoverageCaveat
    };
}
