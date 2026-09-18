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
///   <item><b>Baseline-banded</b> (<see cref="BandSourceBaseline"/>): a key whose value is measured in the
///   same unit as one of the stored per-(server, metric, hour × day-of-week) baselines
///   (<see cref="BaselinedMetricFor"/>) expresses its value delta in that bucket's robust sigma
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

    /// <summary>The <c>BAD_ACTOR_&lt;query_hash&gt;</c> family — per-query identity, reported as churn.</summary>
    public const string PlanCacheIdentityPrefix = "BAD_ACTOR_";

    /// <summary>
    /// The one sentence per band that the payload carries so a reader never has to infer the rule from
    /// the numbers. Written once, here, so both SKUs say it in the same words.
    /// </summary>
    public static object BandRulesPayload => new
    {
        baseline = $"delta_sigma is the value delta in robust-sigma units (MAD-based) of this server's own hour-of-day x day-of-week baseline for the comparison window's hour; stable within ±{StableWithinRobustSigmas:0.#}σ, worse/better beyond. Used only when that baseline is trustworthy (baseline_confidence > 0); beyond_anomaly_cutoff says whether the move also clears the anomaly detector's own cutoff for the metric.",
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
    /// </summary>
    public static string? BaselinedMetricFor(string key) => key switch
    {
        "CPU_SQL_PERCENT" => MetricNames.Cpu,
        "CPU_SPIKE" => MetricNames.Cpu,
        "IO_READ_LATENCY_MS" => MetricNames.IoLatency,
        "SESSION_STATS" => MetricNames.SessionCount,
        _ => null
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
            rows.Add(bucket is { IsTrustworthy: true } && bucket.EffectiveRobustSigma > 0
                ? BandBySigma(key, baseline, comparison, metric!, bucket, coverageCaveat)
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
            new PlanCacheChurn(appeared, disappeared, presentInBoth),
            coverageCaveat);
    }

    private static ComparisonRow BandBySigma(string key, Fact baseline, Fact comparison, string metric, BaselineBucket bucket, bool coverageCaveat)
    {
        var valueDelta = comparison.Value - baseline.Value;
        var sigma = bucket.EffectiveRobustSigma;
        var rawDeltaSigma = valueDelta / sigma;
        /* Display-capped like the detectors' deviation_sigma (#1486): the band is decided on the raw
           value, the payload never renders a collapsed-variance thousand-sigma. */
        var deltaSigma = Math.Clamp(rawDeltaSigma, -AnomalyThresholds.SigmaDisplayCap, AnomalyThresholds.SigmaDisplayCap);
        var moved = Math.Abs(rawDeltaSigma) > StableWithinRobustSigmas;
        /* Every baselined key is higher-is-worse (CPU %, read latency, connections). */
        var status = !moved ? StatusStable : valueDelta > 0 ? StatusWorse : StatusBetter;

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

/// <summary>Plan-cache identity churn: what the top-5 cut held on one side and not the other.</summary>
public sealed record PlanCacheChurn(IReadOnlyList<ChurnEntry> Appeared, IReadOnlyList<ChurnEntry> Disappeared, int PresentInBoth)
{
    public object ToPayload() => new
    {
        appeared = Appeared.Select(e => e.ToPayload()).ToList(),
        disappeared = Disappeared.Select(e => e.ToPayload()).ToList(),
        present_in_both = PresentInBoth,
        note = "BAD_ACTOR_<hash> keys are plan-cache identities: a hash present in one window only means the cache held a different plan for the top-5 cut, not that a problem began or ended. Not counted in new_issues / resolved_issues."
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
