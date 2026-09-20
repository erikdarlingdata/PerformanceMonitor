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
/// <c>pg_io</c> — data-file I/O latency (filled by lane 11 of #3691, design §3.9): <c>PG_IO_READ_LATENCY_MS</c> /
/// <c>PG_IO_WRITE_LATENCY_MS</c> composed from the reset-aware <c>pg_io_stats</c> counter differences
/// (<c>pg_stat_io</c>, PostgreSQL 16+; absent below). <see cref="Fact.Value"/> is milliseconds PER OPERATION over
/// the window — Δ<c>read_time_ms</c> ÷ Δ<c>reads</c>, summed over every (backend_type, context) identity of the
/// <c>relation</c> object type first — never a total: a total scales with <c>hours_back</c> and a per-op figure
/// does not (#3538 A7).
///
/// <para><b>Trackedness before any number (the #3541 A8 lesson).</b> <c>pg_stat_io</c>'s timing columns are
/// populated only under <c>track_io_timing = on</c>; with it off the engine counts reads and reports
/// <c>read_time = 0</c> for every one of them, so a naive quotient is 0.000 ms — a fabricated measurement of
/// infinitely fast storage. The collector therefore emits three shapes and this scorer grades exactly one of
/// them: <see cref="IoLatencyMeasuredKey"/> = 1 (timing on, the value is a measurement); <see cref="IoUnavailableKey"/>
/// = 1 with a reason flag (<see cref="IoReasonTrackIoTimingOffKey"/> — reads happened, no timing;
/// <see cref="IoReasonPgStatIoAbsentKey"/> — the major has no <c>pg_stat_io</c>), whose <see cref="Fact.Value"/> is
/// 0 as the placeholder of a fact that makes NO claim and which the advice never renders as a latency; and
/// <see cref="IoInsufficientOpsKey"/> = 1 (timing on, but fewer than <see cref="IoMinimumOps"/> operations in the
/// window — the quotient is stated and not graded, because a p99 over four reads is noise, measured). Write
/// latency (<c>PG_IO_WRITE_LATENCY_MS</c>) carries the same flags and is NEVER graded in v2: on the measured
/// population (Aurora) the write side of <c>pg_stat_io</c> is NULL — backends do not write data files there — and
/// no bar exists for the stock write path; putting one here would be an unmeasured number pretending otherwise.</para>
///
/// <para><b>Lineage — measured, with the population named.</b> Both read bars come from the #3691 calibration
/// pass (measurements-3691.md §B1): hourly ms per read over 14 days × 50 Aurora PostgreSQL clusters of the dogfood
/// fleet, 2026-09-19 — per-server p50 1.0–10 ms (median 1.29), per-server p99 median 7.3 ms, fleet p90 of per-server
/// p99 26 ms, fleet max 33.7 ms. WARNING 10 ms ≈ the fleet p90 of per-server p99 (a server whose WINDOW average sits
/// where the fleet's worst hours sit); CRITICAL 30 ms ≈ the fleet max. The quantity is engine-neutral but the
/// POPULATION is Aurora storage: stock PostgreSQL on local NVMe routinely reads under 0.5 ms and on network block
/// storage can idle at 5–10 ms, so these bars are Aurora-storage shapes, and the advice says so. The facts carry
/// <c>threshold_lineage = 1</c> because the bars ARE measured — the honesty is in naming the population, not in
/// flagging the number as a guess.</para>
///
/// <para><b>Self-gating below the warning bar, the session and CPU families' shape.</b>
/// <see cref="FactScorer.ApplyThresholdFormula"/> grades any positive value below the concerning bar as a fraction
/// of it, so a 3 ms server would read 0.15 — and the graph's predicates, the anomaly's fold and the wait chain's
/// edge all ask <c>BaseSeverity &gt; 0</c>. Zero below 10 ms, 0.5 at it, 1.0 at 30 ms: a fired latency fact
/// always means the window's storage was slow by the measured population's own standard.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── the pg_io metadata vocabulary; the collector stamps, the scorer, amplifiers, detector and advice read ── */

    /// <summary>1 when <see cref="Fact.Value"/> is a measured ms-per-operation (timing on, <c>pg_stat_io</c> present);
    /// 0 when the fact makes no latency claim.</summary>
    public const string IoLatencyMeasuredKey = "latency_measured";
    /// <summary>1 when the family cannot know the latency on this server; exactly one reason flag rides beside it.</summary>
    public const string IoUnavailableKey = "unavailable";
    /// <summary>Reads (or writes) were counted but their time was 0 for all of them — <c>track_io_timing</c> is off.
    /// Cross-reference: the <c>CONFIG_PG_TRACK_IO_TIMING</c> knob fact (lane 2), which is the advisory card.</summary>
    public const string IoReasonTrackIoTimingOffKey = "reason_track_io_timing_off";
    /// <summary>The registry major is below <see cref="IoStatIoMinimumMajor"/> — there is no <c>pg_stat_io</c> to read.</summary>
    public const string IoReasonPgStatIoAbsentKey = "reason_pg_stat_io_absent";
    /// <summary>1 when timing is on but fewer than <see cref="IoMinimumOps"/> operations fell in the window — the
    /// quotient is stated, not graded.</summary>
    public const string IoInsufficientOpsKey = "insufficient_ops";
    /// <summary>Δ operations (reads for the read fact, writes for the write fact) summed over identities.</summary>
    public const string IoOpsKey = "ops";
    /// <summary>Δ operation time in milliseconds, summed over identities.</summary>
    public const string IoOpTimeMsKey = "op_time_ms";
    /// <summary>Operations per second of OBSERVED time (<c>context.ObservedDurationMs</c>), never the nominal window.</summary>
    public const string IoOpsPerSecKey = "ops_per_sec";
    /// <summary>Whether the engine reported the counter at all (<c>reads IS NOT NULL</c> / <c>writes IS NOT NULL</c>
    /// on any row) — Aurora's write side is NULL, which is "not reported", never zero.</summary>
    public const string IoOpsTrackedKey = "ops_tracked";
    /// <summary>Explicit <c>stats_reset</c> moves seen inside the window; the sums are floors when &gt; 0.</summary>
    public const string IoResetCountKey = "stats_reset_count";
    /// <summary>Distinct collections in the window (the family's own witness; <c>pg_io_stats</c> is one-minute).</summary>
    public const string IoSampleCountKey = "sample_count";
    /// <summary>Distinct (backend_type, context) identities that contributed a difference (an identity whose counter is
    /// NULL throughout — the checkpointer's reads, Aurora's writes — is not one of them).</summary>
    public const string IoIdentityCountKey = "identity_count";
    public const string IoObservedMsKey = "observed_ms";
    /// <summary>The registry major the trackedness decision read (0 when the registry had none).</summary>
    public const string IoServerMajorKey = "server_major";
    /// <summary>1 on Aurora (off the registry fact) — the advice's storage-tier note hangs on it.</summary>
    public const string IoIsAuroraKey = "is_aurora";
    /// <summary>The <c>track_io_timing</c> knob's value (0/1) when the config fact was collected; absent otherwise.
    /// Corroboration for the data-side decision, never the decision itself.</summary>
    public const string IoTrackIoTimingConfigKey = "track_io_timing_config";

    /// <summary>
    /// The read-latency bars, milliseconds per read. measured: WARNING ≈ the fleet p90 of per-server p99 and
    /// CRITICAL ≈ the fleet max of hourly ms per read over 14 days × 50 Aurora PostgreSQL clusters of the dogfood
    /// fleet, 2026-09-19 (measurements-3691.md §B1). Aurora-STORAGE shapes: stock local-disk PostgreSQL is a
    /// different population and is not what these were read from; the advice names it.
    /// </summary>
    public const double IoReadLatencyWarningMs = 10.0;
    public const double IoReadLatencyCriticalMs = 30.0;

    /// <summary>
    /// The operations floor under which a ms-per-op is stated and not graded. measured: servers with 1–17 reads
    /// per hour produced the calibration's p99 noise (a quotient over four reads), and the per-hour ms/read
    /// distribution stabilised above roughly a thousand reads an hour over 14 days × 50 Aurora PostgreSQL clusters
    /// of the dogfood fleet, 2026-09-19 (§B1). Applied to the WINDOW total by the collector; the baseline and the
    /// detector work at a finer grain and apply <see cref="IoBaselineBucketMinimumReads"/> instead.
    /// </summary>
    public const double IoMinimumOps = 1000.0;

    /// <summary>
    /// The reads floor a FIFTEEN-MINUTE sample must clear to enter the <c>pg_io_read_latency</c> baseline or to be
    /// rated by the detector (#3691 between waves, lane 11's own proposal). Why the grain moved: at the hour grain
    /// one sample per hour-of-week bucket per week is four or five in a 30-day window, under
    /// <c>BaselineMath.RestoreThreshold</c> (15), so the hour-of-week tier was never restorable and every I/O
    /// anomaly was judged against the hour-of-day collapse at 0.85 confidence; four samples an hour puts ~17–20 in
    /// each bucket and the hour-of-week tier is the one the operator reads. The ms-per-read quotient survives
    /// re-bucketing unchanged (it is a ratio of two sums), and the floor is what keeps the low-reads tail the
    /// calibration read saw (1–17 reads an hour) from entering as noise at the finer grain — a quarter-hour's
    /// share of <see cref="IoMinimumOps"/>, so a sample admitted here would have been admitted at the hour grain
    /// had its hour run at the same rate. Lineage: the RESTORE threshold it serves is engine-defined (the shared
    /// <c>BaselineMath.RestoreThreshold</c>); the 250 itself is <b>measured</b> as an admission floor: reads per
    /// 15-minute bucket over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-20 (§C5,
    /// <c>pg_io_stats</c>) — 57 % of the fleet's buckets carry fewer than 250 reads (62 % fewer than 1,000); per
    /// server the share below the floor runs 0 – 100 % (median 70 %), and 25 of the 50 clusters have a MEDIAN of 0
    /// reads per quarter-hour (the working set is cached). The floor is right for what it does — a ms-per-read over a
    /// handful of reads is noise — and the read states its consequence rather than hiding it: at this grain a 30-day
    /// hour-of-week bucket holds at most 17 samples, so excluding 57 – 100 % of them leaves HALF the fleet under the
    /// 15-sample restore threshold, and their I/O baseline is the hour-of-DAY collapse (0.85 confidence) BY
    /// CONSTRUCTION — honest, and stated on the fact's <c>baseline_tier</c>; the hour-of-week tier is restorable
    /// only on the ~25 % of clusters that read continuously. Aurora population (Aurora storage; a stock local-disk
    /// server's read counts are not yet measured). It is a sample-ADMISSION floor, not a grading bar: the anomaly's
    /// fire/no-fire bars (<c>PgIoLatencyFloorMs</c>, <c>PgIoLatencyFallbackMs</c>) are measured (§B1) and the fact
    /// carries <c>threshold_lineage = 1</c> — which this read now also supports — and the floor is published beside
    /// them as <c>bucket_reads_floor</c> so <c>get_analysis_facts</c> shows which reads counted.
    /// The baseline and detector SQL carry the same literal beside a comment naming this constant;
    /// <c>PgTargetIoTests</c> pins the two equal.
    /// </summary>
    public const double IoBaselineBucketMinimumReads = 250.0;

    /// <summary>engine-defined: <c>pg_stat_io</c> arrived in PostgreSQL 16; below it the view does not exist and
    /// the collector never wrote a row. The fact then says "absent", not 0.</summary>
    public const int IoStatIoMinimumMajor = 16;

    /// <summary>
    /// Layer-1 base severity for the <c>pg_io</c> source. Only <c>PG_IO_READ_LATENCY_MS</c> is graded, and only when
    /// it is a measurement: an unavailable fact is context (0) and carries no lineage stamp because no bar decided
    /// anything; a measured fact is stamped <c>threshold_lineage = 1</c> the moment the floor or a bar is consulted
    /// — including the ones graded to 0 — so <c>get_analysis_facts</c> shows the bar is measured. The write fact
    /// is 0 whatever it carries (class summary: no v2 bar).
    /// </summary>
    private static partial double ScoreIoFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.IoReadLatencyMs) return 0.0;
        if (fact.Metadata.GetValueOrDefault(IoUnavailableKey) >= 1 || fact.Metadata.GetValueOrDefault(IoLatencyMeasuredKey) < 1)
            return 0.0;

        /* measured: IoMinimumOps / IoReadLatencyWarningMs / IoReadLatencyCriticalMs (see the constants). */
        fact.Metadata["threshold_lineage"] = 1;
        if (fact.Metadata.GetValueOrDefault(IoInsufficientOpsKey) >= 1 || fact.Value <= 0)
            return 0.0;

        /* measured: IoReadLatencyWarningMs — below the warning bar the fact is context, never a fraction of the
           bar; see the class summary on self-gating. */
        if (fact.Value < IoReadLatencyWarningMs)
            return 0.0;

        /* measured: IoReadLatencyWarningMs / IoReadLatencyCriticalMs (see the constants). */
        return FactScorer.ApplyThresholdFormula(fact.Value, IoReadLatencyWarningMs, IoReadLatencyCriticalMs);
    }

    /* unmeasured: chosen, not measured — calibrate against the co-fire rate of pg_io_stats latency with pg_wait_stats
       IO standouts and the pg_database_stats miss share before the next release. The three corroborations of slow
       reads: the anomaly co-fired (this hour is unusual for THIS storage, not its routine); an IO wait standout
       crossed its own bar (backends felt it); the buffer cache is measurably short (the cache is sending the
       reads). Any two lift a WARNING (0.5) to 0.8; all three with a CRITICAL base reach 1.9 — one condition, one
       story, the parent never capped. */
    public const double IoCorroborationBoost = 0.3;

    /// <summary>
    /// The I/O chain's Layer-2 amplifiers, on the read fact only. Every predicate reads a sibling's
    /// <see cref="Fact.BaseSeverity"/>, never its amplified <see cref="Fact.Severity"/> — the shared scorer assigns
    /// amplified severities one fact at a time, so a predicate on <c>Severity</c> would depend on emission order
    /// (lane 4's finding). The IO wait predicate accepts the <c>IO</c> type rollup OR any named <c>IO:</c> standout,
    /// because <c>PgTargetScorer.Waits.cs</c> grades one wait once — the rollup is 0 whenever a standout fired — so
    /// exactly one of them can be the fired one. The write fact has no amplifiers: it has no base to amplify.
    /// </summary>
    private static partial List<AmplifierDefinition> IoAmplifiers(string key)
    {
        if (key != PgTargetFactKeys.IoReadLatencyMs) return [];

        var ioRollup = PgTargetFactKeys.WaitKey("IO", null);
        var ioStandoutPrefix = ioRollup + "_";
        return
        [
            new AmplifierDefinition
            {
                Description = "ANOMALY_PG_IO_LATENCY co-fired — this window's read latency is far above this server's own hour-of-week routine, not its storage's normal",
                /* unmeasured: IoCorroborationBoost (see the constant). */
                Boost = IoCorroborationBoost,
                Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyIoLatency, out var anomaly) && anomaly.BaseSeverity > 0,
            },
            new AmplifierDefinition
            {
                Description = "An IO wait (the IO rollup or a named IO: standout) crossed its own threshold — backends spent measurable time waiting on these slow operations",
                /* unmeasured: IoCorroborationBoost (see the constant). */
                Boost = IoCorroborationBoost,
                Predicate = facts =>
                {
                    foreach (var (factKey, fact) in facts)
                    {
                        if (fact.BaseSeverity > 0
                            && (string.Equals(factKey, ioRollup, StringComparison.Ordinal) || factKey.StartsWith(ioStandoutPrefix, StringComparison.Ordinal)))
                            return true;
                    }
                    return false;
                },
            },
            new AmplifierDefinition
            {
                Description = "PG_BUFFER_CACHE_PRESSURE fired — the cache is measurably short for this working set, so the misses are what is reaching this slow storage",
                /* unmeasured: IoCorroborationBoost (see the constant). */
                Boost = IoCorroborationBoost,
                Predicate = facts => facts.TryGetValue(PgTargetFactKeys.BufferCachePressure, out var pressure) && pressure.BaseSeverity > 0,
            },
        ];
    }
}
