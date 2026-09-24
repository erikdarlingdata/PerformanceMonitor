/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Anomaly detection for a PostgreSQL-target pass (#3542) — the engine-sibling of <see cref="PgAnomalyDetector"/>
/// behind the same <see cref="IAnomalyDetector"/> seam. Same posture throughout: one baseline-data gate ahead of
/// every detector, each detector fenced in its own try so one metric's failure costs the pass that metric and
/// nothing else, and every anomaly fact keyed <c>ANOMALY_PG_*</c> (<see cref="PgTargetFactKeys"/>) so the shared
/// scorer routes it to the PostgreSQL ramps rather than the SQL Server literals (#3584). <b>The gate is whole-pass and
/// one-tabled, by design:</b> <see cref="HasBaselineDataSql"/> asks whether <c>pg_database_stats</c> has rows in the
/// 30 days before the window — the coverage witness of the #3542 D-list, the one universal one-minute series — and
/// a target whose database-stats collector fails gets no anomaly of ANY family, however healthy its other tables,
/// stated here so a reader of an anomaly-less pass looks at that collector first (lane 35's ask).
///
/// <para><b>The five detectors (lane 9, #3542 step 9; design §2b).</b> Three z-score families through the shared
/// <see cref="AnomalyGate"/> — transactions per second (<see cref="PgTargetFactKeys.AnomalyTps"/>), session count
/// (<see cref="PgTargetFactKeys.AnomalySessionSpike"/>) and, on Aurora only, percent of the configured capacity
/// ceiling (<see cref="PgTargetFactKeys.AnomalyCpuSpike"/>) — and two ratio-vs-own-baseline families: the
/// deadlock rate off the reset-aware counter difference (<see cref="PgTargetFactKeys.AnomalyDeadlockRate"/>) and
/// the Aurora all-types wait rate (<see cref="PgTargetFactKeys.AnomalyWaitProfile"/>). Each reads its window
/// statistic from the SAME raw table and the SAME difference its regular fact reads (the PG_TPS / PG_DEADLOCK_RATE
/// read, the session peak read, the Aurora wait read), and its baseline from <see cref="PgTargetBaselineProvider"/>'s
/// matching arm, so the number an anomaly is judged against is the number the fact it folds into states. The v2
/// and wave-3 detectors (I/O latency, replication lag, WAL volume, blocking) and lane 24's stock SAMPLED wait profile
/// (<see cref="PgTargetFactKeys.AnomalySampledWaitProfile"/> — <c>pg_wait_sampling</c> over <c>sampled_ms</c>, the
/// Aurora profile's sibling on another instrument) each live in their own partial file.</para>
///
/// <para><b>The metadata is the shared gate's, unchanged.</b> A z-family fact carries <c>deviation_sigma</c>,
/// <c>fire_threshold</c>, <c>baseline_low_quality</c>, <c>fallback_exceedance</c>, <c>baseline_samples</c> and the
/// baseline context (<c>confidence</c>, <c>baseline_tier</c>, median / MAD) exactly as <see cref="PgAnomalyDetector"/>
/// writes them, so <c>FactScorer.ScoreAnomalyFact</c> and <c>IsExtremeAnomaly</c> grade a PostgreSQL anomaly through
/// the ramp and the extremity escape #3584 built without knowing the engine — the membership that routes them
/// there is <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c>. The ratio families carry <c>ratio</c>, <c>is_new</c>
/// and <c>fallback_exceedance</c> and are graded by <c>PgTargetScorer.ScoreRatioAnomaly</c>. Every fact also carries
/// <c>threshold_lineage</c>, the verdict on the PostgreSQL floors and bars in <c>AnomalyThresholds</c> the fact was
/// gated on: 1 when every one of them is fleet-measured (the #3691 calibration of 2026-09-19 — TPS floor and
/// fallback, CPU floor and fallback), 0 when at least one is still a chosen number (the session COUNT floors, which
/// are unmeasured in count terms; the wait profile's borrowed heavy-tail cutoff; and the ratio arms' ramp spans in
/// <c>PgTargetScorer.Anomaly.cs</c>, so the deadlock-rate and wait-profile anomalies stay at 0 even though their
/// own floors are measured and the firing multiple <c>PgRatioAnomalyThreshold</c> was read on 2026-09-20 — well
/// placed for TPS, which never grades on it; routine alone for the wait rate, where the measured magnitude floor
/// and the peak-AND-mean gate do the work; an empty interval on 22 (server, bucket) pairs for deadlocks, which
/// places nothing). Each constant names its lineage, and <c>get_analysis_facts</c> shows the flag.</para>
///
/// <para><b>A known, deferred residue, copied rather than fixed (#3538 A8).</b> Like every SQL Server z-detector,
/// the window statistic is the PEAK per-collection value and the baseline bucket is a distribution of PER-SAMPLE
/// values, so "Nσ" reads the maximum of k samples against a per-sample frame and is biased high in proportion to
/// log k — a four-hour window of one-minute samples pulls the expected peak roughly 2.5 robust sigmas above the
/// median on a Gaussian series before anything is wrong. The bias is the same on both engines and the shipped
/// cutoffs were calibrated with it in; correcting it (a per-window-max baseline, or an order-statistic
/// adjustment) is a cross-engine change to the gate and its calibration, not something to invent for one engine
/// in one lane. Stated here so the comparison's shape is not mistaken for a per-sample one.</para>
///
/// <para><b>Window bounds and the observed clock.</b> Every window read is <c>&gt;= $2 AND &lt;= $3</c>, the
/// PostgreSQL fact reads' closed shape, bound naive-UTC; the deadlock rate divides by
/// <see cref="AnalysisContext.ObservedDurationMs"/> (the coverage witness's observed time, #3538 A7), never the
/// nominal window, so a 24-hour read and a 4-hour read of the same server say the same rate.</para>
/// </summary>
public sealed partial class PgTargetAnomalyDetector : IAnomalyDetector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly PgTargetBaselineProvider _baselineProvider;
    private readonly ILogger? _logger;

    public PgTargetAnomalyDetector(NpgsqlDataSource postgres, PgTargetBaselineProvider baselineProvider, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _baselineProvider = baselineProvider ?? throw new ArgumentNullException(nameof(baselineProvider));
        _logger = logger;
    }

    /// <summary>
    /// Baseline-data gate: <c>pg_database_stats</c> as canary — the one universal one-minute series (D3), the
    /// same table the coverage witness and the data-span gate read, so the three cannot disagree about whether
    /// this server has history. <c>$2</c> is <c>context.TimeRangeEnd.AddDays(-30)</c>, bound naive-UTC — never a
    /// bare <c>now()</c> — so an anchored pass (#2506) asks about the 30 days before ITS window.
    /// </summary>
    public const string HasBaselineDataSql = @"
SELECT COUNT(*)
FROM pg_database_stats
WHERE server_id = $1 AND collection_time >= $2";

    /* ── Window reads. Every one is a public const so PgTargetAnomalyTests can pin its table and shape ungated,
       the PgAnomalyDetector convention. ── */

    /// <summary>
    /// The window's transactions per second, per collection, then peak / average / count — the PG_TPS read's
    /// difference (<c>PgTargetFactCollector.PgTargetDatabaseCountersSql</c>: per-series <c>LAG</c>, <c>GREATEST</c>
    /// clamp, explicit reset) rated over each collection's own gap, exactly as the <c>pg_tps</c> baseline arm rates
    /// it, so peak and bucket are in one unit. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC). The
    /// deadlock difference rides in the same scan (summed, and per database for the fold) so the two families
    /// read the table once each rather than twice.
    /// </summary>
    public const string DatabaseCounterWindowSql = @"
WITH sampled AS (
    SELECT database_name,
           collection_time,
           (xact_commit + xact_rollback) - LAG(xact_commit + xact_rollback) OVER series AS raw_xacts,
           deadlocks - LAG(deadlocks) OVER series AS raw_deadlocks,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec
    FROM pg_database_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    WINDOW series AS (
        PARTITION BY database_name
        ORDER BY collection_time
    )
),
per_collection AS (
    SELECT collection_time,
           SUM(GREATEST(raw_xacts, 0))::DOUBLE PRECISION AS xacts,
           MAX(interval_sec) AS interval_sec
    FROM sampled
    WHERE raw_xacts IS NOT NULL
    GROUP BY collection_time
),
rated AS (
    SELECT collection_time, xacts / interval_sec AS tps
    FROM per_collection
    WHERE interval_sec > 0
),
top_deadlock_database AS (
    SELECT database_name, SUM(GREATEST(raw_deadlocks, 0)) AS deadlocks
    FROM sampled
    WHERE raw_deadlocks IS NOT NULL
    GROUP BY database_name
    ORDER BY SUM(GREATEST(raw_deadlocks, 0)) DESC NULLS LAST, database_name
    LIMIT 1
)
SELECT (SELECT MAX(tps) FROM rated)                                             AS peak_tps,
       (SELECT AVG(tps) FROM rated)                                             AS avg_tps,
       (SELECT COUNT(*) FROM rated)                                             AS tps_samples,
       (SELECT CAST(coalesce(SUM(GREATEST(raw_deadlocks, 0)), 0) AS bigint) FROM sampled WHERE raw_deadlocks IS NOT NULL) AS deadlocks,
       (SELECT COUNT(raw_deadlocks) FROM sampled)                               AS deadlock_intervals,
       (SELECT database_name FROM top_deadlock_database)                        AS top_deadlock_database,
       (SELECT CAST(coalesce(deadlocks, 0) AS bigint) FROM top_deadlock_database) AS top_deadlock_count";

    /// <summary>
    /// The window's instance-wide session count per capture (the denormalised <c>total_sessions</c>, picked by
    /// <c>MAX</c> — the PG_CONNECTION_SATURATION read's rule), then peak / average / count. Exception-table caveat as
    /// on the baseline arm: a quiet minute stores no capture and is absent from both sides alike.
    /// </summary>
    public const string SessionWindowSql = @"
WITH per_collection AS (
    SELECT collection_time, MAX(total_sessions)::DOUBLE PRECISION AS total_sessions
    FROM pg_session_states
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   total_sessions IS NOT NULL
    GROUP BY collection_time
)
SELECT MAX(total_sessions) AS peak_sessions,
       AVG(total_sessions) AS avg_sessions,
       COUNT(*)            AS sample_count
FROM per_collection";

    /// <summary>
    /// The window's percent-of-configured-capacity (Aurora): peak / average / count of <c>acu_utilization_percent</c>
    /// (#3281 — the bandable quantity; rows without a capacity sample are not samples), the time of the peak, and
    /// beside it the peak RAW <c>cpu_percent</c> for the advice to state as "was a core pinned", never to grade.
    /// </summary>
    public const string CpuWindowSql = @"
SELECT MAX(acu_utilization_percent) AS peak_capacity_pct,
       AVG(acu_utilization_percent) AS avg_capacity_pct,
       COUNT(acu_utilization_percent) AS sample_count,
       (SELECT collection_time FROM pg_cpu_utilization
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
        AND   acu_utilization_percent IS NOT NULL
        ORDER BY acu_utilization_percent DESC, collection_time DESC LIMIT 1) AS peak_time,
       MAX(cpu_percent) AS peak_cpu_percent,
       COUNT(*) AS rows_in_window
FROM pg_cpu_utilization
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3";

    /// <summary>
    /// The window's Aurora all-types wait rate per collection (CPU excluded; the three-state interval of the wait
    /// partial and the <c>pg_wait_ms_per_sec</c> baseline arm — stored <c>NULLIF</c>, NULL <c>LAG</c>, a restart
    /// collection is not a sample), then PEAK, MEAN, total and the count of rated collections. The MEAN is the
    /// #3691 parity line ("Aurora wait-profile detector lacks the peak-AND-mean gate"): the same arm and the same
    /// guard as the peak, so a NULL-interval collection contributes NULL to both aggregates and both ignore it —
    /// peak and mean describe the same sample set and the gate compares like with like. The column order is the
    /// reader's ordinal contract (0 peak, 1 mean, 2 total, 3 sample count, 4 collection count), pinned. Zero rows
    /// means this flavour does not write the table and the detector sits out; stock's sampled estimate is its own
    /// detector (<c>SampledWaitRateWindowSql</c>, lane 24).
    /// </summary>
    public const string WaitRateWindowSql = @"
WITH per_collection AS (
    SELECT collection_time,
           CAST(SUM(GREATEST(delta_wait_time_us, 0)) FILTER (WHERE lower(wait_type) IS DISTINCT FROM 'cpu') AS DOUBLE PRECISION) / 1000.0 AS total_wait_ms,
           CASE WHEN MAX(sample_interval_seconds) IS NULL
                THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                ELSE NULLIF(MAX(sample_interval_seconds), 0)
           END AS interval_sec
    FROM pg_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    GROUP BY collection_time
)
SELECT MAX(CASE WHEN interval_sec > 0 THEN coalesce(total_wait_ms, 0) / interval_sec END) AS peak_ms_per_sec,
       AVG(CASE WHEN interval_sec > 0 THEN coalesce(total_wait_ms, 0) / interval_sec END) AS mean_ms_per_sec,
       SUM(coalesce(total_wait_ms, 0)) FILTER (WHERE interval_sec > 0)                    AS total_wait_ms,
       COUNT(*) FILTER (WHERE interval_sec > 0)                                          AS sample_count,
       COUNT(*)                                                                          AS collection_count
FROM per_collection";

    /// <summary>The window's six largest (type, event) wait contributors by measured time, CPU excluded, named in
    /// the anomaly's <c>contrib_Type:event</c> metadata keys (the value is milliseconds) for the advice to lead with.</summary>
    public const string WaitContribWindowSql = @"
SELECT wait_type, wait_event, CAST(SUM(GREATEST(delta_wait_time_us, 0)) / 1000 AS bigint) AS total_ms
FROM pg_wait_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   wait_type IS NOT NULL
AND   lower(wait_type) IS DISTINCT FROM 'cpu'
AND   delta_wait_time_us > 0
GROUP BY wait_type, wait_event
ORDER BY total_ms DESC
LIMIT 6";

    /// <summary>
    /// The detectors, in order, behind the gate. Each is fenced in its own try so one metric's failure costs the
    /// pass that metric and nothing else (<see cref="PgAnomalyDetector"/>'s per-detector tolerance), and each
    /// reaches the shared bucket machinery through <see cref="Baselines"/>.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var anomalies = new List<Fact>();

        if (!await HasBaselineDataAsync(context.ServerId, context.TimeRangeEnd, context.CancellationToken))
            return anomalies;

        await DetectTpsAnomalies(context, anomalies);
        await DetectSessionAnomalies(context, anomalies);
        await DetectCpuAnomalies(context, anomalies);
        await DetectDeadlockRateAnomalies(context, anomalies);
        await DetectWaitProfileAnomalies(context, anomalies);
        /* v2 (#3691): the three new detectors, each in its own partial file (PgTargetAnomalyDetector.{Io,
           Replication,Wal}.cs) so the lanes that fill them (11 / 12 / 15) never edit this one. Reachable and
           inert until then: each stub returns without a read or a fact. */
        await DetectIoAnomalies(context, anomalies);
        await DetectReplicationAnomalies(context, anomalies);
        await DetectWalVolumeAnomalies(context, anomalies);
        /* wave 3 (#3691, between waves): the blocking detector (PgTargetAnomalyDetector.Blocking.cs), inert until lane 17. */
        await DetectBlockingAnomalies(context, anomalies);
        /* lane 24 (#3691): stock's SAMPLED wait profile over sampled_ms (PgTargetAnomalyDetector.WaitsSampled.cs); sits
           out on Aurora and wherever pg_wait_stats also wrote the window. */
        await DetectSampledWaitProfileAnomalies(context, anomalies);
        /* v3 (#3691 plumbing): the plan-regression and CPU-burn detectors (PgTargetAnomalyDetector.Plans.cs / .Kernel.cs),
           inert until lanes 27 / 28. */
        await DetectPlanRegressionAnomalies(context, anomalies);
        await DetectCpuBurnAnomalies(context, anomalies);
        /* lane 34 (#3691, ruled 2026-09-20): one statement's share against its OWN hour-of-week share bucket
           (PgTargetAnomalyDetector.Queries.cs) — the bad actor's grade since the ruling; after the plan detector because
           it reads the same table and its candidates are the queries family's own. */
        await DetectBadActorShareAnomalies(context, anomalies);
        /* lane 38 (#3691): the instance total's growth rate against its own baseline (PgTargetAnomalyDetector.Growth.cs);
           silent wherever total_bytes is NULL (a database the role may not size). */
        await DetectDatabaseGrowthAnomalies(context, anomalies);

        return anomalies;
    }

    /* v2 (#3691) detector partials — declared here in emission order, stubbed in their own files. A filled
       body copies the five above: its own try / catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(
       ex, context.CancellationToken)) fence, its bucket through Baselines.GetBaselineAsync, its window read a
       public const string …Sql over the collector table, and AnomalyGate's metadata on the fact. */
    private partial Task DetectIoAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectReplicationAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectWalVolumeAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectBlockingAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectSampledWaitProfileAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectPlanRegressionAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectCpuBurnAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectBadActorShareAnomalies(AnalysisContext context, List<Fact> anomalies);
    private partial Task DetectDatabaseGrowthAnomalies(AnalysisContext context, List<Fact> anomalies);

    /// <summary>The provider the filled detectors read buckets from; exposed for lane 9's detector bodies.</summary>
    internal PgTargetBaselineProvider Baselines => _baselineProvider;

    /// <summary>
    /// <see cref="PgAnomalyDetector"/>'s gate, verbatim but for the SQL: silent on a genuine fault BY DESIGN (an
    /// unreadable canary reads as "no baseline data" and detection sits out the pass), with shutdown residue
    /// excluded (#2299) so a stop mid-gate unwinds to the pass's one Information line instead of masquerading as
    /// an empty baseline.
    /// </summary>
    private async Task<bool> HasBaselineDataAsync(int serverId, DateTime windowEnd, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            using var cmd = new NpgsqlCommand(HasBaselineDataSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(AsNaive(windowEnd.AddDays(-30)));

            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(cancellationToken) ?? 0);
            return count > 0;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            _logger?.LogDebug(
                "[PgTargetAnomalyDetector] Baseline-data gate could not be read for server {ServerId}; anomaly detection sits out this pass: {Message}",
                serverId, ex.Message);
            return false;
        }
    }

    /* ── Detectors. Each wraps its reads in its own try (the per-detector tolerance PgAnomalyDetector documents),
       sets CommandTimeout on every command, passes context.CancellationToken to every store call, and binds
       every bound naive-UTC. ── */

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalyTps"/>: the window's peak per-collection transactions per second against
    /// the <c>pg_tps</c> bucket, through the shared gate — robust modified-z at the standard 3.5 cutoff when the
    /// bucket carries median/MAD, classical 2σ otherwise, <see cref="AnomalyThresholds.PgTpsFloor"/> as the
    /// magnitude ceiling on the trusted path and <see cref="AnomalyThresholds.PgTpsFallback"/> as the bar on an
    /// untrustworthy one. No regular parent: throughput is context, and a TPS anomaly stays its own card unless
    /// a sibling load anomaly corroborates it (<c>PgTargetScorer.Anomaly.cs</c>).
    /// </summary>
    private async Task DetectTpsAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgTps, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            var window = await ReadDatabaseCounterWindowAsync(context);
            if (window is null || window.Value.TpsSamples == 0) return;
            var (peakTps, avgTps, tpsSamples, _, _, _, _) = window.Value;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakTps, avgTps,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgTps), PgTpsFloor, PgTpsFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            /* measured: PgTpsFloor / PgTpsFallback carry the 2026-09-19 fleet lineage (AnomalyThresholds). */
            var metadata = ZScoreMetadata(baseline, decision, tpsSamples, barsMeasured: true);
            metadata["peak_tps"] = peakTps;
            metadata["avg_tps"] = avgTps;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyTps,
                Value = peakTps,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] TPS anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalySessionSpike"/>: the window's peak capture session count against the
    /// <c>pg_session_count</c> bucket. The floors are COUNTS with unmeasured lineage (a fraction of
    /// <c>max_connections</c> is meaningless as a constant, and this detector cannot see the config fact — it has
    /// the context, not the fact set), and they stay unmeasured after the 2026-09-19 calibration, which read
    /// sessions as a fraction of each server's ceiling and not as counts — so <c>threshold_lineage</c> stays 0 here;
    /// the ceiling-aware arm is the regular <c>PG_CONNECTION_SATURATION</c> fact,
    /// which the reconciler folds this anomaly into and which is the never-blind arm on a young store, so the
    /// fallback bar here is deliberately high enough not to be that finding twice.
    /// </summary>
    private async Task DetectSessionAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgSessionCount, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(SessionWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var peakSessions = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgSessions = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakSessions, avgSessions,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgSessionCount), PgSessionCountFloor, PgSessionCountFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = ZScoreMetadata(baseline, decision, windowSamples);
            metadata["peak_sessions"] = peakSessions;
            metadata["avg_sessions"] = avgSessions;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalySessionSpike,
                Value = peakSessions,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Session anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalyCpuSpike"/> (Aurora / Performance Insights only): the window's peak
    /// percent of the CONFIGURED capacity ceiling against the <c>pg_cpu</c> bucket. On stock PostgreSQL the table
    /// has no rows for the server, the baseline is empty and the detector contributes nothing — structurally
    /// absent, which the D6 disclosure says out loud; on a provisioned Aurora instance with no capacity sample the
    /// same, by the card's "Unknown, never Healthy" rule (#3271). Folds into <c>PG_CPU_PERCENT</c>.
    /// </summary>
    private async Task DetectCpuAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgCpu, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(CpuWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var peakCapacity = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgCapacity = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var peakTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
            var peakCpuPercent = reader.IsDBNull(4) ? (double?)null : Convert.ToDouble(reader.GetValue(4));
            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakCapacity, avgCapacity,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgCpu), PgCpuFloorPct, PgCpuFallbackPct, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            /* measured: PgCpuFloorPct / PgCpuFallbackPct carry the 2026-09-19 fleet lineage (AnomalyThresholds). */
            var metadata = ZScoreMetadata(baseline, decision, windowSamples, barsMeasured: true);
            metadata["peak_capacity_pct"] = peakCapacity;
            metadata["avg_capacity_pct"] = avgCapacity;
            metadata["peak_time_ticks"] = peakTime?.Ticks ?? 0;
            if (peakCpuPercent is { } raw)
                metadata["peak_cpu_percent"] = raw;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyCpuSpike,
                Value = peakCapacity,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] CPU anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalyDeadlockRate"/>: the window's deadlocks per OBSERVED hour (the reset-aware
    /// counter difference the <c>PG_DEADLOCK_RATE</c> fact takes, over <see cref="AnalysisContext.ObservedDurationMs"/>)
    /// as a RATIO of the <c>pg_deadlock_rate</c> bucket's mean — the event-family shape, because a healthy server's
    /// deadlock series is mostly zeros and a modified z against a collapsed frame would be arithmetic, not evidence.
    /// Trustworthy bucket: fires at <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/>× the mean AND at least
    /// <see cref="AnomalyThresholds.PgDeadlockRateFloorPerHour"/>. Untrustworthy: fires on the rate alone at
    /// <see cref="AnomalyThresholds.PgDeadlockRateFallbackPerHour"/> — the regular fact's own measured Warning
    /// tier — marked <c>is_new</c> with NO sentinel ratio (the composer renders "first occurrence", and the scorer
    /// grades <c>fallback_exceedance</c>). Stamped with the database that deadlocked most, as the regular fact is,
    /// because the reconciler folds on (family, database) and a server-scoped anomaly would never meet a
    /// database-scoped parent.
    /// </summary>
    private async Task DetectDeadlockRateAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var observedHours = context.ObservedDurationMs / 3_600_000.0;
            if (observedHours <= 0) return;

            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgDeadlockRate, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            var window = await ReadDatabaseCounterWindowAsync(context);
            if (window is null) return;
            var (_, _, _, deadlocks, deadlockIntervals, topDatabase, topCount) = window.Value;
            if (deadlockIntervals == 0 || deadlocks <= 0) return;

            var ratePerHour = deadlocks / observedHours;
            var trustworthy = baseline.IsTrustworthy;
            var baselineRate = baseline.Mean;

            double ratio;
            double fallbackExceedance;
            bool isNew;
            if (trustworthy && baselineRate > 0)
            {
                isNew = false;
                ratio = ratePerHour / baselineRate;
                fallbackExceedance = 0;
                if (ratio < PgRatioAnomalyThreshold || ratePerHour < PgDeadlockRateFloorPerHour) return;
            }
            else
            {
                /* A bucket that is trustworthy by density but whose mean is exactly 0 (every sample a zero) has no
                   ratio to state either — the same first-occurrence reading as a thin bucket. */
                isNew = true;
                ratio = 0;
                fallbackExceedance = ratePerHour / PgDeadlockRateFallbackPerHour;
                if (fallbackExceedance < 1.0) return;
            }

            var metadata = new Dictionary<string, double>
            {
                ["current_count"] = deadlocks,
                ["current_rate_per_hour"] = ratePerHour,
                ["observed_hours"] = observedHours,
                ["baseline_rate"] = baselineRate,
                ["baseline_samples"] = baseline.SampleCount,
                ["ratio"] = ratio,
                ["is_new"] = isNew ? 1 : 0,
                ["fallback_exceedance"] = fallbackExceedance,
                ["fire_threshold"] = PgRatioAnomalyThreshold,
                ["top_database_count"] = topCount,
                /* 0, not 1: the floor (1/h) and the fallback (the measured alert tier) are measured, and the 2026-09-20
                   read of PgRatioAnomalyThreshold found only 22 of 8,400 (server, bucket) pairs with a non-zero centre
                   and no ratio at 3 in 7 days — an empty interval on a population too thin to PLACE the multiple, so
                   it stays an unplaced bar here (this arm is the is_new one in practice); and the scorer's ramp span
                   (PgTargetScorer.RatioAnomalySaturation) is chosen. One unplaced bar keeps the flag at 0. */
                ["threshold_lineage"] = 0,
            };
            AddBaselineContext(metadata, baseline);

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyDeadlockRate,
                Value = ratePerHour,
                ServerId = context.ServerId,
                DatabaseName = topDatabase,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Deadlock-rate anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalyWaitProfile"/> (Aurora only in v1): the window's peak per-collection
    /// all-types wait rate, CPU excluded, against the <c>pg_wait_ms_per_sec</c> bucket — <see cref="PgAnomalyDetector"/>'s
    /// wait-profile detector with the table swapped. Robust bucket: modified z at the heavy-tail cutoff
    /// (<see cref="AnomalyThresholds.HeavyTailModifiedZThreshold"/> — the SQL Server fleet's calibrated 5.0, used
    /// by reference as the shared robust-statistic cutoff; the PostgreSQL distribution has not been read, so the
    /// fact still carries <c>threshold_lineage = 0</c> although the magnitude bar itself was placed against the
    /// 2026-09-19 fleet read — see <see cref="AnomalyThresholds.PgWaitProfileFallbackMsPerSec"/>) AND the magnitude bar; classical bucket: the ratio at
    /// <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/> (read 2026-09-20 and routine alone for this family —
    /// the bar is what decides; see the constant); untrustworthy: the bar alone, <c>is_new</c>, no
    /// sentinel. Top contributors ride as <c>contrib_Type:event</c>. The stock sampled estimate is never read here
    /// and the fact therefore never carries <c>is_sampled</c>; the sampled arm is its own metric, key and bar
    /// (<c>DetectSampledWaitProfileAnomalies</c>, lane 24) and sits out whenever this table has rows.
    ///
    /// <para><b>The peak-AND-mean gate (#3691 parity line; #3773's shape).</b> Lane 9 shipped this arm gating on the
    /// window PEAK alone — the #3538 A8 shape #3724 removed from every other baseline detector and #3773 then
    /// removed from the SQL Server wait profile: one hot five-minute delta in an otherwise quiet window fired the
    /// profile anomaly, and a longer window bought more of them. The trusted robust arm is now the shared
    /// <see cref="AnomalyGate"/> PAIR call, exactly as <see cref="PgAnomalyDetector"/> takes it: the window peak
    /// AND the window mean must BOTH clear the 5.0 modified-z cutoff against the bucket's median and
    /// <c>EffectiveRobustSigma</c> (the arithmetic of <c>BaselineMath.ModifiedZScore</c>, which still stamps the
    /// uncapped <c>modified_z</c> the scorer grades), with the magnitude bar on the peak only — the gate's rule: the
    /// floor is the "trivial value" ceiling for the value the finding reports, sized for a peak. The
    /// <c>EffectiveRobustSigma &gt; 0</c> guard on this arm makes the gate's classical frame unreachable here, so
    /// the same 5.0 is passed as both cutoffs and the family's one bar as both floor and bar. The ratio arm asks
    /// the same of both ratios (peak/mean AND window-mean/mean over <c>PgRatioAnomalyThreshold</c>); the no-baseline
    /// arm stays on the peak's absolute bar alone — there is no z to trust on either statistic there, and #3741's
    /// ruling keeps that bar where it was (the sampled twin asks its bar of both; that divergence is stated, not
    /// hidden). No bar value changes and no lineage moves: the same constants, asked of one more statistic.
    /// The honest caveat #3773 wrote is this arm's too: the mean's z is judged against the SAME per-collection
    /// median/MAD as the peak's (one centre, one dispersion, one <c>Decide</c> body), which for a heavy-tailed
    /// family is a little LESS strict than a symmetric reading suggests — a bias toward firing, never toward
    /// silence, and bounded, because the mean clause can only admit a window the peak already admitted.</para>
    ///
    /// <para><b>Metadata.</b> <c>current_ms_per_sec</c> stays the PEAK (the value the story leads with and the ratio
    /// is taken on); <c>mean_ms_per_sec</c>, <c>mean_ratio</c> and <c>mean_modified_z</c> ride beside it in the
    /// same uncapped frame — the SAMPLED twin's keys (<c>DetectSampledWaitProfileAnomalies</c>), not
    /// <c>PgAnomalyDetector</c>'s <c>avg_ms_per_sec</c>, because the two PostgreSQL-target profiles share one
    /// vocabulary through <see cref="PgTargetFactKeys.IsWaitProfileAnomaly"/> and their advice composers read the
    /// same names. Stamped on every arm: 0 modified z on a robust-less bucket, exactly as <c>modified_z</c> is.
    /// <see cref="PgTargetScorer.IsExtremeWaitProfileAnomaly"/> and the scorer read <c>modified_z</c> / <c>ratio</c>
    /// unchanged — the peak's — so the extremity escape and the grade are what they were.</para>
    /// </summary>
    private async Task DetectWaitProfileAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgWaitMsPerSec, context.TimeRangeStart, context.CancellationToken);

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            double peakRate, meanRate, totalWaitMs;
            long sampleCount, collectionCount;
            using (var rateCmd = WindowCommand(WaitRateWindowSql, connection, context))
            {
                using var rateReader = await rateCmd.ExecuteReaderAsync(context.CancellationToken);
                if (!await rateReader.ReadAsync(context.CancellationToken)) return;
                peakRate = rateReader.IsDBNull(0) ? 0.0 : Convert.ToDouble(rateReader.GetValue(0));
                meanRate = rateReader.IsDBNull(1) ? 0.0 : Convert.ToDouble(rateReader.GetValue(1));
                totalWaitMs = rateReader.IsDBNull(2) ? 0.0 : Convert.ToDouble(rateReader.GetValue(2));
                sampleCount = rateReader.IsDBNull(3) ? 0L : Convert.ToInt64(rateReader.GetValue(3));
                collectionCount = rateReader.IsDBNull(4) ? 0L : Convert.ToInt64(rateReader.GetValue(4));
            }

            /* No rows: this flavour does not write pg_wait_stats (stock) — sit out. Rows but no rated collection:
               every collection a restart — nothing to judge. */
            if (collectionCount == 0 || sampleCount == 0) return;
            if (baseline.SampleCount == 0) return;

            bool isNew;
            double ratio, meanRatio, fallbackExceedance;
            var modifiedZ = BaselineMath.ModifiedZScore(baseline, peakRate);
            var meanModifiedZ = BaselineMath.ModifiedZScore(baseline, meanRate);
            if (baseline.IsTrustworthy && baseline.EffectiveRobustSigma > 0)
            {
                isNew = false;
                ratio = baseline.Mean > 0 ? peakRate / baseline.Mean : 0;
                meanRatio = baseline.Mean > 0 ? meanRate / baseline.Mean : 0;
                fallbackExceedance = 0;
                /* The shared PAIR gate (#3773's call, verbatim but for this family's bar): peak AND mean at the
                   heavy-tail cutoff by reference, the (measured) magnitude bar on the peak alone. See the summary
                   for why the same cutoff is passed twice and the same bar as floor and fallback. */
                var decision = AnomalyGate.EvaluateZScore(
                    baseline, peakRate, meanRate,
                    HeavyTailModifiedZThreshold, HeavyTailModifiedZThreshold, PgWaitProfileFallbackMsPerSec, PgWaitProfileFallbackMsPerSec, SigmaDisplayCap,
                    window: context.TimeRangeEnd - context.TimeRangeStart);
                if (!decision.Fire) return;
            }
            else if (baseline.IsTrustworthy && baseline.Mean > 0)
            {
                isNew = false;
                ratio = peakRate / baseline.Mean;
                meanRatio = meanRate / baseline.Mean;
                fallbackExceedance = 0;
                /* measured: PgRatioAnomalyThreshold on both statistics (routine alone for the wait rate, 2026-09-20),
                   PgWaitProfileFallbackMsPerSec on the peak (the bar that decides, 2026-09-19). */
                if (ratio < PgRatioAnomalyThreshold || meanRatio < PgRatioAnomalyThreshold || peakRate < PgWaitProfileFallbackMsPerSec) return;
            }
            else
            {
                isNew = true;
                ratio = 0;
                meanRatio = 0;
                fallbackExceedance = peakRate / PgWaitProfileFallbackMsPerSec;
                /* The peak's bar alone, by #3741's ruling (summary): no z to trust on either statistic here. */
                if (fallbackExceedance < 1.0) return;
            }

            var metadata = new Dictionary<string, double>
            {
                ["current_ms_per_sec"] = peakRate,
                ["mean_ms_per_sec"] = meanRate,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_samples"] = baseline.SampleCount,
                ["total_wait_ms"] = totalWaitMs,
                ["window_samples"] = sampleCount,
                ["ratio"] = ratio,
                ["mean_ratio"] = meanRatio,
                ["modified_z"] = modifiedZ,
                ["mean_modified_z"] = meanModifiedZ,
                ["is_new"] = isNew ? 1 : 0,
                ["fallback_exceedance"] = fallbackExceedance,
                ["fire_threshold"] = isNew ? 0 : (baseline.EffectiveRobustSigma > 0 ? HeavyTailModifiedZThreshold : PgRatioAnomalyThreshold),
                /* 0, not 1: the magnitude bar is measured (2026-09-19) and the ratio multiple was read (2026-09-20:
                   routine ALONE at 3.0 — ≈ p93 of wait-rate ratios, 7.1 % of samples at or above it — so the floor
                   and the peak-AND-mean gate are what decide, not the multiple), but the heavy-tail cutoff is the SQL
                   Server fleet's by reference and the scorer's ramp spans are chosen — one unmeasured bar keeps the
                   flag at 0. */
                ["threshold_lineage"] = 0,
            };
            AddBaselineContext(metadata, baseline);

            using (var contribCmd = WindowCommand(WaitContribWindowSql, connection, context))
            {
                using var contribReader = await contribCmd.ExecuteReaderAsync(context.CancellationToken);
                while (await contribReader.ReadAsync(context.CancellationToken))
                {
                    var waitType = contribReader.GetString(0);
                    var waitEvent = contribReader.IsDBNull(1) ? null : contribReader.GetString(1);
                    var name = string.IsNullOrEmpty(waitEvent) ? waitType : waitType + ":" + waitEvent;
                    metadata["contrib_" + name] = Convert.ToDouble(contribReader.GetValue(2));
                }
            }

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyWaitProfile,
                Value = totalWaitMs,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Wait-profile anomaly detection failed: {Message}", ex.Message);
        }
    }

    /* ── Shared pieces. ── */

    /// <summary>The anomaly facts' source — <c>"anomaly"</c> on both engines; the KEY carries the <c>ANOMALY_PG_</c>
    /// prefix the shared scorer routes on (<see cref="PgTargetSources"/>' remarks).</summary>
    internal const string AnomalySource = "anomaly";

    /// <summary>The one <c>pg_database_stats</c> window read, shared by the TPS and deadlock-rate detectors. Each
    /// detector runs it for itself — two indexed scans of a four-hour window, rather than a memo on a detector
    /// the MCP host holds as a per-engine singleton across concurrent server requests. Null when the window has
    /// no rows.</summary>
    private async Task<(double PeakTps, double AvgTps, long TpsSamples, long Deadlocks, long DeadlockIntervals, string? TopDatabase, long TopCount)?> ReadDatabaseCounterWindowAsync(AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
        using var cmd = WindowCommand(DatabaseCounterWindowSql, connection, context);
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return null;

        return (
            PeakTps: reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0)),
            AvgTps: reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
            TpsSamples: reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
            Deadlocks: reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
            DeadlockIntervals: reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
            TopDatabase: reader.IsDBNull(5) ? null : reader.GetString(5),
            TopCount: reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)));
    }

    private static NpgsqlCommand WindowCommand(string sql, NpgsqlConnection connection, AnalysisContext context)
    {
        var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        return cmd;
    }

    /// <summary>The z-family metadata the shared scorer grades from (<c>FactScorer.ScoreAnomalyFact</c> /
    /// <c>IsExtremeAnomaly</c>), key for key what <see cref="PgAnomalyDetector"/> writes, plus the lineage stamp:
    /// <paramref name="barsMeasured"/> is the caller's verdict on ITS floor and fallback pair in
    /// <c>AnomalyThresholds</c> (TPS and CPU measured 2026-09-19; the session counts not), stated at the call so the
    /// stamp sits beside the constants it describes.
    /// <para>#3691 lane 41: <c>baseline_zero_history</c> rides beside <c>baseline_low_quality</c> here, so every
    /// PostgreSQL-target family inherits the zero-history stamp from the one helper it already shares. The two are
    /// mutually exclusive by the gate's construction — a zero-history fire stamps <c>baseline_low_quality = 0</c>
    /// and <c>baseline_zero_history = 1</c>, and the advice reads the pair to choose its third shape.</para></summary>
    private static Dictionary<string, double> ZScoreMetadata(BaselineBucket baseline, AnomalyGate.ZDecision decision, long windowSamples, bool barsMeasured = false)
    {
        var metadata = new Dictionary<string, double>
        {
            ["baseline_mean"] = baseline.Mean,
            ["baseline_stddev"] = baseline.EffectiveStdDev,
            ["deviation_sigma"] = decision.Sigma,
            ["fire_threshold"] = decision.ThresholdUsed,
            ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
            ["baseline_zero_history"] = decision.ZeroHistory ? 1 : 0,
            ["fallback_exceedance"] = decision.FallbackExceedance,
            ["baseline_samples"] = baseline.SampleCount,
            ["window_samples"] = windowSamples,
            ["threshold_lineage"] = barsMeasured ? 1 : 0,
        };
        AddBaselineContext(metadata, baseline);
        return metadata;
    }

    /// <summary><see cref="PgAnomalyDetector"/>'s baseline context, verbatim: the bucket the peak was judged in
    /// and the honest confidence the scorer multiplies by.</summary>
    private static void AddBaselineContext(Dictionary<string, double> metadata, BaselineBucket baseline)
    {
        metadata["baseline_hour"] = baseline.HourOfDay;
        metadata["baseline_dow"] = baseline.DayOfWeek;
        metadata["baseline_tier"] = (double)baseline.Tier;
        metadata["baseline_median"] = baseline.Median;
        metadata["baseline_mad"] = baseline.Mad;
        metadata["confidence"] = baseline.Confidence;
        /* #3691 lane 41: the DISTINCT-DAY count, which the bucket has always carried and no fact ever showed.
           The zero-history extremity's whole claim is "N samples across D days of this hour, never once
           non-zero", and a sample count alone cannot say it — 250 samples from two busy afternoons is not a
           month. Stamped on every z-family fact, not just the zero-history ones, because it is the same
           quality signal IsTrustworthy's day floor reads and an operator reading any baseline fact wants it. */
        metadata["baseline_distinct_days"] = baseline.DistinctDays;
        /* #3859: on the FLAT tier that count is a CEILING PROXY, not a measurement, and the fact says so.
           CollapseToFlat takes MAX(DistinctDays) over the hour buckets (a calendar day recurs across the 24,
           so summing would double-count) and each (hour, dow) bucket holds at most ~5 same-weekday dates in a
           30-day window, so a Flat bucket reports about 5 however much history it pooled. The admission lives
           at the CollapseToFlat site where nothing downstream can read it; this stamp is what stops a
           get_analysis_facts reader treating the ceiling as a measurement. Stamped only where it is true. */
        if (baseline.Tier == BaselineTier.Flat)
            metadata["baseline_distinct_days_is_proxy"] = 1;
    }

    /// <summary>Kind-Unspecified for parameter binds — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
