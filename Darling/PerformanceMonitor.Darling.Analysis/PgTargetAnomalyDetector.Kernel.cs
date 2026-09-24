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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetAnomalyDetector
{
    /// <summary>
    /// The CPU-burn window read: <see cref="PgTargetFactCollector.PgTargetKernelCpuSql"/> BY ALIAS — the same text
    /// the <c>PG_CPU_BURN_CORES</c> fact is built from, so the peak and mean this detector judges and the figures the
    /// fact states are one number from one differencing (per-identity Δ over each collection's own gap, reset-aware
    /// on <c>stats_since</c>, summed per collection — the <c>pg_cpu_burn_cores</c> bucket's unit). Declared here as
    /// well so the detector census (<c>PgTargetAnomalyTests</c>: every <c>public const string …Sql</c> on this type is
    /// PostgreSQL dialect, server-scoped, window-bound, collector tables only) reflects it.
    /// </summary>
    public const string CpuBurnWindowSql = PgTargetFactCollector.PgTargetKernelCpuSql;

    /// <summary>
    /// <c>ANOMALY_PG_CPU_BURN</c> (/* filled by lane 28 of #3691 — the marker stays, as v1's did */): the window's
    /// peak AND mean cores busy per collection against the <c>pg_cpu_burn_cores</c> bucket, through the shared gate —
    /// robust modified-z at the standard 3.5 cutoff when the bucket carries median/MAD, classical 2σ otherwise,
    /// <see cref="AnomalyThresholds.PgCpuBurnCoresFloor"/> as the magnitude floor on the trusted path and
    /// <see cref="AnomalyThresholds.PgCpuBurnCoresFallback"/> as the bar on an untrustworthy one (both unmeasured —
    /// Aurora has no <c>pg_stat_kcache</c>, so the fleet calibration had no population; the fact says so with
    /// <c>threshold_lineage = 0</c>). The detector behind the CONTEXT fact <c>PG_CPU_BURN_CORES</c>: that fact
    /// states the cores, this one grades them, and there is no absolute bar anywhere because no core count is
    /// collected — a server whose routine is 2 cores busy and today burns 6 is the finding. Registered as
    /// deviation-scored by the v3 plumbing; folds onto <c>PG_CPU_BURN_CORES</c> through
    /// <c>PgTargetFactKeys.AnomalyToFamilies</c>; lifts <c>PG_CPU_DECOMPOSITION</c> to the incident line through the
    /// kernel scorer's compute-bound amplifier when both fire.
    ///
    /// <para><b>The PAIR gate (#3653), as the blocking detector.</b> A cores-busy series is spiky by nature — one
    /// collection in which a maintenance statement ran hot is a 4 against a routine of 1 — and the peak alone would
    /// fire on every such minute in a long window. Requiring the window's MEAN to clear the same cutoff (trusted
    /// path) or the magnitude floor (fallback path) makes the finding "the server was burning well above its routine
    /// for much of this window", which is what an operator would call a CPU storm. The consequence is stated: a
    /// one-hour storm inside a 24-hour anchored pass may not fire here — <c>PG_CPU_BURN_CORES</c> still states the
    /// peak, and <c>get_pg_kernel_stats</c> ranks the statements.</para>
    ///
    /// <para><b>Window first, bucket second — the WAL detector's order, for the WAL detector's reason.</b> On every
    /// PostgreSQL target the fleet monitors today the extension is absent and <c>pg_kernel_stats</c> is empty, and a
    /// detector that fetched a thirty-day bucket before discovering the window had nothing to judge would run the
    /// largest read in the family for nothing on every pass. So the four-hour window is read first; a window with no
    /// rated collection returns without asking for the bucket and without a fact — that is not an inference of
    /// absence (the collector reads <c>pg_extension_availability</c> for that and says so on its fact), it is "nothing
    /// to judge". Only a rated window reaches the baseline.</para>
    /// </summary>
    private async partial Task DetectCpuBurnAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            double peakCores, meanCores;
            long ratedSamples;
            double? topQueryId = null;

            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = WindowCommand(CpuBurnWindowSql, connection, context);
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                if (!await reader.ReadAsync(context.CancellationToken)) return;

                peakCores = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
                meanCores = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
                ratedSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
                if (!reader.IsDBNull(9))
                    topQueryId = Convert.ToDouble(reader.GetValue(9));
            }

            if (ratedSamples == 0 || peakCores <= 0) return;

            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgCpuBurnCores, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakCores, meanCores,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgCpuBurnCores), PgCpuBurnCoresFloor, PgCpuBurnCoresFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            /* unmeasured: both bars (see AnomalyThresholds) — the helper's default stamp, threshold_lineage = 0. */
            var metadata = ZScoreMetadata(baseline, decision, ratedSamples);
            metadata["peak_cores_busy"] = peakCores;
            metadata["mean_cores_busy"] = meanCores;
            metadata["mean_sigma"] = decision.MeanSigma ?? 0.0;
            if (topQueryId is { } top)
                metadata[PgTargetScorer.KernelTopQueryIdKey] = top;
            /* The ratio the advice states ("4.2 cores busy against a routine of 1.1 for this hour"): peak over the
               bucket's robust centre — the median when the bucket has one, the mean otherwise — the same centre the
               deviation prose names, so the sentence's two numbers agree. Stamped only when the centre is a number
               to divide by. */
            var centre = baseline.Median > 0 ? baseline.Median : baseline.Mean;
            if (centre > 0)
                metadata["baseline_ratio"] = peakCores / centre;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyCpuBurn,
                Value = peakCores,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] CPU-burn anomaly detection failed: {Message}", ex.Message);
        }
    }
}
