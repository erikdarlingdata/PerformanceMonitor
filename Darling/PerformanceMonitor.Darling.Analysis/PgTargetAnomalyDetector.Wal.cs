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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetAnomalyDetector
{
    /// <summary>
    /// The WAL-volume window read: <see cref="PgTargetFactCollector.PgTargetWalVolumeSql"/> BY ALIAS — the same
    /// text the <c>PG_WAL_VOLUME_SHIFT</c> fact is built from, so the peak this detector judges and the peak the
    /// fact states are one number from one differencing (per-collection Δ<c>wal_bytes</c> over the collection's
    /// own gap, reset-clamped, the <c>pg_wal_bytes_per_sec</c> bucket's unit). Declared here as well so the
    /// detector census (<c>PgTargetAnomalyTests</c>: every <c>public const string …Sql</c> on this type is
    /// PostgreSQL dialect, server-scoped, window-bound, collector tables only) reflects it.
    /// </summary>
    public const string WalVolumeWindowSql = PgTargetFactCollector.PgTargetWalVolumeSql;

    /// <summary>
    /// <c>ANOMALY_PG_WAL_VOLUME</c> (/* filled by lane 15 — #3691 step 15, design §3.11 */): the window's peak WAL
    /// bytes per second per collection against the <c>pg_wal_bytes_per_sec</c> bucket, through the shared gate —
    /// robust modified-z at the standard 3.5 cutoff when the bucket carries median/MAD, classical 2σ otherwise,
    /// <see cref="AnomalyThresholds.PgWalBytesFloorPerSec"/> as the magnitude ceiling on the trusted path and
    /// <see cref="AnomalyThresholds.PgWalBytesFallbackPerSec"/> as the bar on an untrustworthy one (both
    /// unmeasured; the fact says so with <c>threshold_lineage = 0</c>). The detector behind the CONTEXT fact
    /// <c>PG_WAL_VOLUME_SHIFT</c>: that fact states the volume, this one grades it, and there is no absolute bar
    /// anywhere because WAL volume is server-relative. Folds onto <c>PG_CHECKPOINT_PRESSURE</c>'s incident
    /// through <c>PgTargetFactKeys.AnomalyToFamilies</c> and lifts it through the write scorer's trigger
    /// amplifier when both fire — the leading edge of the pressure, stated as the pressure's evidence.
    ///
    /// <para><b>Trackedness first, bucket second — the reverse of the five v1 detectors, on purpose.</b> On every
    /// PostgreSQL target the fleet monitors today WAL is not reported (Aurora types <c>wal_bytes</c> NULL;
    /// calibration §A7 measured fifty clusters at zero for fourteen days), and a detector that fetched a thirty-day
    /// bucket before discovering the window had nothing to judge would run the largest read in the family for
    /// nothing on every pass. So the four-hour window is read first; an untracked window (no non-NULL
    /// <c>wal_bytes</c>, or zero bytes AND zero records across every sample — the collector's rule, same read)
    /// returns without asking for the bucket and without a fact: the context fact already carries
    /// <c>unavailable</c> and its reason, and an anomaly about an unreported quantity would be a claim about
    /// nothing. Only a tracked window with at least one rated collection reaches the baseline.</para>
    /// </summary>
    private async partial Task DetectWalVolumeAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            double peak, avg, walBytes;
            long ratedSamples, walRecords;
            bool walTracked;

            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = new NpgsqlCommand(WalVolumeWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                if (!await reader.ReadAsync(context.CancellationToken)) return;

                peak = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
                avg = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
                ratedSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
                walBytes = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                walRecords = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
                walTracked = !reader.IsDBNull(6) && reader.GetBoolean(6);
            }

            if (!walTracked || (walBytes <= 0 && walRecords <= 0) || ratedSamples == 0) return;

            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgWalBytesPerSec, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peak, avg,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec), PgWalBytesFloorPerSec, PgWalBytesFallbackPerSec, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = ZScoreMetadata(baseline, decision, ratedSamples);
            metadata["peak_wal_bytes_per_sec"] = peak;
            metadata["avg_wal_bytes_per_sec"] = avg;
            /* The ratio the advice states ("9.2 MiB/s against a 1.1 MiB/s routine for this hour"): peak over the
               bucket's robust centre — the median when the bucket has one, the mean otherwise — the same centre
               the deviation prose names, so the sentence's two numbers agree. A burst-inflated mean is exactly what
               the robust path exists to ignore; stamped only when the centre is a number to divide by. */
            var centre = baseline.Median > 0 ? baseline.Median : baseline.Mean;
            if (centre > 0)
                metadata["baseline_ratio"] = peak / centre;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyWalVolume,
                Value = peak,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] WAL-volume anomaly detection failed: {Message}", ex.Message);
        }
    }
}
