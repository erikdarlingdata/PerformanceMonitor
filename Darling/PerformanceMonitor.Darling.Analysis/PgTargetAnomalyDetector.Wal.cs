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
    /// #3653 A8 option B (lane L3c): <see cref="WalVolumeWindowSql"/>'s tile twin — keeps the <c>sampled</c>/<c>rated</c>
    /// CTEs (<c>rated</c> already exposes <c>collection_time</c> under that name, so <see cref="WindowTiles.LocalHourSql"/>
    /// reads it unchanged) and groups the OUTER select by target-local hour. Column order per tile mirrors the
    /// whole-window read (0 local_hour, 1 peak, 2 avg, 3 rated_samples, 4 wal_bytes, 5 wal_records, 6 wal_reset_count,
    /// 7 wal_tracked, 8 sample_count), with a per-tile peak-time column appended at the end for
    /// <see cref="WindowTiles.ReadTile"/>. Binds <c>$4..$6</c> from the analysis window's clock.
    /// </summary>
    public const string WalVolumeTileWindowSql = @"
WITH sampled AS (
    SELECT
        collection_time,
        wal_bytes   - LAG(wal_bytes)   OVER series AS raw_wal_bytes,
        wal_records - LAG(wal_records) OVER series AS raw_wal_records,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec,
        (ROW_NUMBER() OVER series > 1
         AND wal_stats_reset IS DISTINCT FROM LAG(wal_stats_reset) OVER series) AS wal_reset_here,
        (wal_bytes IS NOT NULL) AS wal_tracked
    FROM pg_write_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (ORDER BY collection_time)
),
rated AS (
    SELECT
        collection_time,
        raw_wal_bytes,
        raw_wal_records,
        wal_reset_here,
        wal_tracked,
        CASE WHEN raw_wal_bytes IS NOT NULL AND interval_sec > 0
             THEN GREATEST(raw_wal_bytes, 0)::DOUBLE PRECISION / interval_sec END AS bytes_per_sec
    FROM sampled
)
SELECT " + WindowTiles.LocalHourSql + @" AS local_hour,
       MAX(bytes_per_sec) AS peak_wal_bytes_per_sec,
       AVG(bytes_per_sec) AS avg_wal_bytes_per_sec,
       CAST(count(*) FILTER (WHERE bytes_per_sec IS NOT NULL) AS integer) AS rated_samples,
       coalesce(SUM(GREATEST(raw_wal_bytes, 0)), 0) AS wal_bytes,
       CAST(coalesce(SUM(GREATEST(raw_wal_records, 0)), 0) AS bigint) AS wal_records,
       CAST(count(*) FILTER (WHERE wal_reset_here) AS integer) AS wal_reset_count,
       coalesce(bool_or(wal_tracked), false) AS wal_tracked,
       (array_agg(collection_time ORDER BY bytes_per_sec DESC NULLS LAST))[1] AS peak_time
FROM rated
GROUP BY " + WindowTiles.LocalHourSql + @"
ORDER BY " + WindowTiles.LocalHourSql;

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
            /* #3653 A8 option B: per-hour tiles against the tile's own (hour, dow) bucket, never-blind fallback to
               today's window-peak path when no tile scores. The trackedness/zero-volume early return (design's
               own reason: an untracked window has nothing to judge before a bucket is even fetched) still reads
               the WHOLE window first, summed across tiles, because that decision is about the window's data
               shape, not about any one hour. */
            var map = await _baselineProvider.GetBucketMapAsync(
                context.ServerId, MetricNames.PgWalBytesPerSec, context.TimeRangeStart, context.TimeRangeEnd, context.CancellationToken);

            var tiles = new List<WindowTile>();
            double totalWalBytes = 0;
            long totalWalRecords = 0;
            var anyTracked = false;

            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = new NpgsqlCommand(WalVolumeTileWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
                /* #3653 A8 option B: the tiled read's GROUP BY key (WindowTiles.LocalHourSql) binds $4..$6 from the
                   ANALYSIS window's clock — bound here, not in PgTargetAnomalyDetector.cs, per this lane's brief. */
                cmd.Parameters.AddWithValue(AsNaive(map.WindowClock.TransitionAtUtc));
                cmd.Parameters.AddWithValue(map.WindowClock.OffsetBeforeMinutes);
                cmd.Parameters.AddWithValue(map.WindowClock.OffsetAfterMinutes);

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    tiles.Add(WindowTiles.ReadTile(reader, localHourOrdinal: 0, peakOrdinal: 1, meanOrdinal: 2, samplesOrdinal: 3, peakTimeOrdinal: 8));
                    totalWalBytes += reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
                    totalWalRecords += reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));
                    anyTracked |= !reader.IsDBNull(7) && reader.GetBoolean(7);
                }
            }

            if (!anyTracked || (totalWalBytes <= 0 && totalWalRecords <= 0)) return;

            var whole = WindowTiles.WholeWindow(tiles);
            if (whole.Samples == 0) return;

            var window = context.TimeRangeEnd - context.TimeRangeStart;
            var tv = AnomalyGate.EvaluateTiles(
                tiles, map,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec), PgWalBytesFloorPerSec, PgWalBytesFallbackPerSec, SigmaDisplayCap,
                window);

            AnomalyGate.ZDecision decision;
            BaselineBucket baseline;
            double peak, avg;
            long ratedSamples;

            if (tv is null)
            {
                /* Never-blind fallback: today's whole-window path against the start bucket. */
                baseline = await _baselineProvider.GetBaselineAsync(
                    context.ServerId, MetricNames.PgWalBytesPerSec, context.TimeRangeStart, context.CancellationToken);
                if (baseline.SampleCount == 0) return;

                decision = AnomalyGate.EvaluateZScore(
                    baseline, whole.Peak, whole.Mean,
                    DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec), PgWalBytesFloorPerSec, PgWalBytesFallbackPerSec, SigmaDisplayCap,
                    window: window);
                if (!decision.Fire) return;

                peak = whole.Peak;
                avg = whole.Mean;
                ratedSamples = whole.Samples;
            }
            else
            {
                decision = tv.Value.Decision;
                baseline = tv.Value.Bucket;
                peak = tv.Value.Tile.Peak;
                avg = tv.Value.Tile.Mean;
                ratedSamples = tv.Value.Tile.Samples;
                if (!decision.Fire) return;
            }

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
            if (tv is { } verdict)
                WindowTiles.AddTileMetadata(metadata, verdict, tiles, map.WindowClock);

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
