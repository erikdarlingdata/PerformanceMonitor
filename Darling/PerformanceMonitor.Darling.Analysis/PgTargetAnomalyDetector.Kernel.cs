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
    /// #3653 A8 option B (lane L3c): <see cref="CpuBurnWindowSql"/>'s tile twin — keeps every upstream CTE
    /// (<c>ranked</c>/<c>series</c>/<c>deltas</c>/<c>rated</c>, which already expose <c>collection_time</c> under
    /// that name) and groups the <c>per_collection</c> aggregation, and the outer select, by target-local hour
    /// instead of collapsing the whole window. Column order per tile: 0 local_hour, 1 peak_cores_busy,
    /// 2 mean_cores_busy, 3 rated_samples, 4 peak_time, 5 top_query_id — the top query rides per tile, the worst
    /// tile's is what the detector reports (design's "extra window scalars" rule). Binds <c>$4..$6</c> from the
    /// analysis window's clock.
    /// </summary>
    public const string CpuBurnTileWindowSql = @"
WITH ranked AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        coalesce(exec_user_time_ms, 0)   AS user_ms_total,
        coalesce(exec_system_time_ms, 0) AS system_ms_total,
        coalesce(plan_cpu_time_ms, 0)    AS plan_ms_total,
        stats_since,
        DENSE_RANK() OVER (ORDER BY collection_time) AS k
    FROM pg_kernel_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
),
series AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        user_ms_total,
        system_ms_total,
        plan_ms_total,
        stats_since,
        k,
        LAG(k)               OVER identity AS prev_k,
        LAG(collection_time) OVER identity AS prev_time,
        LAG(user_ms_total)   OVER identity AS prev_user,
        LAG(system_ms_total) OVER identity AS prev_system,
        LAG(plan_ms_total)   OVER identity AS prev_plan,
        LAG(stats_since)     OVER identity AS prev_since
    FROM ranked
    WINDOW identity AS (PARTITION BY database_name, query_id ORDER BY collection_time)
),
deltas AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', prev_time))) AS interval_sec,
        (stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user) AS reset_here,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN user_ms_total   ELSE user_ms_total   - prev_user   END AS user_ms,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN system_ms_total ELSE system_ms_total - prev_system END AS system_ms,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN plan_ms_total   ELSE GREATEST(plan_ms_total - prev_plan, 0) END AS plan_ms
    FROM series
    WHERE prev_k = k - 1
),
rated AS (
    SELECT *
    FROM deltas
    WHERE interval_sec > 0
),
per_collection AS (
    SELECT
        collection_time,
        interval_sec,
        SUM(user_ms)   AS user_ms,
        SUM(system_ms) AS system_ms,
        SUM(plan_ms)   AS plan_ms,
        (SUM(user_ms) + SUM(system_ms) + SUM(plan_ms)) / (interval_sec * 1000.0) AS cores_busy
    FROM rated
    GROUP BY collection_time, interval_sec
),
by_query AS (
    SELECT " + WindowTiles.LocalHourSql + @" AS local_hour, database_name, query_id, SUM(user_ms + system_ms + plan_ms) AS cpu_ms,
           ROW_NUMBER() OVER (PARTITION BY " + WindowTiles.LocalHourSql + @" ORDER BY SUM(user_ms + system_ms + plan_ms) DESC, query_id) AS rk
    FROM rated
    GROUP BY " + WindowTiles.LocalHourSql + @", database_name, query_id
)
SELECT " + WindowTiles.LocalHourSql + @" AS local_hour,
       MAX(cores_busy) AS peak_cores_busy,
       AVG(cores_busy) AS mean_cores_busy,
       CAST(count(*) AS integer) AS rated_samples,
       (array_agg(collection_time ORDER BY cores_busy DESC))[1] AS peak_time,
       (SELECT query_id FROM by_query AS b WHERE b.local_hour = " + WindowTiles.LocalHourSql + @" AND b.rk = 1) AS top_query_id
FROM per_collection
GROUP BY " + WindowTiles.LocalHourSql + @"
ORDER BY " + WindowTiles.LocalHourSql;

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
            /* #3653 A8 option B: per-hour tiles against the tile's own (hour, dow) bucket, never-blind fallback to
               today's window-peak path when no tile scores. */
            var map = await _baselineProvider.GetBucketMapAsync(
                context.ServerId, MetricNames.PgCpuBurnCores, context.TimeRangeStart, context.TimeRangeEnd, context.CancellationToken);

            var tiles = new List<WindowTile>();
            /* top_query_id has no home in WindowTile (a family-specific extra scalar, design's "worst tile" rule),
               so it rides beside the tile list keyed by the tile's own LocalHour. */
            var topQueryIdByTile = new Dictionary<DateTime, double>();

            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = WindowCommand(CpuBurnTileWindowSql, connection, context);
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    var tile = WindowTiles.ReadTile(reader, localHourOrdinal: 0, peakOrdinal: 1, meanOrdinal: 2, samplesOrdinal: 3, peakTimeOrdinal: 4);
                    tiles.Add(tile);
                    if (!reader.IsDBNull(5))
                        topQueryIdByTile[tile.LocalHour] = Convert.ToDouble(reader.GetValue(5));
                }
            }

            var whole = WindowTiles.WholeWindow(tiles);
            if (whole.Samples == 0 || whole.Peak <= 0) return;

            var window = context.TimeRangeEnd - context.TimeRangeStart;
            var tv = AnomalyGate.EvaluateTiles(
                tiles, map,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgCpuBurnCores), PgCpuBurnCoresFloor, PgCpuBurnCoresFallback, SigmaDisplayCap,
                window);

            AnomalyGate.ZDecision decision;
            BaselineBucket baseline;
            double peakCores, meanCores;
            long ratedSamples;
            double? topQueryId;

            if (tv is null)
            {
                /* Never-blind fallback: today's whole-window path against the start bucket. */
                baseline = await _baselineProvider.GetBaselineAsync(
                    context.ServerId, MetricNames.PgCpuBurnCores, context.TimeRangeStart, context.CancellationToken);
                if (baseline.SampleCount == 0) return;

                decision = AnomalyGate.EvaluateZScore(
                    baseline, whole.Peak, whole.Mean,
                    DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgCpuBurnCores), PgCpuBurnCoresFloor, PgCpuBurnCoresFallback, SigmaDisplayCap,
                    window: window);
                if (!decision.Fire) return;

                peakCores = whole.Peak;
                meanCores = whole.Mean;
                ratedSamples = whole.Samples;
                /* The whole-window fallback's top query: the WORST tile's own value — there is no whole-window
                   aggregate for a scalar WindowTiles.WholeWindow does not carry. */
                topQueryId = topQueryIdByTile.Count > 0 ? topQueryIdByTile.Values.Max() : null;
            }
            else
            {
                decision = tv.Value.Decision;
                baseline = tv.Value.Bucket;
                peakCores = tv.Value.Tile.Peak;
                meanCores = tv.Value.Tile.Mean;
                ratedSamples = tv.Value.Tile.Samples;
                topQueryId = topQueryIdByTile.TryGetValue(tv.Value.Tile.LocalHour, out var top) ? top : null;
                if (!decision.Fire) return;
            }

            /* unmeasured: both bars (see AnomalyThresholds) — the helper's default stamp, threshold_lineage = 0. */
            var metadata = ZScoreMetadata(baseline, decision, ratedSamples);
            metadata["peak_cores_busy"] = peakCores;
            metadata["mean_cores_busy"] = meanCores;
            metadata["mean_sigma"] = decision.MeanSigma ?? 0.0;
            if (topQueryId is { } topId)
                metadata[PgTargetScorer.KernelTopQueryIdKey] = topId;
            /* The ratio the advice states ("4.2 cores busy against a routine of 1.1 for this hour"): peak over the
               bucket's robust centre — the median when the bucket has one, the mean otherwise — the same centre the
               deviation prose names, so the sentence's two numbers agree. Stamped only when the centre is a number
               to divide by. */
            var centre = baseline.Median > 0 ? baseline.Median : baseline.Mean;
            if (centre > 0)
                metadata["baseline_ratio"] = peakCores / centre;
            if (tv is { } verdict)
                WindowTiles.AddTileMetadata(metadata, verdict, tiles, map.WindowClock);

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
