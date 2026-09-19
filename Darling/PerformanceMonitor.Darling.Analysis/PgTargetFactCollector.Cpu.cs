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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The window's instance CPU from <c>pg_cpu_utilization</c> — Aurora / Performance Insights only — in one read:
    /// the peak and average of BOTH percentages the collector stores, how many rows carried a capacity sample, the
    /// capacity figures at the peak, and the newest sample's time. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window
    /// (naive UTC). No <c>GROUP BY</c>, so the header is one row even over an empty window and <c>sample_count = 0</c>
    /// is the "this flavour does not write the table" signal the D6 disclosure reads.
    ///
    /// <para><b>Two percentages, one of which may be banded (#3281).</b> <c>cpu_percent</c> is Performance Insights'
    /// <c>os.cpuUtilization.total.avg</c>: percent of the capacity CURRENTLY ALLOCATED. On the <c>db.serverless</c>
    /// class the whole measured fleet runs (153 of 153), that allocation is re-sized continuously, so a one-vCPU
    /// minute reads exactly 100 while the instance sits at a third of its configured ceiling — measured at one such
    /// minute: 4 of 12 ACUs. <c>acu_utilization_percent</c> is percent of the CONFIGURED ceiling, where 100 means the
    /// ceiling really is reached. The fleet card, <c>get_fleet_overview</c> and the PostgreSQL High CPU alert all
    /// band on the second and only report the first; this family does the same, and the raw reading rides as
    /// "was a core pinned", never as a grade.</para>
    /// </summary>
    public const string PgTargetCpuWindowSql = @"
WITH peak AS (
    SELECT collection_time, acu_utilization_percent, serverless_capacity_acu, max_configured_acu
    FROM pg_cpu_utilization
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   acu_utilization_percent IS NOT NULL
    ORDER BY acu_utilization_percent DESC, collection_time DESC
    LIMIT 1
)
SELECT
    MAX(w.cpu_percent)                          AS peak_cpu_percent,
    AVG(w.cpu_percent)                          AS avg_cpu_percent,
    MAX(w.acu_utilization_percent)              AS peak_capacity_pct,
    AVG(w.acu_utilization_percent)              AS avg_capacity_pct,
    CAST(COUNT(*) AS integer)                   AS sample_count,
    CAST(COUNT(w.acu_utilization_percent) AS integer) AS capacity_samples,
    MAX(w.collection_time)                      AS latest_at,
    (SELECT collection_time FROM peak)          AS peak_at,
    (SELECT serverless_capacity_acu FROM peak)  AS peak_capacity_acu,
    (SELECT max_configured_acu FROM peak)       AS peak_max_configured_acu
FROM pg_cpu_utilization AS w
WHERE w.server_id = $1
AND   w.collection_time >= $2
AND   w.collection_time <= $3";

    /// <summary>
    /// <c>PG_CPU_PERCENT</c> (filled by lane 9 — #3542 step 9; design §2a): ONE fact, emitted only when the window has rows,
    /// so on stock PostgreSQL the family is structurally absent — core PostgreSQL exposes no instance-CPU counter in
    /// any version and the only source is the AWS API, which <c>PgCpuUtilizationCollector.AppliesTo</c> gates to
    /// Aurora. The D6 coverage disclosure says "CPU: not collected on this flavour"; this method says nothing.
    ///
    /// <para><b>The value is the bandable percentage</b> — <see cref="FleetCpuProvenance.CpuBandInputPercent"/>'s
    /// pick for a Performance Insights reading, which is the capacity percent — when the window carried one, and the
    /// raw <c>cpu_percent</c> otherwise with <c>capacity_measured = 0</c> beside it so the scorer refuses to grade it
    /// (<c>PgTargetScorer.Cpu.cs</c>: "Unknown, never Healthy", the card's own #3271 rule) and the advice says which
    /// figure it is looking at. Every input rides in the metadata: both peaks, both averages, the sample counts,
    /// the ACU figures at the peak and the newest sample's age against the window's end. The peak is the window's
    /// maximum five-minute average — the collector's grain — never an instantaneous reading.</para>
    /// </summary>
    private partial async Task CollectCpuFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetCpuWindowSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var sampleCount = reader.IsDBNull(4) ? 0 : reader.GetInt32(4);
            if (sampleCount == 0) return;

            double? peakCpu = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0));
            double? avgCpu = reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1));
            double? peakCapacity = reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2));
            double? avgCapacity = reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3));
            var capacitySamples = reader.IsDBNull(5) ? 0 : reader.GetInt32(5);
            DateTime? latestAt = reader.IsDBNull(6) ? null : reader.GetDateTime(6);
            DateTime? peakAt = reader.IsDBNull(7) ? null : reader.GetDateTime(7);
            double? peakCapacityAcu = reader.IsDBNull(8) ? null : Convert.ToDouble(reader.GetValue(8));
            double? peakMaxAcu = reader.IsDBNull(9) ? null : Convert.ToDouble(reader.GetValue(9));

            /* Rows with neither percentage (an ingest that wrote a row and no reading) are not a measurement. */
            if (peakCpu is null && peakCapacity is null) return;

            var banded = FleetCpuProvenance.CpuBandInputPercent(peakCpu, peakCapacity, FleetCpuSource.PerformanceInsights);
            var fact = new Fact
            {
                Source = PgTargetSources.CpuSource,
                Key = PgTargetFactKeys.CpuPercent,
                Value = banded ?? peakCpu ?? 0,
                ServerId = context.ServerId,
                Metadata =
                {
                    [PgTargetScorer.CpuCapacityMeasuredKey] = banded.HasValue ? 1 : 0,
                    [PgTargetScorer.CpuSampleCountKey] = sampleCount,
                    [PgTargetScorer.CpuCapacitySamplesKey] = capacitySamples,
                    [PgTargetScorer.CpuObservedMsKey] = context.ObservedDurationMs,
                },
            };
            if (peakCpu is { } pc) fact.Metadata[PgTargetScorer.CpuPeakPercentKey] = pc;
            if (avgCpu is { } ac) fact.Metadata[PgTargetScorer.CpuAvgPercentKey] = ac;
            if (peakCapacity is { } pk) fact.Metadata[PgTargetScorer.CpuPeakCapacityPctKey] = pk;
            if (avgCapacity is { } ak) fact.Metadata[PgTargetScorer.CpuAvgCapacityPctKey] = ak;
            if (peakCapacityAcu is { } acu) fact.Metadata[PgTargetScorer.CpuPeakCapacityAcuKey] = acu;
            if (peakMaxAcu is { } max) fact.Metadata[PgTargetScorer.CpuMaxConfiguredAcuKey] = max;
            var windowEnd = AsNaive(context.TimeRangeEnd);
            if (peakAt is { } at) fact.Metadata[PgTargetScorer.CpuPeakAgeSecondsKey] = Math.Max(0, (windowEnd - AsNaive(at)).TotalSeconds);
            if (latestAt is { } last) fact.Metadata[PgTargetScorer.CpuLatestAgeSecondsKey] = Math.Max(0, (windowEnd - AsNaive(last)).TotalSeconds);

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_cpu_utilization arrived at V106 and its capacity columns at V115; a pre-migration store raises
               42P01 / 42703 here, which the reporter classifies quiet. Degrades to "no fact" — the D6 disclosure
               then says the family was not collected, and WHY is reported, not assumed (#2826). An abandonment is
               NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
