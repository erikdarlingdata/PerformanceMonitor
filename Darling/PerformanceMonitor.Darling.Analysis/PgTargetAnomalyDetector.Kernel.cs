/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetAnomalyDetector
{
    /// <summary>
    /// <c>ANOMALY_PG_CPU_BURN</c>: the window's cores busy (user-plus-system CPU seconds per wall second from
    /// <c>pg_kernel_stats</c>) against the <c>pg_cpu_burn_cores</c> bucket — the z-score shape, graded by the shared
    /// deviation ramp (registered in <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c> by the v3 plumbing). Folds
    /// onto <c>PG_CPU_BURN_CORES</c>. Sits out where <c>pg_stat_kcache</c> is not available (read from
    /// <c>pg_extension_availability</c>, never inferred from an empty table).
    /// <para>/* filled by lane 28 — returns immediately until then. The filled body copies the five v1 detectors
    /// in the root file: its own <c>try</c> / <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> fence, the bucket through <see cref="Baselines"/>, the window read a
    /// <c>public const string …Sql</c> in THIS file over the collector table (the detector census reflects them),
    /// <c>CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c>, the token on every call, the
    /// #3653 PAIR gate (peak AND window mean — the blocking detector is the precedent), and <c>AnomalyGate</c>'s
    /// metadata (<c>deviation_sigma</c>, <c>fire_threshold</c>, <c>baseline_low_quality</c>, <c>fallback_exceedance</c>)
    /// plus <c>threshold_lineage = 0</c> on the fact unless the bar is measured. Any new bar goes in the
    /// <c>AnomalyThresholds</c> PostgreSQL block WITH its lineage marker. */</para>
    /// </summary>
    private partial Task DetectCpuBurnAnomalies(AnalysisContext context, List<Fact> anomalies) => Task.CompletedTask;
}
