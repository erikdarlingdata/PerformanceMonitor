/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_kernel</c> — the kernel-time family (filled by lane 28 of #3691): <c>PG_CPU_BURN_CORES</c>, user-plus-system
/// CPU seconds per WALL second from the reset-aware <c>pg_kernel_stats</c> (<c>pg_stat_kcache</c>) differences summed
/// across statements — cores busy, the self-hosted CPU PROXY that stands where the Aurora capacity percent does not
/// exist — and <c>PG_CPU_DECOMPOSITION</c>, kernel CPU time against measured wait time over the window (burning
/// versus waiting). The extension's availability is read from <c>pg_extension_availability</c>, never inferred from
/// rows. Bars carry their lineage marker within six lines; a cores-busy bar is stated against the host's core count
/// when a lane can read one and as unmeasured (<c>threshold_lineage = 0</c>) when it cannot; gates are rates over
/// <c>ObservedDurationMs</c>, never absolute totals.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 28 */
    private static partial double ScoreKernelFact(Fact fact) => 0.0;

    /* filled by lane 28 */
    private static partial List<AmplifierDefinition> KernelAmplifiers(string key) => [];
}
