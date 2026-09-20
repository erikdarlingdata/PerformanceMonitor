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
/// Advice for the kernel-time family. Value-stated from the facts — cores busy over the window against the host's
/// core count when known, the user/system split, the statements that burned the most kernel time by <c>query_id</c>,
/// and the burning-versus-waiting decomposition that says whether the box is out of CPU or the backends are parked —
/// with the counter-objective named on every recommendation. The advice must say the reading is <c>pg_stat_kcache</c>'s
/// (per-statement, so background processes and non-tracked work are outside it) and never a host CPU percent.
/// Filled by lane 28 of #3691.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 28 */
    private static partial AdviceBlock? ComposeKernel(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;

    /* filled by lane 28 — the ANOMALY_PG_CPU_BURN arm of ComposeAnomaly delegates here (PgTargetAdvice.Anomaly.cs), so
       the anomaly's prose lives with its family and the shared prefix routing never reaches a SQL Server composer. */
    private static partial AdviceBlock? ComposeCpuBurnAnomaly(IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
