/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// <c>pg_cpu_burn_cores</c>: cores busy per collection from <c>pg_kernel_stats</c> — the reset-aware
    /// <c>exec_user_time_ms + exec_system_time_ms</c> differences (<c>stats_since</c> the reset witness) summed across
    /// statements, over the collection's wall ms. A collection with rows and zero delta is a ZERO sample, not a missing
    /// one; a collection with no rows (extension absent, collector off) is missing and the arm must say so.
    /// <para>/* filled by lane 28 — answers null until then, which the shared reader treats as "no arm for this
    /// metric". The filled arm is a CTE chain ending in <c>clean(collection_time, v)</c> followed by the ONE
    /// <c>PgBaselineProvider.RobustTierScaffold</c> — the #3653 Q6 contract: the scaffold binds the hour-of-week key
    /// through the ROOT's clock parameters (<c>$4..$6</c> after <c>$3</c>), never its own <c>EXTRACT</c>
    /// (LocalClockBucketKeyTests reads every arm) — window bounds <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c>
    /// on the value, <c>server_id = $1</c>, no bare <c>now()</c>; the baseline census in <c>PgTargetAnomalyTests</c>
    /// gains the metric's (name, table) row in the same PR, and the table joins
    /// <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> by COLLECTOR name the day this arm reads it
    /// (BaselineSupplyTests derives that set from the arms' text and fails without the floor). */</para>
    /// </summary>
    private static partial string? CpuBurnCoresBaselineQuery() => null;
}
