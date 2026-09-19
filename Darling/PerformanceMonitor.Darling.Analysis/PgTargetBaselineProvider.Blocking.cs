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
    /// <c>pg_blocked_sessions</c>: the number of blocked sessions per capture, from <c>pg_blocking_edges</c> (<c>COUNT(*)</c> of waiter rows per <c>collection_time</c>) — a point series (no differencing), the <c>PgSessionCount</c> arm's shape. A capture with no edge rows is a ZERO sample, not a missing one, and the arm must say so (the witness table's captures are the denominator).
    /// <para>/* filled by lane 17 — answers null until then, which the shared reader treats as "no arm for this
    /// metric". The filled arm is a CTE chain ending in <c>clean(collection_time, v)</c> followed by the ONE
    /// <c>PgBaselineProvider.RobustTierScaffold</c>, window bounds <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c>
    /// on the value (the io-arm rule), <c>server_id = $1</c>, no bare <c>now()</c>; the baseline census in
    /// <c>PgTargetAnomalyTests</c> gains the metric's (name, table) row in the same PR, and <c>pg_blocking_edges</c>
    /// joins <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> the day this arm reads it (BaselineSupplyTests
    /// derives that set from the arms' text and fails without the floor). */</para>
    /// </summary>
    private static partial string? BlockedSessionsBaselineQuery() => null;
}
