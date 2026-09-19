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
    /// <c>pg_io_read_latency</c>: mean ms per data-file read per collection, from the reset-aware <c>pg_io_stats</c> counter differences (<c>read_time</c> / <c>reads</c> deltas), the <c>DatabaseCounterDeltasCte</c> shape in the root file.
    /// <para>/* filled by lane 11 — answers null until then, which the shared reader treats as "no arm for this
    /// metric". The filled arm is a CTE chain ending in <c>clean(collection_time, v)</c> followed by the ONE
    /// <c>PgBaselineProvider.RobustTierScaffold</c>, window bounds <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c>
    /// on the value (the io-arm rule), <c>server_id = $1</c>, no bare <c>now()</c>; the baseline census in
    /// <c>PgTargetAnomalyTests</c> gains the metric's (name, table) row in the same PR. */</para>
    /// </summary>
    private static partial string? IoReadLatencyBaselineQuery() => null;
}
