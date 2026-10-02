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
    /// <c>pg_replay_lag_bytes</c>: the worst standby's replay gap in bytes per collection, from <c>pg_replication_stats</c>
    /// — a point series (no differencing), the <c>PgSessionCount</c> arm's shape. <c>MAX(replay_bytes_behind)</c> per
    /// <c>collection_time</c> is a PICK across the standbys sampled in that collection (the PG_REPLICATION_LAG read's
    /// own worst-standby rule), so the bucket describes "how far behind was the furthest standby" and the detector's
    /// window read (<c>PgTargetAnomalyDetector.ReplayLagWindowSql</c>) takes the same pick — like for like. NULL gaps
    /// (a standby in <c>startup</c>) are not samples. A server with no standby has no rows and an empty bucket, and
    /// the detector sits out. <c>::DOUBLE PRECISION</c> on the value (the io-arm rule), window bounds
    /// <c>&gt;= $2 AND &lt; $3</c>, no bare <c>now()</c>.
    /// <para>/* filled by lane 12 of #3691 — a CTE ending in <c>clean(collection_time, v)</c> followed by the ONE
    /// <c>PgBaselineProvider.RobustTierScaffold</c>; the baseline census in <c>PgTargetAnomalyTests</c> carries the
    /// metric's (name, table) row, and <c>pg_replication_stats</c> joins <c>BaselineServingRawCollectors</c> so a
    /// shortened schedule cannot starve the 30-day window silently. */</para>
    /// </summary>
    private static partial string? ReplayLagBaselineQuery() => @"
WITH clean AS (
    SELECT collection_time, MAX(replay_bytes_behind)::DOUBLE PRECISION AS v
    FROM pg_replication_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   replay_bytes_behind IS NOT NULL
    GROUP BY collection_time
)," + RobustTierScaffold;
}
