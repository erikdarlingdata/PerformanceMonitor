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
/// <c>pg_replication</c> — replication lag and slot retention (filled by lane 12 of #3691, design §3.10):
/// <c>PG_REPLICATION_LAG</c> from <c>pg_replication_stats</c>, <c>PG_SLOT_RETENTION</c> / <c>PG_SLOT_XMIN</c> from
/// <c>pg_replication_slot_stats</c>. Engine-defined bars where PostgreSQL draws its own line
/// (<c>max_slot_wal_keep_size</c>, <c>autovacuum_freeze_max_age</c> multiples shared with the Tier-0 alerts);
/// unmeasured otherwise, with <c>threshold_lineage = 0</c>. The 5-minute replication cadence means every fact here
/// states its own sample count and never borrows the one-minute coverage fraction.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 12 */
    private static partial double ScoreReplicationFact(Fact fact) => 0.0;

    /* filled by lane 12 */
    private static partial List<AmplifierDefinition> ReplicationAmplifiers(string key) => [];
}
