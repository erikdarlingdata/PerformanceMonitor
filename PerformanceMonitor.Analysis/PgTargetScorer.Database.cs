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
/// <c>pg_database</c> — per-database counter facts from the one <c>pg_database_stats</c> read (lane 2):
/// <c>PG_TPS</c>, <c>PG_HIT_RATIO</c>, <c>PG_DEADLOCK_RATE</c>. TPS and hit ratio are context (0); the deadlock
/// rate grades events per observed hour, with the rate/exemplar split stated in its advice.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 2 */
    private static partial double ScoreDatabaseFact(Fact fact) => 0.0;
}
