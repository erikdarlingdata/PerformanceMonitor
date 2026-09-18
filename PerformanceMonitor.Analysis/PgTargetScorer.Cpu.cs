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
/// <c>pg_cpu</c> — instance CPU (lane 9; Aurora / Performance Insights only). <c>PG_CPU_PERCENT</c> is the
/// measured confirmer the PostgreSQL load-family anomaly arm reads; on stock there is no CPU series and the arm
/// is absent — the advice must not imply CPU was checked (D6).
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 9 */
    private static partial double ScoreCpuFact(Fact fact) => 0.0;
}
