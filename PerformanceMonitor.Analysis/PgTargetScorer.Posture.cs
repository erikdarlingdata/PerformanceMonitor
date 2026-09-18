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
/// <c>pg_posture</c> — durability posture (lane 8). <c>fsync</c> / <c>full_page_writes</c> off score the
/// CRITICAL band (1.5 — <c>FactScorer</c> reads ≥ 1.5 as CRITICAL), <c>synchronous_commit</c> off the 0.4
/// advisory band. There is deliberately NO amplifier partial for this source (D6): a posture fact is a
/// durability statement, never a performance argument, and <c>PgTargetPostureIsolationTests</c> pins that no
/// amplifier or edge anywhere references a posture key.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 8 */
    private static partial double ScorePostureFact(Fact fact) => 0.0;
}
