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
/// <c>pg_write</c> — checkpoint / WAL pressure (lane 2). <c>PG_CHECKPOINT_PRESSURE</c> grades the
/// requested-vs-timed checkpoint ratio over the window; the bars are rates, never absolute totals (A7).
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 2 */
    private static partial double ScoreWriteFact(Fact fact) => 0.0;

    /* filled by lane 2 (PG_CHECKPOINT_PRESSURE ↔ CONFIG_PG_MAX_WAL_SIZE co-fire); lane 5 adds the IO:WALSync / LWLock:WALWrite confirmers */
    private static partial List<AmplifierDefinition> WriteAmplifiers(string key) => [];
}
