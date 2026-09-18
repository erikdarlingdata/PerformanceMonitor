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
/// <c>pg_waits</c> — the wait profile (lane 5): type rollups (<c>Lock</c>, <c>LWLock</c>, <c>IO</c>, <c>IPC</c>)
/// and named standouts, keyed through <see cref="PgTargetFactKeys.WaitKey"/>. Thresholds are fractions of the
/// wait source's OWN observed time (<c>wait_source_observed_ms</c>), the PostgreSQL twin of
/// <c>FactScorer.GetWaitThresholds</c>. A sampled fact (<c>is_sampled = 1</c>) with fewer than three samples
/// scores 0 — below that the estimate has no resolution.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 5 */
    private static partial double ScoreWaitFact(Fact fact) => 0.0;

    /* filled by lane 5 */
    private static partial List<AmplifierDefinition> WaitAmplifiers(string key) => [];
}
