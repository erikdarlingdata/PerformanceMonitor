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
/// <c>pg_buffer</c> — the buffer-cache pressure composite (lane 2): hit ratio + evictions (PG 16+, metadata
/// <c>evictions_tracked</c>) + bgwriter, with the hit-ratio arm suppressed on Aurora (<c>hit_ratio_suppressed</c>).
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 2 */
    private static partial double ScoreBufferFact(Fact fact) => 0.0;

    /* filled by lane 2 (composite ↔ CONFIG_PG_SHARED_BUFFERS); lane 5 adds the IO:DataFileRead confirmer */
    private static partial List<AmplifierDefinition> BufferAmplifiers(string key) => [];
}
