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
/// Advice for data-file I/O latency (design §3.9). Value-stated from the <c>pg_io_stats</c> fact — the measured ms per read / write, the PostgreSQL major (the family is 16+ only), and <c>track_io_timing</c>'s state when the timings are absent, never folklore about storage; every recommendation names its counter-objective (filled by lane 11 of #3691).
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 11 */
    private static partial AdviceBlock? ComposeIo(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
