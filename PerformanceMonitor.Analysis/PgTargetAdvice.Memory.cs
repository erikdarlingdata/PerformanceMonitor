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
/// Advice for the memory-composition family (design §4b). Value-stated from the facts — each knob's value as read
/// (<c>shared_buffers</c>, <c>max_connections</c>, <c>work_mem</c>, <c>maintenance_work_mem</c>,
/// <c>autovacuum_max_workers</c>), the sum, and the host's <c>memory_total_bytes</c> it is measured against; the
/// free-plus-cached share when the pressure fact fired — with the counter-objective named on every recommendation
/// (a smaller <c>work_mem</c> spills sorts to disk; fewer connections queue the application). The advice must say
/// that <c>work_mem</c> is per sort/hash node, not per connection, so the product is a CEILING the workload may
/// never approach — which is why the card is advisory at 0.4 until a workload co-fire lifts it (D5). Never touches
/// <c>fsync</c> / <c>synchronous_commit</c> / <c>full_page_writes</c> (posture is the posture family's alone).
/// Filled by lane 32 of #3691.
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 32 */
    private static partial AdviceBlock? ComposeMemory(string key, IReadOnlyDictionary<string, Fact> factsByKey) => null;
}
