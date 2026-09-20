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
/// <c>pg_memory</c> — the memory-composition family (filled by lane 32 of #3691, design §4b):
/// <c>CONFIG_PG_MEMORY_OVERCOMMIT</c>, the arithmetic <c>shared_buffers + max_connections × work_mem</c> (plus
/// <c>maintenance_work_mem × autovacuum_max_workers</c>) from <c>pg_server_config</c> against the host's
/// <c>memory_total_bytes</c> (on <c>pg_cpu_utilization</c>'s row since V136), and <c>PG_HOST_MEMORY_PRESSURE</c>, the host's
/// free-plus-cached share of total over the window (Aurora only, where host memory is reported). The composition
/// check is a CONVENTION reading: it roots an ADVISORY card at 0.4 and reaches ≥ 0.5 ONLY when a workload co-fire
/// (<c>PG_HOST_MEMORY_PRESSURE</c> or <c>PG_TEMP_SPILL</c>) amplifies it — D5, which the lane pins. The overcommit
/// ratio's bar is engine-arithmetic (the sum exceeds the box, or it does not); a pressure bar is measured or
/// unmeasured (<c>threshold_lineage = 0</c>) and says which. Every value the advice states is read from the fact.
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 32 */
    private static partial double ScoreMemoryFact(Fact fact) => 0.0;

    /* filled by lane 32 */
    private static partial List<AmplifierDefinition> MemoryAmplifiers(string key) => [];
}
