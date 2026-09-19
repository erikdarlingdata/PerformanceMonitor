/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The I/O chain (filled by lane 11 of #3691, design §3.9): <c>PG_IO_READ_LATENCY_MS</c> ↔ <c>PG_BUFFER_CACHE_PRESSURE</c>
/// (a cold cache reads from disk) and the <c>IO:</c>-type wait standouts; write latency ↔ <c>PG_CHECKPOINT_PRESSURE</c>.
/// Predicates read the destination fact's verdict (<c>BaseSeverity &gt; 0</c>), never a bar of their own.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 11 */
    private partial void BuildIoEdges()
    {
    }
}
