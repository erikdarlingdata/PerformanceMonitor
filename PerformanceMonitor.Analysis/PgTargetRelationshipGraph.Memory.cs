/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The memory / I-O chain (lane 2): <c>PG_BUFFER_CACHE_PRESSURE</c> → <c>CONFIG_PG_SHARED_BUFFERS</c>; lane 5
/// adds <c>IO:DataFileRead</c> waits → the composite. <c>PG_TEMP_SPILL</c> → <c>CONFIG_PG_WORK_MEM</c> is lane 6, in
/// the query chain file, because its leaf is a statement.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 2 */
    private partial void BuildMemoryEdges()
    {
    }
}
