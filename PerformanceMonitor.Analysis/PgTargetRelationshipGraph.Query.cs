/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The query chain (lane 7, with lane 6): <c>PG_BAD_ACTOR_*</c> is the leaf wherever a statement is the answer;
/// <c>PG_TEMP_SPILL</c> → { <c>CONFIG_PG_WORK_MEM</c>, <c>PG_BAD_ACTOR_*</c> } is lane 6's edge.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 7 */
    private partial void BuildQueryEdges()
    {
    }
}
