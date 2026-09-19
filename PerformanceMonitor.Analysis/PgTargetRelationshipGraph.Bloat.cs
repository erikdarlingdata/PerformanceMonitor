/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The bloat chain (filled by lane 13 of #3691, design §3.12): <c>PG_BLOAT_TREND</c> → <c>PG_AUTOVACUUM_BACKLOG</c> /
/// <c>PG_XMIN_HOLD</c> (dead tuples vacuum could not reclaim are where the growth comes from), and the
/// buffer-cache leaf (a bloated heap is more pages for the same rows). Predicates read the destination fact's
/// verdict, never a bar of their own.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 13 */
    private partial void BuildBloatEdges()
    {
    }
}
