/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The PostgreSQL-target relationship graph (#3542): the causal chains the inference engine walks from a
/// PostgreSQL root fact, declared against <see cref="PgTargetFactKeys"/> constants through the shared
/// <see cref="RelationshipGraph.AddEdge"/> machinery. Derives EMPTY — none of the SQL Server chains are
/// built underneath — because a PostgreSQL fact can never carry a SQL Server key (D2) and an edge that
/// could never fire is noise in the audit trail.
///
/// <para>One partial file per chain, each built by the lane that owns its facts, so the content lanes
/// never edit this file, each other's chain files, or <see cref="RelationshipGraph"/>: the saturation
/// chain (lane 3), the vacuum chain (lane 4), the write chain and the memory / I-O chain (lane 2, with
/// lane 5 adding the wait-event edges into both), and the query chain (lane 7, with lane 6's temp edge).
/// Lane 8's posture facts have NO chain by design (D6) — a posture card stands alone.</para>
///
/// <para>The one engine-level rule every edge inherits: an edge's PREDICATE reads the fact set, never a
/// bar of its own. Thresholds live in <see cref="PgTargetScorer"/> with their lineage; the graph asks
/// only whether the destination fact fired.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph : RelationshipGraph
{
    public PgTargetRelationshipGraph()
        : base(buildSqlServerEdges: false)
    {
        BuildSaturationEdges();
        BuildVacuumEdges();
        BuildWriteEdges();
        BuildMemoryEdges();
        BuildQueryEdges();
    }

    private partial void BuildSaturationEdges();
    private partial void BuildVacuumEdges();
    private partial void BuildWriteEdges();
    private partial void BuildMemoryEdges();
    private partial void BuildQueryEdges();
}
