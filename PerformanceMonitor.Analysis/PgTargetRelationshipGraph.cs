/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

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
/// Lane 8's posture facts have NO chain by design (D6) — a posture card stands alone. v2 (#3691) adds the
/// I/O chain (lane 11), the replication chain (lane 12) and the bloat chain (lane 13), each in its own file.</para>
///
/// <para>The one engine-level rule every edge inherits: an edge's PREDICATE reads the fact set, never a
/// bar of its own. Thresholds live in <see cref="PgTargetScorer"/> with their lineage; the graph asks
/// only whether the destination fact fired.</para>
///
/// <para><b>The one resolution this graph performs itself: the bad-actor alias.</b> <see cref="RelationshipGraph.AddEdge"/>
/// takes an exact destination string and <c>InferenceEngine.Traverse</c> looks it up in the fact set by that
/// string, while the queries family's keys are dynamic (<see cref="PgTargetFactKeys.BadActorKey"/>, one per
/// <c>queryid</c>) — so no static edge can name the statement a spill or a CPU spike leads to (lane 7's
/// note in <c>PgTargetRelationshipGraph.Query.cs</c>). The lanes that own an edge INTO a bad actor declare it
/// against <see cref="PgTargetFactKeys.BadActorFamily"/>, and <see cref="GetActiveEdges"/> rewrites that
/// destination, per call, to the highest-severity <c>PG_BAD_ACTOR_*</c> fact the pass emitted. Two candidates
/// → the higher <see cref="Fact.Severity"/> (ties by ordinal key, so a pass is deterministic); none → the
/// edge is dropped from the active set, never thrown, and the story ends where it did before. The rewrite
/// returns a COPY of the edge — the stored edge keeps the alias, because the graph is a process-wide
/// singleton walked for many servers and a mutated destination would leak one pass's statement into the
/// next. The base body is unchanged for every non-alias edge, which is every edge the SQL Server graph has.</para>
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
        /* v2 (#3691): the three new chains, each an empty stub until its lane lands (11 / 12 / 13). */
        BuildIoEdges();
        BuildReplicationEdges();
        BuildBloatEdges();
        /* wave 3 (#3691, between waves): the blocking chain, an empty stub until lane 17. */
        BuildBlockingEdges();
    }

    private partial void BuildSaturationEdges();
    private partial void BuildVacuumEdges();
    private partial void BuildWriteEdges();
    private partial void BuildMemoryEdges();
    private partial void BuildQueryEdges();
    private partial void BuildIoEdges();
    private partial void BuildReplicationEdges();
    private partial void BuildBloatEdges();
    private partial void BuildBlockingEdges();

    /// <summary>
    /// The shared active-edge read, with the bad-actor alias resolved — see the class summary. Every edge
    /// whose destination is not <see cref="PgTargetFactKeys.BadActorFamily"/> passes through untouched (the
    /// same <see cref="Edge"/> instances the base returns); an alias edge is replaced by a copy pointing at
    /// <see cref="ResolveBadActor"/>'s answer, or omitted when there is none.
    /// </summary>
    public override List<Edge> GetActiveEdges(string sourceKey, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var active = base.GetActiveEdges(sourceKey, factsByKey);
        if (active.Count == 0 || !active.Any(e => IsBadActorAlias(e.Destination)))
            return active;

        var resolved = ResolveBadActor(factsByKey);
        var result = new List<Edge>(active.Count);
        foreach (var edge in active)
        {
            if (!IsBadActorAlias(edge.Destination))
            {
                result.Add(edge);
                continue;
            }

            if (resolved is null)
                continue;

            result.Add(new Edge
            {
                Source = edge.Source,
                Destination = resolved,
                Category = edge.Category,
                PredicateDescription = edge.PredicateDescription,
                Predicate = edge.Predicate,
            });
        }

        return result;
    }

    /// <summary>Whether <paramref name="destination"/> is the alias, exactly — ordinal, because the alias is
    /// declared upper-case and edges are written against the constant.</summary>
    public static bool IsBadActorAlias(string? destination) =>
        string.Equals(destination, PgTargetFactKeys.BadActorFamily, StringComparison.Ordinal);

    /// <summary>
    /// The key of the highest-severity <c>PG_BAD_ACTOR_*</c> fact in <paramref name="factsByKey"/>, or null when
    /// the pass emitted none. Severity, not base severity: the traversal orders candidates by
    /// <see cref="Fact.Severity"/> and the alias should land where the walk would have gone had every statement
    /// been nameable. Ties break on the ordinal key so the same fact set always resolves the same way.
    /// </summary>
    public static string? ResolveBadActor(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        string? bestKey = null;
        Fact? best = null;
        foreach (var (key, fact) in factsByKey)
        {
            if (!key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
                continue;

            if (best is null
                || fact.Severity > best.Severity
                || (fact.Severity == best.Severity && string.CompareOrdinal(key, bestKey) < 0))
            {
                best = fact;
                bestKey = key;
            }
        }

        return bestKey;
    }
}
