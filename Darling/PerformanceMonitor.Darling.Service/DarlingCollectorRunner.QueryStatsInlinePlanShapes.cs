/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingCollectorRunner
{
    /// <summary>
    /// #8994: one plan per statement shape for the inline capture (Azure SQL Database, and any target with
    /// <c>queryStatsDeferredPlanFetch</c> off), which renders its plans in the main read. Runs on a batch just before its
    /// write and returns the shapes the batch stores as pending, for the caller to settle after the commit. Null when the
    /// batch is not query_stats rows that carry inline plans, so every other collector and the deferred fetch pass through.
    /// </summary>
    private List<QueryStatsPlanShapeKey>? LinkInlineQueryStatsPlansByShape<TRow>(
        ServerRuntime server, CollectorContext context, List<TRow> rows)
    {
        if (!context.CapturePlanXml || context.DeferPlanXmlFetch || rows is not List<QueryStatsCollector.Row> inlineRows)
        {
            return null;
        }

        var shapes = _queryStatsShapeCaches.GetOrAdd(server.ServerId, static _ => new PlanDigestCache<QueryStatsPlanShapeKey>());
        var now = DateTime.UtcNow;
        shapes.Prune(now - QueryStatsPlanCacheMaxAge);
        return LinkInlineQueryStatsPlansByShape(server.ServerId, inlineRows, shapes, now);
    }

    /// <summary>
    /// #8994: the inline rows' shape bookkeeping, the inline form of the deferred fetch's (part 1). For each row that carries
    /// plan XML and has a usable shape (<see cref="QueryStatsPlanShapeKey"/>):
    /// <list type="bullet">
    /// <item>The shape's plan is committed: the row links to that digest (<see cref="QueryStatsCollector.Row.KnownPlanDigest"/>)
    /// and drops its XML. It keeps its own measured size.</item>
    /// <item>An earlier row of this batch stored the shape's plan: the row carries that same XML, so the batch stores one
    /// digest for the shape.</item>
    /// <item>Otherwise, when the XML is the statement's real plan, it becomes the shape's plan, pending until the commit
    /// (<see cref="RecordFetchedShape"/>). A row whose XML is the filter's marker
    /// (<see cref="QueryStatsCollector.Row.QueryPlanXmlIsPlaceholder"/>) never enters the shape cache: it stores the marker,
    /// and the shape's next real sighting stores the real plan.</item>
    /// </list>
    /// A row with no plan XML (outside the inline slots, over the size cap, or aged out) is left alone, and so is a row whose
    /// hashes are missing or all zeros.
    /// </summary>
    internal static List<QueryStatsPlanShapeKey> LinkInlineQueryStatsPlansByShape(
        int serverId,
        IEnumerable<QueryStatsCollector.Row> rows,
        PlanDigestCache<QueryStatsPlanShapeKey> shapes,
        DateTime now)
    {
        var pendingShapes = new List<QueryStatsPlanShapeKey>();
        var storedByShape = new Dictionary<QueryStatsPlanShapeKey, (string? PlanXml, long? Bytes)>();
        foreach (var row in rows)
        {
            if (row.QueryPlanXml is null
                || row.KnownPlanDigest is not null
                || QueryStatsPlanShapeKey.TryCreate(serverId, row) is not { } shape)
            {
                continue;
            }

            if (shapes.TryGet(shape, now, out var hit) && hit.Digest is not null)
            {
                row.KnownPlanDigest = hit.Digest;
                row.QueryPlanXml = null;
                row.QueryPlanXmlIsPlaceholder = false;
                continue;
            }

            if (storedByShape.TryGetValue(shape, out var stored))
            {
                row.QueryPlanXml = stored.PlanXml;
                row.QueryPlanXmlIsPlaceholder = false;
                continue;
            }

            if (row.QueryPlanXmlIsPlaceholder)
            {
                continue;
            }

            RecordFetchedShape(
                serverId, row, ProcedureStatsPlanReuse.DigestOf(row.QueryPlanXml), shapes, pendingShapes, storedByShape, now);
        }

        return pendingShapes;
    }
}
